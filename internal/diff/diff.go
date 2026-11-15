// Package diff implements the snapshot-diff engine: per-bucket row-hash
// comparison against the persisted state of the previous batch, producing
// insert/update/delete change sets. Soft deletes are classified from the
// configured soft-delete column; delete+reinsert of the same key between
// snapshots naturally surfaces as an update.
package diff

import (
	"encoding/hex"
	"encoding/json"
	"fmt"

	"pmssync/internal/rowhash"
	"pmssync/internal/source"
	"pmssync/internal/store"
)

// KeyOf computes the storage key for a row according to the table config.
func KeyOf(t *store.Table, cols []string, vals []any) string {
	if t.KeyMode == "row" {
		return "row:" + hex.EncodeToString(u64bytes(rowhash.HashRowCols(cols, vals)))
	}
	kv := make([]any, len(t.KeyCols))
	for i, kc := range t.KeyCols {
		kv[i] = vals[indexOf(cols, kc)]
	}
	return rowhash.KeyString(kv)
}

func u64bytes(v uint64) []byte {
	return []byte{byte(v >> 56), byte(v >> 48), byte(v >> 40), byte(v >> 32), byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
}

func indexOf(ss []string, s string) int {
	for i, x := range ss {
		if x == s {
			return i
		}
	}
	return -1
}

// scanBuckets does one full pass and returns per-bucket (count, sum-hash).
func scanBuckets(src *source.DB, t *store.Table) (map[int]*store.ChunkState, error) {
	acc := map[int]*store.ChunkState{}
	err := src.Scan(t.Name, func(cols []string, vals []any) error {
		k := KeyOf(t, cols, vals)
		h := rowhash.HashRowCols(cols, vals)
		b := int(rowhash.BucketOf(k, t.Buckets))
		cs := acc[b]
		if cs == nil {
			cs = &store.ChunkState{Bucket: b}
			acc[b] = cs
		}
		cs.RowCount++
		cs.Hash += h
		return nil
	})
	return acc, err
}

// Run executes the full diff for one table and persists results atomically.
// Returns change counts.
func Run(st *store.Store, src *source.DB, t *store.Table, batchID int64) (ins, upd, del int64, err error) {
	oldChunks, err := st.LoadChunkStates(t.ID)
	if err != nil {
		return 0, 0, 0, err
	}
	newChunks, err := scanBuckets(src, t)
	if err != nil {
		return 0, 0, 0, err
	}
	// find changed buckets (new, changed, or vanished)
	changed := map[int]bool{}
	for b, nc := range newChunks {
		oc, ok := oldChunks[b]
		if !ok || oc.RowCount != nc.RowCount || oc.Hash != nc.Hash {
			changed[b] = true
		}
	}
	for b := range oldChunks {
		if _, ok := newChunks[b]; !ok {
			changed[b] = true
		}
	}

	// preload old key state of changed buckets
	oldKeys := map[int]map[string]uint64{}
	for b := range changed {
		m, err := st.LoadBucketKeys(t.ID, b)
		if err != nil {
			return 0, 0, 0, err
		}
		oldKeys[b] = m
	}

	// second pass: collect new state of changed buckets + full rows of I/U
	type pending struct {
		key  string
		hash uint64
		row  map[string]any
	}
	newState := map[int]map[string]uint64{}
	pendings := map[int][]pending{} // inserts & updates only
	for b := range changed {
		newState[b] = map[string]uint64{}
	}
	err = src.Scan(t.Name, func(cols []string, vals []any) error {
		k := KeyOf(t, cols, vals)
		b := int(rowhash.BucketOf(k, t.Buckets))
		if !changed[b] {
			return nil
		}
		h := rowhash.HashRowCols(cols, vals)
		newState[b][k] = h
		oldH, existed := oldKeys[b][k]
		if !existed || oldH != h {
			row := map[string]any{}
			for i, c := range cols {
				row[c] = vals[i]
			}
			pendings[b] = append(pendings[b], pending{key: k, hash: h, row: row})
		}
		return nil
	})
	if err != nil {
		return 0, 0, 0, err
	}

	// build diffs
	diffs := []store.BucketDiff{}
	for b := range changed {
		bd := store.BucketDiff{Bucket: b, Keys: newState[b]}
		if nc, ok := newChunks[b]; ok {
			bd.RowCount, bd.Hash = nc.RowCount, nc.Hash
		}
		for _, p := range pendings[b] {
			_, existed := oldKeys[b][p.key]
			op := "I"
			if existed {
				op = "U"
			}
			rj, _ := json.Marshal(p.row)
			d := store.Change{BatchID: batchID, TableID: t.ID, Op: op, Key: p.key, RowJSON: string(rj), Hash: p.hash}
			diffsAppend(&bd, d, &ins, &upd, &del)
		}
		// remaining old keys vanished => physical deletes
		for k := range oldKeys[b] {
			if _, ok := newState[b][k]; !ok {
				d := store.Change{BatchID: batchID, TableID: t.ID, Op: "D", Key: k}
				diffsAppend(&bd, d, &ins, &upd, &del)
			}
		}
		diffs = append(diffs, bd)
	}

	// classify soft deletes: an update whose after-image has the soft-delete
	// flag set is reported as 'S' in the change detail.
	if t.SoftDeleteCol != "" {
		for bi := range diffs {
			for ci := range diffs[bi].Changes {
				c := &diffs[bi].Changes[ci]
				if c.Op != "U" {
					continue
				}
				var row map[string]any
				if json.Unmarshal([]byte(c.RowJSON), &row) == nil {
					if truthy(row[t.SoftDeleteCol]) {
						c.Op = "S"
					}
				}
			}
		}
	}

	if err := st.PersistBucketDiffs(batchID, t.ID, diffs, ins, upd, del); err != nil {
		return 0, 0, 0, err
	}
	return ins, upd, del, nil
}

func diffsAppend(bd *store.BucketDiff, c store.Change, ins, upd, del *int64) {
	bd.Changes = append(bd.Changes, c)
	switch c.Op {
	case "I":
		*ins++
	case "U", "S":
		*upd++
	case "D":
		*del++
	}
}

func truthy(v any) bool {
	switch t := v.(type) {
	case nil:
		return false
	case int64:
		return t != 0
	case float64:
		return t != 0
	case string:
		return t != "" && t != "0"
	case bool:
		return t
	}
	return false
}

// Rebaseline recomputes all key/chunk state with the current schema
// (used after a schema change altered the hash basis) without emitting changes.
func Rebaseline(st *store.Store, src *source.DB, t *store.Table) error {
	perBucket := map[int]map[string]uint64{}
	acc := map[int]*store.ChunkState{}
	err := src.Scan(t.Name, func(cols []string, vals []any) error {
		k := KeyOf(t, cols, vals)
		h := rowhash.HashRowCols(cols, vals)
		b := int(rowhash.BucketOf(k, t.Buckets))
		m := perBucket[b]
		if m == nil {
			m = map[string]uint64{}
			perBucket[b] = m
			acc[b] = &store.ChunkState{Bucket: b}
		}
		m[k] = h
		acc[b].RowCount++
		acc[b].Hash += h
		return nil
	})
	if err != nil {
		return err
	}
	diffs := make([]store.BucketDiff, 0, len(perBucket))
	for b, keys := range perBucket {
		diffs = append(diffs, store.BucketDiff{
			Bucket:   b,
			RowCount: acc[b].RowCount,
			Hash:     acc[b].Hash,
			Keys:     keys,
		})
	}
	if err := st.Rebaseline(t.ID, diffs); err != nil {
		return fmt.Errorf("rebaseline: %w", err)
	}
	return nil
}
