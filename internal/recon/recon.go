// Package recon reconciles source tables against the DuckDB zipper tables:
// row counts, configured amount-column sums, and — on mismatch — row-level
// localization of the differing keys.
package recon

import (
	"encoding/json"
	"fmt"
	"math"

	"pmssync/internal/diff"
	"pmssync/internal/rowhash"
	"pmssync/internal/source"
	"pmssync/internal/store"
	"pmssync/internal/target"
)

type Mismatch struct {
	Key     string `json:"key"`
	Kind    string `json:"kind"` // missing_in_target | extra_in_target | content_diff
	SrcHash uint64 `json:"src_hash,omitempty"`
	TgtHash uint64 `json:"tgt_hash,omitempty"`
}

type Result struct {
	Run      *store.ReconRun
	Mismatch []Mismatch
}

const maxDetail = 500

// Run reconciles one table and persists a recon run record.
func Run(st *store.Store, tg *target.Target, t *store.Table) (*Result, error) {
	src, err := st.GetSource(t.SourceID)
	if err != nil {
		return nil, err
	}
	sdb, err := source.Open(src.Path)
	if err != nil {
		return nil, err
	}
	defer sdb.Close()

	cols, err := sdb.TableSchema(t.Name)
	if err != nil {
		return nil, err
	}
	if t.KeyMode == "pk" && len(t.KeyCols) == 0 {
		t.KeyCols = source.PKCols(cols)
	}

	run := &store.ReconRun{TableID: t.ID}

	run.SrcCount, err = sdb.Count(t.Name)
	if err != nil {
		return nil, err
	}
	run.TgtCount, err = tg.LiveCount(t.SourceID, t.Name)
	if err != nil {
		run.Status = "error"
		run.Detail = err.Error()
		st.SaveReconRun(run)
		return &Result{Run: run}, nil
	}

	// amount sums
	type sumPair struct {
		Src float64 `json:"src"`
		Tgt float64 `json:"tgt"`
		OK  bool    `json:"ok"`
	}
	sums := map[string]sumPair{}
	sumsOK := true
	for _, col := range t.AmountCols {
		ss, err1 := sdb.Sum(t.Name, col)
		ts, err2 := tg.LiveSum(t.SourceID, t.Name, col)
		if err1 != nil || err2 != nil {
			sumsOK = false
			continue
		}
		ok := math.Abs(ss-ts) < 0.01
		if !ok {
			sumsOK = false
		}
		sums[col] = sumPair{Src: math.Round(ss*100) / 100, Tgt: ts, OK: ok}
	}
	sj, _ := json.Marshal(sums)
	run.SumsJSON = string(sj)

	if run.SrcCount == run.TgtCount && sumsOK {
		run.Status = "ok"
		st.SaveReconRun(run)
		return &Result{Run: run}, nil
	}

	// row-level localization: hash-compare source rows vs target live versions
	// (hashes recomputed from target content on the current schema basis)
	colNames := make([]string, len(cols))
	for i, c := range cols {
		colNames[i] = c.Name
	}
	tgtHashes := map[string]uint64{}
	if err := tg.ScanLiveRows(t.SourceID, t.Name, colNames, func(key string, vals []any) error {
		tgtHashes[key] = rowhash.HashRowCols(colNames, vals)
		return nil
	}); err != nil {
		run.Status = "error"
		run.Detail = err.Error()
		st.SaveReconRun(run)
		return &Result{Run: run}, nil
	}
	var mismatches []Mismatch
	err = sdb.Scan(t.Name, func(cols []string, vals []any) error {
		k := diff.KeyOf(t, cols, vals)
		h := rowhash.HashRowCols(cols, vals)
		th, ok := tgtHashes[k]
		if !ok {
			mismatches = append(mismatches, Mismatch{Key: k, Kind: "missing_in_target", SrcHash: h})
		} else if th != h {
			mismatches = append(mismatches, Mismatch{Key: k, Kind: "content_diff", SrcHash: h, TgtHash: th})
		}
		delete(tgtHashes, k)
		if len(mismatches) >= maxDetail {
			return fmt.Errorf("detail cap reached")
		}
		return nil
	})
	if err != nil && len(mismatches) < maxDetail {
		return nil, err
	}
	for k, h := range tgtHashes {
		mismatches = append(mismatches, Mismatch{Key: k, Kind: "extra_in_target", TgtHash: h})
		if len(mismatches) >= maxDetail {
			break
		}
	}
	dj, _ := json.Marshal(mismatches)
	run.Detail = string(dj)
	run.Status = "mismatch"
	st.SaveReconRun(run)
	return &Result{Run: run, Mismatch: mismatches}, nil
}
