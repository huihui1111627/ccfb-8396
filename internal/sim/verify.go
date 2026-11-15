package sim

import (
	"database/sql"
	"fmt"
	"sort"

	"pmssync/internal/diff"
	"pmssync/internal/rowhash"
	"pmssync/internal/source"
	"pmssync/internal/store"
	"pmssync/internal/target"
)

// Report is the outcome of verifying one sync round against the truth log.
type Report struct {
	OK              bool     `json:"ok"`
	KeysChecked     int      `json:"keys_checked"`
	VersionsChecked int      `json:"versions_checked"`
	LiveChecked     int      `json:"live_checked"`
	Mismatches      []string `json:"mismatches"`
}

const maxMismatch = 50

func (r *Report) add(format string, args ...any) {
	if len(r.Mismatches) < maxMismatch {
		r.Mismatches = append(r.Mismatches, fmt.Sprintf(format, args...))
	}
	r.OK = false
}

// Verify compares, for every registered source/table, the full per-key
// version chain in the DuckDB zipper against the truth log, and the current
// live state against the source database. Every version recorded by the
// simulator must be reproduced exactly.
func Verify(st *store.Store, tg *target.Target, truthDir string) (*Report, error) {
	rep := &Report{OK: true}
	tr, err := openTruth(truthDir)
	if err != nil {
		return nil, err
	}
	defer tr.Close()

	sources, err := st.ListSources()
	if err != nil {
		return nil, err
	}
	for i := range sources {
		src := &sources[i]
		tables, err := st.ListTables(src.ID)
		if err != nil {
			return nil, err
		}
		for i := range tables {
			t := &tables[i]
			if !t.Enabled {
				continue
			}
			if err := verifyTable(rep, tr, tg, src, t); err != nil {
				return nil, err
			}
		}
	}
	return rep, nil
}

func verifyTable(rep *Report, tr *sql.DB, tg *target.Target, src *store.Source, t *store.Table) error {
	label := fmt.Sprintf("src%d.%s", src.ID, t.Name)

	// 1. expected chains from truth
	rows, err := tr.Query(`SELECT k, op, h FROM truth WHERE source_id=? AND tbl=? ORDER BY id`, src.ID, t.Name)
	if err != nil {
		return err
	}
	expected := map[string][]target.Version{}
	for rows.Next() {
		var k, op string
		var h int64
		if err := rows.Scan(&k, &op, &h); err != nil {
			rows.Close()
			return err
		}
		expected[k] = append(expected[k], target.Version{Op: op, Hash: uint64(h)})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	// 2. actual chains from the zipper
	actual, err := tg.VersionChains(src.ID, t.Name)
	if err != nil {
		return err
	}

	keys := map[string]bool{}
	for k := range expected {
		keys[k] = true
	}
	for k := range actual {
		keys[k] = true
	}
	sorted := make([]string, 0, len(keys))
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)
	for _, k := range sorted {
		exp, act := expected[k], actual[k]
		rep.KeysChecked++
		rep.VersionsChecked += len(act)
		if len(exp) != len(act) {
			rep.add("%s key %s: version count truth=%d target=%d", label, k, len(exp), len(act))
			continue
		}
		for i := range exp {
			if exp[i].Op != act[i].Op || exp[i].Hash != act[i].Hash {
				rep.add("%s key %s: version %d truth=(%s,%x) target=(%s,%x)",
					label, k, i, exp[i].Op, exp[i].Hash, act[i].Op, act[i].Hash)
			}
		}
	}

	// 3. live-state compare: source rows vs target current live versions
	// (target hashes recomputed from content on the current schema basis)
	sdb, err := source.Open(src.Path)
	if err != nil {
		return err
	}
	defer sdb.Close()
	cols, err := sdb.TableSchema(t.Name)
	if err != nil {
		return err
	}
	if t.KeyMode == "pk" && len(t.KeyCols) == 0 {
		t.KeyCols = source.PKCols(cols)
	}
	colNames := make([]string, len(cols))
	for i, c := range cols {
		colNames[i] = c.Name
	}
	liveTgt := map[string]uint64{}
	if err := tg.ScanLiveRows(src.ID, t.Name, colNames, func(key string, vals []any) error {
		liveTgt[key] = rowhash.HashRowCols(colNames, vals)
		return nil
	}); err != nil {
		return err
	}
	seen := map[string]bool{}
	err = sdb.Scan(t.Name, func(cols []string, vals []any) error {
		k := diff.KeyOf(t, cols, vals)
		h := rowhash.HashRowCols(cols, vals)
		rep.LiveChecked++
		th, ok := liveTgt[k]
		if !ok {
			rep.add("%s key %s: live in source, missing in target", label, k)
		} else if th != h {
			rep.add("%s key %s: live hash mismatch src=%x tgt=%x", label, k, h, th)
		}
		seen[k] = true
		return nil
	})
	if err != nil {
		return err
	}
	for k := range liveTgt {
		if !seen[k] {
			rep.add("%s key %s: live in target, missing in source", label, k)
		}
	}
	return nil
}
