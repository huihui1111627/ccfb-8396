// Package source reads registered SQLite source databases: schema discovery,
// primary-key detection and streaming full scans used by the diff engine.
package source

import (
	"database/sql"
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"

	_ "modernc.org/sqlite"

	"pmssync/internal/rowhash"
)

type Col struct {
	Name     string
	DeclType string // declared type in SQLite, upper-cased
	PK       bool
	Default  any // parsed column default (nil when NULL or none)
}

type DB struct {
	DB   *sql.DB
	Path string
}

// Open opens a source database read-only.
func Open(path string) (*DB, error) {
	d, err := sql.Open("sqlite", "file:"+path+"?mode=ro&_pragma=busy_timeout(10000)")
	if err != nil {
		return nil, err
	}
	if err := d.Ping(); err != nil {
		d.Close()
		return nil, fmt.Errorf("open source %s: %w", path, err)
	}
	return &DB{DB: d, Path: path}, nil
}

func (d *DB) Close() error { return d.DB.Close() }

// ListTables returns user table names.
func (d *DB) ListTables() ([]string, error) {
	rows, err := d.DB.Query(`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// TableSchema returns columns in definition order with PK flags.
func (d *DB) TableSchema(table string) ([]Col, error) {
	rows, err := d.DB.Query(fmt.Sprintf(`PRAGMA table_info(%q)`, table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Col
	for rows.Next() {
		var cid, pk int
		var name, decl string
		var notnull int
		var dflt any
		if err := rows.Scan(&cid, &name, &decl, &notnull, &dflt, &pk); err != nil {
			return nil, err
		}
		out = append(out, Col{Name: name, DeclType: strings.ToUpper(decl), PK: pk > 0, Default: parseDefault(dflt)})
	}
	return out, rows.Err()
}

// parseDefault converts a SQLite dflt_value literal to a Go value matching
// what a table scan returns for rows materialized with that default.
func parseDefault(dflt any) any {
	s, ok := dflt.(string)
	if !ok || s == "" {
		return nil
	}
	u := strings.ToUpper(strings.TrimSpace(s))
	if u == "NULL" {
		return nil
	}
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
		return strings.ReplaceAll(s[1:len(s)-1], "''", "'")
	}
	if i, err := strconv.ParseInt(u, 10, 64); err == nil {
		return i
	}
	if f, err := strconv.ParseFloat(u, 64); err == nil {
		return f
	}
	return s // expressions (e.g. CURRENT_TIMESTAMP): best effort
}

// PKCols returns primary-key column names in PK order, empty if none.
func PKCols(cols []Col) []string {
	var out []string
	for _, c := range cols {
		if c.PK {
			out = append(out, c.Name)
		}
	}
	return out
}

// NameSig fingerprints the set of column names (order-insensitive: additive
// hashing makes column order irrelevant). A change means columns were added
// or dropped.
func NameSig(cols []Col) string {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	sort.Strings(names)
	h := fnv.New64a()
	h.Write([]byte(strings.Join(names, ",")))
	return fmt.Sprintf("%x", h.Sum64())
}

// TypeSig fingerprints column name -> mapped DuckDB type. A change means the
// target DDL needs evolution (hash basis is unaffected).
func TypeSig(cols []Col, duckType func(string) string) string {
	parts := make([]string, len(cols))
	for i, c := range cols {
		parts[i] = c.Name + ":" + duckType(c.DeclType)
	}
	sort.Strings(parts)
	h := fnv.New64a()
	h.Write([]byte(strings.Join(parts, ",")))
	return fmt.Sprintf("%x", h.Sum64())
}

// Scan streams every row of table, invoking fn with column names and values.
// Values are driver natives: int64 / float64 / string / []byte / nil.
func (d *DB) Scan(table string, fn func(cols []string, vals []any) error) error {
	rows, err := d.DB.Query(fmt.Sprintf(`SELECT * FROM %q`, table))
	if err != nil {
		return err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}
		if err := fn(cols, vals); err != nil {
			return err
		}
	}
	return rows.Err()
}

// FetchRows returns full rows for the given keys as JSON-able maps.
// keyCols must align with the tuples in keys (canonical-encoded values).
func (d *DB) FetchRows(table string, keyCols []string, keys [][]any) (map[string]map[string]any, error) {
	out := map[string]map[string]any{}
	if len(keys) == 0 {
		return out, nil
	}
	const chunk = 400
	for i := 0; i < len(keys); i += chunk {
		end := min(i+chunk, len(keys))
		batch := keys[i:end]
		var where string
		args := []any{}
		if len(keyCols) == 1 {
			ph := strings.TrimSuffix(strings.Repeat("?,", len(batch)), ",")
			where = fmt.Sprintf("%q IN (%s)", keyCols[0], ph)
			for _, k := range batch {
				args = append(args, k[0])
			}
		} else {
			ors := make([]string, len(batch))
			for j, k := range batch {
				ands := make([]string, len(keyCols))
				for c, col := range keyCols {
					ands[c] = fmt.Sprintf("%q IS ?", col) // IS handles NULL
					args = append(args, k[c])
				}
				ors[j] = "(" + strings.Join(ands, " AND ") + ")"
			}
			where = strings.Join(ors, " OR ")
		}
		rows, err := d.DB.Query(fmt.Sprintf(`SELECT * FROM %q WHERE %s`, table, where), args...)
		if err != nil {
			return nil, err
		}
		cols, err := rows.Columns()
		if err != nil {
			rows.Close()
			return nil, err
		}
		keyIdx := make([]int, len(keyCols))
		for c, kc := range keyCols {
			keyIdx[c] = indexOf(cols, kc)
		}
		for rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i2 := range vals {
				ptrs[i2] = &vals[i2]
			}
			if err := rows.Scan(ptrs...); err != nil {
				rows.Close()
				return nil, err
			}
			m := map[string]any{}
			kv := make([]any, len(keyCols))
			for i2, c := range cols {
				m[c] = vals[i2]
			}
			for c := range keyCols {
				kv[c] = vals[keyIdx[c]]
			}
			out[keyStringOf(kv)] = m
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func indexOf(ss []string, s string) int {
	for i, x := range ss {
		if x == s {
			return i
		}
	}
	return -1
}

func keyStringOf(vals []any) string {
	return rowhash.KeyString(vals)
}

// Count returns the row count of a table.
func (d *DB) Count(table string) (int64, error) {
	var n int64
	err := d.DB.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM %q`, table)).Scan(&n)
	return n, err
}

// Sum returns SUM(col) as float64.
func (d *DB) Sum(table, col string) (float64, error) {
	var n sql.NullFloat64
	err := d.DB.QueryRow(fmt.Sprintf(`SELECT SUM(%q) FROM %q`, col, table)).Scan(&n)
	return n.Float64, err
}

// SortedCols is a helper for deterministic JSON marshaling in tests.
func SortedCols(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
