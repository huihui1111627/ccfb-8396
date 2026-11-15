// Package target manages the DuckDB warehouse: per-source schemas, SCD2
// (zipper) tables with version validity windows, automatic schema evolution
// and transactional change application.
package target

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/marcboeker/go-duckdb/v2"

	"pmssync/internal/source"
	"pmssync/internal/store"
)

// Meta columns appended to every zipper table.
var MetaCols = []string{"_k", "_op", "_batch_id", "_valid_from", "_valid_to", "_is_current", "_row_hash"}

type Target struct {
	db   *sql.DB
	wrMu sync.Mutex // serialize writers
	Path string
}

func Open(path string) (*Target, error) {
	db, err := sql.Open("duckdb", path)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("open duckdb %s: %w", path, err)
	}
	return &Target{db: db, Path: path}, nil
}

func (t *Target) Close() error { return t.db.Close() }

func SchemaName(sourceID int64) string { return fmt.Sprintf("src_%d", sourceID) }

// DuckType maps a SQLite declared type to a DuckDB type.
func DuckType(decl string) string {
	u := strings.ToUpper(decl)
	switch {
	case u == "":
		return "VARCHAR"
	case strings.Contains(u, "INT"):
		return "BIGINT"
	case strings.Contains(u, "CHAR"), strings.Contains(u, "CLOB"), strings.Contains(u, "TEXT"):
		return "VARCHAR"
	case strings.Contains(u, "BLOB"):
		return "BLOB"
	case strings.Contains(u, "REAL"), strings.Contains(u, "FLOA"), strings.Contains(u, "DOUB"):
		return "DOUBLE"
	case strings.Contains(u, "NUMERIC"), strings.Contains(u, "DEC"):
		return "DOUBLE"
	case strings.Contains(u, "BOOL"):
		return "BIGINT"
	case strings.Contains(u, "DATE"), strings.Contains(u, "TIME"):
		return "VARCHAR" // keep source text, avoid tz/precision drift
	default:
		return "VARCHAR"
	}
}

func (t *Target) EnsureSchema(sourceID int64) error {
	_, err := t.db.Exec(fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, SchemaName(sourceID)))
	return err
}

// EnsureTable creates the zipper table if missing and evolves its schema to
// match the source columns. History is never dropped: type changes fall back
// to renaming the old column and adding a new one.
func (t *Target) EnsureTable(sourceID int64, table string, cols []source.Col) error {
	t.wrMu.Lock()
	defer t.wrMu.Unlock()
	if err := t.EnsureSchema(sourceID); err != nil {
		return err
	}
	full := fmt.Sprintf(`%s.%q`, SchemaName(sourceID), table)
	exists, err := t.tableExists(sourceID, table)
	if err != nil {
		return err
	}
	if !exists {
		defs := []string{}
		for _, c := range cols {
			defs = append(defs, fmt.Sprintf(`%q %s`, c.Name, DuckType(c.DeclType)))
		}
		defs = append(defs,
			`_k VARCHAR`, `_op CHAR(1)`, `_batch_id BIGINT`,
			`_valid_from TIMESTAMP`, `_valid_to TIMESTAMP`, `_is_current BOOLEAN`, `_row_hash BIGINT`)
		if _, err := t.db.Exec(fmt.Sprintf(`CREATE TABLE %s (%s)`, full, strings.Join(defs, ","))); err != nil {
			return err
		}
		if _, err := t.db.Exec(fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %q ON %s(_k, _is_current)`,
			"idx_"+table+"_k", full)); err != nil {
			return err
		}
		return nil
	}
	return t.evolveLocked(sourceID, table, cols)
}

func (t *Target) tableExists(sourceID int64, table string) (bool, error) {
	var n int
	err := t.db.QueryRow(`SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=? AND table_name=?`,
		SchemaName(sourceID), table).Scan(&n)
	return n > 0, err
}

// Evolve aligns the target table with a changed source schema.
func (t *Target) Evolve(sourceID int64, table string, cols []source.Col) error {
	t.wrMu.Lock()
	defer t.wrMu.Unlock()
	return t.evolveLocked(sourceID, table, cols)
}

func (t *Target) evolveLocked(sourceID int64, table string, cols []source.Col) error {
	full := fmt.Sprintf(`%s.%q`, SchemaName(sourceID), table)
	cur, err := t.columns(sourceID, table)
	if err != nil {
		return err
	}
	for _, c := range cols {
		want := DuckType(c.DeclType)
		got, ok := cur[c.Name]
		if !ok {
			if _, err := t.db.Exec(fmt.Sprintf(`ALTER TABLE %s ADD COLUMN %q %s%s`, full, c.Name, want, defaultClause(c.Default))); err != nil {
				return fmt.Errorf("add column %s: %w", c.Name, err)
			}
			continue
		}
		if got == want {
			continue
		}
		// type change: try in-place cast first
		if _, err := t.db.Exec(fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN %q SET DATA TYPE %s`, full, c.Name, want)); err != nil {
			// fallback: keep history under a renamed column, add fresh column
			bak := fmt.Sprintf("%s__%d", c.Name, time.Now().Unix())
			if _, err2 := t.db.Exec(fmt.Sprintf(`ALTER TABLE %s RENAME COLUMN %q TO %q`, full, c.Name, bak)); err2 != nil {
				return fmt.Errorf("rename column %s: %w (after type change failed: %v)", c.Name, err2, err)
			}
			if _, err2 := t.db.Exec(fmt.Sprintf(`ALTER TABLE %s ADD COLUMN %q %s`, full, c.Name, want)); err2 != nil {
				return fmt.Errorf("re-add column %s: %w", c.Name, err2)
			}
		}
	}
	return nil
}

func (t *Target) columns(sourceID int64, table string) (map[string]string, error) {
	rows, err := t.db.Query(`SELECT column_name, data_type FROM information_schema.columns WHERE table_schema=? AND table_name=?`,
		SchemaName(sourceID), table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var n, ty string
		if err := rows.Scan(&n, &ty); err != nil {
			return nil, err
		}
		out[n] = ty
	}
	return out, rows.Err()
}

// decodeRow converts a stored JSON row back to typed driver values.
func decodeRow(rowJSON string) (map[string]any, error) {
	dec := json.NewDecoder(strings.NewReader(rowJSON))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		return nil, err
	}
	for k, v := range m {
		if n, ok := v.(json.Number); ok {
			s := n.String()
			if strings.ContainsAny(s, ".eE") {
				if f, err := n.Float64(); err == nil {
					m[k] = f
				}
			} else if i, err := n.Int64(); err == nil {
				m[k] = i
			} else if f, err := n.Float64(); err == nil {
				m[k] = f
			}
		}
	}
	return m, nil
}

// ApplyChanges applies one table's change set as SCD2 versions inside a
// single DuckDB transaction. Re-running after a crash is safe: the whole
// table apply either commits or rolls back, and the meta store only marks
// the table applied after commit.
func (t *Target) ApplyChanges(sourceID int64, table string, cols []source.Col, changes []store.Change, batchID int64, ts time.Time) error {
	t.wrMu.Lock()
	defer t.wrMu.Unlock()

	full := fmt.Sprintf(`%s.%q`, SchemaName(sourceID), table)
	ctx := context.Background()
	conn, err := t.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	// staging tables (temp: vanish on crash, meta store still has the changes)
	if _, err := conn.ExecContext(ctx, `CREATE OR REPLACE TEMP TABLE stg_close(k VARCHAR)`); err != nil {
		return err
	}
	stgCols := []string{}
	for _, c := range cols {
		stgCols = append(stgCols, fmt.Sprintf(`%q %s`, c.Name, DuckType(c.DeclType)))
	}
	stgCols = append(stgCols, `_op CHAR(1)`, `_k VARCHAR`, `_row_hash BIGINT`)
	if _, err := conn.ExecContext(ctx, fmt.Sprintf(`CREATE OR REPLACE TEMP TABLE stg_rows(%s)`, strings.Join(stgCols, ","))); err != nil {
		return err
	}

	err = conn.Raw(func(dc any) error {
		closeApp, err := duckdb.NewAppenderFromConn(dc.(driver.Conn), "", "stg_close")
		if err != nil {
			return err
		}
		rowApp, err := duckdb.NewAppenderFromConn(dc.(driver.Conn), "", "stg_rows")
		if err != nil {
			return err
		}
		for _, ch := range changes {
			switch ch.Op {
			case "D":
				if err := closeApp.AppendRow(ch.Key); err != nil {
					return fmt.Errorf("append close key %s: %w", ch.Key, err)
				}
				// tombstone version: business columns NULL
				vals := make([]driver.Value, 0, len(cols)+3)
				for range cols {
					vals = append(vals, nil)
				}
				vals = append(vals, "D", ch.Key, int64(ch.Hash))
				if err := rowApp.AppendRow(vals...); err != nil {
					return fmt.Errorf("append tombstone %s: %w", ch.Key, err)
				}
			case "I", "U", "S":
				row, err := decodeRow(ch.RowJSON)
				if err != nil {
					return fmt.Errorf("decode row %s: %w", ch.Key, err)
				}
				if ch.Op != "I" {
					if err := closeApp.AppendRow(ch.Key); err != nil {
						return fmt.Errorf("append close key %s: %w", ch.Key, err)
					}
				}
				vals := make([]driver.Value, 0, len(cols)+3)
				for _, c := range cols {
					vals = append(vals, coerce(row[c.Name]))
				}
				vals = append(vals, ch.Op, ch.Key, int64(ch.Hash))
				if err := rowApp.AppendRow(vals...); err != nil {
					return fmt.Errorf("append row %s: %w", ch.Key, err)
				}
			}
		}
		if err := closeApp.Close(); err != nil {
			return err
		}
		return rowApp.Close()
	})
	if err != nil {
		return err
	}

	// durable mutation: one transaction
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(
		`UPDATE %s t SET _valid_to=?, _is_current=false FROM stg_close c WHERE t._k=c.k AND t._is_current`, full), ts); err != nil {
		return fmt.Errorf("close versions: %w", err)
	}
	// stg_rows layout: bizcols..., _op, _k, _row_hash
	// target layout:   bizcols..., _k, _op, _batch_id, _valid_from, _valid_to, _is_current, _row_hash
	insertSQL := fmt.Sprintf(`INSERT INTO %s (%s) SELECT %s, ?, ?, NULL, true, _row_hash FROM stg_rows`,
		full,
		strings.Join(append(bizQuoted(cols), `_k`, `_op`, `_batch_id`, `_valid_from`, `_valid_to`, `_is_current`, `_row_hash`), ","),
		strings.Join(append(bizQuoted(cols), `_k`, `_op`), ","))
	if _, err := tx.ExecContext(ctx, insertSQL, batchID, ts); err != nil {
		return fmt.Errorf("insert versions: %w", err)
	}
	return tx.Commit()
}

func bizQuoted(cols []source.Col) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = fmt.Sprintf(`%q`, c.Name)
	}
	return out
}

// defaultClause renders a parsed SQLite column default as a DuckDB DEFAULT
// clause, so rows materialized on both sides carry the same value.
func defaultClause(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return " DEFAULT '" + strings.ReplaceAll(t, "'", "''") + "'"
	case int64:
		return fmt.Sprintf(" DEFAULT %d", t)
	case float64:
		return fmt.Sprintf(" DEFAULT %v", t)
	}
	return ""
}

// coerce maps decoded JSON values to driver-friendly types.
func coerce(v any) any {
	switch t := v.(type) {
	case json.Number:
		s := t.String()
		if strings.ContainsAny(s, ".eE") {
			f, _ := t.Float64()
			return f
		}
		if i, err := t.Int64(); err == nil {
			return i
		}
		f, _ := t.Float64()
		return f
	default:
		return v
	}
}

// LiveCount counts current non-tombstone versions.
func (t *Target) LiveCount(sourceID int64, table string) (int64, error) {
	var n int64
	err := t.db.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM %s.%q WHERE _is_current AND _op<>'D'`, SchemaName(sourceID), table)).Scan(&n)
	return n, err
}

// LiveSum sums a numeric column over current live versions.
func (t *Target) LiveSum(sourceID int64, table, col string) (float64, error) {
	var n sql.NullFloat64
	err := t.db.QueryRow(fmt.Sprintf(`SELECT ROUND(SUM(%q),2) FROM %s.%q WHERE _is_current AND _op<>'D'`, col, SchemaName(sourceID), table)).Scan(&n)
	return n.Float64, err
}

// LiveHashes returns key -> row hash of current live versions.
func (t *Target) LiveHashes(sourceID int64, table string) (map[string]uint64, error) {
	rows, err := t.db.Query(fmt.Sprintf(`SELECT _k, _row_hash FROM %s.%q WHERE _is_current AND _op<>'D'`, SchemaName(sourceID), table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]uint64{}
	for rows.Next() {
		var k string
		var h int64
		if err := rows.Scan(&k, &h); err != nil {
			return nil, err
		}
		out[k] = uint64(h)
	}
	return out, rows.Err()
}

// ScanLiveRows streams the business columns of every current live version.
// Used to recompute content hashes on the current schema basis (the stored
// _row_hash of older versions may predate schema changes).
func (t *Target) ScanLiveRows(sourceID int64, table string, cols []string, fn func(key string, vals []any) error) error {
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = fmt.Sprintf(`%q`, c)
	}
	q := fmt.Sprintf(`SELECT _k, %s FROM %s.%q WHERE _is_current AND _op<>'D'`,
		strings.Join(quoted, ","), SchemaName(sourceID), table)
	rows, err := t.db.Query(q)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		vals := make([]any, len(cols)+1)
		ptrs := make([]any, len(cols)+1)
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}
		key, _ := vals[0].(string)
		if err := fn(key, vals[1:]); err != nil {
			return err
		}
	}
	return rows.Err()
}

// Version is one zipper row for chain verification.
type Version struct {
	Op   string
	Hash uint64
}

// VersionChains returns key -> ordered (op, hash) version chain.
func (t *Target) VersionChains(sourceID int64, table string) (map[string][]Version, error) {
	exists, err := t.tableExists(sourceID, table)
	if err != nil || !exists {
		return map[string][]Version{}, err
	}
	rows, err := t.db.Query(fmt.Sprintf(`SELECT _k, _op, _row_hash FROM %s.%q ORDER BY _k, _batch_id`, SchemaName(sourceID), table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string][]Version{}
	for rows.Next() {
		var k, op string
		var h int64
		if err := rows.Scan(&k, &op, &h); err != nil {
			return nil, err
		}
		out[k] = append(out[k], Version{Op: op, Hash: uint64(h)})
	}
	return out, rows.Err()
}
