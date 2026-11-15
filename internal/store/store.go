// Package store is the system's own metadata database (SQLite): registered
// sources, table sync configs, batches, per-bucket chunk hashes, per-key row
// hashes, change details and reconciliation runs.
package store

import (
	"database/sql"
	"encoding/json"
	"time"

	_ "modernc.org/sqlite"
)

type Store struct {
	db *sql.DB
}

func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate() error {
	_, err := s.db.Exec(`
CREATE TABLE IF NOT EXISTS sources(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  path TEXT NOT NULL,
  created_at TEXT,
  last_batch_id INTEGER DEFAULT 0,
  last_success_at TEXT DEFAULT '',
  fail_count INTEGER DEFAULT 0,
  total_count INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS tables(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_id INTEGER NOT NULL,
  name TEXT NOT NULL,
  enabled INTEGER DEFAULT 1,
  key_mode TEXT NOT NULL DEFAULT 'pk',
  key_cols TEXT NOT NULL DEFAULT '[]',
  soft_delete_col TEXT DEFAULT '',
  amount_cols TEXT DEFAULT '[]',
  buckets INTEGER DEFAULT 64,
  schema_sig TEXT DEFAULT '',
  UNIQUE(source_id, name)
);
CREATE TABLE IF NOT EXISTS batches(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_id INTEGER NOT NULL,
  status TEXT NOT NULL,
  started_at TEXT, finished_at TEXT DEFAULT '',
  error TEXT DEFAULT '',
  inserts INTEGER DEFAULT 0, updates INTEGER DEFAULT 0, deletes INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS batch_tables(
  batch_id INTEGER NOT NULL,
  table_id INTEGER NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  inserts INTEGER DEFAULT 0, updates INTEGER DEFAULT 0, deletes INTEGER DEFAULT 0,
  error TEXT DEFAULT '',
  PRIMARY KEY(batch_id, table_id)
);
CREATE TABLE IF NOT EXISTS key_state(
  table_id INTEGER NOT NULL,
  k TEXT NOT NULL,
  h INTEGER NOT NULL,
  bucket INTEGER NOT NULL,
  PRIMARY KEY(table_id, k)
);
CREATE INDEX IF NOT EXISTS idx_keystate_bucket ON key_state(table_id, bucket);
CREATE TABLE IF NOT EXISTS chunk_state(
  table_id INTEGER NOT NULL,
  bucket INTEGER NOT NULL,
  row_count INTEGER NOT NULL,
  h INTEGER NOT NULL,
  PRIMARY KEY(table_id, bucket)
);
CREATE TABLE IF NOT EXISTS changes(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id INTEGER NOT NULL,
  table_id INTEGER NOT NULL,
  op TEXT NOT NULL,
  k TEXT NOT NULL,
  row_json TEXT DEFAULT '',
  h INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_changes_batch ON changes(batch_id, table_id);
CREATE TABLE IF NOT EXISTS recon_runs(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  table_id INTEGER NOT NULL,
  ran_at TEXT,
  status TEXT,
  src_count INTEGER, tgt_count INTEGER,
  sums_json TEXT DEFAULT '',
  detail TEXT DEFAULT ''
);
`)
	return err
}

// ---------- types ----------

type Source struct {
	ID            int64
	Name          string
	Path          string
	CreatedAt     string
	LastBatchID   int64
	LastSuccessAt string
	FailCount     int64
	TotalCount    int64
}

type Table struct {
	ID            int64
	SourceID      int64
	Name          string
	Enabled       bool
	KeyMode       string // pk | business | row
	KeyCols       []string
	SoftDeleteCol string
	AmountCols    []string
	Buckets       int
	SchemaSig     string
}

type Batch struct {
	ID         int64
	SourceID   int64
	Status     string
	StartedAt  string
	FinishedAt string
	Error      string
	Inserts    int64
	Updates    int64
	Deletes    int64
}

type BatchTable struct {
	BatchID int64
	TableID int64
	Status  string
	Inserts int64
	Updates int64
	Deletes int64
	Error   string
}

type Change struct {
	ID      int64
	BatchID int64
	TableID int64
	Op      string
	Key     string
	RowJSON string
	Hash    uint64
}

type ReconRun struct {
	ID       int64
	TableID  int64
	RanAt    string
	Status   string
	SrcCount int64
	TgtCount int64
	SumsJSON string
	Detail   string
}

func now() string { return time.Now().Format("2006-01-02 15:04:05.000") }

// ---------- sources ----------

func (s *Store) AddSource(name, path string) (int64, error) {
	r, err := s.db.Exec(`INSERT INTO sources(name,path,created_at) VALUES(?,?,?)`, name, path, now())
	if err != nil {
		return 0, err
	}
	return r.LastInsertId()
}

func (s *Store) DeleteSource(id int64) error {
	_, err := s.db.Exec(`DELETE FROM sources WHERE id=?`, id)
	return err
}

func (s *Store) ListSources() ([]Source, error) {
	rows, err := s.db.Query(`SELECT id,name,path,created_at,last_batch_id,last_success_at,fail_count,total_count FROM sources ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Source
	for rows.Next() {
		var x Source
		if err := rows.Scan(&x.ID, &x.Name, &x.Path, &x.CreatedAt, &x.LastBatchID, &x.LastSuccessAt, &x.FailCount, &x.TotalCount); err != nil {
			return nil, err
		}
		out = append(out, x)
	}
	return out, rows.Err()
}

func (s *Store) GetSource(id int64) (*Source, error) {
	var x Source
	err := s.db.QueryRow(`SELECT id,name,path,created_at,last_batch_id,last_success_at,fail_count,total_count FROM sources WHERE id=?`, id).
		Scan(&x.ID, &x.Name, &x.Path, &x.CreatedAt, &x.LastBatchID, &x.LastSuccessAt, &x.FailCount, &x.TotalCount)
	if err != nil {
		return nil, err
	}
	return &x, nil
}

func (s *Store) SourceSynced(id int64, batchID int64) error {
	_, err := s.db.Exec(`UPDATE sources SET last_batch_id=?, last_success_at=?, total_count=total_count+1 WHERE id=?`, batchID, now(), id)
	return err
}

func (s *Store) SourceFailed(id int64) error {
	_, err := s.db.Exec(`UPDATE sources SET fail_count=fail_count+1, total_count=total_count+1 WHERE id=?`, id)
	return err
}

// ---------- tables ----------

func (s *Store) UpsertTable(t *Table) (int64, error) {
	kc, _ := json.Marshal(t.KeyCols)
	ac, _ := json.Marshal(t.AmountCols)
	en := 0
	if t.Enabled {
		en = 1
	}
	var id int64
	err := s.db.QueryRow(`INSERT INTO tables(source_id,name,enabled,key_mode,key_cols,soft_delete_col,amount_cols,buckets)
	  VALUES(?,?,?,?,?,?,?,?)
	  ON CONFLICT(source_id,name) DO UPDATE SET enabled=excluded.enabled
	  RETURNING id`,
		t.SourceID, t.Name, en, t.KeyMode, string(kc), t.SoftDeleteCol, string(ac), max(t.Buckets, 1)).Scan(&id)
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (s *Store) UpdateTableConfig(t *Table) error {
	kc, _ := json.Marshal(t.KeyCols)
	ac, _ := json.Marshal(t.AmountCols)
	en := 0
	if t.Enabled {
		en = 1
	}
	_, err := s.db.Exec(`UPDATE tables SET enabled=?, key_mode=?, key_cols=?, soft_delete_col=?, amount_cols=?, buckets=? WHERE id=?`,
		en, t.KeyMode, string(kc), t.SoftDeleteCol, string(ac), max(t.Buckets, 1), t.ID)
	return err
}

func (s *Store) SetTableSchemaSig(id int64, sig string) error {
	_, err := s.db.Exec(`UPDATE tables SET schema_sig=? WHERE id=?`, sig, id)
	return err
}

func (s *Store) ListTables(sourceID int64) ([]Table, error) {
	rows, err := s.db.Query(`SELECT id,source_id,name,enabled,key_mode,key_cols,soft_delete_col,amount_cols,buckets,schema_sig FROM tables WHERE source_id=? ORDER BY name`, sourceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Table
	for rows.Next() {
		var t Table
		var en int
		var kc, ac string
		if err := rows.Scan(&t.ID, &t.SourceID, &t.Name, &en, &t.KeyMode, &kc, &t.SoftDeleteCol, &ac, &t.Buckets, &t.SchemaSig); err != nil {
			return nil, err
		}
		t.Enabled = en != 0
		_ = json.Unmarshal([]byte(kc), &t.KeyCols)
		_ = json.Unmarshal([]byte(ac), &t.AmountCols)
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *Store) GetTable(id int64) (*Table, error) {
	var t Table
	var en int
	var kc, ac string
	err := s.db.QueryRow(`SELECT id,source_id,name,enabled,key_mode,key_cols,soft_delete_col,amount_cols,buckets,schema_sig FROM tables WHERE id=?`, id).
		Scan(&t.ID, &t.SourceID, &t.Name, &en, &t.KeyMode, &kc, &t.SoftDeleteCol, &ac, &t.Buckets, &t.SchemaSig)
	if err != nil {
		return nil, err
	}
	t.Enabled = en != 0
	_ = json.Unmarshal([]byte(kc), &t.KeyCols)
	_ = json.Unmarshal([]byte(ac), &t.AmountCols)
	return &t, nil
}

// ---------- batches ----------

func (s *Store) NewBatch(sourceID int64) (int64, error) {
	r, err := s.db.Exec(`INSERT INTO batches(source_id,status,started_at) VALUES(?,'running',?)`, sourceID, now())
	if err != nil {
		return 0, err
	}
	return r.LastInsertId()
}

func (s *Store) FinishBatch(id int64, status, errMsg string) error {
	_, err := s.db.Exec(`UPDATE batches SET status=?, finished_at=?, error=?,
	  inserts=(SELECT COALESCE(SUM(inserts),0) FROM batch_tables WHERE batch_id=?),
	  updates=(SELECT COALESCE(SUM(updates),0) FROM batch_tables WHERE batch_id=?),
	  deletes=(SELECT COALESCE(SUM(deletes),0) FROM batch_tables WHERE batch_id=?)
	  WHERE id=?`, status, now(), errMsg, id, id, id, id)
	return err
}

func (s *Store) ListBatches(sourceID int64, limit int) ([]Batch, error) {
	q := `SELECT id,source_id,status,started_at,finished_at,error,inserts,updates,deletes FROM batches`
	args := []any{}
	if sourceID > 0 {
		q += ` WHERE source_id=?`
		args = append(args, sourceID)
	}
	q += ` ORDER BY id DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Batch
	for rows.Next() {
		var b Batch
		if err := rows.Scan(&b.ID, &b.SourceID, &b.Status, &b.StartedAt, &b.FinishedAt, &b.Error, &b.Inserts, &b.Updates, &b.Deletes); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

func (s *Store) GetBatch(id int64) (*Batch, error) {
	var b Batch
	err := s.db.QueryRow(`SELECT id,source_id,status,started_at,finished_at,error,inserts,updates,deletes FROM batches WHERE id=?`, id).
		Scan(&b.ID, &b.SourceID, &b.Status, &b.StartedAt, &b.FinishedAt, &b.Error, &b.Inserts, &b.Updates, &b.Deletes)
	if err != nil {
		return nil, err
	}
	return &b, nil
}

func (s *Store) RunningBatches() ([]Batch, error) {
	rows, err := s.db.Query(`SELECT id,source_id,status,started_at,finished_at,error,inserts,updates,deletes FROM batches WHERE status='running' ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Batch
	for rows.Next() {
		var b Batch
		if err := rows.Scan(&b.ID, &b.SourceID, &b.Status, &b.StartedAt, &b.FinishedAt, &b.Error, &b.Inserts, &b.Updates, &b.Deletes); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// UnfinishedBatch returns the latest non-done batch for a source, if any.
func (s *Store) UnfinishedBatch(sourceID int64) (*Batch, error) {
	var b Batch
	err := s.db.QueryRow(`SELECT id,source_id,status,started_at,finished_at,error,inserts,updates,deletes
	  FROM batches WHERE source_id=? AND status IN ('running','failed') ORDER BY id DESC LIMIT 1`, sourceID).
		Scan(&b.ID, &b.SourceID, &b.Status, &b.StartedAt, &b.FinishedAt, &b.Error, &b.Inserts, &b.Updates, &b.Deletes)
	if err != nil {
		return nil, err
	}
	return &b, nil
}

// ReopenBatch puts a failed batch back into running state for resume.
func (s *Store) ReopenBatch(id int64) error {
	_, err := s.db.Exec(`UPDATE batches SET status='running', error='', finished_at='' WHERE id=?`, id)
	return err
}

// ---------- batch_tables ----------

func (s *Store) EnsureBatchTable(batchID, tableID int64) error {
	_, err := s.db.Exec(`INSERT OR IGNORE INTO batch_tables(batch_id,table_id,status) VALUES(?,?,'pending')`, batchID, tableID)
	return err
}

func (s *Store) SetBatchTableStatus(batchID, tableID int64, status, errMsg string) error {
	_, err := s.db.Exec(`UPDATE batch_tables SET status=?, error=? WHERE batch_id=? AND table_id=?`, status, errMsg, batchID, tableID)
	return err
}

func (s *Store) SetBatchTableStats(batchID, tableID int64, ins, upd, del int64) error {
	_, err := s.db.Exec(`UPDATE batch_tables SET inserts=?, updates=?, deletes=? WHERE batch_id=? AND table_id=?`, ins, upd, del, batchID, tableID)
	return err
}

func (s *Store) ListBatchTables(batchID int64) ([]BatchTable, error) {
	rows, err := s.db.Query(`SELECT batch_id,table_id,status,inserts,updates,deletes,error FROM batch_tables WHERE batch_id=? ORDER BY table_id`, batchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []BatchTable
	for rows.Next() {
		var bt BatchTable
		if err := rows.Scan(&bt.BatchID, &bt.TableID, &bt.Status, &bt.Inserts, &bt.Updates, &bt.Deletes, &bt.Error); err != nil {
			return nil, err
		}
		out = append(out, bt)
	}
	return out, rows.Err()
}

// ---------- chunk/key state ----------

type ChunkState struct {
	Bucket   int
	RowCount int64
	Hash     uint64
}

func (s *Store) LoadChunkStates(tableID int64) (map[int]ChunkState, error) {
	rows, err := s.db.Query(`SELECT bucket,row_count,h FROM chunk_state WHERE table_id=?`, tableID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int]ChunkState{}
	for rows.Next() {
		var c ChunkState
		var h int64
		if err := rows.Scan(&c.Bucket, &c.RowCount, &h); err != nil {
			return nil, err
		}
		c.Hash = uint64(h)
		out[c.Bucket] = c
	}
	return out, rows.Err()
}

func (s *Store) LoadBucketKeys(tableID int64, bucket int) (map[string]uint64, error) {
	rows, err := s.db.Query(`SELECT k,h FROM key_state WHERE table_id=? AND bucket=?`, tableID, bucket)
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

// BucketDiff is one bucket's new state plus the changes detected in it.
type BucketDiff struct {
	Bucket   int
	RowCount int64
	Hash     uint64
	Keys     map[string]uint64 // full new key->hash state of the bucket
	Changes  []Change
}

// PersistBucketDiffs atomically stores diff results for a table within a batch:
// change rows, new key_state/chunk_state for the touched buckets, batch status.
func (s *Store) PersistBucketDiffs(batchID, tableID int64, diffs []BucketDiff, ins, upd, del int64) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, d := range diffs {
		if _, err := tx.Exec(`DELETE FROM key_state WHERE table_id=? AND bucket=?`, tableID, d.Bucket); err != nil {
			return err
		}
		stmt, err := tx.Prepare(`INSERT INTO key_state(table_id,k,h,bucket) VALUES(?,?,?,?)`)
		if err != nil {
			return err
		}
		for k, h := range d.Keys {
			if _, err := stmt.Exec(tableID, k, int64(h), d.Bucket); err != nil {
				stmt.Close()
				return err
			}
		}
		stmt.Close()
		if _, err := tx.Exec(`INSERT INTO chunk_state(table_id,bucket,row_count,h) VALUES(?,?,?,?)
		  ON CONFLICT(table_id,bucket) DO UPDATE SET row_count=excluded.row_count, h=excluded.h`,
			tableID, d.Bucket, d.RowCount, int64(d.Hash)); err != nil {
			return err
		}
	}
	cstmt, err := tx.Prepare(`INSERT INTO changes(batch_id,table_id,op,k,row_json,h) VALUES(?,?,?,?,?,?)`)
	if err != nil {
		return err
	}
	for _, d := range diffs {
		for _, c := range d.Changes {
			if _, err := cstmt.Exec(batchID, tableID, c.Op, c.Key, c.RowJSON, int64(c.Hash)); err != nil {
				cstmt.Close()
				return err
			}
		}
	}
	cstmt.Close()
	if _, err := tx.Exec(`UPDATE batch_tables SET status='diffed', inserts=?, updates=?, deletes=? WHERE batch_id=? AND table_id=?`,
		ins, upd, del, batchID, tableID); err != nil {
		return err
	}
	return tx.Commit()
}

// Rebaseline replaces all key/chunk state for a table without emitting changes
// (used after a schema change alters the hash basis).
func (s *Store) Rebaseline(tableID int64, buckets []BucketDiff) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`DELETE FROM key_state WHERE table_id=?`, tableID); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM chunk_state WHERE table_id=?`, tableID); err != nil {
		return err
	}
	ks, err := tx.Prepare(`INSERT INTO key_state(table_id,k,h,bucket) VALUES(?,?,?,?)`)
	if err != nil {
		return err
	}
	for _, b := range buckets {
		for k, h := range b.Keys {
			if _, err := ks.Exec(tableID, k, int64(h), b.Bucket); err != nil {
				ks.Close()
				return err
			}
		}
		if _, err := tx.Exec(`INSERT INTO chunk_state(table_id,bucket,row_count,h) VALUES(?,?,?,?)`,
			tableID, b.Bucket, b.RowCount, int64(b.Hash)); err != nil {
			ks.Close()
			return err
		}
	}
	ks.Close()
	return tx.Commit()
}

func (s *Store) LoadChanges(batchID, tableID int64) ([]Change, error) {
	rows, err := s.db.Query(`SELECT id,batch_id,table_id,op,k,row_json,h FROM changes WHERE batch_id=? AND table_id=? ORDER BY id`, batchID, tableID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Change
	for rows.Next() {
		var c Change
		var h int64
		if err := rows.Scan(&c.ID, &c.BatchID, &c.TableID, &c.Op, &c.Key, &c.RowJSON, &h); err != nil {
			return nil, err
		}
		c.Hash = uint64(h)
		out = append(out, c)
	}
	return out, rows.Err()
}

// ListChangesPaged returns change rows for the console, newest first.
func (s *Store) ListChangesPaged(batchID, tableID int64, op string, limit, offset int) ([]Change, int64, error) {
	q := `FROM changes WHERE 1=1`
	args := []any{}
	if batchID > 0 {
		q += ` AND batch_id=?`
		args = append(args, batchID)
	}
	if tableID > 0 {
		q += ` AND table_id=?`
		args = append(args, tableID)
	}
	if op != "" {
		q += ` AND op=?`
		args = append(args, op)
	}
	var total int64
	if err := s.db.QueryRow(`SELECT COUNT(*) `+q, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	rows, err := s.db.Query(`SELECT id,batch_id,table_id,op,k,row_json,h `+q+` ORDER BY id DESC LIMIT ? OFFSET ?`,
		append(args, limit, offset)...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var out []Change
	for rows.Next() {
		var c Change
		var h int64
		if err := rows.Scan(&c.ID, &c.BatchID, &c.TableID, &c.Op, &c.Key, &c.RowJSON, &h); err != nil {
			return nil, 0, err
		}
		c.Hash = uint64(h)
		out = append(out, c)
	}
	return out, total, rows.Err()
}

// ---------- recon ----------

func (s *Store) SaveReconRun(r *ReconRun) (int64, error) {
	res, err := s.db.Exec(`INSERT INTO recon_runs(table_id,ran_at,status,src_count,tgt_count,sums_json,detail) VALUES(?,?,?,?,?,?,?)`,
		r.TableID, now(), r.Status, r.SrcCount, r.TgtCount, r.SumsJSON, r.Detail)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (s *Store) ListReconRuns(tableID int64, limit int) ([]ReconRun, error) {
	q := `SELECT id,table_id,ran_at,status,src_count,tgt_count,sums_json,detail FROM recon_runs`
	args := []any{}
	if tableID > 0 {
		q += ` WHERE table_id=?`
		args = append(args, tableID)
	}
	q += ` ORDER BY id DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ReconRun
	for rows.Next() {
		var r ReconRun
		if err := rows.Scan(&r.ID, &r.TableID, &r.RanAt, &r.Status, &r.SrcCount, &r.TgtCount, &r.SumsJSON, &r.Detail); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ResetTableState clears all sync state for a table (full re-sync on next run).
func (s *Store) ResetTableState(tableID int64) error {
	for _, q := range []string{
		`DELETE FROM key_state WHERE table_id=?`,
		`DELETE FROM chunk_state WHERE table_id=?`,
		`UPDATE tables SET schema_sig='' WHERE id=?`,
	} {
		if _, err := s.db.Exec(q, tableID); err != nil {
			return err
		}
	}
	return nil
}

// AdjustHashes shifts every stored row hash of a table by a constant delta
// (used when columns are ADDed: with additive per-column hashing every
// existing row's hash moves by the hash of the new column's default value).
func (s *Store) AdjustHashes(tableID int64, delta uint64) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	rows, err := tx.Query(`SELECT k,h FROM key_state WHERE table_id=?`, tableID)
	if err != nil {
		return err
	}
	type kv struct {
		k string
		h uint64
	}
	var keys []kv
	for rows.Next() {
		var x kv
		var h int64
		if err := rows.Scan(&x.k, &h); err != nil {
			rows.Close()
			return err
		}
		x.h = uint64(h) + delta
		keys = append(keys, x)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	stmt, err := tx.Prepare(`UPDATE key_state SET h=? WHERE table_id=? AND k=?`)
	if err != nil {
		return err
	}
	for _, x := range keys {
		if _, err := stmt.Exec(int64(x.h), tableID, x.k); err != nil {
			stmt.Close()
			return err
		}
	}
	stmt.Close()
	crows, err := tx.Query(`SELECT bucket,row_count,h FROM chunk_state WHERE table_id=?`, tableID)
	if err != nil {
		return err
	}
	type cs struct {
		bucket int
		h      uint64
	}
	var chunks []cs
	for crows.Next() {
		var c cs
		var h, rc int64
		if err := crows.Scan(&c.bucket, &rc, &h); err != nil {
			crows.Close()
			return err
		}
		c.h = uint64(h) + delta*uint64(rc)
		chunks = append(chunks, c)
	}
	crows.Close()
	cstmt, err := tx.Prepare(`UPDATE chunk_state SET h=? WHERE table_id=? AND bucket=?`)
	if err != nil {
		return err
	}
	for _, c := range chunks {
		if _, err := cstmt.Exec(int64(c.h), tableID, c.bucket); err != nil {
			cstmt.Close()
			return err
		}
	}
	cstmt.Close()
	return tx.Commit()
}
