// Package sim builds demo hotel-store SQLite databases, mutates them round
// by round, records the ground truth of every change, and verifies that the
// DuckDB zipper tables reproduce that truth exactly.
package sim

import (
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"

	"pmssync/internal/diff"
	"pmssync/internal/rowhash"
	"pmssync/internal/source"
	"pmssync/internal/store"
)

// Table layouts of the fake PMS.
var (
	OrdersCols = []string{"id", "room_no", "guest_name", "checkin_date", "nights", "amount", "status", "is_deleted", "created_at"}
)

type Spec struct {
	Dir            string
	Stores         int
	OrdersPerStore int
	MetaPath       string // path to the sync system's meta.db
}

func storePath(dir string, i int) string { return filepath.Join(dir, fmt.Sprintf("store_%d.db", i)) }
func truthPath(dir string) string        { return filepath.Join(dir, "truth.db") }

// Init creates the store databases, loads initial data, registers sources and
// table configs in the meta store, and records the initial load in truth.db.
func Init(sp Spec) error {
	if err := os.MkdirAll(sp.Dir, 0o755); err != nil {
		return err
	}
	st, err := store.Open(sp.MetaPath)
	if err != nil {
		return err
	}
	defer st.Close()

	tr, err := openTruth(sp.Dir)
	if err != nil {
		return err
	}
	defer tr.Close()

	rng := rand.New(rand.NewSource(42))
	for i := 1; i <= sp.Stores; i++ {
		p := storePath(sp.Dir, i)
		os.Remove(p)
		db, err := sql.Open("sqlite", "file:"+p)
		if err != nil {
			return err
		}
		if err := createSchema(db); err != nil {
			db.Close()
			return err
		}
		if err := loadInitial(db, i, sp.OrdersPerStore, rng); err != nil {
			db.Close()
			return err
		}
		db.Close()

		srcID, err := st.AddSource(fmt.Sprintf("门店-%03d", i), p)
		if err != nil {
			return err
		}
		cfgs := []store.Table{
			{SourceID: srcID, Name: "orders", Enabled: true, KeyMode: "pk", KeyCols: []string{"id"}, SoftDeleteCol: "is_deleted", AmountCols: []string{"amount"}, Buckets: 64},
			{SourceID: srcID, Name: "members", Enabled: true, KeyMode: "pk", KeyCols: []string{"id"}, SoftDeleteCol: "deleted_at", Buckets: 32},
			{SourceID: srcID, Name: "room_status", Enabled: true, KeyMode: "pk", KeyCols: []string{"room_no"}, Buckets: 16},
			{SourceID: srcID, Name: "order_logs", Enabled: true, KeyMode: "business", KeyCols: []string{"log_id"}, Buckets: 16},
			{SourceID: srcID, Name: "rate_plans", Enabled: true, KeyMode: "row", Buckets: 4},
		}
		for j := range cfgs {
			if _, err := st.UpsertTable(&cfgs[j]); err != nil {
				return err
			}
		}
		// record initial load as truth (round 0, op I)
		if err := recordInitialTruth(tr, srcID, p, cfgs); err != nil {
			return err
		}
	}
	return nil
}

func createSchema(db *sql.DB) error {
	_, err := db.Exec(`
CREATE TABLE orders(
  id INTEGER PRIMARY KEY,
  room_no TEXT, guest_name TEXT, checkin_date TEXT,
  nights INTEGER, amount REAL, status TEXT, is_deleted INTEGER, created_at TEXT
);
CREATE TABLE members(
  id INTEGER PRIMARY KEY,
  name TEXT, phone TEXT, level INTEGER, points INTEGER, deleted_at TEXT
);
CREATE TABLE room_status(
  room_no TEXT PRIMARY KEY, status TEXT, clean_status TEXT, updated_by TEXT
);
CREATE TABLE order_logs(
  log_id TEXT, order_id INTEGER, action TEXT, detail TEXT
);
CREATE TABLE rate_plans(
  code TEXT, season TEXT, price REAL
);`)
	return err
}

func loadInitial(db *sql.DB, storeIdx, nOrders int, rng *rand.Rand) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ost, err := tx.Prepare(`INSERT INTO orders VALUES(?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		return err
	}
	for i := 1; i <= nOrders; i++ {
		_, err := ost.Exec(i, fmt.Sprintf("%d%03d", rng.Intn(30)+1, rng.Intn(200)+1),
			fmt.Sprintf("客人%06d", rng.Intn(999999)),
			base.Add(time.Duration(rng.Intn(300)*24)*time.Hour).Format("2006-01-02"),
			rng.Intn(7)+1, float64(rng.Intn(200000))/100.0+100,
			[]string{"booked", "checked_in", "checked_out", "cancelled"}[rng.Intn(4)],
			0, base.Format("2006-01-02 15:04:05"))
		if err != nil {
			return err
		}
	}
	ost.Close()
	mst, _ := tx.Prepare(`INSERT INTO members VALUES(?,?,?,?,?,?)`)
	for i := 1; i <= 5000; i++ {
		if _, err := mst.Exec(i, fmt.Sprintf("会员%05d", i), fmt.Sprintf("138%08d", rng.Intn(100000000)),
			rng.Intn(5), rng.Intn(10000), ""); err != nil {
			return err
		}
	}
	mst.Close()
	rst, _ := tx.Prepare(`INSERT INTO room_status VALUES(?,?,?,?)`)
	for f := 1; f <= 5; f++ {
		for r := 1; r <= 40; r++ {
			if _, err := rst.Exec(fmt.Sprintf("%d%02d", f, r),
				[]string{"vacant", "occupied", "maintenance"}[rng.Intn(3)],
				[]string{"clean", "dirty"}[rng.Intn(2)], "system"); err != nil {
				return err
			}
		}
	}
	rst.Close()
	lst, _ := tx.Prepare(`INSERT INTO order_logs VALUES(?,?,?,?)`)
	for i := 1; i <= 2000; i++ {
		if _, err := lst.Exec(fmt.Sprintf("L%08d", i), rng.Intn(nOrders)+1, "create", "初始导入"); err != nil {
			return err
		}
	}
	lst.Close()
	pst, _ := tx.Prepare(`INSERT INTO rate_plans VALUES(?,?,?)`)
	for _, c := range []string{"BAR", "AAA", "CORP", "PKG", "MEM"} {
		for _, s := range []string{"peak", "off"} {
			if _, err := pst.Exec(c, s, float64(rng.Intn(100000))/100.0+200); err != nil {
				return err
			}
		}
	}
	pst.Close()
	return tx.Commit()
}

func openTruth(dir string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", "file:"+truthPath(dir)+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(10000)")
	if err != nil {
		return nil, err
	}
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS truth(
	  id INTEGER PRIMARY KEY AUTOINCREMENT,
	  round INTEGER, source_id INTEGER, tbl TEXT, k TEXT, op TEXT, h INTEGER
	);
	CREATE INDEX IF NOT EXISTS idx_truth_key ON truth(source_id, tbl, k);`)
	if err != nil {
		return nil, err
	}
	return db, nil
}

// recordInitialTruth scans each table and writes op='I' truth rows.
func recordInitialTruth(tr *sql.DB, srcID int64, path string, cfgs []store.Table) error {
	sdb, err := source.Open(path)
	if err != nil {
		return err
	}
	defer sdb.Close()
	tx, err := tr.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.Prepare(`INSERT INTO truth(round,source_id,tbl,k,op,h) VALUES(0,?,?,?,?,?)`)
	if err != nil {
		return err
	}
	for i := range cfgs {
		t := &cfgs[i]
		err := sdb.Scan(t.Name, func(cols []string, vals []any) error {
			k := diff.KeyOf(t, cols, vals)
			h := rowhash.HashRowCols(cols, vals)
			_, err := stmt.Exec(srcID, t.Name, k, "I", int64(h))
			return err
		})
		if err != nil {
			stmt.Close()
			return err
		}
	}
	stmt.Close()
	return tx.Commit()
}

// truthRecorder buffers truth inserts for one round.
type truthRecorder struct {
	tx    *sql.Tx
	stmt  *sql.Stmt
	round int
	srcID int64
}

func newTruthRecorder(tr *sql.DB, round int, srcID int64) (*truthRecorder, error) {
	tx, err := tr.Begin()
	if err != nil {
		return nil, err
	}
	st, err := tx.Prepare(`INSERT INTO truth(round,source_id,tbl,k,op,h) VALUES(?,?,?,?,?,?)`)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	return &truthRecorder{tx: tx, stmt: st, round: round, srcID: srcID}, nil
}

func (r *truthRecorder) add(tbl, k, op string, h uint64) error {
	_, err := r.stmt.Exec(r.round, r.srcID, tbl, k, op, int64(h))
	return err
}

func (r *truthRecorder) commit() error {
	if err := r.stmt.Close(); err != nil {
		return err
	}
	return r.tx.Commit()
}

// hashRowByKey reads a row back from the source DB and computes key + hash
// exactly as the sync engine will see them.
func hashRowByKey(db *sql.DB, t *store.Table, table string, where string, args ...any) (string, uint64, error) {
	rows, err := db.Query(fmt.Sprintf(`SELECT * FROM %q WHERE %s`, table, where), args...)
	if err != nil {
		return "", 0, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return "", 0, err
	}
	if !rows.Next() {
		return "", 0, fmt.Errorf("row not found in %s", table)
	}
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return "", 0, err
	}
	return diff.KeyOf(t, cols, vals), rowhash.HashRowCols(cols, vals), nil
}
