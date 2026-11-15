package sim

import (
	"database/sql"
	"fmt"
	"math/rand"
	"time"

	_ "modernc.org/sqlite"

	"pmssync/internal/store"
)

// Mutate applies one round of random changes to every registered store DB and
// records the ground truth. Key sets for different op types are disjoint
// within a round, so each key has at most one truth entry per round (the net
// effect a snapshot diff can observe).
func Mutate(sp Spec, round int) error {
	st, err := store.Open(sp.MetaPath)
	if err != nil {
		return err
	}
	defer st.Close()
	sources, err := st.ListSources()
	if err != nil {
		return err
	}
	tr, err := openTruth(sp.Dir)
	if err != nil {
		return err
	}
	defer tr.Close()

	for _, src := range sources {
		tables, err := st.ListTables(src.ID)
		if err != nil {
			return err
		}
		byName := map[string]*store.Table{}
		for i := range tables {
			byName[tables[i].Name] = &tables[i]
		}
		db, err := sql.Open("sqlite", "file:"+src.Path+"?_pragma=busy_timeout(10000)")
		if err != nil {
			return err
		}
		rng := rand.New(rand.NewSource(int64(round)*10007 + src.ID))

		// 1. schema evolution first, so every truth hash below uses the new schema
		if err := evolveSchema(db, round); err != nil {
			db.Close()
			return fmt.Errorf("source %d evolve: %w", src.ID, err)
		}

		rec, err := newTruthRecorder(tr, round, src.ID)
		if err != nil {
			db.Close()
			return err
		}
		if err := mutateOrders(db, byName["orders"], rng, rec); err != nil {
			db.Close()
			return err
		}
		if err := mutateMembers(db, byName["members"], rng, rec); err != nil {
			db.Close()
			return err
		}
		if err := mutateRooms(db, byName["room_status"], rng, rec); err != nil {
			db.Close()
			return err
		}
		if err := mutateLogs(db, byName["order_logs"], rng, rec); err != nil {
			db.Close()
			return err
		}
		if err := mutatePlans(db, byName["rate_plans"], rng, rec); err != nil {
			db.Close()
			return err
		}
		if err := rec.commit(); err != nil {
			db.Close()
			return err
		}
		db.Close()
	}
	return nil
}

// evolveSchema simulates source-side DDL: every 5th round adds a column to
// orders; every 7th changes a scratch column's type (drop + re-add, the only
// way to change a declared type in SQLite).
func evolveSchema(db *sql.DB, round int) error {
	has := func(table, col string) bool {
		var n int
		_ = db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info(?) WHERE name=?`, table, col).Scan(&n)
		return n > 0
	}
	if round%5 == 0 && !has("orders", "channel") {
		if _, err := db.Exec(`ALTER TABLE orders ADD COLUMN channel TEXT DEFAULT ''`); err != nil {
			return err
		}
	}
	if round%7 == 0 {
		if !has("orders", "score") {
			if _, err := db.Exec(`ALTER TABLE orders ADD COLUMN score INTEGER`); err != nil {
				return err
			}
		} else {
			var decl string
			_ = db.QueryRow(`SELECT type FROM pragma_table_info('orders') WHERE name='score'`).Scan(&decl)
			if decl == "INTEGER" {
				if _, err := db.Exec(`ALTER TABLE orders DROP COLUMN score`); err != nil {
					return err
				}
				if _, err := db.Exec(`ALTER TABLE orders ADD COLUMN score REAL`); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// sampleLiveIDs returns n distinct random ids from a query.
func sampleLiveIDs(db *sql.DB, query string, n int, rng *rand.Rand) ([]int64, error) {
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if len(ids) == 0 {
		return nil, nil
	}
	rng.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
	if n > len(ids) {
		n = len(ids)
	}
	return ids[:n], nil
}

func mutateOrders(db *sql.DB, cfg *store.Table, rng *rand.Rand, rec *truthRecorder) error {
	var n int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM orders`).Scan(&n); err != nil {
		return err
	}
	if n == 0 {
		return nil
	}
	var maxID int64
	if err := db.QueryRow(`SELECT COALESCE(MAX(id),0) FROM orders`).Scan(&maxID); err != nil {
		return err
	}
	now := time.Now().Format("2006-01-02 15:04:05")

	// inserts
	nIns := max(1, int(n)*4/1000)
	for j := 0; j < nIns; j++ {
		id := maxID + int64(j) + 1
		if _, err := db.Exec(`INSERT INTO orders(id,room_no,guest_name,checkin_date,nights,amount,status,is_deleted,created_at)
		  VALUES(?,?,?,?,?,?,?,0,?)`,
			id, fmt.Sprintf("%d%03d", rng.Intn(5)+1, rng.Intn(40)+1), fmt.Sprintf("新客%06d", rng.Intn(999999)),
			"2026-11-15", rng.Intn(5)+1, float64(rng.Intn(150000))/100.0+120,
			"booked", now); err != nil {
			return err
		}
		k, h, err := hashRowByKey(db, cfg, "orders", "id=?", id)
		if err != nil {
			return err
		}
		if err := rec.add("orders", k, "I", h); err != nil {
			return err
		}
	}

	// disjoint live-id segments for update / soft-delete / physical-delete / delete+reinsert
	// (exclude this round's inserts so each key gets at most one truth entry)
	need := int(n)*5/1000 + int(n)*2/1000 + int(n)*1/1000 + max(1, int(n)*5/10000)
	ids, err := sampleLiveIDs(db, fmt.Sprintf(`SELECT id FROM orders WHERE is_deleted=0 AND id<=%d`, maxID), need, rng)
	if err != nil {
		return err
	}
	i := 0
	take := func(m int) []int64 {
		if i+m > len(ids) {
			m = len(ids) - i
		}
		seg := ids[i : i+m]
		i += m
		return seg
	}
	statuses := []string{"booked", "checked_in", "checked_out", "cancelled"}

	for _, id := range take(int(n) * 5 / 1000) { // updates
		_, oldH, err := hashRowByKey(db, cfg, "orders", "id=?", id)
		if err != nil {
			return err
		}
		if _, err := db.Exec(`UPDATE orders SET amount=?, status=?, nights=? WHERE id=?`,
			float64(rng.Intn(200000))/100.0+100, statuses[rng.Intn(4)], rng.Intn(7)+1, id); err != nil {
			return err
		}
		k, h, err := hashRowByKey(db, cfg, "orders", "id=?", id)
		if err != nil {
			return err
		}
		if h == oldH {
			continue // no-op update: invisible to a snapshot diff
		}
		if err := rec.add("orders", k, "U", h); err != nil {
			return err
		}
	}
	for _, id := range take(int(n) * 2 / 1000) { // soft deletes
		if _, err := db.Exec(`UPDATE orders SET is_deleted=1 WHERE id=?`, id); err != nil {
			return err
		}
		k, h, err := hashRowByKey(db, cfg, "orders", "id=?", id)
		if err != nil {
			return err
		}
		if err := rec.add("orders", k, "S", h); err != nil {
			return err
		}
	}
	for _, id := range take(int(n) * 1 / 1000) { // physical deletes
		k, _, err := hashRowByKey(db, cfg, "orders", "id=?", id)
		if err != nil {
			return err
		}
		if _, err := db.Exec(`DELETE FROM orders WHERE id=?`, id); err != nil {
			return err
		}
		if err := rec.add("orders", k, "D", 0); err != nil {
			return err
		}
	}
	for _, id := range take(max(1, int(n)*5/10000)) { // delete + reinsert same PK => update
		if _, err := db.Exec(`DELETE FROM orders WHERE id=?`, id); err != nil {
			return err
		}
		if _, err := db.Exec(`INSERT INTO orders(id,room_no,guest_name,checkin_date,nights,amount,status,is_deleted,created_at)
		  VALUES(?,?,?,?,?,?,?,0,?)`,
			id, fmt.Sprintf("%d%03d", rng.Intn(5)+1, rng.Intn(40)+1), fmt.Sprintf("重录%06d", rng.Intn(999999)),
			"2026-11-16", rng.Intn(3)+1, float64(rng.Intn(90000))/100.0+200, "booked", now); err != nil {
			return err
		}
		k, h, err := hashRowByKey(db, cfg, "orders", "id=?", id)
		if err != nil {
			return err
		}
		if err := rec.add("orders", k, "U", h); err != nil {
			return err
		}
	}
	return nil
}

func mutateMembers(db *sql.DB, cfg *store.Table, rng *rand.Rand, rec *truthRecorder) error {
	var n int64
	if err := db.QueryRow(`SELECT COUNT(*) FROM members`).Scan(&n); err != nil || n == 0 {
		return err
	}
	ids, err := sampleLiveIDs(db, `SELECT id FROM members WHERE deleted_at=''`, int(n)*12/1000+2, rng)
	if err != nil {
		return err
	}
	cut := len(ids) * 10 / 12
	now := time.Now().Format("2006-01-02 15:04:05")
	for _, id := range ids[:cut] { // points/level updates
		_, oldH, err := hashRowByKey(db, cfg, "members", "id=?", id)
		if err != nil {
			return err
		}
		if _, err := db.Exec(`UPDATE members SET points=points+?, level=? WHERE id=?`, rng.Intn(500), rng.Intn(5), id); err != nil {
			return err
		}
		k, h, err := hashRowByKey(db, cfg, "members", "id=?", id)
		if err != nil {
			return err
		}
		if h == oldH {
			continue
		}
		if err := rec.add("members", k, "U", h); err != nil {
			return err
		}
	}
	for _, id := range ids[cut:] { // soft delete via deleted_at
		if _, err := db.Exec(`UPDATE members SET deleted_at=? WHERE id=?`, now, id); err != nil {
			return err
		}
		k, h, err := hashRowByKey(db, cfg, "members", "id=?", id)
		if err != nil {
			return err
		}
		if err := rec.add("members", k, "S", h); err != nil {
			return err
		}
	}
	return nil
}

func mutateRooms(db *sql.DB, cfg *store.Table, rng *rand.Rand, rec *truthRecorder) error {
	rows, err := db.Query(`SELECT room_no FROM room_status`)
	if err != nil {
		return err
	}
	var rooms []string
	for rows.Next() {
		var r string
		if err := rows.Scan(&r); err != nil {
			rows.Close()
			return err
		}
		rooms = append(rooms, r)
	}
	rows.Close()
	rng.Shuffle(len(rooms), func(i, j int) { rooms[i], rooms[j] = rooms[j], rooms[i] })
	statuses := []string{"vacant", "occupied", "maintenance"}
	for _, r := range rooms[:max(1, len(rooms)/20)] {
		_, oldH, err := hashRowByKey(db, cfg, "room_status", "room_no=?", r)
		if err != nil {
			return err
		}
		if _, err := db.Exec(`UPDATE room_status SET status=?, clean_status=?, updated_by='pms' WHERE room_no=?`,
			statuses[rng.Intn(3)], []string{"clean", "dirty"}[rng.Intn(2)], r); err != nil {
			return err
		}
		k, h, err := hashRowByKey(db, cfg, "room_status", "room_no=?", r)
		if err != nil {
			return err
		}
		if h == oldH {
			continue
		}
		if err := rec.add("room_status", k, "U", h); err != nil {
			return err
		}
	}
	return nil
}

func mutateLogs(db *sql.DB, cfg *store.Table, rng *rand.Rand, rec *truthRecorder) error {
	var maxNum int64
	if err := db.QueryRow(`SELECT COALESCE(MAX(CAST(SUBSTR(log_id,2) AS INTEGER)),0) FROM order_logs`).Scan(&maxNum); err != nil {
		return err
	}
	for j := 0; j < 20; j++ { // inserts
		lid := fmt.Sprintf("L%08d", maxNum+int64(j)+1)
		if _, err := db.Exec(`INSERT INTO order_logs VALUES(?,?,?,?)`, lid, rng.Intn(1000)+1, "modify", "模拟变更"); err != nil {
			return err
		}
		k, h, err := hashRowByKey(db, cfg, "order_logs", "log_id=?", lid)
		if err != nil {
			return err
		}
		if err := rec.add("order_logs", k, "I", h); err != nil {
			return err
		}
	}
	rows, err := db.Query(`SELECT log_id FROM order_logs WHERE CAST(SUBSTR(log_id,2) AS INTEGER) <= ? ORDER BY RANDOM() LIMIT 5`, maxNum)
	if err != nil {
		return err
	}
	var lids []string
	for rows.Next() {
		var l string
		if err := rows.Scan(&l); err != nil {
			rows.Close()
			return err
		}
		lids = append(lids, l)
	}
	rows.Close()
	for _, lid := range lids { // updates by business key
		_, oldH, err := hashRowByKey(db, cfg, "order_logs", "log_id=?", lid)
		if err != nil {
			return err
		}
		if _, err := db.Exec(`UPDATE order_logs SET action=?, detail=? WHERE log_id=?`, "adjust", fmt.Sprintf("调整%d", rng.Intn(100)), lid); err != nil {
			return err
		}
		k, h, err := hashRowByKey(db, cfg, "order_logs", "log_id=?", lid)
		if err != nil {
			return err
		}
		if h == oldH {
			continue
		}
		if err := rec.add("order_logs", k, "U", h); err != nil {
			return err
		}
	}
	return nil
}

func mutatePlans(db *sql.DB, cfg *store.Table, rng *rand.Rand, rec *truthRecorder) error {
	// row-fingerprint mode: changing a price = delete old fingerprint + insert new one
	rows, err := db.Query(`SELECT rowid, code, season FROM rate_plans`)
	if err != nil {
		return err
	}
	type plan struct {
		rid          int64
		code, season string
	}
	var plans []plan
	for rows.Next() {
		var p plan
		if err := rows.Scan(&p.rid, &p.code, &p.season); err != nil {
			rows.Close()
			return err
		}
		plans = append(plans, p)
	}
	rows.Close()
	if len(plans) == 0 {
		return nil
	}
	p := plans[rng.Intn(len(plans))]
	oldKey, _, err := hashRowByKey(db, cfg, "rate_plans", "rowid=?", p.rid)
	if err != nil {
		return err
	}
	if _, err := db.Exec(`UPDATE rate_plans SET price=? WHERE rowid=?`, float64(rng.Intn(100000))/100.0+200, p.rid); err != nil {
		return err
	}
	newKey, newH, err := hashRowByKey(db, cfg, "rate_plans", "rowid=?", p.rid)
	if err != nil {
		return err
	}
	if newKey != oldKey { // price actually changed (fingerprint moved)
		if err := rec.add("rate_plans", oldKey, "D", 0); err != nil {
			return err
		}
		if err := rec.add("rate_plans", newKey, "I", newH); err != nil {
			return err
		}
	}
	if rng.Intn(3) == 0 { // occasionally a brand-new plan (unique code per round)
		code := fmt.Sprintf("P%04d", rec.round*100+int(rec.srcID%100))
		if _, err := db.Exec(`INSERT INTO rate_plans VALUES(?,?,?)`, code, "peak", float64(rng.Intn(80000))/100.0+300); err != nil {
			return err
		}
		k, h, err := hashRowByKey(db, cfg, "rate_plans", "code=? AND season=?", code, "peak")
		if err != nil {
			return err
		}
		if err := rec.add("rate_plans", k, "I", h); err != nil {
			return err
		}
	}
	return nil
}
