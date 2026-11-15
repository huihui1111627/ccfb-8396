// Package web serves the admin console: source registration, table sync
// config, batch history, change details, reconciliation and the per-store
// latency/failure dashboard.
package web

import (
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"strings"
	"time"

	"pmssync/internal/recon"
	"pmssync/internal/sim"
	"pmssync/internal/source"
	"pmssync/internal/store"
	"pmssync/internal/syncer"
	"pmssync/internal/target"
)

//go:embed templates
var tplFS embed.FS

type Server struct {
	St       *store.Store
	Tg       *target.Target
	Sy       *syncer.Syncer
	TruthDir string // simulator truth dir (for /api/sim/verify)

	tpl *template.Template
}

func New(st *store.Store, tg *target.Target, sy *syncer.Syncer, truthDir string) (*Server, error) {
	funcs := template.FuncMap{
		"add": func(a, b int) int { return a + b },
		"sub": func(a, b int) int { return a - b },
		"opname": func(op string) string {
			switch op {
			case "I":
				return "新增"
			case "U":
				return "更新"
			case "D":
				return "物理删除"
			case "S":
				return "软删除"
			}
			return op
		},
		"statuscn": func(s string) string {
			switch s {
			case "done":
				return "完成"
			case "running":
				return "运行中"
			case "failed":
				return "失败"
			case "pending":
				return "待处理"
			case "diffed":
				return "已比对"
			case "ok":
				return "一致"
			case "mismatch":
				return "不一致"
			case "error":
				return "错误"
			}
			return s
		},
		"latency": func(s store.Source) string {
			if s.LastSuccessAt == "" {
				return "从未同步"
			}
			t, err := time.ParseInLocation("2006-01-02 15:04:05.000", s.LastSuccessAt, time.Local)
			if err != nil {
				return "-"
			}
			d := time.Since(t)
			if d < time.Minute {
				return fmt.Sprintf("%d秒前", int(d.Seconds()))
			}
			if d < time.Hour {
				return fmt.Sprintf("%d分钟前", int(d.Minutes()))
			}
			return fmt.Sprintf("%.1f小时前", d.Hours())
		},
		"failrate": func(s store.Source) string {
			if s.TotalCount == 0 {
				return "-"
			}
			return fmt.Sprintf("%.1f%% (%d/%d)", float64(s.FailCount)/float64(s.TotalCount)*100, s.FailCount, s.TotalCount)
		},
	}
	tpl, err := template.New("all").Funcs(funcs).ParseFS(tplFS, "templates/*.html")
	if err != nil {
		return nil, err
	}
	return &Server{St: st, Tg: tg, Sy: sy, TruthDir: truthDir, tpl: tpl}, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", s.dashboard)
	mux.HandleFunc("GET /sources", s.sources)
	mux.HandleFunc("POST /sources", s.addSource)
	mux.HandleFunc("POST /sources/{id}/delete", s.deleteSource)
	mux.HandleFunc("POST /sources/{id}/discover", s.discoverTables)
	mux.HandleFunc("POST /sources/{id}/sync", s.triggerSync)
	mux.HandleFunc("GET /tables", s.tables)
	mux.HandleFunc("POST /tables/{id}", s.updateTable)
	mux.HandleFunc("POST /tables/{id}/reset", s.resetTable)
	mux.HandleFunc("GET /batches", s.batches)
	mux.HandleFunc("GET /batches/{id}", s.batchDetail)
	mux.HandleFunc("GET /changes", s.changes)
	mux.HandleFunc("GET /recon", s.reconPage)
	mux.HandleFunc("POST /recon/run", s.runRecon)
	// JSON API (used by the simulator)
	mux.HandleFunc("POST /api/sources/{id}/sync", s.apiSync)
	mux.HandleFunc("POST /api/sim/verify", s.apiVerify)
	mux.HandleFunc("GET /api/status", s.apiStatus)
	return mux
}

func (s *Server) render(w http.ResponseWriter, name string, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.tpl.ExecuteTemplate(w, name+".html", data); err != nil {
		http.Error(w, err.Error(), 500)
	}
}

func (s *Server) dashboard(w http.ResponseWriter, r *http.Request) {
	sources, _ := s.St.ListSources()
	batches, _ := s.St.ListBatches(0, 10)
	s.render(w, "dashboard", map[string]any{"Sources": sources, "Batches": batches})
}

func (s *Server) sources(w http.ResponseWriter, r *http.Request) {
	list, _ := s.St.ListSources()
	s.render(w, "sources", map[string]any{"Sources": list, "Err": r.URL.Query().Get("err")})
}

func (s *Server) addSource(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimSpace(r.FormValue("name"))
	path := strings.TrimSpace(r.FormValue("path"))
	if name == "" || path == "" {
		http.Redirect(w, r, "/sources?err=名称和路径不能为空", 303)
		return
	}
	if _, err := source.Open(path); err != nil {
		http.Redirect(w, r, "/sources?err="+err.Error(), 303)
		return
	}
	id, err := s.St.AddSource(name, path)
	if err != nil {
		http.Redirect(w, r, "/sources?err="+err.Error(), 303)
		return
	}
	http.Redirect(w, r, fmt.Sprintf("/sources/%d/discover", id), 303)
}

func (s *Server) deleteSource(w http.ResponseWriter, r *http.Request) {
	id, _ := strconv.ParseInt(r.PathValue("id"), 10, 64)
	_ = s.St.DeleteSource(id)
	http.Redirect(w, r, "/sources", 303)
}

// discoverTables scans the source DB and creates default table configs.
func (s *Server) discoverTables(w http.ResponseWriter, r *http.Request) {
	id, _ := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if r.Method == "GET" {
		// allow redirect-target GET to just show the tables page
		http.Redirect(w, r, fmt.Sprintf("/tables?source_id=%d", id), 303)
		return
	}
	src, err := s.St.GetSource(id)
	if err != nil {
		http.Error(w, err.Error(), 404)
		return
	}
	db, err := source.Open(src.Path)
	if err != nil {
		http.Redirect(w, r, "/sources?err="+err.Error(), 303)
		return
	}
	defer db.Close()
	names, _ := db.ListTables()
	for _, n := range names {
		cols, _ := db.TableSchema(n)
		t := &store.Table{SourceID: id, Name: n, Enabled: true, KeyMode: "pk", KeyCols: source.PKCols(cols), Buckets: 64}
		if len(t.KeyCols) == 0 {
			t.KeyMode = "row" // no PK: default to full-row fingerprint, editable
		}
		_, _ = s.St.UpsertTable(t)
	}
	http.Redirect(w, r, fmt.Sprintf("/tables?source_id=%d", id), 303)
}

func (s *Server) triggerSync(w http.ResponseWriter, r *http.Request) {
	id, _ := strconv.ParseInt(r.PathValue("id"), 10, 64)
	go func() {
		if _, err := s.Sy.RunBatch(id); err != nil {
			fmt.Printf("sync source %d: %v\n", id, err)
		}
	}()
	http.Redirect(w, r, "/batches", 303)
}

func (s *Server) tables(w http.ResponseWriter, r *http.Request) {
	srcID, _ := strconv.ParseInt(r.URL.Query().Get("source_id"), 10, 64)
	src, _ := s.St.GetSource(srcID)
	list, _ := s.St.ListTables(srcID)
	s.render(w, "tables", map[string]any{"Source": src, "Tables": list})
}

func (s *Server) updateTable(w http.ResponseWriter, r *http.Request) {
	id, _ := strconv.ParseInt(r.PathValue("id"), 10, 64)
	t, err := s.St.GetTable(id)
	if err != nil {
		http.Error(w, err.Error(), 404)
		return
	}
	t.Enabled = r.FormValue("enabled") == "on"
	t.KeyMode = r.FormValue("key_mode")
	if kc := strings.TrimSpace(r.FormValue("key_cols")); kc != "" {
		t.KeyCols = strings.Split(kc, ",")
		for i := range t.KeyCols {
			t.KeyCols[i] = strings.TrimSpace(t.KeyCols[i])
		}
	} else {
		t.KeyCols = nil
	}
	t.SoftDeleteCol = strings.TrimSpace(r.FormValue("soft_delete_col"))
	if ac := strings.TrimSpace(r.FormValue("amount_cols")); ac != "" {
		t.AmountCols = strings.Split(ac, ",")
		for i := range t.AmountCols {
			t.AmountCols[i] = strings.TrimSpace(t.AmountCols[i])
		}
	} else {
		t.AmountCols = nil
	}
	if b, err := strconv.Atoi(r.FormValue("buckets")); err == nil && b > 0 {
		t.Buckets = b
	}
	_ = s.St.UpdateTableConfig(t)
	http.Redirect(w, r, fmt.Sprintf("/tables?source_id=%d", t.SourceID), 303)
}

func (s *Server) resetTable(w http.ResponseWriter, r *http.Request) {
	id, _ := strconv.ParseInt(r.PathValue("id"), 10, 64)
	t, _ := s.St.GetTable(id)
	_ = s.St.ResetTableState(id)
	http.Redirect(w, r, fmt.Sprintf("/tables?source_id=%d", t.SourceID), 303)
}

func (s *Server) batches(w http.ResponseWriter, r *http.Request) {
	srcID, _ := strconv.ParseInt(r.URL.Query().Get("source_id"), 10, 64)
	list, _ := s.St.ListBatches(srcID, 100)
	sources, _ := s.St.ListSources()
	names := map[int64]string{}
	for _, x := range sources {
		names[x.ID] = x.Name
	}
	s.render(w, "batches", map[string]any{"Batches": list, "Sources": sources, "Names": names, "SourceID": srcID})
}

func (s *Server) batchDetail(w http.ResponseWriter, r *http.Request) {
	id, _ := strconv.ParseInt(r.PathValue("id"), 10, 64)
	b, err := s.St.GetBatch(id)
	if err != nil {
		http.Error(w, err.Error(), 404)
		return
	}
	bts, _ := s.St.ListBatchTables(id)
	type row struct {
		store.BatchTable
		TableName string
	}
	var rows []row
	for _, bt := range bts {
		t, err := s.St.GetTable(bt.TableID)
		name := fmt.Sprintf("#%d", bt.TableID)
		if err == nil {
			name = t.Name
		}
		rows = append(rows, row{bt, name})
	}
	s.render(w, "batch_detail", map[string]any{"Batch": b, "Rows": rows})
}

func (s *Server) changes(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	batchID, _ := strconv.ParseInt(q.Get("batch_id"), 10, 64)
	tableID, _ := strconv.ParseInt(q.Get("table_id"), 10, 64)
	op := q.Get("op")
	page, _ := strconv.Atoi(q.Get("page"))
	if page < 1 {
		page = 1
	}
	const size = 200
	list, total, _ := s.St.ListChangesPaged(batchID, tableID, op, size, (page-1)*size)
	s.render(w, "changes", map[string]any{
		"Changes": list, "Total": total, "Page": page, "Size": size,
		"BatchID": batchID, "TableID": tableID, "Op": op,
		"Pages": (total + size - 1) / size,
	})
}

func (s *Server) reconPage(w http.ResponseWriter, r *http.Request) {
	tableID, _ := strconv.ParseInt(r.URL.Query().Get("table_id"), 10, 64)
	runs, _ := s.St.ListReconRuns(tableID, 50)
	sources, _ := s.St.ListSources()
	type tblOpt struct {
		ID       int64
		Label    string
		Selected bool
	}
	var opts []tblOpt
	for _, src := range sources {
		tables, _ := s.St.ListTables(src.ID)
		for _, t := range tables {
			opts = append(opts, tblOpt{t.ID, fmt.Sprintf("%s / %s", src.Name, t.Name), t.ID == tableID})
		}
	}
	s.render(w, "recon", map[string]any{"Runs": runs, "Tables": opts, "TableID": tableID})
}

func (s *Server) runRecon(w http.ResponseWriter, r *http.Request) {
	tableID, _ := strconv.ParseInt(r.FormValue("table_id"), 10, 64)
	t, err := s.St.GetTable(tableID)
	if err != nil {
		http.Error(w, err.Error(), 404)
		return
	}
	if _, err := recon.Run(s.St, s.Tg, t); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	http.Redirect(w, r, fmt.Sprintf("/recon?table_id=%d", tableID), 303)
}

// ---------- JSON API ----------

func (s *Server) apiSync(w http.ResponseWriter, r *http.Request) {
	id, _ := strconv.ParseInt(r.PathValue("id"), 10, 64)
	batchID, err := s.Sy.RunBatch(id)
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		json.NewEncoder(w).Encode(map[string]any{"ok": false, "batch_id": batchID, "error": err.Error()})
		return
	}
	json.NewEncoder(w).Encode(map[string]any{"ok": true, "batch_id": batchID})
}

func (s *Server) apiVerify(w http.ResponseWriter, r *http.Request) {
	rep, err := sim.Verify(s.St, s.Tg, s.TruthDir)
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		json.NewEncoder(w).Encode(map[string]any{"ok": false, "error": err.Error()})
		return
	}
	json.NewEncoder(w).Encode(rep)
}

func (s *Server) apiStatus(w http.ResponseWriter, r *http.Request) {
	sources, _ := s.St.ListSources()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(sources)
}
