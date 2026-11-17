// Package console 提供管理控制台的 REST API 与内嵌 Web 界面。
package console

import (
	"embed"
	"encoding/json"
	"io/fs"
	"net/http"

	"erasurestore/internal/disk"
	"erasurestore/internal/lifecycle"
	"erasurestore/internal/meta"
	"erasurestore/internal/repair"
	"erasurestore/internal/storage"
)

//go:embed static/*
var staticFS embed.FS

// Server 控制台服务。
type Server struct {
	store  *storage.Store
	repair *repair.Manager
	life   *lifecycle.Runner
	token  string
}

// New 创建控制台。
func New(st *storage.Store, rm *repair.Manager, lr *lifecycle.Runner, token string) *Server {
	return &Server{store: st, repair: rm, life: lr, token: token}
}

// Handler 返回路由。
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /api/health", s.health)
	mux.HandleFunc("GET /api/disks", s.auth(s.listDisks))
	mux.HandleFunc("POST /api/disks/{id}/offline", s.auth(s.diskOffline))
	mux.HandleFunc("POST /api/disks/{id}/online", s.auth(s.diskOnline))
	mux.HandleFunc("POST /api/disks/probe", s.auth(s.probeDisks))

	mux.HandleFunc("GET /api/buckets", s.auth(s.listBuckets))
	mux.HandleFunc("GET /api/buckets/{bucket}/objects", s.auth(s.listObjects))
	mux.HandleFunc("GET /api/objects", s.auth(s.getObject))
	mux.HandleFunc("DELETE /api/objects", s.auth(s.deleteObject))
	mux.HandleFunc("GET /api/objects/scan", s.auth(s.scanObject))

	mux.HandleFunc("GET /api/repair/tasks", s.auth(s.listRepairTasks))
	mux.HandleFunc("POST /api/repair/scan", s.auth(s.triggerRepair))
	mux.HandleFunc("POST /api/repair/objects", s.auth(s.manualRepair))
	mux.HandleFunc("POST /api/repair/tasks/{id}/pause", s.auth(s.pauseTask))
	mux.HandleFunc("POST /api/repair/tasks/{id}/resume", s.auth(s.resumeTask))
	mux.HandleFunc("POST /api/repair/tasks/{id}/cancel", s.auth(s.cancelTask))
	mux.HandleFunc("GET /api/repair/rate", s.auth(s.getRate))
	mux.HandleFunc("PUT /api/repair/rate", s.auth(s.setRate))

	mux.HandleFunc("GET /api/lifecycle/rules", s.auth(s.listRules))
	mux.HandleFunc("PUT /api/lifecycle/rules/{bucket}", s.auth(s.putRule))
	mux.HandleFunc("DELETE /api/lifecycle/rules/{bucket}", s.auth(s.deleteRule))
	mux.HandleFunc("POST /api/lifecycle/run", s.auth(s.runLifecycle))
	mux.HandleFunc("GET /api/lifecycle/events", s.auth(s.listEvents))

	// 静态界面
	sub, _ := fs.Sub(staticFS, "static")
	mux.Handle("GET /", http.FileServer(http.FS(sub)))
	return mux
}

func (s *Server) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token := r.Header.Get("X-Console-Token")
		if token == "" {
			if c, err := r.Cookie("console_token"); err == nil {
				token = c.Value
			}
		}
		if token != s.token {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "控制台 token 无效"})
			return
		}
		next(w, r)
	}
}

func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) listDisks(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.store.DiskManager().ManagerStats())
}

func (s *Server) diskOffline(w http.ResponseWriter, r *http.Request) {
	d, ok := s.store.DiskManager().Get(r.PathValue("id"))
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "磁盘不存在"})
		return
	}
	d.SetStatus(disk.StatusOffline)
	s.repair.Trigger()
	writeJSON(w, http.StatusOK, map[string]string{"id": d.ID(), "status": string(d.Status())})
}

func (s *Server) diskOnline(w http.ResponseWriter, r *http.Request) {
	d, ok := s.store.DiskManager().Get(r.PathValue("id"))
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "磁盘不存在"})
		return
	}
	d.SetStatus(disk.StatusOnline)
	s.repair.Trigger()
	writeJSON(w, http.StatusOK, map[string]string{"id": d.ID(), "status": string(d.Status())})
}

func (s *Server) probeDisks(w http.ResponseWriter, _ *http.Request) {
	for _, d := range s.store.DiskManager().All() {
		d.Probe()
	}
	s.repair.Trigger()
	writeJSON(w, http.StatusOK, s.store.DiskManager().ManagerStats())
}

func (s *Server) listBuckets(w http.ResponseWriter, _ *http.Request) {
	buckets, err := s.store.Meta().ListBuckets()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errMap(err))
		return
	}
	writeJSON(w, http.StatusOK, buckets)
}

func (s *Server) listObjects(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	q := r.URL.Query()
	res, err := s.store.Meta().ListObjects(
		bucket, q.Get("prefix"), q.Get("delimiter"), q.Get("token"), atoiDefault(q.Get("max_keys"), 200))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errMap(err))
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) getObject(w http.ResponseWriter, r *http.Request) {
	bucket, key := r.URL.Query().Get("bucket"), r.URL.Query().Get("key")
	m, err := s.store.Meta().GetObjectMeta(bucket, key)
	if err != nil {
		writeJSON(w, http.StatusNotFound, errMap(err))
		return
	}
	writeJSON(w, http.StatusOK, m)
}

func (s *Server) deleteObject(w http.ResponseWriter, r *http.Request) {
	bucket, key := r.URL.Query().Get("bucket"), r.URL.Query().Get("key")
	if err := s.store.DeleteObject(bucket, key); err != nil {
		writeJSON(w, http.StatusInternalServerError, errMap(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"deleted": key})
}

func (s *Server) scanObject(w http.ResponseWriter, r *http.Request) {
	bucket, key := r.URL.Query().Get("bucket"), r.URL.Query().Get("key")
	m, err := s.store.Meta().GetObjectMeta(bucket, key)
	if err != nil {
		writeJSON(w, http.StatusNotFound, errMap(err))
		return
	}
	res, err := s.store.ScanObject(m)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errMap(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"object": m, "scan": res, "healthy": res.Healthy(),
	})
}

func (s *Server) listRepairTasks(w http.ResponseWriter, r *http.Request) {
	tasks, err := s.store.Meta().ListRepairTasks(r.URL.Query().Get("status"), atoiDefault(r.URL.Query().Get("limit"), 100))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errMap(err))
		return
	}
	writeJSON(w, http.StatusOK, tasks)
}

func (s *Server) triggerRepair(w http.ResponseWriter, _ *http.Request) {
	s.repair.Trigger()
	writeJSON(w, http.StatusOK, map[string]string{"status": "triggered"})
}

func (s *Server) manualRepair(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Bucket string `json:"bucket"`
		Key    string `json:"key"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errMap(err))
		return
	}
	task, err := s.repair.RunManual(req.Bucket, req.Key)
	if err != nil {
		writeJSON(w, http.StatusConflict, errMap(err))
		return
	}
	writeJSON(w, http.StatusOK, task)
}

func (s *Server) pauseTask(w http.ResponseWriter, r *http.Request) {
	if err := s.repair.SetPaused(r.PathValue("id"), true); err != nil {
		writeJSON(w, http.StatusInternalServerError, errMap(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"id": r.PathValue("id"), "status": "paused"})
}

func (s *Server) resumeTask(w http.ResponseWriter, r *http.Request) {
	if err := s.repair.SetPaused(r.PathValue("id"), false); err != nil {
		writeJSON(w, http.StatusInternalServerError, errMap(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"id": r.PathValue("id"), "status": "running"})
}

func (s *Server) cancelTask(w http.ResponseWriter, r *http.Request) {
	if err := s.repair.CancelTask(r.PathValue("id")); err != nil {
		writeJSON(w, http.StatusConflict, errMap(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"id": r.PathValue("id"), "cancelled": "true"})
}

func (s *Server) getRate(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]int64{"bytes_per_sec": s.repair.Limiter().Rate()})
}

func (s *Server) setRate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		BytesPerSec int64 `json:"bytes_per_sec"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errMap(err))
		return
	}
	s.repair.Limiter().SetRate(req.BytesPerSec)
	writeJSON(w, http.StatusOK, map[string]int64{"bytes_per_sec": req.BytesPerSec})
}

func (s *Server) listRules(w http.ResponseWriter, _ *http.Request) {
	rules, err := s.store.Meta().ListLifecycleRules()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errMap(err))
		return
	}
	writeJSON(w, http.StatusOK, rules)
}

func (s *Server) putRule(w http.ResponseWriter, r *http.Request) {
	var rule meta.LifecycleRule
	if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
		writeJSON(w, http.StatusBadRequest, errMap(err))
		return
	}
	rule.Bucket = r.PathValue("bucket")
	if err := s.store.Meta().SetLifecycleRule(&rule); err != nil {
		writeJSON(w, http.StatusInternalServerError, errMap(err))
		return
	}
	writeJSON(w, http.StatusOK, rule)
}

func (s *Server) deleteRule(w http.ResponseWriter, r *http.Request) {
	if err := s.store.Meta().DeleteLifecycleRule(r.PathValue("bucket")); err != nil {
		writeJSON(w, http.StatusInternalServerError, errMap(err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"deleted": r.PathValue("bucket")})
}

func (s *Server) runLifecycle(w http.ResponseWriter, _ *http.Request) {
	go s.life.RunOnce()
	writeJSON(w, http.StatusOK, map[string]string{"status": "started"})
}

func (s *Server) listEvents(w http.ResponseWriter, r *http.Request) {
	events, err := s.store.Meta().ListLifecycleEvents(atoiDefault(r.URL.Query().Get("limit"), 100))
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errMap(err))
		return
	}
	writeJSON(w, http.StatusOK, events)
}

// ---- helpers ----

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func errMap(err error) map[string]string {
	return map[string]string{"error": err.Error()}
}

func atoiDefault(s string, def int) int {
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return def
		}
		n = n*10 + int(c-'0')
	}
	if s == "" {
		return def
	}
	return n
}
