// Package admin serves the management console API and embeds the single-page
// UI. All endpoints live under /api/v1 and are guarded by a bearer token
// distinct from the S3 credentials.
package admin

import (
	"context"
	"encoding/json"
	"io/fs"
	"net/http"
	"strconv"
	"strings"
	"time"

	"imstore/internal/index"
	"imstore/internal/lifecycle"
	"imstore/internal/repair"
	"imstore/internal/s3"
	"imstore/internal/volume"
)

// API wires the console endpoints.
type API struct {
	token    string
	volumes  *volume.Manager
	repair   *repair.Manager
	idx      *index.Index
	lc       *lifecycle.Runner
	s3h      *s3.Handler
	external string
	webFS    fs.FS
}

// NewAPI constructs the console API. webFS is the embedded UI filesystem
// (sub-root containing index.html).
func NewAPI(token, external string, vols *volume.Manager, rm *repair.Manager,
	idx *index.Index, lc *lifecycle.Runner, s3h *s3.Handler, webFS fs.FS,
) *API {
	return &API{
		token:    token,
		volumes:  vols,
		repair:   rm,
		idx:      idx,
		lc:       lc,
		s3h:      s3h,
		external: external,
		webFS:    webFS,
	}
}

// Mux returns the HTTP mux for the console. A single root handler dispatches
// by path prefix so the S3 API receives every method (including POST) on
// bucket/object paths without the std mux's method-based redirects.
func (a *API) Mux(s3Handler http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/login", a.login)
	mux.HandleFunc("/api/v1/", a.requireToken(a.route))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/ui" || r.URL.Path == "/ui/":
			http.StripPrefix("/ui/", http.FileServer(http.FS(a.webFS))).ServeHTTP(w, r)
		case strings.HasPrefix(r.URL.Path, "/ui/"):
			http.StripPrefix("/ui/", http.FileServer(http.FS(a.webFS))).ServeHTTP(w, r)
		case r.URL.Path == "/" || r.URL.Path == "/index.html":
			http.Redirect(w, r, "/ui/", http.StatusFound)
		default:
			s3Handler.ServeHTTP(w, r)
		}
	})
	return mux
}

func (a *API) login(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var in struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.Token != a.token {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid token"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"token": a.token})
}

// requireToken validates the bearer token on every API call.
func (a *API) requireToken(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h := r.Header.Get("Authorization")
		tok := strings.TrimPrefix(h, "Bearer ")
		if h == "" || tok != a.token {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}
		next(w, r)
	}
}

func (a *API) route(w http.ResponseWriter, r *http.Request) {
	p := strings.TrimPrefix(r.URL.Path, "/api/v1/")
	switch {
	case p == "overview":
		a.overview(w, r)
	case p == "volumes" || strings.HasPrefix(p, "volumes/"):
		a.volumesAPI(w, r, strings.TrimPrefix(p, "volumes/"))
	case p == "repair/tasks" || strings.HasPrefix(p, "repair/"):
		a.repairAPI(w, r, strings.TrimPrefix(p, "repair/"))
	case p == "lifecycle/rules" || strings.HasPrefix(p, "lifecycle/"):
		a.lifecycleAPI(w, r, strings.TrimPrefix(p, "lifecycle/"))
	case p == "buckets" || strings.HasPrefix(p, "buckets/"):
		a.bucketsAPI(w, r, strings.TrimPrefix(p, "buckets/"))
	case p == "presign":
		a.presign(w, r)
	default:
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown endpoint"})
	}
}

// ---------------- overview & volumes ----------------

func (a *API) overview(w http.ResponseWriter, _ *http.Request) {
	objCount, _ := a.idx.CountObjects()
	buckets, _ := a.idx.ListBuckets()
	var totalCap, totalUsed int64
	statusCounts := map[string]int{}
	for _, s := range a.volumes.Snapshot() {
		totalCap += s.CapacityBytes
		totalUsed += s.UsedBytes
		statusCounts[string(s.Status)]++
	}
	writeJSON(w, http.StatusOK, map[string]any{
		objectCount:    objCount,
		"bucket_count": len(buckets),
		"capacity":     totalCap,
		"used":         totalUsed,
		"volumes":      statusCounts,
		"repair":       a.repair.ListTasks(),
		"lifecycle":    a.lc.Stats(),
	})
}

const objectCount = "object_count"

func (a *API) volumesAPI(w http.ResponseWriter, r *http.Request, rest string) {
	if rest == "" {
		writeJSON(w, http.StatusOK, a.volumes.Snapshot())
		return
	}
	// rest: <id> or <id>/offline|online
	parts := strings.SplitN(rest, "/", 2)
	vol, err := a.volumes.Get(parts[0])
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}
	if len(parts) == 1 {
		writeJSON(w, http.StatusOK, vol.Snapshot())
		return
	}
	switch parts[1] {
	case "offline":
		if r.Method != http.MethodPost {
			http.Error(w, "POST required", http.StatusMethodNotAllowed)
			return
		}
		vol.SetOffline("manual offline via console")
		writeJSON(w, http.StatusOK, vol.Snapshot())
	case "online":
		if r.Method != http.MethodPost {
			http.Error(w, "POST required", http.StatusMethodNotAllowed)
			return
		}
		if err := vol.SetOnline(); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		// Kick a rebuild so missing shards on the re-inserted disk heal.
		ctx, cancel := context.WithTimeout(r.Context(), time.Minute)
		defer cancel()
		task, err := a.repair.RebuildVolume(ctx, vol.ID)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"volume": vol.Snapshot(), "repair_task": task})
	default:
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown volume action"})
	}
}

// ---------------- repair ----------------

func (a *API) repairAPI(w http.ResponseWriter, r *http.Request, rest string) {
	switch {
	case rest == "tasks" && r.Method == http.MethodGet:
		writeJSON(w, http.StatusOK, a.repair.ListTasks())
	case rest == "scan" && r.Method == http.MethodPost:
		go func() { _ = a.repair.ScanAndRepair("manual") }()
		writeJSON(w, http.StatusAccepted, map[string]string{"status": "scan started"})
	case rest == "scrub" && r.Method == http.MethodPost:
		task, err := a.repair.Scrub(r.Context())
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusAccepted, task)
	case strings.HasPrefix(rest, "rebuild/") && r.Method == http.MethodPost:
		vid := strings.TrimPrefix(rest, "rebuild/")
		task, err := a.repair.RebuildVolume(r.Context(), vid)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusAccepted, task)
	case rest == "rate" && r.Method == http.MethodPost:
		var in struct {
			BytesPerSecond int64 `json:"bytes_per_second"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		a.repair.Limiter().SetRate(in.BytesPerSecond)
		writeJSON(w, http.StatusOK, map[string]int64{"bytes_per_second": in.BytesPerSecond})
	default:
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown repair endpoint"})
	}
}

// ---------------- lifecycle ----------------

func (a *API) lifecycleAPI(w http.ResponseWriter, r *http.Request, rest string) {
	switch {
	case rest == "rules" && r.Method == http.MethodGet:
		rules, err := a.idx.ListLifecycleRules("")
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, rules)
	case rest == "rules" && r.Method == http.MethodPost:
		var rule index.LifecycleRule
		if err := json.NewDecoder(r.Body).Decode(&rule); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		if rule.ID == "" {
			rule.ID = "lc-" + strconv.FormatInt(time.Now().UnixNano(), 10)
		}
		if err := a.idx.PutLifecycleRule(rule); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusCreated, rule)
	case strings.HasPrefix(rest, "rules/") && r.Method == http.MethodDelete:
		id := strings.TrimPrefix(rest, "rules/")
		if _, err := a.idx.DeleteLifecycleRule(id); err != nil {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case rest == "run" && r.Method == http.MethodPost:
		go a.lc.RunOnce()
		writeJSON(w, http.StatusAccepted, map[string]string{"status": "lifecycle run started"})
	case rest == "stats":
		writeJSON(w, http.StatusOK, a.lc.Stats())
	default:
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown lifecycle endpoint"})
	}
}

// ---------------- buckets ----------------

func (a *API) bucketsAPI(w http.ResponseWriter, r *http.Request, rest string) {
	if rest == "" {
		buckets, err := a.idx.ListBuckets()
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, buckets)
		return
	}
	parts := strings.SplitN(rest, "/", 2)
	bucket := parts[0]
	if len(parts) == 2 && parts[1] == "create" && r.Method == http.MethodPost {
		if err := a.idx.CreateBucket(bucket); err != nil {
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusCreated, map[string]string{"name": bucket})
		return
	}
	if len(parts) == 1 || parts[1] == "" || parts[1] == "objects" {
		prefix := r.URL.Query().Get("prefix")
		token := r.URL.Query().Get("token")
		maxKeys := 200
		page, err := a.idx.ListObjects(bucket, prefix, token, "/", maxKeys)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, page)
	}
}

// ---------------- presign ----------------

func (a *API) presign(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	bucket := q.Get("bucket")
	key := q.Get("key")
	expires, _ := time.ParseDuration(q.Get("expires"))
	if expires <= 0 || expires > 7*24*time.Hour {
		expires = time.Hour
	}
	rawURL := strings.TrimRight(a.external, "/") + "/" + bucket + "/" + key
	ak, sk := a.s3h.Credentials()
	url, err := a.s3h.Verifier().Presign(http.MethodGet, rawURL, nil, expires, ak, sk)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"url": url, "expires_at": time.Now().Add(expires).Format(time.RFC3339)})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
