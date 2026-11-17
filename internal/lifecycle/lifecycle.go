// Package lifecycle applies expiration rules in the background: objects older
// than a rule's age are deleted and stalled multipart uploads are aborted so
// their part shards do not leak storage.
package lifecycle

import (
	"context"
	"sync"
	"time"

	"imstore/internal/index"
	"imstore/internal/store"
)

// Runner periodically evaluates lifecycle rules.
type Runner struct {
	st       *store.Store
	interval time.Duration
	mpMaxAge time.Duration

	mu      sync.Mutex
	lastRun time.Time
	expired int64
	aborted int64

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewRunner builds a lifecycle runner.
func NewRunner(st *store.Store, interval time.Duration, mpMaxAge time.Duration) *Runner {
	return &Runner{st: st, interval: interval, mpMaxAge: mpMaxAge}
}

// Start launches the background loop.
func (r *Runner) Start() {
	r.ctx, r.cancel = context.WithCancel(context.Background())
	r.wg.Add(1)
	go r.loop()
}

// Stop terminates the loop.
func (r *Runner) Stop() {
	if r.cancel != nil {
		r.cancel()
	}
	r.wg.Wait()
}

// Stats is a console snapshot.
type Stats struct {
	LastRun        time.Time `json:"last_run"`
	ExpiredObjects int64     `json:"expired_objects"`
	AbortedUploads int64     `json:"aborted_uploads"`
}

// Stats returns cumulative cleanup counters.
func (r *Runner) Stats() Stats {
	r.mu.Lock()
	defer r.mu.Unlock()
	return Stats{LastRun: r.lastRun, ExpiredObjects: r.expired, AbortedUploads: r.aborted}
}

func (r *Runner) loop() {
	defer r.wg.Done()
	t := time.NewTicker(r.interval)
	defer t.Stop()
	// Run once shortly after boot so rules take effect without a full interval.
	select {
	case <-r.ctx.Done():
		return
	case <-time.After(2 * time.Second):
		r.runOnce()
	}
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-t.C:
			r.runOnce()
		}
	}
}

// RunOnce exposes a manual evaluation (also used by tests).
func (r *Runner) RunOnce() { r.runOnce() }

func (r *Runner) runOnce() {
	now := time.Now().UTC()
	var expired, aborted int64

	// 1. Expire objects matching enabled rules. The index scan holds a bbolt
	// read transaction, so collect candidates first and delete afterwards —
	// opening a write transaction mid-scan would self-deadlock.
	type target struct{ bucket, key string }
	var victims []target
	_ = r.st.Index().AllExpiredObjects(now, func(meta index.ObjectMeta, _ index.LifecycleRule) error {
		victims = append(victims, target{meta.Bucket, meta.Key})
		return nil
	})
	for _, v := range victims {
		if err := r.st.DeleteObject(v.bucket, v.key); err == nil {
			expired++
		}
	}

	// 2. Abort multipart uploads that have been open too long.
	old, err := r.st.Index().OldMPUploads(now.Add(-r.mpMaxAge))
	if err == nil {
		for _, up := range old {
			if err := r.st.AbortMultipart(up.UploadID); err == nil {
				aborted++
			}
		}
	}

	r.mu.Lock()
	r.lastRun = now
	r.expired += expired
	r.aborted += aborted
	r.mu.Unlock()
}
