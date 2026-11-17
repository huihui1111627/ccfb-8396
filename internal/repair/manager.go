package repair

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"imstore/internal/index"
	"imstore/internal/shard"
	"imstore/internal/store"
	"imstore/internal/volume"
)

// TaskStatus values.
type TaskStatus string

const (
	StatusQueued    TaskStatus = "queued"
	StatusRunning   TaskStatus = "running"
	StatusSucceeded TaskStatus = "succeeded"
	StatusFailed    TaskStatus = "failed"
)

// Task is the console-visible record of one repair job.
type Task struct {
	ID           int64      `json:"id"`
	Kind         string     `json:"kind"` // object-repair | volume-rebuild | scrub
	Target       string     `json:"target"`
	Bucket       string     `json:"bucket,omitempty"`
	Key          string     `json:"key,omitempty"`
	Status       TaskStatus `json:"status"`
	ShardsTotal  int        `json:"shards_total"`
	ShardsDone   int        `json:"shards_done"`
	BytesFixed   int64      `json:"bytes_fixed"`
	Error        string     `json:"error,omitempty"`
	StartedAt    time.Time  `json:"started_at"`
	FinishedAt   *time.Time `json:"finished_at,omitempty"`
	ThrottledBps int64      `json:"throttled_bps"`
}

func (t *Task) snapshot() Task {
	// Return a value copy; FinishedAt pointer is fine for JSON snapshots.
	return *t
}

// Manager runs background healing: periodic degraded-object scans, full
// volume rebuilds and bit-rot scrubs, all under the shared limiter.
type Manager struct {
	st       *store.Store
	limiter  *Limiter
	interval time.Duration

	mu     sync.Mutex
	tasks  map[int64]*Task
	order  []int64
	nextID int64

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// runMu ensures only one repair sweep executes at a time.
	runMu sync.Mutex
}

// NewManager builds the repair manager.
func NewManager(st *store.Store, bps int64, interval time.Duration, yieldWhenBusy bool) *Manager {
	var yield func() bool
	if yieldWhenBusy {
		yield = func() bool {
			for _, v := range st.Volumes().All() {
				if v.ForegroundLoad() > 0 {
					return true
				}
			}
			return false
		}
	}
	return &Manager{
		st:       st,
		limiter:  NewLimiter(bps, yield),
		interval: interval,
		tasks:    map[int64]*Task{},
	}
}

// Limiter exposes the limiter for runtime tuning.
func (m *Manager) Limiter() *Limiter { return m.limiter }

// Start launches the background sweep loop.
func (m *Manager) Start() {
	m.ctx, m.cancel = context.WithCancel(context.Background())
	m.wg.Add(1)
	go m.loop()
}

// Stop signals the loop to exit and waits for it.
func (m *Manager) Stop() {
	if m.cancel != nil {
		m.cancel()
	}
	m.wg.Wait()
}

func (m *Manager) loop() {
	defer m.wg.Done()
	t := time.NewTicker(m.interval)
	defer t.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-t.C:
			if err := m.ScanAndRepair("auto-sweep"); err != nil {
				// Error is recorded on the task; nothing to log here without
				// coupling to a logger interface.
				_ = err
			}
		}
	}
}

// createTask registers a new task record.
func (m *Manager) createTask(kind, target, bucket, key string, shards int) *Task {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextID++
	t := &Task{
		ID:           m.nextID,
		Kind:         kind,
		Target:       target,
		Bucket:       bucket,
		Key:          key,
		Status:       StatusQueued,
		ShardsTotal:  shards,
		StartedAt:    time.Now().UTC(),
		ThrottledBps: m.limiter.Rate(),
	}
	m.tasks[t.ID] = t
	m.order = append(m.order, t.ID)
	return t
}

// ListTasks returns recent tasks newest-first (bounded to last 200).
func (m *Manager) ListTasks() []Task {
	m.mu.Lock()
	defer m.mu.Unlock()
	ids := append([]int64(nil), m.order...)
	sort.Slice(ids, func(i, j int) bool { return ids[i] > ids[j] })
	if len(ids) > 200 {
		ids = ids[:200]
	}
	out := make([]Task, 0, len(ids))
	for _, id := range ids {
		out = append(out, m.tasks[id].snapshot())
	}
	return out
}

func (m *Manager) finish(t *Task, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now().UTC()
	t.FinishedAt = &now
	if err != nil {
		t.Status = StatusFailed
		t.Error = err.Error()
	} else {
		t.Status = StatusSucceeded
	}
}

// ScanAndRepair walks every object and heals any shard gaps it finds. It is
// safe to call while serving traffic; the runMu serialises sweeps.
func (m *Manager) ScanAndRepair(trigger string) error {
	if !m.runMu.TryLock() {
		return nil // a sweep is already running
	}
	defer m.runMu.Unlock()

	var firstErr error
	err := m.st.Index().ScanObjects(func(meta index.ObjectMeta) error {
		if m.ctx != nil && m.ctx.Err() != nil {
			return m.ctx.Err()
		}
		if err := m.repairObject(meta, "auto-sweep"); err != nil && firstErr == nil {
			firstErr = err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return firstErr
}

// RebuildVolume heals every object that placed a shard on volumeID. Used when
// a replacement disk is inserted (the volume id stays stable).
func (m *Manager) RebuildVolume(ctx context.Context, volumeID string) (*Task, error) {
	if _, err := m.st.Volumes().Get(volumeID); err != nil {
		return nil, err
	}
	task := m.createTask("volume-rebuild", volumeID, "", "", 0)
	task.Status = StatusRunning
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		var firstErr error
		err := m.st.Index().ObjectsOnVolume(volumeID, func(bucket, key string) error {
			meta, err := m.st.Index().GetObject(bucket, key)
			if err != nil {
				return err
			}
			if err := m.repairObjectCtx(ctx, *meta, "volume:"+volumeID); err != nil && firstErr == nil {
				firstErr = err
			}
			return nil
		})
		if err != nil && firstErr == nil {
			firstErr = err
		}
		m.finish(task, firstErr)
	}()
	return task, nil
}

// repairObject inspects each part of an object and rewrites any shard whose
// file is missing, unreadable or checksum-corrupt.
func (m *Manager) repairObject(meta index.ObjectMeta, trigger string) error {
	return m.repairObjectCtx(context.Background(), meta, trigger)
}

func (m *Manager) repairObjectCtx(ctx context.Context, meta index.ObjectMeta, trigger string) error {
	parts := meta.AllParts()
	missing := 0
	for _, p := range parts {
		for shardIdx, vid := range p.VolumeIDs {
			v, err := m.st.Volumes().Get(vid)
			if err != nil {
				missing++
				continue
			}
			if v.Status() != volume.StatusOnline {
				missing++
				continue
			}
			exists, err := v.ShardExists(p.ObjectID, shardIdx)
			if err != nil || !exists {
				missing++
				continue
			}
			// Detect bit rot by opening and reading every stripe.
			rd, err := v.OpenShard(p.ObjectID, shardIdx)
			if err != nil {
				missing++
				continue
			}
			bad := false
			for s := 0; s < p.StripeCount; s++ {
				if _, err := rd.ReadStripe(s); err != nil {
					if errors.Is(err, shard.ErrChecksumMismatch) {
						v.RecordError(err)
					}
					bad = true
					break
				}
			}
			rd.Close()
			if bad {
				missing++
			}
		}
	}
	if missing == 0 {
		return nil
	}

	task := m.createTask("object-repair", meta.ObjectID, meta.Bucket, meta.Key, missing)
	task.Status = StatusRunning
	err := m.healObject(ctx, meta, task)
	m.finish(task, err)
	return err
}

// healObject reconstructs every degraded part stripe and writes the healed
// shards back, honoring the limiter for both reconstruction reads and writes.
func (m *Manager) healObject(ctx context.Context, meta index.ObjectMeta, task *Task) error {
	codec := m.st.Codec()
	for _, part := range meta.AllParts() {
		// Determine which shard indices need healing.
		bad := map[int]bool{}
		for shardIdx, vid := range part.VolumeIDs {
			v, err := m.st.Volumes().Get(vid)
			if err != nil || v.Status() != volume.StatusOnline {
				bad[shardIdx] = true
				continue
			}
			ok, err := v.ShardExists(part.ObjectID, shardIdx)
			if err != nil || !ok {
				bad[shardIdx] = true
				continue
			}
		}
		if len(bad) == 0 {
			continue
		}

		// Open surviving shard readers once for the whole part.
		readers := make([]*shard.Reader, codec.TotalShards())
		for i, vid := range part.VolumeIDs {
			if bad[i] {
				continue
			}
			v, err := m.st.Volumes().Get(vid)
			if err != nil {
				continue
			}
			rd, err := v.OpenShard(part.ObjectID, i)
			if err != nil {
				v.RecordError(err)
				bad[i] = true
				continue
			}
			readers[i] = rd
		}
		defer closeReaders(readers)

		// Open replacement writers for every bad shard on its target volume.
		writers := make([]*shard.StreamWriter, codec.TotalShards())
		for idx := range bad {
			vid := part.VolumeIDs[idx]
			v, err := m.st.Volumes().Get(vid)
			if err != nil {
				return fmt.Errorf("repair: target volume %s unavailable: %w", vid, err)
			}
			w, err := v.CreateStreamShard(shard.Header{
				ObjectID:    part.ObjectID,
				ShardIndex:  idx,
				TotalShards: codec.TotalShards(),
				ShardSize:   codec.ShardSize(),
				CreatedNano: time.Now().UTC().UnixNano(),
			})
			if err != nil {
				return fmt.Errorf("repair: create replacement shard: %w", err)
			}
			writers[idx] = w
		}

		for stripe := 0; stripe < part.StripeCount; stripe++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			slots := make([][]byte, codec.TotalShards())
			good := 0
			for i, rd := range readers {
				if rd == nil {
					continue
				}
				buf, err := rd.ReadStripe(stripe)
				if err != nil {
					// A reader that looked healthy has a corrupt stripe;
					// reconstruct it too by nil-ing the slot.
					slots[i] = nil
					continue
				}
				slots[i] = buf
				good++
			}
			if good < codec.DataShards() {
				return fmt.Errorf("%w: object %s stripe %d has %d/%d healthy shards",
					store.ErrTooManyMissing, meta.ObjectID, stripe, good, codec.DataShards())
			}
			if err := codec.Reconstruct(slots); err != nil {
				return fmt.Errorf("repair: reconstruct stripe %d: %w", stripe, err)
			}
			for idx, w := range writers {
				if w == nil {
					continue
				}
				// Throttle the replacement write bytes.
				if err := m.limiter.Wait(ctx, int(codec.ShardSize())); err != nil {
					return err
				}
				if err := w.WriteStripe(slots[idx]); err != nil {
					return fmt.Errorf("repair: write healed stripe: %w", err)
				}
				m.addBytes(part.VolumeIDs[idx], codec.ShardSize())
			}
			task.ShardsDone = len(bad) * (stripe + 1)
			task.BytesFixed += int64(len(bad)) * codec.ShardSize()
		}
		for _, w := range writers {
			if w == nil {
				continue
			}
			if err := w.Close(part.Size); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *Manager) addBytes(volumeID string, n int64) {
	if v, err := m.st.Volumes().Get(volumeID); err == nil {
		v.AddRepairBytes(n)
	}
}

func closeReaders(rs []*shard.Reader) {
	for _, r := range rs {
		if r != nil {
			r.Close()
		}
	}
}

// Scrub walks all objects, verifies integrity and heals bit rot in one pass.
func (m *Manager) Scrub(ctx context.Context) (*Task, error) {
	count, err := m.st.Index().CountObjects()
	if err != nil {
		return nil, err
	}
	task := m.createTask("scrub", "all-volumes", "", "", int(count))
	task.Status = StatusRunning
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		var firstErr error
		_ = m.st.Index().ScanObjects(func(meta index.ObjectMeta) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := m.repairObjectCtx(ctx, meta, "scrub"); err != nil && firstErr == nil {
				firstErr = err
			}
			task.ShardsDone++
			return nil
		})
		m.finish(task, firstErr)
	}()
	return task, nil
}
