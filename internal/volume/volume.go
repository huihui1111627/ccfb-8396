// Package volume manages the local directories that simulate physical disks.
// Each Volume is one failure domain member; volumes grouped under the same
// Enclosure share a second-level fault domain and never receive two shards of
// the same object.
package volume

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"imstore/internal/shard"
)

// Status of a volume.
type Status string

const (
	StatusOnline   Status = "online"
	StatusOffline  Status = "offline" // disk pulled / path missing
	StatusReadOnly Status = "readonly"
)

// ErrOffline is returned when an IO operation targets an offline volume.
var ErrOffline = errors.New("volume: disk is offline")

// Stats is a point-in-time snapshot of one volume.
type Stats struct {
	ID            string    `json:"id"`
	Path          string    `json:"path"`
	Enclosure     string    `json:"enclosure"`
	Status        Status    `json:"status"`
	CapacityBytes int64     `json:"capacity_bytes"`
	UsedBytes     int64     `json:"used_bytes"`
	FreeBytes     int64     `json:"free_bytes"`
	ErrorCount    int64     `json:"error_count"`
	ShardCount    int64     `json:"shard_count"`
	LastError     string    `json:"last_error"`
	LastCheckedAt time.Time `json:"last_checked_at"`
}

// Volume represents one simulated disk backed by a directory.
type Volume struct {
	ID        string
	Path      string
	Enclosure string

	mu          sync.RWMutex
	status      Status
	manualOff   bool // operator/admin forced offline (disk pulled)
	errors      int64
	lastError   string
	lastCheck   time.Time
	foreground  atomic.Int64 // in-flight foreground IO operations
	repairBytes atomic.Int64 // bytes written by repair since boot
}

// New creates a volume and ensures its directory structure exists.
func New(id, path, enclosure string) (*Volume, error) {
	v := &Volume{ID: id, Path: path, Enclosure: enclosure, status: StatusOnline}
	if err := os.MkdirAll(filepath.Join(path, "shards"), 0o755); err != nil {
		return nil, fmt.Errorf("volume %s: init path: %w", id, err)
	}
	return v, nil
}

// Status returns the current status under lock.
func (v *Volume) Status() Status {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.status
}

// SetOffline simulates pulling the disk; SetOnline re-inserts it.
func (v *Volume) SetOffline(reason string) {
	v.mu.Lock()
	v.manualOff = true
	v.status = StatusOffline
	if reason != "" {
		v.lastError = reason
	}
	v.mu.Unlock()
}

// SetOnline marks the disk as present again.
func (v *Volume) SetOnline() error {
	if err := os.MkdirAll(filepath.Join(v.Path, "shards"), 0o755); err != nil {
		return fmt.Errorf("volume %s: re-init path: %w", v.ID, err)
	}
	v.mu.Lock()
	v.manualOff = false
	v.status = StatusOnline
	v.mu.Unlock()
	return nil
}

// healthy reports whether foreground writes may target this volume.
func (v *Volume) healthy() bool {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.status == StatusOnline
}

// RecordError increments the error counter and stores the message.
func (v *Volume) RecordError(err error) {
	atomic.AddInt64(&v.errors, 1)
	v.mu.Lock()
	v.lastError = err.Error()
	v.mu.Unlock()
}

// EnterForeground / LeaveForeground bracket a foreground IO operation so the
// repair loop can yield while user traffic is active.
func (v *Volume) EnterForeground() { v.foreground.Add(1) }
func (v *Volume) LeaveForeground() { v.foreground.Add(-1) }

// ForegroundLoad returns the number of in-flight foreground operations.
func (v *Volume) ForegroundLoad() int64 { return v.foreground.Load() }

// AddRepairBytes accounts bytes produced by rebuild work.
func (v *Volume) AddRepairBytes(n int64) { v.repairBytes.Add(n) }

// ShardPath returns the on-disk path of one shard on this volume.
func (v *Volume) ShardPath(objectID string, shardIndex int) string {
	return shard.Path(v.Path, objectID, shardIndex)
}

// OpenShard opens a shard for reading, enforcing offline state.
func (v *Volume) OpenShard(objectID string, shardIndex int) (*shard.Reader, error) {
	if !v.healthy() {
		return nil, fmt.Errorf("%w: %s", ErrOffline, v.ID)
	}
	r, err := shard.Open(v.ShardPath(objectID, shardIndex))
	if err != nil {
		v.RecordError(err)
		return nil, err
	}
	return r, nil
}

// CreateShard opens a shard for writing, enforcing offline state.
func (v *Volume) CreateShard(h shard.Header) (*shard.Writer, error) {
	if !v.healthy() {
		return nil, fmt.Errorf("%w: %s", ErrOffline, v.ID)
	}
	w, err := shard.Create(v.ShardPath(h.ObjectID, h.ShardIndex), h)
	if err != nil {
		v.RecordError(err)
		return nil, err
	}
	return w, nil
}

// CreateStreamShard opens a streaming shard writer, enforcing offline state.
func (v *Volume) CreateStreamShard(h shard.Header) (*shard.StreamWriter, error) {
	if !v.healthy() {
		return nil, fmt.Errorf("%w: %s", ErrOffline, v.ID)
	}
	w, err := shard.CreateStream(v.ShardPath(h.ObjectID, h.ShardIndex), h)
	if err != nil {
		v.RecordError(err)
		return nil, err
	}
	return w, nil
}

// RemoveShard deletes a shard file; missing files are not an error.
func (v *Volume) RemoveShard(objectID string, shardIndex int) error {
	if !v.healthy() {
		return fmt.Errorf("%w: %s", ErrOffline, v.ID)
	}
	err := os.Remove(v.ShardPath(objectID, shardIndex))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		v.RecordError(err)
		return err
	}
	return nil
}

// ShardExists reports whether the shard file is present on disk.
func (v *Volume) ShardExists(objectID string, shardIndex int) (bool, error) {
	if !v.healthy() {
		return false, fmt.Errorf("%w: %s", ErrOffline, v.ID)
	}
	_, err := os.Stat(v.ShardPath(objectID, shardIndex))
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, err
}

// CountShards walks the shard tree and counts shard files.
func (v *Volume) CountShards() (int64, error) {
	var n int64
	err := filepath.WalkDir(filepath.Join(v.Path, "shards"), func(_ string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && filepath.Ext(d.Name()) == ".ims" {
			n++
		}
		return nil
	})
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return 0, err
	}
	return n, nil
}

// ListShards returns every shard file path on this volume (used by scrub).
func (v *Volume) ListShards() ([]string, error) {
	var out []string
	err := filepath.WalkDir(filepath.Join(v.Path, "shards"), func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && filepath.Ext(d.Name()) == ".ims" {
			out = append(out, p)
		}
		return nil
	})
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return out, nil
}

// Snapshot gathers capacity and counters for the console.
func (v *Volume) Snapshot() Stats {
	v.mu.RLock()
	status := v.status
	lastErr := v.lastError
	manualOff := v.manualOff
	v.mu.RUnlock()

	s := Stats{
		ID:            v.ID,
		Path:          v.Path,
		Enclosure:     v.Enclosure,
		Status:        status,
		ErrorCount:    atomic.LoadInt64(&v.errors),
		LastError:     lastErr,
		LastCheckedAt: v.lastCheck,
	}
	total, free, err := diskSpace(v.Path)
	if err != nil {
		s.LastError = err.Error()
	} else {
		s.CapacityBytes = total
		s.FreeBytes = free
		s.UsedBytes = total - free
	}
	if n, err := v.CountShards(); err == nil {
		s.ShardCount = n
	}
	_ = manualOff
	v.mu.Lock()
	v.lastCheck = time.Now()
	v.mu.Unlock()
	return s
}

// markChecked is exposed for tests/scrubbing that want cheap stats without
// re-walking the whole disk.
func (v *Volume) markChecked() {
	v.mu.Lock()
	v.lastCheck = time.Now()
	v.mu.Unlock()
}
