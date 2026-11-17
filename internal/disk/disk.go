// Package disk 管理由本地目录模拟的存储盘及其故障域（盘柜）。
package disk

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"

	"erasurestore/internal/config"
)

// Status 磁盘状态。
type Status string

const (
	StatusOnline  Status = "online"
	StatusOffline Status = "offline" // 被管理员置为离线（模拟拔盘）
	StatusFaulted Status = "faulted" // 目录不可访问（I/O 错误）
)

// Stats 磁盘运行时统计与容量信息。
type Stats struct {
	ID            string  `json:"id"`
	Path          string  `json:"path"`
	Enclosure     string  `json:"enclosure"`
	Status        Status  `json:"status"`
	TotalBytes    uint64  `json:"total_bytes"`
	FreeBytes     uint64  `json:"free_bytes"`
	UsedBytes     uint64  `json:"used_bytes"`
	UsedPercent   float64 `json:"used_percent"`
	ReadErrors    uint64  `json:"read_errors"`
	WriteErrors   uint64  `json:"write_errors"`
	ShardCount    int64   `json:"shard_count"`
	LastCheckedAt int64   `json:"last_checked_at"`
}

// Disk 单块盘。
type Disk struct {
	cfg       config.DiskConfig
	status    atomic.Value // Status
	readErr   atomic.Uint64
	writeErr  atomic.Uint64
	lastCheck atomic.Int64
	mu        sync.RWMutex
}

// Manager 管理全部磁盘。
type Manager struct {
	disks map[string]*Disk
	order []string
	mu    sync.RWMutex
}

// NewManager 根据配置创建磁盘管理器并确保目录存在。
func NewManager(cfgs []config.DiskConfig) (*Manager, error) {
	m := &Manager{disks: map[string]*Disk{}}
	for _, c := range cfgs {
		if err := os.MkdirAll(filepath.Join(c.Path, "shards"), 0o755); err != nil {
			return nil, fmt.Errorf("初始化磁盘 %s 失败: %w", c.ID, err)
		}
		d := &Disk{cfg: c}
		d.status.Store(StatusOnline)
		m.disks[c.ID] = d
		m.order = append(m.order, c.ID)
	}
	return m, nil
}

// Get 按 ID 取盘。
func (m *Manager) Get(id string) (*Disk, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	d, ok := m.disks[id]
	return d, ok
}

// All 返回全部磁盘（按配置顺序）。
func (m *Manager) All() []*Disk {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Disk, 0, len(m.order))
	for _, id := range m.order {
		out = append(out, m.disks[id])
	}
	return out
}

// Online 返回当前可写的在线磁盘。
func (m *Manager) Online() []*Disk {
	all := m.All()
	out := make([]*Disk, 0, len(all))
	for _, d := range all {
		if d.Status() == StatusOnline {
			out = append(out, d)
		}
	}
	return out
}

// ID 磁盘 ID。
func (d *Disk) ID() string { return d.cfg.ID }

// Enclosure 所属盘柜。
func (d *Disk) Enclosure() string {
	if d.cfg.Enclosure == "" {
		return d.cfg.ID
	}
	return d.cfg.Enclosure
}

// Path 磁盘根目录。
func (d *Disk) Path() string { return d.cfg.Path }

// Status 当前状态。
func (d *Disk) Status() Status { return d.status.Load().(Status) }

// SetStatus 修改状态（模拟拔盘/插回）。
func (d *Disk) SetStatus(s Status) { d.status.Store(s) }

// AddReadError / AddWriteError 累加错误计数。
func (d *Disk) AddReadError() uint64  { return d.readErr.Add(1) }
func (d *Disk) AddWriteError() uint64 { return d.writeErr.Add(1) }

// TouchCheck 记录最近巡检时间。
func (d *Disk) TouchCheck() { d.lastCheck.Store(time.Now().Unix()) }

// ShardPath 返回某对象某分片的完整路径。
func (d *Disk) ShardPath(objectID string, shardIndex int) string {
	return filepath.Join(d.cfg.Path, "shards", shardPrefix(objectID), fmt.Sprintf("%s.%d.ers", objectID, shardIndex))
}

// TempPath 返回临时分片路径（上传/合并期间使用）。
func (d *Disk) TempPath(name string) string {
	return filepath.Join(d.cfg.Path, "tmp", name)
}

// shardPrefix 按对象 ID 前两位做一级散列，避免单目录文件过多。
func shardPrefix(objectID string) string {
	if len(objectID) < 2 {
		return "00"
	}
	return objectID[:2]
}

// ShardDir 返回分片目录（供扫描使用）。
func (d *Disk) ShardDir() string { return filepath.Join(d.cfg.Path, "shards") }

// ErrDiskUnavailable 磁盘不可用。
var ErrDiskUnavailable = errors.New("磁盘不可用")

// OpenShard 打开分片文件；盘离线/故障时返回 ErrDiskUnavailable。
func (d *Disk) OpenShard(objectID string, shardIndex int) (*os.File, error) {
	if d.Status() != StatusOnline {
		return nil, ErrDiskUnavailable
	}
	f, err := os.Open(d.ShardPath(objectID, shardIndex))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, err
		}
		d.AddReadError()
		d.detectFault()
		return nil, err
	}
	return f, nil
}

// StatShard 统计分片文件信息，不存在返回 ok=false。
func (d *Disk) StatShard(objectID string, shardIndex int) (os.FileInfo, bool) {
	if d.Status() != StatusOnline {
		return nil, false
	}
	fi, err := os.Stat(d.ShardPath(objectID, shardIndex))
	if err != nil {
		return nil, false
	}
	return fi, true
}

// detectFault 探测根目录是否可访问，不可访问则标记 faulted。
func (d *Disk) detectFault() {
	if _, err := os.Stat(d.cfg.Path); err != nil {
		d.status.Store(StatusFaulted)
	}
}

// Probe 主动巡检一次磁盘：目录不可达 -> faulted；faulted 但目录恢复 -> online。
func (d *Disk) Probe() {
	d.TouchCheck()
	_, err := os.Stat(d.cfg.Path)
	cur := d.Status()
	if err != nil {
		if cur == StatusOnline {
			d.status.Store(StatusFaulted)
			d.AddReadError()
		}
		return
	}
	if cur == StatusFaulted {
		d.status.Store(StatusOnline)
	}
}

// CountShards 统计盘上分片文件数。
func (d *Disk) CountShards() int64 {
	var n int64
	root := d.ShardDir()
	_ = filepath.WalkDir(root, func(_ string, e os.DirEntry, err error) error {
		if err == nil && !e.IsDir() {
			n++
		}
		return nil
	})
	return n
}

// Stats 采集容量与统计。
func (d *Disk) Stats() Stats {
	d.TouchCheck()
	s := Stats{
		ID:            d.cfg.ID,
		Path:          d.cfg.Path,
		Enclosure:     d.Enclosure(),
		Status:        d.Status(),
		ReadErrors:    d.readErr.Load(),
		WriteErrors:   d.writeErr.Load(),
		LastCheckedAt: d.lastCheck.Load(),
		ShardCount:    d.CountShards(),
	}
	var fs unix.Statfs_t
	if err := unix.Statfs(d.cfg.Path, &fs); err != nil {
		d.AddReadError()
		return s
	}
	s.TotalBytes = fs.Blocks * uint64(fs.Bsize)
	s.FreeBytes = fs.Bavail * uint64(fs.Bsize)
	s.UsedBytes = s.TotalBytes - s.FreeBytes
	if s.TotalBytes > 0 {
		s.UsedPercent = float64(s.UsedBytes) / float64(s.TotalBytes) * 100
	}
	return s
}

// ManagerStats 汇总所有磁盘。
func (m *Manager) ManagerStats() []Stats {
	all := m.All()
	out := make([]Stats, 0, len(all))
	for _, d := range all {
		out = append(out, d.Stats())
	}
	return out
}
