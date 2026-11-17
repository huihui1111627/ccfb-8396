package repair

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"erasurestore/internal/disk"
	"erasurestore/internal/meta"
	"erasurestore/internal/storage"
)

// Manager 修复任务编排器。
type Manager struct {
	store   *storage.Store
	limiter *Limiter

	mu        sync.Mutex
	running   map[string]context.CancelFunc // taskID -> cancel
	paused    map[string]bool
	inflight  map[string]bool // objectID -> 正在重建，防止同对象并发重建互相截断
	triggerCh chan struct{}

	stopOnce sync.Once
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// NewManager 创建修复管理器。
func NewManager(st *storage.Store, defaultRate int64) *Manager {
	return &Manager{
		store:     st,
		limiter:   NewLimiter(defaultRate),
		running:   map[string]context.CancelFunc{},
		paused:    map[string]bool{},
		inflight:  map[string]bool{},
		triggerCh: make(chan struct{}, 1),
		stopCh:    make(chan struct{}),
	}
}

// Limiter 暴露限速器（控制台动态调速）。
func (m *Manager) Limiter() *Limiter { return m.limiter }

// Start 启动后台巡检：定时扫描 + 事件触发（去抖）。
func (m *Manager) Start() {
	m.wg.Add(1)
	go m.loop()
}

// Stop 停止后台循环并等待在跑任务收尾。
func (m *Manager) Stop() {
	m.stopOnce.Do(func() { close(m.stopCh) })
	m.wg.Wait()
}

// Trigger 通知管理器尽快扫描一次（拔盘/换盘后调用）。
func (m *Manager) Trigger() {
	select {
	case m.triggerCh <- struct{}{}:
	default:
	}
}

func (m *Manager) loop() {
	defer m.wg.Done()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		m.scanAndRepair(context.Background())
		select {
		case <-m.stopCh:
			return
		case <-m.triggerCh:
			// 去抖：连续触发合并为一次扫描
			select {
			case <-m.triggerCh:
			default:
			}
		case <-ticker.C:
		}
	}
}

// SetPaused 暂停/恢复某个任务。
func (m *Manager) SetPaused(taskID string, paused bool) error {
	if _, err := m.store.Meta().GetRepairTask(taskID); err != nil {
		return err
	}
	m.mu.Lock()
	m.paused[taskID] = paused
	m.mu.Unlock()
	return m.store.Meta().SaveRepairTask(&meta.RepairTask{
		ID: taskID, Status: map[bool]meta.RepairTaskStatus{true: meta.RepairPaused, false: meta.RepairRunning}[paused],
		UpdatedAt: time.Now(),
	})
}

// pauseLimiter 把限速器按任务暂停状态动态置零/恢复。
type pauseLimiter struct {
	parent *Limiter
	mgr    *Manager
	taskID string
}

func (p *pauseLimiter) Wait(n int) error {
	for {
		p.mgr.mu.Lock()
		paused := p.mgr.paused[p.taskID]
		cancel := p.mgr.running[p.taskID] == nil
		p.mgr.mu.Unlock()
		if cancel {
			return context.Canceled
		}
		if !paused {
			return p.parent.Wait(n)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// scanAndRepair 扫描全部对象，为不健康对象创建并执行修复任务。
func (m *Manager) scanAndRepair(parentCtx context.Context) {
	// 先主动探测磁盘状态（模拟拔盘后目录消失能被发现）
	for _, d := range m.store.DiskManager().All() {
		d.Probe()
	}

	type work struct {
		meta meta.ObjectMeta
		scan storage.ScanResult
	}
	var works []work
	err := m.store.Meta().AllObjectMetas(func(om meta.ObjectMeta) error {
		res, err := m.store.ScanObject(&om)
		if err != nil {
			log.Printf("scan 对象 %s/%s 出错: %v", om.Bucket, om.Key, err)
			return nil
		}
		if !res.Healthy() {
			works = append(works, work{om, res})
		}
		return nil
	})
	if err != nil {
		log.Printf("修复扫描失败: %v", err)
		return
	}
	for _, w := range works {
		select {
		case <-m.stopCh:
			return
		case <-parentCtx.Done():
			return
		default:
		}
		if err := m.repairOne(w.meta, w.scan); err != nil {
			log.Printf("修复 %s/%s 失败: %v", w.meta.Bucket, w.meta.Key, err)
		}
	}
}

// repairOne 为单个对象选择目标盘并执行重建。
func (m *Manager) repairOne(om meta.ObjectMeta, scan storage.ScanResult) error {
	bad := append(append([]int{}, scan.Missing...), scan.Corrupt...)
	if len(bad) == 0 {
		return nil
	}
	// 同对象已有重建任务（运行中或排队）则跳过，避免并发重建互相截断
	m.mu.Lock()
	if m.inflight[om.ID] {
		m.mu.Unlock()
		return nil
	}
	m.inflight[om.ID] = true
	m.mu.Unlock()

	// 选目标盘：排除对象仍健康占用的盘。
	// originalDisk 记录每个故障分片原本所在的盘（换盘后该盘在线但分片已丢失）。
	exclude := map[string]bool{}
	originalDisk := map[int]string{}
	badSet := map[int]bool{}
	for _, idx := range bad {
		badSet[idx] = true
	}
	for _, p := range om.Placement {
		if badSet[p.ShardIndex] {
			originalDisk[p.ShardIndex] = p.DiskID
		} else {
			exclude[p.DiskID] = true
		}
	}
	targets := map[int]string{}
	for _, idx := range bad {
		d, err := m.pickDiskForShard(idx, originalDisk[idx], exclude)
		if err != nil {
			return fmt.Errorf("对象 %s 选择重建目标盘失败: %w", om.Key, err)
		}
		targets[idx] = d.ID()
		exclude[d.ID()] = true
	}

	task := &meta.RepairTask{
		ID:            fmt.Sprintf("repair-%d-%s", time.Now().UnixNano(), om.ID[:8]),
		Bucket:        om.Bucket,
		Key:           om.Key,
		ObjectID:      om.ID,
		MissingShards: append([]int{}, scan.Missing...),
		CorruptShards: append([]int{}, scan.Corrupt...),
		Status:        meta.RepairQueued,
		TotalBytes:    om.Size,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}
	if err := m.store.Meta().SaveRepairTask(task); err != nil {
		return err
	}
	return m.runTask(task, targets)
}

// pickDisk 选择一块在线、未被排除且剩余空间最多的盘。
func (m *Manager) pickDisk(exclude map[string]bool) (*disk.Disk, error) {
	var best *disk.Disk
	var bestFree uint64
	for _, d := range m.store.DiskManager().Online() {
		if exclude[d.ID()] {
			continue
		}
		st := d.Stats()
		if best == nil || st.FreeBytes > bestFree {
			best, bestFree = d, st.FreeBytes
		}
	}
	if best == nil {
		return nil, fmt.Errorf("没有可用的在线磁盘")
	}
	return best, nil
}

// pickDiskForShard 为某个故障分片选目标盘：
// 优先回到它原本所在的盘（典型换盘场景：原盘已清空并重新上线），
// 前提是该盘在线、未被本对象其他健康分片占用；否则退化为剩余空间最多的盘。
func (m *Manager) pickDiskForShard(shardIdx int, originalDiskID string, exclude map[string]bool) (*disk.Disk, error) {
	if originalDiskID != "" && !exclude[originalDiskID] {
		if d, ok := m.store.DiskManager().Get(originalDiskID); ok && d.Status() == disk.StatusOnline {
			return d, nil
		}
	}
	return m.pickDisk(exclude)
}

func (m *Manager) runTask(task *meta.RepairTask, targets map[int]string) error {
	ctx, cancel := context.WithCancel(context.Background())
	m.mu.Lock()
	m.running[task.ID] = cancel
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		delete(m.running, task.ID)
		delete(m.paused, task.ID)
		delete(m.inflight, task.ObjectID)
		m.mu.Unlock()
		cancel()
	}()

	task.Status = meta.RepairRunning
	task.UpdatedAt = time.Now()
	if err := m.store.Meta().SaveRepairTask(task); err != nil {
		return err
	}
	lim := &pauseLimiter{parent: m.limiter, mgr: m, taskID: task.ID}
	// 必须加载完整对象元数据（含 Placement），否则无法定位健康分片
	om, err := m.store.Meta().GetObjectMeta(task.Bucket, task.Key)
	if err != nil {
		task.Status = meta.RepairFailed
		task.Error = err.Error()
		_ = m.store.Meta().SaveRepairTask(task)
		return err
	}
	if om.ID != task.ObjectID {
		return fmt.Errorf("对象 ID 与任务记录不一致")
	}
	err = m.store.RebuildObject(ctx, om, targets, lim, func(written int64) {
		task.ProgressBytes = written
		_ = m.store.Meta().SaveRepairTask(task)
	})
	now := time.Now()
	task.UpdatedAt = now
	if err != nil {
		task.Status = meta.RepairFailed
		task.Error = err.Error()
	} else {
		task.Status = meta.RepairDone
		task.ProgressBytes = task.TotalBytes
		t := now
		task.FinishedAt = &t
	}
	return m.store.Meta().SaveRepairTask(task)
}

// RunManual 手动对指定对象触发一次修复（控制台调用）。
func (m *Manager) RunManual(bucket, key string) (*meta.RepairTask, error) {
	om, err := m.store.Meta().GetObjectMeta(bucket, key)
	if err != nil {
		return nil, err
	}
	scan, err := m.store.ScanObject(om)
	if err != nil {
		return nil, err
	}
	if scan.Healthy() {
		return nil, fmt.Errorf("对象 %s/%s 分片健康，无需修复", bucket, key)
	}
	m.mu.Lock()
	if m.inflight[om.ID] {
		m.mu.Unlock()
		return nil, fmt.Errorf("对象 %s/%s 已有修复任务在运行", bucket, key)
	}
	m.inflight[om.ID] = true
	m.mu.Unlock()
	bad := append(append([]int{}, scan.Missing...), scan.Corrupt...)
	exclude := map[string]bool{}
	badSet := map[int]bool{}
	for _, idx := range bad {
		badSet[idx] = true
	}
	for _, p := range om.Placement {
		if !badSet[p.ShardIndex] {
			exclude[p.DiskID] = true
		}
	}
	targets := map[int]string{}
	for _, idx := range bad {
		d, err := m.pickDisk(exclude)
		if err != nil {
			return nil, err
		}
		targets[idx] = d.ID()
		exclude[d.ID()] = true
	}
	task := &meta.RepairTask{
		ID:     fmt.Sprintf("repair-manual-%d", time.Now().UnixNano()),
		Bucket: bucket, Key: key, ObjectID: om.ID,
		MissingShards: append([]int{}, scan.Missing...),
		CorruptShards: append([]int{}, scan.Corrupt...),
		Status:        meta.RepairQueued, TotalBytes: om.Size,
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	if err := m.store.Meta().SaveRepairTask(task); err != nil {
		return nil, err
	}
	go func() { _ = m.runTask(task, targets) }()
	return task, nil
}

// CancelTask 取消正在运行的任务。
func (m *Manager) CancelTask(taskID string) error {
	m.mu.Lock()
	cancel, ok := m.running[taskID]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("任务 %s 未在运行", taskID)
	}
	cancel()
	return nil
}
