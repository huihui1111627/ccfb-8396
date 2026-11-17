package main

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"

	"erasurestore/internal/disk"
	"erasurestore/internal/meta"
	"erasurestore/internal/storage"
)

// chaos 故障注入器，直接操作存储引擎与磁盘目录。
type chaos struct {
	store *storage.Store
	rng   *rand.Rand
}

// deleteRandomShards 随机删除 n 个属于对象 m 的分片文件，返回受影响的 shardIndex。
func (c *chaos) deleteRandomShards(m *meta.ObjectMeta, n int) ([]int, error) {
	idxs := c.rng.Perm(len(m.Placement))
	if n > len(idxs) {
		n = len(idxs)
	}
	hit := make([]int, 0, n)
	for _, i := range idxs[:n] {
		p := m.Placement[i]
		d, ok := c.store.DiskManager().Get(p.DiskID)
		if !ok {
			continue
		}
		path := d.ShardPath(m.ID, p.ShardIndex)
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return hit, fmt.Errorf("删除分片失败 %s: %w", path, err)
		}
		hit = append(hit, p.ShardIndex)
	}
	return hit, nil
}

// flipRandomBits 在 n 个分片文件中随机翻转若干字节。
func (c *chaos) flipRandomBits(m *meta.ObjectMeta, n int) ([]int, error) {
	idxs := c.rng.Perm(len(m.Placement))
	if n > len(idxs) {
		n = len(idxs)
	}
	hit := make([]int, 0, n)
	for _, i := range idxs[:n] {
		p := m.Placement[i]
		d, ok := c.store.DiskManager().Get(p.DiskID)
		if !ok {
			continue
		}
		path := d.ShardPath(m.ID, p.ShardIndex)
		if err := flipBitsInFile(path, 1+c.rng.Intn(3), c.rng); err != nil {
			return hit, err
		}
		hit = append(hit, p.ShardIndex)
	}
	return hit, nil
}

// flipBitsInFile 在文件的随机位置（跳过文件头）翻转 bitFlips 个字节。
func flipBitsInFile(path string, bitFlips int, rng *rand.Rand) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("打开分片失败 %s: %w", path, err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	if fi.Size() <= 64 {
		return fmt.Errorf("分片过小: %s", path)
	}
	for i := 0; i < bitFlips; i++ {
		off := int64(64 + rng.Intn(int(fi.Size()-64)))
		buf := make([]byte, 1)
		if _, err := f.ReadAt(buf, off); err != nil {
			return err
		}
		buf[0] ^= 1 << uint(rng.Intn(8))
		if _, err := f.WriteAt(buf, off); err != nil {
			return err
		}
	}
	return nil
}

// unplugDisk 把一整块盘置为离线（模拟拔盘），返回盘 ID 与该盘上受影响对象。
func (c *chaos) unplugDisk(diskID string) error {
	d, ok := c.store.DiskManager().Get(diskID)
	if !ok {
		return fmt.Errorf("磁盘不存在: %s", diskID)
	}
	d.SetStatus(disk.StatusOffline)
	return nil
}

// replugDisk 把离线盘重新上线（模拟插回原盘）。
func (c *chaos) replugDisk(diskID string) error {
	d, ok := c.store.DiskManager().Get(diskID)
	if !ok {
		return fmt.Errorf("磁盘不存在: %s", diskID)
	}
	d.SetStatus(disk.StatusOnline)
	d.Probe()
	return nil
}

// replaceDisk 模拟换盘：把旧盘目录整个移走（数据消失），再建空目录并上线。
func (c *chaos) replaceDisk(diskID, quarantineDir string) error {
	d, ok := c.store.DiskManager().Get(diskID)
	if !ok {
		return fmt.Errorf("磁盘不存在: %s", diskID)
	}
	d.SetStatus(disk.StatusOffline)
	quarantine := filepath.Join(quarantineDir, diskID+"-old")
	if err := os.MkdirAll(quarantineDir, 0o755); err != nil {
		return err
	}
	if err := os.Rename(d.Path(), quarantine); err != nil {
		return fmt.Errorf("移走旧盘目录失败: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(d.Path(), "shards"), 0o755); err != nil {
		return err
	}
	d.SetStatus(disk.StatusOnline)
	d.Probe()
	return nil
}

// pickRandomDisk 随机选一块在线盘。
func (c *chaos) pickRandomDisk() string {
	online := c.store.DiskManager().Online()
	if len(online) == 0 {
		return ""
	}
	return online[c.rng.Intn(len(online))].ID()
}
