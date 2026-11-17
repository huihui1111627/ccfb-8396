// Package lifecycle 周期执行过期对象删除与未完成分片上传中止。
package lifecycle

import (
	"log"
	"strings"
	"time"

	"erasurestore/internal/meta"
	"erasurestore/internal/storage"
)

// Runner 生命周期执行器。
type Runner struct {
	store  *storage.Store
	stopCh chan struct{}
	done   chan struct{}
	// Now 可被测试覆盖。
	Now func() time.Time
}

// New 创建执行器。
func New(st *storage.Store) *Runner {
	return &Runner{store: st, stopCh: make(chan struct{}), done: make(chan struct{}), Now: time.Now}
}

// Start 每 6 小时执行一次。
func (r *Runner) Start() {
	go func() {
		defer close(r.done)
		ticker := time.NewTicker(6 * time.Hour)
		defer ticker.Stop()
		r.RunOnce()
		for {
			select {
			case <-r.stopCh:
				return
			case <-ticker.C:
				r.RunOnce()
			}
		}
	}()
}

// Stop 停止。
func (r *Runner) Stop() {
	close(r.stopCh)
	<-r.done
}

// RunOnce 立即执行一轮清理。
func (r *Runner) RunOnce() {
	rules, err := r.store.Meta().ListLifecycleRules()
	if err != nil {
		log.Printf("生命周期: 读取规则失败: %v", err)
		return
	}
	now := r.Now()
	for _, rule := range rules {
		if !rule.Enabled {
			continue
		}
		if rule.ExpirationDays > 0 {
			r.expireObjects(rule, now)
		}
		if rule.AbortIncompleteDays > 0 {
			r.abortMPUs(rule, now)
		}
	}
}

func (r *Runner) expireObjects(rule meta.LifecycleRule, now time.Time) {
	cutoff := now.Add(-time.Duration(rule.ExpirationDays) * 24 * time.Hour)
	err := r.store.Meta().AllObjectMetas(func(m meta.ObjectMeta) error {
		if m.Bucket != rule.Bucket || !strings.HasPrefix(m.Key, rule.Prefix) {
			return nil
		}
		if m.ModifiedAt.Before(cutoff) {
			if err := r.store.DeleteObject(m.Bucket, m.Key); err != nil {
				log.Printf("生命周期删除失败 %s/%s: %v", m.Bucket, m.Key, err)
				return nil
			}
			_ = r.store.Meta().AppendLifecycleEvent(&meta.LifecycleEvent{
				Time: now, Bucket: m.Bucket, Key: m.Key,
				Action: "Expiration", Reason: "对象超过保留天数",
			})
		}
		return nil
	})
	if err != nil {
		log.Printf("生命周期对象清理失败: %v", err)
	}
}

func (r *Runner) abortMPUs(rule meta.LifecycleRule, now time.Time) {
	cutoff := now.Add(-time.Duration(rule.AbortIncompleteDays) * 24 * time.Hour)
	states, err := r.store.Meta().ListMPU(rule.Bucket)
	if err != nil {
		return
	}
	for _, st := range states {
		if !strings.HasPrefix(st.Key, rule.Prefix) || !st.CreatedAt.Before(cutoff) {
			continue
		}
		if err := r.store.AbortMPU(st.Bucket, st.UploadID); err != nil {
			log.Printf("生命周期中止 MPU 失败 %s: %v", st.UploadID, err)
			continue
		}
		_ = r.store.Meta().AppendLifecycleEvent(&meta.LifecycleEvent{
			Time: now, Bucket: st.Bucket, Key: st.Key,
			Action: "AbortIncompleteMultipartUpload", Reason: "分片上传超时未完成",
		})
	}
}
