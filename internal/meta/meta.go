// Package meta 基于 bbolt 的对象元数据索引。
//
// 顶层 bucket 布局：
//   - buckets:  bucket 名 -> BucketInfo
//   - obj:<b>: 对象 key -> ObjectMeta（每个存储桶一个嵌套 bucket，前缀扫描即索引顺序）
//   - mpu:<b>: uploadID -> MultipartState
//   - repair:   taskID -> RepairTask
//   - lc:       bucket 名 -> LifecycleRule
//   - lchist:   自增序号 -> LifecycleEvent
//   - sys:      自增对象 ID 等系统计数
package meta

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
)

// ErrNotFound 元数据不存在。
var ErrNotFound = errors.New("元数据不存在")

// ShardPlacement 记录一个分片放在哪块盘。
type ShardPlacement struct {
	ShardIndex int    `json:"shard_index"`
	DiskID     string `json:"disk_id"`
}

// ObjectMeta 对象元数据。
type ObjectMeta struct {
	ID           string            `json:"id"` // 内部对象 ID，决定分片文件名
	Bucket       string            `json:"bucket"`
	Key          string            `json:"key"`
	Size         int64             `json:"size"`
	ETag         string            `json:"etag"`
	SHA256       string            `json:"sha256"`
	ContentType  string            `json:"content_type"`
	UserMeta     map[string]string `json:"user_meta"`
	DataShards   int               `json:"data_shards"`
	ParityShards int               `json:"parity_shards"`
	StripeSize   int64             `json:"stripe_size"`
	Placement    []ShardPlacement  `json:"placement"`
	CreatedAt    time.Time         `json:"created_at"`
	ModifiedAt   time.Time         `json:"modified_at"`
	RepairGen    int               `json:"repair_gen"` // 重建过多少次
}

// BucketInfo 存储桶信息。
type BucketInfo struct {
	Name        string    `json:"name"`
	CreatedAt   time.Time `json:"created_at"`
	ObjectCount int64     `json:"object_count"`
	TotalBytes  int64     `json:"total_bytes"`
}

// PartMeta 分片上传的一个 part。
type PartMeta struct {
	Number int    `json:"number"`
	Size   int64  `json:"size"`
	ETag   string `json:"etag"`
	SHA256 string `json:"sha256"`
	// 该 part 以临时对象形式编码落盘，复用对象分片结构
	TempObjectID string    `json:"temp_object_id"`
	CreatedAt    time.Time `json:"created_at"`
}

// MultipartState 一次分片上传会话。
type MultipartState struct {
	UploadID     string            `json:"upload_id"`
	Bucket       string            `json:"bucket"`
	Key          string            `json:"key"`
	ContentType  string            `json:"content_type"`
	UserMeta     map[string]string `json:"user_meta"`
	Parts        []PartMeta        `json:"parts"`
	DataShards   int               `json:"data_shards"`
	ParityShards int               `json:"parity_shards"`
	StripeSize   int64             `json:"stripe_size"`
	Placement    []ShardPlacement  `json:"placement"`
	CreatedAt    time.Time         `json:"created_at"`
}

// RepairTaskStatus 修复任务状态。
type RepairTaskStatus string

const (
	RepairQueued  RepairTaskStatus = "queued"
	RepairRunning RepairTaskStatus = "running"
	RepairPaused  RepairTaskStatus = "paused"
	RepairDone    RepairTaskStatus = "done"
	RepairFailed  RepairTaskStatus = "failed"
)

// RepairTask 修复任务。
type RepairTask struct {
	ID            string           `json:"id"`
	Bucket        string           `json:"bucket"`
	Key           string           `json:"key"`
	ObjectID      string           `json:"object_id"`
	MissingShards []int            `json:"missing_shards"`
	CorruptShards []int            `json:"corrupt_shards"`
	TargetDiskID  string           `json:"target_disk_id"`
	Status        RepairTaskStatus `json:"status"`
	TotalBytes    int64            `json:"total_bytes"`
	ProgressBytes int64            `json:"progress_bytes"`
	Error         string           `json:"error"`
	CreatedAt     time.Time        `json:"created_at"`
	UpdatedAt     time.Time        `json:"updated_at"`
	FinishedAt    *time.Time       `json:"finished_at,omitempty"`
}

// LifecycleRule 生命周期规则（每桶一条，前缀匹配）。
type LifecycleRule struct {
	Bucket              string    `json:"bucket"`
	Prefix              string    `json:"prefix"`
	ExpirationDays      int       `json:"expiration_days"`       // 过期对象 N 天后删除，0=不启用
	AbortIncompleteDays int       `json:"abort_incomplete_days"` // 未完成 MPU N 天后中止，0=不启用
	Enabled             bool      `json:"enabled"`
	UpdatedAt           time.Time `json:"updated_at"`
}

// LifecycleEvent 生命周期执行历史。
type LifecycleEvent struct {
	Time   time.Time `json:"time"`
	Bucket string    `json:"bucket"`
	Key    string    `json:"key"`
	Action string    `json:"action"` // Expiration / AbortIncompleteMultipartUpload
	Reason string    `json:"reason"`
}

// ListEntry 列举结果条目。
type ListEntry struct {
	Key            string
	ETag           string
	Size           int64
	ModifiedAt     time.Time
	ContentType    string
	IsCommonPrefix bool
}

// ListResult 列举结果。
type ListResult struct {
	Entries        []ListEntry
	CommonPrefixes []string
	NextToken      string // 不透明续页 token（空表示结束）
	IsTruncated    bool
}

// Store bbolt 封装。
type Store struct {
	db *bolt.DB
}

// Open 打开（必要时创建）元数据库。
func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("打开 bbolt 失败: %w", err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, b := range []string{"buckets", "repair", "lc", "lchist", "sys"} {
			if _, err := tx.CreateBucketIfNotExists([]byte(b)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

// Close 关闭数据库。
func (s *Store) Close() error { return s.db.Close() }

// DB 暴露底层句柄（高级事务场景使用）。
func (s *Store) DB() *bolt.DB { return s.db }

func objBucket(b string) []byte { return []byte("obj:" + b) }
func mpuBucket(b string) []byte { return []byte("mpu:" + b) }

// NextObjectID 生成全局唯一对象 ID（16 位十六进制）。
func (s *Store) NextObjectID() (string, error) {
	var id uint64
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("sys"))
		v, err := b.NextSequence()
		if err != nil {
			return err
		}
		id = v
		return nil
	})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%016x", id), nil
}

// ---- 存储桶 ----

// CreateBucket 创建存储桶。
func (s *Store) CreateBucket(name string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		roots := tx.Bucket([]byte("buckets"))
		if roots.Get([]byte(name)) != nil {
			return fmt.Errorf("存储桶已存在: %s", name)
		}
		info, err := json.Marshal(BucketInfo{Name: name, CreatedAt: time.Now()})
		if err != nil {
			return err
		}
		if err := roots.Put([]byte(name), info); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(objBucket(name)); err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists(mpuBucket(name))
		return err
	})
}

// BucketExists 判断桶是否存在。
func (s *Store) BucketExists(name string) (bool, error) {
	exists := false
	err := s.db.View(func(tx *bolt.Tx) error {
		exists = tx.Bucket([]byte("buckets")).Get([]byte(name)) != nil
		return nil
	})
	return exists, err
}

// DeleteBucket 删除空桶。
func (s *Store) DeleteBucket(name string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		ob := tx.Bucket(objBucket(name))
		if ob != nil && ob.Stats().KeyN > 0 {
			return fmt.Errorf("存储桶非空: %s", name)
		}
		if err := tx.DeleteBucket(objBucket(name)); err != nil && !errors.Is(err, bolt.ErrBucketNotFound) {
			return err
		}
		if err := tx.DeleteBucket(mpuBucket(name)); err != nil && !errors.Is(err, bolt.ErrBucketNotFound) {
			return err
		}
		return tx.Bucket([]byte("buckets")).Delete([]byte(name))
	})
}

// ListBuckets 列出全部桶。
func (s *Store) ListBuckets() ([]BucketInfo, error) {
	out := []BucketInfo{}
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("buckets")).ForEach(func(k, v []byte) error {
			var info BucketInfo
			if err := json.Unmarshal(v, &info); err != nil {
				return err
			}
			if ob := tx.Bucket(objBucket(info.Name)); ob != nil {
				info.ObjectCount = int64(ob.Stats().KeyN)
			}
			out = append(out, info)
			return nil
		})
	})
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, err
}

// updateBucketStats 在对象增删后刷新桶统计。
func updateBucketStats(tx *bolt.Tx, name string) error {
	roots := tx.Bucket([]byte("buckets"))
	raw := roots.Get([]byte(name))
	if raw == nil {
		return nil
	}
	var info BucketInfo
	if err := json.Unmarshal(raw, &info); err != nil {
		return err
	}
	ob := tx.Bucket(objBucket(name))
	info.ObjectCount = int64(ob.Stats().KeyN)
	var total int64
	if err := ob.ForEach(func(_, v []byte) error {
		var m ObjectMeta
		if err := json.Unmarshal(v, &m); err != nil {
			return err
		}
		total += m.Size
		return nil
	}); err != nil {
		return err
	}
	info.TotalBytes = total
	enc, err := json.Marshal(info)
	if err != nil {
		return err
	}
	return roots.Put([]byte(name), enc)
}

// ---- 对象 ----

// PutObjectMeta 写入对象元数据。
func (s *Store) PutObjectMeta(m *ObjectMeta) error {
	raw, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(objBucket(m.Bucket))
		if b == nil {
			return fmt.Errorf("%w: 存储桶 %s", ErrNotFound, m.Bucket)
		}
		if err := b.Put([]byte(m.Key), raw); err != nil {
			return err
		}
		return updateBucketStats(tx, m.Bucket)
	})
}

// GetObjectMeta 读取对象元数据。
func (s *Store) GetObjectMeta(bucket, key string) (*ObjectMeta, error) {
	var m ObjectMeta
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(objBucket(bucket))
		if b == nil {
			return fmt.Errorf("%w: 存储桶 %s", ErrNotFound, bucket)
		}
		raw := b.Get([]byte(key))
		if raw == nil {
			return fmt.Errorf("%w: %s/%s", ErrNotFound, bucket, key)
		}
		return json.Unmarshal(raw, &m)
	})
	if err != nil {
		return nil, err
	}
	return &m, nil
}

// DeleteObjectMeta 删除对象元数据，返回被删除的 meta（不存在返回 nil）。
func (s *Store) DeleteObjectMeta(bucket, key string) (*ObjectMeta, error) {
	var removed *ObjectMeta
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(objBucket(bucket))
		if b == nil {
			return fmt.Errorf("%w: 存储桶 %s", ErrNotFound, bucket)
		}
		raw := b.Get([]byte(key))
		if raw == nil {
			return nil
		}
		var m ObjectMeta
		if err := json.Unmarshal(raw, &m); err != nil {
			return err
		}
		if err := b.Delete([]byte(key)); err != nil {
			return err
		}
		if err := updateBucketStats(tx, bucket); err != nil {
			return err
		}
		removed = &m
		return nil
	})
	return removed, err
}

// AllObjectMetas 遍历全部桶的全部对象（供修复扫描使用）。
func (s *Store) AllObjectMetas(fn func(m ObjectMeta) error) error {
	return s.db.View(func(tx *bolt.Tx) error {
		roots := tx.Bucket([]byte("buckets"))
		var names []string
		if err := roots.ForEach(func(k, _ []byte) error {
			names = append(names, string(k))
			return nil
		}); err != nil {
			return err
		}
		for _, name := range names {
			b := tx.Bucket(objBucket(name))
			if b == nil {
				continue
			}
			if err := b.ForEach(func(_, v []byte) error {
				var m ObjectMeta
				if err := json.Unmarshal(v, &m); err != nil {
					return err
				}
				return fn(m)
			}); err != nil {
				return err
			}
		}
		return nil
	})
}

// ListObjects 按前缀分页列举。startToken 为上一页返回的 NextToken（不透明）。
// maxKeys<=0 时取 1000。
func (s *Store) ListObjects(bucket, prefix, delimiter, startToken string, maxKeys int) (*ListResult, error) {
	if maxKeys <= 0 {
		maxKeys = 1000
	}
	res := &ListResult{Entries: []ListEntry{}, CommonPrefixes: []string{}}
	commonSeen := map[string]bool{}

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(objBucket(bucket))
		if b == nil {
			return fmt.Errorf("%w: 存储桶 %s", ErrNotFound, bucket)
		}
		seek := prefix
		if startToken != "" {
			// token 是上一页最后一个 key 的 base64url，续扫从它之后开始
			if dec, err := decodeToken(startToken); err == nil {
				seek = dec + "\x00"
			}
		}
		cursor := b.Cursor()
		count := 0
		var lastKey string
		for k, v := cursor.Seek([]byte(seek)); k != nil && strings.HasPrefix(string(k), prefix); k, v = cursor.Next() {
			key := string(k)
			if delimiter != "" {
				rest := strings.TrimPrefix(key, prefix)
				if idx := strings.Index(rest, delimiter); idx >= 0 {
					cp := prefix + rest[:idx+len(delimiter)]
					if !commonSeen[cp] {
						commonSeen[cp] = true
						res.CommonPrefixes = append(res.CommonPrefixes, cp)
						count++
					}
					lastKey = key
					if count >= maxKeys {
						break
					}
					continue
				}
			}
			var m ObjectMeta
			if err := json.Unmarshal(v, &m); err != nil {
				return err
			}
			res.Entries = append(res.Entries, ListEntry{
				Key: key, ETag: m.ETag, Size: m.Size,
				ModifiedAt: m.ModifiedAt, ContentType: m.ContentType,
			})
			count++
			lastKey = key
			if count >= maxKeys {
				break
			}
		}
		// 判断是否还有更多
		if count >= maxKeys {
			if k, _ := cursor.Seek([]byte(lastKey + "\x00")); k != nil && strings.HasPrefix(string(k), prefix) {
				res.IsTruncated = true
				res.NextToken = encodeToken(lastKey)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(res.CommonPrefixes)
	return res, nil
}

// ---- 分片上传 ----

// SaveMPU 持久化 MPU 会话。
func (s *Store) SaveMPU(st *MultipartState) error {
	raw, err := json.Marshal(st)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(mpuBucket(st.Bucket))
		if b == nil {
			return fmt.Errorf("%w: 存储桶 %s", ErrNotFound, st.Bucket)
		}
		return b.Put([]byte(st.UploadID), raw)
	})
}

// GetMPU 读取 MPU 会话。
func (s *Store) GetMPU(bucket, uploadID string) (*MultipartState, error) {
	var st MultipartState
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(mpuBucket(bucket))
		if b == nil {
			return fmt.Errorf("%w: upload %s", ErrNotFound, uploadID)
		}
		raw := b.Get([]byte(uploadID))
		if raw == nil {
			return fmt.Errorf("%w: upload %s", ErrNotFound, uploadID)
		}
		return json.Unmarshal(raw, &st)
	})
	if err != nil {
		return nil, err
	}
	return &st, nil
}

// DeleteMPU 删除 MPU 会话。
func (s *Store) DeleteMPU(bucket, uploadID string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(mpuBucket(bucket))
		if b == nil {
			return nil
		}
		return b.Delete([]byte(uploadID))
	})
}

// MutateMPU 在单个 bbolt 事务内读取并修改 MPU 会话。
// bbolt 写事务串行执行，因此并发 PutPart 的读-改-写不会互相覆盖。
func (s *Store) MutateMPU(bucket, uploadID string, fn func(*MultipartState) error) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(mpuBucket(bucket))
		if b == nil {
			return fmt.Errorf("%w: upload %s", ErrNotFound, uploadID)
		}
		raw := b.Get([]byte(uploadID))
		if raw == nil {
			return fmt.Errorf("%w: upload %s", ErrNotFound, uploadID)
		}
		var st MultipartState
		if err := json.Unmarshal(raw, &st); err != nil {
			return err
		}
		if err := fn(&st); err != nil {
			return err
		}
		enc, err := json.Marshal(&st)
		if err != nil {
			return err
		}
		return b.Put([]byte(uploadID), enc)
	})
}

// ListMPU 列出桶内全部 MPU 会话。
func (s *Store) ListMPU(bucket string) ([]MultipartState, error) {
	out := []MultipartState{}
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(mpuBucket(bucket))
		if b == nil {
			return nil
		}
		return b.ForEach(func(_, v []byte) error {
			var st MultipartState
			if err := json.Unmarshal(v, &st); err != nil {
				return err
			}
			out = append(out, st)
			return nil
		})
	})
	return out, err
}

// ---- 修复任务 ----

// SaveRepairTask 保存修复任务。
func (s *Store) SaveRepairTask(t *RepairTask) error {
	raw, err := json.Marshal(t)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("repair")).Put([]byte(t.ID), raw)
	})
}

// GetRepairTask 读取修复任务。
func (s *Store) GetRepairTask(id string) (*RepairTask, error) {
	var t RepairTask
	err := s.db.View(func(tx *bolt.Tx) error {
		raw := tx.Bucket([]byte("repair")).Get([]byte(id))
		if raw == nil {
			return fmt.Errorf("%w: 修复任务 %s", ErrNotFound, id)
		}
		return json.Unmarshal(raw, &t)
	})
	if err != nil {
		return nil, err
	}
	return &t, nil
}

// ListRepairTasks 列出修复任务，可按状态过滤（空字符串=全部），最新优先。
func (s *Store) ListRepairTasks(status string, limit int) ([]RepairTask, error) {
	out := []RepairTask{}
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("repair")).ForEach(func(_, v []byte) error {
			var t RepairTask
			if err := json.Unmarshal(v, &t); err != nil {
				return err
			}
			if status == "" || string(t.Status) == status {
				out = append(out, t)
			}
			return nil
		})
	})
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, err
}

// ---- 生命周期 ----

// SetLifecycleRule 保存生命周期规则。
func (s *Store) SetLifecycleRule(rule *LifecycleRule) error {
	rule.UpdatedAt = time.Now()
	raw, err := json.Marshal(rule)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("lc")).Put([]byte(rule.Bucket), raw)
	})
}

// GetLifecycleRule 读取规则。
func (s *Store) GetLifecycleRule(bucket string) (*LifecycleRule, error) {
	var rule LifecycleRule
	err := s.db.View(func(tx *bolt.Tx) error {
		raw := tx.Bucket([]byte("lc")).Get([]byte(bucket))
		if raw == nil {
			return fmt.Errorf("%w: 生命周期规则 %s", ErrNotFound, bucket)
		}
		return json.Unmarshal(raw, &rule)
	})
	if err != nil {
		return nil, err
	}
	return &rule, nil
}

// DeleteLifecycleRule 删除规则。
func (s *Store) DeleteLifecycleRule(bucket string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("lc")).Delete([]byte(bucket))
	})
}

// ListLifecycleRules 列出全部规则。
func (s *Store) ListLifecycleRules() ([]LifecycleRule, error) {
	out := []LifecycleRule{}
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("lc")).ForEach(func(_, v []byte) error {
			var r LifecycleRule
			if err := json.Unmarshal(v, &r); err != nil {
				return err
			}
			out = append(out, r)
			return nil
		})
	})
	return out, err
}

// AppendLifecycleEvent 追加一条执行历史（保留最近 500 条）。
func (s *Store) AppendLifecycleEvent(e *LifecycleEvent) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("lchist"))
		id, _ := b.NextSequence()
		raw, err := json.Marshal(e)
		if err != nil {
			return err
		}
		if err := b.Put([]byte(fmt.Sprintf("%016x", id)), raw); err != nil {
			return err
		}
		// 裁剪历史
		keys := [][]byte{}
		if err := b.ForEach(func(k, _ []byte) error {
			keys = append(keys, append([]byte(nil), k...))
			return nil
		}); err != nil {
			return err
		}
		if len(keys) > 500 {
			for _, k := range keys[:len(keys)-500] {
				if err := b.Delete(k); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// ListLifecycleEvents 列出生命周期执行历史，最新优先。
func (s *Store) ListLifecycleEvents(limit int) ([]LifecycleEvent, error) {
	out := []LifecycleEvent{}
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("lchist")).ForEach(func(_, v []byte) error {
			var e LifecycleEvent
			if err := json.Unmarshal(v, &e); err != nil {
				return err
			}
			out = append(out, e)
			return nil
		})
	})
	sort.Slice(out, func(i, j int) bool { return out[i].Time.After(out[j].Time) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, err
}
