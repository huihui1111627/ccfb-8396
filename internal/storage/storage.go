// Package storage 是对象存储引擎：纠删码流式写、降级读、分片重建与分片上传。
package storage

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"log"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/klauspost/reedsolomon"

	"erasurestore/internal/config"
	"erasurestore/internal/disk"
	"erasurestore/internal/erasure"
	"erasurestore/internal/meta"
	"erasurestore/internal/placement"
)

// ByteLimiter 修复限速抽象（Wait 消费 n 字节的配额）。
type ByteLimiter interface {
	Wait(n int) error
}

// PutOptions 上传参数。
type PutOptions struct {
	ContentType string
	UserMeta    map[string]string
}

// Store 存储引擎。
type Store struct {
	cfg     *config.Config
	dm      *disk.Manager
	planner *placement.Planner
	meta    *meta.Store

	rsOnce sync.Once
	rs     reedsolomon.Encoder
	rsErr  error
}

// New 创建引擎并完成磁盘/元数据初始化。
func New(cfg *config.Config) (*Store, error) {
	dm, err := disk.NewManager(cfg.Disks)
	if err != nil {
		return nil, err
	}
	ms, err := meta.Open(cfg.MetaDBPath)
	if err != nil {
		return nil, err
	}
	return &Store{
		cfg:     cfg,
		dm:      dm,
		planner: placement.NewPlanner(dm, cfg.StrictEnclosure),
		meta:    ms,
	}, nil
}

// DiskManager / Meta / Config 暴露内部组件。
func (s *Store) DiskManager() *disk.Manager { return s.dm }
func (s *Store) Meta() *meta.Store          { return s.meta }
func (s *Store) Config() *config.Config     { return s.cfg }

func (s *Store) encoder() (reedsolomon.Encoder, error) {
	s.rsOnce.Do(func() {
		s.rs, s.rsErr = reedsolomon.New(s.cfg.DataShards, s.cfg.ParityShards)
	})
	return s.rs, s.rsErr
}

func (s *Store) totalShards() int { return s.cfg.DataShards + s.cfg.ParityShards }

// planPlacement 做一次故障域放置。
func (s *Store) planPlacement(exclude map[string]bool) ([]meta.ShardPlacement, error) {
	disks, err := s.planner.Plan(s.totalShards(), exclude)
	if err != nil {
		return nil, err
	}
	out := make([]meta.ShardPlacement, len(disks))
	for i, d := range disks {
		out[i] = meta.ShardPlacement{ShardIndex: i, DiskID: d.ID()}
	}
	return out, nil
}

// hashReader 同时计算 MD5 与 SHA256。
type hashReader struct {
	r   io.Reader
	md5 hash.Hash
	sha hash.Hash
	n   int64
}

func (h *hashReader) Read(p []byte) (int, error) {
	n, err := h.r.Read(p)
	if n > 0 {
		h.md5.Write(p[:n])
		h.sha.Write(p[:n])
		h.n += int64(n)
	}
	return n, err
}

// PutObject 流式编码上传：边收边编码边落盘，对象大小不进内存。
func (s *Store) PutObject(ctx context.Context, bucket, key string, r io.Reader, opts PutOptions) (*meta.ObjectMeta, error) {
	objectID, err := s.meta.NextObjectID()
	if err != nil {
		return nil, err
	}
	pl, err := s.planPlacement(nil)
	if err != nil {
		return nil, err
	}

	hr := &hashReader{r: &ctxReadCloser{ctx: ctx, r: r}, md5: md5.New(), sha: sha256.New()}
	sink := newFileSink(s.dm, objectID, pl, s.cfg.DataShards, s.cfg.ParityShards)
	size, err := erasure.EncodeStream(hr, s.cfg.DataShards, s.cfg.ParityShards, int(s.cfg.StripeSize), sink)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	m := &meta.ObjectMeta{
		ID:           objectID,
		Bucket:       bucket,
		Key:          key,
		Size:         size,
		ETag:         hex.EncodeToString(hr.md5.Sum(nil)),
		SHA256:       hex.EncodeToString(hr.sha.Sum(nil)),
		ContentType:  opts.ContentType,
		UserMeta:     opts.UserMeta,
		DataShards:   s.cfg.DataShards,
		ParityShards: s.cfg.ParityShards,
		StripeSize:   s.cfg.StripeSize,
		Placement:    pl,
		CreatedAt:    now,
		ModifiedAt:   now,
	}
	if err := s.meta.PutObjectMeta(m); err != nil {
		_ = s.deleteShards(m)
		return nil, err
	}
	// 覆盖同名对象：清理旧分片
	if old, err := s.meta.GetObjectMeta(bucket, key); err == nil && old.ID != objectID {
		if err := s.deleteShards(old); err != nil {
			// 残留不影响新对象读取，仅记录
			log.Printf("WARN 清理旧对象分片失败 %s/%s: %v", bucket, key, err)
		}
	}
	return m, nil
}

// GetObject 降级读取对象并写入 w；分片缺失或 CRC 损坏时自动 RS 重建。
func (s *Store) GetObject(m *meta.ObjectMeta, w io.Writer) error {
	src := &fileSource{mgr: s.dm, objectID: m.ID, placement: m.Placement}
	return erasure.DecodeStream(w, m.DataShards, m.ParityShards, src, m.Size)
}

// VerifyObjectSHA256 读回整个对象并校验哈希，返回实际 sha256（hex）。
func (s *Store) VerifyObjectSHA256(m *meta.ObjectMeta) (string, error) {
	h := sha256.New()
	if err := s.GetObject(m, h); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// DeleteObject 删除对象元数据与全部分片。
func (s *Store) DeleteObject(bucket, key string) error {
	m, err := s.meta.DeleteObjectMeta(bucket, key)
	if err != nil {
		return err
	}
	if m == nil {
		return nil
	}
	return s.deleteShards(m)
}

func (s *Store) deleteShards(m *meta.ObjectMeta) error {
	var firstErr error
	for _, p := range m.Placement {
		d, ok := s.dm.Get(p.DiskID)
		if !ok {
			continue
		}
		path := d.ShardPath(m.ID, p.ShardIndex)
		if err := removeIfExists(path); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func removeIfExists(path string) error {
	err := osRemove(path)
	if errors.Is(err, errNotFound) {
		return nil
	}
	return err
}

// ---- 分片上传（Multipart Upload）----

// InitMPU 创建分片上传会话，返回 uploadID。
func (s *Store) InitMPU(bucket, key, contentType string, userMeta map[string]string) (string, error) {
	uploadID := fmt.Sprintf("mpu-%d-%s", time.Now().UnixNano(), randHex(8))
	pl, err := s.planPlacement(nil)
	if err != nil {
		return "", err
	}
	st := &meta.MultipartState{
		UploadID:     uploadID,
		Bucket:       bucket,
		Key:          key,
		ContentType:  contentType,
		UserMeta:     userMeta,
		DataShards:   s.cfg.DataShards,
		ParityShards: s.cfg.ParityShards,
		StripeSize:   s.cfg.StripeSize,
		Placement:    pl,
		CreatedAt:    time.Now(),
	}
	if err := s.meta.SaveMPU(st); err != nil {
		return "", err
	}
	return uploadID, nil
}

// PutPart 上传一个 part：part 本身也按纠删码流式落盘（临时对象）。
// 可被并发调用（SDK transfer manager 并行上传），MPU 状态变更在单个 bbolt 事务内完成。
func (s *Store) PutPart(ctx context.Context, bucket, uploadID string, number int, r io.Reader) (*meta.PartMeta, error) {
	st, err := s.meta.GetMPU(bucket, uploadID)
	if err != nil {
		return nil, err
	}
	placement := st.Placement
	tempID, err := s.meta.NextObjectID()
	if err != nil {
		return nil, err
	}
	hr := &hashReader{r: &ctxReadCloser{ctx: ctx, r: r}, md5: md5.New(), sha: sha256.New()}
	sink := newFileSink(s.dm, tempID, placement, st.DataShards, st.ParityShards)
	size, err := erasure.EncodeStream(hr, st.DataShards, st.ParityShards, int(st.StripeSize), sink)
	if err != nil {
		return nil, err
	}
	pm := meta.PartMeta{
		Number:       number,
		Size:         size,
		ETag:         hex.EncodeToString(hr.md5.Sum(nil)),
		SHA256:       hex.EncodeToString(hr.sha.Sum(nil)),
		TempObjectID: tempID,
		CreatedAt:    time.Now(),
	}
	// 事务内完成同号覆盖与追加，避免并发读-改-写互相覆盖
	var oldTempID string
	err = s.meta.MutateMPU(bucket, uploadID, func(cur *meta.MultipartState) error {
		for i, existing := range cur.Parts {
			if existing.Number == number {
				oldTempID = existing.TempObjectID
				cur.Parts = append(cur.Parts[:i], cur.Parts[i+1:]...)
				break
			}
		}
		cur.Parts = append(cur.Parts, pm)
		return nil
	})
	if err != nil {
		_ = s.deleteTempShards(placement, tempID)
		return nil, err
	}
	if oldTempID != "" {
		_ = s.deleteTempShards(placement, oldTempID)
	}
	return &pm, nil
}

// CompleteMPU 组装 parts 为最终对象。parts 按序号拼接后重新纠删编码。
func (s *Store) CompleteMPU(ctx context.Context, bucket, uploadID string, clientParts []int) (*meta.ObjectMeta, error) {
	st, err := s.meta.GetMPU(bucket, uploadID)
	if err != nil {
		return nil, err
	}
	byNum := map[int]meta.PartMeta{}
	for _, p := range st.Parts {
		byNum[p.Number] = p
	}
	numbers := clientParts
	if len(numbers) == 0 {
		numbers = make([]int, 0, len(st.Parts))
		for n := range byNum {
			numbers = append(numbers, n)
		}
	}
	sort.Ints(numbers)
	ordered := make([]meta.PartMeta, 0, len(numbers))
	concatMD5 := md5.New()
	for i, n := range numbers {
		p, ok := byNum[n]
		if !ok {
			return nil, fmt.Errorf("part %d 未上传", n)
		}
		if i > 0 && n <= numbers[i-1] {
			return nil, fmt.Errorf("part 序号必须严格递增")
		}
		ordered = append(ordered, p)
		raw, _ := hex.DecodeString(p.ETag)
		concatMD5.Write(raw)
	}
	if len(ordered) == 0 {
		return nil, errors.New("没有可组装的 part")
	}
	mpuETag := fmt.Sprintf("%s-%d", hex.EncodeToString(concatMD5.Sum(nil)), len(ordered))

	pr, pw := io.Pipe()
	go func() {
		var werr error
		for _, p := range ordered {
			temp := &meta.ObjectMeta{
				ID: p.TempObjectID, DataShards: st.DataShards, ParityShards: st.ParityShards,
				StripeSize: st.StripeSize, Size: p.Size, Placement: st.Placement,
			}
			src := &fileSource{mgr: s.dm, objectID: temp.ID, placement: st.Placement}
			if err := erasure.DecodeStream(pw, st.DataShards, st.ParityShards, src, p.Size); err != nil {
				werr = err
				break
			}
		}
		_ = pw.CloseWithError(werr)
	}()

	objectID, err := s.meta.NextObjectID()
	if err != nil {
		_ = pr.Close()
		return nil, err
	}
	pl, err := s.planPlacement(nil)
	if err != nil {
		_ = pr.Close()
		return nil, err
	}
	sha := sha256.New()
	tee := io.TeeReader(&ctxReadCloser{ctx: ctx, r: pr}, sha)
	sink := newFileSink(s.dm, objectID, pl, s.cfg.DataShards, s.cfg.ParityShards)
	size, err := erasure.EncodeStream(tee, s.cfg.DataShards, s.cfg.ParityShards, int(s.cfg.StripeSize), sink)
	_ = pr.Close()
	if err != nil {
		return nil, err
	}

	now := time.Now()
	m := &meta.ObjectMeta{
		ID:           objectID,
		Bucket:       bucket,
		Key:          st.Key,
		Size:         size,
		ETag:         mpuETag,
		SHA256:       hex.EncodeToString(sha.Sum(nil)),
		ContentType:  st.ContentType,
		UserMeta:     st.UserMeta,
		DataShards:   s.cfg.DataShards,
		ParityShards: s.cfg.ParityShards,
		StripeSize:   s.cfg.StripeSize,
		Placement:    pl,
		CreatedAt:    now,
		ModifiedAt:   now,
	}
	if err := s.meta.PutObjectMeta(m); err != nil {
		_ = s.deleteShards(m)
		return nil, err
	}
	// 清理临时 part 分片与会话
	for _, p := range ordered {
		_ = s.deleteTempShards(st.Placement, p.TempObjectID)
	}
	_ = s.meta.DeleteMPU(bucket, uploadID)
	return m, nil
}

// AbortMPU 中止分片上传并清理临时分片。
func (s *Store) AbortMPU(bucket, uploadID string) error {
	st, err := s.meta.GetMPU(bucket, uploadID)
	if err != nil {
		return err
	}
	for _, p := range st.Parts {
		if err := s.deleteTempShards(st.Placement, p.TempObjectID); err != nil {
			return err
		}
	}
	return s.meta.DeleteMPU(bucket, uploadID)
}

func (s *Store) deleteTempShards(pl []meta.ShardPlacement, tempID string) error {
	m := &meta.ObjectMeta{ID: tempID, Placement: pl}
	return s.deleteShards(m)
}

// ---- 巡检与重建 ----

// ScanResult 对象分片健康扫描结果。
type ScanResult struct {
	Missing []int
	Corrupt []int
}

// Healthy 数据是否完整（无缺失无损坏）。
func (r ScanResult) Healthy() bool { return len(r.Missing) == 0 && len(r.Corrupt) == 0 }

// ScanObject 检查对象每个分片：缺失（盘离线/文件不存在）与损坏（头或 CRC 错误）。
func (s *Store) ScanObject(m *meta.ObjectMeta) (ScanResult, error) {
	res := ScanResult{}
	for _, p := range m.Placement {
		d, ok := s.dm.Get(p.DiskID)
		if !ok || d.Status() != disk.StatusOnline {
			res.Missing = append(res.Missing, p.ShardIndex)
			continue
		}
		f, err := d.OpenShard(m.ID, p.ShardIndex)
		if err != nil {
			if errors.Is(err, disk.ErrDiskUnavailable) || osIsNotExist(err) {
				res.Missing = append(res.Missing, p.ShardIndex)
				continue
			}
			return res, err
		}
		fr, err := newFrameReader(f, m.DataShards)
		if err != nil {
			d.AddReadError()
			_ = f.Close()
			res.Corrupt = append(res.Corrupt, p.ShardIndex)
			continue
		}
		corrupt := false
		for {
			_, err := fr.next()
			if err == io.EOF {
				break
			}
			if err != nil {
				if erasure.IsCRCError(err) {
					d.AddReadError()
				}
				corrupt = true
				break
			}
		}
		fr.close()
		if corrupt {
			res.Corrupt = append(res.Corrupt, p.ShardIndex)
		}
	}
	return res, nil
}

// RebuildObject 重建 targets（shardIndex -> 新盘 ID）中的分片，限速由 limiter 控制。
// progress 在每 stripe 完成后回调已重建字节数。
func (s *Store) RebuildObject(ctx context.Context, m *meta.ObjectMeta, targets map[int]string, limiter ByteLimiter, progress func(written int64)) error {
	if len(targets) == 0 {
		return nil
	}
	enc, err := s.encoder()
	if err != nil {
		return err
	}

	// 打开所有健康分片的顺序 reader
	readers := map[int]*frameReader{}
	files := map[int]*os.File{}
	defer func() {
		for _, f := range files {
			_ = f.Close()
		}
	}()
	for _, p := range m.Placement {
		if _, isTarget := targets[p.ShardIndex]; isTarget {
			continue
		}
		d, ok := s.dm.Get(p.DiskID)
		if !ok || d.Status() != disk.StatusOnline {
			continue
		}
		f, err := d.OpenShard(m.ID, p.ShardIndex)
		if err != nil {
			continue // 缺失分片若不在 targets 中，重建会因分片不足而失败
		}
		fr, err := newFrameReader(f, m.DataShards)
		if err != nil {
			_ = f.Close()
			d.AddReadError()
			continue
		}
		readers[p.ShardIndex] = fr
		files[p.ShardIndex] = f
	}
	if len(readers) < m.DataShards {
		return fmt.Errorf("%w: 健康分片 %d < 数据分片 %d", erasure.ErrNotEnoughShards, len(readers), m.DataShards)
	}

	// 打开目标分片临时文件并写头。
	// 临时文件名带本次重建唯一后缀，避免同对象重复/并发重建时共享路径、O_TRUNC 互相截断。
	runSuffix := randHex(8)
	outFiles := map[int]*os.File{}
	outDisks := map[int]*disk.Disk{}
	outTmp := map[int]string{}
	for idx, diskID := range targets {
		d, ok := s.dm.Get(diskID)
		if !ok || d.Status() != disk.StatusOnline {
			return fmt.Errorf("目标盘 %s 不可用", diskID)
		}
		tmp := d.TempPath(fmt.Sprintf("%s.%d.%s.rebuild", m.ID, idx, runSuffix))
		if err := osMkdirAll(dirOf(tmp), 0o755); err != nil {
			return err
		}
		f, err := osOpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			d.AddWriteError()
			return err
		}
		hdr := erasure.Header{
			Magic: erasure.Magic, Version: 1,
			DataShards: uint16(m.DataShards), ParityShards: uint16(m.ParityShards),
			ShardIndex: uint16(idx),
		}
		if _, err := f.Write(erasure.EncodeHeader(hdr)); err != nil {
			_ = f.Close()
			return err
		}
		outFiles[idx] = f
		outDisks[idx] = d
		outTmp[idx] = tmp
	}
	closeOuts := func() {
		for _, f := range outFiles {
			_ = f.Close()
		}
	}

	var written int64
	stripe := 0
	// 每个 reader 独立维护是否已到 EOF：只有全部分片读完才结束，
	// 避免某个 reader 先返回 EOF 就提前终止、导致重建分片被截断。
	done := map[int]bool{}
	for len(done) < len(readers) {
		if err := ctx.Err(); err != nil {
			closeOuts()
			return err
		}
		shards := make([][]byte, s.totalShards())
		payloadSize := 0
		realLen := uint32(0)
		good := 0
		for idx, fr := range readers {
			if done[idx] {
				continue
			}
			frame, err := fr.next()
			if err == io.EOF {
				done[idx] = true
				continue
			}
			if err != nil {
				if erasure.IsCRCError(err) {
					// 仅本 stripe 该分片不可用；reader 已越过坏帧，下一 stripe 仍可读
					continue
				}
				closeOuts()
				return err
			}
			if payloadSize != 0 && len(frame.Payload) != payloadSize {
				closeOuts()
				return fmt.Errorf("stripe %d 分片 %d payload 长度不一致", stripe, idx)
			}
			shards[idx] = frame.Payload
			payloadSize = len(frame.Payload)
			realLen = frame.RealLen
			good++
		}
		if len(done) == len(readers) {
			break // 所有 reader 同时到达 EOF
		}
		// 部分 reader 已 EOF 但还有未读完的：说明分片 stripe 数不一致（文件被截断）
		if len(done) > 0 {
			closeOuts()
			return fmt.Errorf("%w: stripe %d 部分分片提前结束（%d/%d 已 EOF）",
				erasure.ErrNotEnoughShards, stripe, len(done), len(readers))
		}
		if good < m.DataShards {
			closeOuts()
			return fmt.Errorf("%w: stripe %d 健康分片不足（%d<%d）",
				erasure.ErrNotEnoughShards, stripe, good, m.DataShards)
		}
		// 缺失位保持 nil，由 Reconstruct 自动分配并重建（数据+校验分片都覆盖）。
		// 不能用 ReconstructData：它在数据分片齐全时会直接返回，不重建缺失的校验分片。
		if err := enc.Reconstruct(shards); err != nil {
			closeOuts()
			return fmt.Errorf("stripe %d 重建失败: %w", stripe, err)
		}
		for idx, f := range outFiles {
			if _, err := f.Write(erasure.EncodeFrame(realLen, shards[idx])); err != nil {
				outDisks[idx].AddWriteError()
				closeOuts()
				return err
			}
			if limiter != nil {
				if err := limiter.Wait(payloadSize); err != nil {
					closeOuts()
					return err
				}
			}
			written += int64(payloadSize)
		}
		if progress != nil {
			progress(written)
		}
		stripe++
	}

	// 落盘改名，更新 placement
	for idx, f := range outFiles {
		if err := f.Sync(); err != nil {
			closeOuts()
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
		d := outDisks[idx]
		tmp := outTmp[idx]
		final := d.ShardPath(m.ID, idx)
		if err := osMkdirAll(dirOf(final), 0o755); err != nil {
			return err
		}
		// 损坏分片可能在原盘残留，先删后改名
		_ = osRemove(final)
		if err := osRename(tmp, final); err != nil {
			d.AddWriteError()
			return err
		}
	}

	for idx, diskID := range targets {
		replaced := false
		for i, p := range m.Placement {
			if p.ShardIndex == idx {
				m.Placement[i].DiskID = diskID
				replaced = true
				break
			}
		}
		if !replaced {
			m.Placement = append(m.Placement, meta.ShardPlacement{ShardIndex: idx, DiskID: diskID})
		}
		sort.Slice(m.Placement, func(i, j int) bool { return m.Placement[i].ShardIndex < m.Placement[j].ShardIndex })
	}
	m.RepairGen++
	m.ModifiedAt = time.Now()
	return s.meta.PutObjectMeta(m)
}
