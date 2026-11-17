package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"

	"erasurestore/internal/disk"
	"erasurestore/internal/erasure"
	"erasurestore/internal/meta"
)

// fileSink 把一个对象的全部分片并行写到各自磁盘：先写临时文件，Close 时原子改名。
type fileSink struct {
	mgr       *disk.Manager
	objectID  string
	placement []meta.ShardPlacement
	data      int
	parity    int

	mu      sync.Mutex
	files   map[int]*os.File // shardIndex -> 临时文件
	closed  bool
	aborted bool
}

func newFileSink(mgr *disk.Manager, objectID string, placement []meta.ShardPlacement, data, parity int) *fileSink {
	return &fileSink{
		mgr:       mgr,
		objectID:  objectID,
		placement: placement,
		data:      data,
		parity:    parity,
		files:     map[int]*os.File{},
	}
}

func (s *fileSink) diskFor(shardIndex int) (*disk.Disk, error) {
	if shardIndex >= len(s.placement) {
		return nil, fmt.Errorf("分片序号越界: %d", shardIndex)
	}
	d, ok := s.mgr.Get(s.placement[shardIndex].DiskID)
	if !ok {
		return nil, fmt.Errorf("放置记录中的磁盘不存在: %s", s.placement[shardIndex].DiskID)
	}
	return d, nil
}

func (s *fileSink) Open(h erasure.Header) error {
	for idx := range s.placement {
		d, err := s.diskFor(idx)
		if err != nil {
			s.abort()
			return err
		}
		if d.Status() != disk.StatusOnline {
			s.abort()
			return fmt.Errorf("%w: %s", disk.ErrDiskUnavailable, d.ID())
		}
		hdr := h
		hdr.ShardIndex = uint16(idx)
		tmp := d.TempPath(fmt.Sprintf("%s.%d.tmp", s.objectID, idx))
		if err := os.MkdirAll(dirOf(tmp), 0o755); err != nil {
			d.AddWriteError()
			s.abort()
			return err
		}
		f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			d.AddWriteError()
			s.abort()
			return fmt.Errorf("磁盘 %s 创建临时分片失败: %w", d.ID(), err)
		}
		if _, err := f.Write(erasure.EncodeHeader(hdr)); err != nil {
			d.AddWriteError()
			_ = f.Close()
			s.abort()
			return err
		}
		s.files[idx] = f
	}
	return nil
}

func (s *fileSink) WriteShard(stripe, shardIndex int, payload []byte, realLen uint32) error {
	s.mu.Lock()
	f := s.files[shardIndex]
	s.mu.Unlock()
	if f == nil {
		return fmt.Errorf("分片 %d 的文件未打开", shardIndex)
	}
	frame := erasure.EncodeFrame(realLen, payload)
	if _, err := f.Write(frame); err != nil {
		d, _ := s.diskFor(shardIndex)
		if d != nil {
			d.AddWriteError()
		}
		s.abort()
		return fmt.Errorf("写分片 %d stripe %d 失败: %w", shardIndex, stripe, err)
	}
	return nil
}

// Close flush 并把临时文件改名为正式分片。
func (s *fileSink) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	var firstErr error
	for idx, f := range s.files {
		d, err := s.diskFor(idx)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if err := f.Sync(); err != nil {
			d.AddWriteError()
			if firstErr == nil {
				firstErr = err
			}
		}
		if err := f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		tmp := d.TempPath(fmt.Sprintf("%s.%d.tmp", s.objectID, idx))
		final := d.ShardPath(s.objectID, idx)
		if err := os.MkdirAll(dirOf(final), 0o755); err != nil {
			d.AddWriteError()
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if err := os.Rename(tmp, final); err != nil {
			d.AddWriteError()
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	if firstErr != nil {
		s.abort()
	}
	return firstErr
}

// abort 清理所有临时文件。
func (s *fileSink) abort() {
	if s.aborted {
		return
	}
	s.aborted = true
	for idx, f := range s.files {
		_ = f.Close()
		if d, err := s.diskFor(idx); err == nil {
			_ = os.Remove(d.TempPath(fmt.Sprintf("%s.%d.tmp", s.objectID, idx)))
		}
	}
}

// fileSource 为解码提供各分片 reader；缺失/离线的分片返回 nil。
type fileSource struct {
	mgr       *disk.Manager
	objectID  string
	placement []meta.ShardPlacement
}

func (src *fileSource) GetShardReader(shardIndex int) (erasure.ShardReader, error) {
	if shardIndex >= len(src.placement) {
		return nil, nil
	}
	d, ok := src.mgr.Get(src.placement[shardIndex].DiskID)
	if !ok {
		return nil, nil
	}
	f, err := d.OpenShard(src.objectID, shardIndex)
	if err != nil {
		if os.IsNotExist(err) || err == disk.ErrDiskUnavailable {
			return nil, nil
		}
		return nil, err
	}
	return f, nil
}

// frameReader 顺序读取单个分片文件的帧（修复流程使用）。
type frameReader struct {
	f      *os.File
	data   int
	header erasure.Header
}

func newFrameReader(f *os.File, data int) (*frameReader, error) {
	var hb [erasure.HeaderSize]byte
	if _, err := io.ReadFull(f, hb[:]); err != nil {
		return nil, err
	}
	h, err := erasure.DecodeHeader(hb[:])
	if err != nil {
		return nil, err
	}
	return &frameReader{f: f, data: data, header: h}, nil
}

// next 读取下一帧；io.EOF 表示文件结束。
func (r *frameReader) next() (erasure.Frame, error) {
	var head [8]byte
	if _, err := io.ReadFull(r.f, head[:]); err != nil {
		return erasure.Frame{}, err
	}
	realLen := binary.BigEndian.Uint32(head[0:4])
	wantCRC := binary.BigEndian.Uint32(head[4:8])
	size := erasure.AlignUp((int(realLen) + r.data - 1) / r.data)
	payload := make([]byte, size)
	if _, err := io.ReadFull(r.f, payload); err != nil {
		return erasure.Frame{}, err
	}
	if got := erasure.CRC32C(payload); got != wantCRC {
		return erasure.Frame{}, &erasure.CRCError{Want: wantCRC, Got: got}
	}
	return erasure.Frame{RealLen: realLen, Payload: payload}, nil
}

func (r *frameReader) close() { _ = r.f.Close() }

func dirOf(p string) string {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' || p[i] == os.PathSeparator {
			return p[:i]
		}
	}
	return "."
}
