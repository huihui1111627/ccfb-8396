package erasure_test

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"erasurestore/internal/erasure"
)

// memSink 把分片收集到内存，用于编解码闭环测试。
type memSink struct {
	data   int
	parity int
	shards [][]byte // shardIndex -> 拼接后的 payload（每 stripe 一段）
	frames map[int][]erasure.Frame
}

func (m *memSink) Open(h erasure.Header) error {
	m.data = int(h.DataShards)
	m.parity = int(h.ParityShards)
	m.frames = map[int][]erasure.Frame{}
	return nil
}
func (m *memSink) WriteShard(_, idx int, payload []byte, realLen uint32) error {
	buf := make([]byte, len(payload))
	copy(buf, payload)
	m.frames[idx] = append(m.frames[idx], erasure.Frame{RealLen: realLen, Payload: buf})
	return nil
}
func (m *memSink) Close() error {
	total := m.data + m.parity
	m.shards = make([][]byte, total)
	for i := 0; i < total; i++ {
		for _, f := range m.frames[i] {
			m.shards[i] = append(m.shards[i], f.Payload...)
		}
	}
	return nil
}

type memSource struct {
	frames map[int][]erasure.Frame
	data   int
}

func (s *memSource) GetShardReader(idx int) (erasure.ShardReader, error) {
	fs, ok := s.frames[idx]
	if !ok {
		return nil, nil
	}
	var buf bytes.Buffer
	buf.Write(erasure.EncodeHeader(erasure.Header{Magic: erasure.Magic, DataShards: uint16(s.data)}))
	for _, f := range fs {
		buf.Write(erasure.EncodeFrame(f.RealLen, f.Payload))
	}
	return io.NopCloser(bytes.NewReader(buf.Bytes())), nil
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	for _, size := range []int{0, 1, 63, 64, 1024, 1 << 20, (1 << 20) + 123} {
		data, parity := 4, 2
		input := make([]byte, size)
		_, _ = rand.Read(input)

		sink := &memSink{}
		n, err := erasure.EncodeStream(bytes.NewReader(input), data, parity, 1<<20, sink)
		if err != nil {
			t.Fatalf("size=%d encode: %v", size, err)
		}
		if n != int64(size) {
			t.Fatalf("size=%d 编码计数=%d", size, n)
		}

		// 随机丢掉最多 parity 个分片
		drop := map[int]bool{0: true, 5: true}
		if size == 0 {
			drop = map[int]bool{2: true}
		}
		src := &memSource{data: data, frames: map[int][]erasure.Frame{}}
		for idx, fs := range sink.frames {
			if !drop[idx] {
				src.frames[idx] = fs
			}
		}
		var out bytes.Buffer
		if err := erasure.DecodeStream(&out, data, parity, src, int64(size)); err != nil {
			t.Fatalf("size=%d 丢片 %v 解码失败: %v", size, drop, err)
		}
		if !bytes.Equal(out.Bytes(), input) {
			t.Fatalf("size=%d 解码内容不一致", size)
		}
	}
}

func TestCRCDetectsBitflip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "f.ers")
	payload := []byte("hello erasure coding world")
	frame := erasure.EncodeFrame(uint32(len(payload)), payload)
	// 翻转 payload 中的一个比特
	frame[len(frame)-1] ^= 0x01
	if err := os.WriteFile(path, frame, 0o644); err != nil {
		t.Fatal(err)
	}
	f, _ := os.Open(path)
	defer f.Close()
	var head [8]byte
	_, _ = io.ReadFull(f, head[:])
	ps := erasure.AlignUp(len(payload))
	buf := make([]byte, ps)
	_, _ = io.ReadFull(f, buf)
	if got := erasure.CRC32C(buf); got == binaryU32(head[4:]) {
		t.Fatal("比特翻转后 CRC 竟然仍然匹配")
	}
}

func binaryU32(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

func TestNotEnoughShards(t *testing.T) {
	data, parity := 4, 2
	input := make([]byte, 4096)
	sink := &memSink{}
	if _, err := erasure.EncodeStream(bytes.NewReader(input), data, parity, 1<<20, sink); err != nil {
		t.Fatal(err)
	}
	// 只剩 3 个分片（< data=4）
	src := &memSource{data: data, frames: map[int][]erasure.Frame{}}
	for idx := 0; idx < 3; idx++ {
		src.frames[idx] = sink.frames[idx]
	}
	var out bytes.Buffer
	err := erasure.DecodeStream(&out, data, parity, src, int64(len(input)))
	if !errors.Is(err, erasure.ErrNotEnoughShards) {
		t.Fatalf("期望 ErrNotEnoughShards，得到 %v", err)
	}
}
