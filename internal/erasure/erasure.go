// Package erasure 实现 Reed-Solomon 纠删码的流式编解码以及分片文件格式。
//
// 分片文件布局：
//
//	Header（16 字节定长）
//	+ Frame 0 (8 字节帧头 + payload，payload 末尾按 64 字节对齐补零)
//	+ Frame 1 ...
//
// 每个 Frame 带独立 CRC32C，用于检测比特翻转（bitrot）。
package erasure

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/klauspost/reedsolomon"
)

// Magic 标识分片文件。
const Magic uint32 = 0x455331 // "ERS1"

// HeaderSize 分片文件头长度。
const HeaderSize = 16

// Header 是分片文件头。
type Header struct {
	Magic        uint32
	Version      uint16
	DataShards   uint16
	ParityShards uint16
	ShardIndex   uint16 // 本分片在 RS 组中的序号 [0, data+parity)
	Reserved     uint32
}

// Frame 表示一个 stripe 帧。
type Frame struct {
	RealLen uint32 // 该 stripe 的逻辑数据长度（未补齐前）
	Payload []byte // 长度 = 补齐后的 shard 长度
}

// AlignSize payload 按 64 字节对齐（reedsolomon 要求 shard 长度可被 64 整除）。
const AlignSize = 64

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// CRC32C 计算 Castagnoli CRC。
func CRC32C(b []byte) uint32 { return crc32.Checksum(b, crcTable) }

// AlignUp 将长度补齐到 64 的倍数。
func AlignUp(n int) int {
	if n <= 0 {
		return AlignSize
	}
	if n%AlignSize == 0 {
		return n
	}
	return n + AlignSize - n%AlignSize
}

// EncodeHeader 序列化文件头。
func EncodeHeader(h Header) []byte {
	buf := make([]byte, HeaderSize)
	binary.BigEndian.PutUint32(buf[0:4], h.Magic)
	binary.BigEndian.PutUint16(buf[4:6], h.Version)
	binary.BigEndian.PutUint16(buf[6:8], h.DataShards)
	binary.BigEndian.PutUint16(buf[8:10], h.ParityShards)
	binary.BigEndian.PutUint16(buf[10:12], h.ShardIndex)
	binary.BigEndian.PutUint32(buf[12:16], h.Reserved)
	return buf
}

// DecodeHeader 解析文件头。
func DecodeHeader(b []byte) (Header, error) {
	if len(b) < HeaderSize {
		return Header{}, errors.New("分片文件过短，缺少文件头")
	}
	h := Header{
		Magic:        binary.BigEndian.Uint32(b[0:4]),
		Version:      binary.BigEndian.Uint16(b[4:6]),
		DataShards:   binary.BigEndian.Uint16(b[6:8]),
		ParityShards: binary.BigEndian.Uint16(b[8:10]),
		ShardIndex:   binary.BigEndian.Uint16(b[10:12]),
		Reserved:     binary.BigEndian.Uint32(b[12:16]),
	}
	if h.Magic != Magic {
		return Header{}, fmt.Errorf("分片魔数不匹配: %#x", h.Magic)
	}
	return h, nil
}

// EncodeFrame 序列化一个 stripe 帧（帧头 + payload）。
func EncodeFrame(realLen uint32, payload []byte) []byte {
	out := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(out[0:4], realLen)
	binary.BigEndian.PutUint32(out[4:8], CRC32C(payload))
	copy(out[8:], payload)
	return out
}

// CRCError 表示分片帧 CRC 校验失败（疑似 bitrot）。
type CRCError struct {
	Want uint32
	Got  uint32
}

func (e *CRCError) Error() string {
	return fmt.Sprintf("CRC32C 校验失败: want=%#x got=%#x", e.Want, e.Got)
}

// IsCRCError 判断是否为 CRC 错误。
func IsCRCError(err error) bool {
	var c *CRCError
	return errors.As(err, &c)
}

// Sink 接收一个对象的所有分片输出（按 stripe 流式写入）。
type Sink interface {
	Open(h Header) error
	// WriteShard 写入一个 stripe 的某个分片。
	WriteShard(stripe int, shardIndex int, payload []byte, realLen uint32) error
	Close() error
}

// EncodeStream 从 r 读取数据，按 stripe 做 RS 编码并输出到 sink。
// 返回对象逻辑大小。
func EncodeStream(r io.Reader, data, parity, stripeSize int, sink Sink) (int64, error) {
	enc, err := reedsolomon.New(data, parity)
	if err != nil {
		return 0, fmt.Errorf("创建 RS 编码器失败: %w", err)
	}
	if err := sink.Open(Header{
		Magic:        Magic,
		Version:      1,
		DataShards:   uint16(data),
		ParityShards: uint16(parity),
	}); err != nil {
		return 0, err
	}

	total := data + parity
	stripeBuf := make([]byte, stripeSize)
	var objectSize int64
	stripe := 0

	emit := func(logicalN int) error {
		// reedsolomon 要求每个数据分片等长且可被 64 整除。
		shardLen := AlignUp((logicalN + data - 1) / data)
		logical := make([]byte, data*shardLen)
		copy(logical, stripeBuf[:logicalN])
		shards := make([][]byte, total)
		for i := 0; i < data; i++ {
			shards[i] = logical[i*shardLen : (i+1)*shardLen]
		}
		for i := data; i < total; i++ {
			shards[i] = make([]byte, shardLen)
		}
		if err := enc.Encode(shards); err != nil {
			return fmt.Errorf("RS 编码失败: %w", err)
		}
		for i := 0; i < total; i++ {
			if err := sink.WriteShard(stripe, i, shards[i], uint32(logicalN)); err != nil {
				return err
			}
		}
		stripe++
		return nil
	}

	for {
		n, readErr := io.ReadFull(r, stripeBuf)
		if readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
			_ = sink.Close()
			return 0, fmt.Errorf("读取输入失败: %w", readErr)
		}
		if n > 0 {
			objectSize += int64(n)
			if err := emit(n); err != nil {
				_ = sink.Close()
				return 0, err
			}
		}
		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
			break
		}
	}

	if stripe == 0 { // 空对象也产出一个空 stripe，保证分片文件存在
		if err := emit(0); err != nil {
			_ = sink.Close()
			return 0, err
		}
	}
	if err := sink.Close(); err != nil {
		return 0, err
	}
	return objectSize, nil
}

// ShardReader 提供某个分片的顺序读取能力。
type ShardReader interface {
	io.Reader
	io.Closer
}

// Source 为解码提供各分片输入。GetShardReader 返回 nil,nil 表示该分片缺失。
type Source interface {
	GetShardReader(shardIndex int) (ShardReader, error)
}

// stripeReader 包装单个分片 reader，逐帧读取。
type stripeReader struct {
	r        io.Reader
	data     int
	hdr      Header
	finished bool
}

// next 读取下一帧；io.EOF 表示正常结束。
func (s *stripeReader) next() (Frame, error) {
	var head [8]byte
	if _, err := io.ReadFull(s.r, head[:]); err != nil {
		return Frame{}, err
	}
	realLen := binary.BigEndian.Uint32(head[0:4])
	wantCRC := binary.BigEndian.Uint32(head[4:8])
	payloadSize := AlignUp((int(realLen) + s.data - 1) / s.data)
	payload := make([]byte, payloadSize)
	if _, err := io.ReadFull(s.r, payload); err != nil {
		return Frame{}, fmt.Errorf("读取帧 payload 失败: %w", err)
	}
	if got := CRC32C(payload); got != wantCRC {
		return Frame{}, &CRCError{Want: wantCRC, Got: got}
	}
	return Frame{RealLen: realLen, Payload: payload}, nil
}

// DecodeStream 从 source 读取分片、RS 解码并把对象逻辑数据写入 w。
// expectSize 为对象逻辑大小，用于截断尾部补零。
func DecodeStream(w io.Writer, data, parity int, source Source, expectSize int64) error {
	enc, err := reedsolomon.New(data, parity)
	if err != nil {
		return err
	}
	total := data + parity

	srs := make([]*stripeReader, total)
	closers := make([]io.Closer, 0, total)
	available := 0
	for i := 0; i < total; i++ {
		r, err := source.GetShardReader(i)
		if err != nil {
			return fmt.Errorf("打开分片 %d 失败: %w", i, err)
		}
		if r == nil {
			continue
		}
		var hb [HeaderSize]byte
		if _, err := io.ReadFull(r, hb[:]); err != nil {
			return fmt.Errorf("读取分片 %d 头失败: %w", i, err)
		}
		h, err := DecodeHeader(hb[:])
		if err != nil {
			return fmt.Errorf("分片 %d 头损坏: %w", i, err)
		}
		srs[i] = &stripeReader{r: r, data: data, hdr: h}
		closers = append(closers, r)
		available++
	}
	defer func() {
		for _, c := range closers {
			_ = c.Close()
		}
	}()
	if available < data {
		return fmt.Errorf("%w: 可用分片 %d < 数据分片 %d", ErrNotEnoughShards, available, data)
	}

	var written int64
	stripe := 0
	for {
		shards := make([][]byte, total)
		payloadSize := 0
		good := 0
		for i, sr := range srs {
			if sr == nil {
				continue
			}
			f, err := sr.next()
			if err != nil {
				if err == io.EOF {
					srs[i] = nil // 该分片已读完
					continue
				}
				// CRC 损坏：把该分片视为缺失，交给 RS 重建
				if IsCRCError(err) {
					srs[i] = nil
					continue
				}
				return fmt.Errorf("分片 %d stripe %d 读取失败: %w", i, stripe, err)
			}
			if payloadSize == 0 {
				payloadSize = len(f.Payload)
			} else if len(f.Payload) != payloadSize {
				return fmt.Errorf("分片 %d stripe %d payload 长度不一致", i, stripe)
			}
			shards[i] = f.Payload
			good++
		}
		if good == 0 {
			break // 所有分片都到 EOF
		}
		if good < data {
			return fmt.Errorf("%w: stripe %d 健康分片 %d < %d", ErrNotEnoughShards, stripe, good, data)
		}
		// 缺失位保持 nil，由 Reconstruct 自动分配并重建（含校验分片）。
		if err := enc.Reconstruct(shards); err != nil {
			return fmt.Errorf("stripe %d RS 重建失败: %w", stripe, err)
		}

		logical := make([]byte, data*payloadSize)
		for i := 0; i < data; i++ {
			copy(logical[i*payloadSize:(i+1)*payloadSize], shards[i])
		}
		remaining := expectSize - written
		chunk := int64(len(logical))
		if remaining < chunk {
			chunk = remaining
		}
		if _, err := w.Write(logical[:chunk]); err != nil {
			return err
		}
		written += chunk
		stripe++
		if written >= expectSize {
			break
		}
	}
	if written != expectSize {
		return fmt.Errorf("解码大小不匹配: got=%d want=%d", written, expectSize)
	}
	return nil
}

// ErrNotEnoughShards 可用分片不足以重建数据。
var ErrNotEnoughShards = errors.New("分片数量不足，无法重建")
