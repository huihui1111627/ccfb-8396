// Package shard defines the on-disk format of one erasure-coded shard file.
//
// Layout:
//
//	+------------------- header (variable length) -------------------+
//	| magic "IMSH" | version | flags | objIDLen | objID | idx | total |
//	| shardSize(uint32) | stripeCount(uint32) | dataSize(int64)       |
//	| createdUnixNano(int64)                                          |
//	+------------------- payload -------------------------------------+
//	| stripe 0 shard bytes | stripe 1 shard bytes | ...               |
//	+------------------- checksum table ------------------------------+
//	| sha256(stripe0 payload) | sha256(stripe1 payload) | ...         |
//	+-----------------------------------------------------------------+
//
// Every stripe's payload is exactly shardSize bytes (the last stripe is
// zero-padded after Reed-Solomon encoding). Checksums are per stripe so a
// single flipped byte is localised to one stripe and can be healed using
// parity rather than losing the whole object.
//
// StreamWriter supports uploads whose stripe count is unknown up front (the
// common HTTP PUT case): it writes a placeholder header, appends stripes as
// they are encoded and seeks back on Close to fill the real stripe count and
// checksum table.
package shard

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Magic and format version.
const (
	Magic   = "IMSH"
	Version = uint16(1)
	// SumSize is the per-stripe checksum length (SHA-256).
	SumSize = sha256.Size
)

// Sentinel errors.
var (
	ErrBadMagic         = errors.New("shard: bad magic")
	ErrBadVersion       = errors.New("shard: unsupported version")
	ErrChecksumMismatch = errors.New("shard: stripe checksum mismatch (bit rot)")
	ErrStripeOutOfRange = errors.New("shard: stripe index out of range")
)

// Header carries everything stored in a shard file header.
type Header struct {
	ObjectID    string
	ShardIndex  int
	TotalShards int
	ShardSize   int64
	StripeCount int
	DataSize    int64 // full object payload size across all data shards
	CreatedNano int64
}

// Field offsets inside the fixed header tail (the 24 bytes following the
// variable-length object id). The tail is packed without padding:
//
//	shardSize uint32 | stripeCount uint32 | dataSize uint64 | created uint64
const (
	offShardSize   = 0
	offStripeCount = 4
	offDataSize    = 8
	offCreated     = 16
	fixedTailLen   = 24
)

func headerLen(objID string) int {
	return 4 + // magic
		2 + // version
		2 + // flags
		1 + len(objID) + // object id
		1 + 1 + // shard index, total shards
		fixedTailLen
}

func encodeHeader(h Header) ([]byte, error) {
	if len(h.ObjectID) > 255 {
		return nil, errors.New("shard: object id too long")
	}
	buf := make([]byte, headerLen(h.ObjectID))
	pos := 0
	copy(buf[pos:], Magic)
	pos += 4
	binary.BigEndian.PutUint16(buf[pos:], Version)
	pos += 2
	binary.BigEndian.PutUint16(buf[pos:], 0) // flags
	pos += 2
	buf[pos] = byte(len(h.ObjectID))
	pos++
	copy(buf[pos:], h.ObjectID)
	pos += len(h.ObjectID)
	buf[pos] = byte(h.ShardIndex)
	pos++
	buf[pos] = byte(h.TotalShards)
	pos++
	binary.BigEndian.PutUint32(buf[pos:], uint32(h.ShardSize))
	pos += 4
	binary.BigEndian.PutUint32(buf[pos:], uint32(h.StripeCount))
	pos += 4
	binary.BigEndian.PutUint64(buf[pos:], uint64(h.DataSize))
	pos += 8
	binary.BigEndian.PutUint64(buf[pos:], uint64(h.CreatedNano))
	return buf, nil
}

func decodeHeader(buf []byte) (Header, int, error) {
	h := Header{}
	if len(buf) < 4 || string(buf[:4]) != Magic {
		return h, 0, ErrBadMagic
	}
	pos := 4
	ver := binary.BigEndian.Uint16(buf[pos:])
	if ver != Version {
		return h, 0, fmt.Errorf("%w: %d", ErrBadVersion, ver)
	}
	pos += 2 // version
	pos += 2 // flags
	idLen := int(buf[pos])
	pos++
	if len(buf) < pos+idLen+2+fixedTailLen {
		return h, 0, io.ErrUnexpectedEOF
	}
	h.ObjectID = string(buf[pos : pos+idLen])
	pos += idLen
	h.ShardIndex = int(buf[pos])
	pos++
	h.TotalShards = int(buf[pos])
	pos++
	h.ShardSize = int64(binary.BigEndian.Uint32(buf[pos:]))
	pos += 4
	h.StripeCount = int(binary.BigEndian.Uint32(buf[pos:]))
	pos += 4
	h.DataSize = int64(binary.BigEndian.Uint64(buf[pos:]))
	pos += 8
	h.CreatedNano = int64(binary.BigEndian.Uint64(buf[pos:]))
	pos += 8
	return h, pos, nil
}

// Writer writes one complete shard whose stripe count is known up front.
type Writer struct {
	f          *os.File
	h          Header
	payloadOff int64
	sums       []byte
	closed     bool
}

// Create opens a new shard file (truncating any existing one) and writes its
// header.
func Create(path string, h Header) (*Writer, error) {
	if h.StripeCount < 1 {
		return nil, errors.New("shard: stripe count must be >= 1")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("shard: mkdir: %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("shard: create %s: %w", path, err)
	}
	hb, err := encodeHeader(h)
	if err != nil {
		f.Close()
		return nil, err
	}
	if _, err := f.Write(hb); err != nil {
		f.Close()
		return nil, fmt.Errorf("shard: write header: %w", err)
	}
	return &Writer{
		f:          f,
		h:          h,
		payloadOff: int64(len(hb)),
		sums:       make([]byte, 0, h.StripeCount*SumSize),
	}, nil
}

// WriteStripe appends one stripe's shard bytes and records its checksum.
func (w *Writer) WriteStripe(b []byte) error {
	if int64(len(b)) != w.h.ShardSize {
		return fmt.Errorf("shard: stripe size %d, expected %d", len(b), w.h.ShardSize)
	}
	if len(w.sums)/SumSize >= w.h.StripeCount {
		return errors.New("shard: more stripes written than declared")
	}
	if _, err := w.f.Write(b); err != nil {
		return fmt.Errorf("shard: write payload: %w", err)
	}
	sum := sha256.Sum256(b)
	w.sums = append(w.sums, sum[:]...)
	return nil
}

// Close flushes the checksum table and closes the file.
func (w *Writer) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	if len(w.sums)/SumSize != w.h.StripeCount {
		w.f.Close()
		return fmt.Errorf("shard: wrote %d stripes, header declared %d",
			len(w.sums)/SumSize, w.h.StripeCount)
	}
	if _, err := w.f.Write(w.sums); err != nil {
		w.f.Close()
		return fmt.Errorf("shard: write checksums: %w", err)
	}
	if err := w.f.Sync(); err != nil {
		w.f.Close()
		return fmt.Errorf("shard: fsync: %w", err)
	}
	return w.f.Close()
}

// StreamWriter writes a shard stripe-by-stripe without knowing the final
// stripe count when the file is created.
type StreamWriter struct {
	f           *os.File
	h           Header
	payloadOff  int64
	tailOff     int64 // absolute offset of the fixed tail within the header
	sums        []byte
	stripeCount int
	closed      bool
	aborted     bool
}

// CreateStream opens a shard for streaming. stripeCount/dataSize in h are
// ignored and filled in on Close.
func CreateStream(path string, h Header) (*StreamWriter, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("shard: mkdir: %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("shard: create %s: %w", path, err)
	}
	h.StripeCount = 0
	h.DataSize = 0
	hb, err := encodeHeader(h)
	if err != nil {
		f.Close()
		return nil, err
	}
	if _, err := f.Write(hb); err != nil {
		f.Close()
		return nil, fmt.Errorf("shard: write header: %w", err)
	}
	return &StreamWriter{
		f:          f,
		h:          h,
		payloadOff: int64(len(hb)),
		tailOff:    int64(len(hb) - fixedTailLen),
		sums:       make([]byte, 0, 64*SumSize),
	}, nil
}

// WriteStripe appends one encoded stripe piece.
func (w *StreamWriter) WriteStripe(b []byte) error {
	if w.closed || w.aborted {
		return errors.New("shard: writer is closed")
	}
	if int64(len(b)) != w.h.ShardSize {
		return fmt.Errorf("shard: stripe size %d, expected %d", len(b), w.h.ShardSize)
	}
	if _, err := w.f.Write(b); err != nil {
		return fmt.Errorf("shard: write payload: %w", err)
	}
	sum := sha256.Sum256(b)
	w.sums = append(w.sums, sum[:]...)
	w.stripeCount++
	return nil
}

// Abort closes and removes the partial shard.
func (w *StreamWriter) Abort() {
	w.aborted = true
	name := w.f.Name()
	w.f.Close()
	_ = os.Remove(name)
}

// Close rewrites the header fields, appends the checksum table, fsyncs and
// closes. dataSize is the full object payload size (all data shards).
func (w *StreamWriter) Close(dataSize int64) error {
	if w.closed {
		return nil
	}
	w.closed = true
	if w.aborted {
		return w.f.Close()
	}
	if w.stripeCount == 0 {
		w.Abort()
		return errors.New("shard: no stripes written")
	}
	// Patch only the two fields unknown at creation time. Writing the full
	// tail would zero shardSize and created, which stay valid from Create.
	// Within the fixed tail the layout is: shardSize(0..3), stripeCount(4..7),
	// dataSize(8..15), created(16..23).
	stripeBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(stripeBuf, uint32(w.stripeCount))
	if _, err := w.f.WriteAt(stripeBuf, w.tailOff+4); err != nil {
		w.f.Close()
		return fmt.Errorf("shard: patch stripe count: %w", err)
	}
	sizeBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(sizeBuf, uint64(dataSize))
	if _, err := w.f.WriteAt(sizeBuf, w.tailOff+8); err != nil {
		w.f.Close()
		return fmt.Errorf("shard: patch data size: %w", err)
	}
	if _, err := w.f.Seek(0, io.SeekEnd); err != nil {
		w.f.Close()
		return err
	}
	if _, err := w.f.Write(w.sums); err != nil {
		w.f.Close()
		return fmt.Errorf("shard: write checksums: %w", err)
	}
	if err := w.f.Sync(); err != nil {
		w.f.Close()
		return fmt.Errorf("shard: fsync: %w", err)
	}
	return w.f.Close()
}

// Reader provides random access to the stripes of one shard file and verifies
// every stripe against its stored checksum.
type Reader struct {
	f          *os.File
	h          Header
	payloadOff int64
	sums       []byte
}

// Open parses a shard file header and loads its checksum table.
func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("shard: open %s: %w", path, err)
	}
	// A 512-byte prefix always covers the header (object ids are <= 255 bytes).
	prefix := make([]byte, 512)
	n, err := io.ReadFull(f, prefix)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		f.Close()
		return nil, err
	}
	h, hdrLen, err := decodeHeader(prefix[:n])
	if err != nil {
		f.Close()
		return nil, err
	}
	if h.StripeCount == 0 {
		f.Close()
		return nil, errors.New("shard: file has zero stripes (unfinalised upload)")
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	want := int64(hdrLen) + int64(h.StripeCount)*h.ShardSize + int64(h.StripeCount)*SumSize
	if info.Size() != want {
		f.Close()
		return nil, fmt.Errorf("shard: file size %d does not match header-derived size %d", info.Size(), want)
	}
	sums := make([]byte, h.StripeCount*SumSize)
	if _, err := f.ReadAt(sums, int64(hdrLen)+int64(h.StripeCount)*h.ShardSize); err != nil {
		f.Close()
		return nil, fmt.Errorf("shard: read checksum table: %w", err)
	}
	return &Reader{f: f, h: h, payloadOff: int64(hdrLen), sums: sums}, nil
}

// Header returns the parsed header.
func (r *Reader) Header() Header { return r.h }

// Close closes the underlying file.
func (r *Reader) Close() error { return r.f.Close() }

// ReadStripe returns one stripe's bytes after checksum verification.
func (r *Reader) ReadStripe(index int) ([]byte, error) {
	if index < 0 || index >= r.h.StripeCount {
		return nil, fmt.Errorf("%w: %d of %d", ErrStripeOutOfRange, index, r.h.StripeCount)
	}
	buf := make([]byte, r.h.ShardSize)
	off := r.payloadOff + int64(index)*r.h.ShardSize
	if _, err := r.f.ReadAt(buf, off); err != nil {
		return nil, fmt.Errorf("shard: read stripe %d: %w", index, err)
	}
	want := r.sums[index*SumSize : (index+1)*SumSize]
	got := sha256.Sum256(buf)
	if !bytes.Equal(got[:], want) {
		return nil, fmt.Errorf("%w: object %s shard %d stripe %d",
			ErrChecksumMismatch, r.h.ObjectID, r.h.ShardIndex, index)
	}
	return buf, nil
}

// Path joins a volume root with the on-disk location of one shard. Shards are
// spread over 256 prefix directories to keep directory fan-out bounded even
// with millions of objects.
func Path(root, objectID string, shardIndex int) string {
	prefix := objectID
	if len(prefix) > 2 {
		prefix = prefix[:2]
	}
	return filepath.Join(root, "shards", prefix, fmt.Sprintf("%s.%d.ims", objectID, shardIndex))
}
