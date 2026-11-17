package store

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"

	"imstore/internal/index"
	"imstore/internal/shard"
)

// ObjectReader is a streaming, reconstruction-tolerant reader for one object.
// It implements io.Reader and lazily reconstructs stripes on first access.
type ObjectReader struct {
	s     *Store
	meta  *index.ObjectMeta
	parts []index.PartMeta // ordered segments of the object
	pos   int64            // absolute read position within object payload
	limit int64            // remaining bytes allowed by an active range; -1 = all

	// Reusable per-read state.
	curPart   int
	curStripe int
	stripeBuf []byte // decoded payload of the current stripe (data shards joined)
	stripeOff int64  // offset within stripeBuf already consumed
}

// OpenObject resolves metadata and prepares a reader. A missing object yields
// ErrObjectNotFound.
func (s *Store) OpenObject(bucket, key string) (*ObjectReader, error) {
	meta, err := s.idx.GetObject(bucket, key)
	if err != nil {
		if errors.Is(err, index.ErrNotFound) {
			return nil, fmt.Errorf("%w: %s/%s", ErrObjectNotFound, bucket, key)
		}
		return nil, err
	}
	return s.openMeta(meta), nil
}

func (s *Store) openMeta(meta *index.ObjectMeta) *ObjectReader {
	return &ObjectReader{
		s:         s,
		meta:      meta,
		parts:     meta.AllParts(),
		limit:     -1,
		curPart:   0,
		curStripe: 0,
	}
}

// Size returns the full object size.
func (r *ObjectReader) Size() int64 { return r.meta.Size }

// Meta returns the stored metadata record.
func (r *ObjectReader) Meta() *index.ObjectMeta { return r.meta }

// Seek sets the absolute read position within the object. It implements the
// standard io.Seeker contract (whence SeekStart only; other modes error).
func (r *ObjectReader) Seek(offset int64, whence int) (int64, error) {
	if whence != io.SeekStart {
		return r.pos, errors.New("store: only SeekStart is supported")
	}
	if offset < 0 || offset > r.meta.Size {
		return r.pos, fmt.Errorf("store: seek offset %d outside [0,%d]", offset, r.meta.Size)
	}
	r.pos = offset
	r.limit = -1
	r.curPart = 0
	r.curStripe = 0
	r.stripeBuf = nil
	r.stripeOff = 0
	return r.pos, nil
}

// SetRange constrains reads to [start, end) and seeks to start.
func (r *ObjectReader) SetRange(start, end int64) error {
	if start < 0 || end > r.meta.Size || start > end {
		return fmt.Errorf("store: invalid range [%d,%d) size %d", start, end, r.meta.Size)
	}
	if _, err := r.Seek(start, io.SeekStart); err != nil {
		return err
	}
	r.limit = end - start
	return nil
}

// Read implements io.Reader.
func (r *ObjectReader) Read(p []byte) (int, error) {
	if r.limit == 0 || r.pos >= r.meta.Size {
		return 0, io.EOF
	}
	if r.stripeBuf == nil || r.stripeOff >= int64(len(r.stripeBuf)) {
		if err := r.loadNextStripe(); err != nil {
			return 0, err
		}
	}
	n := copy(p, r.stripeBuf[r.stripeOff:])
	// Never read past the object's real size (last stripe is padded).
	if remaining := r.meta.Size - r.pos; int64(n) > remaining {
		n = int(remaining)
	}
	// Honor an active byte range.
	if r.limit >= 0 && int64(n) > r.limit {
		n = int(r.limit)
	}
	r.stripeOff += int64(n)
	r.pos += int64(n)
	if r.limit >= 0 {
		r.limit -= int64(n)
	}
	return n, nil
}

// locatePart returns the part index and the absolute object offset where that
// part begins, for a given object-level offset.
func (r *ObjectReader) locatePart(offset int64) (int, int64) {
	var start int64
	for i, p := range r.parts {
		if offset < start+p.Size || i == len(r.parts)-1 {
			return i, start
		}
		start += p.Size
	}
	return len(r.parts) - 1, start
}

// loadNextStripe locates the part/stripe for the current position, reads the
// available shards, reconstructs from parity when needed and joins the data
// shards into stripeBuf.
func (r *ObjectReader) loadNextStripe() error {
	partIdx, partStart := r.locatePart(r.pos)
	if partIdx < 0 || partIdx >= len(r.parts) {
		return io.EOF
	}
	part := r.parts[partIdx]
	within := r.pos - partStart
	stripePayload := r.s.shardSz * int64(r.s.dataN)
	stripeIdx := int(within / stripePayload)
	if stripeIdx >= part.StripeCount {
		stripeIdx = part.StripeCount - 1
	}

	// Read each shard slot; nil means missing, offline or checksum-corrupt.
	slots := make([][]byte, r.s.codec.TotalShards())
	good := 0
	for shardIdx, vid := range part.VolumeIDs {
		v, err := r.s.volumes.Get(vid)
		if err != nil {
			continue
		}
		v.EnterForeground()
		rd, err := v.OpenShard(part.ObjectID, shardIdx)
		v.LeaveForeground()
		if err != nil {
			continue // missing file / offline disk / bad header
		}
		buf, err := rd.ReadStripe(stripeIdx)
		rd.Close()
		if err != nil {
			// Bit rot: discard the piece and let parity heal this stripe.
			if errors.Is(err, shard.ErrChecksumMismatch) {
				v.RecordError(err)
			}
			continue
		}
		slots[shardIdx] = buf
		good++
	}

	if good < r.s.dataN {
		return fmt.Errorf("%w: object %s part %d stripe %d has %d/%d healthy shards",
			ErrTooManyMissing, r.meta.ObjectID, partIdx, stripeIdx, good, r.s.dataN)
	}

	if good < r.s.codec.TotalShards() {
		if err := r.s.codec.Reconstruct(slots); err != nil {
			return fmt.Errorf("store: reconstruct %s stripe %d: %w",
				r.meta.ObjectID, stripeIdx, err)
		}
	} else if ok, _ := r.s.codec.Verify(slots); !ok {
		if err := r.s.codec.Reconstruct(slots); err != nil {
			return err
		}
	}

	payload := r.s.codec.DataPayload(slots)
	partStripeStart := partStart + int64(stripeIdx)*stripePayload
	inside := r.pos - partStripeStart
	if inside < 0 {
		inside = 0
	}
	r.curPart = partIdx
	r.curStripe = stripeIdx
	r.stripeBuf = payload
	r.stripeOff = inside

	// A part's final stripe is zero-padded up to the full stripe size. Trim
	// the padding so consecutive multipart segments join without extra bytes.
	// The part's real payload ends at partStart+part.Size.
	if stripeIdx == part.StripeCount-1 {
		validInPart := part.Size - int64(stripeIdx)*stripePayload
		if validInPart < int64(len(r.stripeBuf)) {
			if validInPart < 0 {
				validInPart = 0
			}
			r.stripeBuf = r.stripeBuf[:validInPart]
		}
	}
	return nil
}

// VerifyHash streams the whole object and compares its SHA-256 with the value
// recorded at upload time. Used by the chaos test and by scrub.
func (s *Store) VerifyHash(bucket, key string) error {
	meta, err := s.idx.GetObject(bucket, key)
	if err != nil {
		if errors.Is(err, index.ErrNotFound) {
			return fmt.Errorf("%w: %s/%s", ErrObjectNotFound, bucket, key)
		}
		return err
	}
	r := s.openMeta(meta)
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return err
	}
	got := fmt.Sprintf("%x", h.Sum(nil))
	if got != meta.SHA256 {
		return fmt.Errorf("store: hash mismatch for %s/%s: recorded %s, computed %s",
			bucket, key, meta.SHA256, got)
	}
	return nil
}
