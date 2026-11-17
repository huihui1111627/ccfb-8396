// Package store orchestrates the full object lifecycle: streaming
// Reed-Solomon encoding across failure-domain-aware volumes, tolerant reads
// with on-the-fly reconstruction, multipart assembly and deletion.
//
// Memory model: a Put only buffers one stripe (DataShards*ShardSize bytes,
// default 1 MiB) plus per-shard file handles. Multi-gibibyte objects stream
// from the HTTP request body to disk without ever being materialised whole.
package store

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"imstore/internal/erasure"
	"imstore/internal/index"
	"imstore/internal/placement"
	"imstore/internal/shard"
	"imstore/internal/volume"
)

// Common errors.
var (
	ErrObjectNotFound = errors.New("store: object not found")
	ErrTooManyMissing = errors.New("store: too many shards missing/corrupt to read")
	ErrNoWritableDisk = errors.New("store: not enough writable fault domains")
)

// PartResult is returned after a (multipart) segment write.
type PartResult struct {
	ObjectID    string
	Size        int64
	ETag        string
	SHA256      string
	StripeCount int
	VolumeIDs   []string
}

// Store ties together codec, volumes, placement and metadata.
type Store struct {
	codec   *erasure.Codec
	volumes *volume.Manager
	idx     *index.Index
	planner *placement.Planner
	shardSz int64
	dataN   int
	parityN int

	// placementMu serialises placement snapshots so concurrent uploads do not
	// all choose the same least-loaded enclosure set based on stale stats.
	placementMu sync.Mutex
}

// New wires a Store from its collaborators.
func New(codec *erasure.Codec, vols *volume.Manager, idx *index.Index) *Store {
	return &Store{
		codec:   codec,
		volumes: vols,
		idx:     idx,
		planner: placement.NewPlanner(),
		shardSz: codec.ShardSize(),
		dataN:   codec.DataShards(),
		parityN: codec.ParityShards(),
	}
}

// Index exposes the metadata store (used by repair/lifecycle/console).
func (s *Store) Index() *index.Index { return s.idx }

// Volumes exposes the volume manager.
func (s *Store) Volumes() *volume.Manager { return s.volumes }

// Codec exposes the erasure codec.
func (s *Store) Codec() *erasure.Codec { return s.codec }

// NewObjectID generates a random hex object identifier.
func NewObjectID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("store: crypto/rand failed: %v", err))
	}
	return hex.EncodeToString(b[:])
}

// PutPart streams one independent object segment from r, encoding stripes as
// bytes arrive and writing each shard straight to its target volume.
//
// Degraded writes are supported: when one or more target volumes fail to open
// (disk pulled mid-upload) their writer slots stay nil, and as long as at
// least dataN shards land the part commits. The missing shards are recorded
// implicitly via VolumeIDs and healed by the repair worker.
func (s *Store) PutPart(ctx context.Context, objectID string, r io.Reader) (PartResult, error) {
	res := PartResult{ObjectID: objectID}

	s.placementMu.Lock()
	chosen, err := s.planner.Plan(s.volumes.Healthy(), s.codec.TotalShards())
	s.placementMu.Unlock()
	if err != nil {
		return res, err
	}
	res.VolumeIDs = append([]string(nil), chosen...)

	created := time.Now().UTC().UnixNano()
	writers := make([]*shard.StreamWriter, s.codec.TotalShards())
	usable := 0
	for i, vid := range chosen {
		v, verr := s.volumes.Get(vid)
		if verr != nil {
			continue
		}
		v.EnterForeground()
		w, werr := v.CreateStreamShard(shard.Header{
			ObjectID:    objectID,
			ShardIndex:  i,
			TotalShards: s.codec.TotalShards(),
			ShardSize:   s.shardSz,
			CreatedNano: created,
		})
		v.LeaveForeground()
		if werr != nil {
			continue
		}
		writers[i] = w
		usable++
	}
	if usable < s.dataN {
		s.abortWriters(writers)
		return res, fmt.Errorf("%w: only %d of %d target volumes writable (need %d)",
			ErrNoWritableDisk, usable, s.codec.TotalShards(), s.dataN)
	}

	hash := sha256.New()
	md5h := md5.New()
	tr := io.TeeReader(r, io.MultiWriter(hash, md5h))

	var size int64
	var stripeCount int
	failures := 0

	abortAll := func() {
		for i, w := range writers {
			if w != nil {
				w.Abort()
				if v, _ := s.volumes.Get(chosen[i]); v != nil {
					v.RecordError(errors.New("stream aborted"))
				}
			}
		}
	}

	for {
		if err := ctx.Err(); err != nil {
			abortAll()
			return res, err
		}
		pieces, dataLen, encErr := s.codec.Encode(tr)
		if encErr == io.EOF {
			break
		}
		if encErr != nil {
			abortAll()
			return res, encErr
		}
		size += dataLen
		stripeCount++
		for i, piece := range pieces {
			w := writers[i]
			if w == nil {
				failures++
				continue
			}
			if werr := w.WriteStripe(piece); werr != nil {
				// Disk vanished mid-stream: stop writing to it, keep going.
				w.Abort()
				writers[i] = nil
				failures++
				if v, _ := s.volumes.Get(chosen[i]); v != nil {
					v.RecordError(werr)
				}
			}
		}
		available := 0
		for _, w := range writers {
			if w != nil {
				available++
			}
		}
		if available < s.dataN {
			abortAll()
			return res, fmt.Errorf("%w: %d writable shards remain after stripe %d",
				ErrNoWritableDisk, available, stripeCount)
		}
	}

	if stripeCount == 0 {
		// Empty object: emit one all-zero stripe so shard files stay valid.
		empty := make([]byte, s.dataN*int(s.shardSz))
		pieces, _, encErr := s.codec.Encode(bytesReader(empty))
		if encErr != nil {
			abortAll()
			return res, encErr
		}
		stripeCount = 1
		for i, piece := range pieces {
			if writers[i] != nil {
				if err := writers[i].WriteStripe(piece); err != nil {
					abortAll()
					return res, err
				}
			}
		}
	}

	for i, w := range writers {
		if w == nil {
			continue
		}
		if err := w.Close(size); err != nil {
			return res, fmt.Errorf("store: finalise shard %d on %s: %w", i, chosen[i], err)
		}
	}

	res.Size = size
	res.ETag = hex.EncodeToString(md5h.Sum(nil))
	res.SHA256 = hex.EncodeToString(hash.Sum(nil))
	res.StripeCount = stripeCount
	return res, nil
}

func (s *Store) abortWriters(ws []*shard.StreamWriter) {
	for _, w := range ws {
		if w != nil {
			w.Abort()
		}
	}
}

func bytesReader(b []byte) io.Reader { return &byteReader{b: b} }

type byteReader struct {
	b []byte
	o int
}

func (r *byteReader) Read(p []byte) (int, error) {
	if r.o >= len(r.b) {
		return 0, io.EOF
	}
	n := copy(p, r.b[r.o:])
	r.o += n
	return n, nil
}
