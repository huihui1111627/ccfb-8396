// Package erasure wraps Reed-Solomon encoding with the stripe conventions
// used across imstore: every stripe is split into DataShards fixed-size
// pieces and extended with ParityShards parity pieces.
package erasure

import (
	"errors"
	"fmt"
	"io"

	"github.com/klauspost/reedsolomon"
)

// ErrNotEnoughShards is returned when fewer than dataShards are available.
var ErrNotEnoughShards = errors.New("erasure: not enough shards to reconstruct")

// Codec is a reusable Reed-Solomon encoder for one (data, parity) layout.
type Codec struct {
	enc       reedsolomon.Encoder
	data      int
	parity    int
	shardSize int64
}

// New creates a codec. shardSize is the fixed byte size of one shard piece
// within a stripe.
func New(data, parity int, shardSize int64) (*Codec, error) {
	if data < 1 || parity < 1 {
		return nil, errors.New("erasure: data and parity must be >= 1")
	}
	if shardSize <= 0 {
		return nil, errors.New("erasure: shardSize must be > 0")
	}
	enc, err := reedsolomon.New(data, parity,
		reedsolomon.WithAutoGoroutines(int(shardSize)),
	)
	if err != nil {
		return nil, fmt.Errorf("erasure: build codec: %w", err)
	}
	return &Codec{enc: enc, data: data, parity: parity, shardSize: shardSize}, nil
}

func (c *Codec) DataShards() int   { return c.data }
func (c *Codec) ParityShards() int { return c.parity }
func (c *Codec) TotalShards() int  { return c.data + c.parity }
func (c *Codec) ShardSize() int64  { return c.shardSize }

// Encode reads exactly one full stripe of dataShards*shardSize bytes from r
// (the final stripe may be shorter) and returns totalShards buffers, each of
// shardSize bytes. Missing trailing bytes are zero-padded; dataLen reports the
// real payload length contained in the stripe (used to trim on read).
func (c *Codec) Encode(r io.Reader) (shards [][]byte, dataLen int64, err error) {
	total := c.TotalShards()
	shards = make([][]byte, total)
	for i := range shards {
		shards[i] = make([]byte, c.shardSize)
	}
	// Fill each data shard in order. io.ReadFull returns ErrUnexpectedEOF on
	// the short final stripe; plain EOF only when nothing remained at all.
	for i := 0; i < c.data; i++ {
		n, rerr := io.ReadFull(r, shards[i])
		dataLen += int64(n)
		if rerr == nil {
			continue
		}
		if errors.Is(rerr, io.ErrUnexpectedEOF) {
			break // short final stripe: remaining shards stay zero-padded
		}
		if errors.Is(rerr, io.EOF) {
			if i == 0 && dataLen == 0 {
				return nil, 0, io.EOF
			}
			break
		}
		return nil, 0, fmt.Errorf("erasure: read stripe: %w", rerr)
	}
	if err := c.enc.Encode(shards); err != nil {
		return nil, 0, fmt.Errorf("erasure: encode: %w", err)
	}
	return shards, dataLen, nil
}

// Verify reports whether all shards satisfy the parity equations. A false
// result with nil error means the data is present but inconsistent (bit rot).
func (c *Codec) Verify(shards [][]byte) (bool, error) {
	if len(shards) != c.TotalShards() {
		return false, fmt.Errorf("erasure: expected %d shard slots, got %d", c.TotalShards(), len(shards))
	}
	return c.enc.Verify(shards)
}

// Reconstruct fills nil/missing entries in shards using the available pieces.
// At least dataShards non-nil pieces are required. Corrupted pieces should be
// nil-ed by the caller once checksum verification fails.
func (c *Codec) Reconstruct(shards [][]byte) error {
	if len(shards) != c.TotalShards() {
		return fmt.Errorf("erasure: expected %d shard slot(s), got %d", c.TotalShards(), len(shards))
	}
	available := 0
	for _, s := range shards {
		if s != nil {
			available++
		}
	}
	if available < c.data {
		return fmt.Errorf("%w: have %d, need %d", ErrNotEnoughShards, available, c.data)
	}
	if err := c.enc.Reconstruct(shards); err != nil {
		return fmt.Errorf("erasure: reconstruct: %w", err)
	}
	ok, err := c.enc.Verify(shards)
	if err != nil {
		return fmt.Errorf("erasure: verify after reconstruct: %w", err)
	}
	if !ok {
		return errors.New("erasure: reconstructed shards failed verification")
	}
	return nil
}

// DataPayload joins the data shards of one stripe into one contiguous buffer.
func (c *Codec) DataPayload(shards [][]byte) []byte {
	out := make([]byte, 0, c.data*int(c.shardSize))
	for i := 0; i < c.data; i++ {
		out = append(out, shards[i]...)
	}
	return out
}
