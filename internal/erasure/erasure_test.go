package erasure_test

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"testing"

	"imstore/internal/erasure"
)

func TestEncodeReconstructAllFailurePairs(t *testing.T) {
	const data, parity, shardSize = 4, 2, 256
	codec, err := erasure.New(data, parity, shardSize)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	payload := make([]byte, data*shardSize)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	shards, n, err := codec.Encode(bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if n != int64(len(payload)) {
		t.Fatalf("dataLen = %d, want %d", n, len(payload))
	}
	original := make([][]byte, len(shards))
	for i := range shards {
		original[i] = append([]byte(nil), shards[i]...)
	}

	// Every pair of shards removed must be recoverable (4+2 tolerates 2).
	for missingA := 0; missingA < data+parity; missingA++ {
		for missingB := missingA; missingB < data+parity; missingB++ {
			slots := make([][]byte, data+parity)
			for i := range original {
				if i != missingA && i != missingB {
					slots[i] = append([]byte(nil), original[i]...)
				}
			}
			if err := codec.Reconstruct(slots); err != nil {
				t.Fatalf("reconstruct with %d,%d missing: %v", missingA, missingB, err)
			}
			for i := range slots {
				if !bytes.Equal(slots[i], original[i]) {
					t.Fatalf("shard %d differs after reconstruct (missing %d,%d)", i, missingA, missingB)
				}
			}
			got := codec.DataPayload(slots)
			if !bytes.Equal(got, payload) {
				t.Fatalf("payload mismatch with %d,%d missing", missingA, missingB)
			}
		}
	}
}

func TestEncodeShortFinalStripe(t *testing.T) {
	const data, parity, shardSize = 4, 2, 128
	codec, _ := erasure.New(data, parity, shardSize)

	// Payload smaller than a full stripe and not aligned to shard boundaries.
	payload := make([]byte, shardSize*2+37)
	rand.Read(payload)
	shards, n, err := codec.Encode(bytes.NewReader(payload))
	if err != nil || n != int64(len(payload)) {
		t.Fatalf("Encode: n=%d err=%v", n, err)
	}
	if err := codec.Reconstruct(shards); err != nil {
		t.Fatalf("Reconstruct: %v", err)
	}
	got := codec.DataPayload(shards)[:len(payload)]
	if !bytes.Equal(got, payload) {
		t.Fatal("short stripe payload mismatch")
	}
}

func TestReconstructTooFewShards(t *testing.T) {
	const data, parity, shardSize = 4, 2, 64
	codec, _ := erasure.New(data, parity, shardSize)
	shards, _, err := codec.Encode(bytes.NewReader(make([]byte, data*shardSize)))
	if err != nil {
		t.Fatal(err)
	}
	// Only data-1 shards available -> must fail.
	for i := data - 1; i < len(shards); i++ {
		shards[i] = nil
	}
	err = codec.Reconstruct(shards)
	if !errors.Is(err, erasure.ErrNotEnoughShards) {
		t.Fatalf("want ErrNotEnoughShards, got %v", err)
	}
}

func TestEncodeEmptyReader(t *testing.T) {
	codec, _ := erasure.New(4, 2, 64)
	if _, _, err := codec.Encode(bytes.NewReader(nil)); err != io.EOF {
		t.Fatalf("want io.EOF, got %v", err)
	}
}
