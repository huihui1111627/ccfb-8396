package shard_test

import (
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"imstore/internal/shard"
)

func TestStreamWriteReadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "obj.0.ims")
	const stripeSize = 128
	const stripes = 5
	h := shard.Header{ObjectID: "abc123", ShardIndex: 0, TotalShards: 6, ShardSize: stripeSize}

	w, err := shard.CreateStream(path, h)
	if err != nil {
		t.Fatalf("CreateStream: %v", err)
	}
	want := make([][]byte, stripes)
	for i := 0; i < stripes; i++ {
		b := make([]byte, stripeSize)
		rand.Read(b)
		want[i] = b
		if err := w.WriteStripe(b); err != nil {
			t.Fatalf("WriteStripe: %v", err)
		}
	}
	if err := w.Close(int64(stripes * stripeSize)); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r, err := shard.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()
	if r.Header().StripeCount != stripes {
		t.Fatalf("stripe count = %d, want %d", r.Header().StripeCount, stripes)
	}
	for i := 0; i < stripes; i++ {
		got, err := r.ReadStripe(i)
		if err != nil {
			t.Fatalf("ReadStripe %d: %v", i, err)
		}
		if string(got) != string(want[i]) {
			t.Fatalf("stripe %d mismatch", i)
		}
	}
}

func TestBitFlipIsDetected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "obj.1.ims")
	const stripeSize = 4096 // large enough that header and payload regions are distinct
	w, _ := shard.CreateStream(path, shard.Header{ObjectID: "x", ShardIndex: 1, TotalShards: 6, ShardSize: stripeSize})
	b := make([]byte, stripeSize)
	rand.Read(b)
	if err := w.WriteStripe(b); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(int64(stripeSize)); err != nil {
		t.Fatal(err)
	}

	// Corrupt one byte well inside the payload region (header is < 100 bytes
	// here; the checksum table sits after the 4096-byte payload).
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	target := int64(512)
	buf := []byte{0}
	if _, err := f.ReadAt(buf, target); err != nil {
		t.Fatal(err)
	}
	buf[0] ^= 0xFF
	if _, err := f.WriteAt(buf, target); err != nil {
		t.Fatal(err)
	}
	f.Close()

	r, err := shard.Open(path)
	if err != nil {
		t.Fatalf("Open after corruption: %v", err)
	}
	defer r.Close()
	if _, err := r.ReadStripe(0); !errors.Is(err, shard.ErrChecksumMismatch) {
		t.Fatalf("want ErrChecksumMismatch, got %v", err)
	}
}
