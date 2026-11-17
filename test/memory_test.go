//go:build s3compat

package s3compat

import (
	"context"
	"io"
	"runtime"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// TestLargeUploadMemoryBounded streams a multi-hundred-MiB object through the
// erasure-coded write path and asserts peak heap stays a small fraction of
// the object size. The store only buffers one stripe at a time, so a 300 MiB
// upload must not allocate anywhere near 300 MiB of live heap.
func TestLargeUploadMemoryBounded(t *testing.T) {
	srv, cleanup := startNode(t)
	defer cleanup()
	client := newSDKClient(t, srv.URL)
	ctx := context.Background()
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("big"),
	}); err != nil {
		t.Fatal(err)
	}

	const objectSize = 300 << 20 // 300 MiB
	// A SectionReader over a tiny base emulates a large seekable source
	// without allocating the object: the SDK needs Seek for its checksum
	// logic on plain (non-TLS) HTTP, while the store still streams stripe by
	// stripe and never materialises the full size.
	rng := io.NewSectionReader(zeroBase{}, 0, objectSize)

	var ms0, ms1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&ms0)

	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("big"), Key: aws.String("300m.bin"), Body: rng,
	}); err != nil {
		t.Fatalf("large PutObject: %v", err)
	}

	runtime.GC()
	runtime.ReadMemStats(&ms1)
	peak := ms1.HeapAlloc
	t.Logf("object=%d MiB heap_alloc=%d MiB heap_idle=%d MiB total_alloc=%d MiB",
		objectSize>>20, peak>>20, ms1.HeapIdle>>20, ms1.TotalAlloc>>20)

	// The whole object must never be resident. Allow generous headroom for
	// SDK buffers, but require live heap well under the object size.
	if peak > objectSize/4 {
		t.Fatalf("heap alloc %d exceeds 25%% of %d-byte object; upload is not streaming",
			peak, objectSize)
	}

	// Confirm the object is readable and reports the full size.
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("big"), Key: aws.String("300m.bin"),
	})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer out.Body.Close()
	if aws.ToInt64(out.ContentLength) != objectSize {
		t.Fatalf("stored size = %d, want %d", aws.ToInt64(out.ContentLength), objectSize)
	}
	if _, err := io.Copy(io.Discard, out.Body); err != nil {
		t.Fatalf("draining object: %v", err)
	}
}

// zeroBase is an effectively infinite, zero-allocation ReaderAt used to back
// a large SectionReader without holding the object in memory.
type zeroBase struct{}

func (zeroBase) ReadAt(p []byte, _ int64) (int, error) {
	for i := range p {
		p[i] = byte(i & 0xff)
	}
	return len(p), nil
}

var _ io.ReaderAt = zeroBase{}
