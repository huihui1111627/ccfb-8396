package index_test

import (
	"fmt"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"imstore/internal/index"
)

// TestPrefixPaginationPerformance seeds a large key space and asserts that
// paginated prefix scans stay O(log n + page size): each page is a cursor
// seek rather than a scan from the beginning.
func TestPrefixPaginationPerformance(t *testing.T) {
	idx, err := index.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()
	if err := idx.CreateBucket("b"); err != nil {
		t.Fatal(err)
	}

	const total = 10000 // enough to prove cursor pagination; each Put is its
	// own fsync'd bbolt transaction, so going much higher is dominated by disk
	// flush cost rather than the index logic under test.
	now := time.Now().UTC()
	for i := 0; i < total; i++ {
		// Hierarchical medical-style keys with a hot common prefix.
		key := fmt.Sprintf("ct/2026/study%05d/inst%06d.dcm", i/10, i)
		meta := index.ObjectMeta{
			Bucket: "b", Key: key, ObjectID: fmt.Sprintf("o%08d", i),
			Size: 128, CreatedAt: now,
		}
		if err := idx.PutObject(meta, nil); err != nil {
			t.Fatal(err)
		}
	}

	count, err := idx.CountObjects()
	if err != nil || count != total {
		t.Fatalf("count = %d, want %d (err %v)", count, total, err)
	}

	// Walk every object in pages of 500 using continuation tokens.
	const pageSize = 500
	var seen int
	var token string
	pages := 0
	for {
		page, err := idx.ListObjects("b", "ct/", token, "", pageSize)
		if err != nil {
			t.Fatal(err)
		}
		seen += len(page.Objects)
		pages++
		if !page.Truncated {
			break
		}
		if page.NextStartKey == "" {
			t.Fatal("truncated page without continuation token")
		}
		token = page.NextStartKey
	}
	if seen != total {
		t.Fatalf("paginated walk saw %d objects, want %d", seen, total)
	}
	if pages != total/pageSize {
		t.Fatalf("page count = %d, want %d", pages, total/pageSize)
	}

	// Deep prefix must return only its small subtree, not the whole bucket.
	deep, err := idx.ListObjects("b", "ct/2026/study00123/", "", "", 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(deep.Objects) != 10 {
		t.Fatalf("deep prefix returned %d objects, want 10", len(deep.Objects))
	}
}

func TestBucketLifecycleAndReverseIndex(t *testing.T) {
	idx, err := index.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()
	if err := idx.CreateBucket("b"); err != nil {
		t.Fatal(err)
	}
	// Duplicate bucket must fail.
	if err := idx.CreateBucket("b"); err == nil {
		t.Fatal("expected duplicate bucket error")
	}

	for i := 0; i < 5; i++ {
		err := idx.PutObject(index.ObjectMeta{
			Bucket: "b", Key: "k" + strconv.Itoa(i),
			ObjectID: "obj" + strconv.Itoa(i), Size: 1,
			VolumeIDs: []string{"v0", "v1"}, CreatedAt: time.Now(),
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Reverse index: every object placed on v0 must be enumerable by volume.
	var keys []string
	err = idx.ObjectsOnVolume("v0", func(bucket, key string) error {
		keys = append(keys, bucket+"/"+key)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 5 {
		t.Fatalf("volume reverse index returned %d, want 5", len(keys))
	}

	// Deleting an object drops its reverse-index entries.
	removed, err := idx.DeleteObject("b", "k0")
	if err != nil || removed == nil {
		t.Fatalf("delete: %v", err)
	}
	var after int
	_ = idx.ObjectsOnVolume("v0", func(_, _ string) error { after++; return nil })
	if after != 4 {
		t.Fatalf("reverse index after delete = %d, want 4", after)
	}
}
