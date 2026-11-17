package lifecycle_test

import (
	"context"
	"io"
	"path/filepath"
	"testing"
	"time"

	"imstore/internal/erasure"
	"imstore/internal/index"
	"imstore/internal/lifecycle"
	"imstore/internal/store"
	"imstore/internal/volume"
)

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

func newHarness(t *testing.T) (*store.Store, *index.Index) {
	t.Helper()
	root := t.TempDir()
	var vols []*volume.Volume
	for i := 0; i < 6; i++ {
		id := string(rune('a' + i))
		v, _ := volume.New(id, filepath.Join(root, id), id)
		vols = append(vols, v)
	}
	vm := volume.NewManager(vols)
	idx, err := index.Open(filepath.Join(root, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { idx.Close() })
	codec, _ := erasure.New(4, 2, 4096)
	return store.New(codec, vm, idx), idx
}

func TestExpirationDeletesAgedObjects(t *testing.T) {
	st, idx := newHarness(t)
	ctx := context.Background()
	if err := idx.CreateBucket("b"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PutObject(ctx, "b", "temp/old.dcm", "", nil,
		&byteReader{b: []byte("old")}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PutObject(ctx, "b", "keep/new.dcm", "", nil,
		&byteReader{b: []byte("new")}); err != nil {
		t.Fatal(err)
	}

	// Backdate the temp object so it is older than the 1-day rule.
	old, err := idx.GetObject("b", "temp/old.dcm")
	if err != nil {
		t.Fatal(err)
	}
	old.CreatedAt = time.Now().Add(-48 * time.Hour)
	cur, _ := idx.GetObject("b", "keep/new.dcm")
	if err := idx.PutObject(*old, cur); err != nil {
		t.Fatal(err)
	}

	if err := idx.PutLifecycleRule(index.LifecycleRule{
		ID: "r1", Bucket: "b", Prefix: "temp/", ExpireDays: 1, Status: "Enabled",
	}); err != nil {
		t.Fatal(err)
	}

	runner := lifecycle.NewRunner(st, time.Hour, time.Hour)
	runner.RunOnce()

	if _, err := idx.GetObject("b", "temp/old.dcm"); err == nil {
		t.Fatal("aged temp/ object should have been expired")
	}
	if _, err := idx.GetObject("b", "keep/new.dcm"); err != nil {
		t.Fatalf("non-matching object must survive: %v", err)
	}
}

func TestStalledMultipartAborted(t *testing.T) {
	st, idx := newHarness(t)
	if err := idx.CreateBucket("b"); err != nil {
		t.Fatal(err)
	}
	uploadID, err := st.InitMultipart("b", "k", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	// Backdate the upload beyond the 1-hour max age.
	if err := idx.TouchMPUpload(uploadID, time.Now().Add(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}

	runner := lifecycle.NewRunner(st, time.Hour, time.Hour)
	runner.RunOnce()

	if _, err := idx.GetMPUpload(uploadID); err == nil {
		t.Fatal("stalled multipart upload should be aborted")
	}
}
