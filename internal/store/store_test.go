package store_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"imstore/internal/erasure"
	"imstore/internal/index"
	"imstore/internal/repair"
	"imstore/internal/store"
	"imstore/internal/volume"
)

type harness struct {
	root string
	vm   *volume.Manager
	idx  *index.Index
	st   *store.Store
	rm   *repair.Manager
}

func newHarness(t *testing.T, data, parity int) *harness {
	t.Helper()
	root := t.TempDir()
	// data+parity distinct enclosures, one volume each.
	total := data + parity
	var vols []*volume.Volume
	var cfgs []struct{ id, enc string }
	for i := 0; i < total; i++ {
		id := fmt.Sprintf("vol%d", i)
		enc := fmt.Sprintf("enc%d", i)
		cfgs = append(cfgs, struct{ id, enc string }{id, enc})
	}
	for _, c := range cfgs {
		v, err := volume.New(c.id, filepath.Join(root, c.id), c.enc)
		if err != nil {
			t.Fatal(err)
		}
		vols = append(vols, v)
	}
	vm := volume.NewManager(vols)
	idx, err := index.Open(filepath.Join(root, "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { idx.Close() })
	shardSize := int64(64 * 1024) // 64 KiB shard -> 256 KiB stripe at data=4
	codec, err := erasure.New(data, parity, shardSize)
	if err != nil {
		t.Fatal(err)
	}
	st := store.New(codec, vm, idx)
	rm := repair.NewManager(st, 0, 50*time.Millisecond, false)
	rm.Start()
	t.Cleanup(rm.Stop)
	return &harness{root: root, vm: vm, idx: idx, st: st, rm: rm}
}

func randomPayload(t *testing.T, size int) []byte {
	b := make([]byte, size)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

func putAndCheck(t *testing.T, h *harness, key string, payload []byte) {
	t.Helper()
	meta, err := h.st.PutObject(context.Background(), "bkt", key, "application/octet-stream", nil, bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	wantHash := sha256.Sum256(payload)
	if meta.SHA256 != hex.EncodeToString(wantHash[:]) {
		t.Fatalf("stored hash %s != computed", meta.SHA256)
	}
}

func readBack(t *testing.T, h *harness, key string, want []byte) {
	t.Helper()
	r, err := h.st.OpenObject("bkt", key)
	if err != nil {
		t.Fatalf("OpenObject: %v", err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("payload mismatch: got %d bytes, want %d", len(got), len(want))
	}
}

func TestPutGetRoundTrip(t *testing.T) {
	h := newHarness(t, 4, 2)
	if err := h.idx.CreateBucket("bkt"); err != nil {
		t.Fatal(err)
	}
	for _, size := range []int{0, 1, 7, 4095, 256 * 1024, 1024*1024 + 123} {
		payload := randomPayload(t, size)
		key := fmt.Sprintf("obj-%d", size)
		putAndCheck(t, h, key, payload)
		readBack(t, h, key, payload)
	}
}

func TestReadWithTwoMissingShards(t *testing.T) {
	h := newHarness(t, 4, 2)
	_ = h.idx.CreateBucket("bkt")
	payload := randomPayload(t, 512*1024)
	putAndCheck(t, h, "img", payload)

	meta, _ := h.idx.GetObject("bkt", "img")
	part := meta.AllParts()[0]
	// Delete two whole shard files (any two; 4+2 tolerates exactly two).
	if err := os.Remove(shardPath(h, part.VolumeIDs[1], part.ObjectID, 1)); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(shardPath(h, part.VolumeIDs[4], part.ObjectID, 4)); err != nil {
		t.Fatal(err)
	}
	readBack(t, h, "img", payload)

	// A third missing shard must fail loudly.
	if err := os.Remove(shardPath(h, part.VolumeIDs[2], part.ObjectID, 2)); err != nil {
		t.Fatal(err)
	}
	r, err := h.st.OpenObject("bkt", "img")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(r); err == nil {
		t.Fatal("expected failure with 3 missing shards, got success")
	}
}

func TestBitRotHealsOnReadAndRepair(t *testing.T) {
	h := newHarness(t, 4, 2)
	_ = h.idx.CreateBucket("bkt")
	payload := randomPayload(t, 700*1024)
	putAndCheck(t, h, "rot", payload)

	meta, _ := h.idx.GetObject("bkt", "rot")
	part := meta.AllParts()[0]
	path := shardPath(h, part.VolumeIDs[0], part.ObjectID, 0)
	flipByte(t, path)

	// Read still succeeds via parity reconstruction.
	readBack(t, h, "rot", payload)

	// Repair sweep should rewrite the corrupt shard.
	if err := h.rm.ScanAndRepair("test"); err != nil {
		t.Fatalf("ScanAndRepair: %v", err)
	}
	// After repair, reading again and hash verification pass.
	if err := h.st.VerifyHash("bkt", "rot"); err != nil {
		t.Fatalf("VerifyHash after repair: %v", err)
	}
}

func TestOfflineVolumeThenRebuild(t *testing.T) {
	h := newHarness(t, 4, 2)
	_ = h.idx.CreateBucket("bkt")
	payload := randomPayload(t, 600*1024)
	putAndCheck(t, h, "disk", payload)

	meta, _ := h.idx.GetObject("bkt", "disk")
	part := meta.AllParts()[0]
	targetVol := part.VolumeIDs[3]
	v, _ := h.vm.Get(targetVol)
	v.SetOffline("simulated pull")

	// Object remains readable through parity.
	readBack(t, h, "disk", payload)

	// Re-insert disk and rebuild.
	if err := v.SetOnline(); err != nil {
		t.Fatal(err)
	}
	task, err := h.rm.RebuildVolume(context.Background(), targetVol)
	if err != nil {
		t.Fatalf("RebuildVolume: %v", err)
	}
	waitTask(t, h, task.ID)

	// The missing shard file now exists again.
	if _, err := os.Stat(shardPath(h, targetVol, part.ObjectID, 3)); err != nil {
		t.Fatalf("rebuilt shard missing: %v", err)
	}
	if err := h.st.VerifyHash("bkt", "disk"); err != nil {
		t.Fatalf("VerifyHash: %v", err)
	}
}

func TestMultipartSurvivesShardLoss(t *testing.T) {
	h := newHarness(t, 4, 2)
	_ = h.idx.CreateBucket("bkt")
	uploadID, err := h.st.InitMultipart("bkt", "mp", "application/octet-stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	chunks := [][]byte{randomPayload(t, 300*1024), randomPayload(t, 200*1024), randomPayload(t, 123)}
	var full []byte
	for i, c := range chunks {
		full = append(full, c...)
		if _, err := h.st.UploadPart(context.Background(), uploadID, i+1, bytes.NewReader(c)); err != nil {
			t.Fatalf("UploadPart %d: %v", i+1, err)
		}
	}
	if _, err := h.st.CompleteMultipart(uploadID, []int{1, 2, 3}); err != nil {
		t.Fatalf("CompleteMultipart: %v", err)
	}
	readBack(t, h, "mp", full)

	// Damage one shard in the second part and repair.
	meta, _ := h.idx.GetObject("bkt", "mp")
	part := meta.Parts[1]
	flipByte(t, shardPath(h, part.VolumeIDs[2], part.ObjectID, 2))
	readBack(t, h, "mp", full)
	if err := h.rm.ScanAndRepair("test"); err != nil {
		t.Fatal(err)
	}
	readBack(t, h, "mp", full)
}

func TestPrefixPagination(t *testing.T) {
	h := newHarness(t, 4, 2)
	_ = h.idx.CreateBucket("bkt")
	keys := []string{"ct/2026/a1.dcm", "ct/2026/a2.dcm", "ct/2027/b1.dcm", "mr/x.dcm"}
	for _, k := range keys {
		putAndCheck(t, h, k, randomPayload(t, 100))
	}
	// Page size 2: walk every object via continuation tokens.
	var seen []string
	var token string
	for {
		page, err := h.idx.ListObjects("bkt", "", token, "", 2)
		if err != nil {
			t.Fatal(err)
		}
		for _, o := range page.Objects {
			seen = append(seen, o.Key)
		}
		if !page.Truncated {
			break
		}
		token = page.NextStartKey
	}
	if len(seen) != len(keys) {
		t.Fatalf("pagination returned %d objects, want %d", len(seen), len(keys))
	}
	// Prefix filter.
	page, err := h.idx.ListObjects("bkt", "ct/2026", "", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) != 2 {
		t.Fatalf("prefix filter returned %d, want 2", len(page.Objects))
	}
	// Delimiter folds "folders": at the root every key containing a slash is
	// grouped into its first segment, so we get two common prefixes and no
	// top-level object.
	dpage, err := h.idx.ListObjects("bkt", "", "", "/", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(dpage.CommonPrefixes) != 2 {
		t.Fatalf("want 2 common prefixes, got %+v", dpage.CommonPrefixes)
	}
	gotPrefixes := map[string]bool{}
	for _, cp := range dpage.CommonPrefixes {
		gotPrefixes[cp] = true
	}
	if !gotPrefixes["ct/"] || !gotPrefixes["mr/"] {
		t.Fatalf("common prefixes wrong: %+v", dpage.CommonPrefixes)
	}
	if len(dpage.Objects) != 0 {
		t.Fatalf("want no top-level objects, got %+v", dpage.Objects)
	}

	// Inside "ct/" the next-level delimiter yields the "2026/" and "2027/"
	// folders and still no direct objects.
	sub, err := h.idx.ListObjects("bkt", "ct/", "", "/", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(sub.CommonPrefixes) != 2 || len(sub.Objects) != 0 {
		t.Fatalf("sub-prefix listing wrong: %+v", sub)
	}
	subPrefixes := map[string]bool{}
	for _, cp := range sub.CommonPrefixes {
		subPrefixes[cp] = true
	}
	if !subPrefixes["ct/2026/"] || !subPrefixes["ct/2027/"] {
		t.Fatalf("sub common prefixes wrong: %+v", sub.CommonPrefixes)
	}
	// With no delimiter, all four objects list flat.
	flat, err := h.idx.ListObjects("bkt", "", "", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(flat.Objects) != 4 {
		t.Fatalf("flat listing want 4 objects, got %d", len(flat.Objects))
	}
}

// ---------------- helpers ----------------

func shardPath(h *harness, volumeID, objectID string, shardIdx int) string {
	v, err := h.vm.Get(volumeID)
	if err != nil {
		panic(err)
	}
	return v.ShardPath(objectID, shardIdx)
}

func flipByte(t *testing.T, path string) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	pos := info.Size() / 2
	buf := []byte{0}
	if _, err := f.ReadAt(buf, pos); err != nil {
		t.Fatal(err)
	}
	buf[0] ^= 0x01
	if _, err := f.WriteAt(buf, pos); err != nil {
		t.Fatal(err)
	}
	f.Sync()
}

func waitTask(t *testing.T, h *harness, id int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, task := range h.rm.ListTasks() {
			if task.ID == id && (task.Status == repair.StatusSucceeded || task.Status == repair.StatusFailed) {
				if task.Status != repair.StatusSucceeded {
					t.Fatalf("repair task %d failed: %s", id, task.Error)
				}
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("repair task %d did not finish", id)
}
