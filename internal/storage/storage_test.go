package storage_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"erasurestore/internal/config"
	"erasurestore/internal/disk"
	"erasurestore/internal/meta"
	"erasurestore/internal/storage"
)

func newTestStore(t *testing.T, disks int) (*storage.Store, *config.Config) {
	t.Helper()
	root := t.TempDir()
	cfg := config.Default()
	cfg.DataShards, cfg.ParityShards = 4, 2
	cfg.StripeSize = 256 * 1024 // 小 stripe 让单对象覆盖多 stripe
	cfg.MetaDBPath = filepath.Join(root, "meta.db")
	for i := 1; i <= disks; i++ {
		enc := "enc-a"
		if i > 3 {
			enc = "enc-b"
		}
		if i > 6 {
			enc = "enc-c"
		}
		cfg.Disks = append(cfg.Disks, config.DiskConfig{
			ID: diskName(i), Path: filepath.Join(root, diskName(i)), Enclosure: enc,
		})
	}
	st, err := storage.New(cfg)
	if err != nil {
		t.Fatalf("storage.New: %v", err)
	}
	t.Cleanup(func() { _ = st.Meta().Close() })
	return st, cfg
}

func diskName(i int) string {
	return "disk" + string(rune('0'+i))
}

func putAndVerify(t *testing.T, st *storage.Store, bucket, key string, size int) (string, []byte) {
	t.Helper()
	content := make([]byte, size)
	_, _ = rand.Read(content)
	m, err := st.PutObject(context.Background(), bucket, key, bytes.NewReader(content),
		storage.PutOptions{ContentType: "application/dicom"})
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if m.Size != int64(size) {
		t.Fatalf("大小不符: %d != %d", m.Size, size)
	}
	var got bytes.Buffer
	if err := st.GetObject(m, &got); err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	if !bytes.Equal(got.Bytes(), content) {
		t.Fatal("下载内容与上传不一致")
	}
	sum := sha256.Sum256(content)
	if m.SHA256 != hex.EncodeToString(sum[:]) {
		t.Fatal("sha256 记录错误")
	}
	return m.ID, content
}

func TestPutGetDelete(t *testing.T) {
	st, _ := newTestStore(t, 7)
	if err := st.Meta().CreateBucket("images"); err != nil {
		t.Fatal(err)
	}
	for _, size := range []int{0, 1, 4096, 700 * 1024, 2 << 20} {
		key := "obj-" + string(rune('a'+size%26)) + "-" + itoa(size)
		putAndVerify(t, st, "images", key, size)
	}
	m, err := st.Meta().GetObjectMeta("images", "obj-a-0")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.DeleteObject("images", m.Key); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Meta().GetObjectMeta("images", m.Key); err == nil {
		t.Fatal("删除后元数据仍存在")
	}
}

func TestReadWithTwoMissingShards(t *testing.T) {
	st, _ := newTestStore(t, 7)
	_ = st.Meta().CreateBucket("images")
	id, _ := putAndVerify(t, st, "images", "ct/big.dcm", 2<<20)

	m, _ := st.Meta().GetObjectMeta("images", "ct/big.dcm")
	// 删除两个分片文件（任意两块盘）
	removed := 0
	for _, p := range m.Placement {
		if removed >= 2 {
			break
		}
		d, _ := st.DiskManager().Get(p.DiskID)
		path := d.ShardPath(id, p.ShardIndex)
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		removed++
	}
	scan, err := st.ScanObject(m)
	if err != nil {
		t.Fatal(err)
	}
	if len(scan.Missing) != 2 {
		t.Fatalf("期望 2 个缺失分片，得到 %v", scan.Missing)
	}
	// 仍然可读（RS 重建）
	var got bytes.Buffer
	if err := st.GetObject(m, &got); err != nil {
		t.Fatalf("缺两片后读取失败: %v", err)
	}
	if got.Len() != int(m.Size) {
		t.Fatalf("降级读大小错误: %d", got.Len())
	}
}

func TestBitflipDetectedAndRebuilt(t *testing.T) {
	st, _ := newTestStore(t, 7)
	_ = st.Meta().CreateBucket("images")
	id, content := putAndVerify(t, st, "images", "mr/scan.dcm", 1<<20)
	m, _ := st.Meta().GetObjectMeta("images", "mr/scan.dcm")

	// 翻转某分片文件中间的一个字节
	p := m.Placement[3]
	d, _ := st.DiskManager().Get(p.DiskID)
	path := d.ShardPath(id, p.ShardIndex)
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	fi, _ := f.Stat()
	offset := fi.Size() / 2
	if _, err := f.WriteAt([]byte{0xFF}, offset); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	scan, err := st.ScanObject(m)
	if err != nil {
		t.Fatal(err)
	}
	if len(scan.Corrupt) != 1 {
		t.Fatalf("期望检出 1 个损坏分片，得到 %v", scan.Corrupt)
	}

	// 重建到新盘（未被该对象健康分片占用的在线盘）
	targetDisk := pickSpareDisk(st, m, scan.Corrupt)
	if targetDisk == "" {
		t.Fatal("找不到备盘")
	}
	targets := map[int]string{}
	for _, idx := range scan.Corrupt {
		targets[idx] = targetDisk
	}
	if err := st.RebuildObject(context.Background(), m, targets, nil, nil); err != nil {
		t.Fatalf("RebuildObject: %v", err)
	}
	scan2, err := st.ScanObject(m)
	if err != nil {
		t.Fatal(err)
	}
	if !scan2.Healthy() {
		t.Fatalf("重建后仍不健康: %+v", scan2)
	}
	// 哈希与写入时一致
	gotSHA, err := st.VerifyObjectSHA256(m)
	if err != nil {
		t.Fatal(err)
	}
	want := sha256.Sum256(content)
	if gotSHA != hex.EncodeToString(want[:]) {
		t.Fatal("重建后哈希不一致")
	}
}

func TestWholeDiskOfflineAndReinsert(t *testing.T) {
	st, _ := newTestStore(t, 7)
	_ = st.Meta().CreateBucket("images")
	_, _ = putAndVerify(t, st, "images", "x.dcm", 512*1024)
	m, _ := st.Meta().GetObjectMeta("images", "x.dcm")

	// 模拟整块盘拔掉
	victim := m.Placement[0].DiskID
	d, _ := st.DiskManager().Get(victim)
	d.SetStatus(disk.StatusOffline)

	scan, err := st.ScanObject(m)
	if err != nil {
		t.Fatal(err)
	}
	if len(scan.Missing) != 1 || scan.Missing[0] != 0 {
		t.Fatalf("拔盘后缺失检测错误: %+v", scan)
	}
	// 离线状态下数据仍可读
	var buf bytes.Buffer
	if err := st.GetObject(m, &buf); err != nil {
		t.Fatalf("拔盘后读取失败: %v", err)
	}

	// 插回：标记在线并触发探测
	d.SetStatus(disk.StatusOnline)
	d.Probe()
	scan2, err := st.ScanObject(m)
	if err != nil {
		t.Fatal(err)
	}
	if !scan2.Healthy() {
		t.Fatalf("插回后应恢复健康: %+v", scan2)
	}
}

func TestMultipartUpload(t *testing.T) {
	st, _ := newTestStore(t, 7)
	_ = st.Meta().CreateBucket("images")
	uploadID, err := st.InitMPU("images", "big/multi.dcm", "application/dicom", nil)
	if err != nil {
		t.Fatal(err)
	}
	parts := [][]byte{}
	for i := 1; i <= 4; i++ {
		part := make([]byte, 600*1024)
		_, _ = rand.Read(part)
		parts = append(parts, part)
		if _, err := st.PutPart(context.Background(), "images", uploadID, i, bytes.NewReader(part)); err != nil {
			t.Fatalf("PutPart %d: %v", i, err)
		}
	}
	m, err := st.CompleteMPU(context.Background(), "images", uploadID, []int{1, 2, 3, 4})
	if err != nil {
		t.Fatalf("CompleteMPU: %v", err)
	}
	var got bytes.Buffer
	if err := st.GetObject(m, &got); err != nil {
		t.Fatal(err)
	}
	var want bytes.Buffer
	for _, p := range parts {
		want.Write(p)
	}
	if !bytes.Equal(got.Bytes(), want.Bytes()) {
		t.Fatal("MPU 组装内容错误")
	}
}

func TestPlacementNoSameDisk(t *testing.T) {
	st, _ := newTestStore(t, 7)
	_ = st.Meta().CreateBucket("images")
	_, _ = putAndVerify(t, st, "images", "p.dcm", 128*1024)
	m, _ := st.Meta().GetObjectMeta("images", "p.dcm")
	seenDisk := map[string]bool{}
	encCount := map[string]int{}
	for _, p := range m.Placement {
		if seenDisk[p.DiskID] {
			t.Fatalf("分片共盘: %s", p.DiskID)
		}
		seenDisk[p.DiskID] = true
		d, _ := st.DiskManager().Get(p.DiskID)
		encCount[d.Enclosure()]++
	}
	if len(m.Placement) != st.Config().DataShards+st.Config().ParityShards {
		t.Fatal("分片数量不符")
	}
}

func pickSpareDisk(st *storage.Store, m *meta.ObjectMeta, corruptIdx []int) string {
	bad := map[int]bool{}
	for _, i := range corruptIdx {
		bad[i] = true
	}
	used := map[string]bool{}
	for _, p := range m.Placement {
		if !bad[p.ShardIndex] {
			used[p.DiskID] = true
		}
	}
	for _, d := range st.DiskManager().Online() {
		if !used[d.ID()] {
			return d.ID()
		}
	}
	return ""
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
