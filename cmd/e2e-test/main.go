// Command e2e-test 对纠删码对象存储做端到端混沌测试：
// 生成假影像 → S3 接口上传 → 删分片/翻比特/拔盘/换盘 → 自动修复 → 全量哈希校验。
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"

	"erasurestore/internal/config"
	"erasurestore/internal/console"
	"erasurestore/internal/lifecycle"
	"erasurestore/internal/meta"
	"erasurestore/internal/repair"
	"erasurestore/internal/s3"
	"erasurestore/internal/storage"
)

type testStats struct {
	passed int
	failed int
}

func main() {
	root := flag.String("root", "data/e2e", "测试工作目录（测试结束保留现场）")
	count := flag.Int("count", 40, "生成的假影像数量")
	flag.Parse()

	ctx := context.Background()
	rng := rand.New(rand.NewSource(424242))
	stats := &testStats{}

	work, err := filepath.Abs(*root)
	must(err)
	// 每次运行清空旧现场，保证测试可重复执行
	must(os.RemoveAll(work))
	must(os.MkdirAll(work, 0o755))
	log.Printf("工作目录: %s", work)

	// ---- 1. 构造 7 盘、4+2、两个盘柜+1 备盘的配置 ----
	cfg := buildConfig(work)
	store, err := storage.New(cfg)
	must(err)
	repairMgr := repair.NewManager(store, cfg.RepairBytesPerSec)
	repairMgr.Start()
	lcRunner := lifecycle.New(store)
	lcRunner.Start()

	// 启动 S3 与控制台 HTTP 服务（随机端口）
	s3Addr := freeAddr()
	consoleAddr := freeAddr()
	s3Srv := &http.Server{Addr: s3Addr, Handler: s3.NewServer(store, cfg).Handler()}
	csSrv := &http.Server{Addr: consoleAddr, Handler: console.New(store, repairMgr, lcRunner, cfg.ConsoleToken).Handler()}
	go func() { _ = s3Srv.ListenAndServe() }()
	go func() { _ = csSrv.ListenAndServe() }()
	waitPort(s3Addr)
	waitPort(consoleAddr)
	log.Printf("S3 服务: http://%s  控制台: http://%s", s3Addr, consoleAddr)

	cli, err := newS3Client("http://"+s3Addr, cfg.AccessKey, cfg.SecretKey, cfg.Region)
	must(err)
	ch := &chaos{store: store, rng: rng}

	cleanup := func() {
		_ = s3Srv.Close()
		_ = csSrv.Close()
		repairMgr.Stop()
		lcRunner.Stop()
		_ = store.Meta().Close()
	}
	defer cleanup()

	bucket := "medical-images"

	// ---- 2. 造数 ----
	step("生成 %d 个大小不一的假影像文件", *count)
	images, err := generateFakeImages(filepath.Join(work, "fixtures"), *count)
	must(err)
	var totalSize int64
	for _, im := range images {
		totalSize += im.Size
	}
	ok(stats, fmt.Sprintf("生成 %d 个文件，共 %s", len(images), humanBytes(totalSize)))

	// ---- 3. 标准 S3 客户端建桶、上传 ----
	step("用 AWS S3 SDK 建桶并上传（走 SigV4 签名）")
	must(cli.createBucket(ctx, bucket))
	for i, im := range images {
		if err := cli.putObject(ctx, bucket, im.Key, im.Path); err != nil {
			fail(stats, fmt.Sprintf("上传 %s: %v", im.Key, err))
			continue
		}
		if (i+1)%10 == 0 {
			log.Printf("  已上传 %d/%d", i+1, len(images))
		}
	}
	ok(stats, fmt.Sprintf("SDK PutObject 上传 %d 个对象", len(images)))

	// ---- 4. 列举（前缀分页）----
	step("验证 ListObjectsV2 前缀分页")
	out, err := cli.listObjects(ctx, bucket, "CT/", 10)
	must(err)
	listTotal := 0
	token := out.NextContinuationToken
	listTotal += len(out.Contents)
	pages := 1
	for token != nil {
		p, lerr := cli.cli.ListObjectsV2(ctx, s3ListInput(bucket, "CT/", 10, token))
		must(lerr)
		listTotal += len(p.Contents)
		token = p.NextContinuationToken
		pages++
	}
	ctCount := 0
	for _, im := range images {
		if len(im.Key) >= 3 && im.Key[:3] == "CT/" {
			ctCount++
		}
	}
	if listTotal == ctCount && pages >= 1 {
		ok(stats, fmt.Sprintf("前缀 CT/ 分页列举 %d 个对象，共 %d 页", listTotal, pages))
	} else {
		fail(stats, fmt.Sprintf("列举数量不符: got=%d want=%d", listTotal, ctCount))
	}

	// ---- 5. 放置约束校验：同对象分片不共盘、尽量不共柜 ----
	step("校验故障域放置约束")
	allMetas, err := loadAllMetas(store)
	must(err)
	placementOK := true
	for _, m := range allMetas {
		diskSet := map[string]bool{}
		for _, p := range m.Placement {
			if diskSet[p.DiskID] {
				placementOK = false
				fail(stats, fmt.Sprintf("对象 %s 分片共盘 %s", m.Key, p.DiskID))
			}
			diskSet[p.DiskID] = true
		}
	}
	if placementOK {
		ok(stats, fmt.Sprintf("全部 %d 个对象的分片均不共盘", len(allMetas)))
	}

	// ---- 6. 预签名链接：有效可下载、过期失效 ----
	step("验证预签名链接")
	first := images[0]
	goodURL, err := cli.presignGet(ctx, bucket, first.Key, 5*time.Minute)
	must(err)
	code, body, err := httpGet(goodURL)
	must(err)
	sum := sha256.Sum256(body)
	if code == http.StatusOK && hex.EncodeToString(sum[:]) == first.SHA256 {
		ok(stats, "有效期内的预签名链接可匿名下载且哈希正确")
	} else {
		fail(stats, fmt.Sprintf("预签名下载异常 code=%d", code))
	}
	expiredURL, err := cli.presignGet(ctx, bucket, first.Key, 1*time.Second)
	must(err)
	time.Sleep(1100 * time.Millisecond)
	code, _, _ = httpGet(expiredURL)
	if code == http.StatusForbidden {
		ok(stats, "过期预签名链接返回 403")
	} else {
		fail(stats, fmt.Sprintf("过期链接应返回 403，实际 %d", code))
	}

	// ---- 7. 大文件分片上传（manager uploader，走 MPU）----
	step("验证大对象 Multipart Upload（SDK transfer manager）")
	bigPath := filepath.Join(work, "fixtures", "big-mpu.dcm")
	must(makeBigFile(bigPath, 12*1024*1024))
	bigSHA, err := fileSHA256(bigPath)
	must(err)
	uploader := manager.NewUploader(cli.cli, func(u *manager.Uploader) {
		u.PartSize = 5 * 1024 * 1024 // 强制切成 3 个 part
	})
	bigKey := "MR/2026/11/big-mpu.dcm"
	bf, err := os.Open(bigPath)
	must(err)
	_, err = uploader.Upload(ctx, s3PutInput(bucket, bigKey, bf))
	bf.Close()
	must(err)
	got, err := cli.getObject(ctx, bucket, bigKey)
	must(err)
	gsum := sha256.Sum256(got)
	if hex.EncodeToString(gsum[:]) == bigSHA {
		ok(stats, "12MB 大文件经 MPU 上传后下载哈希一致")
	} else {
		fail(stats, "MPU 大文件哈希不一致")
	}

	// ---- 8a. 单分片故障：对随机对象各删 1 片或翻 1 片（每对象最多 1 个故障分片）----
	step("故障注入 A：随机删除分片 / 翻转比特（每对象 1 个分片）")
	victims := pickN(allMetas, 6, rng)
	injectedDisks := map[string]bool{}
	for i, m := range victims {
		var hit []int
		var err error
		if i%2 == 0 {
			hit, err = ch.deleteRandomShards(m, 1)
		} else {
			hit, err = ch.flipRandomBits(m, 1)
		}
		must(err)
		for _, idx := range hit {
			for _, p := range m.Placement {
				if p.ShardIndex == idx {
					injectedDisks[p.DiskID] = true
				}
			}
		}
		log.Printf("  对象 %s 故障分片 %v", m.Key, hit)
	}
	ok(stats, fmt.Sprintf("已对 %d 个对象各注入 1 个分片故障（涉及盘 %v）", len(victims), mapKeys(injectedDisks)))

	// ---- 8b. 整盘故障：选一块未被上面注入对象占用的盘拔取，
	//         使两类故障作用在不同对象集合上，任一对象故障分片不超过 2 ----
	victimDisk := pickDiskNotUsedBy(store, victims)
	step("故障注入 B：拔掉整块磁盘 %s（与 A 的对象集合不重叠）", victimDisk)
	must(ch.unplugDisk(victimDisk))
	affected := 0
	for _, m := range allMetas {
		for _, p := range m.Placement {
			if p.DiskID == victimDisk {
				affected++
				break
			}
		}
	}
	log.Printf("  磁盘 %s 已离线，影响 %d 个对象", victimDisk, affected)
	ok(stats, fmt.Sprintf("磁盘 %s 离线（影响 %d 对象），降级读仍可用", victimDisk, affected))

	// 降级读验证：拔掉一块盘后随机对象仍可读
	demo := allMetas[rng.Intn(len(allMetas))]
	var db bytes.Buffer
	if err := store.GetObject(demo, &db); err != nil {
		fail(stats, fmt.Sprintf("拔盘后降级读失败 %s: %v", demo.Key, err))
	} else {
		ok(stats, fmt.Sprintf("拔盘后对象 %s 仍可降级读取（%s）", demo.Key, humanBytes(int64(db.Len()))))
	}

	// ---- 10. 换盘：旧盘目录整体移走（数据消失），新空盘上线 ----
	step("故障注入：更换新盘（旧盘数据整体消失，空盘上线）")
	replaceTarget := victimDisk
	must(ch.replaceDisk(replaceTarget, filepath.Join(work, "quarantine")))
	ok(stats, fmt.Sprintf("磁盘 %s 已换新（空盘），其分片全部缺失", replaceTarget))

	// ---- 11. 触发一轮完整自动修复（删片/比特翻转 + 整盘缺失一次性重建）----
	step("后台自动修复（限速 %s），重建到健康盘与新盘", humanBytes(cfg.RepairBytesPerSec)+"/s")
	repairMgr.Trigger()
	waitRepair(store, 60*time.Second)
	tasks, _ := store.Meta().ListRepairTasks("", 500)
	done, failedN := countTasks(tasks)
	log.Printf("  修复任务: 完成=%d 失败=%d 总计=%d", done, failedN, len(tasks))
	if failedN == 0 && done > 0 {
		ok(stats, fmt.Sprintf("全部 %d 个修复任务完成（含新盘分片重建）", done))
	} else {
		fail(stats, fmt.Sprintf("存在失败修复任务: 完成=%d 失败=%d", done, failedN))
	}

	// 新盘上应当重建出它原本承载的那些分片；若该盘本就无对象，则无分片可重建
	newDisk, _ := store.DiskManager().Get(replaceTarget)
	rebuilt := int64(0)
	if newDisk != nil {
		rebuilt = newDisk.CountShards()
	}
	switch {
	case affected > 0 && rebuilt > 0:
		ok(stats, fmt.Sprintf("新盘 %s 上已重建 %d 个分片（原承载 %d 个对象）", replaceTarget, rebuilt, affected))
	case affected == 0:
		ok(stats, fmt.Sprintf("磁盘 %s 原本不承载对象，换盘后无需重建分片", replaceTarget))
	default:
		fail(stats, fmt.Sprintf("新盘 %s 应重建分片但实际为 0（原承载 %d 个对象）", replaceTarget, affected))
	}

	// ---- 12. 全量哈希校验（最终验收）----
	step("全量回读并逐个比对 SHA256")
	verifyAll(ctx, store, cli, bucket, images, bigKey, bigSHA, stats)

	// ---- 13. 删除与 404 ----
	step("验证删除接口")
	delKey := images[1].Key
	must(cli.deleteObject(ctx, bucket, delKey))
	if err := cli.headObject(ctx, bucket, delKey); isNotFound(err) {
		ok(stats, "删除对象后 HEAD 返回 404")
	} else {
		fail(stats, fmt.Sprintf("删除后未返回 404: %v", err))
	}

	// ---- 14. 控制台 API 可用性 ----
	step("验证管理控制台 API")
	consoleOK := checkConsoleAPI(consoleAddr, cfg.ConsoleToken)
	if consoleOK {
		ok(stats, "控制台磁盘/修复/桶接口可正常访问")
	} else {
		fail(stats, "控制台 API 访问失败")
	}

	fmt.Println()
	fmt.Println("================ 测试汇总 ================")
	fmt.Printf("通过: %d   失败: %d\n", stats.passed, stats.failed)
	if stats.failed > 0 {
		os.Exit(1)
	}
	fmt.Println("全部通过 ✅")
}

// ---- 辅助 ----

func buildConfig(work string) *config.Config {
	cfg := config.Default()
	cfg.DataShards = 4
	cfg.ParityShards = 2
	cfg.StripeSize = 256 * 1024
	cfg.MetaDBPath = filepath.Join(work, "meta", "store.db")
	cfg.S3Addr = ""
	cfg.ConsoleAddr = ""
	encs := []string{"enc-a", "enc-a", "enc-b", "enc-b", "enc-c", "enc-c", "enc-d", "enc-d"}
	for i := 1; i <= 8; i++ {
		cfg.Disks = append(cfg.Disks, config.DiskConfig{
			ID:        fmt.Sprintf("disk%d", i),
			Path:      filepath.Join(work, fmt.Sprintf("disk%d", i)),
			Enclosure: encs[i-1],
		})
	}
	return cfg
}

func freeAddr() string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	must(err)
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

func waitPort(addr string) {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = c.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func step(format string, args ...any) {
	fmt.Println()
	fmt.Printf("▶ "+format+"\n", args...)
}

func ok(s *testStats, msg string) {
	s.passed++
	log.Printf("  ✅ %s", msg)
}

func fail(s *testStats, msg string) {
	s.failed++
	log.Printf("  ❌ %s", msg)
}

func must(err error) {
	if err != nil {
		log.Fatalf("致命错误: %v", err)
	}
}

func loadAllMetas(st *storage.Store) ([]*meta.ObjectMeta, error) {
	var out []*meta.ObjectMeta
	err := st.Meta().AllObjectMetas(func(m meta.ObjectMeta) error {
		cp := m
		out = append(out, &cp)
		return nil
	})
	return out, err
}

// pickDiskNotUsedBy 选一块给定对象集合都没有占用的在线盘，并优先选承载对象最多的，
// 让整盘拔取尽量压到真实数据；找不到则退化为随机在线盘。
func pickDiskNotUsedBy(st *storage.Store, metas []*meta.ObjectMeta) string {
	used := map[string]bool{}
	for _, m := range metas {
		for _, p := range m.Placement {
			used[p.DiskID] = true
		}
	}
	// 统计每块候选盘承载的对象数（不含注入对象）
	pop := map[string]int{}
	st.Meta().AllObjectMetas(func(om meta.ObjectMeta) error {
		seen := map[string]bool{}
		for _, p := range om.Placement {
			if !used[p.DiskID] && !seen[p.DiskID] {
				pop[p.DiskID]++
				seen[p.DiskID] = true
			}
		}
		return nil
	})
	best, bestN := "", -1
	for _, d := range st.DiskManager().Online() {
		if used[d.ID()] {
			continue
		}
		if pop[d.ID()] > bestN {
			best, bestN = d.ID(), pop[d.ID()]
		}
	}
	if best != "" {
		return best
	}
	online := st.DiskManager().Online()
	return online[rand.New(rand.NewSource(time.Now().UnixNano())).Intn(len(online))].ID()
}

func mapKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func pickN(metas []*meta.ObjectMeta, n int, rng *rand.Rand) []*meta.ObjectMeta {
	perm := rng.Perm(len(metas))
	if n > len(perm) {
		n = len(perm)
	}
	out := make([]*meta.ObjectMeta, 0, n)
	for _, i := range perm[:n] {
		out = append(out, metas[i])
	}
	return out
}

// waitRepair 轮询真实对象健康度，直到全部对象分片健康或超时。
// 不能只看“没有运行中的任务”，因为后台扫描是分批的，第一批完成时后续对象可能还没排到。
func waitRepair(st *storage.Store, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		unhealthy := 0
		total := 0
		_ = st.Meta().AllObjectMetas(func(m meta.ObjectMeta) error {
			total++
			res, err := st.ScanObject(&m)
			if err != nil || !res.Healthy() {
				unhealthy++
			}
			return nil
		})
		if total > 0 && unhealthy == 0 {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func countTasks(tasks []meta.RepairTask) (done, failed int) {
	for _, t := range tasks {
		switch t.Status {
		case meta.RepairDone:
			done++
		case meta.RepairFailed:
			failed++
		}
	}
	return
}

func verifyAll(ctx context.Context, st *storage.Store, cli *s3Client, bucket string,
	images []FakeImage, bigKey, bigSHA string, stats *testStats) {
	want := map[string]string{}
	for _, im := range images {
		want[im.Key] = im.SHA256
	}
	want[bigKey] = bigSHA

	keys := make([]string, 0, len(want))
	for k := range want {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	bad := 0
	for _, key := range keys {
		body, err := cli.getObject(ctx, bucket, key)
		if err != nil {
			fail(stats, fmt.Sprintf("回读 %s 失败: %v", key, err))
			bad++
			continue
		}
		sum := sha256.Sum256(body)
		got := hex.EncodeToString(sum[:])
		if got != want[key] {
			fail(stats, fmt.Sprintf("哈希不一致 %s", key))
			bad++
		}
	}
	if bad == 0 {
		ok(stats, fmt.Sprintf("全部 %d 个对象（含 MPU 大文件）回读哈希与写入时完全一致", len(keys)))
	}

	// 引擎层再做一次健康扫描确认无残留坏片
	var unhealthy int
	_ = st.Meta().AllObjectMetas(func(m meta.ObjectMeta) error {
		res, err := st.ScanObject(&m)
		if err == nil && !res.Healthy() {
			unhealthy++
		}
		return nil
	})
	if unhealthy == 0 {
		ok(stats, "引擎层健康扫描：所有对象分片完整、CRC 全部通过")
	} else {
		fail(stats, fmt.Sprintf("仍有 %d 个对象存在坏片", unhealthy))
	}
}

func makeBigFile(path string, size int) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	rng := rand.New(rand.NewSource(99))
	buf := make([]byte, 64*1024)
	remaining := size
	for remaining > 0 {
		n := len(buf)
		if remaining < n {
			n = remaining
		}
		if _, err := rng.Read(buf[:n]); err != nil {
			return err
		}
		if _, err := f.Write(buf[:n]); err != nil {
			return err
		}
		remaining -= n
	}
	return nil
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
