# ErasureStore — 兼容 S3 的纠删码对象存储

面向医院影像科等**海量文件长期归档**场景的对象存储：用 Reed-Solomon 纠删码把对象切片分散到多块盘，
替代传统 NAS 三副本，显著降低存储成本，并提供静默数据损坏（bitrot）检测、自动修复与标准 S3 接口。

- **纠删码**：默认 4 数据 + 2 校验（4+2），任意坏 2 块盘仍可读
- **S3 兼容**：标准 SigV4 签名，AWS SDK / 支持 S3 的客户端可直连
- **故障域放置**：同一对象分片不共盘，尽量不共盘柜（enclosure）
- **流式处理**：边收边编码边落盘，几 GB 大文件不整体进内存
- **自动修复**：巡检发现缺片/坏片后后台重建，令牌桶限速不拖垮正常读写
- **bitrot 检测**：每个 stripe 独立 CRC32C，翻比特可被检出并重建
- **元数据索引**：bbolt 存储，前缀分页列举，百万级对象不退化
- **管理控制台**：存储桶 / 对象浏览 / 磁盘状态 / 修复任务 / 生命周期 五个功能区

## 目录结构

```
internal/
  config/      配置定义与校验
  erasure/     Reed-Solomon 流式编解码、分片文件格式与 CRC32C
  disk/        磁盘管理（目录模拟盘、盘柜故障域、容量/错误统计、上下线）
  placement/   故障域放置算法
  meta/        bbolt 元数据索引（桶/对象/MPU/修复任务/生命周期）
  storage/     存储引擎（流式写、降级读、重建、分片上传）
  s3auth/      SigV4 签名校验（含预签名、aws-chunked 解帧）
  s3/          S3 协议层（XML、错误码、兼容 SDK）
  repair/      后台巡检与重建编排、令牌桶限速
  lifecycle/   过期对象删除与未完成 MPU 中止
  console/     管理控制台 REST API 与内嵌 Web 界面
cmd/
  erasure-store/  服务端
  e2e-test/       造数 + 混沌测试（删分片/翻比特/拔盘/换盘 + 全量哈希校验）
  smoke-client/   用 AWS SDK 对运行中服务做联调冒烟
configs/
  erasure-store.json  示例配置（7 盘）
  demo.json           8 盘演示配置
```

## 快速开始

### 1. 启动服务

```bash
go build -o bin/erasure-store ./cmd/erasure-store
./bin/erasure-store -config configs/erasure-store.json
```

- S3 接口默认监听 `:9000`
- 管理控制台默认监听 `:9001`，浏览器打开后用配置中的 `console_token` 登录

### 2. 用标准 S3 客户端访问

服务实现 path-style 寻址，AWS SDK v2 配置示例：

```go
cfg, _ := config.LoadDefaultConfig(ctx,
    config.WithRegion("us-east-1"),
    config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
        "testaccesskey", "testsecretkey", "")),
)
cli := s3.NewFromConfig(cfg, func(o *s3.Options) {
    o.BaseEndpoint = aws.String("http://127.0.0.1:9000")
    o.UsePathStyle = true
})
```

支持的 S3 接口：

| 类别 | 操作 |
|------|------|
| 桶 | CreateBucket / HeadBucket / DeleteBucket / ListBuckets |
| 对象 | PutObject / GetObject / HeadObject / DeleteObject / ListObjectsV2（含 V1 marker）/ DeleteObjects |
| 分片上传 | CreateMultipartUpload / UploadPart / CompleteMultipartUpload / AbortMultipartUpload / ListParts / ListMultipartUploads |
| 其他 | 预签名 URL（PresignGetObject）、生命周期 Get/Put/Delete |

### 3. 管理控制台

五个功能区：

- **存储桶**：桶列表、对象数、总容量
- **对象浏览**：按前缀/分隔符分页浏览，支持健康检查与删除
- **磁盘状态**：每块盘容量、使用率、分片数、读/写错误数，可一键模拟拔盘/插回、重新探测
- **修复任务**：任务状态与进度条，支持暂停/恢复/取消，可动态调整重建限速
- **生命周期**：按前缀配置对象过期天数、未完成 MPU 中止天数，查看执行历史

## 核心设计

### 分片文件格式

每个对象的每个分片是一个独立文件，布局：

```
Header(16B 魔数/版本/数据分片数/校验分片数/分片序号)
Frame 0: [帧头 8B: 逻辑长度 + CRC32C][payload，按 64B 对齐补零]
Frame 1: ...
```

逐 stripe 独立 CRC，使任意位置的比特翻转都能被精确定位到具体分片并交给 RS 重建。

### 流式编码与降级读

- 上传按 stripe（默认 1 MiB）读取 → RS 编码 → 并行写各盘临时文件 → `rename` 原子落盘，
  全程只缓存一个 stripe，内存占用与对象大小无关。
- 读取时打开可用分片，缺失分片返回 `nil`，CRC 损坏的分片同样视为缺失；
  只要健康分片数 ≥ 数据分片数，调用 `Reconstruct` 即可还原（数据与校验分片都能重建）。

### 故障域放置

放置器贪心选盘：优先选尚未使用的盘柜，同条件下选剩余空间最多的盘。
`strict_enclosure=true` 时盘柜不足直接报错；默认模式退化为“不共盘、尽量不共柜”。

### 修复与限速

- 后台每 30 秒巡检一次，也可由拔盘/换盘事件立即触发（带防抖）。
- 每个不健康对象生成一个修复任务，选盘时**优先回到分片原所在盘**（换盘场景），
  否则选剩余空间最多的备盘；同对象任务去重，避免并发重建互相截断。
- 令牌桶限速器在 stripe 写入时消费配额，可在控制台动态调速（0 = 不限速），
  保证重建带宽不挤占正常读写。

### 元数据与列举

bbolt 每个桶一个嵌套 bucket，对象 key 即 bbolt key，天然有序；
列举走游标 + base64url 续页 token，配合前缀与 `/` 分隔符在索引顺序上完成，
不随对象总数增长而变慢。

## 测试

### 单元测试

```bash
go test ./...
go test -race ./...
```

覆盖：RS 编解码闭环、CRC bitrot 检测、缺片降级读、比特翻转重建、整盘拔插、
MPU 并发上传、故障域放置、SigV4 签名校验（合法/篡改/过期）、前缀分页、令牌桶限速。

### 端到端混沌测试

```bash
go run ./cmd/e2e-test -root data/e2e -count 40
```

流程：

1. 生成 40 个大小不一（8KB~6MB）的假 DICOM 影像，记录每个文件 SHA256
2. 用 AWS S3 SDK 建桶、上传（走真实 SigV4），验证前缀分页与放置约束
3. 验证预签名链接（有效可下载、过期 403）
4. 12MB 大文件经 transfer manager 分片上传并校验
5. 混沌注入（总量控制在 4+2 容忍域内）：
   - 随机删除分片文件
   - 随机翻转分片字节（bitrot）
   - 整块盘离线（拔盘）→ 验证降级读
   - 旧盘目录整体移走、空盘上线（换盘）
6. 后台自动修复后，**逐个对象回读比对 SHA256，全部与写入时一致**，并做引擎层健康扫描

连续多轮、`-race` 模式下均 17 项检查全部通过。

## 配置说明

```jsonc
{
  "data_shards": 4,          // 数据分片数
  "parity_shards": 2,        // 校验分片数（允许同时坏的盘数）
  "stripe_size": 1048576,    // 单 stripe 逻辑大小（字节）
  "s3_addr": ":9000",
  "console_addr": ":9001",
  "console_token": "admin123",
  "access_key": "...",
  "secret_key": "...",
  "region": "us-east-1",
  "strict_enclosure": false, // true=分片必须落在不同盘柜
  "repair_bytes_per_sec": 33554432, // 修复限速，0=不限
  "meta_db_path": "data/meta/erasure-store.db",
  "disks": [
    {"id": "disk1", "path": "data/disk1", "enclosure": "enc-a"}
    // 磁盘数必须 >= data_shards + parity_shards，建议额外留备盘供重建
  ]
}
```

## 与三副本 NAS 的对比

| 维度 | NAS 三副本 | ErasureStore（4+2） |
|------|-----------|---------------------|
| 存储开销 | 300% | 150% |
| 容错 | 坏 2 份副本 | 坏任意 2 块盘 |
| 静默损坏 | 无检测，可能读到坏副本 | 逐 stripe CRC，检出即重建 |
| 换盘恢复 | 人工/全量拷贝 | 后台自动、限速重建 |
| 访问协议 | 文件协议 | 标准 S3，生态工具直接对接 |
