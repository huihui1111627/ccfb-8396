# imstore — 纠删码 S3 兼容对象存储

面向医院影像科长期归档场景，用本地目录模拟物理磁盘，提供 **Reed-Solomon 纠删码 +
S3 兼容接口 + 管理控制台** 的对象存储。目标是替代昂贵的三副本 NAS：4+2 纠删码用
1.5x 冗余即可容忍任意两块盘（或两个盘柜）同时故障，而三副本需要 3x。

## 能力对照（8 项需求）

| # | 需求 | 实现 |
|---|------|------|
| 1 | Reed-Solomon 4+2，坏两块盘仍可读 | `internal/erasure`（klauspost/reedsolomon 封装），单元测试枚举全部 C(6,2) 丢片组合 |
| 2 | S3 上传/下载/列举/删除 + SigV4，标准客户端直连 | `internal/s3` + `internal/s3sig`，含流式分块签名；`test/sdk_compat_test.go` 用 **aws-sdk-go-v2 原版客户端**全流程验证 |
| 3 | 分片按故障域分散，同对象两片不落同盘/同盘柜 | `internal/placement`，先选 N 个不同 enclosure 再各选一盘，启动期强校验 |
| 4 | 换新盘重建缺失分片，限速不拖垮读写 | `internal/repair`，令牌桶限速 + 前台 IO 避让（`internal/repair/limiter.go`） |
| 5 | 元数据独立索引，百万对象前缀分页快 | `internal/index`（bbolt），有序 key + 游标 seek，O(log n + 页大小) |
| 6 | 预签名链接，到期失效 | `internal/s3sig` 的 Presign/verifyPresigned，过期独立于时钟容差判断 |
| 7 | 边收边编码边写盘，大文件不整入内存 | `internal/store` 流式条带，300 MiB 对象实测活跃堆仅 1 MiB |
| 8 | 控制台看容量/错误数/修复任务 | `internal/admin` + `internal/web`（内嵌单页应用，无外部依赖） |

## 架构

```
                 ┌─────────────────────────────────────────────┐
   S3 客户端 ───► │ s3 (SigV4 校验 / XML 协议 / Range / 多段)    │
   浏览器   ───► │ admin (Bearer Token / REST) + web (SPA)      │
                 └───────────────┬─────────────────────────────┘
                                 ▼
                          store (编排层)
              ┌──────────────┬───────────────┬──────────────┐
              ▼              ▼               ▼              ▼
         erasure        placement        index          repair / lifecycle
       (Reed-Solomon)  (故障域摆放)    (bbolt 元数据)    (重建限速/过期清理)
              └──────────────┴───────────────┴──────────────┘
                                 ▼
                  volume (本地目录 = 模拟磁盘) × N
                  每个 .ims 分片: 头 + 条带数据 + 每条带 SHA-256
```

### 数据布局

- 对象切成条带（默认每数据条带 1 MiB），每数据条带经 RS 编码成 6 个等长分片（4 数据 + 2 校验）。
- 6 个分片落在 **6 个不同盘柜的 6 块不同盘**上。
- 分片文件 `.ims` 带头部和**逐条带 SHA-256 校验表**：单比特翻转被定位到具体条带，
  读时用奇偶分片即时重建，不会丢失整个对象。
- 读路径对每个条带：能读的分片直接校验，缺失/校验失败的置空，凑够 4 片即 RS 重建。

## 目录结构

```
cmd/imstore        服务端主程序
cmd/imctl          测试/运维 CLI（造假数据、签名请求、故障注入、校验）
internal/config    配置加载与校验
internal/erasure   Reed-Solomon 编解码封装
internal/shard     分片磁盘格式（头 + 逐条带校验和）
internal/volume    模拟磁盘（上下线、容量、错误计数）
internal/placement 故障域感知摆放
internal/index     bbolt 元数据（对象/桶/多段/反向索引/生命周期规则）
internal/store     流式写入、容错读取、多段、删除
internal/s3sig     SigV4 签名校验（含 AWS 分块传输解码、预签名）
internal/s3        S3 HTTP/XML 协议层
internal/repair    重建管理器（令牌桶限速 + 前台避让 + 巡检）
internal/lifecycle 过期对象与僵死多段清理
internal/admin     控制台 REST API
internal/web       内嵌控制台界面
test               端到端脚本、故障注入器、AWS SDK 兼容性测试
```

## 快速开始

```bash
# 构建
go build -o bin/imstore ./cmd/imstore
go build -o bin/imctl   ./cmd/imctl

# 启动（仓库自带 6 盘 6 盘柜的示例 config.json）
./bin/imstore --config config.json
# S3 端点 http://127.0.0.1:8333 ，控制台 http://127.0.0.1:8333/ui/
```

## 一键端到端验收

```bash
./test/e2e.sh
```

脚本会自动完成：生成假影像 → S3 上传 → 基线哈希 → **每对象删 1 片 + 翻转 1 片** →
读穿故障 → 后台修复还原 → **整盘拔插与重建** → 预签名过期 → 多段上传校验，
最后断言每个对象的 SHA-256 与写入时完全一致。

## 手动演练（imctl）

```bash
export IMSTORE_ACCESS_KEY=imstoreadmin IMSTORE_SECRET_KEY=imstoresecret IMSTORE_REGION=us-east-1

./bin/imctl gen --out fakes --count 30 --min 16 --max 2048   # 造假影像
./bin/imctl mb  --endpoint http://127.0.0.1:8333 --bucket images
./bin/imctl upload   --endpoint http://127.0.0.1:8333 --bucket images --dir fakes
./bin/imctl verify   --endpoint http://127.0.0.1:8333 --bucket images --manifest fakes/upload-manifest.json

# 故障注入（直接改本地目录，等价于真实硬件故障）
./bin/imctl chaos --data-root data --mode delete-shard --ratio 0.1
./bin/imctl chaos --data-root data --mode flip-bit --ratio 0.05
./bin/imctl chaos --data-root data --mode remove-volume --volume data/disk3
# 确定性注入：每对象恰好坏 2 片（4+2 容忍边界）
python3 -I test/inject.py --data-root data

./bin/imctl ls       --endpoint http://127.0.0.1:8333 --bucket images
./bin/imctl presign  --endpoint http://127.0.0.1:8333 --bucket images --key K --expires 5m
./bin/imctl multipart --endpoint http://127.0.0.1:8333 --bucket images --key big --file F --part-size 256
```

## 标准 S3 客户端兼容性

```bash
go test -tags s3compat ./test/ -v
```

该测试用未修改的 `aws-sdk-go-v2` 完成建桶、PutObject（SDK 默认的流式 SigV4 分块上传）、
HeadObject、Range GET、ListObjectsV2、预签名下载、删除、列桶，全部通过。

## 测试

```bash
go test ./...                      # 单元 + 存储端到端（纠删码/修复/分页/签名/生命周期/限速）
go test -tags s3compat ./test/     # AWS SDK v2 兼容性 + 大文件内存边界
./test/e2e.sh                      # 完整故障演练验收
```

## 关键设计取舍

- **逐条带校验而非整文件校验**：比特翻转只作废一个条带，用该条带的奇偶片即可修复，
  避免一个坏字节导致整个对象不可恢复。
- **流式写盘**：写入只缓冲一个条带（默认 1 MiB 级），分片用占位头、Close 时回写条带数
  与校验表，多 GiB 对象不进内存。
- **降级写**：上传中途某盘失效，只要落盘分片数 ≥ 数据片数即提交，缺口记入元数据交由修复补齐。
- **修复限速双保险**：令牌桶约束总重建字节/秒；开启 `yield_when_busy` 时，前台有在途 IO
  则重建主动暂停。
- **故障域强约束**：配置启动即校验不同 enclosure 数量 ≥ 总分片数，不满足直接拒绝启动。

## 已知边界

- 单节点架构，元数据 bbolt 为单机存储（生产多节点需引入 raft 或元数据复制）。
- 4+2 的数学上限是同时坏 2 片；随机故障若让单对象损失 3 片则不可恢复，`imctl chaos`
  的纯随机模式可能构造出这种超出规格的极端样本，`test/inject.py` 提供严格按容忍边界的确定性注入。
- ListObjects 实现 V2 与 V1 marker 兼容；未实现对象版本控制与对象锁。
