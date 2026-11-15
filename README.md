# PMS 快照比对式增量同步系统

面向连锁酒店集团的增量同步中间件:300+ 门店的老 PMS(本地 SQLite,无 binlog、无更新时间字段)→ 总部数据仓库(DuckDB),带网页控制台。

## 架构

```
门店 SQLite ──► 快照比对引擎(分桶哈希) ──► 变更集 ──► DuckDB 拉链表(SCD2)
                      ▲                        │
                 meta.db(状态机)         网页控制台 / 对账 / 看板
```

- **源库**(`internal/source`):只读打开门店 SQLite,流式全表扫描,PRAGMA 发现表结构与主键
- **比对引擎**(`internal/diff`):两轮全扫 + 分桶哈希比对,只深挖哈希变了的桶
- **目标端**(`internal/target`):DuckDB,每门店一个 schema,拉链表 + 自动表结构演进
- **编排**(`internal/syncer`):批次状态机 `pending → diffed → done`,崩溃安全
- **对账**(`internal/recon`):行数 / 金额汇总 / 逐行定位
- **控制台**(`internal/web`):源库登记、表配置、批次、变更明细、对账、门店看板
- **模拟器**(`internal/sim` + `cmd/simulator`):造数、变更模拟、真实变更日志、逐版本校验

## 关键设计

### 1. 分桶哈希比对(需求 1、2)
- 每行计算 `key`(主键 / 业务键 / 整行指纹)和**可加行哈希** `h = Σ fnv64(列名, 规范值)`
- key 按 FNV 分到 K 个桶(默认 64),桶哈希 = 桶内行哈希之和;`chunk_state` 持久化上轮桶哈希
- 桶哈希一致 → 整桶跳过;不一致 → 只加载该桶的 `key_state`(逐行哈希)做精确比对,得出增/删/改
- 行哈希是**逐列可加**的:源表加列时,存量行的哈希只平移一个常量(新列默认值的列哈希),无需重扫源库(见需求 5)

### 2. 删除语义(需求 3)
- 物理删除:上轮有 key、本轮没有 → `D`,目标端写墓碑版本
- 软删除:配置软删除列(如 `is_deleted`、`deleted_at`),置位的更新记为 `S`,目标端是普通新版本
- 删了又插回同主键:按 key 比对,内容变了就是 `U`(内容完全相同则天然不可见,也无害)

### 3. 拉链表(需求 4)
每张目标表 = 业务列 + `_k, _op, _batch_id, _valid_from, _valid_to, _is_current, _row_hash`。
更新/删除先闭合当前版本(`_valid_to`),再插入新版本;删除写墓碑(`_op='D'`)。
查当前快照:`WHERE _is_current AND _op<>'D'`;查历史:按 `_valid_from/_valid_to` 区间。

### 4. 表结构演进(需求 5)
- 加列:`ALTER TABLE ADD COLUMN ... DEFAULT <源库默认值>`,历史行自动补默认值,不丢历史;
  哈希状态用常量平移(`store.AdjustHashes`),**不需要重扫源库、不产生伪变更**
- 改列类型:先尝试 `ALTER COLUMN SET DATA TYPE`;失败则旧列改名保留(`col__<ts>`)+ 新列
- 删列:目标列保留(历史不丢),哈希基准重扫重建(见"限制")

### 5. 断点续跑(需求 6)
- 比对结果(变更明细 + 新 key/chunk 状态 + `diffed` 状态)在 meta.db **一个事务**内落盘
- 目标端应用是 DuckDB **单事务**(暂存表 + UPDATE 闭合 + INSERT 版本),崩溃整体回滚
- 重启后 `ResumeAll` 接续未完成的批次:`diffed` 的重放应用,`pending` 的重跑比对;
  失败批次重同步时**续跑原批次**而不是开新批次,避免已落盘变更被新基准吞掉

### 6. 对账(需求 7)
行数、配置金额列汇总(源 `SUM` vs 目标当前存活版本 `SUM`,分位精度比对);
不一致时逐行哈希定位,输出 `missing_in_target / extra_in_target / content_diff` 的具体 key(上限 500 条)。

### 7. 看板(需求 8)
首页按门店展示:最近成功时间、同步延迟、失败率(失败批次/总批次)、最近批次链接。

## 运行

```bash
# 1. 造测试数据(4 门店 x 5 万订单,含会员/房态/日志/房价表)
go run ./cmd/simulator init -dir demo -stores 4 -orders 50000 -meta demo/meta.db

# 2. 启动服务(控制台 http://localhost:8080)
go run ./cmd/server -meta demo/meta.db -target demo/dw.duckdb -addr :8080 -truth-dir demo

# 3. 跑 10 轮 "变更 → 同步 → 逐版本校验"
go run ./cmd/simulator run -dir demo -meta demo/meta.db -rounds 10 -server http://localhost:8080
```

模拟器每轮对门店库施加:新增/更新/物理删除/软删除/同主键删后重插,每 5 轮加列、
每 7 轮改列类型,全部变更记入 `truth.db`;校验器把目标端**每个 key 的完整版本链**
(op + 内容哈希)与真实日志逐条比对,并做全量存活行比对。

其他命令:`simulator mutate -round N`(单轮变更)、`simulator verify`(独立校验)。

## 测试覆盖的关键场景

- 初始全量(30 万+ 行)→ 10 轮增量,每轮全量校验版本链与存活行 ✅
- 软删除(is_deleted / deleted_at 两种形态)✅ 物理删除 ✅ 删后重插同主键 ✅
- 无主键表:业务键(order_logs)与整行指纹(rate_plans)✅
- 加列、列类型变更(INTEGER→REAL)✅
- 同步中途 `kill -9`,重启后从断点续跑并完成 ✅
- 对账:未同步时检出金额/行数不一致并定位到具体 key,同步后恢复一致 ✅

## 已知限制

- 源表**删列**与数据变更落在同一批次窗口时,变更会被重基线吸收(目标内容仍通过
  后续对账可发现);加列、改类型无此问题
- 快照比对天然只能看到两次同步间的**净效果**(同一行多次变更只保留最终态)
- SQLite 声明为 DATE/DATETIME 的列会被驱动转成 time.Time,建议源库用 TEXT 存时间
  (模拟数据即如此);哈希规范值对两边类型做了对齐,但跨类型存储的脏数据需自行清洗
