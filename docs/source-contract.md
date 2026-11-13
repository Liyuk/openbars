# 通用 K 线来源信息契约（调用方适配器用）

本文冻结 openbars 核心对**调用方适配器**暴露的「通用 K 线 + 来源信息」契约。核心只产出
通用 K 线和来源信息，不认识任何调用方的官方表；调用方把行映射到自己的表结构，是适配器
自己的责任。

- 适用版本：`openbars-storage-v1`（`SCHEMA_VERSION = 1`），`research-daily-v2` /
  `crypto-daily-v1` / `cn-daily-v1` / `hk-daily-v1` 规范层共用。
- 权威字段声明：`src/openbars/storage/schema.py`（落盘 / 数据库）与
  `src/openbars/core/schema.py`（内存日线行）。
- 本文只描述 openbars 自己的字段。调用方官方表名、外键和整数 id 不在本契约内。

## 1. 两层数据与读取路径

| 层 | 内容 | 读取入口 |
| --- | --- | --- |
| raw | 每个来源、每次抓取的原始 bar；保留来源身份和抓取血缘 | `backend.raw.scan_rows(...)`、`backend.raw.scan(...)`、raw Parquet / `raw_bars` 表 |
| canonical | 按 `policy_version` 选源、对账后的规范 bar；一行一个 `(instrument_id, 时间, policy_version)` | `backend.canonical.scan_rows(...)`、`backend.reader.bars(...)`、canonical Parquet / `canonical_bars` 表 |

默认后端是本地 Parquet + DuckDB；`storage.postgres` 的 `native` 模式使用同一份字段声明。
`openbars serve` 的 `/api/v1` 是本地 UI 接口，**不是**稳定的适配器契约；适配器应直接读
storage 层或 canonical / raw 数据文件。

同键去重规则是「最后写入者胜出」（raw 的键含 `revision`；canonical 的键不含
`revision`，被覆盖的历史行只用于 `available_by` 时点读取）。重试同一抓取、重复 upsert
不会产生新行。

## 2. raw 层字段

`RAW_BARS_SCHEMA`（Parquet 另含 `extra`；Postgres native 也有 `extra JSONB`）。
`?` 表示可空。

| 字段 | 类型 | 含义 / 空值语义 |
| --- | --- | --- |
| `provider` | string | 数据源 id（如 `massive`、`alpaca`、`baostock`）。必填，非空。 |
| `market` | string | 市场分区键，同时写入文件：`us` / `cn` / `hk` / `crypto`。必填。 |
| `feed` | string | 来源内部 feed / venue / 数据集。必填；来源没有 feed 概念时为 `""`（不是 null）。 |
| `adjustment` | string | 源交付的复权口径（如 `raw`、`split`）。与 `provider`、`feed` 组成来源身份三元组。必填。 |
| `symbol_as_returned` | string | provider 原样返回的代码。必填，非空。 |
| `instrument_id` | string? | openbars 已解析的稳定字符串 id；解析前为 null。日线原生 raw 行通常在写入前由 `extra["instrument_id"]` 提供。 |
| `interval` | string | `1d` / `1h`。必填。 |
| `ts_start` | timestamp(us, UTC) | bar 开始时刻。必填，带时区。 |
| `session_date` | date32 | 交易所本地交易日。必填。 |
| `price_basis` | string | 价格基准：`raw` / `split_adjusted` / `total_return_adjusted` / `provider_defined` / `unknown`。必填。 |
| `adjustment_scope` | string | 已应用的调整范围：`none` / `splits` / `dividends` / `splits_and_dividends` / `provider_defined` / `unknown`。必填。 |
| `adjustment_as_of` | date32? | 复权知识日期；未知为 null。 |
| `session_semantics` | string | `regular` / `extended` / `provider_aligned` / `utc_day` / `unknown`。必填。 |
| `currency` | string? | 价格币种，3 位大写 ISO 4217；未知为 null。 |
| `volume_basis` | string | `as_traded` / `split_adjusted` / `provider_defined` / `unknown`。必填。 |
| `revision` | int32 | 源 bar 版本，非负整数。新 revision 在 raw 中是**独立行**，不覆盖旧 revision。 |
| `open` / `high` / `low` / `close` | decimal128(20,8) | 精确价格，必填；不经过二进制 float。 |
| `volume` | decimal128(28,8)? | 成交量。**null = 源没有给成交量，不是 0**。 |
| `vwap` | decimal128(20,8)? | 源给的 VWAP；未知为 null。 |
| `trade_count` | int64? | 成交笔数；未知为 null。 |
| `fetch_id` | string | **抓取批次标识**。必填，非空；同一批次的多根 bar 共享同一个值。与 `provider` 一起对应 `state.db` 的 `fetch_log` 主键 `(provider, fetch_id)`。原生抓取形如 `<job_id>-<随机后缀>`；兼容导入行形如 `compat:<行 id>`。 |
| `source_artifact_ref` | string? | 响应归档引用（`data/raw-archive` 下的相对路径或等价的稳定引用）；没有归档为 null。 |
| `ingest_run_id` | string? | 上游导入批次 id；原生抓取为 null。 |
| `ingested_at` | timestamp(us, UTC) | openbars 落库该 raw 行的时刻，必填。canonical 的 `available_at` 优先取它。 |
| `extra` | JSON object? | 原生存储特有：没有独立列、但需要保留的 provider 字段（如 `tradestatus` / `isST` / `preclose`）。未知为 null。兼容模式不保留。 |

## 3. canonical 层字段

`CANONICAL_BARS_SCHEMA`。`?` 表示可空。canonical 行来自**一根被选中的源 bar**：价格和
成交量始终同源（whole-bar selection）。

| 字段 | 类型 | 含义 / 空值语义 |
| --- | --- | --- |
| `instrument_id` | string | openbars 稳定字符串 id；来源未解析时对账器可能用代码本身作为分组键，因此适配器应把它当**不透明字符串**，不要假定整数或官方表外键。 |
| `market` | string | `us` / `cn` / `hk` / `crypto`。 |
| `interval` | string | `1d` / `1h`。 |
| `ts_start` | timestamp(us, UTC) | bar 开始时刻；日线为会话日 00:00 UTC（占位，语义看 `session_date`）。 |
| `session_date` | date32 | 交易所本地交易日。 |
| `session` | string | `regular` / `extended` / … |
| `policy_version` | string | 选数口径版本（如 `research-daily-v2`、`cn-daily-v1`）。同一 `(instrument_id, 时间)` 允许多个口径并行。 |
| `provider` / `feed` / `adjustment` | string | 被选中源 bar 的来源身份三元组（`feed` 无则为 `""`）。 |
| `revision` | int32 | 规范行修订号；重写同一键时会递增，因此它**不一定等于** raw 行的 `revision`。追溯抓取批次请用 `source_fetch_id`。 |
| `open` / `high` / `low` / `close` | decimal128(20,8) | 规范价格，必填。 |
| `volume` | decimal128(28,8)? | 规范成交量。canonical 选取要求源 bar 有成交量；旧行或特殊适配路径仍可能为 null。 |
| `vwap` | decimal128(20,8)? | 选中源的 VWAP；未知为 null。 |
| `trade_count` | int64? | 成交笔数；未知为 null。 |
| `flags` | list<string> | 质量事实标记（`DIVERGENT` / `SPIKE_SUSPECT` / `VOLUME_MISSING` / `STALE` / `ADJUSTMENT_UNVERIFIED` / `LIMIT_UP` / `LIMIT_DOWN` 等）。空 list 表示没有标记。 |
| `selection_reason` | string | 选源原因：`primary_source` / `primary_source_corroborated` / `compatible_gap_fill` / `fill_<provider>` / `spike_replaced`。 |
| `quality_status` | string | 由 flags 汇总的单一状态：`complete` / `divergent` / `volume_missing` / `spike_suspect`。它与 `freshness` 是**独立**维度。 |
| `capabilities` | list<string> | 有序能力字符串，如 `price_basis:split_adjusted`、`adjustment_scope:splits`、`session_semantics:provider_aligned`、`volume_basis:split_adjusted`、`currency:USD`、`adjustment_as_of:YYYY-MM-DD`、`price_ohlc`。**币种从 `currency:<ISO4217>` 读取**；native canonical 没有独立 `currency` 列。 |
| `source_fingerprint` | string | 选中源 bar 的不透明指纹，用于内容比对和同一源 bar 判定，**不是**外键。重写或换源后可能变化。 |
| `price_source` | string | 选中价格源的 `provider:feed`。 |
| `volume_source` | string? | 选中成交量源；whole-bar selection 下通常与 `price_source` 相同，未知为 null。 |
| `sources_seen` | list<string> | 该次对账见过的来源 provider 列表（去重）。 |
| `reconcile_rule` | string | 本次选数使用的规则标签；一般等于 `selection_reason` 或 `policy_version`。 |
| `generated_at` | timestamp(us, UTC) | 规范行生成时刻，必填。`as_of` 读取用它做知识日期过滤。 |
| `source_fetch_id` | string? | **#202 新增、可空**：被选中 raw 行的 `fetch_id`。这是 canonical → raw 抓取批次的稳定指针；旧文件 / 旧库没有该列时读回 null，兼容导入行可能为 `compat:<行 id>`。 |
| `available_at` | timestamp(us, UTC)? | 这根 bar **最早可以用于研究**的时刻，必填语义见 §4。null = 未知，`available_by` 时点读取会排除它。 |
| `freshness` | string? | `provisional`（当日临时）或 `final`（正式）。**缺列 / null 读作 `final`**。 |

### `selected_source_bar_id` 的定位（重要）

`selected_source_bar_id` 是 openbars **内部概念**：内存日线行
（`CanonicalDailyRow`）里有一个可选的**字符串**来源行标识，某些兼容适配路径会用它保存
调用方自己的行 id。它不是 openbars 通用契约的一部分：

- native canonical（Parquet / `canonical_bars`）**没有**这一列；
- 它不是调用方官方表的整数外键，也不保证是数字、不保证存在；
- 适配器不得把它当作跨表 join 键或持久化契约；需要追溯来源时用
  `source_fetch_id` + 来源身份（§5）。

## 4. `freshness` 与 `available_at`

`freshness` 是**时效分层**，不是质量：

- `provisional`：当日临时数据，可能还会被正式数据替换；
- `final`：正式数据；
- 旧行缺列或为 null 时按 `final` 处理；
- 一旦某个 cell 有正式行，同一天再来的临时行不会覆盖它；临时行升级为正式行时
  canonical `revision` 递增并产生审计事件。

`available_at` 是**时点可得性**（point-in-time）：

- 语义是「这根 bar 最早可以被研究使用的时刻」，不是来源交付 SLA；
- 取选中价格源 raw 行的 `ingested_at`；内存中尚未落库的 bar 用 canonical 写入时刻
  （只会更保守，不会前视）；兼容导入行用上游选中源行的接收时刻；
- **同一个源 bar 保留最早的可得时间**：同键重写且 `source_fingerprint`、来源身份、
  `price_source` / `volume_source`、OHLCV、`vwap`、`trade_count` 都没变时，取新旧值中
  较早的非空值；任一项变化（换源、新 revision、价格 / 成交量修正）就用新行自己的值；
- 不带 `available_by` 读取时返回当前可见行；带 `available_by` 时只返回
  `available_at <= available_by` 的最新版本，null 不可用。

来源自身的延迟 / 就绪状态见 [`source-readiness.md`](source-readiness.md)；这些是来源声明，
不是本契约的保证。

## 5. 适配器如何追溯「数据源 / 抓取批次 / 临时或正式 / 可得时间」

一次抓取批次可以用下面的最小路径还原：

1. **数据源**：canonical 的 `provider` / `feed` / `adjustment`（以及 `price_source`）；
   需要序列语义时读 `capabilities`（`price_basis:*`、`adjustment_scope:*`、
   `session_semantics:*`、`volume_basis:*`、`currency:*`）。
2. **抓取批次**：read 侧的 raw `fetch_id`；canonical 用 `source_fetch_id` 指回同一值。
   `state.db` 的 `fetch_log` 主键 `(provider, fetch_id)` 提供该批次的窗口、状态、行数、
   归档引用和起止时间。
3. **临时 / 正式**：canonical `freshness`（null 视为 `final`）。
4. **可得时间**：canonical `available_at`；未知为 null，不要用 `generated_at` 冒充。

canonical → raw 的精确匹配方式（原生生产者）：

```text
raw.provider      = canonical.provider
raw.feed          = canonical.feed
raw.adjustment    = canonical.adjustment
raw.interval      = canonical.interval
raw.时间           = canonical.时间（1d 用 session_date，1h 用 ts_start）
raw 的实体         = coalesce(raw.instrument_id, raw.symbol_as_returned) = canonical.instrument_id
raw.fetch_id      = canonical.source_fetch_id
```

`source_fetch_id` 是稳定指针；不要用 `canonical.revision` 去 join raw，因为规范层的
`revision` 会因为临时转正式或重写而递增，可能大于 raw 行的 revision。

如果 `source_fetch_id` 为 null（旧文件、旧库、某些兼容导入行），只能按来源身份 + 日期
做**尽力**匹配，无法保证唯一追溯到某个抓取批次。

## 6. 空值语义速查

| 字段 | null 的含义 |
| --- | --- |
| raw `instrument_id` | 代码尚未解析为 openbars instrument |
| raw `volume` | 源没有提供成交量（不是 0） |
| raw `vwap` / `trade_count` | 源没有提供 |
| raw `source_artifact_ref` / `ingest_run_id` | 没有归档 / 不是上游导入 |
| canonical `source_fetch_id` | 生产者不知道抓取批次（旧行或导入行） |
| canonical `available_at` | 可得时间未知；`available_by` 读取不可用 |
| canonical `freshness` | 缺列 / null 按 `final` 处理 |
| canonical `volume` / `vwap` / `trade_count` | 未知或该路径不提供 |
| canonical `capabilities` 里没有 `currency:*` | 币种未知 |
| `selected_source_bar_id` | 不适用于 native canonical；不要依赖 |

## 7. 版本与兼容

`SCHEMA_VERSION` 只在列名、类型、唯一键或分区变化时递增。新增**可空**列属于向后兼容的
加性变更：

- 新 reader 读旧 Parquet 文件 / Postgres 当前行时，缺列按 null 处理；Postgres native 的写连接
  （`write=True`）打开时会为 `canonical_bars` 和 `canonical_bars_history` 补列，只读旧库不做 DDL；
- 旧 reader 读新文件时按 schema 版本拒绝未知的更高版本；
- `source_fetch_id` 是 #202 的加性可空列，`SCHEMA_VERSION` 保持 1。

适配器必须容忍列缺失（按 null）和值未知，不要把来源信息字段当成必然存在。调用方的
官方表结构、整数主键和写入目标由适配器自己维护；openbars 核心不感知、也不引用它们。
