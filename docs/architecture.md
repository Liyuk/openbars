# openbars 架构设计

状态：draft v0.1 · 2026-11-11（11-11 补充 §11 搬运与兼容） · 讨论见 GitHub Discussion「v0.1 架构与范围」

## 1. 目标与非目标

**目标**

- 在**免费额度**内，把用户选定的市场 × 粒度的 K 线稳定拉全，并持续增量更新。
- 多源数据经清洗、对齐、仲裁后形成**唯一的规范层（canonical）**，每根 bar 可追溯来源与质量。
- 高抽象、分层清晰：新增一个市场或数据源只需新增 provider 插件与口径配置，不改核心。
- 将来能替换上游内部实现（一套已在运行的研究 backend）的抓取与清洗服务，并保持口径兼容（见 §11）。

**非目标**

- 不分发、不托管数据；不做付费数据转售。
- 不做实时行情、逐笔、交易执行、回测、策略。
- v0.x 不追求分钟线全市场；分钟线只作为可选能力。

## 2. 分层总览

```mermaid
flowchart TD
    subgraph Entry["① 入口 Entry"]
        CFG[config.yaml + .env<br/>markets × intervals 多选]
        CLI[CLI: openbars init / plan / run / backfill / verify / status]
    end

    subgraph Orchestration["② 编排 Orchestration"]
        PLAN[Planner<br/>期望网格 − 已有覆盖 = 待抓 Job]
        ORCH[Orchestrator<br/>Job 队列 · Checkpoint · 重试 · 熔断]
        NODE[CollectorNode<br/>local / remote]
    end

    subgraph Quota["③ 配额闸门 Quota Gate"]
        GATE[QuotaGate<br/>持久化令牌桶 · 日/分/月额度 · retry_at]
    end

    subgraph Providers["④ Provider 适配器（插件）"]
        CAP[SourceCapability<br/>市场×粒度·历史深度·额度·条款]
        P1[alpaca] --- P2[massive] --- P3[yfinance] --- P4[binance …]
    end

    subgraph Data["⑤–⑧ 数据管线"]
        RAW[(Raw 原始层<br/>每源一份 · 不可变 · fetch_log)]
        NORM[Normalize 标准化<br/>InstrumentMaster · TradingCalendar · 公司行动 · 时区/会话]
        REC[Reconcile 合并仲裁<br/>优先级 · 容差 · 质量标记 · 审计]
        CAN[(Canonical 规范层<br/>唯一源 · 不复权 + 公司行动)]
    end

    subgraph Store["⑨ 存储后端"]
        PQ[Parquet + DuckDB 默认]
        PG[Postgres 可选]
    end

    subgraph Read["⑩ 读取 API"]
        LIB[Python: BarReader<br/>查询时派生复权]
        HTTP[本机只读 UI]
    end

    CFG --> CLI --> PLAN --> ORCH --> NODE --> GATE --> Providers
    Providers --> RAW --> NORM --> REC --> CAN
    RAW -.-> Store
    CAN -.-> Store
    Store --> LIB --> HTTP
    REC -. 缺口/冲突回写待补 Job .-> PLAN
```

依赖方向自上而下：上层依赖下层的**接口**（`typing.Protocol` / ABC），不依赖实现。`core/` 中的数据模型被所有层共享，自身不依赖任何层。

| 层 | 包 | 职责 | 不负责 |
| --- | --- | --- | --- |
| ① 入口 | `openbars.config`, `openbars.cli` | 读配置、展开环境变量、校验多选组合、命令行 | 业务逻辑 |
| ② 编排 | `openbars.orchestrator` | 计算待抓网格、生成幂等 Job、Checkpoint、重试/熔断、分派到节点 | 具体 HTTP |
| ③ 配额 | `openbars.quota` | 按 provider（及 key）共享的持久化额度；所有出站请求必须先拿令牌 | 重试策略 |
| ④ Provider | `openbars.providers` | 声明能力；把请求翻译成源 API；返回 `RawBatch` | 合并、复权 |
| ⑤ 原始层 | `openbars.raw` | 按源原样落盘（追加、不可变）+ `fetch_log` | 清洗 |
| ⑥ 标准化 | `openbars.normalize` | 代码 → `instrument_id`；时区/会话/日线切分；公司行动；单源清洗 | 跨源决策 |
| ⑦ 仲裁 | `openbars.reconcile` | 多源对齐、优先级、容差、兜底拼接、质量标记、审计记录 | 抓取 |
| ⑧ 规范层 | `openbars.reconcile` 输出 | 唯一源 `bars_canonical` | — |
| ⑨ 存储 | `openbars.storage` | Raw/Canonical/主数据/日志的读写，原子写入、分区 | 业务规则 |
| ⑩ 读取 | `openbars.api` | 查询 canonical、派生复权、导出；可选的本机只读 HTTP（`openbars serve`，仅回环，见 §13） | 写入 |

## 3. 核心数据模型（`openbars.core.models`）

所有模型为不可变 dataclass（frozen），存储侧对应 Arrow/Parquet schema。

### 3.1 枚举

- `Market`: `us` | `crypto` | `cn` | `hk`
- `AssetClass`: `equity` | `etf` | `adr` | `index` | `crypto_spot` | `fund` …
- `Interval`: `1d` | `1h` | `30m` | `15m` | `5m` | `1m`（带 `is_intraday`、`duration`）
- `Session`: `regular` | `pre` | `post` | `continuous`（加密）
- `Adjustment`: `raw` | `split` | `split_dividend` | `all` | `qfq` | `hfq`（`qfq` 前复权 / `hfq` 后复权只用于 A 股 `cn-daily-v1`：落库存 `raw`，查询时由复权因子派生，见 §3.4）
- `QualityFlag`（位集合）：`divergent`（跨源超容差）、`spike_suspect`（无公司行动的大幅跳变）、`volume_missing`、`single_source`、`filled_from_fallback`、`stale`（源晚到/未更新；读取路径上还表示“抓取时间早于之后某次拆股，价格口径可能过时”，见 §3.4）、`gap`、`revised`（源修订过历史）、`adjustment_unverified`（该 instrument 没有任何公司行动行 / A 股复权因子行，无法验证/派生所请求的复权口径，bar 仍保持落库口径，见 §3.4）、`limit_up` / `limit_down`（A 股收盘价恰在当日涨停 / 跌停价，§3.4.4）、`suspended`（A 股停牌日，`tradestatus=0`，§3.4.4）；三个 A 股标记不改变 `quality_status`（仍是 `complete`，flags 是事实来源）

### 3.2 Instrument 与代码映射（解决 ticker 重用串台）

```text
Instrument
  instrument_id: str        # 稳定内部 ID，例: "us:eq:01J…"（ULID），永不复用
  market: Market
  asset_class: AssetClass
  name: str | None
  exchange: str | None      # MIC, e.g. XNAS
  currency: str
  listed_on / delisted_on: date | None
  external_ids: dict        # figi / cik / isin / provider-native id

SymbolMapping               # 代码在某段时间指向哪个 instrument
  instrument_id, provider ("*" = 通用), symbol
  valid_from: date, valid_to: date | None
```

原则：**bar 永远挂在 `instrument_id` 上，不挂在 ticker 上**。provider 返回的 symbol 必须结合 bar 日期经 `InstrumentMaster.resolve(symbol, on=date, provider=…)` 映射；映射不到或歧义时进入 `unresolved` 队列，不写入 canonical。

已知失败模式：ticker 被另一家公司回收后，按 ticker 存的历史会把新公司的 bar 挂到旧公司的 instrument 上。下面的规则（实现在 `openbars.normalize.*`，存储层只做持久化）保证这种情况不发生：

- **稳定 ID**：`<market>:eq:<26 位 ULID 形 id>`，永不复用。已知 share-class FIGI 时 id 由 `sha256("share_class_figi:<FIGI>")` 前 128 位的 Crockford base32 确定性导出，否则是随机 ULID；新建前总是先按外部 id（FIGI / provider-native id）查找，已存在则复用同一个 instrument。`asset_class`（含独立的 `etf`）是属性，不进 id。可用 `upstream_ids` 表记录 `instrument_id` ↔ upstream 整数 id（兼容层用，一对一）。
- **身份证据**（`normalize.identity`）：只有 share-class FIGI 或 provider-native id 能构成稳定 key；仅 ticker（`symbol_only_evidence`）、仅 CIK（`issuer_only_evidence`）、或只是复述 ticker 的 `symbol:` 兜底 id 都是 `unresolved`，不能创建、改名、回收或质疑任何 instrument。稳定 key 冲突 → `disputed`；单源 → `provisional`；多源互证 → `verified`。`observed_at` / `as_known_at` 必须带时区。
- **上市清单入库**（`InstrumentDirectory.ingest`，输入为任意 provider 的 `ListingRecord` 快照）：新稳定身份 → 新 instrument + 开放映射（`valid_from` = 上市日，无则首次观测日）；**ticker 回收**（该 symbol 在另一 instrument 上仍有开放映射）→ 旧映射 `valid_to` 关在新上市日前一天，没有上市日则关在旧映射最后一次被观测的日期，两者都定不出来就进 `unresolved`，绝不猜；**改名**（同一稳定身份换 symbol）→ 旧 symbol 映射关闭，新 symbol 在同一 `instrument_id` 上开启；**退市** → 写 `delisted_on` 并关闭开放映射（provider 把“换 ticker”也报成退市时只关旧 symbol 的窗口）。默认股票池为快照中全部普通股 + ETF，额外类型（如 ADR、基金）只来自配置，默认为空；ticker type → `AssetClass` 的映射见 `normalize.security_types`，未列入的类型（优先股、权证等）不会被写成股票。
- **`resolve` 规则**：只看同市场映射，`valid_to` 含当日；provider 专属映射优先于 `"*"`；同优先级多个 instrument → 歧义 → `None`。日期没被任何窗口覆盖时有一个**唯一历史回退**：该 symbol 历史上只绑定过一个 instrument，且日期不在其最后一个已关闭窗口之后，才返回该 instrument（应对首次观测日晚于真实上市日）；只要曾绑定过两个及以上 instrument（回收），**永远不回退**，未覆盖日期一律 unresolved。
- **`unresolved` 队列**：映射不到、歧义、或证据冲突的 bar / 清单行写入 `ref/unresolved.parquet`（含 `kind`、`reason_code`、`symbol`、`provider`、`session_date`），`unresolved_count()` 供 `status` 展示；这些 bar 不写入 canonical。`assign_instrument` / `assign_instruments` 是纯函数式的入库助手：解析出 id 的 bar 填上 `instrument_id`，其余留空并入队。队列键 `UnresolvedRecord.key`：bar 按 `(provider, symbol, 日)`、清单行按 `(provider, symbol)`（无日期）、公司行动（`kind="corporate_action"`）按 `(provider, symbol, ex_date, 行动种类)`（`session_date` 存 ex-date，`detail` 存行动种类），重复入库同一事件不会让队列膨胀。

#### 3.2.1 加密交易对主数据（`openbars.normalize.crypto_pairs`）

加密现货的 instrument 是**某交易所上的某个交易对**，不走 ULID / FIGI，也不使用 `<market>:eq`：

- **id 规则**：`crypto:spot:<venue>:<BASE>-<QUOTE>`，例 `crypto:spot:binance:TEST-USDT`。由 `(venue, base, quote)` 确定性导出，永不复用；同一交易对再次上架复用同一个 id，任何一段写错就是另一个 id，已有 id 不会被“修正”。`venue` 为小写 `[a-z0-9]+` slug（`BINANCE` / `Binance` → `binance`），`base` / `quote` 为大写字母数字资产代码，空值或含 `-` / `:` 一律拒绝。`market_of` 取 id 首段，自然得到 `crypto`。
- **base / quote 分开存**：不拆单一 symbol 字符串。`Instrument.external_ids` 固定含 `base` / `quote` / `venue` 三个键（另有信息性的 `provider_symbol`）；`currency` = quote（如 `USDT`，不要求 3 位 ISO）；`exchange` = venue 的大写形式（`BINANCE`）；`asset_class=crypto_spot`。读取走 `pair_assets(instrument) -> (base, quote)`、`pair_venue(instrument)`、`CryptoPair.parse(id)`。
- **不同交易所的交易对是不同 instrument**，即使 base/quote 相同，也不能跨所合并、不能拿来对价格。
- 同一交易所、同一 base、不同 quote（`TEST-USDT` vs `TEST-USDC`）也是两个 instrument。
- **映射只写 provider 专属 symbol**（`binance` + `TESTUSDT`、`coinbase` + `TEST-USDT`），不写通用 `"*"` 的 `BASE-QUOTE`：两个交易所共用这种显示形式，`"*"` 会造成歧义甚至把它们并成一个。`resolve("TESTUSDT", provider="binance", market=CRYPTO)` 因此只会命中 Binance 的交易对。
- **入库**（`ingest_crypto_pairs(master, listings, as_of=…)`，输入 `CryptoPairListing`）：按确定性 id 查找，已存在则复用（名称更新、`listed_on` 取较早者），并确认/开启该 `(provider, symbol)` 的映射；退市（`active=False` 或带 `delisted_on`）写 `delisted_on` 并关闭该 provider symbol 的开放窗口（`valid_to` 含当日）；已退市的交易对被再次报为活跃时复用同一 id，在已关闭窗口之后开新窗口。非法 venue/base/quote、同一批内 `(provider, symbol)` 被不同交易对认领、或该 symbol 已在另一交易对上开放，均进 `unresolved`（计数并写入队列，不抛异常、不合并）。同一 master 里的美股 instrument 不受影响。
- **测试覆盖**：入库 / resolve / 退市关窗 / 幂等重入库等用例对内存、Parquet、Postgres native 三种 master 参数化运行；Postgres 部分标记 `postgres_live`，需设置 `OPENBARS_TEST_PG_DSN`（每个用例使用临时 schema），默认测试套件会跳过。
- **编排器 / CLI 已接入（#51）**：`openbars run` 与 `openbars plan --submit`（写路径）在规划前自动对已启用、声明 `crypto` 能力的 provider（binance / coinbase）调用 `list_instruments(Market.CRYPTO)`，经 `listing_from_provider_instrument` 转换后 `ingest_crypto_pairs` 入库（`cli.wiring.ingest_crypto_listings`；缺 base/quote 的行计入 unresolved，某个 venue 列表失败只记一条 note 并继续，不会让整个 run 失败），随后以 `DefaultPlanner(instruments=backend.instruments)` 规划。不会对 alpaca / massive / yfinance 调用；仅美股配置不产生额外网络请求，也不入库加密交易对。`plan`（dry-run）不入库、不调用 `list_instruments`、不写 master；master 里已有交易对时可用于规划，否则沿用 symbol 启发式。
- 不改 instruments 表 schema / `SCHEMA_VERSION`，也不新增列。

#### 3.2.2 A 股证券 id（`openbars.normalize.cn_instruments`）

设计见 [discussion #54](https://github.com/Liyuk/openbars/discussions/54)（issue #56）。本节只含纯函数，尚无上市清单入库与 provider。

- **格式**：`cn:eq:<venue>:<code>`。`venue` 为小写 slug：`xshg`（上交所，含科创板）或 `xshe`（深交所，含创业板）；北交所（`xbse`）预留，当前拒绝。`code` 恰为 6 位数字，保留前导 0（`000001`、`600000`、`688001`）。`asset_class` 不进 id。`market_of` 取首段得到 `cn`。
- **必须带交易所**：裸 6 位代码不是合法 id，也不能被归一化。
- **常见写法归一化**（`normalize_cn_symbol`，大小写不敏感）：`600000.SH` / `600000.SS` / `sh.600000` / `SH600000` → `xshg` + `600000`；`000001.SZ` / `000001.SZSE` / `sz.000001` / `SZ000001` → `xshe` + `000001`。
- **代码复用与代际**：已退役的 id 永不给新公司复用。首次上市为 `cn:eq:xshg:600000`；同一 `(venue, code)` 再次上市则为 `cn:eq:xshg:600000:g2`（第 2 代，之后 `g3` …）。第 1 代无后缀，`:g1` 不是合法写法。`cn_instrument_id(venue, code, generation=1)` 构造，`parse_cn_instrument_id` 反解为 `(venue, code, generation)`，非法部分抛 `ValueError`。
- 选择下一代的逻辑（读取已退役 id）属于后续的上市清单入库，不在本模块。

#### 3.2.3 港股证券 id（`openbars.normalize.hk_instruments`）

设计见 [discussion #68](https://github.com/Liyuk/openbars/discussions/68)（issue #70）。

- **格式**：`hk:eq:xhkg:<code>`。`venue` 仅为 `xhkg`（港交所，含主板与 GEM）。`code` 恰为 5 位数字，保留前导 0（`00700`、`09988`）。`asset_class` 不进 id。`market_of` 取首段得到 `hk`。
- **常见写法归一化**（`normalize_hk_symbol`）：`00700` / `700` / `0700.HK` / `HK.00700` / `HK00700` / `xhkg:00700` → `xhkg` + `00700`。港股只有一个交易所，裸 1–5 位数字是明确的（与 A 股不同）。
- **代码复用与代际**：与 A 股相同，已退役 id 永不复用；再次上市为 `:g2`、`:g3` …。第 1 代无后缀，`:g1` 不是合法写法。`hk_instrument_id` / `parse_hk_instrument_id` 构造与反解。
- **美股 ADR 关联**：港股正股与美股 ADR 是两个 instrument。正股的 `external_ids["us_adr"]` 指向美股 ADR 的 id；ADR 的 `external_ids["hk_ordinary"]` 指向港股正股 id（`hk_adr_external_ids` / `us_hk_ordinary_external_ids`）。不把 ADR ticker 写进港股 id。
- **yfinance `.HK` 映射**（#71）：`to_yahoo_hk_ticker` 把 5 位代码 / `hk:eq:xhkg:00700` 写成 Yahoo 常见的 `0700.HK`（去掉一位前导 0）。适配器港股源身份为 `yfinance/yahoo/raw`（`auto_adjust=False`，Discussion #68 默认；美股仍是 `yfinance/yahoo/split`），按 XHKG 丢弃非交易日行；不把 Yahoo 复权序列当规范层。规划器接入见 #77（只用 yfinance；AKShare 不进规划器）。规范层见 #99。
- **复权口径**（#75 / #99）：规范层 profile 为 `hk-daily-v1`，**落库只存不复权（`RAW`）**，查询时由 `openbars.normalize.hk_adjust`（薄封装 `adjust_rows`）派生 `SPLIT` / `SPLIT_DIVIDEND` / `ALL`（默认读取 `SPLIT`，与美股同一套 Adjustment 枚举，不是 A 股的 QFQ/HFQ）。拆股 `hk_split`、合股 `hk_consolidation` 都是 `kind=SPLIT`（ratio = 新/旧，合股 < 1）。红股 `hk_bonus` 编码为带 `ratio` 的股票股利，**默认不按拆股套用**（与美股 `include_stock_dividends=False` 一致，Discussion #68 默认，以后可改），需显式 `include_bonus=True`。供股 `hk_rights` 是独立的 `kind=RIGHTS`，只记录、永不参与价格调整。现金分红 `hk_cash_dividend` 带 ISO 4217 `currency`（默认 `HKD`），仅当分红币种与 K 线币种一致时才套用；ADR 的美元分红不能套到港币 K 线上。以上四项默认见 [discussion #68](https://github.com/Liyuk/openbars/discussions/68)，以后可以改。
- **AKShare 港股备源**（#72）：`openbars.providers.hk.akshare`，入口 `stock_hk_hist`，代码用 5 位（`00700`）。只请求不复权（`adjust=""`），按 XHKG 丢弃非交易日；源身份 `akshare/eastmoney/raw`。库为 MIT，**不转授**东财等上游数据权；官方声明偏学术研究；[东财法律声明](https://about.eastmoney.com/home/disclaimer) 禁止未经交易所书面同意复制传播行情。openbars 只在用户本机落库。默认低频限流（4 次/分钟、80 次/24h），按代码逐只请求，不在默认配置里鼓励批量爬取。规划器（#77）仍只用 yfinance，本备源不自动进 plan/run。

### 3.3 Bar

```text
Bar
  instrument_id: str
  interval: Interval
  ts_start: datetime (UTC)  # bar 开始时刻，统一 UTC
  session_date: date        # 交易所本地交易日（日线主键）
  session: Session
  open, high, low, close: Decimal   # 精度 (20,8)，0 < 价格 < 1e12
  volume: Decimal | None            # 精度 (28,8)，0 <= 成交量 < 1e20
  vwap: Decimal | None, trade_count: int | None
  adjustment: Adjustment    # v0.1 / research-daily-v2：规范层为 split（按源交付的拆股复权，见 §3.4、§9）；raw 层为源交付口径
  source: str               # raw: provider id; canonical: 选中的源
  feed: str | None          # 如 alpaca:sip / alpaca:iex —— 成交量口径标注
  flags: QualityFlag
  ingested_at: datetime | None
```

`RawBar` = Bar 的价量字段 + `provider`、`fetch_id`、`symbol_as_returned`、`payload_hash`、`revision`（源 bar 版本，>= 1）、可选 `instrument_id` / `session_date`（符号解析后填入）、可选 `series: BarSeriesIdentity`（序列语义，见 §11.2 / §10.1）。
`CanonicalBar` = Bar + `price_source`、`volume_source`、`sources_seen`、`reconcile_rule`、`revision`。

数值口径（`openbars.core.ohlcv`）：**价格与成交量一律 `Decimal`，不经 `float` 往返**。构造时接受 `Decimal` / `int` / `str`，`float`（含 `bool`）直接 `TypeError`；非有限值、超过 8 位小数（无法无损落库）的值抛 `ValueError`。`validate_ohlcv(...)` 校验：价格 > 0、成交量 >= 0、`high >= low`、`low <= open <= high`、`low <= close <= high`；价格量级上限 `1e12`、成交量 `1e20`（均为开区间，保证落在 Numeric/decimal128 的声明精度内）。`Bar`、`RawBar`、`CanonicalDailyRow` 的构造函数都会调用它。

### 3.4 公司行动 / 日历

- `CorporateAction`: `instrument_id, ex_date, kind (split|dividend|spinoff|symbol_change), ratio / amount, source`。`ratio` / `amount` 是 **`Decimal | None`**（构造时拒绝 `float`、非有限值，拆股 `ratio` 必须 > 0）；`ratio = new / old`（4 拆 1 = `4`，1 合 4 = `0.25`）。落库为 `decimal128(20,8)`，读取不经 `float`；无限小数（如 1/3）在写入处按 8 位小数半偶舍入，这是公司行动路径上唯一的舍入。
- **观测追加 + 投票合并**（`normalize.actions.merge_corporate_actions`）：各源各写各的行（键含 `source`）；同一 `(instrument_id, ex_date, kind)` 的行对 `ratio` / `amount` 分字段投票（`None` 视为弃权）。全票一致 → 一行规范行；有分歧 → 得票最多的值胜出，平票取优先级列表（`source_priority`）中靠前的源，未列出的源按字典序排在其后；**绝不取平均**，冲突记录在 `MergedAction.conflicts`（每个源的取值），并标 `disputed` / `QualityFlag.DIVERGENT`。同一来源对同一键的多行，输入中靠后者胜出。
- **查询时复权**（`normalize.adjust`，纯函数 + `QueryTimeAdjuster`）：v0.1 默认 profile `research-daily-v2` 落库的就是按源交付的拆股复权序列（向最新一次拆股回溯复权，不含分红），所以 **`SPLIT` 且无 `as_of` = 落库值，原样返回**。其余口径由落库 bar + 公司行动派生（精确 `Decimal`，最后统一舍入到 8 位）：`RAW` = 撤销 `ex_date` 晚于该 bar `session_date` 的全部拆股（价格 × ratio，成交量 ÷ ratio）；`SPLIT` + `as_of=D` = 撤销 `ex_date > D` 的拆股，使序列呈现 D 当天所见；`SPLIT_DIVIDEND` / `ALL` = 先得 `SPLIT` 结果，再对每笔现金分红把 `session_date < ex_date` 的价格乘以 `P/(P+amount)`（`P` = 除息日前最后一个交易日的**未复权**收盘价，即落库收盘价撤销其后拆股；`ex_date ≤ as_of` 才生效，多笔连乘，成交量不变）。若落库的是 `RAW`（后续可选 profile），`RAW` 为 no-op，`SPLIT` 则按 `ex_date ≤ as_of` 的拆股向下复权。股票红利（`kind=dividend` 带 `ratio`、无 `amount`）含义因源而异，默认不当拆股处理（`include_stock_dividends=True` 才按拆股），拆分/分拆（spinoff）不做复权；任何未能应用的行动都不会悄悄改 bar，而是列在 `AdjustResult.skipped`（带原因）里。`adjustment_as_of` 不同的行混在同一窗口里 = `adjustment_cutoff_mismatch`（`AdjustmentError`）。
- **入库时把 ticker 映射为稳定 id**：Alpaca / Massive 的 `fetch_corporate_actions` 在 provider 边界按“源返回”原样把 ticker 放在 `CorporateAction.instrument_id`；映射发生在**入库**（`normalize.instruments.ingest_corporate_actions`，其下 `assign_corporate_actions` 只做映射与入队）。`instrument_id` 已是主数据中已知的 id 则原样保留；否则当作 ticker，以 `resolve(symbol, on=ex_date, provider=action.source, market=…)` 解析（规则同 bar：provider 专属映射优先于 `"*"`、歧义 → unresolved、唯一历史回退只在该日期适用时生效）。解析出的行动 upsert 进公司行动表；映射不到/歧义的**不写**公司行动表，进 `unresolved` 队列（`kind="corporate_action"`），并返回摘要（resolved / unresolved / already_mapped / reason_code 计数）。之后能解析的事件会在下次入库时写入并移出队列。`upsert_corporate_actions` 本身仍是对已映射 id 的普通写入。
- **缺公司行动时的 RAW / 派生口径**：没有任何公司行动行 ≠ 这只股票从未拆股，所以对**没有任何公司行动行**的 instrument，派生非落库口径（`RAW`、从落库 `SPLIT` 派生 `SPLIT_DIVIDEND` / `ALL`，或从落库 `RAW` 派生 `SPLIT` 等）**绝不把落库价格改标签**：默认抛 `AdjustmentError`（`reason_code="missing_corporate_actions"`）；显式 `on_missing_actions="flag"` 时改为原样返回落库口径的 bar（`adjustment` 仍是 `split`，不声称是 RAW）并打 `QualityFlag.ADJUSTMENT_UNVERIFIED`（`quality_status` 不变，仍以 flags 为准）。instrument 有公司行动行但某 bar 之后没有拆股 = 已核对过，scale = 1，`RAW` 照常有效。`SPLIT` + `as_of` 且该 instrument 没有任何公司行动行时，价格仍是落库的 `SPLIT`（不改标签、不报错），但 as-of 声称无法核实，打上 `ADJUSTMENT_UNVERIFIED`。默认 `SPLIT` 读（无 `as_of`、快路径）不受影响。
- **A 股前/后复权（`normalize.cn_adjust`，#60）**：A 股走独立 profile `cn-daily-v1`，**落库只存不复权（`RAW`）**，绝不落 `qfq` / `hfq`（前复权每出现一次新除权日就要改写整段历史）。`QFQ` / `HFQ` 在查询时由落库 `RAW` bar + 复权因子表派生，不从因子“反推”出 `CorporateAction` 行。因子是独立的 Parquet 表 `ref/adjust_factors.parquet`（见 §10.3；缺文件 = 空表，`SCHEMA_VERSION` 不变），记录因子来源 `source` 与方法 `method`（BaoStock：`baostock` / `pct_return` 涨跌幅复权法），键 `(instrument_id, source, effective_from)`。公式用**稳定的累计后复权因子** `back_adjust_factor`：会话 `d` 的因子 `f(d)` 取 `effective_from ≤ d`（且 `as_of` 给定时 `≤ as_of`）的最后一个事件，没有则为 1（首个事件之前的会话）；`HFQ = raw × f(d)`，`QFQ = raw × f(d) / ref`，`ref` 为 `as_of`（缺省取最新）时可见的最后一个事件的 `back_adjust_factor`（没有可见事件则为 1，此时 `QFQ = HFQ = raw`）。`open/high/low/close` 及行上有的 `vwap` / `preclose` 一并缩放，统一舍入到 8 位（同 `adjust.py`）；`volume` / `amount` 保持成交口径、不缩放；结果行的 `adjustment` 标为所请求的口径。**同一 instrument 不混用不同来源的因子**：不传 `source` 且存在多个来源 → `AdjustmentError(ambiguous_factor_source)`，传 `source=` 则只用该来源。**缺因子**：与缺公司行动同一策略——instrument 一行因子都没有时，默认 `AdjustmentError(missing_adjust_factors)`，`on_missing_factors="flag"` 则原样返回落库 `RAW`（`adjustment` 仍是 `raw`）并打 `ADJUSTMENT_UNVERIFIED`；有至少一行因子 = 已核对过，首个事件前的会话因子为 1。`SPLIT_DIVIDEND` / `ALL` 对 A 股不适用（`unsupported_cn_adjustment`），美股行请求 `QFQ` / `HFQ` 同样报错。
- **读取路径的 STALE 标记**：`DuckDbBarReader.bars` 在 instrument 有拆股行时对落库 bar 运行 `find_stale_after_split` 的逐 bar 版本（`stale_bar_indexes`），把 `QualityFlag.STALE` OR 到“抓取早于之后某次拆股”的 bar 上（只标记，不重抓、不调 API；派生口径的行保持同样的标记）。
- **拆股后旧历史检测**（`normalize.actions.find_stale_after_split`，纯函数）：落库 bar 的 `session_date` 早于某次拆股 `ex_date`，且抓取时间（`generated_at` / `ingested_at`）也早于该 `ex_date`，则该段未经此次拆股复权，需在新价格基准下重抓；返回按（instrument, 拆股）聚合的区间。`raw` bar 不会过期；没有抓取时间的 bar 默认按过期报告（`treat_unknown_as_stale=False` 可关闭）。
- `TradingCalendar`（Protocol）：`sessions(start, end)`、`is_session(d)`、`session_bounds(d) -> [(open_utc, close_utc), …]`（支持午休、半日市；加密为 24/7 UTC）。实现见 §3.4.1–§3.4.3、§3.4.5

#### 3.4.1 XNYS 日历（`openbars.calendars.xnys`）

- **复用 [`exchange_calendars`](https://pypi.org/project/exchange-calendars/)（PyPI `exchange-calendars>=4.9,<5`）的 `XNYS` 规则，不在仓库内维护节假日表**。许可证 Apache-2.0，与本项目（Apache-2.0）兼容。
- `XNYSCalendar` 实现 `TradingCalendar` 协议，经入口点 `openbars.calendars` 的 `xnys` 注册；`market = Market.US`，`timezone = "America/New_York"`。
- `sessions(start, end)`：闭区间交易日，`end < start` 抛 `ValueError`；`is_session(day)`；`session_bounds(day)` 返回 **UTC** `(open, close)`，XNYS 无午休故只有一段，半日市（如感恩节次日）13:00 America/New_York 提前收盘；非交易日返回空元组。夏令时由时区自动处理（09:30–16:00 EST = 14:30–21:00 UTC，EDT = 13:30–20:00 UTC）。
- `TradingSession(session_date, opens_at, closes_at)`（定义于 `core.calendar`，带时区；`XNYSCalendar.trading_session(s)` 返回 America/New_York 时间）。
- `session_index(as_of, *, epoch=date(2000,1,1))`：不晚于 `as_of` 的最近一个会话的从 0 起序号；`as_of < epoch` 返回 0。
- `latest_completed_session(now, wait_minutes=30) -> date | None`：`now` 必须带时区；会话收盘后至少等待 `wait_minutes` 分钟才算完成（在 America/New_York 比较）；向前最多找约 14 天；`wait_minutes < 0` 抛错。
- 范围策略：缓存默认 `XNYS` 日历；请求范围超出其首/末会话时，构造起止各扩一天的有界日历（带缓存）。
- `is_non_session_date(day)`：周末恒为非交易日；节假日只在默认日历范围内判定（约 2006 年起），范围之外的工作日**不**当作非交易日，避免拒收更早的长历史。`reject_non_session_bars(bars, *, session_date_attr="session_date")` 是纯函数，返回 `accepted` 与 `rejected`（理由 `"non_session_date"`）。

#### 3.4.2 加密市场日历（`openbars.calendars.crypto`）

- `CryptoCalendar` 实现 `TradingCalendar` 协议，经入口点 `openbars.calendars` 的 `crypto` 注册；`market = Market.CRYPTO`，`timezone = "UTC"`。纯标准库实现，不依赖 `exchange_calendars`。
- **24/7**：每个 UTC 日历日都是会话，无周末、无节假日、无午休。`sessions(start, end)` 返回闭区间内的每一天，`end < start` 抛 `ValueError`；`is_session(day)` 恒为 `True`。
- **切日口径**：日线 `session_date` = 该 bar 的 **UTC 日期**；`ts_start` = 当日 `00:00:00 UTC`；对应的序列口径为 `session_semantics=utc_day`（§3.5.1、§10.7 目录表）。按交易所本地时区切日的源（例如以 UTC+8 午夜为日界）不能直接当作规范日线，需先换算到 UTC 00:00 口径（从更细粒度 bar 重新聚合，或改用按 UTC 切日的接口）。
- `session_bounds(day)` 返回恰好一段 UTC 半开区间 `[00:00:00 UTC, 次日 00:00:00 UTC)`；相邻两日首尾相接（第 N 日 `closes_at` == 第 N+1 日 `opens_at`），无重叠、无空隙；收盘瞬时属于下一会话。`closes_at > opens_at` 恒成立，满足 `TradingSession` 的约束。
- `trading_session(day)` / `trading_sessions(start, end)` 返回 `TradingSession`，开收盘瞬时保持 UTC（不换到其他时区）。
- `session_index(as_of, *, epoch=date(2000,1,1))`：不晚于 `as_of` 的最近一个会话的从 0 起序号，即 `(as_of - epoch).days`；`as_of < epoch` 返回 0。
- `latest_completed_session(now, wait_minutes=30) -> date | None`：`now` 必须带时区；**在 UTC 比较**，`now >= closes_at + wait_minutes` 才算完成（即次日 00:00 UTC 之后再等 N 分钟）；向前最多找约 14 天；`wait_minutes < 0` 抛错。
- `is_non_session_date(day)` 恒为 `False`；`reject_non_session_bars` 与 XNYS 版本签名、返回类型（`BarFilterResult` / `BarRejection`）一致，但不会拒收任何 bar（含周末日期）。

#### 3.4.3 沪深日历（`openbars.calendars.cn`）

设计见 [discussion #54](https://github.com/Liyuk/openbars/discussions/54)（issue #55）。

- 包装 `exchange_calendars` 的 **XSHG**；该包没有 XSHE，而沪深交易所公布同一份年度休市安排，所以 **XSHE 作为 XSHG 的别名**（`XSHECalendar is XSHGCalendar is CNCalendar`）。北交所不在范围内。`market = Market.CN`，`timezone = "Asia/Shanghai"`，入口点 `cn`。
- **午休**：连续竞价 09:30–11:30、13:00–15:00。`session_bounds(day)` 在交易日返回**两段** UTC `(open, close)`（11:30–13:00 上海时间为空档），非交易日返回空元组。`trading_session(s)` 的 `opens_at` / `closes_at` 为当日 09:30 与 15:00（Asia/Shanghai，带时区），不只是上午段。
- `sessions`、`is_session`、`latest_completed_session(now, wait_minutes=30)`（15:00 收盘后再等待 N 分钟）。`openbars.calendars` 以 `cn_` 前缀导出函数（`cn_sessions` 等），避免与 XNYS 同名函数冲突。
- 覆盖范围：库内节假日只记录到固定年份（4.13.x 为 2026），超出时库抛 `ValueError`，不会静默当作无假日；需升级依赖来扩展。
- **规划器已接入（#62）**：`Market.CN` 不再产生 `no_calendar`：`_sessions` 用本日历、`_today` 取 `Asia/Shanghai`、`latest_ready_session(now, CN)` = `latest_completed_session(now, 30)`（15:00 收盘后再等 30 分钟）。没有声明 `cn`/`1d` 的 provider 时仍记 `no_provider`，不会凭空生成作业。港股见 §3.4.5 与 §3.5.4。

#### 3.4.4 A 股涨跌停与停牌标记（`openbars.normalize.cn_quality`，#61）

- **板块**取自 6 位代码（保留前导 0；`sh.688001` / `688001.SH` / `cn:eq:xshg:688001` 都可）：`688`/`689` 科创板；`300`/`301` 创业板；`600`/`601`/`603`/`605`/`000`/`001`/`002`/`003` 主板（`002` 中小板按主板 ±10%）。北交所（`8xxxxx` / `43xxxx` / `92xxxx`）及 B 股等其它代码不在范围内：`board_of_code` 抛 `ValueError`，不虚构北交所限价，`flags_for_cn_bar` 对这类代码不打 `LIMIT_*`。
- **涨跌幅比例**（`limit_ratio`）：主板 ±10%；创业板自 2020-08-24（含）起 ±20%，此前 ±10%；科创板自开板日 2019-07-22 起 ±20%；ST（`is_st`，BaoStock `isST=1`）**按板块与日期**（#95，截至 2026-11）：创业板 2020-08-24（含）起 ±20%（深证上〔2020〕515号《创业板交易特别规定》第2.1条，深交所 2020-07-10 通知），此前 ±5%；科创板始终 ±20%（《科创板股票交易特别规定》第十八条；上交所 2024-04-12 ST 问答：科创板 ST 不进风险警示板，涨跌幅仍为 20%）；主板至 2026-07-05（含）±5%，2026-07-06（含）起 ±10%（上证发〔2026〕41号《上海证券交易所交易规则（2026年修订）》，上交所公告 c_20260424_10816474，深交所同步修订）。北交所不在范围内；IPO 无涨跌幅窗口仍优先。`st_ratio` 仅作测试覆盖用。**价格**以未复权前收盘（或除权参考价）计算，`limit_prices(preclose, ratio)` 得 `(涨停价, 跌停价)`，**精确到 0.01 元、`ROUND_HALF_UP`**（不是银行家舍入；`Decimal`，不经 float）。
- **新股前期**按**上市时**的规则（`listed_on`，上市日算第 1 个交易日，交易日取沪深日历而非自然日）：科创板前 5 个交易日不设涨跌幅；创业板 2020-08-24 起上市的前 5 日不设，更早上市的仅第 1 日特殊（无常规涨跌幅）、之后 ±10%；主板 2023-04-10 起上市的前 5 日不设，更早上市的仅第 1 日特殊、之后 ±10%。无涨跌幅的时段 `limit_ratio` 返回 `None`，**无论涨跌多大都不打 `LIMIT_*`**；`listed_on` 未知则不假设新股窗口。
- **参考价**优先 `RawBar.extra["preclose"]`（源给的未复权/除权参考价），否则取上一交易日未复权收盘。
- **标记**（`flags_for_cn_bar(bar, *, previous_close, listed_on, sessions, is_st)` 与按序列的 `flags_for_cn_series`）：`SUSPENDED` = `extra["tradestatus"]` 为 `0`/`"0"`（停牌占位行成交量/成交额可为 0、OHLC = 前收）；未停牌且收盘价**恰等于**涨停价 → `LIMIT_UP`，恰等于跌停价 → `LIMIT_DOWN`（差 0.01 不标）；停牌行不会同时是 `LIMIT_*`。三个标记只进 `flags`，`quality_status` 保持 `complete`。
- **raw 与 canonical**：raw 层保留停牌占位行（BaoStock 适配器如此）；canonical 默认**不为停牌日写占位 bar**（日历上的空洞 + 质量标记）。缺失日判断 `missing_sessions(calendar_sessions, present_session_dates, suspended_session_dates)`：停牌日**不算缺失**，不在日历里的日期（节假日）更不算，只有“日历内、无 bar、非停牌”的才是缺失。停牌事实要在 canonical 丢掉该 bar 之后仍可查，所以从 **raw** 收集：`suspended_dates(raw_bars)` 优先读 `extra["tradestatus"]`；Parquet raw 层把 `tradestatus` / `isST` / `preclose` 等没有独立列的 `extra` 键存进可空 JSON 列 `extra`（#94，见 §10.1）；Postgres native 把同一份 leftover 存进可空 JSONB 列 `raw_bars.extra`（#122，见 §10.5）。读回的 `RawBar.extra` 仍带 `tradestatus`；**只有**没有该字段的行（该列出现之前写的旧 raw 文件，或尚未迁移的 Postgres 行）才退回到停牌占位行的特征——成交量为 0。成交量为 0 但 `tradestatus=1` 的交易日不算停牌。（本版不建专门的标记表。）

#### 3.4.5 港股日历（`openbars.calendars.hk`）

设计见 [discussion #68](https://github.com/Liyuk/openbars/discussions/68)（issue #69）。

- 包装 `exchange_calendars` 的 **XHKG**。`market = Market.HK`，`timezone = "Asia/Hong_Kong"`，入口点 `hk`。
- **常规时段**（库自 2011-03-07 起记录）：连续竞价 09:30–12:00、13:00–16:00（16:00 为收市竞价结束）。`session_bounds(day)` 在常规交易日返回**两段** UTC `(open, close)`。
- **半日市**：圣诞节前夕、新年前夕、农历新年前夕，以及库列出的其他提早收市日，只有上午一段，收市 12:00 HKT，无午休。`TradingSession` 的 `opens_at` / `closes_at` 仍是当日首开与尾收（Asia/Hong_Kong，带时区）。
- `sessions`、`is_session`、`latest_completed_session(now, wait_minutes=30)`（按当日实际收市时间再等待 N 分钟）。模块函数以 `hk_` 前缀从 `openbars.calendars` 导出。
- **覆盖范围**：库内默认会话区间有上下限（`exchange-calendars` 4.13.x 约为 2006-11-13 至 2027-11-11，以 `coverage()` 为准）。超出时抛 `ValueError`，**不要手写假期或未来台风/黑雨休市**。历史上库已收录的临时休市（部分台风/黑雨日出现在 `adhoc_holidays`）按库数据使用。
- **临时休市登记**（#76，`openbars.calendars.hk_closures`）：本仓库**不随包发布**台风/黑雨日期表。用户可根据港交所公告，手动或半自动调用 `register_adhoc_closure` / `load_adhoc_closures` 登记一日，**必须**同时给出 `source` 与 `reference`（公告 URL、通告编号或标题）。半自动入口只接收已经解析好的记录，openbars **不抓取**港交所网站。覆盖范围之外可以留下记录，但 `is_session` 仍拒绝编造假期表。可选 `JsonAdhocClosureStore` 把登记写到用户自己的文件，进程默认是空的内存表。规划器与 yfinance / AKShare 适配器共用模块级 `is_session`，登记生效后会自动跳过该日。
- **规划器已接入（#77）**：`Market.HK` 不再产生 `no_calendar`：`_sessions` 用本日历、`_today` 取 `Asia/Hong_Kong`、`latest_ready_session(now, HK)` = `latest_completed_session(now, 30)`（按当日实际收市时间，16:00 或半日市 12:00，再等 30 分钟）。规划器只用已启用且声明 `hk`/`1d` 的 **yfinance**；AKShare 可本机调用，但不进入 plan/run。没有 yfinance 时记 `no_provider`，不会凭空生成作业。symbol 必须是 `normalize_hk_symbol` 认得的写法（`00700` / `0700.HK` / `hk:eq:xhkg:00700` …，同一只股票多种写法只规划一次）；无法识别的写法跳过并记 `no_symbols`。覆盖判断同时认配置写法、5 位代码、Yahoo `0700.HK` 与 instrument id。`run` 写入 raw 后由 `HkCanonicalSink` 写成 `hk-daily-v1` canonical（见 §3.5.4）；不会误走美股 XNYS 对账。

### 3.5 Provider 能力声明

```text
SourceCapability
  provider: str
  market: Market
  asset_classes: set[AssetClass]
  interval: Interval
  history_start: date | "listing" | RollingWindow(days)
  latency: timedelta        # 数据最早何时可取（如 Massive 收盘日 0 点 ET 后）
  batch: BatchShape         # per_symbol | multi_symbol(max_n) | whole_market_per_day
  max_bars_per_request: int
  adjusted_modes: set[Adjustment]
  feed: str | None
  terms: ProviderTerms      # personal_only / no_redistribution / url

QuotaPolicy
  provider: str, key_scope: "per_key" | "per_ip"
  limits: list[RateLimit(count, per: timedelta)]   # 例: 5/min + 25/day
  weight_of: callable | dict  # 请求权重（Binance 权重制）
  safety_margin: float        # 默认 0.9，避免窗口边界突发
  backoff: BackoffPolicy      # 429/5xx/451 处理，尊重 retry-after
```

能力与额度**由 provider 插件声明默认值**，用户配置只能收紧不能放宽（除非显式 `override: true`，例如付费档）。

#### 3.5.1 Binance 现货日线（`openbars.providers.crypto.binance`）

- 公开 spot klines（`/api/v3/klines`，`interval=1d`），**无需 API key**，`secret_values()` 为空；来源标识 `binance/spot/raw`（不属于美股 `KNOWN_SOURCES`）。
- **默认 base URL 为 `https://data-api.binance.vision`**（只读行情镜像）。`api.binance.com` 对部分地区（含美国 IP）返回 451，因此不作默认；构造时可用 `base_url` 覆盖。451 映射为不可重试的 `provider_rejected_request`，消息提示该地区不可用。`https://data.binance.vision` 的批量归档是后续可选来源，本版不实现。
- **UTC 00:00 切日**：序列口径 `session_semantics=utc_day`（不是泛化的 `provider_aligned`）；`ts_start` 必须恰为当日 `00:00:00 UTC`，`session_date` = 其 UTC 日期，与 §3.4.2 一致；`openTime` 不在 UTC 零点的行视为切日口径不符，整批以 `invalid_payload` 拒收。请求窗口按 UTC 毫秒取闭区间（起日 `00:00:00.000` 至止日 `23:59:59.999`）。
- 价格、成交量按字符串解析为 `Decimal`（不经 float）；`volume` 为基础资产成交量，成交额与成交笔数放 `extra`。口径 `price_basis=raw`、`volume_basis=as_traded`，`currency=None`（计价资产如 USDT 不是 ISO 货币）。
- 每次请求 1 个 symbol；单页最多 1000 根，窗口更长时用“末根 `openTime` + 1ms”翻页（游标必须前进，否则报 `pagination_loop`）。无数据返回空批次而非错误。
- **权重制额度**：spot `REQUEST_WEIGHT` 每 IP 每滚动分钟 6000（`key_scope="per_ip"`，`safety_margin=0.9`）；klines 权重 2，`exchangeInfo` 权重 20，均经 `QuotaGate.acquire(weight=…)`。429 / 418 尊重 `Retry-After` 并 `penalize`，5xx 为 `provider_unavailable`。官方限额文档见 `QuotaPolicy.official_docs_url`。
- `list_instruments(Market.CRYPTO)` 读 `exchangeInfo`，只返回现货 symbol（`exchange="BINANCE"`，`active` = 状态为 `TRADING`），并按 `baseAsset` / `quoteAsset`（去空白；缺失或空白为 `None`）填充 `base_asset` / `quote_asset`；不生成 `instrument_id`（交易对主数据与 base/quote 见 §3.2.1）。`normalize.crypto_pairs.listing_from_provider_instrument(row, provider=…, venue=None)` 把此类行转成 `CryptoPairListing`（缺 base/quote 抛 `ValueError`，`venue` 缺省取 `row.exchange`），CLI 写路径（`run` / `plan --submit`）会自动调用并入库（见 §3.2.1）；dry-run `plan` 不调用。无公司行动。
- **编排器接入（#44）**：`markets: [crypto]` 时 `plan` / `run` 生成并执行加密日线任务。规划器按市场分派日历：`Market.US` → XNYS，`Market.CRYPTO` → `CryptoCalendar`（`latest_ready_session(now, market=CRYPTO)` = 已收盘满 30 分钟的最近 UTC 日，周末也出任务）；`Market.CN` 见 §3.5.3（沪深日历）；`Market.HK` 见 §3.5.4（XHKG 日历，只用 yfinance）。声明 `crypto`/`1d` 的已启用 provider 参与；yfinance 只是美股回退，不参与。Binance、Coinbase 均为 `PER_SYMBOL`，且**每个 symbol 只发给方言匹配的 provider**：未注入交易对主数据时按启发式——含 `-` 的是 Coinbase product id，`[A-Z0-9]+` 的是 Binance symbol；`DefaultPlanner(instruments=master)` 注入主数据后改为只在 `resolve(symbol, provider=…, market=CRYPTO)` 命中时才规划。宁可跳过一个 symbol，也不产生一个必然失败的任务。两家交易所永不合并为一个任务或一个 instrument。配置只需 `markets: [crypto]`、启用 `providers.binance` / `providers.coinbase`（无 API key）、`universe.crypto.symbols`；`openbars verify` 现按 `markets` 中每个市场各自的日历与 `policy_version` 出报告（加密为 `CryptoCalendar` + `crypto-daily-v1`，细节见 README）。**写路径自动入库交易对（#51）**：`run` / `plan --submit` 先列出并入库交易对再规划，规划器因此改走 `resolve`；master 为空且列表失败时回退启发式。dry-run `plan` 不入库。

#### 3.5.2 Coinbase Exchange 日线（`openbars.providers.crypto.coinbase`）

- 公开 Coinbase Exchange candles（`GET /products/{product_id}/candles?granularity=86400`），**无需 API key**，`secret_values()` 为空；来源标识 `coinbase/exchange/raw`（feed 为 Coinbase Exchange，不是 Advanced Trade；不属于美股 `KNOWN_SOURCES`）。
- **默认 base URL 为 `https://api.exchange.coinbase.com`**（不是 `api.coinbase.com`）；构造时可用 `base_url` 覆盖。`User-Agent` 为 `openbars/<version>`，不带邮箱。
- **UTC 00:00 切日**：`granularity=86400` 的桶按 UTC 零点切分，序列口径 `session_semantics=utc_day`；`ts_start` 必须恰为当日 `00:00:00 UTC`，`session_date` = 其 UTC 日期，与 §3.4.2 一致；`time` 不在 UTC 零点的行整批以 `invalid_payload` 拒收。请求 `start` = 起日 `00:00:00Z`，`end` = 止日次日 `00:00:00Z`，并丢弃 `session_date` 落在窗口外的行（API 若把 `end` 视为闭区间，次日零点那根不会被保留）。
- **响应是数组的数组，不是对象**，行布局为 `[time, low, high, open, close, volume]`（low/high 在 open/close 之前），`time` 为 Unix 秒；接口常按新到旧返回，适配器先按 `time` 升序排序，重复时间为 `invalid_payload`。价格、成交量用 `json.loads(..., parse_float=Decimal)` 解析为 `Decimal`（不经 float）；`volume` 为基础资产成交量。口径 `price_basis=raw`、`volume_basis=as_traded`，`currency=None`。
- 每次请求 1 个产品（Coinbase product id，如 `TEST-USDT`）；单次请求最多 300 根。窗口更长时按每页最多 300 天分段翻页，下一页从本页末根之后开始（游标必须前进，否则报 `pagination_loop`；跨页不升序按 `invalid_payload`）；后续页失败时返回已得部分并带 `next_cursor`（Unix 秒）。无数据返回空批次而非错误。
- **额度**：公开 REST 每 IP 每秒 10 次（`key_scope="per_ip"`，`safety_margin=0.9`），每页权重 1，经 `QuotaGate.acquire` 获取；429 尊重 `Retry-After` 并 `penalize`，5xx 为 `provider_unavailable`，其余 4xx 为 `provider_rejected_request`。官方文档见 `QuotaPolicy.official_docs_url`。
- `list_instruments(Market.CRYPTO)` 读 `GET /products`（`exchange="COINBASE"`，`active` = `status == "online"` 且（若有该字段）未 `trading_disabled`），按 `base_currency` / `quote_currency` 填充 `base_asset` / `quote_asset`；不生成 `instrument_id`。无公司行动。
- 已接入编排器（见 §3.5.1 的“编排器接入”：用 `BASE-QUOTE` product id 作 symbol，只发往 Coinbase；`run` / `plan --submit` 会自动列出并入库 Coinbase 交易对），但**不在 `KNOWN_SOURCES`** 中；公开端点冒烟见 `tests/test_crypto_public_smoke.py`（仅在 `OPENBARS_CRYPTO_SMOKE=1` 时运行，默认跳过，只用 `tmp_path`，不入库交易对）；加密对账口径见 §11.2.1 `crypto-daily-v1`。

#### 3.5.3 BaoStock A 股日线（`openbars.providers.cn.baostock`）

设计讨论：[#54](https://github.com/Liyuk/openbars/discussions/54)。

- **默认存不复权价**：`query_history_k_data_plus` 固定 `adjustflag="3"`（不复权），永不请求 1/2；来源标识 `baostock/ashare/raw`（常量 `BAOSTOCK_ASHARE_RAW`，不属于美股 `KNOWN_SOURCES`），口径 `raw / none / regular / as_traded`，币种 `CNY`。前/后复权在查询时由落库 `raw` bar + 复权因子派生（#60，见 §3.4）；涨跌停 / 停牌标记见 §3.4.4（#61）。
- **同时取复权因子与停牌字段**：`fetch_adjust_factors(request)` 调 `query_adjust_factor`，返回 `BaoStockAdjustFactor`（`code`、`divid_operate_date`、`fore/back_adjust_factor`、`adjust_factor` 均为 `Decimal`，`source="baostock"`、`method="pct_return"` 涨跌幅复权法）；窗口内无事件返回 `[]`。官方默认起点是 2015-01-01，窗口起点太晚会漏掉更早的因子，需要完整历史时请由调用方传入上市日或 `1990-01-01`。`preclose`、`tradestatus`、`isST`、`pctChg`、`amount`、`turn` 写入 `RawBar.extra`；**停牌行（`tradestatus=0`）保留在 raw 中**，成交量/成交额可为 0。
- **可选依赖，不需要 key**：`pip install openbars[baostock]`（匿名登录，`secret_values()` 为空）。模块导入不依赖该包，仅默认客户端在取数时懒加载；未安装抛不可重试的 `baostock_not_installed`。BaoStock 是 login + socket 协议而非 HTTP，适配器通过可注入的小客户端（login / logout / 查询）工作，测试用假客户端，默认无网络。默认客户端登录时设 10 秒 socket 超时，每次取数“登录 → 一次查询 → 登出”，**不做循环重试**，失败映射为 `provider_unavailable`（网络类，可重试标记）/ `provider_rejected_request`（参数类）。
- **symbol** 用 BaoStock 原生写法 `sh.600000` / `sz.000001`；`600000.SH` 等写法经 `normalize.cn_instruments` 归一，裸 6 位代码拒绝。`PER_SYMBOL`，每次一个 symbol；`ts_start` 为该交易日 `00:00 Asia/Shanghai`，价格与成交量以字符串转 `Decimal`，不经 float。
- **额度**：BaoStock 没有公开的限速数字，`QuotaGate` 用保守策略（每秒 2 次、`safety_margin=0.9`、`key_scope="per_process"`），每次登录+查询取 1 个令牌，用户只能进一步收紧。
- **条款**：PyPI 包为 BSD，但官网声明未经书面许可不得复制、传播或商用；`personal_only=True`、`redistribution_allowed=False`，openbars 只做本地存储。
- `list_instruments(Market.CN, day=None)` 包装 `query_all_stock`（一次调用，只留 A 股股票，跳过指数/基金），不生成 `instrument_id`（见 §3.2 CN 规则）。
- **冒烟**：`tests/test_baostock_public_smoke.py` 仅在 `OPENBARS_BAOSTOCK_SMOKE=1` 时运行，一只股票两个交易日、只尝试一次、无重试；海外网络常连不上，此时 skip。只写 pytest `tmp_path`，不入库。
- `config.example.yaml` 中 `providers.baostock` 默认 `enabled: false`。
- **编排器接入（#62）**：`markets: [cn]`、启用 `providers.baostock`、`universe.cn.symbols`（如 `sh.600000`）即可 `plan` / `run`（无需 instrument 主数据）。规划器与加密一样：无整市场（Massive）路径、无 yfinance 回退，所有声明 `cn`/`1d` 的已启用 provider 参与（BaoStock 为 `PER_SYMBOL`）；日期取 `Asia/Shanghai`，只规划沪深日历（XSHG）上的交易日，`latest_ready_session` = 15:00 收盘 + 30 分钟。symbol 必须是 `normalize_cn_symbol` 认得的写法（`sh.600000` / `600000.SH` …，同一只股票的多种写法只规划一次）；裸 6 位代码等无法识别的写法跳过并记 `no_symbols` 备注，不规划任务。覆盖判断同时认配置写法与 BaoStock 原生写法（`sh.600000`），换一种写法不会重复拉取。

#### 3.5.4 yfinance 港股日线编排器（#77）

配置 `markets: [hk]`、启用 `providers.yfinance`（`openbars[yfinance]`）、`universe.hk.symbols`（如 `00700` / `0700.HK`）即可 `plan` / `run`。日历为 XHKG（§3.4.5），日期取 `Asia/Hong_Kong`，只规划交易所交易日，`latest_ready_session` = 当日实际收市 + 30 分钟。规划器没有整市场路径，也不走美股那套 yfinance 回退窗口；yfinance 是港股唯一规划源（`MULTI_SYMBOL`）。同一只股票的多种写法只规划一次；覆盖同时认配置写法、5 位代码、Yahoo ticker 与 `hk:eq:xhkg:…`。`run` 落 raw 后由 `HkCanonicalSink`（`DispatchingSink(hk=…)`，`cli.wiring.make_sink` 已接好）写成 `hk-daily-v1` canonical，**不走美股 XNYS 对账器**；没有 `hk=` 的 `DispatchingSink` 对 HK 只保留 raw。

- **规范层（#99）**：yfinance（`yfinance/yahoo/raw`）是主源；AKShare（`akshare/eastmoney/raw`）只做同市场对账，不进 plan/run，不补洞、不平均。意见不合（超出美股同等容忍：价格 0.5%、成交量 5%）打现有 `MemoryReconciler` 标记（`divergent` 等），不用 A 股涨跌停/停牌标记。`ensure_hk_instruments`（`openbars.normalize.hk_ingest`）首次登记 `Instrument(market=HK, asset_class=EQUITY, currency=HKD)` 与 5 位代码 / Yahoo ticker 映射（第 1 代 id）。行上带 `currency=HKD`（能力字段 `currency:HKD`；查询时也可回退到 instrument 币种）。公司行动的 `currency` 是 `corporate_actions` 的可空附加列，旧文件缺列视为 `None`，`SCHEMA_VERSION` 仍为 1。


- **落库（`CnCanonicalSink`）**：`DispatchingSink(cn=…)` 把 `job.market = CN` 的日线交给它，**不走美股 XNYS 对账器**；没有 `cn=` 的 `DispatchingSink` 对 CN 只保留 raw。流程：用 `bar.instrument_id` / 主数据解析 / 确定性的 `cn_instrument_id(*normalize_cn_symbol(symbol))`（第 1 代 id）得到 `instrument_id`，首次出现时由 `ensure_cn_instruments`（`openbars.normalize.cn_ingest`）写入 `Instrument(market=CN, asset_class=EQUITY, currency=CNY)` 与 `baostock` 原生写法 + 通用 `sh.600000` / `600000.SH` 映射（只做首次 upsert，不复用已退役 id）；**丢弃停牌日**（不写占位，成交量为 0 但未停牌的交易日保留）；按 §3.4.4 附加 `LIMIT_UP` / `LIMIT_DOWN`；写 `adjustment=raw`、`policy_version=cn-daily-v1`、`session=regular`、`selection_reason=primary_source`（单源投影）。`observation_from_raw` 要求有成交量，所以 CN 不走它。`cli.wiring.make_sink` 已接好 `cn=`。
- **复权因子入库**：`CnCanonicalSink` 带 `factor_store` 时，写完 canonical 后对每个 symbol 调 provider 的 `fetch_adjust_factors`，用 `normalize.cn_adjust.from_baostock` 转成 `AdjustFactor`（`source=baostock`、`method=pct_return`）并 upsert 到 `ref/adjust_factors.parquet`，这样 `run` 之后即可读 `QFQ` / `HFQ`。**窗口由 `normalize.cn_ingest.adjust_factor_fetch_window` 决定（#93）**：该 instrument 在因子表里还没有 `baostock` 因子时，**首次拉全量**（起点 = `listed_on`，未知则 `1990-01-01`；BaoStock 因子自上市累计，起点太晚会漏掉更早的除权日）；已有因子时只拉**增量**（起点 = 已存最后一个 `effective_from`，闭区间，顺带刷新最后一个事件）；设了 `factor_start` 则显式覆盖起点；终点恒为作业窗口终点。**种子行**：首次全量成功但返回 `[]`（从未除权）时，写一条 `effective_from = 作业窗口终点`（已核对至该日）、三个因子均为 1 的 `AdjustFactor`（`source=baostock`、`method=pct_return`），这样之后的 QFQ / HFQ 是 scale=1 的恒等变换而不是 `missing_adjust_factors`；窗口起点没有回溯到全量起点（如较晚的 `factor_start`）时不写种子，已有因子（含种子）时增量返回 `[]` 也不会再写。真正抓取失败时不写任何东西，QFQ 仍会因 `missing_adjust_factors` 报错。入口为 `ingest_job_adjust_factors`（`CnCanonicalSink._ingest_factors` 只是调用它）。因子抓取失败不会让作业失败（bar 已写入），记入 `CnCanonicalSink.factor_errors`。不生成 `CorporateAction`。

### 3.6 Job / Checkpoint

```text
Job
  job_id: str               # 由 (provider, market, interval, instrument 批, 窗口) 哈希得出 → 幂等
  kind: backfill | daily | repair | verify
  provider, market, interval
  instruments: tuple[str, …] | "whole_market"
  window: DateRange
  priority: int
  node: str | None          # 指定采集节点（如 cn-node）
  state: pending | leased | running | done | failed | dead
  attempts, last_error, retry_at

Checkpoint
  job_id, cursor (provider 分页游标 / 最后完成的日期), updated_at
```

Job 与 Checkpoint 持久化在本地状态库（默认 SQLite，`data/state.db`；PG 后端时放 PG）。`kill -9` 后重启从 Checkpoint 继续，写入以 `(provider, instrument_id, interval, ts_start)` 去重。

## 4. 各层接口（`Protocol`）

```python
class Provider(Protocol):
    id: str
    def capabilities(self) -> Sequence[SourceCapability]: ...
    def quota_policy(self) -> QuotaPolicy: ...
    def list_instruments(self, market: Market) -> Iterable[ProviderInstrument]: ...
    def fetch_bars(self, req: FetchRequest) -> RawBatch: ...          # 一次请求 = 一次令牌
    def fetch_corporate_actions(self, req: FetchRequest) -> Iterable[CorporateAction]: ...

class QuotaGate(Protocol):
    def acquire(self, provider: str, weight: int = 1, *, key_scope: str = "default",
                timeout: float | None = None) -> Lease: ...
    def penalize(self, provider: str, until: datetime) -> None: ...    # 429/retry-after
    def snapshot(self) -> Mapping[str, QuotaUsage]: ...

class Planner(Protocol):
    def plan(self, selection: Selection, coverage: CoverageIndex) -> Iterable[Job]: ...

class Orchestrator(Protocol):
    def submit(self, jobs: Iterable[Job]) -> None: ...
    def run(self, *, until_idle: bool = True) -> RunReport: ...

class CollectorNode(Protocol):
    name: str
    def markets(self) -> set[Market]: ...
    def execute(self, job: Job) -> JobResult: ...                     # local: 直接调 provider；remote: HTTP

class RawStore(Protocol):
    def append(self, batch: RawBatch) -> None: ...
    def scan(self, provider: str, market: Market, interval: Interval, window: DateRange) -> Iterable[RawBar]: ...

class InstrumentMaster(Protocol):
    def resolve(self, symbol: str, *, on: date, provider: str, market: Market) -> str | None: ...
    def upsert(self, instruments: Iterable[Instrument], mappings: Iterable[SymbolMapping]) -> None: ...

class Normalizer(Protocol):
    def normalize(self, raw: Iterable[RawBar]) -> Iterable[Bar]: ...

class Reconciler(Protocol):
    def reconcile(self, candidates: Mapping[str, Sequence[Bar]], policy: ReconcilePolicy) -> ReconcileResult: ...

class StorageBackend(Protocol):
    raw: RawStore
    canonical: CanonicalStore
    instruments: InstrumentMaster
    state: StateStore      # jobs, checkpoints, quota buckets, fetch_log

class BarReader(Protocol):
    def bars(self, symbols_or_ids, *, interval="1d", start=None, end=None,
             adjustment: Adjustment = Adjustment.SPLIT, as_of: date | None = None) -> "pyarrow.Table": ...
    # 默认 SPLIT = research-daily-v2 落库值；ticker 按日解析；as_of = 知识日期 + 复权 as-of（见 §10.4）
```

## 5. 编排与节奏规则

1. **期望网格**：`Selection(markets × intervals × universe × history)` + `TradingCalendar` → 每个 instrument 的期望 bar 集合。
2. **覆盖索引**：从存储读已有 raw/canonical 覆盖 → 差集即待抓窗口。
3. **源路由**：对每个待抓窗口，按 `reconcile` 优先级和 `SourceCapability`（历史深度、批形状、延迟）挑选能覆盖的源，生成**最省额度**的 Job（例：Massive 一天一请求覆盖全市场；Alpaca 200 只一批；yfinance 只补 2016 年前）。
4. **执行**：worker 从队列租用 Job → `QuotaGate.acquire` → provider 请求 → `RawStore.append` → 更新 Checkpoint。
5. **错误**：`ProviderError(code, retryable, retry_at)` 统一错误模型（沿用上游内部实现）；429 → `penalize` 整个 provider；连续失败 N 次 → 熔断该 provider 一段时间；不可重试 → `dead` 并进入 `status` 报告。
6. **每日增量**：按各源 `latency` 定时（例如美股 06:30 ET 后跑前一交易日），先主源后兜底源。
7. **回补预算**：`openbars plan` 输出每个源预计请求数、耗时下限与额度占用；启用非日线粒度时若超过预算，要求用户确认（`--accept-budget`）。

## 6. 粒度扩展方式

- 粒度是 `Interval` 枚举 + provider 能力声明，不是代码分支。v0.1 只有 `1d` 的 provider 实现与测试；`1h`/`1m` 等的模型、分区、口径字段已就位。
- 开启某粒度时：Planner 只会把 Job 路由给声明了该粒度的源；QuotaGate 用同一份额度（日线和小时线共享一个 provider 的额度），因此 `plan` 会显示叠加后的占用。
- **派生粒度**：`Resampler` 接口支持由细到粗合成（如 Alpaca 30m → 1h，按 `us_hourly_alignment` 对齐 9:30），派生 bar 标注 `source=derived:<base>`。
- 存储分区包含 `interval`，不同粒度互不干扰。

## 7. 远程采集节点

A 股/港股的部分源（东财等）对海外 IP 不可用，Binance 主站对美国 IP 返回 451，因此采集与存储可分离：

- `CollectorNode` 两种实现：`LocalNode`（进程内）与 `RemoteNode`（HTTP 调用远端 `openbars node serve`）。
- 远端节点**只执行 Job 并回传 RawBatch**（或写入共享对象存储后回传引用），不持有规范层；额度闸门对 `per_ip` 类源以节点为作用域。
- 认证：共享 token（`OPENBARS_NODE_TOKEN`）+ TLS；节点白名单只接受声明的市场/源。
- 配置中按 `nodes[].markets` 绑定路由；Job 可指定 `node`。

## 8. 插件机制

- Provider 通过 Python entry points 注册：`[project.entry-points."openbars.providers"] alpaca = "openbars_alpaca:AlpacaProvider"`。核心内置几个首批 provider，第三方可单独发包。
- 同样的 entry point 组：`openbars.calendars`、`openbars.storage`、`openbars.reconcile_rules`。
- `openbars.providers.registry` 负责发现、按配置实例化、校验能力声明（缺字段直接拒绝加载）。
- 插件 SDK 稳定性：v0.x 期间接口可变，`Provider.api_version` 字段用于兼容检查。

## 9. 口径约定

| 议题 | 约定 |
| --- | --- |
| 时间戳 | 一律存 UTC 的 **bar 开始时刻** `ts_start`；另存交易所时区的 `session_date`。适配层负责把“收盘时刻标识”的源转换过来。 |
| 日线切分 | 股票：交易所本地交易日，只含常规时段（盘前盘后不并入日线）。加密：**UTC 00:00 切日**（见 §3.4.2）。 |
| 小时线（预留） | 美股默认按开盘对齐（9:30 起，最后一根 15:30–16:00），整点对齐可选；A 股每天 4 根（午休不出 bar）；港股含午休，收市竞价口径待定。 |
| 复权 | v0.1 默认使用兼容 profile `research-daily-v2`：只拆股复权、按源交付落库（见 §11.2）。后续可选 profile：**落库只存不复权价 + 公司行动表**；复权在查询时按 `adjustment` 参数派生。前复权（qfq）不落库，因为每次除权都会改写整段历史。默认读取口径 `split`。 |
| 拆股时点冲突 | 以公司行动表为准，多源投票；不一致区段标 `divergent` 并记审计。 |
| 成交量 | 每根 bar 标 `feed`；**不同 feed 的成交量不混用**（如 Alpaca IEX vs SIP）；volume 优先级与 price 分开配置。 |
| 货币 | 存交易所原币种，不做汇率换算。 |
| 缺口 | 期望网格中缺失 → 先尝试下一优先级源 → 仍缺写入 `known_gaps`（附原因），canonical 中不插值。 |
| 异常值 | 单日涨跌幅超阈值且无公司行动 → `spike_suspect`，不自动删除。 |
| 修订 | 源改写历史时 canonical 产生新 `revision`，旧值保留在审计表。 |

## 10. 存储布局（Parquet + DuckDB 默认）

实现：`openbars.storage.schema`（schema 定义）、`parquet_duckdb`（默认后端）、`postgres`（可选后端）、`openbars.raw.archive`（原始响应归档）。schema 版本 **`SCHEMA_VERSION = 1`**（`openbars-storage-v1`）。

```text
data/
  state.db                         # SQLite：jobs、checkpoints、fetch_log（quota_* 表名预留给 QuotaGate，本层不创建）
  raw/provider=alpaca/market=us/interval=1d/year=2024/part-*.parquet
  raw/provider=…/market=…/interval=5m/year=2024/month=03/part-*.parquet   # 日内粒度多一层 month
  canonical/market=us/interval=1d/year=2024/part-*.parquet
  ref/instruments.parquet, symbol_mappings.parquet, corporate_actions.parquet, calendars/   # instruments 含加密交易对（§3.2.1）
  ref/adjust_factors.parquet       # A 股复权因子（可选，缺文件 = 空表；§3.4、§10.3）
  raw-archive/provider=…/market=…/interval=…/<window>/<request_digest>-<content_digest>.json
  audit/reconcile/…, known_gaps.parquet     # 预留
  openbars.duckdb                  # 只含视图与 catalog_meta，可随时重建
```

- 写入原子化：同目录临时文件（`.<name>.<token>.tmp`）→ `fsync` → `os.replace`，再 fsync 目录。写到一半被 `kill -9` 只会留下被读取方忽略的 `.tmp`（`compact()` 顺带清理陈旧 `.tmp`），已有 part 不受影响。
- zstd 压缩；分区键 `market / interval / year`（raw 另有 `provider` 在最前；分钟线再加 `month`）。分区值同时作为列写入文件（自描述，不依赖 hive 路径解析）。
- Postgres 后端实现同一 `StorageBackend` 接口，表结构与 Parquet schema 一一对应（见 §10.5、§11.3）。

### 10.1 写入模型：不可变 part + 最后写入者胜出

- 每次 `append` / `upsert` 对涉及的每个分区写入**一个新的不可变 part**（文件名 `part-<19 位纳秒时间戳>-<随机后缀>.parquet`，字典序即写入先后；同分区内时间戳严格递增，不受时钟回拨影响）。绝不原地修改旧文件。
- **去重规则（same key, last write wins）**：键相同的行里，文件名最大的 part 中的那一行可见。读取与 DuckDB 视图都用窗口函数实现该规则，所以重复行在 compaction 之前就已不可见。同一批内键重复时，批内最后一行胜出。
- 写入前与分区内当前可见行比较（忽略血缘列，见下）；**内容完全一致则不写文件**。因此重试同一个 Job、重复 upsert 不会产生新文件或重复行。血缘列：raw 的 `fetch_id / source_artifact_ref / ingest_run_id / ingested_at`，canonical 的 `generated_at`——只有这些列不同的重写视为无变化（历史请求见 `fetch_log`）。
- `compact()`：把每个分区内的所有 part 合并成**一个**去重后的 part（原子写入，再删除输入）。合并产物保留最新输入的排序位次，因此并发写入的更新 part 仍然胜出；崩溃发生在“新 part 已落盘、旧 part 未删”之间时，数据冗余但读取结果一致。
- 写入者用 `data/.lock`（`flock`）串行化；读取不加锁。
- 新增的新 revision（`revision` 属于键的一部分）是独立的行，不会覆盖旧 revision。（这一条只对 raw 成立；canonical 的键不含 `revision`，新 revision 覆盖同键旧行，时点读取见 §10.4 `available_by`。）

### 10.2 数值与模型对应

- 价格 `decimal128(20,8)`，成交量 `decimal128(28,8)`（Parquet / DuckDB `DECIMAL` / Postgres `NUMERIC` 一致），内存中用 `Decimal`，**不经过二进制 float 往返**。
- `core.models` 的 `Bar / RawBar / CanonicalBar` 价格与成交量已是 `Decimal`。存储层摄入：`Decimal` / `str` / `int` **不做静默舍入**，小数位或总位数超出列精度直接报错；若仍传入 `float`，则用 `Decimal(str(value))`（最短 repr）再量化到 8 位小数。读取侧 `scan` / `scan_rows` / `scan_table` 均返回精确 `Decimal` 或 Arrow decimal，不经二进制 float 往返。日线字段声明以 `openbars.core.schema` 为准，存储层在其上补充分区键（`market`）、`session`、`flags`、`price_source` 等实现列（见 §10.3、§10.7）。
- 模型没有的字段不改模型，而是放在存储行映射里：`RawBar.extra` 的键 `instrument_id / session_date / adjustment / price_basis / adjustment_scope / adjustment_as_of / session_semantics / currency / volume_basis / revision / vwap / trade_count`（其余键不丢：见下 `extra` 列；Postgres 没有该列，仍只存这些列）；`market` 取自 `RawBatch.request.market`；`session_date` 取 `extra["session_date"]`，否则取 `RawBar.session_date`（A 股 bar 从上海 00:00 开始，UTC 日期是前一天），再否则缺省为 `ts_start` 的 UTC 日期（normalizer 应提供交易所本地交易日）。规范层 `CanonicalBar` 的 `market` 取自 instrument id 前缀（`us:eq:…`）或 instrument master；`policy_version`（默认 `research-daily-v2`）、`selection_reason`（默认 `unspecified`）、`capabilities`、`source_fingerprint`（默认由键与 OHLCV 的 SHA-256 生成）为 `upsert` 的参数，`quality_status` 由 `QualityFlag` 推导（`divergent` > `spike_suspect` > `volume_missing` > `complete`；`stale` / `adjustment_unverified` 只体现在 `flags` 列，不改变状态，flags 是事实来源）。时间戳必须带时区（naive 一律拒绝）。
- 校验：写入前检查 OHLCV 不变量（价格 > 0、`low ≤ open/close ≤ high`、成交量 ≥ 0），与 upstream 一致；raw 可用 `validate=False` 照原样归档。

### 10.3 Schema v1

**`raw_bars`**（与 upstream `daily_bar` 对应，外加抓取身份）

| 列 | 类型 | 说明 |
| --- | --- | --- |
| `provider`, `market`, `feed`, `adjustment` | string | `feed` 无则为 `""`；`adjustment` 为源交付口径，未复权为 `raw` |
| `symbol_as_returned` | string | provider 原样返回的代码 |
| `instrument_id` | string, null | 解析前为空 |
| `interval` | string | `1d` … |
| `ts_start` | timestamp(us, UTC) | bar 开始时刻 |
| `session_date` | date32 | 交易所本地交易日 |
| `price_basis`, `adjustment_scope`, `session_semantics`, `volume_basis` | string | 取值同 §11.2；未知为 `unknown`（`volume_basis` 未提供时默认 `unknown`） |
| `adjustment_as_of` | date32, null | |
| `currency` | string, null | 3 位大写字母 |
| `revision` | int32 | 默认 0 |
| `open/high/low/close` | decimal128(20,8) | |
| `volume` | decimal128(28,8), null | 空 = 源未给成交量 |
| `vwap` | decimal128(20,8), null | |
| `trade_count` | int64, null | |
| `fetch_id` | string | 对应 `fetch_log` |
| `source_artifact_ref` | string, null | `raw-archive` 下的相对路径（或摘要） |
| `ingest_run_id` | string, null | |
| `ingested_at` | timestamp(us, UTC) | |

唯一键：`(market, interval, provider, feed, adjustment, revision, coalesce(instrument_id, symbol_as_returned), 时间)`，其中时间在 `1d` 为 `session_date`、日内为 `ts_start`（相对需求中的键多了 `market`：symbol 只在市场内唯一）。

**`canonical_bars`**（与 `research_daily_bar` 对应，外加 openbars `Bar` 元数据）

`instrument_id, market, interval, ts_start, session_date, session, policy_version（默认 research-daily-v2）, provider, feed, adjustment, revision, open/high/low/close decimal128(20,8), volume decimal128(28,8)（可空）, vwap（可空）, trade_count（可空）, flags list<string>（QualityFlag 成员名，如 DIVERGENT；不丢信息）, selection_reason, quality_status, capabilities list<string>, source_fingerprint, price_source, volume_source（可空）, sources_seen list<string>, reconcile_rule, generated_at, available_at timestamp(us, UTC)（可空，#154）`。

**`available_at`（#154，附加可空列，`SCHEMA_VERSION` 不变）**：这根 bar 最早可以被研究使用的时刻。取值：选中价格源对应 raw 行的 `ingested_at`（从 raw 层读回的 bar 经 `to_raw_bar` 带出）；本次刚抓到、内存里还没有落库 `ingested_at` 的 bar，用 canonical 写入时刻（节点总是先写 raw 再调 sink，所以写入时刻不早于 raw 的 `ingested_at`，只会偏保守，不会前视）；从上游导入时写上游选中源行的 `received_at`。**同一个源 bar 保留最早的可得时间**：同键重写且 `source_fingerprint` 不变时，取已存值与新值中较早的非空值（重跑同一 Job 不会把可得时间往后推，已存的 null 可以被补上）；`source_fingerprint` 变了（换源、新 revision、价量变化）就用新行自己的值。旧文件和 #154 之前的 Postgres 表没有这一列，读回为 null。加性可空列与 `corporate_actions.currency` 同理：列名、类型、键、分区都没有变，旧文件照常可读，所以不升 `SCHEMA_VERSION`（§10.6）。

唯一键：`(instrument_id, interval, policy_version, 时间)`，`1d` 时间为 `session_date`，即 `(instrument_id, session_date, interval, policy_version)`（upstream 是 `(instrument_id, session_date, policy_version)`，这里为将来的日内粒度加入 `interval`；日内以 `ts_start` 取代 `session_date`，否则同一交易日的多根 bar 会互相覆盖）。

**`ref/`**：`instruments`（`instrument_id, market, asset_class, currency, name, exchange, listed_on, delisted_on, external_ids map<string,string>`，键 `instrument_id`）；`symbol_mappings`（`instrument_id, provider, symbol, valid_from, valid_to`，键 `(instrument_id, provider, symbol, valid_from)`，`valid_to` 含当日）；`corporate_actions`（`instrument_id, ex_date, kind, source, ratio decimal(20,8), amount decimal(20,8), currency` 可空 ISO 4217，键 `(instrument_id, ex_date, kind, source)`；`kind` 含 `rights`（供股，只记录不调价）；**`currency` 为附加可空列，旧文件缺列 = `None`，`SCHEMA_VERSION` 不变**）。`adjust_factors`（A 股复权因子；Parquet 为 `ref/adjust_factors.parquet`，Postgres native 为同列同键的 `adjust_factors` 表（#124，见 §10.5）；`instrument_id, source, method, effective_from, fore_adjust_factor, back_adjust_factor, adjust_factor decimal(20,8), symbol 可空`，键 `(instrument_id, source, effective_from)`；**新增表，缺文件 = 空表，`SCHEMA_VERSION` 不变**；DuckDB 视图 `adjust_factors`；API：`ParquetInstrumentMaster.upsert_adjust_factors(...)` / `adjust_factors(instrument_ids=None, source=None)`）。ref 表很小，每次 upsert 合并后整文件原子重写。`resolve(symbol, on, provider, market)`：按 `valid_from ≤ on ≤ valid_to` 与 instrument 的市场过滤；provider 专属映射优先于 `"*"`；同一优先级命中多个不同 instrument 视为歧义返回 `None`。

**`state.db`（SQLite）**：`schema_meta`、`jobs`（`job_id` 主键；window 拆为 `window_start/end`，`instruments` 为 JSON 数组）、`checkpoints`（`job_id` 主键）、`fetch_log`（主键 `(provider, fetch_id)`；`market, interval, window_start/end, status(started|ok|empty|partial|error), artifact_ref, row_count, error_code, error_message, started_at, finished_at, job_id`）。

### 10.4 DuckDB 目录与读取

- `openbars.duckdb` 只含视图（`raw_bars`、`canonical_bars`、`instruments`、`symbol_mappings`、`corporate_actions`）与 `catalog_meta`；视图对 `raw/**/part-*.parquet` 取 glob 并套用 §10.1 的去重规则，因此永远只看到完整文件。`rebuild_catalog()` 删除并重建全部视图；`query()` 在目录缺失、数据目录被移动或某层首次出现文件时自动重建。
- `DuckDbBarReader.bars(...)` 返回 `pyarrow.Table`（精确 decimal），支持整个市场的一年日线查询（`symbols_or_ids` 为空即全市场）。**复权**：请求口径等于落库口径（`stored_adjustment`，`research-daily-v2` 为 `SPLIT`）且无需撤销时走快路径，直接过滤已存的 `adjustment` 列（落库值原样）；`RAW` / `SPLIT_DIVIDEND` / `ALL`，或 `SPLIT` 且 `as_of` 之后还有拆股时，读取落库 bar + 公司行动并交给 `normalize.adjust` 派生（见 §3.4）；分红口径会越过 `end` 加载后续交易日（算前收盘）再裁剪回查询窗口。`adjustment=None` 不过滤。派生时 instrument 没有任何公司行动行 → 默认抛 `AdjustmentError(missing_corporate_actions)`，`on_missing_actions="flag"` 则返回落库口径并打 `ADJUSTMENT_UNVERIFIED`；`SPLIT` + `as_of` 且无公司行动行时不报错，返回落库价并打 `ADJUSTMENT_UNVERIFIED`（as-of 声称未核实）。instrument 有拆股行时，抓取早于拆股的 bar 另打 `STALE`（见 §3.4）。**A 股（`cn-daily-v1`）**：`market=CN` 或 `cn:` id 自动选 `cn-daily-v1`，落库口径是 `RAW`；默认 `adjustment=SPLIT` 在这些行上表示“读落库序列”（= `RAW`，值原样），显式 `RAW` 同；`QFQ` / `HFQ` 读取落库 bar + `ref/adjust_factors.parquet` 并交给 `normalize.cn_adjust` 派生（`as_of` 既隐藏 `generated_at` 晚于它的行，也是因子的知识日期；`factor_source=` 指定因子来源，缺省取唯一来源）。缺因子：默认 `AdjustmentError(missing_adjust_factors)`，`on_missing_factors="flag"`（缺省沿用 `on_missing_actions`）则返回落库 `RAW` 并打 `ADJUSTMENT_UNVERIFIED`。A 股行绝不套用美股公司行动；`SPLIT_DIVIDEND` / `ALL` 对 A 股报 `unsupported_cn_adjustment`。
- **时点可得性 `available_by`（#154）**：`DuckDbBarReader.bars(..., available_by=)`、`StoreBarReader.bars`（Postgres native）与 `api.queries.load_canonical(..., available_by=)` 接受带时区的 `datetime`（naive 抛 `ValueError` / `QueryError`）。给了就只返回 `available_at <= available_by` 的行（边界含等号）；`available_at` 为 null 的行视为不可用（保守）。过滤作用在**每个键当前可见的那一行**上（§10.1 去重之后），在任何复权派生之前：同一 `(instrument_id, 时间, policy_version)` 被新 revision 覆盖后，新行在 `available_by` 时还不可得就不返回，**不会退回到被覆盖的旧 revision**。这与上游 `research_daily_bar`（每个 `(instrument_id, session_date, policy_version)` 只有一行，VCP 用选中源行的 `received_at <= available_by` 过滤）一致，结果也不随 compaction 变化。不给 `available_by` 时行为与以前完全相同。
- **`as_of` 有两层含义**：（1）知识日期：`generated_at` 晚于 `as_of` 的行不可见；（2）派生复权时它就是复权的 as-of 日期（`ex_date ≤ as_of` 的行动生效，之后的拆股被撤销）。
- **代码按日解析，覆盖整个查询窗口**：传入 `instrument_id` 原样查询；传入 ticker 时，对该 symbol 所有与 `[start, end]`（缺省 = 无界）有交集的映射（通用 `"*"` 映射，同市场、`valid_to` 含当日，歧义与唯一历史回退规则同 `resolve`）各展开为 `(instrument_id, 起, 止)` 区间，查询条件为 `(instrument_id = A AND session_date BETWEEN …) OR (instrument_id = B AND …)`。因此 ticker 被回收时，同一窗口内每家公司只返回其持有该 ticker 的那些日子的 bar，两家互不混合，ticker 指向别处（空档）的日子没有 bar。不再用 `end or today` 对整个查询做一次性解析。
- **按上游整数 id 读取（#155，`id_kind="upstream"`）**：`DuckDbBarReader.bars`、`StoreBarReader.bars`（Postgres native）、`api.queries.load_canonical` / `list_bars` 与 HTTP `GET /api/v1/bars?id=42&market=us&id_kind=upstream` 接受 `id_kind`，缺省 `auto`（即上面的 id / ticker 规则，行为不变）。`upstream` 时传入的是上游整数 id（`"42"` 或 `42`，只认 ASCII 数字），**原样匹配 `instrument_id`，覆盖整个查询窗口**：不查 symbol 映射、不查 instrument master、不要求 openbars 有任何 `instruments` / `symbol_mappings` 记录；身份（代码、别名有效期、代码复用）以上游自己的别名表为准，同一代码先后对应的两个 id 各读各的，不会合并。返回行的 `instrument_id` 原样带回；`list_bars` / HTTP 响应的 `symbol` 为 `null`，`market` 必须显式给出，raw 源也只按 `instrument_id` 匹配（不走代码回退）。非数字的 id 报错（`ValueError` / `QueryError` / HTTP 400）。取舍：选显式参数而不是“纯数字自动识别”，因为港股代码（如 `00700`）本身就是数字串并且有映射，自动识别会改变现有按代码查询的结果。
- 原始响应归档（`RawArchive.write`）：内容摘要（SHA-256）、`license_tag`（必填）、脱敏后的请求参数（键名含 `api_key / apikey / secret / token / password / authorization / credential` 的值替换为 `[REDACTED]`，并掩码字符串里的 `key=value` 形式凭据）；文件名由请求摘要与内容摘要决定，相同输入得到相同路径，重复写入是 no-op；同名但内容不同（摘要碰撞/被篡改）抛 `RawArchiveCollisionError`；原子写入（临时文件 + fsync + 独占 `link`）。响应正文原样保存。

### 10.5 Postgres 后端

`storage.postgres.PostgresBackend(dsn, mode=…, write=False)`：

- `native`：由同一份 schema 生成 DDL（`native_ddl()`），表与列和 Parquet 一一对应（`NUMERIC(20,8)/(28,8)`、`TEXT[]`、`JSONB`）。`raw_bars.extra` 是附加的可空 JSONB 列（#122），不在 `RAW_BARS_SCHEMA` / `TABLE_SCHEMAS` 里；`CREATE TABLE` 仍与共享 schema 一一对应，`write=True` 时再 `ALTER TABLE raw_bars ADD COLUMN IF NOT EXISTS extra JSONB`。只读打开尚未迁移的库不把缺列当成错误（视为 NULL）。`upstream_compat` 不改上游表、不跑该迁移，extra 在兼容模式下落掉。`canonical_bars.available_at`（#154）同理：`CREATE TABLE` 已带该列（`TIMESTAMPTZ`，可空），#154 之前建的库在 `write=True` 打开时 `ALTER TABLE canonical_bars ADD COLUMN IF NOT EXISTS available_at TIMESTAMPTZ`；只读打开时缺这一列不报错（读回 null），类型不符仍报 `SchemaMismatchError`。canonical upsert 对 `available_at` 用与 Parquet 相同的规则：`source_fingerprint` 不变时 `LEAST(已存值, 新值)`（`LEAST` 忽略 NULL），否则取新值。`SCHEMA_VERSION` 保持 1。唯一键用两个部分唯一索引（日线用 `session_date`，日内用 `ts_start`），写入走 `INSERT … ON CONFLICT DO UPDATE`（同样是最后写入者胜出）。只有 `write=True` 才建表；只读连接只校验表结构（不符抛 `SchemaMismatchError`）。除 `raw_bars / canonical_bars / instruments / symbol_mappings / corporate_actions` 与 state 表外，还有三张身份侧表（列同 `REF_IDENTITY_SCHEMAS`）：`unresolved`（键同 `UnresolvedRecord.key`：`kind, provider, market, symbol` + 仅 bar / corporate_action 计入的 `session_date` + 仅 corporate_action 计入的 `detail`；因 `session_date` / `detail` 可空，用表达式唯一索引而非主键）、`symbol_observations`（主键 `(instrument_id, symbol, provider)`，`last_observed` 取 `GREATEST`）、`upstream_ids`（主键 `instrument_id`，另对 `upstream_id` 唯一，1:1）。`PgInstrumentMaster` 实现完整 `InstrumentStore`（`instruments / mappings / remove_mappings / record_unresolved / unresolved / unresolved_count / discard_unresolved / observe / observations / set_upstream_id / upstream_ids`），`resolve` / `resolve_windows` 与 Parquet / 内存 master 共用 `resolve_symbol`（唯一历史回退、歧义规则一致），`ratio` / `amount` 读回 `Decimal`（入库同样把超过 8 位小数的值半偶舍入）。**A 股复权因子 `adjust_factors`（#124）**：列与 `ADJUST_FACTORS_SCHEMA` 一致（因子 `NUMERIC(20,8)`，`symbol` 可空），主键 `(instrument_id, source, effective_from)`；不放进 `TABLE_SCHEMAS` / `NATIVE_REQUIRED`，单独由 `ADJUST_FACTORS_REQUIRED` 校验：表存在时列和类型必须对，缺表的旧库只读打开视为空表（`adjust_factors()` 返回 `[]`），`write=True` 时 `CREATE TABLE IF NOT EXISTS` 补上。`PgInstrumentMaster.upsert_adjust_factors` / `adjust_factors(instrument_ids=None, source=None)` 与 Parquet 同名同义：按键 upsert（新值胜出），超过 8 位小数半偶舍入，按键排序读回精确 `Decimal`。`CnCanonicalSink` / `ingest_job_adjust_factors` 在 native 路径上照常写因子，`StoreBarReader` 对 A 股 `QFQ` / `HFQ` 用这些因子经 `normalize.cn_adjust` 派生（`factor_source`、`on_missing_factors` 同 `DuckDbBarReader`）。`upstream_compat` 不存因子（A 股不走上游 `research-daily-v2` 表），也不建这张表。`SCHEMA_VERSION` 不变。额度闸门（`PostgresQuotaStore`）的表 `quota_gate` / `quota_usage` 属于另一个 schema 域，由 `ensure_schema()` 创建，不在 `native_ddl()` 里。
- **`status` 覆盖摘要（#146）**：`PgRawStore.coverage_summary(provider, market, interval)` 与 Parquet 同签名同口径，返回 `(首日, 末日, 交易日数, bar 数)`。native 一条聚合查询走 `ix_raw_bars_scan` 的 `(provider, market, interval)` 前缀，键在库里已唯一，不需要再去重；`upstream_compat` 只统计 `daily_bar` 中该 provider 的美股日线（其它市场 / 粒度为空，多个 revision 各算一根 bar）。每条查询在事务里 `SET LOCAL statement_timeout`（默认 15 秒），超时抛 `CoverageTimeout`，`status` 对该行打印 `unavailable (...)` 并继续，不打印连接信息。
- `upstream_compat`：**不执行任何 DDL / 迁移**；构造时按 `information_schema` 做列级检查，缺表、缺列、类型不符（整数宽度不限，`numeric` 需精度一致）抛带 `problems` 列表的 `UpstreamSchemaError`。读取把 upstream 行映射为 openbars 行类型：`instrument_id` 转字符串；upstream 没有 `ts_start / fetch_id / price_source / reconcile_rule`，分别用会话日 00:00 UTC、`compat:<行号>`、`provider`、`policy_version` 占位；`quality_status` 映射回对应 `QualityFlag`。当 `write=True` 时写入现有 upstream 表（**仍不执行任何 DDL / 迁移**）：openbars 的字符串 `instrument_id` 若为整数则复用该 bigint，否则按代码在 `symbol_alias` 上按会话日查找；唯一命中则复用，零命中则插入 `instrument` + 覆盖该会话日的 `symbol_alias`（`valid_from` 不晚于首根 K 线），同一代码同一日命中多个 instrument（代码复用）抛出带候选人列表的 `AmbiguousInstrumentError`（数值 id、别名有效期、交易所 / 证券类型），要求改传其中一个数值 id。`daily_bar` 按 `(instrument_id, session_date, provider, feed, adjustment, revision)` upsert；`research_daily_bar` 按 `(instrument_id, session_date, policy_version)` upsert；能唯一对应到已有 `daily_bar` 则填写 `selected_source_bar_id`；只写 canonical 且尚无 raw 行时，按同一唯一键插入一行以 canonical OHLCV 为内容的 `daily_bar`（不覆盖已有 raw 值），从而成功的 canonical upsert 一定带上 source id。公司行动写入 `corporate_action_observation` / `corporate_action_event` / `corporate_action_revision`（及 link 表），`event_key` 为 `openbars:{action_type}:{instrument_id}:{ex_date}`，重复写入幂等；`corporate_actions()` 读回每个事件的最新修订并映射为 openbars `CorporateAction`（拆股 / 分红 / 分拆 / 代码变更）；无法识别的 `action_type` 不会静默丢弃，而是记入本地 unresolved 队列（`reason_code=unknown_action_type`）并发出告警。`mappings(provider=…)` 把 `symbol_alias` 列为 provider-agnostic（`provider="*"`）的 `SymbolMapping`，按 provider 筛选时通配项仍可见。`PgCompatInstrumentMaster` 实现完整 `InstrumentStore`：`instruments` / `get` / `remove_mappings` 走 upstream 表；`unresolved` / `observe` / 显式 `upstream_ids` 放在本地 SQLite（与 `state.db` 同目录的 `identity.db`），compat 永不向 upstream 做这些表的 DDL。规范层写入按会话日对 `(policy_version, session_date)` 加事务级 advisory lock，算法为 `sha256("research-daily:{policy_version}:{session_date}")` 的前 8 字节大端有符号 int64，再 `pg_advisory_xact_lock`，避免与 upstream 投影器争用唯一键；写入后递增 `research_series_watermark` / `research_series_date_watermark`。`write=False` 仍然只读。jobs / checkpoints / fetch_log 放在本地 SQLite（`state_path`）。
- 默认只读（只读事务 + 写方法抛 `ReadOnlyError`）。Postgres 后端的 schema 定义依赖 pyarrow，需同时安装 `parquet` extra。

### 10.6 Schema 版本与迁移

v1 是第一个落盘版本：每个 Parquet 文件的 key-value 元数据带 `openbars.schema_version=1` 与 `openbars.layer`，`state.db` 的 `schema_meta` 记录 `schema_version`。此后任何列名/类型/键/分区的变更都必须提升 `SCHEMA_VERSION` 并在此处记录迁移方式（Parquet：离线重写 part 并 `compact`；SQLite / Postgres：编号迁移脚本）；**读取方遇到更高版本的 Parquet 文件会拒绝而不是猜测**：`parquet_schema_version()` 读取页脚元数据 `openbars.schema_version`（缺失按 v1），若 `> SCHEMA_VERSION` 抛 `SchemaVersionError`（`StorageError` 子类），不迁移、不截断列。检查覆盖打开后端、`ref/*.parquet` 读取、raw / canonical 分区读取与写入前的去重比较、`compact`，以及 `rebuild_catalog()` / `query()` 建 DuckDB 视图之前（DuckDB 不会悄悄 union 一个 v2 文件）；通过的文件按 `(mtime, size)` 缓存，重复读取只做 `stat`。低于当前版本的文件不拒绝（v1 是第一个版本，暂无迁移器）。

**Parquet raw 的可选 `extra` 列（#94，`SCHEMA_VERSION` 仍为 1）**：Parquet raw 层的 schema（`RAW_BARS_PARQUET_SCHEMA`，仅 `RAW_LAYER` 使用）在 `RAW_BARS_SCHEMA` 之外多一列可空字符串 `extra`，内容是 JSON 对象，装 `RawBar.extra` 里没有独立列的键（BaoStock 的 `tradestatus` / `isST` / `preclose` / `code` 等；`Decimal` / `date` / `datetime` 以 `{"$decimal": …}` / `{"$date": …}` / `{"$datetime": …}` 带类型存储，`bool` / `int` / `str` 原样，键排序以保证重写无变化）。这是**加法式的可选列，仅 Parquet**：`RAW_BARS_SCHEMA` / `TABLE_SCHEMAS` 与 Postgres 不变（Postgres 不存 leftover extra）；旧文件没有这一列，读取时 DuckDB `union_by_name` / `_conform` 把缺失的可空列补成全 null，`_drop_unchanged` 也把它当 NULL 比较，所以旧文件照常可读，重新拉取同一天会因 `extra` 由空变有而重写一次（顺带补全），此后不再重写。

**canonical 的可空 `available_at` 列（#154，`SCHEMA_VERSION` 仍为 1）**：加在 `CANONICAL_BARS_SCHEMA` 末尾（Parquet 与 Postgres native 共用）。旧 part 缺这一列，`union_by_name` / `_conform` 读成 null，`DuckDbBarReader` 在所有文件都缺列时也补一列全 null；`_drop_unchanged` 把缺列当 NULL 比较，所以给旧行补上可得时间会写一个新 part。Postgres 的迁移见 §10.5。

**字符串枚举的追加成员**（例如 `session_semantics` 新增 `utc_day`，`openbars.core.series` 与 `openbars.storage.schema` 两份枚举的值列表必须一致，由测试断言）：向字符串枚举**追加**成员不提升 `SCHEMA_VERSION`；列名、类型、键、分区都没变，磁盘上仍是字符串列。旧文件（`provider_aligned` / `regular` 等）可被新代码读取；新写入方可写出 `utc_day`。旧代码若遇到未知值，会按现有 `_enum_value` 拒绝该行（`InvalidBarError`），而不是整文件 `SchemaVersionError`。这是有意的：列集合未变。`policy_version`（口径版本，§11.2）与 `SCHEMA_VERSION`（存储格式版本）互相独立。

### 10.7 Arrow / Parquet schema 声明（`openbars.core.schema`）

日线字段的权威声明在 `openbars.core.schema`；本层的 Parquet 写入、DuckDB 视图、compaction 与 Postgres 映射见上文 §10.1–10.6，并在实现中复用这些字段（另补分区与元数据列）。`SCHEMA_VERSION = 1`；schema 的 metadata 带 `openbars.schema_version`。模块不强依赖 pyarrow：字段声明是纯数据（`RAW_DAILY_BAR_FIELDS`、`CANONICAL_DAILY_FIELDS`），`raw_daily_bar_schema()` / `canonical_daily_schema()` 在调用时才惰性构造 `pyarrow.Schema`（需要 `openbars[parquet]`）。

类型映射：价格 `decimal128(20,8)`；成交量 `decimal128(28,8)`；`session_date` / `adjustment_as_of` 为 `date32`；时间戳为 `timestamp[us, tz=UTC]`；`revision` 为 `int64`；`capabilities` 为 `list<item: string>`；其余为 `string`。

原始日线（raw daily bars），`?` 表示可空：

| 组 | 字段 |
| --- | --- |
| 标识 | `instrument_id?`、`session_date?`、`ts_start`、`symbol_as_returned`、`fetch_id`、`revision`（>= 1） |
| 源身份三元组 | `provider`、`feed`、`adjustment`（旧版 adjustment 字符串，如 `split` / `raw`） |
| 序列语义 | `interval`（`"1d"`）、`price_basis`、`adjustment_scope`、`adjustment_as_of?`、`session_semantics`、`currency?`、`volume_basis` |
| 数值 | `open`、`high`、`low`、`close`（价格）、`volume?`（成交量）、`vwap?`（价格） |
| 溯源 | `payload_hash?`、`ingested_at?` |

规范层日线（research-daily-v2，对应 `CanonicalDailyRow`），唯一键 `(instrument_id, session_date, policy_version)`：

`instrument_id`(string)、`session_date`(date32)、`policy_version`(string)、`selected_source_bar_id?`(string)、`provider`、`feed`、`adjustment`(string)、`revision`(int64)、`open`、`high`、`low`、`close`(decimal 20,8)、`volume`(decimal 28,8)、`selection_reason`、`quality_status`(string)、`capabilities`(list<string>)、`source_fingerprint`(string)、`generated_at`(timestamp UTC)、`currency?`(string)、`available_at?`(timestamp UTC，#154，见 §10.3)。

`instrument_id` 在 openbars 中是稳定字符串 ID（非整数外键）；`selected_source_bar_id` 在存储 ID 出现之前可为空。`selection_reason` 取 `primary_source` / `primary_source_corroborated` / `compatible_gap_fill` / `fill_<provider>` / `spike_replaced`；`quality_status` 取 `complete` / `divergent` / `volume_missing` / `spike_suspect`（均以字符串存储，`fill_<provider>` 不是固定枚举）。

内存模型：源身份 `SourceIdentity(provider, feed, adjustment)`、`BarSeriesIdentity`、枚举 `PriceBasis` / `AdjustmentScope` / `SessionSemantics` / `VolumeBasis` 与 `BarSeriesCatalog.resolve(source, *, interval=Interval.D1, currency="USD")` 位于 `openbars.core.series`。目录的保守映射：

| 源身份（`1d`） | `price_basis` | `adjustment_scope` | `session_semantics` | `volume_basis` |
| --- | --- | --- | --- | --- |
| `alpaca/iex/split`、`alpaca/sip/split`、`massive/eod/split` | `split_adjusted` | `splits` | `provider_aligned` | `split_adjusted` |
| `alpha_vantage/daily/raw` | `raw` | `none` | `provider_aligned` | `as_traded` |
| `twelve_data/daily/split` | `split_adjusted` | `splits` | `provider_aligned` | `unknown`（成交量处理依赖源） |
| `yfinance/yahoo/split` | `provider_defined` | `unknown` | `provider_aligned` | `provider_defined`（可检视，不视为已证明兼容） |
| `yfinance/yahoo/raw`（港股日线，`currency=HKD`） | `raw` | `none` | `regular` | `as_traded` |
| `akshare/eastmoney/raw`（港股日线备源，`currency=HKD`） | `raw` | `none` | `regular` | `as_traded` |
| `binance/spot/raw` | `raw` | `none` | `utc_day` | `as_traded` |
| `coinbase/exchange/raw` | `raw` | `none` | `utc_day` | `as_traded` |
| `baostock/ashare/raw` | `raw` | `none` | `regular` | `as_traded` |
| 其他 | `unknown` | `unknown` | `unknown` | `unknown`（`currency` 仍按入参设置） |

## 11. 上游搬运来源与 backend 兼容与切换

openbars 的数据层**以一套已在生产运行的上游内部实现（下称 upstream）为主搬运**，不是重新发明；搬运时按 [`docs/sanitization.md`](sanitization.md) 脱敏。upstream 的模块路径与 openbars 的逐文件对照只记录在仓库外的私有笔记中，本文与 issue 只写功能描述。

### 11.1 搬运来源（按层）

| openbars 层 | 来自 upstream 的功能 | 搬运方式 |
| --- | --- | --- |
| `core.errors` / `providers.base` | 统一错误模型 `ProviderError(code, retryable, retry_at, status_code, request_id)`、有界诊断信息、HTTP 状态 → 错误码映射、响应中凭据替换为 `[REDACTED]` | 直接搬 |
| `providers`（HTTP 公共层） | 共享 httpx 客户端、请求前限速钩子、超时/传输错误映射、分页 token 防死循环、payload 摘要与数值校验 | 直接搬 |
| `providers.us.*` | Alpaca SIP/IEX 日线（多代码批量、免费档最近时段处理）、Massive grouped daily / 单代码区间 / 代码清单 / 退市清单 / 代码类型、yfinance 批量（`auto_adjust=False`）、公司行动适配器；Twelve Data、Alpha Vantage 作为可选兜底源 | 直接搬，改导入与配置注入 |
| `quota` | 进程内固定节奏限速、**持久化令牌闸门**（按 provider 行锁预约下一个请求时刻）、可选日额度（UTC 日 / 滚动 24h）与 `retry_at`、各源请求上限常量 | 改接口后搬：常量变为 `QuotaPolicy` 默认值；新增 SQLite 实现（upstream 只有 PG 行锁） |
| `orchestrator` | Job 租约与重试、优先级策略、过期租约回收、调度单例锁、每日同步计划与批次切分、历史回补窗口与“覆盖即废弃”规则、每会话重建调度 | 改接口后搬：脱离 upstream ORM 与 worker 进程模型 |
| `core.calendar` / `calendars` | XNYS 交易日历（复用 `exchange_calendars`，见 §3.4.1）、加密 24/7 UTC 日历（见 §3.4.2）、收盘后等待 N 分钟才算完成的会话、非交易日拒收 | 直接搬（已完成） |
| `normalize.instruments` | 点时（point-in-time）身份证据与解析、代码别名有效期、退市目录、身份对账、代码类型 → 证券类型映射、ETF 作为独立证券类型 | 改接口后搬 |
| `normalize.adjust` / `normalize.actions` | 公司行动“观测追加 + 规范修订”模型、拆股后旧历史重抓检测、`adjustment_as_of` 一致性检查（落库口径只拆股复权，不含分红；分红口径仅查询时派生） | 改接口后搬（已完成：投票合并、检测、查询时派生，见 §3.4） |
| `reconcile` | **research-daily-v2 选数**：按源优先级、兼容性闸门、兜底源能力集、价格 0.5% / 成交量 5% 容差、独立源佐证（quorum 2）、质量标记、人工/规则覆盖、确定性数据修复 R1–R4（预览/应用、熔断上限、可撤销审计） | 核心逻辑直接搬，持久化改接口 |
| `storage` / `raw` | 原始响应归档（内容摘要、许可标签）、按源 bar 表、规范层表、水位表 | Parquet+DuckDB 重写；PG 提供 upstream 兼容适配（§11.3） |
| `verify` | 跨源对比与每日校准、覆盖率对账、覆盖汇总、历史深度报告 | 改接口后搬 |

不搬：基本面与申报、宏观序列、新闻、扫描/策略/回测/提醒/选股、研究代理、HTTP API 与鉴权、计费与权限、运维控制台、指数成分校验、AI 辅助退市复核、依赖授权数据的成分股导入。理由：不属于“行情采集与清洗”，或包含私有业务/授权数据。

### 11.2 口径对齐

openbars 提供一个与 upstream 逐字段一致的**兼容口径 profile `research-daily-v2`**，v0.1 美股日线默认使用它；§9 中“落库只存不复权 + 查询时派生复权”的模式作为后续可选 profile（见 roadmap），不影响兼容 profile。

| 项 | `research-daily-v2` 约定（与 upstream 一致） |
| --- | --- |
| 源身份 | 三元组 `(provider, feed, adjustment)`，取值保持原字符串：`massive/eod/split`、`alpaca/sip/split`、`alpaca/iex/split`、`yfinance/yahoo/split`、`twelve_data/daily/split`、`alpha_vantage/daily/raw` |
| 序列语义 | 每根源 bar 带 `price_basis`、`adjustment_scope`、`adjustment_as_of`、`session_semantics`、`currency`、`volume_basis`；由单一目录（catalog）按源身份保守解析，未知即 `unknown` |
| 复权 | **只拆股复权**（split-adjusted as delivered），不做分红复权，不是总回报序列；拆股后旧历史按新基准重抓；同一窗口内 `adjustment_as_of` 混用视为 `mismatch` |
| 选数优先级 | 主源 `massive/eod/split`(0) → `alpaca/sip/split`(1)；兜底按信任度 `yfinance`（价量可用）→ `twelve_data`（仅价格，成交量口径未知）。实际效果：近端由 Massive 覆盖，2016 起历史由 Alpaca SIP 覆盖，2016 前由 yfinance 补齐 |
| 容差与佐证 | 价格 0.5%、成交量 5%；与次源分歧时需 2 个独立源佐证主源，才判定次源出错 |
| `selection_reason` | `primary_source`、`primary_source_corroborated`、`compatible_gap_fill`、`fill_<provider>`、`spike_replaced` |
| `quality_status` | `complete`、`divergent`、`volume_missing`（价格有变动但成交量为 0）、`spike_suspect`（R4 标记，是否排除由开关控制） |
| `capabilities` | 有序字符串集合，如 `price_basis:split_adjusted`、`adjustment_scope:splits`、`session_semantics:provider_aligned`、`volume_basis:*`、`currency:USD`、`adjustment_as_of:YYYY-MM-DD`、`price_ohlc`、`consolidated_volume`、`split_adjusted_volume` |
| 规范层行 | `instrument_id, session_date, policy_version, selected_source_bar_id, provider, feed, adjustment, revision, open, high, low, close, volume, selection_reason, quality_status, capabilities, source_fingerprint, generated_at`；唯一键 `(instrument_id, session_date, policy_version)` |
| 数值 | 价格 `Numeric(20,8)`、成交量 `Numeric(28,8)`，内存中用 `Decimal`，不经 float 往返 |
| 会话 | 交易所会话日（XNYS），非交易日 bar 拒收；当日会话收盘后等待可配置分钟数才视为完成 |
| 失效通知 | 每个 policy 一行版本水位 + 按日期的水位表，供下游缓存失效 |

加密日线见 §11.2.1，使用独立的 `policy_version`，美股 profile 不用于加密。A 股日线使用独立的 `policy_version = "cn-daily-v1"`（§11.2.2），同样不套用美股 profile。

`policy_version` 是口径的版本号：任何改变选数结果的规则修改都必须换新版本号（如 `openbars-daily-v1`），不得静默改写 `research-daily-v2` 的语义。

#### 11.2.1 加密日线口径 `crypto-daily-v1`（`openbars.reconcile.crypto`）

> 为避免打乱既有 §11.3–§11.5 的引用，本节编号为 §11.2.1（对应“加密日线口径”）。`research-daily-v2` 是美股口径，**不用于加密**；加密走独立的 `policy_version = "crypto-daily-v1"`，不替代、不修改美股默认口径（`DEFAULT_POLICY`）。

- **按 `instrument_id` 对账**：id 形如 `crypto:spot:<venue>:<BASE>-<QUOTE>`（§3.2.1），归属唯一 venue。不同交易所的同 base/quote 是**两个 instrument**，**不能**拿来对价格、**不能**互相填缺口、**不能**当作独立佐证。
- **只有同一 instrument 的同 venue 多源才进入仲裁**：
  - Binance instrument：主源 `binance/spot/raw`（现货 klines）；备源预留 `binance/archive/raw`（同所批量归档，目前无 provider，不在 `KNOWN_SOURCES`，仅在目录中登记同样的 `utc_day / raw / none / as_traded` 口径）。
  - Coinbase instrument：主源 `coinbase/exchange/raw`（Exchange candles），只服务 Coinbase 的 instrument，暂无备源。
  - 无兜底源（`fill_sources` 为空）：绝不以 Coinbase 填 Binance instrument，反之亦然。
- **venue 过滤**（`reject_cross_venue(observations, instrument_id)`）：先于选数执行。`instrument_id` 不是目标的 bar 丢弃；目标 instrument 上但来源 venue 不属于该 instrument 的 bar（如 Coinbase bar 误挂在 `crypto:spot:binance:…`）以 `cross_venue` 丢弃；来源 provider 未知同样不可挂接。被丢弃的 bar 不进入 `project_canonical_bars`，不做次源、填充或佐证，由 `reconcile_crypto` 作为拒收记录返回。
- **容差**（`Decimal` 百分比）：价格 **1%**、成交量 **10%**（美股为 0.5% / 5%）。同一交易对的不同加密源（实时 klines 与再发布的归档）差异可能大于美股 SIP 与官方收盘价之间的差异，所以放宽；但超出仍然只标记，不平滑。`corroboration_quorum` 保持 2，且跨 venue 的 bar 不计入独立佐证。
- **分歧处理**：同一 instrument 的主源与同所备源超出容差，`quality_status = divergent`（并去掉成交量能力，沿用既有质量标记与 `selection_reason`），**选中的仍是主源整根 bar，绝不取平均**、不拼接不同源的价量。
- **切日与完成时间**：`CryptoCalendar`，UTC 00:00 切日、`session_semantics = utc_day`（§3.4.2）；周末、节假日都是会话；会话收盘（次日 00:00 UTC）后等待 `completion_wait_minutes = 30` 才视为完成。
- **入口**：`policy_for_instrument(instrument_id)`（非加密 id 或无注册源的 venue 抛 `ValueError`，只有主备源随 venue 变化）、`reconcile_crypto(instrument_id, start, end, observations, *, generated_at=None)`；`CRYPTO_DAILY_V1` 是 Binance venue 策略的别名。
- **编排器接入**：`run` 的落库由 `orchestrator.finalize` 完成。`CryptoReconcilingSink` 对 `job.market = CRYPTO` 的 bar：缺 `instrument_id` 时用 `master.resolve(symbol, on=session_date, provider=bar.provider, market=CRYPTO)` 解析，**解析不到的只保留 raw，不写 canonical**；按 `instrument_id` 分组后调用 `reconcile_crypto`（绝不使用 `research-daily-v2`），Coinbase 观测因 `reject_cross_venue` 不会填补或佐证 Binance instrument。`DispatchingSink(us=…, crypto=…)` 按 `job.market` 选 sink，使调用方仍只传一个 sink；`ReconcilingSink`（XNYS）遇到加密任务只留 raw。canonical 行的 `policy_version = crypto-daily-v1`，CLI `run` 现通过 `cli.wiring.make_sink(backend, providers)` 构造 `DispatchingSink`（美股 `ReconcilingSink`/XNYS + 加密 `CryptoReconcilingSink`），`markets: [crypto]` 即写 raw + canonical；美股任务仍走美股对账口径。`openbars verify` 对加密市场使用 `CryptoCalendar` 与 `crypto-daily-v1`。
- **读路径（#51）**：`policy_version_for_market(market, override=None, *, instrument_ids=())`（`openbars.reconcile.crypto`，并由 `openbars.reconcile` 导出）：`override` 优先；`Market.CRYPTO` 或 `crypto:` id → `crypto-daily-v1`；其余 → `research-daily-v2`。`DuckDbBarReader.bars` 的 `policy_version` 默认为 `"auto"`（`storage.schema.AUTO_POLICY_VERSION`）：`market=CRYPTO` 或所请求 id 为加密 id 时读 `crypto-daily-v1`，其它（含省略参数的美股调用）仍读 `research-daily-v2`；未给 `market` 时按每个 id 各自选口径，可混读；显式传入的字符串永远优先。`scan_table(..., policy_version=None)`（不过滤）的语义不变。加密 canonical 不含复权基础，读路径对其行不做复权推导，也不受 `adjustment` 参数影响。
- **ticker 解析**：加密映射只有 provider 专属（`binance` / `coinbase`），不写通用 `"*"`。读取时对加密市场，ticker 除通用映射外还会通过其各 provider 专属映射解析，因此 `bars(["TESTUSDT"], market=CRYPTO)`（或不给 `market`，由 master 推断）返回对应交易对的 `crypto-daily-v1` 行；同一文本若出现在两个 venue，则两个 venue 的 instrument 都会返回（不合并）。美股的 ticker 解析不变。Postgres 读取器未改。

#### 11.2.2 A 股日线口径 `cn-daily-v1`

> 编号 §11.2.2 同样是为了不打乱既有引用。`research-daily-v2` 是美股口径，A 股走独立的 `policy_version = "cn-daily-v1"`（常量 `POLICY_VERSION_CN_DAILY_V1`，`openbars.reconcile.crypto`，由 `openbars.reconcile` 导出）。

- **落库口径**：不复权 `RAW`（`baostock/ashare/raw`，`raw / none / regular / as_traded`，币种 CNY）；`session=regular`，会话日取沪深日历。`QFQ` / `HFQ` 永不落库，读取时由复权因子派生（§3.4）。
- **单源投影**：目前只有 BaoStock，`CnCanonicalSink` 直接把 raw 投影成 canonical 行（`selection_reason=primary_source`），不经过任何对账器；停牌日不写 bar、涨跌停打标记（§3.4.4、§3.5.3）。
- **读路径**：`policy_version_for_market(market, override=None, *, instrument_ids=())`：`override` 优先；`Market.CRYPTO` 或 `crypto:` id → `crypto-daily-v1`；`Market.CN` 或 `cn:` id → `cn-daily-v1`；其余 → `research-daily-v2`。

### 11.3 PG 兼容策略

- `storage.postgres` 有两种 schema 模式：
  - `native`：openbars 自己的表结构（与 Parquet schema 一一对应），由 openbars 迁移管理。
  - `upstream_compat`：直接读写 upstream 现有表（`instrument`、`symbol_alias`、`daily_bar`、`research_daily_bar`、两张水位表、`provider_request_gate` / `provider_request_usage`、`source_artifact`、`data_ingest_run`、公司行动相关表）。
- `upstream_compat` **不执行任何迁移**，表结构归 upstream 所有；启动时做列级 introspection，缺列或类型不符即拒绝运行并报告差异。
- 默认只读；写入需显式 `storage.postgres.write: true`，并且只写 openbars 负责的表。
- 并行期额度共享：CLI 默认仍用本地 SQLite `QuotaGate`；把 `storage.postgres.quota` 设为 `postgres` 时，改用 `PostgresQuotaGate` 读写同一套 `provider_request_gate` / `provider_request_usage`（compat 永不迁移这些表），保证 openbars 与 upstream 共用一个 key 时不会叠加超限（或者给 openbars 配独立 key / 继续用 sqlite）。
- 互斥：规范层重建沿用 upstream 的 PG 事务级 advisory lock 键算法，避免两边同时重建同一 policy。

### 11.4 backend 切换步骤

1. **搬运 + 金标准**：按 11.1 顺序搬运；每个模块搬运时，用合成输入在 upstream 侧生成一次期望输出，冻结为 openbars 的金标准样例（§11.5）。
2. **作为库导入**：upstream 依赖 `openbars`，把对应内部模块改为薄 shim（re-export openbars 的实现），行为由金标准保证不变。可以逐模块替换，先换纯函数层（日历、序列目录、兼容性闸门、选数），再换 provider 与额度闸门。
3. **影子运行**：openbars 以 `upstream_compat` 只读 + 独立 `policy_version`（或独立 schema）跑完整日线管线，`openbars verify` 与 `research-daily-v2` 逐 bar 对比，指标：覆盖率、价格一致率、质量标记一致率。
4. **切换写入**：对比稳定后，由 openbars 接管 `research-daily-v2` 的写入（`write: true`），upstream 抓取 worker 逐个下线：先兜底源，再主源。
5. **回滚**：切换期间保留 upstream worker 代码与开关，任一指标回退时关闭 openbars 写入、恢复 upstream worker；因为口径与表结构一致，无需数据迁移。
6. **之后**：多市场需求直接通过 openbars 配置开启；upstream 只保留业务层。

### 11.5 兼容性测试（只用合成数据）

- `tests/golden/research_daily_v2/`：合成代码（如 `ZZZA`）、合成价格的输入 → 期望的规范层行（含 `selection_reason`、`quality_status`、`capabilities`、`source_fingerprint`）。覆盖：仅主源；主次一致；主次分歧（`divergent`）；佐证 quorum 判定次源出错；兼容缺口填充；yfinance / twelve_data 兜底的能力差异；`volume_missing`；R4 覆盖（替换 / 标记）；非交易日拒收；代码复用的别名有效期；拆股后 `adjustment_as_of` 混用。
- 金标准由维护者在私有环境用 upstream 纯函数对同一合成输入生成后冻结；openbars 测试要求逐字段一致，`source_fingerprint` 一致。
- `tests/compat/upstream_schema.sql`：仅包含相关表的 DDL（无数据、无私有命名），测试时用 Postgres 建表后跑 `upstream_compat` 适配器的读写与 introspection 测试。
- 禁止使用真实行情数据或 provider 抓包作为夹具（见 `docs/sanitization.md`）。

## 12. 代码结构

```text
src/openbars/
  core/          models.py · ohlcv.py · series.py · schema.py · errors.py · calendar.py
  calendars/     xnys.py · crypto.py · cn.py · hk.py
  config/        schema.py · loader.py
  cli/           main.py · serve.py · init.py · shadow.py · wiring.py
  orchestrator/  jobs.py · planner.py · runner.py · nodes.py
  quota/         policy.py · gate.py
  providers/     base.py · registry.py · (us/alpaca … · crypto/binance · crypto/coinbase 实现随 issue 加入)
  raw/           store.py · archive.py
  normalize/     instruments.py · crypto_pairs.py · cn_instruments.py · cn_ingest.py · cn_adjust.py · cn_quality.py · adjust.py · actions.py · normalizer.py · …
  reconcile/     policy.py · reconciler.py · audit.py
  storage/       base.py · schema.py · rows.py · state.py · parquet_duckdb.py · postgres.py · bar_reader.py
  api/           reader.py · http.py · queries.py
  web/           static/   （本机只读界面：index.html、assets、vendor/lightweight-charts 4.2.3）
```

## 13. 本机只读界面（`openbars serve`）

v0.5 提供一个本地数据查看界面，用来看覆盖、质量问题和 K 线，而不是做一个交易终端。入口是 `openbars serve`（需要 `uv sync --extra http`）。

**访问边界。** 默认绑定 `127.0.0.1:8787`。这一版一律拒绝非回环 `--host`：只要传入非回环地址就报错退出，不会在局域网或公网端口上监听。存储以只读方式打开；界面不提供任何写入或抓取入口。额度快照只返回用量，不含密钥。没有 `quota.db` 时不会创建该文件。

**`/api/v1` 不是公开 API。** 这些 JSON 路由只给本机页面内部使用，本版不承诺稳定，也不提供 OpenAPI / 文档页。外部系统应继续使用 Python `BarReader`。

**页面（中英可切换）。** 看板展示各市场覆盖、首末交易日、质量标记计数和额度用量；质量标记页列出明细；K 线页用随包固定版本的 TradingView Lightweight Charts（4.2.3，Apache-2.0）画图。K 线默认显示按规则对齐填充后的聚合数据（canonical），可通过 `source=canonical|<provider>` 切换到各渠道原始数据；原始数据中的缺日以缺口和标注显示，缺字段也会标出。

