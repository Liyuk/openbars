# openbars

> **Free, multi-market, multi-source OHLCV bar collection, cleaning and reconciliation into one canonical dataset.**
>
> **免费、多市场、多数据源的 K 线采集、清洗与合并框架，产出一份可审计的唯一数据源。**

**Status / 状态：pre-alpha** — 架构与接口设计阶段，尚无可用实现。API 随时会变。

---

## 中文

### 它是什么

openbars 是一个**本地运行的行情仓库框架**，不是又一个数据接口库。你在配置里选好市场（美股/ETF、加密、A 股、港股）和粒度（默认日线；小时线、分钟线可选），填上自己的各源 API key，openbars 负责：

1. **Provider 适配**：统一的数据源抽象，每个源声明自己的能力（市场 × 粒度、历史深度、限额、条款）。
2. **配额感知调度**：在免费额度内编排抓取——共享限流闸门、重试、断点续抓、历史回补、每日增量。
3. **清洗与合并**：证券主数据（稳定 ID + 代码有效期）、交易日历、复权参数化、缺口检测、多源仲裁、质量标记，最终写成一份带来源与审计记录的**规范层（canonical）**数据。

默认存储为 Parquet + DuckDB，Postgres 可选。读取方式是 Python 库，HTTP 接口可选。

### 为什么

GitHub 上已有大量单源客户端（yfinance、ccxt、AKShare）和统一查询接口（OpenBB），但几乎没有项目同时做到：**免费额度下的多源调度 + 可审计的多源合并成唯一源 + 跨市场统一口径**。openbars 只做这一块。

### 路线

美股（含 ETF）日线 → 加密日线 → A 股日线 → 港股日线；小时/分钟线作为可选能力逐步开放。详见 [docs/roadmap.md](docs/roadmap.md) 与 [docs/architecture.md](docs/architecture.md)。

### 数据条款声明（重要）

- **openbars 不分发、不托管任何行情数据。** 仓库里只有代码、规则与文档。
- 所有数据由**用户使用自己的账号与 API key**（BYOK）在本地抓取。
- 多数免费数据源（如 Alpaca Basic、Massive Basic、Yahoo、Tiingo、Stooq 等）的条款限定个人或内部使用，禁止展示或再分发。**使用者须自行阅读并遵守各数据源的服务条款**；因违反条款产生的责任由使用者自负。
- openbars 与任何数据源提供方无关联，也不保证数据的准确性、完整性或可用性。

---

## English

### What it is

openbars is a **local-first market data warehouse framework**, not yet another data client. Pick markets (US equities/ETFs, crypto, China A-shares, Hong Kong) and intervals (daily by default; hourly and minute optional) in a config file, bring your own API keys, and openbars will:

1. **Adapt providers** behind one abstraction; each source declares its capabilities (market × interval, history depth, quotas, terms).
2. **Schedule within free quotas**: a shared quota gate, retries, resumable checkpoints, historical backfill and daily increments.
3. **Clean and reconcile**: instrument master (stable IDs + symbol validity ranges), trading calendars, parameterized price adjustment, gap detection, multi-source arbitration and quality flags, producing one **canonical** dataset with provenance and an audit trail.

Default storage is Parquet + DuckDB; Postgres is optional. Read through the Python library, with an optional HTTP API.

### Data terms disclaimer

- **openbars does not distribute or host any market data.** The repository contains code, rules and documentation only.
- All data is fetched locally **with your own accounts and API keys** (BYOK).
- Most free data sources restrict use to personal or internal purposes and forbid display or redistribution. **You are responsible for reading and complying with each provider's terms of service.**
- openbars is not affiliated with any data provider and makes no warranty about data accuracy, completeness or availability.

## License

[Apache-2.0](LICENSE)
