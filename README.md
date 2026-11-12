# openbars

> **Free, multi-market, multi-source OHLCV bar collection, cleaning and reconciliation into one canonical dataset.**
>
> **免费、多市场、多数据源的 K 线采集、清洗与合并框架，产出一份可审计的唯一数据源。**

**Status / 状态：pre-alpha** — 美股日线、加密日线、A 股日线（BaoStock，可选依赖）的本地 `plan` / `run` 链路已在本仓库可用；`verify` 覆盖 `markets` 中的每个市场（美股 / 加密 / A 股 / 港股，各用自己的日历与 `policy_version`）；港股日线（yfinance 主源，AKShare 佐证）同样可用；美股 1h 可选采集到 raw 层；其他非日线粒度仍在路线图中。API 随时会变。

*US daily, crypto daily and A-share daily (BaoStock, optional extra) local `plan` / `run` are available in this tree; `verify` covers every market in `markets` (US / crypto / A-shares / Hong Kong, each on its own calendar and `policy_version`); Hong Kong daily (yfinance primary, AKShare corroboration) is available too; US 1h collection is available as raw bars; other non-daily intervals come later. Pre-alpha: the API may change.*

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

美股（含 ETF）日线 → 加密日线 → A 股日线 → 港股日线 → 本机只读界面（v0.5）；美股小时线顺延至 v0.6。小时/分钟线作为可选能力逐步开放。详见 [docs/roadmap.md](docs/roadmap.md)、[docs/architecture.md](docs/architecture.md) 与 [docs/upstream-switch.md](docs/upstream-switch.md)（把既有研究序列切到 openbars）。

### 快速开始

需要 Python ≥ 3.11 与 [uv](https://docs.astral.sh/uv/)。以下以美股日线为例：

```bash
uv sync --all-extras                 # 安装（含 Parquet / DuckDB）
uv run openbars init                 # 在当前目录生成 config.yaml 和 .env（已被 .gitignore；绝不覆盖已有文件）
# 编辑 .env 填入你自己的 key（或用 OPENBARS_ENV_FILE / --env-file 指向你已有的 dotenv）
uv run openbars config               # 查看生效配置（所有 key 脱敏为 set/missing）
# 可选：在 config.yaml 里设置 universe.symbols（Alpaca / yfinance 需要显式 symbols）
uv run openbars plan                 # 预览任务与请求预算（不写入）
uv run openbars run                  # 规划、入队并执行，直到队列空闲；可随时重跑续抓
uv run openbars status               # 任务状态、失败/死信、断点、raw 覆盖摘要（Parquet / Postgres）
uv run openbars verify               # 各市场的覆盖率与多源一致性报告（只读）
# 可选：安装 http 额外依赖后，在本机打开只读界面（默认仅绑定 127.0.0.1；非回环 --host 会被拒绝）
# uv sync --extra http
# uv run openbars serve              # 打开 http://127.0.0.1:8787
# 可选：与外部 research-daily-v2 只读 API 做影子对账（符号列表与报告都放在仓库外）
# uv run openbars shadow --symbols-file /path/to/symbols.txt \
#   --start YYYY-MM-DD --end YYYY-MM-DD --output /path/to/report-dir --fetch
```

- `init [目录]` 在终端启动向导；非终端或 `--yes` 复制示例配置。参见[数据源密钥申请与额度指南](docs/provider-keys.md)。任一目标已存在就报错退出（退出码 2），什么都不写。
- **密钥优先级（高 → 低）**：① 进程环境变量；② dotenv 文件：`--env-file`，否则 `OPENBARS_ENV_FILE`，否则 `config.yaml` 的 `env_file`，都没有时自动读取 **`config.yaml` 同目录的 `.env`**（不存在也不报错）；③ `config.yaml` 里 `${VAR:-default}` 的默认值。也接受 `ALPACA_API_KEY` / `ALPACA_API_SECRET` 作为 `ALPACA_API_KEY_ID` / `ALPACA_API_SECRET_KEY` 的别名（仅当规范名未设置时）。
- 示例配置里 Alpaca 与 Massive 都是 `enabled: true` 的主源：`run` 在它们缺 key 时会报错并列出变量名（如 `MASSIVE_API_KEY`），请填上 key，或把不用的源改成 `enabled: false`。`plan` 只提示缺失，不会中止（但没有任何可用源、或选了无源支持的粒度时会报错）。
- `quota:` 只能**收紧**默认免费额度；要调高必须写 `quota: { per_minute: 999, override: true }`，否则报错。
- 再次提醒：openbars 不分发任何数据，一切由你用自己的 key（BYOK）在本地抓取，请遵守各数据源的服务条款（详见下方声明）。

#### 本机只读界面

`openbars serve` 在本机打开覆盖看板、质量标记和 K 线页面（中英可切换），只绑定回环地址。界面是只读的：没有抓取或写入入口。K 线默认显示按规则对齐填充后的聚合数据（canonical），也可以切换到各个渠道的原始数据；原始数据中的缺日或缺字段会在图上标出。`/api/v1` 只给这些页面内部使用，本版不承诺稳定，请不要把它当成对外数据 API。

- 数据写在 `storage.path`（默认 `./data/`，已被 git 忽略）；`--data DIR` 可覆盖。`run` 幂等：已完成的窗口不会重复抓取，每天再跑一次即为增量。
- 非日线粒度会触发较大的请求预算，需要显式 `--accept-budget` 才会提交/执行。
- `verify` 是**报告而非闸门**：即使一致率不到 100% 也返回 0。对 `markets` 中的**每个市场**各出一份报告，按各自的日历与 `policy_version`：美股 XNYS + `research-daily-v2`；加密 `CryptoCalendar`（UTC 每天都是交易日，含周末）+ `crypto-daily-v1`；A 股 XSHG（XSHE 为别名，不含北交所）+ `cn-daily-v1`；港股 XHKG（半日市也算交易日）+ `hk-daily-v1`。窗口终点取该市场最近一个“已就绪”的交易日（可被 `history.end` / `--end` 提前）。某个已配置市场尚无落库数据时只给出提示并继续；所有市场都没有数据才报错。内容：预期交易日 × 标的的覆盖率、各来源（provider / feed / 复权）汇总、两源可比较时的 OHLC 与成交量一致率（默认容差价格 0.5%、成交量 5%，可由 `reconcile.tolerances` 调整；IEX 与 SIP 等不可比的序列记为 `not_comparable`，不计入分歧）、质量标记一致率（仅当有多个 projection 时，否则 `n/a`），并附“最近 N 个交易日”分段（`--recent N`）。`--start/--end` 限定窗口，`--json reports/verify.json` 把报告写到数据目录下：只配置一个市场时是单个报告对象（带 `market` / `calendar` 字段），配置多个市场时为 `{"markets": {"us": {…}, "cn": {…}}}`（没有数据的市场列在 `skipped`）。
- 默认规划器把每个交易日分给最便宜的来源，所以多源重叠需要你显式抓取一部分（例如给第二个来源排审计任务）后，`verify` 才有可比较的样本。
- `shadow` 只对外部 research-daily-v2 HTTP API 发 GET：用 `--symbols-file` 提供标的（仓库不带默认股票池），可选 `--fetch` 经 QuotaGate 按**显式标的列表**抓取（不会走整市场 grouped 接口）。报告写到 `--output`；请把符号列表和结果放在仓库外。密钥与 bearer token 不会写入报告。
- 本仓库的端到端测试使用约 100 个合成标的（含 ETF）与离线假 provider 验证整条链路；**没有**对真实标的做抽样，也不与任何外部生产库做对比——测试不访问网络，仓库不含任何行情数据。

### 美股小时线（可选）

在 `config.yaml` 中设置（`1h` 可单独使用，也可与 `1d` 并列）：

```yaml
markets: [us]
intervals: [1d, 1h]
universe:
  us:
    symbols: [TEST1, ZZZA]
conventions:
  us_hourly_alignment: session_open
  include_extended_hours: false
```

类似 `ON` 或数字的代码请加引号，例如 `"ON"`、`"0001"`。

启用所需 provider 并配置自己的凭据；所有美股 1h 来源都需要显式 `universe.us.symbols`，包括 Massive。`session_open` 按纽约时间 9:30 开盘对齐；也可选 `clock_hour`，但规划器会跳过只支持 `session_open` 的 yfinance 和 Twelve Data。默认只存常规时段；Twelve Data 不支持盘前盘后。Massive 和 Alpaca 将 30 分钟数据合成为网格上的 1 小时数据。

将 `storage.path` 指向仓库外的位置；默认 `./data` 位于仓库内且被 git 忽略，请勿把行情数据提交进 git。先运行 `uv run openbars plan` 查看任务和请求预算，再运行 `uv run openbars run --accept-budget`；非日线提交或执行需要此确认。可用 `uv run openbars status` 查看进度，重跑可从断点续抓。

免费档和历史窗口可能变化，请核对官方资料；以下是本项目的保守默认值或实现限制：

- [Massive Basic](https://massive.com/pricing)：小时线约最近 2 年（730 天），默认每分钟 4 次。
- [yfinance](https://ranaroussi.github.io/yfinance/)：非官方 Yahoo 数据，约最近 730 天（实现留 1 天余量）；默认每分钟 30 次是保守节流，并非官方配额。
- [Alpaca](https://docs.alpaca.markets/docs/about-market-data-api)：小时线最早从 2016-01-01 开始；默认每分钟 200 次，示例配置收紧为 190 次。
- [Twelve Data](https://twelvedata.com/pricing)：默认每分钟 8 次、每 UTC 日 800 次；实现将历史起点设为 2016-01-01，实际可用范围取决于服务商。

操作、计时与容量估算见[本地实测手册](docs/measuring-hourly.md)。

小时规范层仍待 #116；小时线与日线的一致性校验仍待 #116。
看板 1h 支持仍待 #119。

### A 股日线（BaoStock）

```bash
uv sync --extra baostock             # 或 pip install "openbars[baostock]"
# config.yaml：markets: [cn]；providers.baostock.enabled: true；universe.cn.symbols: [sh.600000, 000001.SZ]
uv run openbars plan && uv run openbars run
```

- 不需要 API key。`universe.cn.symbols` 写 `sh.600000` 或 `600000.SH`；**不接受**裸 6 位代码（会被跳过并给出提示）。
- 条款：BaoStock 仅限**个人本地使用**；请勿提交或分发行情数据（`data/` 已被 git 忽略）。海外网络可能连不上 BaoStock，此时任务会失败并记入任务状态（`openbars status` 可查），网络恢复后重跑即可续抓。
- 落库的是**不复权**序列（`cn-daily-v1`，`adjustment=raw`）：停牌日只留在 raw 层（带 `tradestatus`），规范层不写占位并标记涨跌停；复权因子首次拉取上市以来全量、之后只拉增量。读取时传 `adjustment=Adjustment.QFQ` / `Adjustment.HFQ`（前/后复权在查询时派生，不落库）。
- 可选冒烟：`OPENBARS_BAOSTOCK_SMOKE=1 uv run pytest tests/test_baostock_public_smoke.py`（1 只股票、2 个交易日，默认跳过）。
- `verify`（A 股）：按 XSHG 日历、`cn-daily-v1`，只评分落库的 **RAW** 序列，QFQ / HFQ 是读取时派生的，不在评分之内。规范层的 `quality_status` 在 `LIMIT_UP` / `LIMIT_DOWN` / `SUSPENDED` 标记下仍为 `complete`，报告会统计这些标记的数量。停牌日 raw 保留、规范层不写占位，这个预期的空洞单独记为 `suspended_cells`，不计入 `missing`。

### 港股日线

- 配置 `markets: [hk]`、启用 `providers.yfinance`（AKShare 作佐证），`universe.hk.symbols` 写 `"00700"` / `0700.HK`；`plan` / `run` 使用 XHKG 日历。落库的是 RAW 序列（`hk-daily-v1`，HKD），仅拆股调整在读取时派生。
- `verify`（港股）：按 XHKG 日历、`hk-daily-v1`，评分落库的 RAW 序列，不评分读取时派生的调整结果。

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

Default storage is Parquet + DuckDB; Postgres is optional. Read through the Python library, or open the local read-only UI with `openbars serve` (loopback only; `/api/v1` is for that UI and is not a stable public API in this version).

### Quick start

Needs Python ≥ 3.11 and [uv](https://docs.astral.sh/uv/). US daily bars as the example:

```bash
uv sync --all-extras                 # install (includes Parquet / DuckDB)
uv run openbars init                 # write config.yaml and .env here (git-ignored; never overwrites)
# fill in .env with your own keys (or point OPENBARS_ENV_FILE / --env-file at a dotenv you already have)
uv run openbars config               # print effective settings (secrets redacted as set/missing)
# optional: set universe.symbols in config.yaml (Alpaca / yfinance jobs need explicit symbols)
uv run openbars plan                 # preview jobs and request budget (writes nothing)
uv run openbars run                  # plan, queue and execute until idle; safe to re-run / resume
uv run openbars status               # job states, dead jobs, checkpoints, raw coverage (Parquet / Postgres)
uv run openbars verify               # per-market coverage and cross-source agreement report (read-only)
# Optional: install the http extra and open the local read-only UI (loopback only; a non-loopback --host is rejected)
# uv sync --extra http
# uv run openbars serve              # http://127.0.0.1:8787
# Optional: shadow-compare against an external research-daily-v2 HTTP API (read-only).
# Keep the symbol list and the report outside this repository.
# uv run openbars shadow --symbols-file /path/to/symbols.txt \
#   --start YYYY-MM-DD --end YYYY-MM-DD --output /path/to/report-dir --fetch
```

- `init [directory]` starts a terminal wizard; non-terminal use or `--yes` copies the example files. See the [provider key and limits guide](docs/provider-keys.md). If either target already exists it prints an error, writes nothing and exits 2.
- **Key priority (highest first):** (1) the process environment; (2) a dotenv file: `--env-file`, else `OPENBARS_ENV_FILE`, else `env_file` in `config.yaml`, and when none of those is set, **`.env` next to `config.yaml`** is loaded automatically (no error if it is absent); (3) `${VAR:-default}` in `config.yaml`. `ALPACA_API_KEY` / `ALPACA_API_SECRET` are accepted as aliases for `ALPACA_API_KEY_ID` / `ALPACA_API_SECRET_KEY` when the canonical name is unset.
- The example config enables both Alpaca and Massive as primary providers: `run` fails with the missing variable names (for example `MASSIVE_API_KEY`) if one lacks credentials, so fill the keys in or set `enabled: false` on providers you do not use. `plan` only notes missing credentials (but errors when no usable provider is left, or when an interval is selected that no enabled provider offers).
- `quota:` may only **tighten** the default free-tier limits; raising one needs an explicit `quota: { per_minute: 999, override: true }`, otherwise it is an error.
- Reminder: openbars distributes no data; everything is fetched locally with your own keys (BYOK), and you must follow each provider's terms (see the disclaimer below).

#### Local read-only UI

`openbars serve` opens a coverage dashboard, quality-flag list, and K-line page (Chinese/English) on a loopback address only. The UI is read-only: it has no fetch or write actions. Charts default to the aggregated canonical series and can switch to each provider's raw bars; missing sessions and fields are marked on the chart. `/api/v1` exists only for this local UI and is not a stable public API in this version.

- Data lands in `storage.path` (default `./data/`, git-ignored); `--data DIR` overrides it. `run` is idempotent: finished windows are not fetched again, so running it daily is the incremental update.
- Non-daily intervals imply a large request budget and need an explicit `--accept-budget`.
- `verify` is a **report, not a gate**: it exits 0 even when agreement is below 100%. It produces one report per market in `markets`, each on its own calendar and `policy_version`: US XNYS + `research-daily-v2`; crypto `CryptoCalendar` (every UTC day is a session, weekends included) + `crypto-daily-v1`; A-shares XSHG (XSHE is an alias; Beijing is out of scope) + `cn-daily-v1`; Hong Kong XHKG (half days count as sessions) + `hk-daily-v1`. The window ends at the market's latest ready session (optionally capped by `history.end` / `--end`). A configured market with no stored bars is a note and the run continues; only when no market has bars is it an error. Each report shows coverage of expected sessions x instruments, per-source summaries (provider / feed / adjustment), OHLC and volume agreement wherever two comparable sources hold the same bar (default tolerances: price 0.5%, volume 5%, adjustable via `reconcile.tolerances`; incomparable series such as IEX vs SIP count as `not_comparable`, never as disagreement), quality-flag agreement (only when several projections exist, else `n/a`), and a "last N sessions" breakdown (`--recent N`). `--start/--end` bound the window; `--json reports/verify.json` writes the report under the data directory: one configured market gives a single report object (with `market` / `calendar` fields), several give `{"markets": {"us": {...}, "cn": {...}}}` (markets without data are listed under `skipped`).
- The default planner assigns each session to the cheapest source, so cross-source overlap needs an explicit fetch of a sample (for example audit jobs for a second provider) before `verify` has anything to compare.
- The repository's end-to-end test exercises the whole path with ~100 synthetic symbols (including ETFs) and offline fake providers. It does **not** sample real tickers or compare against any external production database: tests never touch the network and the repo ships no market data.

### US hourly bars (optional)

Set the following in `config.yaml` (`1h` can also be used without `1d`):

```yaml
markets: [us]
intervals: [1d, 1h]
universe:
  us:
    symbols: [TEST1, ZZZA]
conventions:
  us_hourly_alignment: session_open
  include_extended_hours: false
```

Quote tickers like `ON` or numeric codes, for example `"ON"` and `"0001"`.

Enable the providers you need and configure your own credentials. Every US 1h source, including Massive, needs explicit `universe.us.symbols`. `session_open` aligns to the 9:30 New York open; `clock_hour` is also available, but the planner skips yfinance and Twelve Data, which require `session_open`. Only regular hours are stored by default; Twelve Data does not support extended hours. Massive and Alpaca aggregate 30-minute bars onto the hourly grid.

Point `storage.path` outside the repository; the default `./data` is inside the repository and git-ignored. Do not commit market data. First run `uv run openbars plan` to inspect jobs and the request budget, then `uv run openbars run --accept-budget`; submitting or running non-daily jobs requires this acceptance. Check progress with `uv run openbars status`; rerunning resumes from checkpoints.

Free tiers and history windows can change; check the official references. These are conservative project defaults or implementation limits:

- [Massive Basic](https://massive.com/pricing): approximately the last 2 years (730 days) of hourly data; default 4 requests per minute.
- [yfinance](https://ranaroussi.github.io/yfinance/): unofficial Yahoo data for approximately the last 730 days (the implementation leaves a one-day margin); the default 30 requests per minute is conservative pacing, not an official quota.
- [Alpaca](https://docs.alpaca.markets/docs/about-market-data-api): hourly history starts no earlier than 2016-01-01; default 200 requests per minute, tightened to 190 in the example config.
- [Twelve Data](https://twelvedata.com/pricing): defaults are 8 requests per minute and 800 per UTC day; the implementation sets a 2016-01-01 history floor, while actual availability depends on the provider.

See the [local measurement guide](docs/measuring-hourly.md#english) for timing and storage estimates.

Hourly canonical reconciliation and the hourly-versus-daily consistency check are pending #116.
Dashboard 1h support is pending #119.

### A-share daily (BaoStock)

```bash
uv sync --extra baostock             # or: pip install "openbars[baostock]"
# config.yaml: markets: [cn]; providers.baostock.enabled: true; universe.cn.symbols: [sh.600000, 000001.SZ]
uv run openbars plan && uv run openbars run
```

- No API key. Write `universe.cn.symbols` as `sh.600000` or `600000.SH`; bare 6-digit codes are **not** accepted (they are skipped with a note).
- Terms: BaoStock is for **personal local use only**; never commit or redistribute market data (`data/` is git-ignored). BaoStock may be unreachable from overseas networks; jobs then fail and show up in `openbars status`; re-run once the network is back to resume.
- The stored series is **unadjusted** (`cn-daily-v1`, `adjustment=raw`): suspended sessions stay in the raw layer only (with `tradestatus`), canonical gets no placeholder bar and carries limit-up / limit-down flags. Adjust factors are fetched as full history the first time and incrementally afterwards. Pass `adjustment=Adjustment.QFQ` / `Adjustment.HFQ` at read time (derived on query, never stored).
- Optional smoke test: `OPENBARS_BAOSTOCK_SMOKE=1 uv run pytest tests/test_baostock_public_smoke.py` (1 symbol, 2 sessions; skipped by default).
- `verify` (A-shares): XSHG calendar, `cn-daily-v1`, scores the stored **RAW** series only; QFQ / HFQ are derived at read time and are not scored. `quality_status` stays `complete` under `LIMIT_UP` / `LIMIT_DOWN` / `SUSPENDED` flags, and the report counts those flags. A suspended day keeps its raw bar but gets no canonical placeholder; that expected hole is reported separately as `suspended_cells` and is not counted as `missing`.

### Hong Kong daily

- Set `markets: [hk]`, enable `providers.yfinance` (AKShare corroborates), and list `universe.hk.symbols` as `"00700"` / `0700.HK`; `plan` / `run` use the XHKG calendar. The stored series is RAW (`hk-daily-v1`, HKD); only split adjustment is derived at read time.
- `verify` (Hong Kong): XHKG calendar, `hk-daily-v1`, scores the stored RAW series, not the read-time adjusted view.

### Data terms disclaimer

- **openbars does not distribute or host any market data.** The repository contains code, rules and documentation only.
- All data is fetched locally **with your own accounts and API keys** (BYOK).
- Most free data sources restrict use to personal or internal purposes and forbid display or redistribution. **You are responsible for reading and complying with each provider's terms of service.**
- openbars is not affiliated with any data provider and makes no warranty about data accuracy, completeness or availability.

## License

[Apache-2.0](LICENSE)
