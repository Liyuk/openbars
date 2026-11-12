# 本地测量美股小时线 / Measuring US hourly bars locally

## 中文

本手册补充 [README 的小时线配置](../README.md#美股小时线可选)，侧重可重复的测量。示例仅用合成标识或公开量级；openbars 不分发行情，使用者须遵守数据源条款。配置、行情、计时和测量输出均留在仓库外，不要提交或公开原始报告。以下路径是仓库外的占位示例，请替换为自己的路径。

### 落盘与计时

1. 按 README 设置美股、显式证券代码、`intervals: [1d, 1h]`、`us_hourly_alignment: session_open` 和 `include_extended_hours: false`。配置 `storage.path` 指向仓库外，并启用所需来源、通过环境变量提供凭据。选择相同的证券数 N 和历史窗口 W 以便比较；示例标识可用 `EXAMPLE`，但实际抓取需要来源支持的代码。
2. 运行 `uv run openbars plan --config ~/openbars-data/config.yaml --env-file ~/openbars-data/.env`，记录每个来源估算的请求数和配额推算的最短耗时。它是估算和下界，不是实测承诺；规划输出会列出各粒度作业数，但预算按来源汇总。要分开测日线和小时线，分别设置 `intervals: [1d]` 和 `intervals: [1h]`，对相同 N、W 各运行一次 plan。
3. 核对预算后，用 `time uv run openbars run --config ~/openbars-data/config.yaml --env-file ~/openbars-data/.env --accept-budget` 记录墙钟时间和 run 的请求计数；非日线执行或 `plan --submit` 需要接受预算。记录来源、窗口、粒度、N、有效节流、worker 数、缓存及是否续跑；请求分页、重试、网络、交易日和数据可用性都会改变结果。`uv run openbars status --config ~/openbars-data/config.yaml --env-file ~/openbars-data/.env` 显示作业状态和按粒度的 raw 覆盖；中断后重跑可从检查点续抓。续跑时间不能直接当作完整回补时间。

免费档会变化，请重新核对官方资料和账户权限。实现的窗口与默认节流见 [README](../README.md#美股小时线可选)：[Massive Basic](https://massive.com/pricing) 约 2 年；[yfinance](https://ranaroussi.github.io/yfinance/) 约 730 天（实现留一天余量，30 次/分是保守节流而非官方配额）；[Alpaca](https://docs.alpaca.markets/docs/about-market-data-api) 和 [Twelve Data](https://twelvedata.com/pricing) 以当时公开档为准，代码将小时历史起点设为 2016-01-01，实际可用范围取决于服务商。不要将默认节流当作账户剩余额度。

作为粗略量级，Alpaca 免费档以约 150 次/分节流时，两年常规时段 1h 回补每只证券约 100 次请求、固定间隔节流下墙钟时间约 40–45 秒；每个交易日约 7 根，两年每只约 3,500 行。这不是保证：用上述 plan 与 run 方法在自己的 N 和 W 上重测，分别报告日线与小时线。

### 存储测量

默认 Parquet 后端的 raw 按 provider、market、interval、年（非日线还有月）分区；canonical 按 market、interval、年分区，非日线还有月分区，没有 provider 分区。当前 1h 只落 raw；小时规范层和小时/日线一致性校验待 #116，看板 1h 待 #119。`status` 的覆盖摘要不是磁盘占用。

在数据静止时，将环境变量 `OPENBARS_STORE` 设为自己的仓库外 `storage.path`，从仓库运行以下脚本。它按物理文件统计字节和 Parquet 元数据行数，不读取或输出证券明细；需要已安装的项目依赖。分别比较 `1d` 和 `1h`：

```bash
uv run python - <<'PY'
import os
from collections import defaultdict
from pathlib import Path
import pyarrow.parquet as pq

root = Path(os.environ['OPENBARS_STORE'])
totals = defaultdict(lambda: [0, 0, 0])
for layer in ('raw', 'canonical'):
    for file in (root / layer).rglob('part-*.parquet'):
        dims = dict(part.split('=', 1) for part in file.relative_to(root / layer).parts[:-1] if '=' in part)
        key = (layer, dims.get('provider', 'all'), dims.get('market', 'unknown'), dims.get('interval', 'unknown'))
        totals[key][0] += file.stat().st_size
        totals[key][1] += pq.ParquetFile(file).metadata.num_rows
        totals[key][2] += 1
for key, (size, rows, files) in sorted(totals.items()):
    print(*key, 'bytes', size, 'rows', rows, 'files', files,
          'bytes/row', round(size / rows, 1) if rows else 'n/a')
PY
```

`canonical` 的 `all` 表示跨来源合并，不能据此将其文件字节分摊给单个 provider。物理行数包含旧 part 中可能被覆盖的版本；DuckDB catalog 的 `raw_bars` / `canonical_bars` 视图按键去重，可用于另算可见行数，但不要把去重行数误当作物理文件行数。上述字节仅含两层 Parquet，不含 state、索引、归档或其他文件；可另外用 `du` 测整个存储目录。压实前后和小文件数量会影响结果。

小规模试跑的 Parquet 约每行数百字节，小文件开销占主导，较大分区通常压缩更好。外推时按每层、来源（仅 raw）、粒度分别用测得的字节/行 × 预计行数；例如小时线用证券数 × 交易日/年 × 10 年 × 约 7 根/日，日线另用约 1 根/交易日，再按实际覆盖与重复版本调整。这是容量量级，不是全市场实测报告。若比较 Postgres，在相同数据、索引和保留策略下分别测表及索引的磁盘大小并除以行数，与 Parquet 的对应层和粒度比较数量级；记录压缩和维护状态，不公开连接信息。

## English

This guide complements the [README hourly setup](../README.md#us-hourly-bars-optional) with repeatable measurement. Examples use synthetic identifiers or public orders of magnitude. openbars does not redistribute market data; follow provider terms. Keep configuration, data, timings and measurement output outside the repository; do not commit or publish raw reports. The external paths below are placeholders; replace them with your own.

### Store and time

1. Follow the README for US markets, explicit symbols, `intervals: [1d, 1h]`, `us_hourly_alignment: session_open` and `include_extended_hours: false`. Set `storage.path` outside the repository, enable the providers and supply credentials through environment variables. Choose the same number of securities N and window W for comparisons. `EXAMPLE` is synthetic; actual fetching requires a supported symbol.
2. Run `uv run openbars plan --config ~/openbars-data/config.yaml --env-file ~/openbars-data/.env` and record estimated requests and the quota-derived minimum duration for each provider. These are estimates and lower bounds, not promises. The plan lists jobs by interval but aggregates budgets by provider. Measure daily and hourly separately by setting `intervals: [1d]` and `intervals: [1h]` in separate runs with the same N and W.
3. After inspecting the budget, use `time uv run openbars run --config ~/openbars-data/config.yaml --env-file ~/openbars-data/.env --accept-budget` to record wall time and the run's request count. Non-daily execution or `plan --submit` requires budget acceptance. Record provider, window, interval, N, effective pacing, worker count, cache and whether the run resumed. Pagination, retries, network, trading sessions and availability affect results. `uv run openbars status --config ~/openbars-data/config.yaml --env-file ~/openbars-data/.env` shows job states and raw coverage by interval; rerunning resumes from checkpoints. A resumed run's time is not a full backfill time.

Free tiers change; check official references and account access. See the [README](../README.md#us-hourly-bars-optional) for implemented windows and default pacing: [Massive Basic](https://massive.com/pricing) covers roughly two years; [yfinance](https://ranaroussi.github.io/yfinance/) roughly 730 days (the implementation leaves one day of margin; 30 requests/minute is conservative pacing, not an official quota). For [Alpaca](https://docs.alpaca.markets/docs/about-market-data-api) and [Twelve Data](https://twelvedata.com/pricing), check their current public tiers; the implementation sets a 2016-01-01 hourly history floor, while actual availability depends on the provider. Default pacing is not an account's remaining quota.

As a rough order of magnitude, an Alpaca free-tier backfill paced at about 150 requests/minute took around 100 requests and 40–45 seconds of wall time per symbol with fixed-interval pacing for two years of regular-session 1h bars. Expect about seven bars per trading day, or 3,500 rows per symbol over two years. These are not guarantees: repeat the plan and run method for your own N and W, reporting daily and hourly separately.

### Measure storage

The default Parquet backend partitions raw by provider, market, interval and year (also month for non-daily data); canonical is partitioned by market, interval and year (also month for non-daily data), without a provider partition. At this version 1h is stored only in raw. Hourly canonical and hourly/daily consistency checks are pending #116; dashboard 1h support is pending #119. The `status` coverage summary is not disk usage.

With writes stopped, set `OPENBARS_STORE` to your own external `storage.path` and run the script in the Chinese section above from the repository. It sums physical Parquet file bytes and metadata row counts by layer, provider, market and interval without printing symbol details. Compare `1d` and `1h` separately. Canonical's `all` combines sources: its file bytes cannot be attributed to an individual provider this way. Physical rows can include superseded versions in older parts; DuckDB catalog views `raw_bars` and `canonical_bars` deduplicate by key and can provide visible row counts separately. The script excludes state, indexes, archives and other files; use `du` on the whole store separately. Compaction and small-file counts affect results.

A small pilot found roughly a few hundred Parquet bytes per row, dominated by small-file overhead; larger partitions generally compress better. Extrapolate separately by layer, provider (raw only) and interval: measured bytes per row × expected rows. For example, estimate hourly rows as symbols × trading days/year × 10 years × roughly seven bars/day, and daily rows separately at roughly one bar/trading day; adjust for coverage and duplicate versions. This is a capacity estimate, not a published whole-market run. For a Postgres comparison, measure table and index disk sizes and rows with the same data, indexes and retention, then compare orders of magnitude with the corresponding Parquet layer and interval. Record compression and maintenance state; do not publish connection details.
