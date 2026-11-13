# Measuring US hourly bars locally

The planner checks 1h coverage against the expected session grid: a session counts as covered only when every regular hourly slot is stored (under either `session_open` or `clock_hour` alignment; early closes have fewer slots). A session with only some hours stored is planned again. US windows follow source readiness: Alpaca SIP is planned with the adapter's `now - 16 minutes` request cutoff, and after-close completeness remains best effort; Massive 1h and other unverified sources make no T+0 promise; verified delays are taken from the capability. Exception: when a successful fetch (`ok` / `empty`) after that session closed already answered it, the remaining missing hours are treated as no trades (thin trading, halts) and are not re-fetched forever; there is no need to wait for the next New York calendar day. Queued jobs also reserve their sessions. Raw-file compaction is out of scope.

This guide complements the [README hourly setup](../README.md#us-hourly-bars-optional) with repeatable measurement. Examples use synthetic identifiers or public orders of magnitude. OpenBars does not redistribute market data; follow provider terms. Keep configuration, data, timings and measurement output outside the repository; do not commit or publish raw reports. The external paths below are placeholders; replace them with your own.

## Store and time

1. Follow the README for US markets, explicit symbols, `intervals: [1d, 1h]`, `us_hourly_alignment: session_open` and `include_extended_hours: false`. Set `storage.path` outside the repository, enable the providers and supply credentials through environment variables. Choose the same number of securities N and window W for comparisons. `EXAMPLE` is synthetic; actual fetching requires a supported symbol.
2. Run `uv run openbars plan --config ~/openbars-data/config.yaml --env-file ~/openbars-data/.env` and record estimated requests and the quota-derived minimum duration for each provider. These are estimates and lower bounds, not promises. The plan lists jobs by interval but aggregates budgets by provider. Measure daily and hourly separately by setting `intervals: [1d]` and `intervals: [1h]` in separate runs with the same N and W.
3. After inspecting the budget, use `time uv run openbars run --config ~/openbars-data/config.yaml --env-file ~/openbars-data/.env --accept-budget` to record wall time and the run's request count. Non-daily execution or `plan --submit` requires budget acceptance. Record provider, window, interval, N, effective pacing, worker count, cache and whether the run resumed. Pagination, retries, network, trading sessions and availability affect results. `uv run openbars status --config ~/openbars-data/config.yaml --env-file ~/openbars-data/.env` shows job states and raw coverage by interval; rerunning resumes from checkpoints. A resumed run's time is not a full backfill time.

Free tiers change; check official references and account access. See the [README](../README.md#us-hourly-bars-optional) for implemented windows and default pacing: [Massive Basic](https://massive.com/pricing) covers roughly two years; [yfinance](https://ranaroussi.github.io/yfinance/) roughly 730 days (the implementation leaves one day of margin; 30 requests/minute is conservative pacing, not an official quota). For [Alpaca](https://docs.alpaca.markets/docs/about-market-data-api) and [Twelve Data](https://twelvedata.com/pricing), check their current public tiers; the implementation sets a 2016-01-01 hourly history floor, while actual availability depends on the provider. Default pacing is not an account's remaining quota.

As a rough order of magnitude, an Alpaca free-tier backfill paced at about 150 requests/minute took around 100 requests and 40–45 seconds of wall time per symbol with fixed-interval pacing for two years of regular-session 1h bars. Expect about seven bars per trading day, or 3,500 rows per symbol over two years. These are not guarantees: repeat the plan and run method for your own N and W, reporting daily and hourly separately.

## Measure storage

The default Parquet backend partitions raw by provider, market, interval and year (also month for non-daily data); canonical is partitioned by market, interval and year (also month for non-daily data), without a provider partition. At this version 1h is stored only in raw. Hourly canonical and hourly/daily consistency checks are pending; dashboard 1h support is pending. The `status` coverage summary is not disk usage.

With writes stopped, set `OPENBARS_STORE` to your own external `storage.path` and run the script below from the repository. It sums physical Parquet file bytes and metadata row counts by layer, provider, market and interval without printing symbol details. It needs the project dependencies installed. Compare `1d` and `1h` separately:

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

Canonical's `all` combines sources: its file bytes cannot be attributed to an individual provider this way. Physical rows can include superseded versions in older parts; DuckDB catalog views `raw_bars` and `canonical_bars` deduplicate by key and can provide visible row counts separately. The script excludes state, indexes, archives and other files; use `du` on the whole store separately. Compaction and small-file counts affect results.

A small pilot found roughly a few hundred Parquet bytes per row, dominated by small-file overhead; larger partitions generally compress better. Extrapolate separately by layer, provider (raw only) and interval: measured bytes per row × expected rows. For example, estimate hourly rows as symbols × trading days/year × 10 years × roughly seven bars/day, and daily rows separately at roughly one bar/trading day; adjust for coverage and duplicate versions. This is a capacity estimate, not a published whole-market run. For a Postgres comparison, measure table and index disk sizes and rows with the same data, indexes and retention, then compare orders of magnitude with the corresponding Parquet layer and interval. Record compression and maintenance state; do not publish connection details.

An hour first written under its symbol keeps that identity; rebuild hourly canonical to re-key it.
