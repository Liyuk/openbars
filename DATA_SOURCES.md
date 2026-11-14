# Data sources

**English** | [中文](DATA_SOURCES.zh-CN.md)

This page lists every data source OpenBars supports **in code today**, what each one provides in
this project, whether it needs a key, and what to expect on the day. It is checked against the
provider adapters (`src/openbars/providers/`), the CLI wiring (`src/openbars/cli/wiring.py`),
[`config.example.yaml`](config.example.yaml) and [docs/provider-keys.md](docs/provider-keys.md).
If something is not listed here, OpenBars does not support it.

> **OpenBars distributes no market data and runs no hosted fetcher.** Everything is fetched by
> your own machine with your own accounts and keys, stored in your own data directory, and
> subject to each provider's terms. The [live demo](https://liyuk.github.io/openbars/) shows
> synthetic `TEST*` data only; it never contacts any provider.

## At a glance

| Source | Config key | Markets and intervals | Key / access | In the CLI (`plan` / `run`) |
| --- | --- | --- | --- | --- |
| [Alpaca](#alpaca) | `alpaca` | US equities and ETFs: `1d`, `1h` | API key ID and secret | Yes |
| [Massive](#massive) | `massive` | US equities and ETFs: `1d`, `1h` | API key | Yes |
| [yfinance (Yahoo Finance)](#yfinance-yahoo-finance) | `yfinance` | US: `1d`, `1h`; Hong Kong: `1d` | None | Yes |
| [Twelve Data](#twelve-data) | `twelve_data` | US: `1h` only | API key | Yes |
| [Binance](#binance) | `binance` | Crypto spot: `1d` | None (public data) | Yes |
| [Coinbase Exchange](#coinbase-exchange) | `coinbase` | Crypto spot: `1d` | None (public data) | Yes |
| [BaoStock](#baostock) | `baostock` | China A-shares: `1d` | None | Yes |
| [AKShare, A-shares](#akshare-a-shares) | `akshare_ashare` | China A-shares: `1d` (Eastmoney or Sina feed) | None | Yes |
| [Tushare](#tushare) | `tushare` | China A-shares: `1d` | Token | Yes |
| [AKShare, Hong Kong](#akshare-hong-kong) | `akshare` | Hong Kong: `1d` | None | **No**: adapter only |
| [Futu OpenD](#futu-opend) | `futu` | Hong Kong: `1d` | Local OpenD gateway | **No**: adapter only |
| [LongPort](#longport) | `longport` | Hong Kong: `1d` | LongPort account credentials | **No**: adapter only |

- "In the CLI" means the provider is built by `cli/wiring.py` when it is `enabled` in
  `config.yaml`. Adapters marked **No** exist and are registered as plugins, but the planner
  never routes jobs to them and `openbars run` does not construct them; they can be called from
  Python or wired in explicitly.
- Intervals are `1d` (the default path) and `1h` (US only, optional, needs `--accept-budget`).
  Other intervals (`30m`, `15m`, `5m`, `1m`) are not implemented by any provider.
- **Not implemented:** Alpha Vantage and Tiingo. They appear in `.env.example` or in the
  disabled `alpha_vantage` entry of `config.example.yaml` as placeholders only and **cannot be
  used**.

Optional extras install the SDKs: `openbars[yfinance]`, `openbars[baostock]`,
`openbars[akshare]`, `openbars[tushare]` (for example `uv sync --extra baostock`). Futu and
LongPort SDKs are not OpenBars dependencies; install them yourself if you call those adapters.

## Same-day data: provisional, then final

Daily collection has one path: fetch the newest bars each source can currently provide. There is
no mode switch. Each canonical row carries a separate `freshness` label and an optional
`available_at` (see the [README](README.md#same-day-data-timeliness-is-not-completeness)):

- `freshness=provisional`: a same-day (or just-closed) bar that a later run may overwrite;
- `freshness=final`: final data. When a later `openbars run` fetches final data for the same
  session, it replaces the provisional row; reads prefer the final row;
- missing or null `freshness` in older rows reads as `final`.

| Daily market | `provisional` tier | `final` tier (code threshold) |
| --- | --- | --- |
| US | during the session (T+0) | from 00:00 ET the next day |
| Crypto | during the UTC session day (T+0) | from 00:30 UTC the next day |
| China A-shares | during the session (T+0) | 30 minutes after the close |
| Hong Kong | during the session (T+0) | 30 minutes after the actual close |

These are **tier rules in OpenBars, not provider delivery SLAs and not a completeness
guarantee.** Whether a given provider actually has the day's bar at a given time is a separate
question; per-source evidence is tracked in [docs/source-readiness.md](docs/source-readiness.md).
Only two behaviours are verified in code: Massive's grouped daily bars are ready from 00:00 ET on
the next calendar day (earlier requests raise a retryable `data_not_ready`), and Alpaca SIP
requests for today end at `now − 16 minutes`. Every other source is best effort with no T+0
promise. `schedule.daily_at` in the config is a display field; an external scheduler decides when
`openbars run` starts (see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#scheduling)).

## Sources in detail

### Alpaca

- **Provides:** US equities and ETFs, daily and hourly, from the market data API. Feed `sip`
  (default in the example config) or `iex`; volumes from different feeds are never mixed.
  History from 2016-01-01 (the free-plan SIP floor in the adapter).
- **Access:** account and key required: `ALPACA_API_KEY_ID` and `ALPACA_API_SECRET_KEY`
  (`ALPACA_API_KEY` / `ALPACA_API_SECRET` are accepted as aliases). Default pacing 200
  requests/minute (the example config tightens it to 190).
- **Same day:** SIP requests that include today stop at `now − 16 minutes` (code-verified), so a
  provisional bar can appear during or after the session. After-close availability and
  completeness are **unverified**; the final tier starts at 00:00 ET the next day.
- **Terms:** market data is licensed to the account holder. See
  [Alpaca's disclosures](https://alpaca.markets/disclosures) and the
  [market data docs](https://docs.alpaca.markets/docs/about-market-data-api).

### Massive

- **Provides:** US equities and ETFs. Daily bars come from the whole-market grouped daily
  endpoint (one request per day covers every symbol); hourly bars sum per-ticker 30-minute
  aggregates onto the hourly grid.
- **Access:** key required: `MASSIVE_API_KEY`. The free Basic plan gives about a two-year
  (730-day) rolling window and 5 requests/minute (default pacing 4); check the
  [pricing page](https://massive.com/pricing) for current limits.
- **Same day:** grouped daily bars are ready from 00:00 ET on the next calendar day
  (code-verified); earlier requests fail as retryable and are tried again later, so Massive daily
  bars are only fetched once they are in the final tier. Hourly after-close availability is unverified.
- **Terms:** licensed for personal use per the pricing/terms page.

### yfinance (Yahoo Finance)

- **Provides:** US equities and ETFs (`1d`, and `1h` for roughly the last 730 days) and Hong Kong
  daily bars. For the US it fills pre-2016 history and gaps left by the primary sources; for Hong
  Kong it is the **primary source** and the only one the planner uses.
- **Access:** no key. Install the optional extra `openbars[yfinance]`. There is no published
  quota; the default 30 requests/minute is a conservative setting, not an official limit.
- **Same day:** the adapter enforces no availability delay: best effort, no T+0 promise.
- **Notes:** unofficial Yahoo data, personal use only, not a licensed exchange feed. Hong Kong
  bars are stored unadjusted in HKD (`hk-daily-v1`); split adjustment is derived at read time.
- **Terms:** [Yahoo terms](https://policies.yahoo.com/us/en/yahoo/terms/index.htm); see the
  [yfinance documentation](https://ranaroussi.github.io/yfinance/).

### Twelve Data

- **Provides:** US hourly bars only (`1h`, regular session; no extended hours, no daily bars).
  Off by default.
- **Access:** key required: `TWELVE_DATA_API_KEY`. Free tier: 8 requests/minute, 800 per UTC day,
  up to 5,000 bars per request; history depends on plan and instrument.
- **Same day:** no availability delay is enforced: best effort.
- **Terms:** [Twelve Data terms](https://twelvedata.com/terms) and [pricing](https://twelvedata.com/pricing).

### Binance

- **Provides:** crypto spot daily klines. The daily bar is cut at 00:00 UTC and weekends are
  sessions (`crypto-daily-v1`). Off by default.
- **Access:** no key; public market data from `https://data-api.binance.vision` (configurable
  with `providers.binance.base_url`). Request weight is limited per IP; history depends on the
  pair's listing date.
- **Same day:** the current UTC day is `provisional` until 00:30 UTC the next day (tier rule);
  provider timing is unverified.
- **Notes:** each exchange is its own instrument: Binance bars are never merged with Coinbase.
- **Terms:** [Binance terms](https://www.binance.com/en/terms). Redistribution is **not**
  assumed allowed.

### Coinbase Exchange

- **Provides:** crypto spot daily candles for `BASE-QUOTE` products (Coinbase Exchange public
  REST). At most 300 candles per request, 10 requests/second per IP. Off by default.
- **Access:** no key; public market data.
- **Same day / notes:** as Binance (UTC day, `provisional` until 00:30 UTC the next day); never
  merged with Binance.
- **Terms:** [Coinbase user agreement](https://www.coinbase.com/legal/user_agreement).
  Redistribution is **not** assumed allowed.

### BaoStock

- **Provides:** China A-share daily bars, **unadjusted** (`cn-daily-v1`). Adjustment factors and
  suspension fields are fetched too, so forward/backward adjustment (QFQ / HFQ) is derived at read
  time and never stored. Off by default.
- **Access:** no key (anonymous login). Install `openbars[baostock]`. Symbols are written
  `sh.600000` or `600000.SH`; bare 6-digit codes are skipped. It may be unreachable from some
  overseas networks.
- **Same day:** final tier 30 minutes after the close (tier rule); delivery time and completeness
  are unverified.
- **Terms:** personal local use only; the website forbids copying, distribution or commercial use
  without written permission. See [BaoStock](https://www.baostock.com/).

### AKShare, A-shares

- **Provides:** China A-share daily bars, unadjusted, as a **low-frequency backup**. Feed
  `eastmoney` (default) or `sina`. Config key `akshare_ashare`. Off by default.
- **Access:** no key. Install `openbars[akshare]`. The A-share canonical layer is a single-source
  projection, so enable **one** A-share source at a time (BaoStock, AKShare or Tushare).
- **Same day:** as BaoStock (tier rule, delivery unverified).
- **Terms:** the AKShare library is MIT but **does not relicense upstream quotes**. Eastmoney's
  terms forbid redistributing exchange quotes without written consent and forbid
  disproportionate load; Sina restricts unlicensed scraping. See
  [Eastmoney's disclaimer](https://about.eastmoney.com/home/disclaimer) and the
  [AKShare project](https://akshare.akfamily.xyz/).

### Tushare

- **Provides:** China A-share daily bars, unadjusted, free tier only (about 120 points: roughly
  50 requests/minute and 8,000/day). The adapter never calls `qfq`/`hfq` or `adj_factor`. Off by
  default.
- **Access:** token required: `TUSHARE_TOKEN` (referenced as `${TUSHARE_TOKEN}`; never commit it).
  Install `openbars[tushare]`.
- **Same day:** as BaoStock (tier rule, delivery unverified).
- **Terms:** the licence is personal, non-transferable, non-commercial and revocable; public use
  must carry Tushare's required attribution. See the
  [Tushare terms](https://tushare.pro/document/1?doc_id=405).

### AKShare, Hong Kong

- **Status:** the adapter (`providers/hk/akshare.py`, plugin name `akshare`) exists and the
  Hong Kong reconciliation (`hk-daily-v1`) treats `akshare/eastmoney/raw` as a same-market
  **corroboration** source. It is **not wired into the CLI**: the planner never schedules it and
  `openbars run` does not construct it.
- **Provides:** Hong Kong daily bars (Eastmoney `stock_hk_hist`), unadjusted, HKD.
- **Access:** no key; `openbars[akshare]`.
- **Terms:** the same AKShare / Eastmoney caveats as above; local private storage only.

### Futu OpenD

- **Status:** adapter only, **not wired into the CLI** (the planner does not route Hong Kong jobs
  here; yfinance is the Hong Kong primary).
- **Provides:** Hong Kong daily K-lines, unadjusted only (`AuType.NONE`).
- **Access:** a local OpenD gateway that you run and log in to yourself (default
  `127.0.0.1:11111`; OpenBars never handles the Futu login). A login does not imply Hong Kong
  quote rights; the quote level, history-K quota and mainland/international status depend on your
  account and Futu's terms. The SDK `futu-api` is not an OpenBars dependency.
- **Terms:** the Futu user agreement, the OpenAPI agreement and exchange market-data rights; no
  unauthorised forwarding, resale or commercial redistribution. See
  [Futu OpenAPI](https://openapi.futunn.com/).

### LongPort

- **Status:** adapter only, **not wired into the CLI** (same as Futu).
- **Provides:** Hong Kong daily candlesticks, unadjusted only (`AdjustType.NoAdjust`).
- **Access:** your own LongPort account and developer verification; `app_key`, `app_secret` and
  `access_token` are read from the environment through `${ENV}` references (for example
  `${LONGPORT_APP_KEY}`), never written in plain text. Free core-API / HK LV1 versus paid LV2
  follows LongPort's current terms. The `longport` SDK is not an OpenBars dependency.
- **Terms:** typically personal, revocable, non-commercial and non-transferable; market data may
  not be copied or redistributed without written consent. See
  [LongPort OpenAPI](https://open.longportapp.com/).

## Terms and licensing

- **OpenBars does not distribute or host market data.** The repository contains code, rules and
  documentation only; test and demo data are synthetic.
- Most free sources limit use to personal or internal purposes and forbid display or
  redistribution. **You are responsible for reading and following each provider's terms.**
  Library licences (MIT, BSD, Apache) do not relicense the quotes behind them.
- OpenBars stores data only in your own data directory. Do not commit it, publish it, or serve it
  to others unless a provider's terms explicitly allow that.
- OpenBars is not affiliated with any data provider and gives no warranty about accuracy,
  completeness or availability. Free tiers, history windows and limits change; check the official
  pages linked above.

## See also

- [docs/provider-keys.md](docs/provider-keys.md): sign-up links, free-tier limits and key handling.
- [docs/source-readiness.md](docs/source-readiness.md): per-source readiness and evidence status.
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#adding-a-data-source): how to add a new source.
- [README](README.md): quick start, same-day data and per-market notes.
