# Roadmap

Status: pre-alpha. Daily bars are the default interval; hourly and minute bars are optional capabilities, and enabling them triggers the corresponding quota rules and backfill-budget confirmation.

| Version | Scope | Key deliverables |
| --- | --- | --- |
| **v0.1** US (including ETF) daily | Alpaca SIP, Massive grouped daily, yfinance (pre-2016 fallback) | Core models and interfaces, config/CLI, QuotaGate, Orchestrator + Checkpoint, Parquet/DuckDB, InstrumentMaster (stable IDs), US calendar, corporate actions + query-time adjustment, Reconciler + quality flags, `verify` reconciliation |
| **v0.2** Crypto daily | Binance data-api / data.binance.vision; OKX, Coinbase backup | UTC day boundary, 24/7 calendar, weight-based quotas |
| **v0.3** China A-share daily | Shanghai/Shenzhen calendar, `cn:eq:` security ID rules, then data sources, adjustment semantics, price limits, etc. (Tushare 120 points, baoStock, AKShare) | Lunch-break calendar (XSHG, XSHE alias), ID generation rules, remote collector nodes, qfq/hfq query-time derivation; design in [discussion #54](https://github.com/Liyuk/openbars/discussions/54) |
| **v0.4** Hong Kong daily | XHKG calendar and `hk:eq:xhkg:` five-digit codes, then free data sources, adjustment semantics, US ADR links | Half-day sessions and the library's holiday coverage limit; design in [discussion #68](https://github.com/Liyuk/openbars/discussions/68); minute bars as an option |
| **v0.5** Local read-only UI and self-hosted API | `openbars serve`, coverage dashboard, K-line page (canonical by default, switchable to each provider's raw bars), quality flags and quota snapshot; `/api/v1` documented as a public self-hosted read API | Loopback access by default; a non-loopback bind needs the explicit `--allow-non-loopback` opt-in and prints a startup warning (no built-in authentication or TLS, callers add a firewall or authenticating reverse proxy); the endpoints, parameters and stability rules are in [api.md](api.md); design in [discussion #82](https://github.com/Liyuk/openbars/discussions/82) |
| **v0.6** US hourly (optional) + measurement guide | Massive / Alpaca 30m aggregated to 1h, yfinance / Twelve Data 1h; regular hours by default, alignment optional | Raw collection, budget confirmation and resumable fetching; [local measurement guide](measuring-hourly.md); hourly canonical layer and hourly/daily consistency checks pending, dashboard 1h pending; public benchmark report (carried over from v0.3) |
| Later | Postgres backend improvements, more provider plugins, built-in authentication for the read API as a possible future addition (not planned for v0.5) | — |

Non-goals: data distribution/hosting, real-time quotes, tick data, trading and backtesting.
