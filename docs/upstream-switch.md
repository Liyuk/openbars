# Switching an upstream research series to openbars

Generic cut-over notes for a research-series store that already uses the
`upstream_compat` tables (`instrument`, `symbol_alias`, `daily_bar`,
`research_daily_bar`, and the two watermark tables). This document names no
private implementation. For a particular deployment, keep the concrete file
and config checklist outside this repository.

中文摘要见文末。

## Prerequisites

- Introspection must pass: openbars starts in `upstream_compat` and **never
  runs migrations**. Missing tables/columns or `numeric` precision mismatches
  raise `UpstreamSchemaError` with a `problems` list. Fix the schema on the
  upstream side (or point at a compatible database) before enabling writes.
- Set `storage.postgres.mode: upstream_compat` and `storage.postgres.write: true`.
  Writes stay off when `write` is false or omitted.
- Provide a Postgres DSN at runtime only (environment / env file). Do not
  commit it. `openbars config` redacts it.
- `state_path` (SQLite) is required: jobs, checkpoints and the fetch log are
  not stored in the upstream database.
- Share QuotaGate with the upstream writer (`provider_request_gate` /
  `provider_request_usage`) if both processes use the same provider key;
  otherwise give openbars its own key.
- `upstream_compat` writes US daily bars only.

## Identity

openbars instrument ids are strings; upstream FKs are integers.

- A numeric string (`"42"`) is reused as that bigint (the `instrument` row is
  inserted if missing, types are not overwritten).
- Any other string is treated as a **symbol bootstrap**: look up
  `symbol_alias` covering that session date. One hit is reused; no hit
  inserts `instrument` + a covering alias (`valid_from` is not later than the
  first bar). More than one hit (ticker reuse) is an error — pass the numeric
  id instead.
- Prefer numeric upstream ids (or a pre-seeded identity table) in production.
  Ticker-only bootstrap is fine for a short dry-run, but it can mis-merge a
  reused ticker.

If a covering alias is missing and no symbol is available, the bar can still
be written, but readers that join `symbol_alias` will not see it.

## Shadow, dual-write, cut-over, rollback

1. **Shadow (read-only).** Run the openbars pipeline against Parquet (or a
   separate schema / `policy_version`) and compare it to the existing
   `research-daily-v2` series (`openbars shadow` / `openbars verify`). Watch
   coverage, price agreement, volume agreement, and quality-status agreement.
2. **Dry-run write.** Point `upstream_compat` with `write: true` at a
   *throwaway* database loaded with the upstream DDL (or a snapshot), never
   the live one, and confirm an external reader can join
   `research_daily_bar` to `symbol_alias` with the covering-alias predicate
   (see `RESEARCH_SERIES_READ_SQL` in `openbars.storage.compat_write`).
3. **Dual-write / lock-step.** When cutting onto the live tables, pause or
   serialize the upstream projector for the same `(policy_version, session_date)`
   first. openbars takes a transaction-level advisory lock with this exact
   key (do not change the prefix):

       sha256("research-daily:{policy_version}:{YYYY-MM-DD}")[:8]  # signed big-endian int64
       SELECT pg_advisory_xact_lock(key)

   The upstream projector must use the same algorithm. After a canonical
   write, openbars increments `research_series_watermark` and
   `research_series_date_watermark` so cache-based readers invalidate.
4. **Cut over.** Leave `write: true` on openbars, then take upstream fetch /
   project workers offline one source at a time (fallback sources first, then
   the primary). Keep their code and feature flags in place.
5. **Rollback.** Set openbars `write: false` (or stop the openbars writer) and
   resume the upstream workers. Because the table shapes and policy version
   are shared, no data migration is required. Watermarks keep incrementing
   from whichever process writes last.

`selected_source_bar_id` is filled only when a matching `daily_bar` row
already exists; a canonical-only upsert leaves it null. Volume is required
(null volume is refused, not stored as zero).

## 中文摘要

`upstream_compat` 在 `write: true` 时写入既有表，永不做迁移；启动时做列级校验。
字符串 instrument id 若是整数就复用，否则按代码引导（代码复用冲突必须传数值 id）。
切换前先影子对账，再在临时库验证外部读取 SQL；切到共用表时与 upstream 投影器共用
`research-daily:{policy}:{date}` 的 advisory lock，并依赖水位表让缓存失效。
回滚只需关掉 openbars 写入、恢复 upstream worker，不必迁数据。密钥和 DSN 只在运行时提供，不要入库。
