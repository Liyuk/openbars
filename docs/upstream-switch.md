# Migrating an upstream research series to openbars

`upstream_compat` is retained for migration work and tests only. It is not the
default or a routine production write path. New installations write to local
Parquet + DuckDB and use a local SQLite quota gate. Explicitly configuring
`storage.backend: postgres` with `mode: upstream_compat` continues to select
compat behavior; configuration is never silently redirected.

Compatibility notes for a research-series store that already uses the
`upstream_compat` tables (`instrument`, `symbol_alias`, `daily_bar`,
`research_daily_bar`, and the two watermark tables). This document names no
private implementation. For a particular deployment, keep the concrete file
and config checklist outside this repository.

中文摘要见文末。

## Prerequisites

- For an isolated migration / test database only, introspection must pass:
  openbars starts in `upstream_compat` and **never
  runs migrations**. Missing tables/columns or `numeric` precision mismatches
  raise `UpstreamSchemaError` with a `problems` list. Fix the schema on the
  upstream side (or point at a compatible database) before enabling writes.
- In that isolated environment, explicitly set `storage.postgres.mode:
  upstream_compat` and `storage.postgres.write: true` to test writes. Writes
  stay off when `write` is false or omitted. Do not point this at a live
  production database.
- Provide a Postgres DSN at runtime only (environment / env file). Do not
  commit it. `openbars config` redacts it.
- `state_path` (SQLite) is required: jobs, checkpoints and the fetch log are
  not stored in the upstream database.
- The default quota gate is local SQLite. In isolated migration / test setups,
  `storage.postgres.quota: postgres` can use existing
  `provider_request_gate` / `provider_request_usage` tables; this never
  migrates those tables.
- `upstream_compat` writes US daily bars only.

## CLI

`openbars plan` and `openbars run` honor `storage.backend: postgres` in
migration / test setups. The DSN
is resolved at runtime from settings (`storage.postgres.dsn`, typically
`${OPENBARS_PG_DSN}` or an env file) and is never printed by `openbars config`
or command output. A dry-run `plan` (no `--submit`) opens the backend
read-only even if `write: true`, so it will introspect the upstream schema
but will not write jobs or bars. `plan --submit` and `run` use the
configured `write` flag and can upsert `daily_bar`, `research_daily_bar`,
and the corporate-action tables in `upstream_compat`; use a disposable
database for write validation. Jobs, checkpoints and
the fetch log stay in `{storage.path}/state.db` for compat mode (they are
not stored in the upstream database). Set `storage.postgres.quota: postgres`
to share `provider_request_gate` / `provider_request_usage` with the upstream
writer; the default remains a local SQLite gate.

If symbol bootstrap hits a reused ticker, `run` marks the job dead and
prints an actionable report listing each candidate's numeric
`instrument_id`, alias validity window, and exchange / security type.
Re-run with one of those numeric ids instead of the ticker.

## Identity

openbars instrument ids are strings; upstream FKs are integers.

- A numeric string (`"42"`) is reused as that bigint (the `instrument` row is
  inserted if missing, types are not overwritten).
- Any other string is treated as a **symbol bootstrap**: look up
  `symbol_alias` covering that session date. One hit is reused; no hit
  inserts `instrument` + a covering alias (`valid_from` is not later than the
  first bar). More than one hit (ticker reuse) raises `AmbiguousInstrumentError`
  with an actionable report: each candidate's numeric `instrument_id`, alias
  validity window, and exchange / security type. Pass one of those numeric ids.
- In compatibility tests, prefer numeric ids (or a pre-seeded identity table).
  Ticker-only bootstrap is fine for a short dry-run, but it can mis-merge a
  reused ticker.

If a covering alias is missing and no symbol is available, the bar can still
be written, but readers that join `symbol_alias` will not see it.

## Migration and compatibility verification (non-production)

1. **Read-only comparison.** Run the openbars pipeline against Parquet (or a
   separate schema / `policy_version`) and compare it to the existing
   `research-daily-v2` series (`openbars shadow` / `openbars verify`). Watch
   coverage, price agreement, volume agreement, and quality-status agreement.
2. **Compatibility test write.** Point `upstream_compat` with `write: true` at a
   *throwaway* database loaded with the upstream DDL (or a snapshot), never
   the live one, and confirm an external reader can join
   `research_daily_bar` to `symbol_alias` with the covering-alias predicate
   (see `RESEARCH_SERIES_READ_SQL` in `openbars.storage.compat_write`).
3. Do not configure `upstream_compat` as a routine production writer or
   dual-write it with an upstream projector. Core production writes use
   Parquet + DuckDB and the local SQLite quota gate. A separate export feature
   may be used for external Postgres writes.

For compatibility tests, canonical writes take a transaction-level advisory
lock with this key (do not change the prefix):

       sha256("research-daily:{policy_version}:{YYYY-MM-DD}")[:8]  # signed big-endian int64
       SELECT pg_advisory_xact_lock(key)

Compatibility writes update the watermarks for cache-based readers. This
behavior is retained for migration and test environments only.

`selected_source_bar_id` is always set when a canonical upsert succeeds: a
matching `daily_bar` is reused when present; otherwise a row is materialized
from the canonical OHLCV under the same unique key (existing raw values are
not overwritten). It is missing only if identity resolution fails, in which
case the whole upsert errors. Volume is required (null volume is refused,
not stored as zero). Corporate actions are written to, and can be read back from, the upstream
observation / event / revision tables (`event_key` =
`openbars:{action_type}:{instrument_id}:{ex_date}`).

## 中文摘要

`upstream_compat` 在 `write: true` 时写入既有表，永不做迁移；启动时做列级校验。
字符串 instrument id 若是整数就复用，否则按代码引导；代码复用冲突会给出带数值 id 和有效期的歧义报告，必须改传其中一个 id。只写 canonical 时会补写对应 `daily_bar` 以填 `selected_source_bar_id`；公司行动写入现有观察/事件/修订表，并可通过 `corporate_actions()` 读回最新修订；无法识别的行动类型会记入本地 unresolved 并告警，不会静默跳过。`mappings(provider=…)` 支持按 provider 筛选（别名本身仍是 `*`）。compat master 已是完整 `InstrumentStore`，unresolved / observe 等放在本地 `identity.db`。`storage.postgres.quota: postgres` 时 CLI 与 upstream 共用 `provider_request_gate` / `usage`（默认仍是本地 sqlite）。
`openbars plan` / `run` 已支持 `storage.backend: postgres`：DSN 只在运行时解析且不会打印；dry-run `plan` 只读，`plan --submit` 与 `run` 按 `write` 写入。compat 模式下 jobs 仍在本地 SQLite。
迁移比对只读运行；写入验证只在一次性测试库进行，不能作为正式库日常写入或双写流程。
compat 保留 advisory lock 与水位更新，仅供迁移 / 测试使用。生产核心写入默认走
Parquet + DuckDB，配额走本地 SQLite；密钥和 DSN 只在运行时提供，不要入库。

## 可选：把近期窗口导出到 Postgres（参考实现）

核心默认写入 Parquet + DuckDB，配额使用本地 SQLite。可选的 `postgres-export` 插件能把已生成的
canonical 日线按**通用参考表**（K 线 + 来源信息）写入调用方**已存在**的 PostgreSQL 表；它是参考实现，
不是默认写入路径，也不了解任何调用方的官方表。

```bash
pip install 'openbars[postgres-export]'
openbars export-postgres --source ./data/canonical --dsn "$TARGET_PG_DSN" --days 500   # dry-run
openbars export-postgres --source ./data/canonical --dsn "$TARGET_PG_DSN" --days 500 --apply
```

默认表名 `openbars_bars`；窗口由 `--since` / `--days` 指定；默认 dry-run，`--apply` 才写入；幂等
upsert；不执行 DDL；不写身份表；不填调用方外键。需要官方表 + 溯源列的调用方应自己做适配器读 openbars
数据，不要让本插件直写官方表。表结构、列含义、空值语义与来源信息口径见
[可选 Postgres 导出与通用参考表](postgres-export.md)。
