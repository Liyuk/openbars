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

## 可选：导出近期窗口到调用方 Postgres

核心默认写入 Parquet + DuckDB，配额使用本地 SQLite。需要主动把已生成的 canonical 日线导入调用方**已存在**的
`daily_bar` 或 `research_daily_bar` 时，单独安装 `pip install 'openbars[postgres-export]'`，
再运行：

```bash
openbars export-postgres --source ./data/canonical --dsn "$TARGET_PG_DSN" \
  --table research_daily_bar --mapping ./instrument-ids.csv --days 500
```

映射 CSV 必须包含 `instrument_id,upstream_id` 两列；`upstream_id` 是调用方表使用的整数
instrument id。例如 `us:eq:TEST1,42`。所有待导出的 instrument 都必须显式映射。可用
`--since YYYY-MM-DD` 设含首日的日历日期下界，或 `--days N` 取本地 Parquet 最新交易日及
其前 N-1 个日历日；二者不可同时指定。缺省窗口为全部已存日线。命令默认 dry-run，只报告
行数；确认后附加 `--apply` 才连接 DSN 并写入。

也可用 `--table custom_bars --table-layout daily_bar` 指定自定义表名和沿用的表结构；
`--table-layout` 可选 `daily_bar` 或 `research_daily_bar`，自定义表名时必填，两个原表名
则默认使用同名结构。表名限 63 个 ASCII 字符，以字母或下划线开头，后接字母、数字或下划线；
不支持 schema 限定名，目标 schema 由连接的 search_path 决定。表名区分大小写。

`daily_bar` 按 `(instrument_id, session_date, provider, feed, adjustment, revision)` 冲突键覆盖；
`research_daily_bar` 按 `(instrument_id, session_date, policy_version)` 覆盖。OHLC/VWAP 使用
`NUMERIC(20,8)`、volume 使用 `NUMERIC(28,8)`，驱动收到 Decimal 值。canonical 行没有 raw 专属
元数据时，`daily_bar` 的 `price_basis`、`adjustment_scope`、`session_semantics`、`volume_basis`
写为 `unknown`，raw artifact / ingest run id 留空。规范层写 `provider`、`feed`、`adjustment`、
`revision`、OHLCV、选择原因、质量状态、能力、指纹和生成时间；源表 ID 留空。`policy_version`
直接取 canonical 行（如 `research-daily-v2` 或 `cn-daily-v1`），不在导出器中改写。表和唯一约束必须已由
调用方创建；导出器不执行 DDL，不写 `instrument`、`symbol_alias` 或其它身份表。调用方负责
映射与权限、目标表结构和缓存水位维护。此插件从不进入默认依赖或 `plan` / `run` 写入路径。
