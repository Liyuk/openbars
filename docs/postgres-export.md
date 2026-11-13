# Optional Postgres export: the generic reference layout

`openbars export-postgres` is an **optional, reference implementation**. It copies a window
of canonical daily bars from local Parquet into a PostgreSQL table whose shape is the generic
`bars + source information` layout documented here. It is not part of the default write path:
nothing in `plan` / `run` uses it, the extra is optional, and the command is a dry run unless
`--apply` is given.

The exporter is deliberately caller-agnostic. It knows no caller table names, writes no
caller foreign keys (no integer upstream instrument ids, no caller artifact / run ids) and
never runs DDL. If you need *your own* official tables with your own provenance columns,
write an adapter on your side that reads openbars data (Parquet, `BarReader`, or a table this
exporter filled) and maps it into your schema. Do not point this exporter at those tables.

中文摘要见文末。

## Usage

```bash
pip install 'openbars[postgres-export]'

# dry run (default): counts rows, never connects
openbars export-postgres --source ./data/canonical --dsn "$TARGET_PG_DSN" --days 30

# write the same window
openbars export-postgres --source ./data/canonical --dsn "$TARGET_PG_DSN" --days 30 --apply
```

- `--source` is a canonical Parquet directory (`storage.path` + `canonical/`, or wherever a
  canonical export wrote its files).
- `--table` is an existing table name; the default is `openbars_bars`. Names are limited to
  63 ASCII characters, start with a letter or underscore, and may not be schema-qualified
  (`search_path` of the connection decides the schema). The name is case-sensitive.
- The window is chosen by the caller, exactly like the rest of the plugin: `--since
  YYYY-MM-DD` (inclusive lower bound on `session_date`), or `--days N` (the N calendar days
  ending on the latest stored daily session, inclusive). They are mutually exclusive; without
  either, every stored daily bar is exported.
- This version exports daily bars only (`interval = '1d'`); intraday rows in the same files
  are skipped.
- The DSN comes from the caller at runtime (environment / env file). It is never written to
  the repository and is redacted from error messages.
- The default is a dry run. It reads Parquet, filters the window, reports the row count, and
  does not open a connection.

## The reference table

The table must exist before `--apply`; the exporter never creates or alters it. Create it
with the DDL below (also available as `openbars.export_postgres.reference_ddl()`):

```sql
CREATE TABLE "openbars_bars" (
  "instrument_id" TEXT NOT NULL,
  "market" TEXT NOT NULL,
  "interval" TEXT NOT NULL,
  "session_date" DATE NOT NULL,
  "ts_start" TIMESTAMPTZ NOT NULL,
  "session" TEXT NOT NULL,
  "policy_version" TEXT NOT NULL,
  "open" NUMERIC(20,8) NOT NULL,
  "high" NUMERIC(20,8) NOT NULL,
  "low" NUMERIC(20,8) NOT NULL,
  "close" NUMERIC(20,8) NOT NULL,
  "volume" NUMERIC(28,8),
  "vwap" NUMERIC(20,8),
  "trade_count" BIGINT,
  "provider" TEXT NOT NULL,
  "feed" TEXT NOT NULL,
  "adjustment" TEXT NOT NULL,
  "revision" INTEGER NOT NULL,
  "price_source" TEXT NOT NULL,
  "volume_source" TEXT,
  "sources_seen" JSONB NOT NULL,
  "selection_reason" TEXT NOT NULL,
  "quality_status" TEXT NOT NULL,
  "flags" JSONB NOT NULL,
  "capabilities" JSONB NOT NULL,
  "reconcile_rule" TEXT NOT NULL,
  "source_fingerprint" TEXT NOT NULL,
  "generated_at" TIMESTAMPTZ NOT NULL,
  "source_fetch_id" TEXT,
  "freshness" TEXT NOT NULL,
  "available_at" TIMESTAMPTZ,
  PRIMARY KEY ("instrument_id", "interval", "policy_version", "session_date", "ts_start")
)
```

Add your own indexes (for example on `("session_date")` or `("instrument_id",
"session_date")`) if your read patterns need them; the exporter only upserts.

### Columns

Bar columns are the canonical bar itself, one row per `(instrument_id, interval,
policy_version, session_date, ts_start)` — the same key openbars uses for canonical bars, so
re-running the export is idempotent:

| Column | Meaning / null semantics |
| --- | --- |
| `instrument_id` | openbars instrument id as text (e.g. `us:eq:TEST1`). Not an upstream integer id, no caller mapping is applied. |
| `market`, `interval`, `session`, `policy_version` | canonical identity of the series. |
| `session_date`, `ts_start` | session date (daily) and bar start timestamp (UTC), both always present. |
| `open` / `high` / `low` / `close` / `volume` / `vwap` | `NUMERIC(20,8)` prices, `NUMERIC(28,8)` volume; the driver receives exact `Decimal` values, never binary floats. `volume`, `vwap` and `trade_count` are null when the source did not provide them. |
| `provider`, `feed`, `adjustment`, `revision`, `price_source`, `volume_source`, `sources_seen` | which source supplied the row (see below). |
| `selection_reason`, `quality_status`, `flags`, `capabilities`, `reconcile_rule` | how the canonical row was selected and what openbars recorded about it. `flags` / `capabilities` / `sources_seen` are JSONB arrays. |
| `source_fingerprint`, `generated_at` | canonical generation identity: content fingerprint of the selected source bar and when this canonical row was produced. |

### Source information

The generic contract is "data source + fetch batch + provisional/final + availability time"
(the wording of the generic source-information contract; this layout quotes it, it does not
extend it). The exporter writes what the canonical row carries and leaves the rest null:

| Contract part | Columns | Notes |
| --- | --- | --- |
| Data source | `provider`, `feed`, `adjustment`, `revision`, `price_source`, `volume_source`, `sources_seen` | Always present. `capabilities` carries the series semantics (`price_basis:*`, `adjustment_scope:*`, `session_semantics:*`, `volume_basis:*`, `currency:*`). |
| Fetch batch (lineage) | `source_fetch_id` | The selected source bar's raw `fetch_id`: the stable canonical → raw fetch-batch pointer of the generic contract (`(provider, fetch_id)` keys the local fetch log). Null when the producer did not know the batch (older files, imports without lineage); the exporter never invents it. |
| Provisional vs final | `freshness` | `provisional` or `final`. A null / missing value reads as `final` everywhere in openbars, and is exported as `final`. |
| Availability time | `available_at` | Earliest moment the bar may be used for research. Null when unknown; consumers doing point-in-time work must treat null as "not available yet", like the rest of openbars. |
| Canonical generation | `source_fingerprint`, `generated_at` | Stable identity of the canonical row; always present. |

`selected_source_bar_id` is **not** exported. It is openbars' own internal, string-typed
identifier of a source row, not a caller foreign key. Trace a bar back to its fetch batch with
`source_fetch_id`, never with `revision` (the canonical revision increments on rewrites).

## Behaviour

- **Idempotent upsert.** Rows are written with `INSERT ... ON CONFLICT (...) DO UPDATE`, keyed
  by the canonical key above. Replaying the same window changes no row count and no primary
  key; it only refreshes the non-key columns from the current canonical row.
- **No DDL.** The exporter never creates, alters or drops the table (or anything else).
- **No identity writes.** It never writes instrument / symbol / alias tables; identity stays a
  caller concern.
- **Window only.** Rows outside `--since` / `--days` are not read into the batch, so they are
  never inserted or updated.
- **Optional extra.** Without `openbars[postgres-export]` the rest of the CLI works normally;
  only `--apply` needs `psycopg`.

## What is not supported

Earlier revisions of this plugin could target a caller-specific "daily bar" / "research daily
bar" table layout and remap instrument ids through a CSV. Those layouts wrote caller
provenance foreign keys as `NULL` and are **no longer supported**: `--table-layout` and
`--mapping` were removed, and the only target is the generic layout above. Callers that need
their own official tables build an adapter instead (see the first section).

## 中文摘要

`openbars export-postgres` 是**参考实现**，不是默认写入路径：可选 extra、默认 dry-run、
`--apply` 才连接写入；窗口由 `--since` / `--days` 指定；幂等 upsert；不执行 DDL；不写任何身份表；
不填调用方外键。目标表是本文档的通用结构（K 线 + 来源信息：数据源、抓取批次、临时/正式、可得时间），
默认表名 `openbars_bars`，主键与 canonical 去重键相同，所以重跑行数与主键不变。
`instrument_id` 用 openbars 自己的字符串 id，不做调用方整数 id 映射；`selected_source_bar_id` 是
openbars 内部来源行标识，不导出。来源信息里数据源列总有值；抓取批次用 canonical 的 `source_fetch_id`
（选中源 bar 的 raw `fetch_id`，通用契约里的稳定指针），旧文件缺列时留空，导出器不臆造；
`freshness` 只写 `provisional` / `final`，缺列按 openbars 全仓口径写 `final`；`available_at` 未知时留空，
做时点过滤的调用方要按「空即不可用」处理。
需要自己的官方表 + 溯源列的调用方，请在自己那边做适配器读 openbars 数据，不要指望本插件直写官方表。
旧的调用方专用表布局（`--table-layout` / `--mapping`）已删除、不再支持。
