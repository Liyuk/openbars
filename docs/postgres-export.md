# Optional Postgres export: the generic reference layout

`openbars export-postgres` is an **optional, reference implementation**. It copies a window
of canonical daily bars from local Parquet into a PostgreSQL table whose shape is the generic
`bars + source information` layout documented here. It is not part of the default write path:
nothing in `plan` / `run` uses it, the extra is optional, and the command is a dry run unless
`--apply` is given.

The exporter is deliberately caller-agnostic. It knows no caller table names, writes no
caller foreign keys (no integer upstream instrument ids, no caller artifact / run ids) and
never runs DDL. If you need *your own* official tables with your own provenance columns,
write an adapter on your side that reads OpenBars data (Parquet, `BarReader`, or a table this
exporter filled) and maps it into your schema. Do not point this exporter at those tables.

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

The plugin writes only the generic `openbars_bars` layout described above. It does not target
caller-owned official tables, remap identifiers, or fill caller foreign keys. Callers that
need their own schema should write an adapter (see the first section).
