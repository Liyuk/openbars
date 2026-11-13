# Generic bar + source-information contract (for caller adapters)

This document freezes the "generic bar + source information" contract that the OpenBars core
exposes to **caller adapters**. The core produces generic bars and source information only and
knows nothing about any caller's official tables; mapping rows into a caller's own schema is the
adapter's job.

- Applies to `openbars-storage-v1` (`SCHEMA_VERSION = 1`), shared by the `research-daily-v2` /
  `crypto-daily-v1` / `cn-daily-v1` / `hk-daily-v1` canonical layers.
- Authoritative field declarations: `src/openbars/storage/schema.py` (on disk / in the
  database) and `src/openbars/core/schema.py` (in-memory daily rows).
- Only OpenBars' own fields are described here. Caller table names, foreign keys and integer
  ids are outside this contract.

## 1. Two data layers and their read paths

| Layer | Content | Read entry points |
| --- | --- | --- |
| raw | the raw bars of every source and every fetch, keeping source identity and fetch lineage | `backend.raw.scan_rows(...)`, `backend.raw.scan(...)`, raw Parquet / `raw_bars` table |
| canonical | bars selected and reconciled per `policy_version`; one row per `(instrument_id, time, policy_version)` | `backend.canonical.scan_rows(...)`, `backend.reader.bars(...)`, canonical Parquet / `canonical_bars` table |

The default backend is local Parquet + DuckDB; `storage.postgres` in `native` mode uses the same
field declarations. The `/api/v1` routes of `openbars serve` are the public, self-hosted read
API (see [api.md](api.md)); their version and stability rules live in that page. They are **not**
the storage-layer adapter contract: adapters should read the storage layer or the canonical /
raw data files directly.

Same-key deduplication is "last write wins" (the raw key includes `revision`; the canonical key
does not, and superseded canonical rows are only used for `available_by` point-in-time reads).
Retrying a fetch or re-upserting the same rows produces no new rows.

## 2. Raw layer fields

`RAW_BARS_SCHEMA` (Parquet additionally has `extra`; Postgres native also has `extra JSONB`).
`?` means nullable.

| Field | Type | Meaning / null semantics |
| --- | --- | --- |
| `provider` | string | data-source id (`massive`, `alpaca`, `baostock`, …). Required, non-empty. |
| `market` | string | market partition key, also written as a column: `us` / `cn` / `hk` / `crypto`. Required. |
| `feed` | string | the source's internal feed / venue / dataset. Required; `""` when the source has no feed concept (not null). |
| `adjustment` | string | the adjustedness as delivered (`raw`, `split`, …). Together with `provider` and `feed` it forms the source identity triple. Required. |
| `symbol_as_returned` | string | the symbol exactly as the provider returned it. Required, non-empty. |
| `instrument_id` | string? | the OpenBars stable string id once resolved; null before resolution. Native daily raw rows usually take it from `extra["instrument_id"]` before the write. |
| `interval` | string | `1d` / `1h`. Required. |
| `ts_start` | timestamp(us, UTC) | bar start. Required, tz-aware. |
| `session_date` | date32 | exchange-local trading day. Required. |
| `price_basis` | string | `raw` / `split_adjusted` / `total_return_adjusted` / `provider_defined` / `unknown`. Required. |
| `adjustment_scope` | string | adjustments already applied: `none` / `splits` / `dividends` / `splits_and_dividends` / `provider_defined` / `unknown`. Required. |
| `adjustment_as_of` | date32? | adjustment knowledge date; null when unknown. |
| `session_semantics` | string | `regular` / `extended` / `provider_aligned` / `utc_day` / `unknown`. Required. |
| `currency` | string? | price currency, 3 uppercase ISO 4217 letters; null when unknown. |
| `volume_basis` | string | `as_traded` / `split_adjusted` / `provider_defined` / `unknown`. Required. |
| `revision` | int32 | source bar version, a non-negative integer. A new revision is a **separate row** in raw and never overwrites an older revision. |
| `open` / `high` / `low` / `close` | decimal128(20,8) | exact prices, required; never a binary float. |
| `volume` | decimal128(28,8)? | volume. **null means the source gave no volume — not zero.** |
| `vwap` | decimal128(20,8)? | VWAP as delivered; null when unknown. |
| `trade_count` | int64? | number of trades; null when unknown. |
| `fetch_id` | string | **fetch batch identifier**. Required, non-empty; all bars of one batch share it. Together with `provider` it is the primary key `(provider, fetch_id)` of `state.db`'s `fetch_log`. Native fetches look like `<job_id>-<random suffix>`; compatibility import rows look like `compat:<row id>`. |
| `source_artifact_ref` | string? | response archive reference (a relative path under `data/raw-archive` or an equivalent stable reference); null when nothing was archived. |
| `ingest_run_id` | string? | import batch id for compatibility imports; null for native fetches. |
| `ingested_at` | timestamp(us, UTC) | when OpenBars stored this raw row. Required. Canonical `available_at` prefers it. |
| `extra` | JSON object? | native-storage only: provider fields that have no dedicated column but must be kept (`tradestatus` / `isST` / `preclose` …). Null when unknown. Compatibility mode does not keep it. |

## 3. Canonical layer fields

`CANONICAL_BARS_SCHEMA`. `?` means nullable. A canonical row comes from **one selected source
bar**: prices and volume always come from the same source (whole-bar selection).

| Field | Type | Meaning / null semantics |
| --- | --- | --- |
| `instrument_id` | string | OpenBars stable string id; when the source is unresolved the reconciler may group by the symbol itself, so treat it as an **opaque string** and never assume it is an integer or an official-table foreign key. |
| `market` | string | `us` / `cn` / `hk` / `crypto`. |
| `interval` | string | `1d` / `1h`. |
| `ts_start` | timestamp(us, UTC) | bar start; for daily bars it is the session day at 00:00 UTC (a placeholder — read `session_date` for meaning). |
| `session_date` | date32 | exchange-local trading day. |
| `session` | string | `regular` / `extended` / … |
| `policy_version` | string | version of the selection conventions (`research-daily-v2`, `cn-daily-v1`, …). Several policies may coexist for the same `(instrument_id, time)`. |
| `provider` / `feed` / `adjustment` | string | the source identity triple of the selected bar (`feed` is `""` when absent). |
| `revision` | int32 | canonical row revision; it increases when the same key is rewritten, so it is **not necessarily equal** to the raw row's `revision`. To trace the fetch batch use `source_fetch_id`. |
| `open` / `high` / `low` / `close` | decimal128(20,8) | canonical prices, required. |
| `volume` | decimal128(28,8)? | canonical volume. Selection requires the source bar to have a volume; older rows or special adapter paths may still be null. |
| `vwap` | decimal128(20,8)? | VWAP of the selected source; null when unknown. |
| `trade_count` | int64? | number of trades; null when unknown. |
| `flags` | list<string> | quality fact flags (`DIVERGENT` / `SPIKE_SUSPECT` / `VOLUME_MISSING` / `STALE` / `ADJUSTMENT_UNVERIFIED` / `LIMIT_UP` / `LIMIT_DOWN` …). An empty list means no flags. |
| `selection_reason` | string | why this source won: `primary_source` / `primary_source_corroborated` / `compatible_gap_fill` / `fill_<provider>` / `spike_replaced`. |
| `quality_status` | string | single status summarized from the flags: `complete` / `divergent` / `volume_missing` / `spike_suspect`. It is **independent** of `freshness`. |
| `capabilities` | list<string> | ordered capability strings such as `price_basis:split_adjusted`, `adjustment_scope:splits`, `session_semantics:provider_aligned`, `volume_basis:split_adjusted`, `currency:USD`, `adjustment_as_of:YYYY-MM-DD`, `price_ohlc`. **Read the currency from `currency:<ISO4217>`**; native canonical has no separate `currency` column. |
| `source_fingerprint` | string | opaque fingerprint of the selected source bar, used for content comparison and same-source-bar detection. It is **not** a foreign key and may change on a rewrite or source switch. |
| `price_source` | string | `provider:feed` of the selected price source. |
| `volume_source` | string? | the selected volume source; identical to `price_source` under whole-bar selection, null when unknown. |
| `sources_seen` | list<string> | distinct provider names seen during that reconciliation. |
| `reconcile_rule` | string | the rule label used for this selection; usually the `selection_reason` or the `policy_version`. |
| `generated_at` | timestamp(us, UTC) | when the canonical row was generated. Required. `as_of` reads use it as the knowledge-date filter. |
| `source_fetch_id` | string? | additive nullable column: the `fetch_id` of the selected raw row. This is the stable pointer from canonical to the raw fetch batch. Older files / databases without the column read null, and compatibility import rows may carry `compat:<row id>`. |
| `available_at` | timestamp(us, UTC)? | the earliest moment this bar may be used for research (§4). null means unknown, and `available_by` reads exclude it. |
| `freshness` | string? | `provisional` (same-day, preliminary) or `final`. **A missing column or null reads as `final`.** |

### Where `selected_source_bar_id` fits

`selected_source_bar_id` is an **internal** OpenBars concept: an optional **string** source-row
identifier on in-memory daily rows (`CanonicalDailyRow`) that some compatibility adapter paths
use to store the caller's own row id. It is not part of the generic contract:

- native canonical (Parquet / `canonical_bars`) has **no** such column;
- it is not an integer foreign key of a caller's official table, is not guaranteed to be
  numeric and is not guaranteed to exist;
- adapters must not use it as a join key or as a persistence contract; to trace a source use
  `source_fetch_id` plus the source identity (§5).

## 4. `freshness` and `available_at`

`freshness` is a **timeliness tier**, not quality:

- `provisional`: same-day preliminary data that may still be replaced by final data;
- `final`: final data;
- older rows with a missing column or null are treated as `final`;
- once a cell has a final row, a later provisional row for the same day does not overwrite it;
  upgrading provisional to final increases the canonical `revision` and produces an audit
  event.

`available_at` is **point-in-time availability**:

- its meaning is "the earliest moment this bar could be used for research", not a source
  delivery SLA;
- the value is the `ingested_at` of the raw row behind the selected price source; bars not yet
  stored use the canonical write time (only more conservative, never forward-looking);
  compatibility import rows use the receive time of the selected upstream row;
- **the earliest known time is kept for the same source bar**: on a same-key rewrite where the
  `source_fingerprint`, source identity, `price_source` / `volume_source`, OHLCV, `vwap` and
  `trade_count` are unchanged, the earlier non-null of the old and new value is kept; if any of
  them changed (source switch, new revision, price or volume correction) the new row's own value
  is used;
- without `available_by` reads return the current visible row; with `available_by` they return
  only the newest version with `available_at <= available_by`, and null is not usable.

Per-source latency and readiness are declared in [source-readiness.md](source-readiness.md);
those are source declarations, not guarantees of this contract.

## 5. Tracing source, fetch batch, freshness tier and availability from an adapter

A fetch batch can be reconstructed with this minimal path:

1. **Source**: canonical `provider` / `feed` / `adjustment` (plus `price_source`); for series
   semantics read `capabilities` (`price_basis:*`, `adjustment_scope:*`, `session_semantics:*`,
   `volume_basis:*`, `currency:*`).
2. **Fetch batch**: raw `fetch_id` on the read side; canonical `source_fetch_id` points at the
   same value. `state.db`'s `fetch_log` (primary key `(provider, fetch_id)`) provides the
   batch's window, status, row count, archive reference and start/end times.
3. **Freshness tier**: canonical `freshness` (null means `final`).
4. **Availability**: canonical `available_at`; null when unknown — do not substitute
   `generated_at`.

Exact canonical → raw matching (native producers):

```text
raw.provider      = canonical.provider
raw.feed          = canonical.feed
raw.adjustment    = canonical.adjustment
raw.interval      = canonical.interval
raw.time          = canonical.time (session_date for 1d, ts_start for 1h)
raw entity        = coalesce(raw.instrument_id, raw.symbol_as_returned) = canonical.instrument_id
raw.fetch_id      = canonical.source_fetch_id
```

`source_fetch_id` is the stable pointer; do not join raw on `canonical.revision`, because the
canonical `revision` increases on provisional-to-final upgrades and rewrites and may exceed the
raw row's revision.

When `source_fetch_id` is null (older files, older databases, some compatibility import rows),
only a **best-effort** match on source identity plus date is possible and it cannot be
guaranteed to identify one fetch batch.

## 6. Null semantics at a glance

| Field | Meaning of null |
| --- | --- |
| raw `instrument_id` | the symbol has not been resolved to an OpenBars instrument yet |
| raw `volume` | the source provided no volume (not zero) |
| raw `vwap` / `trade_count` | the source provided none |
| raw `source_artifact_ref` / `ingest_run_id` | nothing archived / not a compatibility import |
| canonical `source_fetch_id` | the producer does not know the fetch batch (older or imported rows) |
| canonical `available_at` | availability unknown; unusable for `available_by` reads |
| canonical `freshness` | missing column / null is treated as `final` |
| canonical `volume` / `vwap` / `trade_count` | unknown, or not provided on that path |
| no `currency:*` in canonical `capabilities` | currency unknown |
| `selected_source_bar_id` | not applicable to native canonical; do not rely on it |

## 7. Versions and compatibility

`SCHEMA_VERSION` only increases when column names, types, unique keys or partitions change.
Adding a **nullable** column is a backward-compatible additive change:

- a new reader reading old Parquet files / Postgres current rows treats a missing column as
  null; a Postgres native write connection (`write=True`) adds the column to `canonical_bars`
  and `canonical_bars_history` when it opens, while read-only access to an old database runs no
  DDL;
- an old reader rejects a newer file by schema version;
- `source_fetch_id` is an additive nullable column and `SCHEMA_VERSION` stays 1.

Adapters must tolerate missing columns (as null) and unknown values, and must never treat
source-information fields as necessarily present. The caller's official table structure,
integer primary keys and write targets are the adapter's own responsibility: the OpenBars core
does not know or reference them.
