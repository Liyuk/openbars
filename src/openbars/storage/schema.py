"""On-disk schemas for the openbars storage layer (``openbars-storage-v1``).

One definition drives every backend: Arrow/Parquet schemas for the raw, canonical and
ref layers, the state-table specs (SQLite default, Postgres native), the dedup keys,
and the SQL type mapping used for DuckDB views and Postgres DDL. See
docs/architecture.md section 10 for the prose version.

Numbers: prices are ``decimal128(20,8)`` and volumes ``decimal128(28,8)`` everywhere
(Parquet, DuckDB, Postgres ``NUMERIC``). Nothing is persisted through binary floats.

Schema versioning: ``SCHEMA_VERSION`` is stamped into every Parquet file's key-value
metadata (``openbars.schema_version``) and into ``state.db`` (``schema_meta``). v1 is the
first on-disk version; any change to column names, types, keys or partitioning bumps
it and ships a documented migration (architecture.md section 10.6). Readers refuse files
stamped with a version newer than ``SCHEMA_VERSION`` (``SchemaVersionError``).
"""

from __future__ import annotations

from enum import StrEnum
from typing import Mapping

try:
    import pyarrow as pa
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "openbars.storage.schema needs pyarrow: pip install 'openbars[parquet]'"
    ) from exc

from openbars.storage.state_schema import (  # noqa: F401  (re-exported)
    FETCH_STATUSES,
    SCHEMA_VERSION,
    STATE_TABLES,
    StateColumn,
    StateTable,
    qident,
    state_ddl,
)

SCHEMA_NAME = f"openbars-storage-v{SCHEMA_VERSION}"
METADATA_VERSION_KEY = b"openbars.schema_version"
METADATA_LAYER_KEY = b"openbars.layer"

DEFAULT_POLICY_VERSION = "research-daily-v2"
UNKNOWN = "unknown"


# --- bar-series semantics (string-compatible with the upstream internal implementation) ---


class PriceBasis(StrEnum):
    RAW = "raw"
    SPLIT_ADJUSTED = "split_adjusted"
    TOTAL_RETURN_ADJUSTED = "total_return_adjusted"
    PROVIDER_DEFINED = "provider_defined"
    UNKNOWN = "unknown"


class AdjustmentScope(StrEnum):
    NONE = "none"
    SPLITS = "splits"
    DIVIDENDS = "dividends"
    SPLITS_AND_DIVIDENDS = "splits_and_dividends"
    PROVIDER_DEFINED = "provider_defined"
    UNKNOWN = "unknown"


class SessionSemantics(StrEnum):
    REGULAR = "regular"
    EXTENDED = "extended"
    PROVIDER_ALIGNED = "provider_aligned"
    UNKNOWN = "unknown"


class VolumeBasis(StrEnum):
    AS_TRADED = "as_traded"
    SPLIT_ADJUSTED = "split_adjusted"
    PROVIDER_DEFINED = "provider_defined"
    UNKNOWN = "unknown"


# --- Arrow types -------------------------------------------------------------------------

PRICE = pa.decimal128(20, 8)
VOLUME = pa.decimal128(28, 8)
PRICE_SCALE = 8
TS = pa.timestamp("us", tz="UTC")
STR_LIST = pa.list_(pa.string())


def _f(name: str, typ: pa.DataType, nullable: bool = False) -> pa.Field:
    return pa.field(name, typ, nullable=nullable)


RAW_BARS_FIELDS = [
    _f("provider", pa.string()),
    _f("market", pa.string()),  # partition key duplicated into the file
    _f("feed", pa.string()),  # "" when the provider has no feed concept
    _f("adjustment", pa.string()),  # as delivered by the provider; "raw" when unadjusted
    _f("symbol_as_returned", pa.string()),
    _f("instrument_id", pa.string(), True),  # null until the symbol is resolved
    _f("interval", pa.string()),
    _f("ts_start", TS),
    _f("session_date", pa.date32()),
    _f("price_basis", pa.string()),
    _f("adjustment_scope", pa.string()),
    _f("adjustment_as_of", pa.date32(), True),
    _f("session_semantics", pa.string()),
    _f("currency", pa.string(), True),
    _f("volume_basis", pa.string()),
    _f("revision", pa.int32()),
    _f("open", PRICE),
    _f("high", PRICE),
    _f("low", PRICE),
    _f("close", PRICE),
    _f("volume", VOLUME, True),  # null = provider sent no volume
    _f("vwap", PRICE, True),
    _f("trade_count", pa.int64(), True),
    _f("fetch_id", pa.string()),
    _f("source_artifact_ref", pa.string(), True),  # relative path under data/raw-archive
    _f("ingest_run_id", pa.string(), True),
    _f("ingested_at", TS),
]

CANONICAL_BARS_FIELDS = [
    _f("instrument_id", pa.string()),
    _f("market", pa.string()),
    _f("interval", pa.string()),
    _f("ts_start", TS),
    _f("session_date", pa.date32()),
    _f("session", pa.string()),
    _f("policy_version", pa.string()),
    _f("provider", pa.string()),
    _f("feed", pa.string()),
    _f("adjustment", pa.string()),
    _f("revision", pa.int32()),
    _f("open", PRICE),
    _f("high", PRICE),
    _f("low", PRICE),
    _f("close", PRICE),
    _f("volume", VOLUME, True),
    _f("vwap", PRICE, True),
    _f("trade_count", pa.int64(), True),
    _f("flags", STR_LIST),  # QualityFlag member names, e.g. ["DIVERGENT"]
    _f("selection_reason", pa.string()),
    _f("quality_status", pa.string()),
    _f("capabilities", STR_LIST),
    _f("source_fingerprint", pa.string()),
    _f("price_source", pa.string()),
    _f("volume_source", pa.string(), True),
    _f("sources_seen", STR_LIST),
    _f("reconcile_rule", pa.string()),
    _f("generated_at", TS),
]

INSTRUMENTS_FIELDS = [
    _f("instrument_id", pa.string()),
    _f("market", pa.string()),
    _f("asset_class", pa.string()),
    _f("currency", pa.string()),
    _f("name", pa.string(), True),
    _f("exchange", pa.string(), True),
    _f("listed_on", pa.date32(), True),
    _f("delisted_on", pa.date32(), True),
    _f("external_ids", pa.map_(pa.string(), pa.string())),
]

SYMBOL_MAPPINGS_FIELDS = [
    _f("instrument_id", pa.string()),
    _f("provider", pa.string()),  # "*" = any provider
    _f("symbol", pa.string()),
    _f("valid_from", pa.date32()),
    _f("valid_to", pa.date32(), True),  # inclusive; null = open ended
]

# Identity-layer side tables (ref/*.parquet, small): queue of bars / listing rows that could
# not be mapped to exactly one instrument, per-(instrument, symbol, provider) last sighting,
# and the optional link from an instrument id to an upstream integer id.
UNRESOLVED_FIELDS = [
    _f("kind", pa.string()),  # "bar" | "listing"
    _f("reason_code", pa.string()),
    _f("symbol", pa.string()),
    _f("provider", pa.string()),
    _f("market", pa.string()),
    _f("session_date", pa.date32(), True),
    _f("detail", pa.string(), True),
]

SYMBOL_OBSERVATIONS_FIELDS = [
    _f("instrument_id", pa.string()),
    _f("symbol", pa.string()),
    _f("provider", pa.string()),
    _f("last_observed", pa.date32()),
]

UPSTREAM_IDS_FIELDS = [
    _f("instrument_id", pa.string()),
    _f("upstream_id", pa.int64()),
]

CORPORATE_ACTIONS_FIELDS = [
    _f("instrument_id", pa.string()),
    _f("ex_date", pa.date32()),
    _f("kind", pa.string()),
    _f("source", pa.string()),
    _f("ratio", PRICE, True),
    _f("amount", PRICE, True),
]


def _schema(layer: str, fields: list[pa.Field]) -> pa.Schema:
    return pa.schema(
        fields,
        metadata={
            METADATA_VERSION_KEY: str(SCHEMA_VERSION).encode(),
            METADATA_LAYER_KEY: layer.encode(),
        },
    )


RAW_BARS_SCHEMA = _schema("raw_bars", RAW_BARS_FIELDS)
CANONICAL_BARS_SCHEMA = _schema("canonical_bars", CANONICAL_BARS_FIELDS)
INSTRUMENTS_SCHEMA = _schema("instruments", INSTRUMENTS_FIELDS)
SYMBOL_MAPPINGS_SCHEMA = _schema("symbol_mappings", SYMBOL_MAPPINGS_FIELDS)
CORPORATE_ACTIONS_SCHEMA = _schema("corporate_actions", CORPORATE_ACTIONS_FIELDS)
UNRESOLVED_SCHEMA = _schema("unresolved", UNRESOLVED_FIELDS)
SYMBOL_OBSERVATIONS_SCHEMA = _schema("symbol_observations", SYMBOL_OBSERVATIONS_FIELDS)
UPSTREAM_IDS_SCHEMA = _schema("upstream_ids", UPSTREAM_IDS_FIELDS)

TABLE_SCHEMAS: Mapping[str, pa.Schema] = {
    "raw_bars": RAW_BARS_SCHEMA,
    "canonical_bars": CANONICAL_BARS_SCHEMA,
    "instruments": INSTRUMENTS_SCHEMA,
    "symbol_mappings": SYMBOL_MAPPINGS_SCHEMA,
    "corporate_actions": CORPORATE_ACTIONS_SCHEMA,
}

# Identity-layer side tables live in ref/ but are not part of the Postgres native
# schema yet (Parquet + DuckDB catalog only).
REF_IDENTITY_SCHEMAS: Mapping[str, pa.Schema] = {
    "unresolved": UNRESOLVED_SCHEMA,
    "symbol_observations": SYMBOL_OBSERVATIONS_SCHEMA,
    "upstream_ids": UPSTREAM_IDS_SCHEMA,
}

# --- identity / dedup keys ---------------------------------------------------------------
# Templates use ``{t}`` as a table-alias prefix ("" or "e.") so the same key can be used in
# window functions and joins. For 1d bars the time component of the key is ``session_date``;
# for intraday bars it is ``ts_start`` (one trading day holds many intraday bars).

_BAR_DATE = "CASE WHEN {t}\"interval\" = '1d' THEN {t}session_date END"
_BAR_TS = "CASE WHEN {t}\"interval\" = '1d' THEN NULL ELSE {t}ts_start END"

RAW_KEY_SQL: tuple[str, ...] = (
    '{t}"market"',
    '{t}"interval"',
    "{t}provider",
    "{t}feed",
    "{t}adjustment",
    "{t}revision",
    "coalesce({t}instrument_id, {t}symbol_as_returned)",
    _BAR_DATE,
    _BAR_TS,
)

CANONICAL_KEY_SQL: tuple[str, ...] = (
    "{t}instrument_id",
    '{t}"interval"',
    "{t}policy_version",
    _BAR_DATE,
    _BAR_TS,
)

# Columns that describe *how* a row arrived, not *what* it says. A re-write that only
# differs in these is a no-op (it does not create a new part file).
RAW_LINEAGE_COLUMNS = frozenset({"fetch_id", "source_artifact_ref", "ingest_run_id", "ingested_at"})
CANONICAL_LINEAGE_COLUMNS = frozenset({"generated_at"})


def key_exprs(template: tuple[str, ...], alias: str = "") -> list[str]:
    prefix = f"{alias}." if alias else ""
    return [expr.format(t=prefix) for expr in template]


# --- type mapping ------------------------------------------------------------------------


def duckdb_type(typ: pa.DataType) -> str:
    if pa.types.is_decimal(typ):
        return f"DECIMAL({typ.precision},{typ.scale})"
    if pa.types.is_string(typ):
        return "VARCHAR"
    if pa.types.is_date32(typ):
        return "DATE"
    if pa.types.is_timestamp(typ):
        return "TIMESTAMPTZ" if typ.tz else "TIMESTAMP"
    if pa.types.is_int32(typ):
        return "INTEGER"
    if pa.types.is_int64(typ):
        return "BIGINT"
    if pa.types.is_list(typ):
        return duckdb_type(typ.value_type) + "[]"
    if pa.types.is_map(typ):
        return f"MAP({duckdb_type(typ.key_type)}, {duckdb_type(typ.item_type)})"
    raise TypeError(f"unsupported arrow type {typ}")


def postgres_type(typ: pa.DataType) -> str:
    if pa.types.is_decimal(typ):
        return f"NUMERIC({typ.precision},{typ.scale})"
    if pa.types.is_string(typ):
        return "TEXT"
    if pa.types.is_date32(typ):
        return "DATE"
    if pa.types.is_timestamp(typ):
        return "TIMESTAMPTZ"
    if pa.types.is_int32(typ):
        return "INTEGER"
    if pa.types.is_int64(typ):
        return "BIGINT"
    if pa.types.is_list(typ):
        return postgres_type(typ.value_type) + "[]"
    if pa.types.is_map(typ):
        return "JSONB"
    raise TypeError(f"unsupported arrow type {typ}")
