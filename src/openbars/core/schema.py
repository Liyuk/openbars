"""Versioned Arrow schema declarations (declaration only; storage comes later).

The field lists are plain data so they can be inspected without pyarrow. The real
``pyarrow.Schema`` objects are built lazily by :func:`raw_daily_bar_schema` and
:func:`canonical_daily_schema`; importing this module never requires pyarrow.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

SCHEMA_VERSION = 1
SCHEMA_VERSION_METADATA_KEY = b"openbars.schema_version"

# Logical type names understood by ``_arrow_type``.
STRING = "string"
INT64 = "int64"
DATE32 = "date32"
TIMESTAMP_UTC = "timestamp[us, tz=UTC]"
PRICE = "decimal128(20, 8)"
VOLUME = "decimal128(28, 8)"
STRING_LIST = "list<item: string>"


@dataclass(frozen=True)
class FieldSpec:
    name: str
    type: str
    nullable: bool = True


RAW_DAILY_BAR_FIELDS: tuple[FieldSpec, ...] = (
    # identity of the bar
    FieldSpec("instrument_id", STRING, True),  # null until the symbol is resolved
    FieldSpec("session_date", DATE32, True),
    FieldSpec("ts_start", TIMESTAMP_UTC, False),
    FieldSpec("symbol_as_returned", STRING, False),
    FieldSpec("fetch_id", STRING, False),
    FieldSpec("revision", INT64, False),  # >= 1
    # source identity triple
    FieldSpec("provider", STRING, False),
    FieldSpec("feed", STRING, False),
    FieldSpec("adjustment", STRING, False),  # legacy adjustment string
    # series semantics
    FieldSpec("interval", STRING, False),  # "1d"
    FieldSpec("price_basis", STRING, False),
    FieldSpec("adjustment_scope", STRING, False),
    FieldSpec("adjustment_as_of", DATE32, True),
    FieldSpec("session_semantics", STRING, False),
    FieldSpec("currency", STRING, True),
    FieldSpec("volume_basis", STRING, False),
    # values
    FieldSpec("open", PRICE, False),
    FieldSpec("high", PRICE, False),
    FieldSpec("low", PRICE, False),
    FieldSpec("close", PRICE, False),
    FieldSpec("volume", VOLUME, True),
    FieldSpec("vwap", PRICE, True),
    # provenance
    FieldSpec("payload_hash", STRING, True),
    FieldSpec("ingested_at", TIMESTAMP_UTC, True),
)

CANONICAL_DAILY_FIELDS: tuple[FieldSpec, ...] = (
    FieldSpec("instrument_id", STRING, False),
    FieldSpec("session_date", DATE32, False),
    FieldSpec("policy_version", STRING, False),
    FieldSpec("selected_source_bar_id", STRING, True),
    FieldSpec("provider", STRING, False),
    FieldSpec("feed", STRING, False),
    FieldSpec("adjustment", STRING, False),
    FieldSpec("revision", INT64, False),
    FieldSpec("open", PRICE, False),
    FieldSpec("high", PRICE, False),
    FieldSpec("low", PRICE, False),
    FieldSpec("close", PRICE, False),
    FieldSpec("volume", VOLUME, False),
    FieldSpec("selection_reason", STRING, False),
    FieldSpec("quality_status", STRING, False),
    FieldSpec("capabilities", STRING_LIST, False),
    FieldSpec("source_fingerprint", STRING, False),
    FieldSpec("generated_at", TIMESTAMP_UTC, False),
    FieldSpec("currency", STRING, True),  # ISO 4217 of the bar prices; null when unknown
    FieldSpec("available_at", TIMESTAMP_UTC, True),  # #154: research availability; null = unknown
)

CANONICAL_DAILY_UNIQUE_KEY = ("instrument_id", "session_date", "policy_version")


def _arrow_type(pa: Any, name: str) -> Any:
    if name == STRING:
        return pa.string()
    if name == INT64:
        return pa.int64()
    if name == DATE32:
        return pa.date32()
    if name == TIMESTAMP_UTC:
        return pa.timestamp("us", tz="UTC")
    if name == PRICE:
        return pa.decimal128(20, 8)
    if name == VOLUME:
        return pa.decimal128(28, 8)
    if name == STRING_LIST:
        return pa.list_(pa.string())
    raise ValueError(f"unknown logical type: {name}")


def _build(fields: tuple[FieldSpec, ...], kind: str) -> Any:
    try:
        import pyarrow as pa
    except ImportError as exc:  # pragma: no cover - depends on the environment
        raise ImportError(
            "pyarrow is required to build Arrow schemas; install openbars[parquet]"
        ) from exc
    return pa.schema(
        [pa.field(f.name, _arrow_type(pa, f.type), nullable=f.nullable) for f in fields],
        metadata={
            SCHEMA_VERSION_METADATA_KEY: str(SCHEMA_VERSION).encode(),
            b"openbars.schema": kind.encode(),
        },
    )


def raw_daily_bar_schema() -> Any:
    """Arrow schema of raw (per-source) daily bars."""
    return _build(RAW_DAILY_BAR_FIELDS, "raw_daily_bar")


def canonical_daily_schema() -> Any:
    """Arrow schema of research-daily-v2 canonical rows."""
    return _build(CANONICAL_DAILY_FIELDS, "canonical_daily")
