"""Optional Postgres backend implementing the ``StorageBackend`` contract.

Two schema modes (docs/architecture-internals.md section 11.3):

``native``
    openbars owns the DDL, which is generated from the same schema definitions as the
    Parquet layout (``schema.py``) - one table per Parquet table, same column names,
    ``NUMERIC(20,8)`` / ``NUMERIC(28,8)`` for prices / volumes, ``TEXT[]`` for list
    columns, ``JSONB`` for ``external_ids``. ``raw_bars.extra`` is a nullable JSONB
    column added additively (#122): it is not in ``RAW_BARS_SCHEMA`` / ``TABLE_SCHEMAS``
    (those stay shared with Parquet). Existing databases get
    ``ALTER TABLE raw_bars ADD COLUMN IF NOT EXISTS extra JSONB`` on ``write=True``;
    a read-only open of a pre-migration catalog treats a missing column as NULL. That includes the identity side tables
    ``unresolved`` / ``symbol_observations`` / ``upstream_ids`` (``PgInstrumentMaster`` is a
    full ``InstrumentStore``). Quota tables are not part of this schema. Tables are created (``CREATE ... IF NOT
    EXISTS``) only when ``write=True``; a read-only connection merely verifies them.

``upstream_compat``
    Tables belong to the upstream internal implementation. **No migration is ever run.**
    On connect the catalog is introspected and a structured ``UpstreamSchemaError``
    (``.problems``: missing table / missing column / type mismatch) is raised on any
    difference. Reads map upstream rows onto the openbars row types (see ``_compat_*``
    for the documented placeholders - upstream has no ``ts_start`` / ``fetch_id`` /
    ``price_source``). Writes are enabled only when ``write=True``: they map openbars
    rows onto the existing upstream tables (no DDL), allocate integer instrument ids,
    bump research-series watermarks, and take the published per-session advisory lock.
    ``write=False`` stays read-only. Jobs, checkpoints and fetch_log live in a local
    SQLite ``state_path`` because upstream owns no such tables.

Connections are short-lived and opened by ``connect`` (default: ``psycopg`` from the
``postgres`` extra). Read-only backends open read-only transactions. Everything that
needs a live server goes through the injected factory, so the introspection checker is
unit-testable with a fake catalog.
"""

from __future__ import annotations

import hashlib
import json
import logging
import warnings
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import ROUND_HALF_EVEN, Context, Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Mapping, Sequence

import pyarrow as pa

from openbars.core.models import (
    AssetClass,
    CanonicalBar,
    CorporateAction,
    CorporateActionKind,
    DateRange,
    Instrument,
    Interval,
    Market,
    RawBar,
    SymbolMapping,
)
from openbars.normalize.instruments import UnresolvedRecord, _check_upstream, _ResolveMixin, select_mappings
from openbars.providers.base import RawBatch
from openbars.storage.errors import (
    CoverageTimeout,
    ReadOnlyError,
    SchemaMismatchError,
    SchemaProblem,
    StorageError,
    UpstreamSchemaError,
)
from openbars.storage.rows import (
    normalize_legacy_daily,
    CanonicalBarRow,
    RawBarRow,
    encode_extra,
    raw_rows_from_batch,
    records_to_rows,
    row_values,
    check_rows,
    to_decimal,
    available_by_utc,
)
from openbars.storage.schema import (
    ADJUST_FACTORS_SCHEMA,
    CANONICAL_BARS_SCHEMA,
    CORPORATE_ACTIONS_SCHEMA,
    DEFAULT_POLICY_VERSION,
    INSTRUMENTS_SCHEMA,
    RAW_BARS_SCHEMA,
    REF_IDENTITY_SCHEMAS,
    SAME_SOURCE_BAR_COLUMNS,
    STATE_TABLES,
    SYMBOL_MAPPINGS_SCHEMA,
    SCHEMA_VERSION,
    postgres_type,
    qident,
    state_ddl,
)
from openbars.storage.bar_reader import StoreBarReader
from openbars.storage.state import SqliteStateStore, SqlStateStore
from openbars.storage.compat_side import CompatSideStore
from openbars.storage.compat_write import (
    AmbiguousInstrumentError,
    CompatIdentity,
    action_details,
    action_event_key,
    action_type_for,
    actions_from_revision,
    advisory_lock_key,
    kind_from_action_type,
    capabilities_json,
    fingerprint_or_raise,
    parse_int_id,
    parse_optional_int_ref,
    quality_status_of,
    security_type_for,
)


class PgMode(str, Enum):
    NATIVE = "native"
    UPSTREAM_COMPAT = "upstream_compat"


# --- catalog description & checker -------------------------------------------------------


@dataclass(frozen=True)
class ColumnInfo:
    """One ``information_schema.columns`` row."""

    data_type: str
    numeric_precision: int | None = None
    numeric_scale: int | None = None


@dataclass(frozen=True)
class ColumnSpec:
    family: str  # int | text | numeric | date | timestamptz | json | bool | array
    precision: int | None = None
    scale: int | None = None

    def describe(self) -> str:
        if self.family == "numeric" and self.precision is not None:
            return f"numeric({self.precision},{self.scale})"
        return self.family


_FAMILY_TYPES: Mapping[str, frozenset[str]] = {
    "int": frozenset({"smallint", "integer", "bigint"}),
    "text": frozenset({"character varying", "text", "character"}),
    "numeric": frozenset({"numeric"}),
    "date": frozenset({"date"}),
    "timestamptz": frozenset({"timestamp with time zone"}),
    "json": frozenset({"json", "jsonb"}),
    "bool": frozenset({"boolean"}),
    "array": frozenset({"ARRAY"}),
}

Catalog = Mapping[str, Mapping[str, ColumnInfo]]
Requirements = Mapping[str, Mapping[str, ColumnSpec]]


def _spec(family: str, p: int | None = None, s: int | None = None) -> ColumnSpec:
    return ColumnSpec(family, p, s)


I, T, D, TZ, J = (_spec(x) for x in ("int", "text", "date", "timestamptz", "json"))
PX, VOL = _spec("numeric", 20, 8), _spec("numeric", 28, 8)

# Column-level contract with the upstream internal implementation's tables
# (names/types only; no data, no ORM).
UPSTREAM_REQUIRED: Requirements = {
    "instrument": {
        "id": I, "issuer_id": I, "asset_class": T, "security_type": T,
        "primary_exchange": T, "created_at": TZ,
    },
    "symbol_alias": {
        "id": I, "instrument_id": I, "symbol": T, "exchange": T, "valid_from": D,
        "valid_from_basis": T, "valid_to": D, "valid_to_basis": T, "source_ref": T,
        "verification_status": T, "known_at": TZ, "recorded_at": TZ,
    },
    "source_artifact": {
        "id": I, "provider": T, "session_date": D, "request_id": T, "request_params": T,
        "content_digest": T, "storage_path": T, "license_tag": T, "received_at": TZ,
    },
    "data_ingest_run": {
        "id": I, "provider": T, "feed": T, "adjustment": T, "scope": T, "start_date": D,
        "end_date": D, "status": T, "expected_count": I, "received_count": I,
        "rejected_count": I, "created_at": TZ, "finished_at": TZ,
    },
    "daily_bar": {
        "id": I, "instrument_id": I, "session_date": D, "provider": T, "feed": T,
        "adjustment": T, "price_basis": T, "adjustment_scope": T, "adjustment_as_of": D,
        "session_semantics": T, "currency": T, "volume_basis": T, "revision": I,
        "open": PX, "high": PX, "low": PX, "close": PX, "volume": VOL, "vwap": PX,
        "source_artifact_id": I, "ingest_run_id": I,
    },
    "research_daily_bar": {
        "id": I, "instrument_id": I, "session_date": D, "policy_version": T,
        "selected_source_bar_id": I, "provider": T, "feed": T, "adjustment": T,
        "revision": I, "open": PX, "high": PX, "low": PX, "close": PX, "volume": VOL,
        "selection_reason": T, "quality_status": T, "capabilities": J,
        "source_fingerprint": T, "generated_at": TZ,
    },
    "research_series_watermark": {"policy_version": T, "version": I, "updated_at": TZ},
    "research_series_date_watermark": {
        "policy_version": T, "session_date": D, "version": I, "updated_at": TZ,
    },
    "provider_request_gate": {"provider": T, "next_request_at": TZ},
    "provider_request_usage": {"id": I, "provider": T, "reserved_at": TZ},
    "corporate_action_observation": {
        "id": I, "provider": T, "dataset": T, "external_id": T, "event_type": T,
        "payload_hash": T, "payload": J, "normalization_status": T,
        "source_artifact_id": I, "observed_at": TZ,
    },
    "corporate_action_event": {"id": I, "event_key": T, "created_at": TZ},
    "corporate_action_revision": {
        "id": I, "event_id": I, "revision": I, "action_type": T, "verification_status": T,
        "valid_from": D, "known_at": TZ, "announced_at": TZ, "ex_date": D,
        "record_date": D, "payable_at": D, "old_symbol": T, "new_symbol": T,
        "instrument_ids": J, "observation_ids": J, "details": J, "actor": T,
        "supersedes_id": I, "created_at": TZ,
    },
    "corporate_action_observation_link": {"revision_id": I, "observation_id": I},
}


def _arrow_family(typ: pa.DataType) -> ColumnSpec:
    if pa.types.is_decimal(typ):
        return _spec("numeric", typ.precision, typ.scale)
    if pa.types.is_string(typ):
        return T
    if pa.types.is_date32(typ):
        return D
    if pa.types.is_timestamp(typ):
        return TZ
    if pa.types.is_integer(typ):
        return I
    if pa.types.is_list(typ):
        return _spec("array")
    if pa.types.is_map(typ):
        return J
    raise TypeError(typ)


_STATE_FAMILY = {"text": T, "int": I, "date": D, "timestamp": TZ, "json": J}

# Native mode requires exactly the tables this module would create.
NATIVE_REQUIRED: Requirements = {
    **{
        name: {f.name: _arrow_family(f.type) for f in schema}
        for name, schema in (
            ("raw_bars", RAW_BARS_SCHEMA),
            ("canonical_bars", CANONICAL_BARS_SCHEMA),
            ("instruments", INSTRUMENTS_SCHEMA),
            ("symbol_mappings", SYMBOL_MAPPINGS_SCHEMA),
            ("corporate_actions", CORPORATE_ACTIONS_SCHEMA),
            *REF_IDENTITY_SCHEMAS.items(),
        )
    },
    **{t.name: {c.name: _STATE_FAMILY[c.kind] for c in t.columns} for t in STATE_TABLES},
}


def check_schema(catalog: Catalog, required: Requirements) -> list[SchemaProblem]:
    """Compare an introspected catalog with a requirement set (pure; no database needed)."""
    problems: list[SchemaProblem] = []
    for table, columns in required.items():
        actual_cols = catalog.get(table)
        if not actual_cols:
            problems.append(SchemaProblem(table, None, "missing_table", "table"))
            continue
        for column, spec in columns.items():
            info = actual_cols.get(column)
            if info is None:
                problems.append(SchemaProblem(table, column, "missing_column", spec.describe()))
                continue
            ok = info.data_type in _FAMILY_TYPES[spec.family]
            if ok and spec.family == "numeric" and spec.precision is not None:
                ok = (info.numeric_precision, info.numeric_scale) == (spec.precision, spec.scale)
            if not ok:
                actual = info.data_type
                if info.data_type == "numeric":
                    actual = f"numeric({info.numeric_precision},{info.numeric_scale})"
                problems.append(SchemaProblem(table, column, "type_mismatch", spec.describe(), actual))
    return problems


def check_upstream_compat(catalog: Catalog) -> None:
    """Raise ``UpstreamSchemaError`` listing every difference from ``UPSTREAM_REQUIRED``."""
    problems = check_schema(catalog, UPSTREAM_REQUIRED)
    if problems:
        raise UpstreamSchemaError(problems)


# Additive nullable columns a pre-migration database may lack (``write=True`` adds them).
_OPTIONAL_NATIVE_COLUMNS = frozenset({
    ("canonical_bars", "available_at"), ("canonical_bars", "freshness"),
    ("canonical_bars", "source_fetch_id"),
})


# #124: A-share adjust factors. Additive table: a database without it reads as empty and gets
# it on ``write=True``; when present its columns must match. Not in ``NATIVE_REQUIRED`` /
# ``TABLE_SCHEMAS`` (the shared raw / canonical mapping stays as it is).
ADJUST_FACTORS_REQUIRED: Requirements = {
    "adjust_factors": {f.name: _arrow_family(f.type) for f in ADJUST_FACTORS_SCHEMA}
}
_ADJUST_FACTORS_KEY = ("instrument_id", "source", "effective_from")


def check_native(catalog: Catalog) -> None:
    problems = [
        p for p in check_schema(catalog, NATIVE_REQUIRED)
        if not (p.kind == "missing_column" and (p.table, p.column) in _OPTIONAL_NATIVE_COLUMNS)
        and not (p.kind == "missing_table" and p.table == "canonical_audit")
    ]
    if catalog.get("adjust_factors"):
        problems += check_schema(catalog, ADJUST_FACTORS_REQUIRED)
    # ``raw_bars.extra`` is optional (pre-#122 databases). If present it must be JSON/JSONB.
    raw = catalog.get("raw_bars") or {}
    if "extra" in raw and raw["extra"].data_type not in _FAMILY_TYPES["json"]:
        info = raw["extra"]
        problems.append(SchemaProblem("raw_bars", "extra", "type_mismatch", "jsonb", info.data_type))
    if problems:
        raise SchemaMismatchError(problems)


_CATALOG_SQL = """
SELECT table_name, column_name, data_type, numeric_precision, numeric_scale
FROM information_schema.columns WHERE table_schema = current_schema()
"""


def introspect_catalog(conn: "_Conn") -> dict[str, dict[str, ColumnInfo]]:
    catalog: dict[str, dict[str, ColumnInfo]] = {}
    for r in conn.execute(_CATALOG_SQL).fetchall():
        catalog.setdefault(r["table_name"], {})[r["column_name"]] = ColumnInfo(
            r["data_type"], r["numeric_precision"], r["numeric_scale"]
        )
    return catalog


# --- native DDL --------------------------------------------------------------------------

_REF_KEYS = {
    "instruments": ("instrument_id",),
    "symbol_mappings": ("instrument_id", "provider", "symbol", "valid_from"),
    "corporate_actions": ("instrument_id", "ex_date", "kind", "source"),
    # identity side tables (same keys as the Parquet merge logic)
    "symbol_observations": ("instrument_id", "symbol", "provider"),
    "upstream_ids": ("instrument_id",),  # plus a unique index on upstream_id: the link is 1:1
    # ``UnresolvedRecord.key``. Not a PRIMARY KEY: session_date / detail are nullable and only
    # part of the key for some kinds, so it is the expression unique index ``_UNRESOLVED_KEY``.
    "unresolved": ("kind", "provider", "market", "symbol", "session_date", "detail"),
}
_UNRESOLVED_DAY = "coalesce(CASE WHEN kind IN ('bar', 'corporate_action') THEN session_date END, DATE '0001-01-01')"
_UNRESOLVED_DETAIL = "(CASE WHEN kind = 'corporate_action' THEN coalesce(detail, '') ELSE '' END)"
_UNRESOLVED_KEY = f'kind, provider, market, symbol, {_UNRESOLVED_DAY}, {_UNRESOLVED_DETAIL}'
_RAW_KEY_COLS = ("market", "interval", "provider", "feed", "adjustment", "revision")
_RAW_ENTITY = "(coalesce(instrument_id, symbol_as_returned))"
_CANON_KEY_COLS = ("instrument_id", "interval", "policy_version")


def _table_ddl(name: str, schema: pa.Schema, primary_key: Sequence[str] = ()) -> str:
    cols = [
        f"{qident(f.name)} {postgres_type(f.type)}{'' if f.nullable else ' NOT NULL'}" for f in schema
    ]
    if primary_key:
        cols.append("PRIMARY KEY (" + ", ".join(qident(c) for c in primary_key) + ")")
    return f"CREATE TABLE IF NOT EXISTS {qident(name)} (\n  " + ",\n  ".join(cols) + "\n)"


def _canonical_history_ddl() -> str:
    columns = [
        f"{qident(f.name)} {postgres_type(f.type)}{'' if f.nullable else ' NOT NULL'}"
        for f in CANONICAL_BARS_SCHEMA
    ]
    return (
        'CREATE TABLE IF NOT EXISTS "canonical_bars_history" '
        '(history_id BIGSERIAL PRIMARY KEY, ' + ", ".join(columns) + ")"
    )


def _unique_index(name: str, table: str, cols: Sequence[str], extra: Sequence[str], time_col: str, daily: bool) -> str:
    key = ", ".join([*(qident(c) for c in cols), *extra, qident(time_col)])
    pred = "= '1d'" if daily else "<> '1d'"
    return f'CREATE UNIQUE INDEX IF NOT EXISTS {qident(name)} ON {qident(table)} ({key}) WHERE "interval" {pred}'


def native_ddl() -> list[str]:
    """DDL for every native table (idempotent). Same columns as the Parquet schemas."""
    stmts = [
        _table_ddl("raw_bars", RAW_BARS_SCHEMA),
        # Additive, nullable; CREATE stays 1:1 with RAW_BARS_SCHEMA so existing DDL tests hold.
        'ALTER TABLE "raw_bars" ADD COLUMN IF NOT EXISTS extra JSONB',
        _unique_index("ux_raw_bars_daily", "raw_bars", _RAW_KEY_COLS, [_RAW_ENTITY], "session_date", True),
        _unique_index("ux_raw_bars_intraday", "raw_bars", _RAW_KEY_COLS, [_RAW_ENTITY], "ts_start", False),
        'CREATE INDEX IF NOT EXISTS "ix_raw_bars_scan" ON "raw_bars" ("provider", "market", "interval", "session_date")',
        _table_ddl("canonical_bars", CANONICAL_BARS_SCHEMA),
        _canonical_history_ddl(),
        _unique_index("ux_canonical_bars_daily", "canonical_bars", _CANON_KEY_COLS, [], "session_date", True),
        _unique_index("ux_canonical_bars_intraday", "canonical_bars", _CANON_KEY_COLS, [], "ts_start", False),
        # #154: additive, nullable; databases created before it get the column on write=True.
        'ALTER TABLE "canonical_bars" ADD COLUMN IF NOT EXISTS available_at TIMESTAMPTZ',
        'ALTER TABLE "canonical_bars" ADD COLUMN IF NOT EXISTS freshness TEXT',
        # #202: additive, nullable; the stable canonical -> raw fetch-batch pointer.
        'ALTER TABLE "canonical_bars" ADD COLUMN IF NOT EXISTS source_fetch_id TEXT',
        'ALTER TABLE "canonical_bars_history" ADD COLUMN IF NOT EXISTS source_fetch_id TEXT',
        'CREATE INDEX IF NOT EXISTS "ix_canonical_bars_scan" ON "canonical_bars" ("market", "interval", "policy_version", "session_date")',
        _table_ddl("instruments", INSTRUMENTS_SCHEMA, _REF_KEYS["instruments"]),
        _table_ddl("symbol_mappings", SYMBOL_MAPPINGS_SCHEMA, _REF_KEYS["symbol_mappings"]),
        'CREATE INDEX IF NOT EXISTS "ix_symbol_mappings_symbol" ON "symbol_mappings" ("symbol")',
        _table_ddl("corporate_actions", CORPORATE_ACTIONS_SCHEMA, _REF_KEYS["corporate_actions"]),
        _table_ddl("unresolved", REF_IDENTITY_SCHEMAS["unresolved"]),
        f'CREATE UNIQUE INDEX IF NOT EXISTS "ux_unresolved_key" ON "unresolved" ({_UNRESOLVED_KEY})',
        _table_ddl("symbol_observations", REF_IDENTITY_SCHEMAS["symbol_observations"], _REF_KEYS["symbol_observations"]),
        _table_ddl("upstream_ids", REF_IDENTITY_SCHEMAS["upstream_ids"], _REF_KEYS["upstream_ids"]),
        'CREATE UNIQUE INDEX IF NOT EXISTS "ux_upstream_ids_upstream_id" ON "upstream_ids" ("upstream_id")',
        # #124: additive table (CREATE TABLE IF NOT EXISTS doubles as the migration)
        _table_ddl("adjust_factors", ADJUST_FACTORS_SCHEMA, _ADJUST_FACTORS_KEY),
        *state_ddl("postgres"),
    ]
    return stmts


# --- connection plumbing -----------------------------------------------------------------


class _Conn:
    """Thin wrapper: ``?`` placeholders -> ``%s``; rows as dicts."""

    def __init__(self, raw: Any) -> None:
        self.raw = raw

    @staticmethod
    def _sql(sql: str) -> str:
        return sql.replace("?", "%s")

    def execute(self, sql: str, params: Sequence[Any] = ()) -> Any:
        return self.raw.execute(self._sql(sql), list(params))

    def executemany(self, sql: str, seq: Iterable[Sequence[Any]]) -> None:
        with self.raw.cursor() as cur:
            cur.executemany(self._sql(sql), [list(p) for p in seq])


ConnectionFactory = Callable[[], Any]


def psycopg_factory(dsn: str, *, read_only: bool, schema: str | None = None) -> ConnectionFactory:
    """Default factory (requires ``pip install openbars[postgres]``)."""

    def make() -> Any:
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as exc:  # pragma: no cover
            raise StorageError("psycopg is required for the Postgres backend: pip install 'openbars[postgres]'") from exc
        conn = psycopg.connect(dsn, row_factory=dict_row)
        if read_only:
            conn.read_only = True
        if schema:
            conn.execute(f"SET search_path TO {qident(schema)}")
        return conn

    return make


class _Session:
    def __init__(self, factory: ConnectionFactory) -> None:
        self._factory = factory

    @contextmanager
    def __call__(self) -> Iterator[_Conn]:
        raw = self._factory()
        try:
            yield _Conn(raw)
            raw.commit()
        except BaseException:
            raw.rollback()
            raise
        finally:
            raw.close()


class _Guard:
    def __init__(self, write: bool) -> None:
        self.write = write

    def require_write(self) -> None:
        if not self.write:
            raise ReadOnlyError("Postgres backend is read-only; open it with write=True")


def _fix(record: Mapping[str, Any]) -> dict[str, Any]:
    """DB record -> row kwargs (JSONB comes back parsed; lists stay lists)."""
    return dict(record)


# --- native stores -----------------------------------------------------------------------


def _upsert_sql(table: str, schema: pa.Schema, key_target: str, predicate: str, key_cols: Sequence[str]) -> str:
    cols = [f.name for f in schema]
    updates = [c for c in cols if c not in key_cols]
    return (
        f"INSERT INTO {qident(table)} ({', '.join(qident(c) for c in cols)}) "
        f"VALUES ({', '.join('?' for _ in cols)}) "
        f"ON CONFLICT ({key_target}) WHERE {predicate} DO UPDATE SET "
        + ", ".join(f"{qident(c)} = EXCLUDED.{qident(c)}" for c in updates)
    )


def _raw_upsert_sql(time_col: str, daily: bool) -> str:
    """Like ``_upsert_sql`` for ``raw_bars``, plus the additive JSONB ``extra`` column."""
    cols = [f.name for f in RAW_BARS_SCHEMA] + ["extra"]
    key_target = ", ".join([*(qident(c) for c in _RAW_KEY_COLS), _RAW_ENTITY, time_col])
    predicate = ("\"interval\" = '1d'" if daily else "\"interval\" <> '1d'")
    key_cols = (*_RAW_KEY_COLS, time_col)
    updates = [c for c in cols if c not in key_cols]
    return (
        f"INSERT INTO {qident('raw_bars')} ({', '.join(qident(c) for c in cols)}) "
        f"VALUES ({', '.join('?' for _ in cols)}) "
        f"ON CONFLICT ({key_target}) WHERE {predicate} DO UPDATE SET "
        + ", ".join(f"{qident(c)} = EXCLUDED.{qident(c)}" for c in updates)
    )


def _extra_param(row: RawBarRow) -> Any:
    """``encode_extra`` JSON object for a JSONB parameter; ``None`` when there is no leftover."""
    text = encode_extra(row.extra)
    if text is None:
        return None
    from psycopg.types.json import Jsonb

    return Jsonb(json.loads(text))


def _write_raw_rows(conn: "_Conn", rows: Sequence[RawBarRow]) -> int:
    latest: dict[tuple, RawBarRow] = {}
    for row in rows:  # last occurrence in the batch wins (same rule as Parquet)
        latest[row.key()] = row

    def values(row: RawBarRow) -> list[Any]:
        return [*row_values(row, RAW_BARS_SCHEMA), _extra_param(row)]

    daily = [values(r) for r in latest.values() if r.interval is Interval.D1]
    intraday = [values(r) for r in latest.values() if r.interval is not Interval.D1]
    if daily:
        conn.executemany(_RAW_DAILY_SQL, daily)
    if intraday:
        conn.executemany(_RAW_INTRADAY_SQL, intraday)
    return len(daily) + len(intraday)


# ``status`` coverage aggregates are bounded by this ``statement_timeout`` (#146).
COVERAGE_TIMEOUT_MS = 15_000
_RAW_DAILY_SQL = _raw_upsert_sql("session_date", True)
_RAW_INTRADAY_SQL = _raw_upsert_sql("ts_start", False)
# #154: the same source bar (every ``SAME_SOURCE_BAR_COLUMNS`` value matches) keeps the earliest
# known ``available_at`` (``LEAST`` ignores NULL), like ``_PartitionedTable._carry_available_at``.
_SAME_SOURCE_BAR_SQL = " AND ".join(
    f'"canonical_bars".{qident(c)} IS NOT DISTINCT FROM EXCLUDED.{qident(c)}' for c in SAME_SOURCE_BAR_COLUMNS
)
_CANON_AVAILABLE_SET = (
    '"available_at" = EXCLUDED."available_at"',
    f'"available_at" = CASE WHEN {_SAME_SOURCE_BAR_SQL} '
    'THEN LEAST("canonical_bars"."available_at", EXCLUDED."available_at") '
    'ELSE EXCLUDED."available_at" END',
)


def _canon_upsert_sql(time_col: str, daily: bool) -> str:
    sql = _upsert_sql(
        "canonical_bars", CANONICAL_BARS_SCHEMA,
        ", ".join([*(qident(c) for c in _CANON_KEY_COLS), time_col]),
        "\"interval\" = '1d'" if daily else "\"interval\" <> '1d'", (*_CANON_KEY_COLS, time_col),
    )
    assert sql.count(_CANON_AVAILABLE_SET[0]) == 1
    return sql.replace(*_CANON_AVAILABLE_SET)


_CANON_DAILY_SQL = _canon_upsert_sql("session_date", True)
_CANON_INTRADAY_SQL = _canon_upsert_sql("ts_start", False)


def _write_rows(conn: _Conn, rows: Sequence[Any], schema: pa.Schema, daily_sql: str, intraday_sql: str) -> int:
    latest: dict[tuple, Any] = {}
    for row in rows:  # last occurrence in the batch wins (same rule as Parquet)
        latest[row.key()] = row
    daily = [row_values(r, schema) for r in latest.values() if r.interval is Interval.D1]
    intraday = [row_values(r, schema) for r in latest.values() if r.interval is not Interval.D1]
    if daily:
        conn.executemany(daily_sql, daily)
    if intraday:
        conn.executemany(intraday_sql, intraday)
    return len(daily) + len(intraday)


def _archive_canonical_versions(conn: _Conn, rows: Sequence[CanonicalBarRow]) -> None:
    """Copy a replaced canonical row to native PIT history before the current-row upsert."""
    cols = [f.name for f in CANONICAL_BARS_SCHEMA]
    insert = (
        'INSERT INTO "canonical_bars_history" (' + ", ".join(qident(c) for c in cols)
        + ") VALUES (" + ", ".join("?" for _ in cols) + ")"
    )
    material = [c for c in cols if c not in {"generated_at", "available_at"}]
    for row in rows:
        time_col = "session_date" if row.interval is Interval.D1 else "ts_start"
        existing = conn.execute(
            f'SELECT * FROM "canonical_bars" WHERE instrument_id = ? AND "interval" = ? '
            f'AND policy_version = ? AND {qident(time_col)} = ?',
            [row.instrument_id, row.interval.value, row.policy_version,
             row.session_date if time_col == "session_date" else row.ts_start],
        ).fetchone()
        if existing is not None and any(existing.get(c) != value for c, value in zip(cols, row_values(row, CANONICAL_BARS_SCHEMA)) if c in material):
            conn.execute(insert, [existing.get(c) for c in cols])


class PgRawStore(_Guard):
    def __init__(self, session: _Session, write: bool) -> None:
        super().__init__(write)
        self._session = session

    def append(
        self,
        batch: RawBatch,
        *,
        source_artifact_ref: str | None = None,
        ingest_run_id: str | None = None,
        validate: bool = True,
    ) -> int:
        self.require_write()
        rows = raw_rows_from_batch(batch, source_artifact_ref=source_artifact_ref, ingest_run_id=ingest_run_id)
        return self.append_rows(rows, validate=validate)

    def append_rows(self, rows: Sequence[RawBarRow], *, validate: bool = True) -> int:
        self.require_write()
        if validate:
            check_rows(rows, "raw")
        if not rows:
            return 0
        with self._session() as conn:
            return _write_raw_rows(conn, rows)

    def scan_rows(
        self,
        provider: str | None,
        market: Market,
        interval: Interval,
        window: DateRange,
        *,
        symbols: Sequence[str] | None = None,
        instrument_ids: Sequence[str] | None = None,
    ) -> list[RawBarRow]:
        sql = 'SELECT * FROM raw_bars WHERE market = ? AND "interval" = ? AND session_date BETWEEN ? AND ?'
        params: list[Any] = [Market(market).value, Interval(interval).value, window.start, window.end]
        if provider is not None:
            sql += " AND provider = ?"
            params.append(provider)
        for column, values in (("symbol_as_returned", symbols), ("instrument_id", instrument_ids)):
            if values is not None:
                sql += f" AND {column} = ANY(?)"
                params.append(list(values))
        sql += " ORDER BY session_date, ts_start, coalesce(instrument_id, symbol_as_returned), provider, feed, adjustment, revision"
        with self._session() as conn:
            records = conn.execute(sql, params).fetchall()
        return normalize_legacy_daily(records_to_rows((_fix(r) for r in records), RawBarRow))  # #149

    def scan(self, provider: str, market: Market, interval: Interval, window: DateRange) -> Iterator[RawBar]:
        for row in self.scan_rows(provider, market, interval, window):
            yield row.to_raw_bar()

    def coverage_summary(
        self, provider: str, market: Market, interval: Interval, *, timeout_ms: int = COVERAGE_TIMEOUT_MS
    ) -> tuple[date | None, date | None, int, int]:
        """``(first, last, sessions, bars)`` like the Parquet store (#146).

        One aggregate on the ``ix_raw_bars_scan`` prefix ``(provider, market, interval)``; rows
        are unique per key in native mode, so no dedup pass. Bounded by ``statement_timeout``.
        """
        sql = (
            'SELECT min(session_date) AS first, max(session_date) AS last, '
            'count(DISTINCT session_date) AS sessions, count(*) AS bars FROM raw_bars '
            'WHERE provider = ? AND market = ? AND "interval" = ?'
        )
        return _coverage(self._session, sql, [provider, Market(market).value, Interval(interval).value], timeout_ms)



def _coverage(session: _Session, sql: str, params: list[Any], timeout_ms: int) -> tuple[date | None, date | None, int, int]:
    try:
        with session() as conn:
            conn.execute(f"SET LOCAL statement_timeout = {int(timeout_ms)}")
            record = conn.execute(sql, params).fetchone()
    except Exception as exc:  # psycopg.errors.QueryCanceled without importing psycopg here
        if type(exc).__name__ == "QueryCanceled":
            raise CoverageTimeout(f"coverage summary exceeded {int(timeout_ms) // 1000}s") from None
        raise
    if not record or not record["bars"]:
        return None, None, 0, 0
    return record["first"], record["last"], int(record["sessions"]), int(record["bars"])


class PgCanonicalStore(_Guard):
    def __init__(self, session: _Session, write: bool, market_of: Callable[[str], Market]) -> None:
        super().__init__(write)
        self._session = session
        self._market_of = market_of

    def upsert(
        self,
        bars: Sequence[CanonicalBar],
        *,
        policy_version: str = DEFAULT_POLICY_VERSION,
        selection_reason: str = "unspecified",
        capabilities: Sequence[str] = (),
        market: Market | None = None,
    ) -> int:
        now = datetime.now(UTC)
        rows = [
            CanonicalBarRow.from_canonical(
                cb,
                market=market or self._market_of(cb.bar.instrument_id),
                policy_version=policy_version,
                selection_reason=selection_reason,
                capabilities=capabilities,
                generated_at=now,
            )
            for cb in bars
        ]
        return self.upsert_rows(rows)

    def upsert_rows(self, rows: Sequence[CanonicalBarRow]) -> int:
        self.require_write()
        check_rows(rows, "canonical")
        if not rows:
            return 0
        with self._session() as conn:
            _archive_canonical_versions(conn, rows)
            return _write_rows(conn, rows, CANONICAL_BARS_SCHEMA, _CANON_DAILY_SQL, _CANON_INTRADAY_SQL)

    def scan_rows(
        self,
        instrument_ids: Sequence[str] | None,
        interval: Interval,
        window: DateRange,
        *,
        market: Market | None = None,
        policy_version: str | None = DEFAULT_POLICY_VERSION,
        available_by: datetime | None = None,
    ) -> list[CanonicalBarRow]:
        table = 'canonical_bars'
        if available_by is not None:
            available_by = available_by_utc(available_by)
            with self._session() as conn:
                history_exists = conn.execute(
                    "SELECT to_regclass('canonical_bars_history') AS name"
                ).fetchone()["name"] is not None
            if history_exists:
                cols = ", ".join(qident(f.name) for f in CANONICAL_BARS_SCHEMA)
                table = (
                    f'(SELECT {cols} FROM (SELECT {cols}, row_number() OVER (PARTITION BY instrument_id, '
                    '"interval", policy_version, CASE WHEN "interval" = \'1d\' THEN session_date END, '
                    'CASE WHEN "interval" <> \'1d\' THEN ts_start END ORDER BY is_current DESC, '
                    'history_id DESC NULLS LAST) AS _rn FROM (SELECT ' + cols + ', history_id, false AS is_current FROM canonical_bars_history '
                    'UNION ALL SELECT ' + cols + ', NULL AS history_id, true AS is_current FROM canonical_bars) h WHERE available_at <= ?) ranked '
                    'WHERE _rn = 1) AS visible'
                )
        sql = f'SELECT * FROM {table} WHERE "interval" = ? AND session_date BETWEEN ? AND ?'
        params: list[Any] = ([available_by] if available_by is not None and history_exists else []) + [Interval(interval).value, window.start, window.end]
        if available_by is not None and not history_exists:
            # Legacy native schemas can only filter the current row, not replay revisions.
            sql += " AND available_at <= ?"
            params.append(available_by)
        if market is not None:
            sql += " AND market = ?"
            params.append(Market(market).value)
        if policy_version is not None:
            sql += " AND policy_version = ?"
            params.append(policy_version)
        if instrument_ids is not None:
            sql += " AND instrument_id = ANY(?)"
            params.append(list(instrument_ids))
        sql += " ORDER BY session_date, ts_start, instrument_id, policy_version"
        with self._session() as conn:
            records = conn.execute(sql, params).fetchall()
        return records_to_rows((_fix(r) for r in records), CanonicalBarRow)

    def scan(self, instrument_ids: Sequence[str], interval: Interval, window: DateRange) -> Iterator[CanonicalBar]:
        for row in self.scan_rows(instrument_ids, interval, window):
            yield row.to_canonical_bar()


def _round_action_value(value: Any) -> Any:
    """Same single rounding as the Parquet master: more than 8 fractional digits -> half-even."""
    if isinstance(value, Decimal) and value.is_finite():
        return value.quantize(Decimal(1).scaleb(-8), rounding=ROUND_HALF_EVEN, context=_ACTION_CTX)
    return value


_ACTION_CTX = Context(prec=60)


def _instrument_from_record(r: Mapping[str, Any]) -> Instrument:
    ext = r["external_ids"]
    return Instrument(
        r["instrument_id"], Market(r["market"]), AssetClass(r["asset_class"]), r["currency"],
        r["name"], r["exchange"], r["listed_on"], r["delisted_on"],
        json.loads(ext) if isinstance(ext, str) else dict(ext or {}),
    )


class PgInstrumentMaster(_ResolveMixin, _Guard):
    """Native ``instruments`` / ``symbol_mappings`` / ``corporate_actions`` plus the identity
    side tables (``unresolved``, ``symbol_observations``, ``upstream_ids``): the full
    ``InstrumentStore``. Resolution reuses ``resolve_symbol`` (windows, provider precedence,
    ambiguity, unique-history fallback), so it answers exactly like the Parquet master;
    only the rows of the requested symbol are read."""

    def __init__(self, session: _Session, write: bool) -> None:
        super().__init__(write)  # type: ignore[call-arg]
        self._session = session

    def upsert(self, instruments: Iterable[Instrument], mappings: Iterable[SymbolMapping]) -> None:
        self.require_write()
        inst_rows = [
            [
                i.instrument_id, Market(i.market).value, AssetClass(i.asset_class).value, i.currency,
                i.name, i.exchange, i.listed_on, i.delisted_on, json.dumps(dict(i.external_ids), sort_keys=True),
            ]
            for i in instruments
        ]
        map_rows = [[m.instrument_id, m.provider, m.symbol, m.valid_from, m.valid_to] for m in mappings]
        with self._session() as conn:
            if inst_rows:
                conn.executemany(
                    "INSERT INTO instruments (instrument_id, market, asset_class, currency, name, exchange, "
                    "listed_on, delisted_on, external_ids) VALUES (?,?,?,?,?,?,?,?,?) "
                    "ON CONFLICT (instrument_id) DO UPDATE SET market=EXCLUDED.market, "
                    "asset_class=EXCLUDED.asset_class, currency=EXCLUDED.currency, name=EXCLUDED.name, "
                    "exchange=EXCLUDED.exchange, listed_on=EXCLUDED.listed_on, "
                    "delisted_on=EXCLUDED.delisted_on, external_ids=EXCLUDED.external_ids",
                    inst_rows,
                )
            if map_rows:
                conn.executemany(
                    "INSERT INTO symbol_mappings (instrument_id, provider, symbol, valid_from, valid_to) "
                    "VALUES (?,?,?,?,?) ON CONFLICT (instrument_id, provider, symbol, valid_from) "
                    "DO UPDATE SET valid_to=EXCLUDED.valid_to",
                    map_rows,
                )

    def remove_mappings(self, mappings: Iterable[SymbolMapping]) -> None:
        self.require_write()
        rows = [[m.instrument_id, m.provider, m.symbol, m.valid_from] for m in mappings]
        if not rows:
            return
        with self._session() as conn:
            conn.executemany(
                "DELETE FROM symbol_mappings WHERE instrument_id = ? AND provider = ? AND symbol = ? "
                "AND valid_from = ?",
                rows,
            )

    def upsert_corporate_actions(self, actions: Iterable[CorporateAction]) -> None:
        """Plain write of already-mapped ids (ingest paths call ``ingest_corporate_actions``)."""
        self.require_write()
        rows = [
            [
                a.instrument_id, a.ex_date, CorporateActionKind(a.kind).value, a.source,
                None if a.ratio is None else to_decimal(_round_action_value(a.ratio), precision=20, name="ratio"),
                None if a.amount is None else to_decimal(_round_action_value(a.amount), precision=20, name="amount"),
                a.currency,
            ]
            for a in actions
        ]
        if not rows:
            return
        with self._session() as conn:
            conn.executemany(
                "INSERT INTO corporate_actions (instrument_id, ex_date, kind, source, ratio, amount, currency) "
                "VALUES (?,?,?,?,?,?,?) ON CONFLICT (instrument_id, ex_date, kind, source) "
                "DO UPDATE SET ratio=EXCLUDED.ratio, amount=EXCLUDED.amount, currency=EXCLUDED.currency",
                rows,
            )

    def corporate_actions(self, instrument_ids: Sequence[str] | None = None) -> list[CorporateAction]:
        """Exact ``Decimal`` ratio/amount (``NUMERIC`` never goes through ``float``)."""
        sql, params = "SELECT * FROM corporate_actions", []
        if instrument_ids is not None:
            sql += " WHERE instrument_id = ANY(?)"
            params.append(list(instrument_ids))
        with self._session() as conn:
            records = conn.execute(sql + " ORDER BY instrument_id, ex_date, kind, source", params).fetchall()
        return [
            CorporateAction(
                r["instrument_id"], r["ex_date"], CorporateActionKind(r["kind"]), r["source"],
                r["ratio"], r["amount"], currency=r.get("currency"),
            )
            for r in records
        ]

    # -- A-share adjust factors (#124; a database without the table reads as empty) --

    def upsert_adjust_factors(self, factors: Iterable[Any]) -> int:
        """Upsert by ``(instrument_id, source, effective_from)`` (new wins), like the Parquet master.

        Returns the number of incoming rows. Factors are ``NUMERIC(20,8)``; more fractional
        digits are rounded half-even to 8 (the Parquet rule). Other sources' rows for the same
        instrument stay side by side.
        """
        self.require_write()
        latest: dict[tuple, list[Any]] = {}
        count = 0
        for f in factors:
            count += 1
            latest[(f.instrument_id, f.source, f.effective_from)] = [
                f.instrument_id, f.source, f.method, f.effective_from,
                *(to_decimal(_round_action_value(Decimal(getattr(f, name))), precision=20, name=name)
                  for name in ("fore_adjust_factor", "back_adjust_factor", "adjust_factor")),
                f.symbol,
            ]
        if not latest:
            return 0
        with self._session() as conn:
            conn.executemany(
                "INSERT INTO adjust_factors (instrument_id, source, method, effective_from, "
                "fore_adjust_factor, back_adjust_factor, adjust_factor, symbol) VALUES (?,?,?,?,?,?,?,?) "
                "ON CONFLICT (instrument_id, source, effective_from) DO UPDATE SET method=EXCLUDED.method, "
                "fore_adjust_factor=EXCLUDED.fore_adjust_factor, back_adjust_factor=EXCLUDED.back_adjust_factor, "
                "adjust_factor=EXCLUDED.adjust_factor, symbol=EXCLUDED.symbol",
                list(latest.values()),
            )
        return count

    def adjust_factors(
        self, instrument_ids: Sequence[str] | None = None, source: str | None = None
    ) -> list[Any]:
        """``AdjustFactor`` rows (exact ``Decimal``), ordered by the key; [] without the table."""
        from openbars.normalize.cn_adjust import AdjustFactor

        sql, params = "SELECT * FROM adjust_factors WHERE TRUE", []
        if instrument_ids is not None:
            sql += " AND instrument_id = ANY(?)"
            params.append(list(instrument_ids))
        if source is not None:
            sql += " AND source = ?"
            params.append(source)
        with self._session() as conn:
            if conn.execute("SELECT to_regclass('adjust_factors') AS t").fetchone()["t"] is None:
                return []  # pre-#124 database opened read-only
            records = conn.execute(sql + ' ORDER BY instrument_id COLLATE "C", source COLLATE "C", effective_from', params).fetchall()
        return [
            AdjustFactor(
                instrument_id=r["instrument_id"], effective_from=r["effective_from"], source=r["source"],
                method=r["method"], fore_adjust_factor=r["fore_adjust_factor"],
                back_adjust_factor=r["back_adjust_factor"], adjust_factor=r["adjust_factor"],
                symbol=r["symbol"],
            )
            for r in records
        ]

    # -- reads --

    def _resolution_view(self, symbol: str | None = None):
        with self._session() as conn:
            if symbol is None:
                maps = conn.execute("SELECT * FROM symbol_mappings").fetchall()
                insts = conn.execute("SELECT * FROM instruments").fetchall()
            else:
                maps = conn.execute("SELECT * FROM symbol_mappings WHERE symbol = ?", [symbol]).fetchall()
                ids = sorted({m["instrument_id"] for m in maps})
                insts = (
                    conn.execute("SELECT * FROM instruments WHERE instrument_id = ANY(?)", [ids]).fetchall()
                    if ids
                    else []
                )
        by_symbol: dict[str, list[SymbolMapping]] = {}
        for m in maps:
            by_symbol.setdefault(m["symbol"], []).append(_mapping_from_record(m))
        return {r["instrument_id"]: _instrument_from_record(r) for r in insts}, by_symbol

    def get(self, instrument_id: str) -> Instrument | None:
        with self._session() as conn:
            r = conn.execute("SELECT * FROM instruments WHERE instrument_id = ?", [instrument_id]).fetchone()
        return None if r is None else _instrument_from_record(r)

    def instruments(self) -> list[Instrument]:
        with self._session() as conn:
            records = conn.execute("SELECT * FROM instruments ORDER BY instrument_id").fetchall()
        return [_instrument_from_record(r) for r in records]

    def mappings(self, provider: str | None = None) -> list[SymbolMapping]:
        with self._session() as conn:
            records = conn.execute(
                "SELECT * FROM symbol_mappings ORDER BY symbol, valid_from, instrument_id, provider"
            ).fetchall()
        return select_mappings((_mapping_from_record(r) for r in records), provider)

    def resolve_any(self, item: str, *, on: date, market: Market | None = None) -> str:
        if self.get(item) is not None:
            return item
        for m in ([Market(market)] if market is not None else list(Market)):
            found = self.resolve(item, on=on, provider="*", market=m)
            if found is not None:
                return found
        return item

    # -- identity layer: unresolved queue, sightings, upstream ids --

    def record_unresolved(self, records: Iterable[UnresolvedRecord]) -> None:
        self.require_write()
        rows = [
            [
                r.kind, r.reason_code, r.symbol, r.provider, Market(r.market).value,
                r.session_date, r.detail,
            ]
            for r in records
        ]
        if not rows:
            return
        with self._session() as conn:
            conn.executemany(
                "INSERT INTO unresolved (kind, reason_code, symbol, provider, market, session_date, detail) "
                f"VALUES (?,?,?,?,?,?,?) ON CONFLICT ({_UNRESOLVED_KEY}) DO UPDATE SET "
                "reason_code=EXCLUDED.reason_code, session_date=EXCLUDED.session_date, detail=EXCLUDED.detail",
                rows,
            )

    def unresolved(self, kind: str | None = None) -> list[UnresolvedRecord]:
        sql, params = "SELECT * FROM unresolved", []
        if kind is not None:
            sql += " WHERE kind = ?"
            params.append(kind)
        with self._session() as conn:
            records = conn.execute(
                sql + " ORDER BY kind, provider, symbol, session_date, detail", params
            ).fetchall()
        return [
            UnresolvedRecord(
                r["kind"], r["reason_code"], r["symbol"], r["provider"], r["session_date"],
                Market(r["market"]), r["detail"],
            )
            for r in records
        ]

    def unresolved_count(self, kind: str | None = None) -> int:
        sql, params = "SELECT count(*) AS n FROM unresolved", []
        if kind is not None:
            sql += " WHERE kind = ?"
            params.append(kind)
        with self._session() as conn:
            return int(conn.execute(sql, params).fetchone()["n"])

    def discard_unresolved(self, records: Iterable[UnresolvedRecord]) -> None:
        self.require_write()
        rows = []
        for r in records:
            kind, provider, market, symbol, day, detail = r.key  # the dedupe key, as the index sees it
            rows.append([kind, provider, market, symbol, day or date.min, detail or ""])
        if not rows:
            return
        with self._session() as conn:
            conn.executemany(
                "DELETE FROM unresolved WHERE kind = ? AND provider = ? AND market = ? AND symbol = ? "
                f"AND {_UNRESOLVED_DAY} = ? AND {_UNRESOLVED_DETAIL} = ?",
                rows,
            )

    def observe(self, observations: Iterable[tuple[str, str, str, date]]) -> None:
        self.require_write()
        rows = [[i, s, p, on] for i, s, p, on in observations]
        if not rows:
            return
        with self._session() as conn:
            conn.executemany(
                "INSERT INTO symbol_observations (instrument_id, symbol, provider, last_observed) "
                "VALUES (?,?,?,?) ON CONFLICT (instrument_id, symbol, provider) DO UPDATE SET "
                "last_observed = GREATEST(symbol_observations.last_observed, EXCLUDED.last_observed)",
                rows,
            )

    def observations(self) -> list[tuple[str, str, str, date]]:
        with self._session() as conn:
            records = conn.execute(
                "SELECT * FROM symbol_observations ORDER BY instrument_id, symbol, provider"
            ).fetchall()
        return [(r["instrument_id"], r["symbol"], r["provider"], r["last_observed"]) for r in records]

    def set_upstream_id(self, instrument_id: str, upstream_id: int) -> None:
        self.require_write()
        with self._session() as conn:
            conn.execute("LOCK TABLE upstream_ids IN SHARE ROW EXCLUSIVE MODE")
            existing = {
                r["instrument_id"]: r["upstream_id"]
                for r in conn.execute("SELECT * FROM upstream_ids").fetchall()
            }
            _check_upstream(existing, instrument_id, upstream_id)
            conn.execute(
                "INSERT INTO upstream_ids (instrument_id, upstream_id) VALUES (?, ?) "
                "ON CONFLICT (instrument_id) DO UPDATE SET upstream_id = EXCLUDED.upstream_id",
                [instrument_id, int(upstream_id)],
            )

    def upstream_ids(self) -> dict[str, int]:
        with self._session() as conn:
            records = conn.execute("SELECT * FROM upstream_ids ORDER BY instrument_id").fetchall()
        return {r["instrument_id"]: int(r["upstream_id"]) for r in records}


def _mapping_from_record(r: Mapping[str, Any]) -> SymbolMapping:
    return SymbolMapping(r["instrument_id"], r["symbol"], r["valid_from"], r["valid_to"], r["provider"])


class PgStateStore(SqlStateStore, _Guard):
    """Native jobs / checkpoints / fetch_log (same columns as the SQLite tables)."""

    def __init__(self, session: _Session, write: bool) -> None:
        _Guard.__init__(self, write)
        self._session = session

    def _connect(self) -> Any:
        return self._session()

    def append_canonical_audit(self, events: Any, *, market: Any, interval: Any) -> None:
        self.require_write()
        super().append_canonical_audit(events, market=market, interval=interval)

    def save_job(self, job: Any) -> None:
        self.require_write()
        super().save_job(job)

    def save_checkpoint(self, checkpoint: Any) -> None:
        self.require_write()
        super().save_checkpoint(checkpoint)

    def start_fetch(self, **kwargs: Any) -> None:
        self.require_write()
        super().start_fetch(**kwargs)

    def finish_fetch(self, *args: Any, **kwargs: Any) -> None:
        self.require_write()
        super().finish_fetch(*args, **kwargs)

    def retry_jobs(self, **kwargs: Any) -> list[Any]:
        """#151: a real retry writes; ``dry_run`` only lists (allowed read-only)."""
        if not kwargs.get("dry_run"):
            self.require_write()
        return super().retry_jobs(**kwargs)


# --- upstream_compat (read/write mapping) -------------------------------------------------

_COMPAT_MARKET = Market.US
_FLAG_BY_STATUS = {"divergent": "divergent", "spike_suspect": "spike_suspect", "volume_missing": "volume_missing"}


def _midnight_utc(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, tzinfo=UTC)


def compat_canonical_row(r: Mapping[str, Any]) -> CanonicalBarRow:
    """``research_daily_bar`` record -> ``CanonicalBarRow``.

    Upstream has no ``ts_start`` / ``price_source`` / ``reconcile_rule`` / ``sources_seen``:
    ``ts_start`` is the session date at 00:00 UTC (placeholder), ``price_source`` and
    ``sources_seen`` come from ``provider``, ``reconcile_rule`` is the ``policy_version``.
    ``quality_status`` maps back to the matching quality flag.
    """
    caps = r["capabilities"]
    if isinstance(caps, str):
        caps = json.loads(caps)
    status = r["quality_status"]
    return CanonicalBarRow(
        instrument_id=str(r["instrument_id"]),
        market=_COMPAT_MARKET,
        interval=Interval.D1,
        ts_start=_midnight_utc(r["session_date"]),
        session_date=r["session_date"],
        session="regular",  # type: ignore[arg-type]
        provider=r["provider"],
        feed=r["feed"],
        adjustment=r["adjustment"],
        revision=r["revision"],
        open=r["open"], high=r["high"], low=r["low"], close=r["close"], volume=r["volume"],
        flags=(_FLAG_BY_STATUS[status].upper(),) if status in _FLAG_BY_STATUS else (),
        selection_reason=r["selection_reason"],
        quality_status=status,
        capabilities=tuple(caps or ()),
        source_fingerprint=r["source_fingerprint"],
        price_source=r["provider"],
        sources_seen=(r["provider"],),
        reconcile_rule=r["policy_version"],
        policy_version=r["policy_version"],
        generated_at=r["generated_at"],
        # #202: upstream rows carry no fetch id; the compat raw view uses ``compat:<row id>``
        # for the selected source row, so expose the same stable string here.
        source_fetch_id=(None if r.get("selected_source_bar_id") is None
                         else f"compat:{r['selected_source_bar_id']}"),
    )


def compat_raw_row(r: Mapping[str, Any]) -> RawBarRow:
    """``daily_bar`` (+ alias symbol) record -> ``RawBarRow``.

    Upstream keeps no fetch identity: ``fetch_id`` is ``compat:<row id>`` and
    ``symbol_as_returned`` is the alias valid on the session date (or the instrument id).
    """
    sid = str(r["instrument_id"])
    return RawBarRow(
        provider=r["provider"],
        market=_COMPAT_MARKET,
        symbol_as_returned=r.get("symbol") or sid,
        interval=Interval.D1,
        ts_start=_midnight_utc(r["session_date"]),
        session_date=r["session_date"],
        open=r["open"], high=r["high"], low=r["low"], close=r["close"], volume=r["volume"],
        fetch_id=f"compat:{r['id']}",
        feed=r["feed"],
        adjustment=r["adjustment"],
        instrument_id=sid,
        price_basis=r["price_basis"],
        adjustment_scope=r["adjustment_scope"],
        adjustment_as_of=r["adjustment_as_of"],
        session_semantics=r["session_semantics"],
        currency=r["currency"],
        volume_basis=r["volume_basis"],
        revision=r["revision"],
        vwap=r["vwap"],
        source_artifact_ref=None if r["source_artifact_id"] is None else f"source_artifact:{r['source_artifact_id']}",
        ingest_run_id=None if r["ingest_run_id"] is None else str(r["ingest_run_id"]),
        ingested_at=_midnight_utc(r["session_date"]),
    )


def _int_ids(ids: Sequence[str]) -> list[int]:
    return [int(i) for i in ids if str(i).lstrip("-").isdigit()]


def _require_us_daily(market: Market | str | None, interval: Interval | str, *, what: str) -> None:
    if Interval(interval) is not Interval.D1 or (market is not None and Market(market) is not _COMPAT_MARKET):
        raise StorageError(f"upstream_compat {what} only supports US daily bars")


def _require_volume(volume: Any, *, what: str) -> Any:
    if volume is None:
        raise StorageError(f"upstream_compat {what} requires a non-null volume")
    return volume


def _lookup_source_bar_id(
    conn: Any,
    instrument_id: int,
    session_date: date,
    provider: str,
    feed: str,
    adjustment: str,
    revision: int,
) -> int | None:
    rows = conn.execute(
        "SELECT id FROM daily_bar WHERE instrument_id = ? AND session_date = ? "
        "AND provider = ? AND feed = ? AND adjustment = ? AND revision = ?",
        [instrument_id, session_date, provider, feed, adjustment, revision],
    ).fetchall()
    if len(rows) == 1:
        return int(rows[0]["id"])
    return None


_DAILY_BAR_INSERT = (
    "INSERT INTO daily_bar ("
    "instrument_id, session_date, provider, feed, adjustment, "
    "price_basis, adjustment_scope, adjustment_as_of, session_semantics, "
    "currency, volume_basis, revision, "
    "open, high, low, close, volume, vwap, source_artifact_id, ingest_run_id"
    ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) RETURNING id"
)


def _ensure_source_bar_id(conn: Any, instrument_id: int, row: Any) -> int:
    """Return a ``daily_bar.id`` for ``selected_source_bar_id``.

    Reuses a unique match when raw has already been written. If none exists,
    materializes a ``daily_bar`` from the canonical OHLCV (unknown basis fields)
    so a canonical-only upsert still has a source id. Does not overwrite an
    existing raw row's values.
    """
    found = _lookup_source_bar_id(
        conn, instrument_id, row.session_date, row.provider, row.feed, row.adjustment, row.revision,
    )
    if found is not None:
        return found
    price_basis = getattr(row, "price_basis", "unknown") or "unknown"
    adjustment_scope = getattr(row, "adjustment_scope", "unknown") or "unknown"
    session_semantics = getattr(row, "session_semantics", "unknown") or "unknown"
    volume_basis = getattr(row, "volume_basis", "unknown") or "unknown"
    rec = conn.execute(
        _DAILY_BAR_INSERT,
        [
            instrument_id, row.session_date, row.provider, row.feed, row.adjustment,
            price_basis, adjustment_scope, getattr(row, "adjustment_as_of", None),
            session_semantics, getattr(row, "currency", None), volume_basis, row.revision,
            row.open, row.high, row.low, row.close, row.volume, getattr(row, "vwap", None),
            None, None,
        ],
    ).fetchone()
    if rec and rec.get("id") is not None:
        return int(rec["id"])
    found = _lookup_source_bar_id(
        conn, instrument_id, row.session_date, row.provider, row.feed, row.adjustment, row.revision,
    )
    if found is None:
        raise StorageError("failed to allocate selected_source_bar_id for a canonical bar")
    return found


def _bump_watermarks(conn: Any, policy_version: str, session_dates: set[date]) -> None:
    if not session_dates:
        return
    conn.execute(
        "INSERT INTO research_series_watermark (policy_version, version, updated_at) "
        "VALUES (?, 1, now()) "
        "ON CONFLICT (policy_version) DO UPDATE SET "
        "version = research_series_watermark.version + 1, updated_at = now()",
        [policy_version],
    )
    for session_date in sorted(session_dates):
        conn.execute(
            "INSERT INTO research_series_date_watermark (policy_version, session_date, version, updated_at) "
            "VALUES (?, ?, 1, now()) "
            "ON CONFLICT (policy_version, session_date) DO UPDATE SET "
            "version = research_series_date_watermark.version + 1, updated_at = now()",
            [policy_version, session_date],
        )


_DAILY_BAR_UPSERT = (
    "INSERT INTO daily_bar ("
    "instrument_id, session_date, provider, feed, adjustment, "
    "price_basis, adjustment_scope, adjustment_as_of, session_semantics, "
    "currency, volume_basis, revision, "
    "open, high, low, close, volume, vwap, source_artifact_id, ingest_run_id"
    ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
    "ON CONFLICT (instrument_id, session_date, provider, feed, adjustment, revision) DO UPDATE SET "
    "price_basis = EXCLUDED.price_basis, adjustment_scope = EXCLUDED.adjustment_scope, "
    "adjustment_as_of = EXCLUDED.adjustment_as_of, session_semantics = EXCLUDED.session_semantics, "
    "currency = EXCLUDED.currency, volume_basis = EXCLUDED.volume_basis, "
    "open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low, close = EXCLUDED.close, "
    "volume = EXCLUDED.volume, vwap = EXCLUDED.vwap, "
    "source_artifact_id = EXCLUDED.source_artifact_id, ingest_run_id = EXCLUDED.ingest_run_id "
    "RETURNING id"
)

_RESEARCH_BAR_UPSERT = (
    "INSERT INTO research_daily_bar ("
    "instrument_id, session_date, policy_version, selected_source_bar_id, "
    "provider, feed, adjustment, revision, "
    "open, high, low, close, volume, selection_reason, quality_status, "
    "capabilities, source_fingerprint, generated_at"
    ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
    "ON CONFLICT (instrument_id, session_date, policy_version) DO UPDATE SET "
    "selected_source_bar_id = EXCLUDED.selected_source_bar_id, "
    "provider = EXCLUDED.provider, feed = EXCLUDED.feed, adjustment = EXCLUDED.adjustment, "
    "revision = EXCLUDED.revision, open = EXCLUDED.open, high = EXCLUDED.high, "
    "low = EXCLUDED.low, close = EXCLUDED.close, volume = EXCLUDED.volume, "
    "selection_reason = EXCLUDED.selection_reason, quality_status = EXCLUDED.quality_status, "
    "capabilities = EXCLUDED.capabilities, source_fingerprint = EXCLUDED.source_fingerprint, "
    "generated_at = EXCLUDED.generated_at "
    "RETURNING id"
)


def _finish_skipped_ambiguous(skipped: list[AmbiguousInstrumentError], written: int, what: str) -> None:
    """Warn about rows skipped as ambiguous; re-raise the first one if nothing was written."""
    if not skipped:
        return
    if written == 0:
        raise skipped[0]
    symbols = sorted({e.symbol for e in skipped})
    dates = sorted({e.on.isoformat() for e in skipped})
    _LOG.warning(
        "%s skipped %d ambiguous row(s) (%d written): symbols=%s session_dates=%s",
        what, len(skipped), written, ",".join(symbols[:50]) + (",..." if len(symbols) > 50 else ""),
        ",".join(dates),
    )


class PgCompatRawStore(_Guard):
    """View of upstream ``daily_bar`` (US daily bars). Writes require ``write=True``.

    Leftover ``RawBar.extra`` is not stored: upstream owns this schema and this mode never
    runs migrations (#122). Native mode keeps extras in ``raw_bars.extra`` JSONB.
    """

    def __init__(self, session: _Session, write: bool, identity: CompatIdentity) -> None:
        super().__init__(write)
        self._session = session
        self._identity = identity

    def append(
        self,
        batch: RawBatch,
        *,
        source_artifact_ref: str | None = None,
        ingest_run_id: str | None = None,
        validate: bool = True,
    ) -> int:
        self.require_write()
        rows = raw_rows_from_batch(batch, source_artifact_ref=source_artifact_ref, ingest_run_id=ingest_run_id)
        return self.append_rows(rows, validate=validate)

    def append_rows(self, rows: Sequence[RawBarRow], *, validate: bool = True) -> int:
        self.require_write()
        if validate:
            check_rows(rows, "raw")
        if not rows:
            return 0
        skipped: list[AmbiguousInstrumentError] = []
        with self._session() as conn:
            written = 0
            for row in rows:
                _require_us_daily(row.market, row.interval, what="raw append")
                _require_volume(row.volume, what="raw append")
                try:
                    iid = self._identity.resolve(
                        conn,
                        row.instrument_id,
                        symbol=row.symbol_as_returned,
                        on=row.session_date,
                        exchange="UNKNOWN",
                    )
                except AmbiguousInstrumentError as exc:
                    skipped.append(exc)
                    continue
                artifact_id = parse_optional_int_ref(row.source_artifact_ref)
                run_id = parse_optional_int_ref(row.ingest_run_id)
                conn.execute(
                    _DAILY_BAR_UPSERT,
                    [
                        iid, row.session_date, row.provider, row.feed, row.adjustment,
                        row.price_basis, row.adjustment_scope, row.adjustment_as_of,
                        row.session_semantics, row.currency, row.volume_basis, row.revision,
                        row.open, row.high, row.low, row.close, row.volume, row.vwap,
                        artifact_id, run_id,
                    ],
                )
                written += 1
            _finish_skipped_ambiguous(skipped, written, "compat raw append")
            return written

    def scan_rows(self, provider: str | None, market: Market, interval: Interval, window: DateRange, *,
                  instrument_ids: Sequence[str] | None = None, **_: Any) -> list[RawBarRow]:
        if Market(market) is not _COMPAT_MARKET or Interval(interval) is not Interval.D1:
            return []
        sql = (
            "SELECT b.*, (SELECT a.symbol FROM symbol_alias a WHERE a.instrument_id = b.instrument_id "
            "AND a.valid_from <= b.session_date AND (a.valid_to IS NULL OR a.valid_to >= b.session_date) "
            "ORDER BY a.valid_from DESC LIMIT 1) AS symbol FROM daily_bar b "
            "WHERE b.session_date BETWEEN ? AND ?"
        )
        params: list[Any] = [window.start, window.end]
        if provider is not None:
            sql += " AND b.provider = ?"
            params.append(provider)
        if instrument_ids is not None:
            sql += " AND b.instrument_id = ANY(?)"
            params.append(_int_ids(instrument_ids))
        sql += " ORDER BY b.session_date, b.instrument_id, b.provider, b.feed, b.adjustment, b.revision"
        with self._session() as conn:
            records = conn.execute(sql, params).fetchall()
        return [compat_raw_row(r) for r in records]

    def scan(self, provider: str, market: Market, interval: Interval, window: DateRange) -> Iterator[RawBar]:
        for row in self.scan_rows(provider, market, interval, window):
            yield row.to_raw_bar()

    def coverage_summary(
        self, provider: str, market: Market, interval: Interval, *, timeout_ms: int = COVERAGE_TIMEOUT_MS
    ) -> tuple[date | None, date | None, int, int]:
        """Upstream ``daily_bar`` holds US daily only; other markets / intervals are empty (#146)."""
        if Market(market) is not _COMPAT_MARKET or Interval(interval) is not Interval.D1:
            return None, None, 0, 0
        sql = (
            "WITH visible AS ("
            "SELECT session_date, row_number() OVER ("
            "PARTITION BY instrument_id, session_date, provider, feed, adjustment "
            "ORDER BY revision DESC) AS revision_rank "
            "FROM daily_bar WHERE provider = ?"
            ") SELECT min(session_date) AS first, max(session_date) AS last, "
            "count(DISTINCT session_date) AS sessions, count(*) AS bars "
            "FROM visible WHERE revision_rank = 1"
        )
        return _coverage(self._session, sql, [provider], timeout_ms)


class PgCompatCanonicalStore(_Guard):
    """View of upstream ``research_daily_bar``. Writes require ``write=True``."""

    def __init__(self, session: _Session, write: bool, identity: CompatIdentity) -> None:
        super().__init__(write)
        self._session = session
        self._identity = identity

    def upsert(
        self,
        bars: Sequence[CanonicalBar],
        *,
        policy_version: str = DEFAULT_POLICY_VERSION,
        selection_reason: str = "unspecified",
        capabilities: Sequence[str] = (),
        market: Market | None = None,
    ) -> int:
        now = datetime.now(UTC)
        rows = [
            CanonicalBarRow.from_canonical(
                cb,
                market=market or _COMPAT_MARKET,
                policy_version=policy_version,
                selection_reason=selection_reason,
                capabilities=capabilities,
                generated_at=now,
            )
            for cb in bars
        ]
        return self.upsert_rows(rows)

    def upsert_rows(self, rows: Sequence[CanonicalBarRow]) -> int:
        self.require_write()
        check_rows(rows, "canonical")
        if not rows:
            return 0
        grouped: dict[tuple[str, date], list[CanonicalBarRow]] = {}
        for row in rows:
            _require_us_daily(row.market, row.interval, what="canonical upsert")
            _require_volume(row.volume, what="canonical upsert")
            fingerprint_or_raise(row)
            grouped.setdefault((row.policy_version, row.session_date), []).append(row)
        written = 0
        skipped: list[AmbiguousInstrumentError] = []
        with self._session() as conn:
            for policy_version, session_date in sorted(grouped):
                conn.execute(
                    "SELECT pg_advisory_xact_lock(?)",
                    [advisory_lock_key(policy_version, session_date)],
                )
                group_written = 0
                for row in grouped[(policy_version, session_date)]:
                    symbol = row.instrument_id if parse_int_id(row.instrument_id) is None else None
                    try:
                        iid = self._identity.resolve(
                            conn, row.instrument_id, symbol=symbol, on=row.session_date,
                        )
                    except AmbiguousInstrumentError as exc:
                        skipped.append(exc)
                        continue
                    source_id = _ensure_source_bar_id(conn, iid, row)
                    conn.execute(
                        _RESEARCH_BAR_UPSERT,
                        [
                            iid, row.session_date, row.policy_version, source_id,
                            row.provider, row.feed, row.adjustment, row.revision,
                            row.open, row.high, row.low, row.close, row.volume,
                            row.selection_reason, quality_status_of(row),
                            capabilities_json(row), fingerprint_or_raise(row), row.generated_at,
                        ],
                    )
                    written += 1
                    group_written += 1
                if group_written:
                    _bump_watermarks(conn, policy_version, {session_date})
            _finish_skipped_ambiguous(skipped, written, "compat canonical upsert")
        return written

    def scan_rows(self, instrument_ids: Sequence[str] | None, interval: Interval, window: DateRange, *,
                  market: Market | None = None, policy_version: str | None = DEFAULT_POLICY_VERSION) -> list[CanonicalBarRow]:
        if Interval(interval) is not Interval.D1 or (market is not None and Market(market) is not _COMPAT_MARKET):
            return []
        sql = "SELECT * FROM research_daily_bar WHERE session_date BETWEEN ? AND ?"
        params: list[Any] = [window.start, window.end]
        if policy_version is not None:
            sql += " AND policy_version = ?"
            params.append(policy_version)
        if instrument_ids is not None:
            sql += " AND instrument_id = ANY(?)"
            params.append(_int_ids(instrument_ids))
        sql += " ORDER BY session_date, instrument_id, policy_version"
        with self._session() as conn:
            records = conn.execute(sql, params).fetchall()
        return [compat_canonical_row(r) for r in records]

    def scan(self, instrument_ids: Sequence[str], interval: Interval, window: DateRange) -> Iterator[CanonicalBar]:
        for row in self.scan_rows(instrument_ids, interval, window):
            yield row.to_canonical_bar()



def _same_compat_revision(row: Mapping[str, Any], action_type: str, ex_date: date, iid: int, details: Mapping[str, Any]) -> bool:
    ids = row["instrument_ids"]
    if isinstance(ids, str):
        ids = json.loads(ids)
    got = row["details"]
    if isinstance(got, str):
        got = json.loads(got)
    return (
        row["action_type"] == action_type
        and row["ex_date"] == ex_date
        and list(ids) == [iid]
        and dict(got or {}) == dict(details)
    )


_LOG = logging.getLogger("openbars.storage.compat")


def _compat_asset_class(value: object) -> AssetClass:
    token = str(value or "").strip().lower()
    for item in AssetClass:
        if item.value == token:
            return item
    return AssetClass.EQUITY


class PgCompatInstrumentMaster(_Guard):
    """Point-in-time symbol lookup over upstream ``symbol_alias`` (US only, provider-agnostic).

    Identity extras that upstream does not have (unresolved queue, sightings, explicit
    upstream-id links) live in a local SQLite file; the constructor never issues DDL
    against Postgres.
    """

    def __init__(
        self, session: _Session, write: bool, identity: CompatIdentity, *, side_path: str | Path
    ) -> None:
        super().__init__(write)
        self._session = session
        self._identity = identity
        self._side = CompatSideStore(side_path)

    def upsert(self, instruments: Iterable[Instrument], mappings: Iterable[SymbolMapping]) -> None:
        self.require_write()
        inst_list = list(instruments)
        map_list = list(mappings)
        inst_by_id = {i.instrument_id: i for i in inst_list}
        with self._session() as conn:
            for inst in inst_list:
                on = inst.listed_on or date(1970, 1, 1)
                maps = [m for m in map_list if m.instrument_id == inst.instrument_id]
                symbol = maps[0].symbol if maps else (
                    None if parse_int_id(inst.instrument_id) is not None else inst.instrument_id
                )
                if symbol is None and inst.listed_on is None and not maps:
                    on = date(1970, 1, 1)
                self._identity.resolve(
                    conn,
                    inst.instrument_id,
                    symbol=symbol,
                    on=on,
                    asset_class=AssetClass(inst.asset_class).value,
                    security_type=security_type_for(inst.asset_class),
                    exchange=inst.exchange or "UNKNOWN",
                )
            for mapping in map_list:
                inst = inst_by_id.get(mapping.instrument_id)
                exchange = (inst.exchange if inst is not None else None) or "UNKNOWN"
                asset_class = AssetClass(inst.asset_class).value if inst is not None else "equity"
                security = security_type_for(inst.asset_class) if inst is not None else "common_stock"
                iid = self._identity.resolve(
                    conn,
                    mapping.instrument_id,
                    symbol=mapping.symbol,
                    on=mapping.valid_from,
                    asset_class=asset_class,
                    security_type=security,
                    exchange=exchange,
                )
                if mapping.valid_to is not None:
                    conn.execute(
                        "UPDATE symbol_alias SET valid_to = ? "
                        "WHERE instrument_id = ? AND symbol = ? AND valid_from = ? "
                        "AND (valid_to IS NULL OR valid_to = ?)",
                        [mapping.valid_to, iid, mapping.symbol, mapping.valid_from, mapping.valid_to],
                    )


    def upsert_corporate_actions(self, actions: Iterable[CorporateAction]) -> None:
        """Map openbars corporate actions onto the upstream observation / event / revision tables."""
        self.require_write()
        items = list(actions)
        if not items:
            return
        with self._session() as conn:
            for action in items:
                kind = CorporateActionKind(action.kind)
                typ = action_type_for(kind, ratio=action.ratio, amount=action.amount)
                symbol = None if parse_int_id(action.instrument_id) is not None else action.instrument_id
                iid = self._identity.resolve(
                    conn, action.instrument_id, symbol=symbol, on=action.ex_date,
                )
                details = action_details(
                    kind, ratio=action.ratio, amount=action.amount, currency=action.currency
                )
                event_key = action_event_key(typ, iid, action.ex_date)
                payload = {
                    "instrument_id": iid,
                    "ex_date": action.ex_date.isoformat(),
                    "kind": typ,
                    "source": action.source,
                    "ratio": None if action.ratio is None else str(action.ratio),
                    "amount": None if action.amount is None else str(action.amount),
                    "currency": action.currency,
                    "details": details,
                }
                payload_hash = hashlib.sha256(
                    json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
                ).hexdigest()
                existing_obs = conn.execute(
                    "SELECT id FROM corporate_action_observation "
                    "WHERE provider = ? AND dataset = ? AND payload_hash = ?",
                    [action.source, "openbars", payload_hash],
                ).fetchone()
                if existing_obs is None:
                    existing_obs = conn.execute(
                        "INSERT INTO corporate_action_observation ("
                        "provider, dataset, external_id, event_type, payload_hash, payload, "
                        "normalization_status, observed_at) "
                        "VALUES (?, ?, ?, ?, ?, ?, 'normalized', now()) RETURNING id",
                        [
                            action.source, "openbars", event_key, typ, payload_hash,
                            json.dumps(payload),
                        ],
                    ).fetchone()
                obs_id = int(existing_obs["id"])
                ev = conn.execute(
                    "SELECT id FROM corporate_action_event WHERE event_key = ?", [event_key]
                ).fetchone()
                if ev is None:
                    ev = conn.execute(
                        "INSERT INTO corporate_action_event (event_key) VALUES (?) RETURNING id",
                        [event_key],
                    ).fetchone()
                event_id = int(ev["id"])
                previous = conn.execute(
                    "SELECT * FROM corporate_action_revision WHERE event_id = ? "
                    "ORDER BY revision DESC LIMIT 1",
                    [event_id],
                ).fetchone()
                if previous is not None and _same_compat_revision(previous, typ, action.ex_date, iid, details):
                    continue
                next_rev = (int(previous["revision"]) + 1) if previous else 1
                supersedes = previous["id"] if previous is not None else None
                created = conn.execute(
                    "INSERT INTO corporate_action_revision ("
                    "event_id, revision, action_type, verification_status, valid_from, known_at, "
                    "ex_date, instrument_ids, observation_ids, details, actor, supersedes_id) "
                    "VALUES (?, ?, ?, 'source_reported', ?, now(), ?, ?, ?, ?, 'openbars', ?) "
                    "RETURNING id",
                    [
                        event_id, next_rev, typ, action.ex_date, action.ex_date,
                        json.dumps([iid]), json.dumps([obs_id]), json.dumps(details), supersedes,
                    ],
                ).fetchone()
                conn.execute(
                    "INSERT INTO corporate_action_observation_link (revision_id, observation_id) "
                    "VALUES (?, ?) ON CONFLICT DO NOTHING",
                    [int(created["id"]), obs_id],
                )

    def resolve(self, symbol: str, *, on: date, provider: str, market: Market) -> str | None:
        if Market(market) is not _COMPAT_MARKET:
            return None
        with self._session() as conn:
            records = conn.execute(
                "SELECT DISTINCT instrument_id FROM symbol_alias WHERE symbol = ? AND valid_from <= ? "
                "AND (valid_to IS NULL OR valid_to >= ?)",
                [symbol, on, on],
            ).fetchall()
        ids = {str(r["instrument_id"]) for r in records}
        return next(iter(ids)) if len(ids) == 1 else None

    def mappings(self, provider: str | None = None) -> list[SymbolMapping]:
        """Every ``symbol_alias`` as a provider-agnostic ``SymbolMapping`` (``provider="*"``).

        ``provider`` keeps rows visible to that name (exact match or ``"*"``). Because
        upstream aliases have no provider column, all rows are ``"*"`` and remain
        visible under any filter.
        """
        with self._session() as conn:
            records = conn.execute(
                "SELECT instrument_id, symbol, valid_from, valid_to FROM symbol_alias "
                "ORDER BY symbol, valid_from, instrument_id"
            ).fetchall()
        rows = [
            SymbolMapping(
                instrument_id=str(r["instrument_id"]),
                symbol=str(r["symbol"]),
                valid_from=r["valid_from"],
                valid_to=r["valid_to"],
                provider="*",
            )
            for r in records
        ]
        return select_mappings(rows, provider)

    def get(self, instrument_id: str) -> Instrument | None:
        numeric = parse_int_id(instrument_id)
        if numeric is None:
            return None
        with self._session() as conn:
            r = conn.execute("SELECT * FROM instrument WHERE id = ?", [numeric]).fetchone()
        if r is None:
            return None
        listed, delisted, exchange = self._listing_bounds().get(str(numeric), (None, None, None))
        return self._instrument_from_row(r, listed_on=listed, delisted_on=delisted, exchange=exchange)

    def instruments(self) -> list[Instrument]:
        bounds = self._listing_bounds()
        with self._session() as conn:
            records = conn.execute("SELECT * FROM instrument ORDER BY id").fetchall()
        out = []
        for r in records:
            listed, delisted, exchange = bounds.get(str(r["id"]), (None, None, None))
            out.append(self._instrument_from_row(r, listed_on=listed, delisted_on=delisted, exchange=exchange))
        return out

    def _listing_bounds(self) -> dict[str, tuple[date | None, date | None, str | None]]:
        with self._session() as conn:
            records = conn.execute(
                "SELECT instrument_id, min(valid_from) AS listed_on, "
                "CASE WHEN count(*) FILTER (WHERE valid_to IS NULL) = 0 THEN max(valid_to) END AS delisted_on, "
                "(array_agg(exchange ORDER BY valid_from DESC))[1] AS exchange "
                "FROM symbol_alias GROUP BY instrument_id"
            ).fetchall()
        return {
            str(r["instrument_id"]): (r["listed_on"], r["delisted_on"], r["exchange"])
            for r in records
        }

    def _instrument_from_row(
        self, r: Any, *, listed_on: date | None, delisted_on: date | None, exchange: str | None
    ) -> Instrument:
        iid = str(r["id"])
        primary = r["primary_exchange"] if "primary_exchange" in r.keys() else None
        return Instrument(
            instrument_id=iid,
            market=_COMPAT_MARKET,
            asset_class=_compat_asset_class(r["asset_class"]),
            currency="USD",
            exchange=exchange or primary,
            listed_on=listed_on,
            delisted_on=delisted_on,
            external_ids={"upstream_id": iid},
        )

    def remove_mappings(self, mappings: Iterable[SymbolMapping]) -> None:
        self.require_write()
        rows = [[parse_int_id(m.instrument_id) or m.instrument_id, m.symbol, m.valid_from] for m in mappings]
        if not rows:
            return
        with self._session() as conn:
            conn.executemany(
                "DELETE FROM symbol_alias WHERE instrument_id = ? AND symbol = ? AND valid_from = ?",
                rows,
            )

    def record_unresolved(self, records: Iterable[UnresolvedRecord]) -> None:
        self._side.record_unresolved(records)

    def unresolved(self, kind: str | None = None) -> list[UnresolvedRecord]:
        return self._side.unresolved(kind)

    def unresolved_count(self, kind: str | None = None) -> int:
        return self._side.unresolved_count(kind)

    def discard_unresolved(self, records: Iterable[UnresolvedRecord]) -> None:
        self._side.discard_unresolved(records)

    def observe(self, observations: Iterable[tuple[str, str, str, date]]) -> None:
        self._side.observe(observations)

    def observations(self) -> list[tuple[str, str, str, date]]:
        return self._side.observations()

    def set_upstream_id(self, instrument_id: str, upstream_id: int) -> None:
        numeric = parse_int_id(instrument_id)
        if numeric is not None and numeric != int(upstream_id):
            raise ValueError(
                f"{instrument_id} is already the upstream bigint; cannot relink to {upstream_id}"
            )
        existing = self.upstream_ids()
        _check_upstream(existing, instrument_id, upstream_id)
        self._side.set_upstream_id(instrument_id, upstream_id)

    def upstream_ids(self) -> dict[str, int]:
        with self._session() as conn:
            records = conn.execute("SELECT id FROM instrument").fetchall()
        out = {str(r["id"]): int(r["id"]) for r in records}
        out.update(self._side.upstream_ids())
        return out

    def corporate_actions(self, instrument_ids: Sequence[str] | None = None) -> list[CorporateAction]:
        """Latest revision of each upstream event, mapped back to openbars ``CorporateAction``.

        Unknown ``action_type`` values are not silently dropped: they are recorded in the
        local unresolved queue (``reason_code=unknown_action_type``) and a warning is emitted.
        """
        wanted = None if instrument_ids is None else {str(i) for i in instrument_ids}
        with self._session() as conn:
            records = conn.execute(
                "SELECT DISTINCT ON (r.event_id) "
                "r.action_type, r.ex_date, r.instrument_ids, r.details, "
                "o.provider AS source "
                "FROM corporate_action_revision r "
                "LEFT JOIN corporate_action_observation_link l ON l.revision_id = r.id "
                "LEFT JOIN corporate_action_observation o ON o.id = l.observation_id "
                "ORDER BY r.event_id, r.revision DESC, o.id"
            ).fetchall()
        out: list[CorporateAction] = []
        unknown: list[UnresolvedRecord] = []
        for r in records:
            typ = r["action_type"]
            mapped = actions_from_revision(
                action_type=typ,
                ex_date=r["ex_date"],
                instrument_ids=r["instrument_ids"],
                details=r["details"],
                source=r["source"],
            )
            if mapped:
                for action in mapped:
                    if wanted is None or action.instrument_id in wanted:
                        out.append(action)
                continue
            if kind_from_action_type(typ) is not None:
                continue
            raw_ids = r["instrument_ids"]
            if isinstance(raw_ids, str):
                import json as _json
                raw_ids = _json.loads(raw_ids)
            ids = [str(i) for i in (raw_ids or [])]
            if wanted is not None and ids and not (wanted & set(ids)):
                continue
            msg = (
                f"unknown corporate action type {typ!r} on {r['ex_date']} "
                f"instrument_ids={ids or raw_ids!r}; recorded as unresolved"
            )
            warnings.warn(msg, UserWarning, stacklevel=2)
            _LOG.warning(msg)
            unknown.append(
                UnresolvedRecord(
                    kind="corporate_action",
                    reason_code="unknown_action_type",
                    symbol=ids[0] if ids else "",
                    provider=(r["source"] or "unknown"),
                    session_date=r["ex_date"],
                    market=_COMPAT_MARKET,
                    detail=str(typ),
                )
            )
        if unknown:
            self._side.record_unresolved(unknown)
        out.sort(key=lambda a: (a.instrument_id, a.ex_date, a.kind.value, a.source))
        return out

# --- backend -----------------------------------------------------------------------------


class PostgresBackend:
    """``StorageBackend`` over Postgres. See the module docstring for the two modes.

    ``connect`` returns a new DB-API-ish connection per operation (default: psycopg via
    ``dsn``). ``state_path`` is only used by ``upstream_compat`` (local SQLite state).
    The schema check runs once, in the constructor.
    """

    def __init__(
        self,
        dsn: str | None = None,
        *,
        mode: PgMode | str = PgMode.NATIVE,
        write: bool = False,
        schema: str | None = None,
        connect: ConnectionFactory | None = None,
        state_path: str | Path | None = None,
    ) -> None:
        self.mode = PgMode(mode)
        self.write = write
        if connect is None:
            if dsn is None:
                raise StorageError("either dsn or connect must be given")
            connect = psycopg_factory(dsn, read_only=not write, schema=schema)
        self._session = _Session(connect)
        self.schema_version = SCHEMA_VERSION
        if self.mode is PgMode.UPSTREAM_COMPAT:
            if state_path is None:
                raise StorageError("upstream_compat needs state_path for the local SQLite state store")
            with self._session() as conn:
                check_upstream_compat(introspect_catalog(conn))  # never runs DDL
            identity = CompatIdentity()
            self.raw = PgCompatRawStore(self._session, write, identity)
            self.canonical = PgCompatCanonicalStore(self._session, write, identity)
            self.instruments = PgCompatInstrumentMaster(
                self._session, write, identity, side_path=Path(state_path).with_name("identity.db")
            )
            self.state = SqliteStateStore(state_path)
        else:
            if write:
                with self._session() as conn:
                    for statement in native_ddl():
                        conn.execute(statement)
                    conn.execute(
                        "INSERT INTO schema_meta (key, value) VALUES ('schema_version', ?) "
                        "ON CONFLICT (key) DO NOTHING",
                        [str(SCHEMA_VERSION)],
                    )
            with self._session() as conn:
                check_native(introspect_catalog(conn))
            self.instruments = PgInstrumentMaster(self._session, write)
            self.raw = PgRawStore(self._session, write)
            self.canonical = PgCanonicalStore(self._session, write, self.market_of)
            self.state = PgStateStore(self._session, write)
        self.reader = StoreBarReader(self)

    def market_of(self, instrument_id: str) -> Market:
        prefix = instrument_id.split(":", 1)[0]
        if prefix in {m.value for m in Market}:
            return Market(prefix)
        inst = self.instruments.get(instrument_id)  # type: ignore[union-attr]
        if inst is None:
            raise StorageError(f"cannot determine market for instrument {instrument_id!r}")
        return inst.market

    def close(self) -> None:
        """Connections are per-operation; nothing to release."""


def open_postgres_backend(dsn: str, **kwargs: Any) -> PostgresBackend:
    return PostgresBackend(dsn, **kwargs)
