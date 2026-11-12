"""Optional Postgres backend implementing the ``StorageBackend`` contract.

Two schema modes (docs/architecture.md section 11.3):

``native``
    openbars owns the DDL, which is generated from the same schema definitions as the
    Parquet layout (``schema.py``) - one table per Parquet table, same column names,
    ``NUMERIC(20,8)`` / ``NUMERIC(28,8)`` for prices / volumes, ``TEXT[]`` for list
    columns, ``JSONB`` for ``external_ids``. Tables are created (``CREATE ... IF NOT
    EXISTS``) only when ``write=True``; a read-only connection merely verifies them.

``upstream_compat``
    Tables belong to the upstream internal implementation. **No migration is ever run.**
    On connect the catalog is introspected and a structured ``UpstreamSchemaError``
    (``.problems``: missing table / missing column / type mismatch) is raised on any
    difference. Reads map upstream rows onto the openbars row types (see ``_compat_*``
    for the documented placeholders - upstream has no ``ts_start`` / ``fetch_id`` /
    ``price_source``). Writes are not implemented in this mode yet (shadow runs are
    read-only; see 11.4), so ``write=True`` is rejected. Jobs, checkpoints and
    fetch_log live in a local SQLite ``state_path`` because upstream owns no such tables.

Connections are short-lived and opened by ``connect`` (default: ``psycopg`` from the
``postgres`` extra). Read-only backends open read-only transactions. Everything that
needs a live server goes through the injected factory, so the introspection checker is
unit-testable with a fake catalog.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
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
from openbars.providers.base import RawBatch
from openbars.storage.errors import (
    ReadOnlyError,
    SchemaMismatchError,
    SchemaProblem,
    StorageError,
    UpstreamSchemaError,
)
from openbars.storage.rows import (
    CanonicalBarRow,
    RawBarRow,
    raw_rows_from_batch,
    records_to_rows,
    row_values,
    check_rows,
    to_decimal,
)
from openbars.storage.schema import (
    CANONICAL_BARS_SCHEMA,
    CORPORATE_ACTIONS_SCHEMA,
    DEFAULT_POLICY_VERSION,
    INSTRUMENTS_SCHEMA,
    RAW_BARS_SCHEMA,
    STATE_TABLES,
    SYMBOL_MAPPINGS_SCHEMA,
    SCHEMA_VERSION,
    postgres_type,
    qident,
    state_ddl,
)
from openbars.storage.state import SqliteStateStore, SqlStateStore


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


def check_native(catalog: Catalog) -> None:
    problems = check_schema(catalog, NATIVE_REQUIRED)
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
}
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


def _unique_index(name: str, table: str, cols: Sequence[str], extra: Sequence[str], time_col: str, daily: bool) -> str:
    key = ", ".join([*(qident(c) for c in cols), *extra, qident(time_col)])
    pred = "= '1d'" if daily else "<> '1d'"
    return f'CREATE UNIQUE INDEX IF NOT EXISTS {qident(name)} ON {qident(table)} ({key}) WHERE "interval" {pred}'


def native_ddl() -> list[str]:
    """DDL for every native table (idempotent). Same columns as the Parquet schemas."""
    stmts = [
        _table_ddl("raw_bars", RAW_BARS_SCHEMA),
        _unique_index("ux_raw_bars_daily", "raw_bars", _RAW_KEY_COLS, [_RAW_ENTITY], "session_date", True),
        _unique_index("ux_raw_bars_intraday", "raw_bars", _RAW_KEY_COLS, [_RAW_ENTITY], "ts_start", False),
        'CREATE INDEX IF NOT EXISTS "ix_raw_bars_scan" ON "raw_bars" ("provider", "market", "interval", "session_date")',
        _table_ddl("canonical_bars", CANONICAL_BARS_SCHEMA),
        _unique_index("ux_canonical_bars_daily", "canonical_bars", _CANON_KEY_COLS, [], "session_date", True),
        _unique_index("ux_canonical_bars_intraday", "canonical_bars", _CANON_KEY_COLS, [], "ts_start", False),
        'CREATE INDEX IF NOT EXISTS "ix_canonical_bars_scan" ON "canonical_bars" ("market", "interval", "policy_version", "session_date")',
        _table_ddl("instruments", INSTRUMENTS_SCHEMA, _REF_KEYS["instruments"]),
        _table_ddl("symbol_mappings", SYMBOL_MAPPINGS_SCHEMA, _REF_KEYS["symbol_mappings"]),
        'CREATE INDEX IF NOT EXISTS "ix_symbol_mappings_symbol" ON "symbol_mappings" ("symbol")',
        _table_ddl("corporate_actions", CORPORATE_ACTIONS_SCHEMA, _REF_KEYS["corporate_actions"]),
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


_RAW_DAILY_SQL = _upsert_sql(
    "raw_bars", RAW_BARS_SCHEMA,
    ", ".join([*(qident(c) for c in _RAW_KEY_COLS), _RAW_ENTITY, "session_date"]),
    "\"interval\" = '1d'", (*_RAW_KEY_COLS, "session_date"),
)
_RAW_INTRADAY_SQL = _upsert_sql(
    "raw_bars", RAW_BARS_SCHEMA,
    ", ".join([*(qident(c) for c in _RAW_KEY_COLS), _RAW_ENTITY, "ts_start"]),
    "\"interval\" <> '1d'", (*_RAW_KEY_COLS, "ts_start"),
)
_CANON_DAILY_SQL = _upsert_sql(
    "canonical_bars", CANONICAL_BARS_SCHEMA,
    ", ".join([*(qident(c) for c in _CANON_KEY_COLS), "session_date"]),
    "\"interval\" = '1d'", (*_CANON_KEY_COLS, "session_date"),
)
_CANON_INTRADAY_SQL = _upsert_sql(
    "canonical_bars", CANONICAL_BARS_SCHEMA,
    ", ".join([*(qident(c) for c in _CANON_KEY_COLS), "ts_start"]),
    "\"interval\" <> '1d'", (*_CANON_KEY_COLS, "ts_start"),
)


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
        rows = raw_rows_from_batch(batch, source_artifact_ref=source_artifact_ref, ingest_run_id=ingest_run_id)
        return self.append_rows(rows, validate=validate)

    def append_rows(self, rows: Sequence[RawBarRow], *, validate: bool = True) -> int:
        self.require_write()
        if validate:
            check_rows(rows, "raw")
        if not rows:
            return 0
        with self._session() as conn:
            return _write_rows(conn, rows, RAW_BARS_SCHEMA, _RAW_DAILY_SQL, _RAW_INTRADAY_SQL)

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
        return records_to_rows((_fix(r) for r in records), RawBarRow)

    def scan(self, provider: str, market: Market, interval: Interval, window: DateRange) -> Iterator[RawBar]:
        for row in self.scan_rows(provider, market, interval, window):
            yield row.to_raw_bar()


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
            return _write_rows(conn, rows, CANONICAL_BARS_SCHEMA, _CANON_DAILY_SQL, _CANON_INTRADAY_SQL)

    def scan_rows(
        self,
        instrument_ids: Sequence[str] | None,
        interval: Interval,
        window: DateRange,
        *,
        market: Market | None = None,
        policy_version: str | None = DEFAULT_POLICY_VERSION,
    ) -> list[CanonicalBarRow]:
        sql = 'SELECT * FROM canonical_bars WHERE "interval" = ? AND session_date BETWEEN ? AND ?'
        params: list[Any] = [Interval(interval).value, window.start, window.end]
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


def _precedence_pick(rows: Iterable[Mapping[str, Any]], provider: str) -> str | None:
    specific = {r["instrument_id"] for r in rows if r["provider"] == provider}
    generic = {r["instrument_id"] for r in rows if r["provider"] == "*" and r["provider"] != provider}
    for ids in (specific, generic):
        if ids:
            return next(iter(ids)) if len(ids) == 1 else None
    return None


class PgInstrumentMaster(_Guard):
    def __init__(self, session: _Session, write: bool) -> None:
        super().__init__(write)
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

    def upsert_corporate_actions(self, actions: Iterable[CorporateAction]) -> None:
        self.require_write()
        rows = [
            [
                a.instrument_id, a.ex_date, CorporateActionKind(a.kind).value, a.source,
                None if a.ratio is None else to_decimal(a.ratio, precision=20, name="ratio"),
                None if a.amount is None else to_decimal(a.amount, precision=20, name="amount"),
            ]
            for a in actions
        ]
        if not rows:
            return
        with self._session() as conn:
            conn.executemany(
                "INSERT INTO corporate_actions (instrument_id, ex_date, kind, source, ratio, amount) "
                "VALUES (?,?,?,?,?,?) ON CONFLICT (instrument_id, ex_date, kind, source) "
                "DO UPDATE SET ratio=EXCLUDED.ratio, amount=EXCLUDED.amount",
                rows,
            )

    def corporate_actions(self, instrument_ids: Sequence[str] | None = None) -> list[CorporateAction]:
        sql, params = "SELECT * FROM corporate_actions", []
        if instrument_ids is not None:
            sql += " WHERE instrument_id = ANY(?)"
            params.append(list(instrument_ids))
        with self._session() as conn:
            records = conn.execute(sql + " ORDER BY instrument_id, ex_date, kind, source", params).fetchall()
        return [
            CorporateAction(
                r["instrument_id"], r["ex_date"], CorporateActionKind(r["kind"]), r["source"],
                None if r["ratio"] is None else float(r["ratio"]),
                None if r["amount"] is None else float(r["amount"]),
            )
            for r in records
        ]

    def resolve(self, symbol: str, *, on: date, provider: str, market: Market) -> str | None:
        """Same rules as the Parquet master: inclusive ``valid_to``, provider beats ``*``."""
        with self._session() as conn:
            records = conn.execute(
                "SELECT m.instrument_id, m.provider FROM symbol_mappings m "
                "JOIN instruments i ON i.instrument_id = m.instrument_id "
                "WHERE m.symbol = ? AND m.valid_from <= ? AND (m.valid_to IS NULL OR m.valid_to >= ?) "
                "AND i.market = ? AND m.provider IN (?, '*')",
                [symbol, on, on, Market(market).value, provider],
            ).fetchall()
        return _precedence_pick(records, provider)

    def get(self, instrument_id: str) -> Instrument | None:
        with self._session() as conn:
            r = conn.execute("SELECT * FROM instruments WHERE instrument_id = ?", [instrument_id]).fetchone()
        if r is None:
            return None
        ext = r["external_ids"]
        return Instrument(
            r["instrument_id"], Market(r["market"]), AssetClass(r["asset_class"]), r["currency"],
            r["name"], r["exchange"], r["listed_on"], r["delisted_on"],
            json.loads(ext) if isinstance(ext, str) else dict(ext or {}),
        )

    def resolve_any(self, item: str, *, on: date, market: Market | None = None) -> str:
        if self.get(item) is not None:
            return item
        for m in ([Market(market)] if market is not None else list(Market)):
            found = self.resolve(item, on=on, provider="*", market=m)
            if found is not None:
                return found
        return item


class PgStateStore(SqlStateStore, _Guard):
    """Native jobs / checkpoints / fetch_log (same columns as the SQLite tables)."""

    def __init__(self, session: _Session, write: bool) -> None:
        _Guard.__init__(self, write)
        self._session = session

    def _connect(self) -> Any:
        return self._session()

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


# --- upstream_compat (read-only mapping) -------------------------------------------------

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


class PgCompatRawStore:
    """Read-only view of upstream ``daily_bar`` (US daily bars)."""

    def __init__(self, session: _Session) -> None:
        self._session = session

    def append(self, *a: Any, **k: Any) -> int:
        raise ReadOnlyError("upstream_compat does not support writes yet")

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


class PgCompatCanonicalStore:
    """Read-only view of upstream ``research_daily_bar``."""

    def __init__(self, session: _Session) -> None:
        self._session = session

    def upsert(self, *a: Any, **k: Any) -> int:
        raise ReadOnlyError("upstream_compat does not support writes yet")

    upsert_rows = upsert

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


class PgCompatInstrumentMaster:
    """Point-in-time symbol lookup over upstream ``symbol_alias`` (US only, provider-agnostic)."""

    def __init__(self, session: _Session) -> None:
        self._session = session

    def upsert(self, *a: Any, **k: Any) -> None:
        raise ReadOnlyError("upstream_compat does not support writes yet")

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
            if write:
                raise StorageError(
                    "upstream_compat is read-only for now: writes arrive with the shadow-run switch-over "
                    "(architecture.md 11.4); open with write=False"
                )
            if state_path is None:
                raise StorageError("upstream_compat needs state_path for the local SQLite state store")
            with self._session() as conn:
                check_upstream_compat(introspect_catalog(conn))  # never runs DDL
            self.raw = PgCompatRawStore(self._session)
            self.canonical = PgCompatCanonicalStore(self._session)
            self.instruments = PgCompatInstrumentMaster(self._session)
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
