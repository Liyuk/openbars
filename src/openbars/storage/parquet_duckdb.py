"""Default backend: partitioned Parquet files + a rebuildable DuckDB catalog.

Layout (``root`` is normally ``./data``; see docs/architecture.md section 10)::

    state.db                                               SQLite: jobs, checkpoints, fetch_log
    raw/provider=P/market=M/interval=I/year=Y[/month=MM]/part-*.parquet
    canonical/market=M/interval=I/year=Y[/month=MM]/part-*.parquet
    ref/{instruments,symbol_mappings,corporate_actions}.parquet
    raw-archive/provider=P/...                             JSON response sidecars
    audit/                                                 reserved
    openbars.duckdb                                        views only; ``rebuild_catalog()``

Write model (LSM-lite, no in-place edits)
-----------------------------------------
* Every upsert writes one *new immutable* part file per touched partition, atomically
  (temp file in the same directory, fsync, ``os.replace``). Existing parts are never
  modified, so a crash cannot corrupt data that was already readable.
* Identity: the key expressions in ``schema.py``. **Last write wins**: among rows with
  the same key, the one in the part with the greatest file name (names embed a
  monotonic nanosecond stamp) is the visible one. Readers and views apply that rule with a
  window function, so duplicates in older parts are invisible even before compaction.
* A row identical to the visible one (ignoring lineage columns) is dropped before
  writing, so retrying a job does not create files.
* ``compact`` merges all parts of a partition into one deduplicated part, atomically,
  then removes the inputs. A crash in between leaves redundant but consistent data.
* Writers are serialised by an advisory lock (``<root>/.lock``); readers take no lock.
"""

from __future__ import annotations

import time
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from openbars.core.atomic import ProcessLock, atomic_write, cleanup_stale_tmp
from openbars.core.models import (
    Adjustment,
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
from openbars.orchestrator.jobs import Checkpoint, Job
from openbars.providers.base import RawBatch
from openbars.raw.archive import RawArchive
from openbars.storage.errors import StorageError
from openbars.storage.rows import (
    CanonicalBarRow,
    InvalidBarError,
    RawBarRow,
    canonical_rows_to_table,
    check_rows,
    raw_rows_from_batch,
    raw_rows_to_table,
    table_to_rows,
    to_decimal,
)
from openbars.storage.schema import (
    CANONICAL_BARS_SCHEMA,
    CANONICAL_KEY_SQL,
    CANONICAL_LINEAGE_COLUMNS,
    CORPORATE_ACTIONS_SCHEMA,
    DEFAULT_POLICY_VERSION,
    INSTRUMENTS_SCHEMA,
    RAW_BARS_SCHEMA,
    RAW_KEY_SQL,
    RAW_LINEAGE_COLUMNS,
    SCHEMA_VERSION,
    SYMBOL_MAPPINGS_SCHEMA,
    TABLE_SCHEMAS,
    duckdb_type,
    key_exprs,
    qident,
)
from openbars.storage.state import FetchLogEntry, SqliteStateStore  # noqa: F401  (re-export)

CATALOG_FILE = "openbars.duckdb"
LOCK_FILE = ".lock"
PART_GLOB = "part-*.parquet"

_BASENAME_RE = r"[^/\\]+$"  # SQL literal (no escapes); RE2 sees [^/\\]


# --- helpers -----------------------------------------------------------------------------


def _run_arrow(con: duckdb.DuckDBPyConnection, sql: str, params: Sequence[Any] = ()) -> pa.Table:
    """Run a query and return a pyarrow.Table (duckdb 1.x and 2.x spellings)."""
    rel = con.sql(sql, params=list(params))
    fn = getattr(rel, "to_arrow_table", None) or rel.arrow
    return fn()


def _mem_connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    con.execute("SET TimeZone='UTC'")
    return con


def _sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _part_ts(path: Path) -> int:
    try:
        return int(path.name.split("-")[1])
    except (IndexError, ValueError):
        return 0


@dataclass(frozen=True)
class LayerSpec:
    name: str  # raw | canonical
    view: str
    base: str
    schema: pa.Schema
    key_sql: tuple[str, ...]
    lineage: frozenset[str]
    partition_dims: tuple[str, ...]
    order_sql: str


RAW_LAYER = LayerSpec(
    "raw",
    "raw_bars",
    "raw",
    RAW_BARS_SCHEMA,
    RAW_KEY_SQL,
    RAW_LINEAGE_COLUMNS,
    ("provider", "market", "interval"),
    'session_date, ts_start, coalesce(instrument_id, symbol_as_returned), provider, feed, adjustment, revision',
)
CANONICAL_LAYER = LayerSpec(
    "canonical",
    "canonical_bars",
    "canonical",
    CANONICAL_BARS_SCHEMA,
    CANONICAL_KEY_SQL,
    CANONICAL_LINEAGE_COLUMNS,
    ("market", "interval"),
    "session_date, ts_start, instrument_id, policy_version",
)
LAYERS: Mapping[str, LayerSpec] = {"raw": RAW_LAYER, "canonical": CANONICAL_LAYER}


def _dedup_sql(spec: LayerSpec, source: str) -> str:
    """Rows of ``source`` (must expose ``filename``) minus older duplicates (last part wins)."""
    partition = ", ".join(key_exprs(spec.key_sql))
    return (
        "SELECT * EXCLUDE (filename, _basename, _rn) FROM ("
        f"SELECT *, row_number() OVER (PARTITION BY {partition} ORDER BY _basename DESC) AS _rn FROM ("
        f"SELECT *, regexp_extract(filename, '{_BASENAME_RE}') AS _basename FROM {source}"
        ")) WHERE _rn = 1"
    )


def _empty_select(schema: pa.Schema) -> str:
    cols = ", ".join(f"CAST(NULL AS {duckdb_type(f.type)}) AS {qident(f.name)}" for f in schema)
    return f"SELECT {cols} WHERE false"


@dataclass(frozen=True)
class CompactionResult:
    layer: str
    partition: str  # relative path of the partition directory
    parts_before: int
    rows_before: int  # including superseded duplicates
    rows_after: int


# --- layer engine ------------------------------------------------------------------------


class _PartitionedTable:
    """Shared upsert / scan / compact machinery for the raw and canonical layers."""

    def __init__(self, backend: "ParquetDuckDbBackend", spec: LayerSpec) -> None:
        self.backend = backend
        self.spec = spec
        self.base = backend.root / spec.base

    # -- file discovery --

    def parts(
        self,
        *,
        filters: Mapping[str, Iterable[str]] | None = None,
        directory: Path | None = None,
    ) -> list[Path]:
        """Complete part files (never ``.tmp``), optionally narrowed by partition values."""
        allowed = {k: set(v) for k, v in (filters or {}).items()}
        root = directory or self.base
        if not root.exists():
            return []
        out: list[Path] = []
        for path in root.rglob(PART_GLOB):
            if not path.is_file():
                continue
            if allowed:
                values = dict(
                    seg.split("=", 1) for seg in path.relative_to(self.base).parts[:-1] if "=" in seg
                )
                if any(values.get(k) not in vals for k, vals in allowed.items()):
                    continue
            out.append(path)
        return sorted(out)

    # -- partitioning --

    def partition_dir(self, row: Any) -> Path:
        path = self.base
        for dim in self.spec.partition_dims:
            value = getattr(row, dim)
            path = path / f"{dim}={getattr(value, 'value', value)}"
        path = path / f"year={row.session_date.year:04d}"
        if Interval(row.interval).is_intraday:
            path = path / f"month={row.session_date.month:02d}"
        return path

    # -- reads --

    def query(self, files: Sequence[Path], where: str = "TRUE", params: Sequence[Any] = ()) -> pa.Table:
        spec = self.spec
        if not files:
            return spec.schema.empty_table()
        sql = (
            f"SELECT * FROM ({_dedup_sql(spec, 'read_parquet(?, filename=true, union_by_name=true)')}) "
            f"WHERE {where} ORDER BY {spec.order_sql}"
        )
        con = _mem_connection()
        try:
            table = _run_arrow(con, sql, [[str(p) for p in files], *params])
        finally:
            con.close()
        return self._conform(table)

    def _conform(self, table: pa.Table) -> pa.Table:
        schema = self.spec.schema
        columns = [table.column(f.name).cast(f.type) for f in schema]
        return pa.Table.from_arrays(columns, schema=schema)

    # -- writes --

    def upsert_table_rows(self, rows: Sequence[Any], to_table: Any) -> int:
        """Write rows (already validated). Returns the number of rows actually written."""
        if not rows:
            return 0
        latest: dict[tuple, Any] = {}
        for row in rows:  # last occurrence in the batch wins
            latest[row.key()] = row
        groups: dict[Path, list[Any]] = defaultdict(list)
        for row in latest.values():
            groups[self.partition_dir(row)].append(row)
        written = 0
        with self.backend.lock:
            for directory, group in sorted(groups.items()):
                table = to_table(group)
                existing = self.parts(directory=directory)
                if existing:
                    table = self._drop_unchanged(table, existing)
                if table.num_rows == 0:
                    continue
                table = table.sort_by(self._sort_columns(table))
                self.write_part(directory, table, floor_ts=max((_part_ts(p) for p in existing), default=0))
                written += table.num_rows
        return written

    def _sort_columns(self, table: pa.Table) -> list[tuple[str, str]]:
        cols = ["instrument_id", "session_date", "ts_start"]
        if self.spec.name == "raw":
            cols = ["symbol_as_returned", "session_date", "ts_start"]
        return [(c, "ascending") for c in cols if c in table.column_names]

    def write_part(self, directory: Path, table: pa.Table, *, floor_ts: int = 0, suffix: str = "") -> Path:
        stamp = max(time.time_ns(), floor_ts + 1)
        path = directory / f"part-{stamp:019d}-{suffix}{uuid.uuid4().hex[:8]}.parquet"
        table = table.cast(self.spec.schema)  # also stamps the schema-version metadata
        atomic_write(
            path,
            lambda fh: pq.write_table(table, fh, compression="zstd", write_statistics=True),
        )
        return path

    def _drop_unchanged(self, incoming: pa.Table, existing_files: Sequence[Path]) -> pa.Table:
        spec = self.spec
        indexed = incoming.append_column("_idx", pa.array(range(incoming.num_rows), pa.int64()))
        on = " AND ".join(
            f"{a} IS NOT DISTINCT FROM {b}"
            for a, b in zip(key_exprs(spec.key_sql, "i"), key_exprs(spec.key_sql, "e"))
        )
        content = [f.name for f in spec.schema if f.name not in spec.lineage]
        differs = " OR ".join(f"i.{qident(c)} IS DISTINCT FROM e.{qident(c)}" for c in content)
        sql = (
            f"WITH e AS (SELECT *, TRUE AS _hit FROM ({_dedup_sql(spec, 'read_parquet(?, filename=true, union_by_name=true)')})) "
            f"SELECT i._idx FROM incoming i LEFT JOIN e ON {on} WHERE e._hit IS NULL OR ({differs})"
        )
        con = _mem_connection()
        try:
            con.register("incoming", indexed)
            keep = [r[0] for r in con.execute(sql, [[str(p) for p in existing_files]]).fetchall()]
        finally:
            con.close()
        if len(keep) == incoming.num_rows:
            return incoming
        return incoming.take(pa.array(sorted(keep), pa.int64()))

    # -- compaction --

    def compact(self, *, min_parts: int = 2) -> list[CompactionResult]:
        by_dir: dict[Path, list[Path]] = defaultdict(list)
        for part in self.parts():
            by_dir[part.parent].append(part)
        results: list[CompactionResult] = []
        with self.backend.lock:
            for directory, files in sorted(by_dir.items()):
                if len(files) < max(min_parts, 2):
                    continue
                rows_before = sum(pq.ParquetFile(p).metadata.num_rows for p in files)
                table = self.query(files)
                table = table.sort_by(self._sort_columns(table))
                self.write_part(
                    directory,
                    table,
                    floor_ts=max(_part_ts(p) for p in files) - 1,  # keep the newest input's rank
                    suffix="c",
                )
                for old in files:
                    old.unlink(missing_ok=True)
                results.append(
                    CompactionResult(
                        self.spec.name,
                        directory.relative_to(self.backend.root).as_posix(),
                        len(files),
                        rows_before,
                        table.num_rows,
                    )
                )
        return results


# --- raw ---------------------------------------------------------------------------------


def _years(window: DateRange | None) -> list[str] | None:
    if window is None:
        return None
    return [f"{y:04d}" for y in range(window.start.year, window.end.year + 1)]


class ParquetRawStore:
    def __init__(self, backend: "ParquetDuckDbBackend") -> None:
        self._table = _PartitionedTable(backend, RAW_LAYER)

    def append(
        self,
        batch: RawBatch,
        *,
        source_artifact_ref: str | None = None,
        ingest_run_id: str | None = None,
        validate: bool = True,
    ) -> int:
        """Persist a batch (idempotent upsert). Returns the number of rows written.

        ``source_artifact_ref`` is the ``RawArchive`` ref of the response that produced it.
        """
        rows = raw_rows_from_batch(
            batch, source_artifact_ref=source_artifact_ref, ingest_run_id=ingest_run_id
        )
        return self.append_rows(rows, validate=validate)

    def append_rows(self, rows: Sequence[RawBarRow], *, validate: bool = True) -> int:
        if validate:
            check_rows(rows, "raw")
        return self._table.upsert_table_rows(rows, raw_rows_to_table)

    def scan_table(
        self,
        provider: str | None,
        market: Market,
        interval: Interval,
        window: DateRange,
        *,
        symbols: Sequence[str] | None = None,
        instrument_ids: Sequence[str] | None = None,
    ) -> pa.Table:
        filters: dict[str, Any] = {"market": [Market(market).value], "interval": [Interval(interval).value]}
        if provider is not None:
            filters["provider"] = [provider]
        filters["year"] = _years(window)
        where = "market = ? AND \"interval\" = ? AND session_date BETWEEN ? AND ?"
        params: list[Any] = [Market(market).value, Interval(interval).value, window.start, window.end]
        if provider is not None:
            where += " AND provider = ?"
            params.append(provider)
        for column, values in (("symbol_as_returned", symbols), ("instrument_id", instrument_ids)):
            if values is not None:
                where += f" AND {column} IN ({', '.join('?' for _ in values) or 'NULL'})"
                params.extend(values)
        return self._table.query(self._table.parts(filters=filters), where, params)

    def scan_rows(self, provider: str | None, market: Market, interval: Interval, window: DateRange, **kw: Any) -> list[RawBarRow]:
        """Exact ``Decimal`` rows."""
        return table_to_rows(self.scan_table(provider, market, interval, window, **kw), RawBarRow)

    def scan(self, provider: str, market: Market, interval: Interval, window: DateRange) -> Iterator[RawBar]:
        """Protocol scan: legacy ``RawBar`` (float, lossy; exact values are in ``extra``)."""
        for row in self.scan_rows(provider, market, interval, window):
            yield row.to_raw_bar()

    def compact(self, *, min_parts: int = 2) -> list[CompactionResult]:
        return self._table.compact(min_parts=min_parts)


# --- canonical ---------------------------------------------------------------------------


class ParquetCanonicalStore:
    def __init__(self, backend: "ParquetDuckDbBackend") -> None:
        self.backend = backend
        self._table = _PartitionedTable(backend, CANONICAL_LAYER)

    def upsert(
        self,
        bars: Sequence[CanonicalBar],
        *,
        policy_version: str = DEFAULT_POLICY_VERSION,
        selection_reason: str = "unspecified",
        capabilities: Sequence[str] = (),
        market: Market | None = None,
    ) -> int:
        """Upsert legacy ``CanonicalBar`` models.

        Fields the model lacks come from the keyword arguments (see architecture.md section
        10). ``market`` defaults to the instrument's market (id prefix or instrument master).
        """
        now = datetime.now(UTC)
        rows = [
            CanonicalBarRow.from_canonical(
                cb,
                market=market or self.backend.market_of(cb.bar.instrument_id),
                policy_version=policy_version,
                selection_reason=selection_reason,
                capabilities=capabilities,
                generated_at=now,
            )
            for cb in bars
        ]
        return self.upsert_rows(rows)

    def upsert_rows(self, rows: Sequence[CanonicalBarRow]) -> int:
        check_rows(rows, "canonical")
        return self._table.upsert_table_rows(rows, canonical_rows_to_table)

    def scan_table(
        self,
        instrument_ids: Sequence[str] | None,
        interval: Interval,
        window: DateRange,
        *,
        market: Market | None = None,
        policy_version: str | None = DEFAULT_POLICY_VERSION,
    ) -> pa.Table:
        """Exact decimal Arrow table. ``instrument_ids=None`` = the whole market."""
        filters: dict[str, Any] = {"interval": [Interval(interval).value], "year": _years(window)}
        where = "\"interval\" = ? AND session_date BETWEEN ? AND ?"
        params: list[Any] = [Interval(interval).value, window.start, window.end]
        if market is not None:
            filters["market"] = [Market(market).value]
            where += " AND market = ?"
            params.append(Market(market).value)
        if policy_version is not None:
            where += " AND policy_version = ?"
            params.append(policy_version)
        if instrument_ids is not None:
            where += f" AND instrument_id IN ({', '.join('?' for _ in instrument_ids) or 'NULL'})"
            params.extend(instrument_ids)
        return self._table.query(self._table.parts(filters=filters), where, params)

    def scan_rows(self, instrument_ids: Sequence[str] | None, interval: Interval, window: DateRange, **kw: Any) -> list[CanonicalBarRow]:
        return table_to_rows(self.scan_table(instrument_ids, interval, window, **kw), CanonicalBarRow)

    def scan(self, instrument_ids: Sequence[str], interval: Interval, window: DateRange) -> Iterator[CanonicalBar]:
        """Protocol scan: legacy ``CanonicalBar`` (float, lossy) for the default policy."""
        for row in self.scan_rows(instrument_ids, interval, window):
            yield row.to_canonical_bar()

    def compact(self, *, min_parts: int = 2) -> list[CompactionResult]:
        return self._table.compact(min_parts=min_parts)


# --- ref: instrument master --------------------------------------------------------------


class ParquetInstrumentMaster:
    """``ref/instruments.parquet`` + ``symbol_mappings.parquet`` + ``corporate_actions.parquet``.

    Small tables: each upsert rewrites the whole file atomically (merge by key, new wins).
    Keys: instruments ``instrument_id``; mappings ``(instrument_id, provider, symbol,
    valid_from)``; actions ``(instrument_id, ex_date, kind, source)``.
    """

    def __init__(self, backend: "ParquetDuckDbBackend") -> None:
        self.backend = backend
        self._cache_sig: tuple | None = None
        self._instruments: dict[str, Instrument] = {}
        self._by_symbol: dict[str, list[SymbolMapping]] = {}

    def _path(self, name: str) -> Path:
        return self.backend.ref_dir / f"{name}.parquet"

    def _read_records(self, name: str) -> list[dict[str, Any]]:
        path = self._path(name)
        return pq.read_table(path).to_pylist() if path.exists() else []

    def _write_records(self, name: str, schema: pa.Schema, records: list[dict[str, Any]]) -> None:
        table = pa.Table.from_pylist(records, schema=schema) if records else schema.empty_table()
        path = self._path(name)
        atomic_write(path, lambda fh: pq.write_table(table, fh, compression="zstd"))

    # -- writes --

    def upsert(self, instruments: Iterable[Instrument], mappings: Iterable[SymbolMapping]) -> None:
        new_instruments = [_instrument_record(i) for i in instruments]
        new_mappings = [_mapping_record(m) for m in mappings]
        with self.backend.lock:
            if new_instruments:
                merged = {r["instrument_id"]: r for r in self._read_records("instruments")}
                merged.update((r["instrument_id"], r) for r in new_instruments)
                self._write_records("instruments", INSTRUMENTS_SCHEMA, [merged[k] for k in sorted(merged)])
            if new_mappings:
                key = lambda r: (r["instrument_id"], r["provider"], r["symbol"], r["valid_from"])  # noqa: E731
                merged_m = {key(r): r for r in self._read_records("symbol_mappings")}
                merged_m.update((key(r), r) for r in new_mappings)
                self._write_records("symbol_mappings", SYMBOL_MAPPINGS_SCHEMA, [merged_m[k] for k in sorted(merged_m)])

    def upsert_corporate_actions(self, actions: Iterable[CorporateAction]) -> None:
        new = [_action_record(a) for a in actions]
        if not new:
            return
        key = lambda r: (r["instrument_id"], r["ex_date"], r["kind"], r["source"])  # noqa: E731
        with self.backend.lock:
            merged = {key(r): r for r in self._read_records("corporate_actions")}
            merged.update((key(r), r) for r in new)
            self._write_records("corporate_actions", CORPORATE_ACTIONS_SCHEMA, [merged[k] for k in sorted(merged)])

    # -- reads --

    def _load(self) -> None:
        sig = tuple(
            (p.stat().st_mtime_ns, p.stat().st_size) if p.exists() else None
            for p in (self._path("instruments"), self._path("symbol_mappings"))
        )
        if sig == self._cache_sig:
            return
        self._instruments = {
            r["instrument_id"]: _instrument_from(r) for r in self._read_records("instruments")
        }
        by_symbol: dict[str, list[SymbolMapping]] = defaultdict(list)
        for r in self._read_records("symbol_mappings"):
            by_symbol[r["symbol"]].append(
                SymbolMapping(r["instrument_id"], r["symbol"], r["valid_from"], r["valid_to"], r["provider"])
            )
        self._by_symbol = dict(by_symbol)
        self._cache_sig = sig

    def resolve(self, symbol: str, *, on: date, provider: str, market: Market) -> str | None:
        """Instrument a symbol meant on ``on`` (``valid_to`` inclusive), else ``None``.

        Provider-specific mappings take precedence over ``"*"`` mappings; more than one
        distinct instrument at the winning precedence means ambiguity -> ``None``.
        """
        self._load()
        market = Market(market)
        specific: set[str] = set()
        generic: set[str] = set()
        for m in self._by_symbol.get(symbol, ()):
            if m.valid_from > on or (m.valid_to is not None and on > m.valid_to):
                continue
            inst = self._instruments.get(m.instrument_id)
            if inst is None or inst.market is not market:
                continue
            if m.provider == provider:
                specific.add(m.instrument_id)
            elif m.provider == "*":
                generic.add(m.instrument_id)
        for ids in (specific, generic):
            if ids:
                return next(iter(ids)) if len(ids) == 1 else None
        return None

    def get(self, instrument_id: str) -> Instrument | None:
        self._load()
        return self._instruments.get(instrument_id)

    def resolve_any(self, item: str, *, on: date, market: Market | None = None) -> str:
        """A known instrument id, else a symbol resolved at ``on`` (generic mappings,
        optional market), else ``item`` unchanged (an id that may simply have no bars)."""
        if self.get(item) is not None:
            return item
        for m in [Market(market)] if market is not None else list(Market):
            found = self.resolve(item, on=on, provider="*", market=m)
            if found is not None:
                return found
        return item

    def instruments(self) -> list[Instrument]:
        self._load()
        return list(self._instruments.values())

    def mappings(self) -> list[SymbolMapping]:
        self._load()
        return [m for ms in self._by_symbol.values() for m in ms]

    def corporate_actions_table(self, instrument_ids: Sequence[str] | None = None) -> pa.Table:
        """Exact-decimal Arrow table of corporate actions."""
        path = self._path("corporate_actions")
        table = pq.read_table(path) if path.exists() else CORPORATE_ACTIONS_SCHEMA.empty_table()
        if instrument_ids is not None:
            import pyarrow.compute as pc

            table = table.filter(pc.is_in(table["instrument_id"], pa.array(list(instrument_ids), pa.string())))
        return table

    def corporate_actions(self, instrument_ids: Sequence[str] | None = None) -> list[CorporateAction]:
        """Legacy float models (lossy ratio/amount)."""
        return [
            CorporateAction(
                instrument_id=r["instrument_id"],
                ex_date=r["ex_date"],
                kind=CorporateActionKind(r["kind"]),
                source=r["source"],
                ratio=None if r["ratio"] is None else float(r["ratio"]),
                amount=None if r["amount"] is None else float(r["amount"]),
            )
            for r in self.corporate_actions_table(instrument_ids).to_pylist()
        ]


def _instrument_record(i: Instrument) -> dict[str, Any]:
    return {
        "instrument_id": i.instrument_id,
        "market": Market(i.market).value,
        "asset_class": AssetClass(i.asset_class).value,
        "currency": i.currency,
        "name": i.name,
        "exchange": i.exchange,
        "listed_on": i.listed_on,
        "delisted_on": i.delisted_on,
        "external_ids": sorted((str(k), str(v)) for k, v in i.external_ids.items()),
    }


def _instrument_from(r: Mapping[str, Any]) -> Instrument:
    return Instrument(
        instrument_id=r["instrument_id"],
        market=Market(r["market"]),
        asset_class=AssetClass(r["asset_class"]),
        currency=r["currency"],
        name=r["name"],
        exchange=r["exchange"],
        listed_on=r["listed_on"],
        delisted_on=r["delisted_on"],
        external_ids=dict(r["external_ids"] or []),
    )


def _mapping_record(m: SymbolMapping) -> dict[str, Any]:
    return {
        "instrument_id": m.instrument_id,
        "provider": m.provider,
        "symbol": m.symbol,
        "valid_from": m.valid_from,
        "valid_to": m.valid_to,
    }


def _action_record(a: CorporateAction) -> dict[str, Any]:
    def dec(v: Any, name: str) -> Decimal | None:
        return None if v is None else to_decimal(v, precision=20, name=name)

    return {
        "instrument_id": a.instrument_id,
        "ex_date": a.ex_date,
        "kind": CorporateActionKind(a.kind).value,
        "source": a.source,
        "ratio": dec(a.ratio, "ratio"),
        "amount": dec(a.amount, "amount"),
    }


# --- reader ------------------------------------------------------------------------------


class DuckDbBarReader:
    """``BarReader`` over the DuckDB catalog views; returns ``pyarrow.Table`` (exact decimals).

    ``adjustment`` filters on the *stored* ``adjustment`` column: the canonical layer holds
    one series per ``policy_version`` (``research-daily-v2`` is split-adjusted as
    delivered, so the default ``SPLIT`` matches it). Query-time derivation of other
    adjustment modes lands with ``normalize.adjust``; pass ``adjustment=None`` to skip the
    filter. ``as_of`` hides rows generated after that date.
    """

    def __init__(self, backend: "ParquetDuckDbBackend") -> None:
        self.backend = backend

    def bars(
        self,
        symbols_or_ids: Sequence[str] | None = None,
        *,
        interval: Interval = Interval.D1,
        start: date | None = None,
        end: date | None = None,
        adjustment: Adjustment | None = Adjustment.SPLIT,
        as_of: date | None = None,
        market: Market | None = None,
        policy_version: str = DEFAULT_POLICY_VERSION,
    ) -> pa.Table:
        where = ['"interval" = ?', "policy_version = ?"]
        params: list[Any] = [Interval(interval).value, policy_version]
        if adjustment is not None:
            where.append("adjustment = ?")
            params.append(Adjustment(adjustment).value)
        if market is not None:
            where.append("market = ?")
            params.append(Market(market).value)
        if symbols_or_ids:
            ids = [
                self.backend.instruments.resolve_any(item, on=end or date.today(), market=market)
                for item in symbols_or_ids
            ]
            ids = [i for i in ids if i is not None]
            where.append(f"instrument_id IN ({', '.join('?' for _ in ids) or 'NULL'})")
            params.extend(ids)
        if start is not None:
            where.append("session_date >= ?")
            params.append(start)
        if end is not None:
            where.append("session_date <= ?")
            params.append(end)
        if as_of is not None:
            where.append("CAST(generated_at AS DATE) <= ?")
            params.append(as_of)
        sql = (
            f"SELECT * FROM {CANONICAL_LAYER.view} WHERE {' AND '.join(where)} "
            "ORDER BY session_date, instrument_id, ts_start"
        )
        return self.backend.query(sql, params)


# --- backend -----------------------------------------------------------------------------


class ParquetDuckDbBackend:
    """``StorageBackend`` implementation rooted at ``root`` (default layout ``./data``)."""

    schema_version = SCHEMA_VERSION

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.ref_dir = self.root / "ref"
        self.audit_dir = self.root / "audit"
        self.catalog_path = self.root / CATALOG_FILE
        for directory in (self.root, self.ref_dir, self.audit_dir):
            directory.mkdir(parents=True, exist_ok=True)
        self.lock = ProcessLock(self.root / LOCK_FILE)
        self.raw = ParquetRawStore(self)
        self.canonical = ParquetCanonicalStore(self)
        self.instruments = ParquetInstrumentMaster(self)
        self.state = SqliteStateStore(self.root / "state.db")
        self.archive = RawArchive(self.root / "raw-archive")
        self.reader = DuckDbBarReader(self)

    def __enter__(self) -> "ParquetDuckDbBackend":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        """Nothing is held open between calls; present for symmetry with other backends."""

    # -- helpers --

    def market_of(self, instrument_id: str) -> Market:
        """Market of an instrument: id prefix (``us:eq:...``) or the instrument master."""
        prefix = instrument_id.split(":", 1)[0]
        if prefix in {m.value for m in Market}:
            return Market(prefix)
        inst = self.instruments.get(instrument_id)
        if inst is None:
            raise StorageError(
                f"cannot determine market for instrument {instrument_id!r}: "
                "register it in the instrument master or pass market="
            )
        return inst.market

    def compact(self, layer: str | None = None, *, min_parts: int = 2, stale_tmp_seconds: float = 3600.0) -> list[CompactionResult]:
        """Merge parts per partition (one deduplicated part each); drop stale ``.tmp`` files."""
        results: list[CompactionResult] = []
        for name in ([layer] if layer else list(LAYERS)):
            if name not in LAYERS:
                raise ValueError(f"unknown layer {name!r}")
            results += _PartitionedTable(self, LAYERS[name]).compact(min_parts=min_parts)
            cleanup_stale_tmp(self.root / LAYERS[name].base, max_age_seconds=stale_tmp_seconds)
        return results

    # -- DuckDB catalog --

    def _layer_files_exist(self, spec: LayerSpec) -> bool:
        return bool(_PartitionedTable(self, spec).parts())

    def _ref_exists(self, name: str) -> bool:
        return (self.ref_dir / f"{name}.parquet").exists()

    def _catalog_signature(self) -> dict[str, str]:
        sig = {"root": str(self.root.resolve()), "schema_version": str(SCHEMA_VERSION)}
        for spec in LAYERS.values():
            sig[spec.name] = str(self._layer_files_exist(spec))
        for name in ("instruments", "symbol_mappings", "corporate_actions"):
            sig[name] = str(self._ref_exists(name))
        return sig

    def rebuild_catalog(self) -> None:
        """Drop and recreate every DuckDB view from the Parquet tree (the file holds no data)."""
        with self.lock:
            sig = self._catalog_signature()
            con = duckdb.connect(str(self.catalog_path))
            try:
                con.execute("SET TimeZone='UTC'")
                for spec in LAYERS.values():
                    con.execute(f"DROP VIEW IF EXISTS {spec.view}")
                    if sig[spec.name] == "True":
                        glob = (self.root / spec.base).resolve().as_posix() + "/**/" + PART_GLOB
                        source = f"read_parquet({_sql_str(glob)}, filename=true, union_by_name=true)"
                        con.execute(f"CREATE VIEW {spec.view} AS {_dedup_sql(spec, source)}")
                    else:
                        con.execute(f"CREATE VIEW {spec.view} AS {_empty_select(spec.schema)}")
                for name in ("instruments", "symbol_mappings", "corporate_actions"):
                    con.execute(f"DROP VIEW IF EXISTS {name}")
                    if sig[name] == "True":
                        path = (self.ref_dir / f"{name}.parquet").resolve().as_posix()
                        con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet({_sql_str(path)})")
                    else:
                        con.execute(f"CREATE VIEW {name} AS {_empty_select(TABLE_SCHEMAS[name])}")
                con.execute("DROP TABLE IF EXISTS catalog_meta")
                con.execute("CREATE TABLE catalog_meta (key VARCHAR PRIMARY KEY, value VARCHAR)")
                con.executemany("INSERT INTO catalog_meta VALUES (?, ?)", list(sig.items()))
            finally:
                con.close()

    def _catalog_is_current(self) -> bool:
        if not self.catalog_path.exists():
            return False
        try:
            con = duckdb.connect(str(self.catalog_path), read_only=True)
            try:
                stored = dict(con.execute("SELECT key, value FROM catalog_meta").fetchall())
            finally:
                con.close()
        except duckdb.Error:
            return False
        return stored == self._catalog_signature()

    def ensure_catalog(self) -> None:
        """Rebuild views when missing, moved, or when a layer gained its first files."""
        if not self._catalog_is_current():
            self.rebuild_catalog()

    def query(self, sql: str, params: Sequence[Any] = ()) -> pa.Table:
        """Run SQL against the catalog views (``raw_bars``, ``canonical_bars``, ``instruments``,
        ``symbol_mappings``, ``corporate_actions``) and return a ``pyarrow.Table``."""
        self.ensure_catalog()
        con = duckdb.connect(str(self.catalog_path), read_only=True)
        try:
            con.execute("SET TimeZone='UTC'")
            return _run_arrow(con, sql, params)
        finally:
            con.close()


def open_backend(path: str | Path = "./data") -> ParquetDuckDbBackend:
    """Open (creating if needed) the default Parquet + DuckDB backend at ``path``."""
    return ParquetDuckDbBackend(path)
