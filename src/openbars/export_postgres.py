"""Optional, caller-controlled export of canonical Parquet bars to a generic PostgreSQL table.

This module is a **reference implementation** of the documented generic layout
(``bars + source information``, see ``docs/postgres-export.md``): daily OHLCV plus the
provenance openbars already stores on canonical rows (data source, fetch lineage when the
file carries it, ``freshness``, ``available_at``). It deliberately knows nothing about any
caller's own tables: there are no caller foreign keys (no integer upstream instrument id,
no caller artifact / run ids) and ``instrument_id`` is written as the openbars string id.

Callers that need their own official tables (with their own provenance columns) are
expected to write an adapter on their side and read openbars data (Parquet / ``BarReader``)
instead of pointing this exporter at those tables.

Nothing here enters the default ``plan`` / ``run`` write path, and the exporter never runs
DDL: the target table must exist. The default is a dry run; ``--apply`` is required to
connect and write.
"""

from __future__ import annotations

import json
import re
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import duckdb

#: Default target table name for the generic reference layout.
REFERENCE_TABLE = "openbars_bars"

#: The generic reference layout: ``(column, PostgreSQL type, nullable)``.
#: Bars first, then the canonical source fields, then ``freshness`` / ``available_at`` and the
#: raw fetch lineage that the canonical row may carry. Ordering is the INSERT column order.
REFERENCE_COLUMNS: tuple[tuple[str, str, bool], ...] = (
    # bar identity and time
    ("instrument_id", "TEXT", False),
    ("market", "TEXT", False),
    ("interval", "TEXT", False),
    ("session_date", "DATE", False),
    ("ts_start", "TIMESTAMPTZ", False),
    ("session", "TEXT", False),
    ("policy_version", "TEXT", False),
    # OHLCV (same precision as the Parquet / Postgres native schema)
    ("open", "NUMERIC(20,8)", False),
    ("high", "NUMERIC(20,8)", False),
    ("low", "NUMERIC(20,8)", False),
    ("close", "NUMERIC(20,8)", False),
    ("volume", "NUMERIC(28,8)", True),
    ("vwap", "NUMERIC(20,8)", True),
    ("trade_count", "BIGINT", True),
    # data source
    ("provider", "TEXT", False),
    ("feed", "TEXT", False),
    ("adjustment", "TEXT", False),
    ("revision", "INTEGER", False),
    ("price_source", "TEXT", False),
    ("volume_source", "TEXT", True),
    ("sources_seen", "JSONB", False),
    # reconciliation / quality
    ("selection_reason", "TEXT", False),
    ("quality_status", "TEXT", False),
    ("flags", "JSONB", False),
    ("capabilities", "JSONB", False),
    ("reconcile_rule", "TEXT", False),
    # source information: content fingerprint, generation time and the fetch-batch pointer
    ("source_fingerprint", "TEXT", False),
    ("generated_at", "TIMESTAMPTZ", False),
    ("source_fetch_id", "TEXT", True),
    # source information: provisional vs final, and when the bar became usable
    ("freshness", "TEXT", False),
    ("available_at", "TIMESTAMPTZ", True),
)

#: Idempotent upsert key, identical to the canonical bar key (openbars ``CANONICAL_KEY_SQL``).
REFERENCE_KEY: tuple[str, ...] = ("instrument_id", "interval", "policy_version", "session_date", "ts_start")

#: Canonical columns the exporter cannot work without (all part of ``openbars-storage-v1``).
_REQUIRED = frozenset(
    {
        "instrument_id", "market", "interval", "ts_start", "session_date", "session", "policy_version",
        "provider", "feed", "adjustment", "revision", "open", "high", "low", "close", "volume", "vwap",
        "trade_count", "flags", "selection_reason", "quality_status", "capabilities", "source_fingerprint",
        "price_source", "volume_source", "sources_seen", "reconcile_rule", "generated_at",
    }
)
_JSONB = frozenset(column for column, kind, _ in REFERENCE_COLUMNS if kind == "JSONB")
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_PASSWORD = re.compile(r"(://[^:/@\s]+:)[^@\s]+@")


def _check_table(table: str) -> None:
    if not isinstance(table, str) or not _IDENT.fullmatch(table) or len(table) > 63:
        raise ValueError("table must be an ASCII identifier of at most 63 characters")


def reference_ddl(table: str = REFERENCE_TABLE) -> str:
    """``CREATE TABLE`` for the generic reference layout. Printed/referenced by the docs only.

    The exporter never executes DDL; this is here so a caller can create the target table
    exactly as documented (and so tests can check the document and the insert agree).
    """
    _check_table(table)
    lines = [
        f'  "{column}" {kind}{"" if nullable else " NOT NULL"}'
        for column, kind, nullable in REFERENCE_COLUMNS
    ]
    key = ", ".join(f'"{column}"' for column in REFERENCE_KEY)
    lines.append(f"  PRIMARY KEY ({key})")
    return f'CREATE TABLE "{table}" (\n' + ",\n".join(lines) + "\n)"


def _connect(dsn: str) -> Any:
    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError("Postgres export requires the optional dependency; install with `pip install 'openbars[postgres-export]'`.") from exc
    return psycopg.connect(dsn)


def _json(value: Any) -> str:
    return json.dumps(list(value or []))


def _sql(table: str) -> str:
    columns = [column for column, _, _ in REFERENCE_COLUMNS]
    names = ", ".join(f'"{column}"' for column in columns)
    placeholders = ", ".join("%s::jsonb" if column in _JSONB else "%s" for column in columns)
    conflict = ", ".join(f'"{column}"' for column in REFERENCE_KEY)
    updates = ", ".join(f'"{column}"=EXCLUDED."{column}"' for column in columns if column not in REFERENCE_KEY)
    return (
        f'INSERT INTO "{table}" ({names}) VALUES ({placeholders}) '
        f"ON CONFLICT ({conflict}) DO UPDATE SET {updates}"
    )


def _load(source: str) -> list[dict[str, Any]]:
    if not sorted(Path(source).glob("**/*.parquet")):
        return []
    # DuckDB reads Parquet values as Decimal; no intermediate float conversion occurs.
    con = duckdb.connect(":memory:")
    try:
        rel = con.execute("SELECT * FROM read_parquet(?, union_by_name=true)", [str(Path(source) / "**/*.parquet")])
        names = [desc[0] for desc in rel.description]
        if missing := _REQUIRED - set(names):
            raise ValueError(f"canonical Parquet is missing columns: {', '.join(sorted(missing))}")
        return [dict(zip(names, row)) for row in rel.fetchall()]
    finally:
        con.close()


def _bar_row(row: dict[str, Any]) -> dict[str, Any]:
    """Map one canonical row to the generic layout; only canonical/source information, no caller FKs."""
    values: dict[str, Any] = {
        "instrument_id": row["instrument_id"],
        "market": row["market"],
        "interval": row["interval"],
        "session_date": row["session_date"],
        "ts_start": row["ts_start"],
        "session": row["session"],
        "policy_version": row["policy_version"],
        "open": row["open"],
        "high": row["high"],
        "low": row["low"],
        "close": row["close"],
        "volume": row["volume"],
        "vwap": row["vwap"],
        "trade_count": row["trade_count"],
        "provider": row["provider"],
        "feed": row["feed"],
        "adjustment": row["adjustment"],
        "revision": row["revision"],
        "price_source": row["price_source"],
        "volume_source": row.get("volume_source"),
        "sources_seen": _json(row.get("sources_seen")),
        "selection_reason": row["selection_reason"],
        "quality_status": row["quality_status"],
        "flags": _json(row.get("flags")),
        "capabilities": _json(row.get("capabilities")),
        "reconcile_rule": row["reconcile_rule"],
        "source_fingerprint": row["source_fingerprint"],
        "generated_at": row["generated_at"],
        # Canonical -> raw fetch-batch pointer (generic source-information contract); null when
        # the producer did not know the batch (older files, version-less imports).
        "source_fetch_id": row.get("source_fetch_id"),
        # Null / missing freshness reads as final everywhere else in openbars.
        "freshness": row.get("freshness") or "final",
        "available_at": row.get("available_at"),
    }
    return values


def _rows(source: str, since: date | None, days: int | None) -> list[dict[str, Any]]:
    objects = _load(source)
    daily = [row for row in objects if row["interval"] == "1d"]
    if days is not None:
        date_max = max((row["session_date"] for row in daily), default=None)
        if date_max is None:
            return []
        since = date_max - timedelta(days=days - 1)
    selected = [row for row in daily if not (since and row["session_date"] < since)]
    return [_bar_row(row) for row in selected]


def _scrub(message: str, dsn: str | None) -> str:
    """Never echo a DSN or an inline password from a driver exception."""
    if dsn:
        message = message.replace(dsn, "[REDACTED]")
    return _PASSWORD.sub(r"\1***@", message)


def _params(rows: list[dict[str, Any]]) -> list[tuple[Any, ...]]:
    return [tuple(row.get(column) for column, _, _ in REFERENCE_COLUMNS) for row in rows]


def run(args, out) -> int:
    try:
        _check_table(args.table)
        rows = _rows(args.source, args.since, args.days)
        out.line(f"export-postgres: {len(rows)} rows to {args.table} ({'apply' if args.apply else 'dry-run'})")
        if not rows:
            out.line("nothing to write")
            return 0
        if not args.apply:
            out.line("dry-run: nothing written")
            return 0
        conn = _connect(args.dsn)
        try:
            with conn.cursor() as cur:
                cur.executemany(_sql(args.table), _params(rows))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        out.line(f"applied {len(rows)} idempotent upserts")
        return 0
    except Exception as exc:
        # Avoid echoing DSNs from driver exceptions.
        out.err(f"error: Postgres export failed: {type(exc).__name__}: {_scrub(str(exc), getattr(args, 'dsn', None))}")
        return 1
