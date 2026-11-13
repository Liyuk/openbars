"""Optional, caller-controlled export of canonical Parquet bars to existing PostgreSQL tables."""
from __future__ import annotations

import csv
import json
import re
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb

_TABLES = {
    "daily_bar": ("instrument_id", "session_date", "provider", "feed", "adjustment", "revision"),
    "research_daily_bar": ("instrument_id", "session_date", "policy_version"),
}
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _connect(dsn: str) -> Any:
    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError("Postgres export requires the optional dependency; install with `pip install 'openbars[postgres-export]'`.") from exc
    return psycopg.connect(dsn)


def _mapping(path: str) -> dict[str, int]:
    result: dict[str, int] = {}
    with open(path, newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        if not reader.fieldnames or not {"instrument_id", "upstream_id"}.issubset(reader.fieldnames):
            raise ValueError("mapping CSV must have instrument_id,upstream_id columns")
        for line, row in enumerate(reader, start=2):
            key = (row.get("instrument_id") or "").strip()
            try:
                value = int(row.get("upstream_id", ""))
            except ValueError:
                raise ValueError(f"mapping CSV line {line}: upstream_id must be an integer") from None
            if not key or value < 0:
                raise ValueError(f"mapping CSV line {line}: invalid instrument_id or upstream_id")
            if key in result and result[key] != value:
                raise ValueError(f"mapping CSV contains conflicting ids for {key!r}")
            result[key] = value
    return result


def _rows(source: str, mapping_file: str, table: str, since: date | None, days: int | None) -> list[tuple]:
    files = sorted(Path(source).glob("**/*.parquet"))
    if not files:
        return []
    # DuckDB reads Parquet values as Decimal; no intermediate float conversion occurs.
    con = duckdb.connect(":memory:")
    try:
        rel = con.execute("SELECT * FROM read_parquet(?, union_by_name=true)", [str(Path(source) / "**/*.parquet")])
        columns = {desc[0] for desc in rel.description}
        required = {"instrument_id", "session_date", "interval", "open", "high", "low", "close", "volume", "provider", "feed", "adjustment", "revision", "policy_version"}
        if missing := required - columns:
            raise ValueError(f"canonical Parquet is missing columns: {', '.join(sorted(missing))}")
        result = rel.fetchall()
        names = [desc[0] for desc in rel.description]
        objects = [dict(zip(names, row)) for row in result]
    finally:
        con.close()
    id_map = _mapping(mapping_file)
    date_max = max((r["session_date"] for r in objects if r["interval"] == "1d"), default=None)
    if days is not None:
        if date_max is None:
            return []
        since = date_max - timedelta(days=days - 1)
    rows = []
    for row in objects:
        day = row["session_date"]
        if row["interval"] != "1d" or (since and day < since):
            continue
        upstream_id = id_map.get(row["instrument_id"])
        if upstream_id is None:
            raise ValueError(f"no caller-provided integer id for instrument_id {row['instrument_id']!r}")
        if row["volume"] is None:
            raise ValueError(f"volume is required by the target schema for {row['instrument_id']!r} on {day}")
        common = (upstream_id, day)
        if table == "daily_bar":
            currency = next((c.split(":", 1)[1].upper() for c in row.get("capabilities", []) if c.lower().startswith("currency:")), None)
            rows.append(common + (row["provider"], row["feed"], row["adjustment"], "unknown", "unknown", None,
                "unknown", currency, "unknown", row["revision"], row["open"], row["high"], row["low"],
                row["close"], row["volume"], row["vwap"], None, None))
        else:
            rows.append(common + (row["policy_version"], None, row["provider"], row["feed"], row["adjustment"],
                row["revision"], row["open"], row["high"], row["low"], row["close"], row["volume"],
                row["selection_reason"], row["quality_status"], json.dumps(row.get("capabilities") or []),
                row["source_fingerprint"], row["generated_at"]))
    return rows


def _sql(table: str, layout: str) -> str:
    if layout == "daily_bar":
        cols = ("instrument_id", "session_date", "provider", "feed", "adjustment", "price_basis", "adjustment_scope", "adjustment_as_of", "session_semantics", "currency", "volume_basis", "revision", "open", "high", "low", "close", "volume", "vwap", "source_artifact_id", "ingest_run_id")
        key = ("instrument_id", "session_date", "provider", "feed", "adjustment", "revision")
    else:
        cols = ("instrument_id", "session_date", "policy_version", "selected_source_bar_id", "provider", "feed", "adjustment", "revision", "open", "high", "low", "close", "volume", "selection_reason", "quality_status", "capabilities", "source_fingerprint", "generated_at")
        key = ("instrument_id", "session_date", "policy_version")
    names = ", ".join(cols)
    placeholders = ", ".join("%s::jsonb" if col == "capabilities" else "%s" for col in cols)
    updates = ", ".join(f"{col}=EXCLUDED.{col}" for col in cols if col not in key)
    return f'INSERT INTO "{table}" ({names}) VALUES ({placeholders}) ON CONFLICT ({", ".join(key)}) DO UPDATE SET {updates}'


def run(args, out) -> int:
    try:
        if not _IDENT.fullmatch(args.table) or len(args.table) > 63:
            raise ValueError("table must be an ASCII identifier of at most 63 characters")
        layout = args.table_layout or (args.table if args.table in _TABLES else None)
        if layout is None:
            raise ValueError("custom table requires --table-layout")
        rows = _rows(args.source, args.mapping, layout, args.since, args.days)
        out.line(f"export-postgres: {len(rows)} rows to {args.table} ({'apply' if args.apply else 'dry-run'})")
        if not args.apply:
            out.line("dry-run: nothing written")
            return 0
        conn = _connect(args.dsn)
        try:
            with conn.cursor() as cur:
                sql = _sql(args.table, layout)
                cur.executemany(sql, rows)
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
        out.err(f"error: Postgres export failed: {type(exc).__name__}: {str(exc).replace(args.dsn, '[REDACTED]')}")
        return 1
