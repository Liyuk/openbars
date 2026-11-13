"""Local SQLite side tables for ``upstream_compat`` identity extras.

Upstream has no ``unresolved`` / ``symbol_observations`` / ``upstream_ids`` tables,
and compat never migrates. These live next to ``state.db`` so
``PgCompatInstrumentMaster`` can implement the full ``InstrumentStore``.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable
from datetime import date
from pathlib import Path

from openbars.core.models import Market
from openbars.normalize.instruments import UnresolvedRecord, _check_upstream

_DDL = (
    """
    CREATE TABLE IF NOT EXISTS unresolved (
        kind TEXT NOT NULL,
        reason_code TEXT NOT NULL,
        symbol TEXT NOT NULL,
        provider TEXT NOT NULL,
        market TEXT NOT NULL,
        session_date TEXT,
        detail TEXT
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS ux_unresolved_key ON unresolved (
        kind, provider, market, symbol,
        coalesce(CASE WHEN kind IN ('bar', 'corporate_action') THEN session_date END, '0001-01-01'),
        (CASE WHEN kind = 'corporate_action' THEN coalesce(detail, '') ELSE '' END)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS symbol_observations (
        instrument_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        provider TEXT NOT NULL,
        last_observed TEXT NOT NULL,
        PRIMARY KEY (instrument_id, symbol, provider)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS upstream_ids (
        instrument_id TEXT NOT NULL PRIMARY KEY,
        upstream_id INTEGER NOT NULL UNIQUE
    )
    """,
)


def _day(value: date | str | None) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


class CompatSideStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as con:
            for statement in _DDL:
                con.execute(statement)

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.path)
        con.row_factory = sqlite3.Row
        return con

    def record_unresolved(self, records: Iterable[UnresolvedRecord]) -> None:
        rows = [
            (
                r.kind, r.reason_code, r.symbol, r.provider, Market(r.market).value,
                None if r.session_date is None else r.session_date.isoformat(), r.detail,
            )
            for r in records
        ]
        if not rows:
            return
        # Expression unique index: delete-by-key then insert (portable UPSERT).
        self.discard_unresolved(records)
        with self._connect() as con:
            con.executemany(
                "INSERT INTO unresolved (kind, reason_code, symbol, provider, market, session_date, detail) "
                "VALUES (?,?,?,?,?,?,?)",
                rows,
            )

    def unresolved(self, kind: str | None = None) -> list[UnresolvedRecord]:
        sql, params = "SELECT * FROM unresolved", []
        if kind is not None:
            sql += " WHERE kind = ?"
            params.append(kind)
        with self._connect() as con:
            records = con.execute(
                sql + " ORDER BY kind, provider, symbol, session_date, detail", params
            ).fetchall()
        return [
            UnresolvedRecord(
                r["kind"], r["reason_code"], r["symbol"], r["provider"],
                _day(r["session_date"]), Market(r["market"]), r["detail"],
            )
            for r in records
        ]

    def unresolved_count(self, kind: str | None = None) -> int:
        sql, params = "SELECT count(*) AS n FROM unresolved", []
        if kind is not None:
            sql += " WHERE kind = ?"
            params.append(kind)
        with self._connect() as con:
            return int(con.execute(sql, params).fetchone()["n"])

    def discard_unresolved(self, records: Iterable[UnresolvedRecord]) -> None:
        rows = []
        for r in records:
            kind, provider, market, symbol, day, detail = r.key
            rows.append(
                (
                    kind, provider, market, symbol,
                    (day or date.min).isoformat(), detail or "",
                )
            )
        if not rows:
            return
        with self._connect() as con:
            con.executemany(
                "DELETE FROM unresolved WHERE kind = ? AND provider = ? AND market = ? AND symbol = ? "
                "AND coalesce(CASE WHEN kind IN ('bar', 'corporate_action') THEN session_date END, '0001-01-01') = ? "
                "AND (CASE WHEN kind = 'corporate_action' THEN coalesce(detail, '') ELSE '' END) = ?",
                rows,
            )

    def observe(self, observations: Iterable[tuple[str, str, str, date]]) -> None:
        rows = [(i, s, p, on.isoformat()) for i, s, p, on in observations]
        if not rows:
            return
        with self._connect() as con:
            con.executemany(
                "INSERT INTO symbol_observations (instrument_id, symbol, provider, last_observed) "
                "VALUES (?,?,?,?) ON CONFLICT (instrument_id, symbol, provider) DO UPDATE SET "
                "last_observed = max(symbol_observations.last_observed, excluded.last_observed)",
                rows,
            )

    def observations(self) -> list[tuple[str, str, str, date]]:
        with self._connect() as con:
            records = con.execute(
                "SELECT * FROM symbol_observations ORDER BY instrument_id, symbol, provider"
            ).fetchall()
        return [(r["instrument_id"], r["symbol"], r["provider"], _day(r["last_observed"])) for r in records]

    def set_upstream_id(self, instrument_id: str, upstream_id: int) -> None:
        with self._connect() as con:
            existing = {
                r["instrument_id"]: int(r["upstream_id"])
                for r in con.execute("SELECT * FROM upstream_ids").fetchall()
            }
            _check_upstream(existing, instrument_id, upstream_id)
            con.execute(
                "INSERT INTO upstream_ids (instrument_id, upstream_id) VALUES (?, ?) "
                "ON CONFLICT (instrument_id) DO UPDATE SET upstream_id = excluded.upstream_id",
                (instrument_id, int(upstream_id)),
            )

    def upstream_ids(self) -> dict[str, int]:
        with self._connect() as con:
            return {r["instrument_id"]: int(r["upstream_id"]) for r in con.execute("SELECT * FROM upstream_ids")}
