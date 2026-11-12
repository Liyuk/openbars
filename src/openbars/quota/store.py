"""Pluggable persistence for the quota gate, and the default SQLite store.

A store provides one primitive: an exclusive transaction over one ``(provider, scope)``
gate row. Inside it the gate reads the row plus the recent usage ledger, decides, and
(if granting) records the reservation. SQLite takes ``BEGIN IMMEDIATE`` (a write lock on
the whole file); Postgres takes ``SELECT ... FOR UPDATE`` on the gate row. Either way two
processes can never be granted the same capacity.

State lives in tables named ``quota_gate`` / ``quota_usage`` so it can share one
``state.db`` with the job, checkpoint and fetch-log tables of the state store.
"""

from __future__ import annotations

import sqlite3
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import ContextManager, Iterator, Protocol, Sequence

# Gate row that carries a provider-wide penalty (``penalize`` without a scope).
PROVIDER_WIDE_SCOPE = "*"

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_MICROSECOND = timedelta(microseconds=1)


def to_micros(moment: datetime) -> int:
    if moment.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return (moment.astimezone(UTC) - _EPOCH) // _MICROSECOND


def from_micros(value: int) -> datetime:
    return _EPOCH + value * _MICROSECOND


@dataclass(frozen=True)
class GateState:
    next_request_at: datetime | None = None
    penalized_until: datetime | None = None  # max of scope-level and provider-wide


@dataclass(frozen=True)
class UsageRow:
    reserved_at: datetime
    weight: int


class QuotaTransaction(Protocol):
    def load(self, since: datetime) -> tuple[GateState, Sequence[UsageRow]]:
        """Gate state plus usage with ``reserved_at >= since`` (ascending)."""
        ...

    def record(
        self, slot: datetime, weight: int, next_request_at: datetime, prune_before: datetime
    ) -> None:
        """Persist a reservation (``weight == 0`` only moves the gate) and drop old usage."""
        ...


class QuotaStore(Protocol):
    def transaction(self, provider: str, scope: str) -> ContextManager[QuotaTransaction]:
        """Exclusive transaction; rolls back if the block raises."""
        ...

    def penalize(self, provider: str, until: datetime, scope: str | None = None) -> None:
        """Persist ``penalized_until = max(existing, until)``; ``scope=None`` = whole provider."""
        ...

    def read(
        self, provider: str, scope: str, since: datetime
    ) -> tuple[GateState, Sequence[UsageRow]]:
        """Non-exclusive view, for snapshots."""
        ...

    def scopes(self, provider: str) -> Sequence[str]:
        """Scopes with persisted state (excluding the provider-wide row)."""
        ...

    def close(self) -> None: ...


_SCHEMA = """
CREATE TABLE IF NOT EXISTS quota_gate (
    provider TEXT NOT NULL,
    scope TEXT NOT NULL,
    next_request_at INTEGER NOT NULL DEFAULT 0,   -- microseconds since the Unix epoch (UTC)
    penalized_until INTEGER,                      -- same unit; NULL = none
    PRIMARY KEY (provider, scope)
);
CREATE TABLE IF NOT EXISTS quota_usage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider TEXT NOT NULL,
    scope TEXT NOT NULL,
    reserved_at INTEGER NOT NULL,
    weight INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_quota_usage_window ON quota_usage (provider, scope, reserved_at);
"""


class _SqliteTransaction:
    def __init__(self, conn: sqlite3.Connection, provider: str, scope: str) -> None:
        self._conn = conn
        self._provider = provider
        self._scope = scope

    def load(self, since: datetime) -> tuple[GateState, Sequence[UsageRow]]:
        return _read_state(self._conn, self._provider, self._scope, since)

    def record(
        self, slot: datetime, weight: int, next_request_at: datetime, prune_before: datetime
    ) -> None:
        conn, provider, scope = self._conn, self._provider, self._scope
        conn.execute(
            "INSERT INTO quota_gate (provider, scope, next_request_at) VALUES (?, ?, ?) "
            "ON CONFLICT (provider, scope) DO UPDATE SET next_request_at = excluded.next_request_at",
            (provider, scope, to_micros(next_request_at)),
        )
        if weight > 0:
            conn.execute(
                "INSERT INTO quota_usage (provider, scope, reserved_at, weight) VALUES (?, ?, ?, ?)",
                (provider, scope, to_micros(slot), weight),
            )
        conn.execute(
            "DELETE FROM quota_usage WHERE provider = ? AND scope = ? AND reserved_at < ?",
            (provider, scope, to_micros(prune_before)),
        )


def _read_state(
    conn: sqlite3.Connection, provider: str, scope: str, since: datetime
) -> tuple[GateState, Sequence[UsageRow]]:
    rows = conn.execute(
        "SELECT scope, next_request_at, penalized_until FROM quota_gate "
        "WHERE provider = ? AND scope IN (?, ?)",
        (provider, scope, PROVIDER_WIDE_SCOPE),
    ).fetchall()
    next_at: datetime | None = None
    penalties: list[int] = []
    for row_scope, next_value, penalty in rows:
        if row_scope == scope and next_value:
            next_at = from_micros(next_value)
        if penalty is not None:
            penalties.append(penalty)
    usage = conn.execute(
        "SELECT reserved_at, weight FROM quota_usage "
        "WHERE provider = ? AND scope = ? AND reserved_at >= ? ORDER BY reserved_at, id",
        (provider, scope, to_micros(since)),
    ).fetchall()
    state = GateState(next_at, from_micros(max(penalties)) if penalties else None)
    return state, [UsageRow(from_micros(at), weight) for at, weight in usage]


class SqliteQuotaStore:
    """SQLite-backed store; safe across threads and processes sharing the file."""

    def __init__(self, path: str | Path, *, busy_timeout: float = 60.0) -> None:
        self.path = str(path)
        self._lock = threading.Lock()
        # isolation_level=None: we issue BEGIN/COMMIT ourselves.
        self._conn = sqlite3.connect(
            self.path, timeout=busy_timeout, isolation_level=None, check_same_thread=False
        )
        self._conn.execute(f"PRAGMA busy_timeout={int(busy_timeout * 1000)}")
        self._init_schema(busy_timeout)

    def _init_schema(self, busy_timeout: float) -> None:
        # Switching to WAL and creating tables can report "locked" immediately (the busy
        # handler does not cover every step) when other processes open the file at the
        # same moment, so retry briefly.
        deadline = time.monotonic() + busy_timeout
        while True:
            try:
                if self.path != ":memory:":
                    mode = self._conn.execute("PRAGMA journal_mode").fetchone()[0]
                    if str(mode).lower() != "wal":
                        self._conn.execute("PRAGMA journal_mode=WAL")
                    self._conn.execute("PRAGMA synchronous=NORMAL")
                self._conn.executescript(_SCHEMA)
                return
            except sqlite3.OperationalError as error:
                if "locked" not in str(error) or time.monotonic() >= deadline:
                    raise
                time.sleep(0.02)

    @contextmanager
    def _write_transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._conn
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            else:
                self._conn.execute("COMMIT")

    @contextmanager
    def transaction(self, provider: str, scope: str) -> Iterator[QuotaTransaction]:
        with self._write_transaction() as conn:
            yield _SqliteTransaction(conn, provider, scope)

    def penalize(self, provider: str, until: datetime, scope: str | None = None) -> None:
        target = PROVIDER_WIDE_SCOPE if scope is None else scope
        value = to_micros(until)
        with self._write_transaction() as conn:
            conn.execute(
                "INSERT INTO quota_gate (provider, scope, penalized_until) VALUES (?, ?, ?) "
                "ON CONFLICT (provider, scope) DO UPDATE SET penalized_until = "
                "MAX(COALESCE(quota_gate.penalized_until, 0), excluded.penalized_until)",
                (provider, target, value),
            )

    def read(
        self, provider: str, scope: str, since: datetime
    ) -> tuple[GateState, Sequence[UsageRow]]:
        with self._lock:
            return _read_state(self._conn, provider, scope, since)

    def scopes(self, provider: str) -> Sequence[str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT scope FROM quota_gate WHERE provider = ? AND scope != ? ORDER BY scope",
                (provider, PROVIDER_WIDE_SCOPE),
            ).fetchall()
        return [row[0] for row in rows]

    def close(self) -> None:
        with self._lock:
            self._conn.close()
