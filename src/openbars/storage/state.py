"""StateStore: jobs, checkpoints and fetch_log (SQLite ``data/state.db`` by default).

Table names/columns come from ``schema.STATE_TABLES``. ``quota_*`` tables are reserved for
the QuotaGate and are not created here. ``SqlStateStore`` holds all logic written with
``?`` placeholders; ``SqliteStateStore`` stores dates/timestamps as ISO-8601 text (UTC;
naive datetimes are interpreted as UTC) and the Postgres backend subclasses it with
native types.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Iterator

from openbars.core.models import DateRange, Interval, Market
from openbars.orchestrator.jobs import Checkpoint, Job, JobKind, JobState
from openbars.storage.state_schema import FETCH_STATUSES, SCHEMA_VERSION, state_ddl


@dataclass(frozen=True)
class FetchLogEntry:
    provider: str
    fetch_id: str
    market: Market
    interval: Interval
    window: DateRange
    status: str
    started_at: datetime
    job_id: str | None = None
    artifact_ref: str | None = None
    row_count: int | None = None
    error_code: str | None = None
    error_message: str | None = None
    finished_at: datetime | None = None


def _ts(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()


def _parse_ts(value: Any) -> datetime | None:
    if value is None or isinstance(value, datetime):
        return value
    return datetime.fromisoformat(value)


def _parse_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    return value if isinstance(value, date) else date.fromisoformat(value)


def _now() -> datetime:
    return datetime.now(UTC)


class SqlStateStore:
    """Dialect-neutral state store; subclasses provide ``_connect``."""

    def _connect(self) -> Any:
        """Context manager yielding a connection whose ``execute(sql, params)`` takes ``?``
        placeholders and returns a cursor of mapping-like rows (commit on success)."""
        raise NotImplementedError

    # ISO strings are accepted by SQLite (TEXT) and cast implicitly by Postgres
    # (DATE / TIMESTAMPTZ / JSONB), so both dialects share these encoders.
    @staticmethod
    def _enc_ts(value: datetime | None) -> str | None:
        return _ts(value)

    @staticmethod
    def _enc_date(value: date) -> str:
        return value.isoformat()

    # --- jobs ---

    def save_job(self, job: Job) -> None:
        now = self._enc_ts(_now())
        with self._connect() as con:
            con.execute(
                """
                INSERT INTO jobs (job_id, kind, provider, market, "interval", instruments,
                  window_start, window_end, priority, node, state, attempts, retry_at,
                  last_error, created_at, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(job_id) DO UPDATE SET
                  kind=excluded.kind, provider=excluded.provider, market=excluded.market,
                  "interval"=excluded."interval", instruments=excluded.instruments,
                  window_start=excluded.window_start, window_end=excluded.window_end,
                  priority=excluded.priority, node=excluded.node, state=excluded.state,
                  attempts=excluded.attempts, retry_at=excluded.retry_at,
                  last_error=excluded.last_error, updated_at=excluded.updated_at
                """,
                (
                    job.job_id,
                    JobKind(job.kind).value,
                    job.provider,
                    Market(job.market).value,
                    Interval(job.interval).value,
                    json.dumps(list(job.instruments)),
                    self._enc_date(job.window.start),
                    self._enc_date(job.window.end),
                    job.priority,
                    job.node,
                    JobState(job.state).value,
                    job.attempts,
                    self._enc_ts(job.retry_at),
                    job.last_error,
                    now,
                    now,
                ),
            )

    def load_job(self, job_id: str) -> Job | None:
        with self._connect() as con:
            row = con.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        return None if row is None else _job(row)

    def list_jobs(self, *, state: JobState | None = None, provider: str | None = None) -> list[Job]:
        sql, params = "SELECT * FROM jobs WHERE 1=1", []
        if state is not None:
            sql += " AND state = ?"
            params.append(JobState(state).value)
        if provider is not None:
            sql += " AND provider = ?"
            params.append(provider)
        sql += " ORDER BY priority, created_at, job_id"
        with self._connect() as con:
            return [_job(r) for r in con.execute(sql, params).fetchall()]

    # --- checkpoints ---

    def save_checkpoint(self, checkpoint: Checkpoint) -> None:
        with self._connect() as con:
            con.execute(
                """
                INSERT INTO checkpoints (job_id, cursor, updated_at) VALUES (?,?,?)
                ON CONFLICT(job_id) DO UPDATE SET cursor=excluded.cursor, updated_at=excluded.updated_at
                """,
                (checkpoint.job_id, checkpoint.cursor, self._enc_ts(checkpoint.updated_at)),
            )

    def load_checkpoint(self, job_id: str) -> Checkpoint | None:
        with self._connect() as con:
            row = con.execute("SELECT * FROM checkpoints WHERE job_id = ?", (job_id,)).fetchone()
        if row is None:
            return None
        return Checkpoint(job_id=row["job_id"], cursor=row["cursor"], updated_at=_parse_ts(row["updated_at"]))

    # --- fetch_log ---

    def start_fetch(
        self,
        *,
        provider: str,
        fetch_id: str,
        market: Market,
        interval: Interval,
        window: DateRange,
        job_id: str | None = None,
        started_at: datetime | None = None,
    ) -> None:
        """Record a request about to be sent. Re-starting the same fetch resets it."""
        with self._connect() as con:
            con.execute(
                """
                INSERT INTO fetch_log (provider, fetch_id, job_id, market, "interval",
                  window_start, window_end, status, started_at)
                VALUES (?,?,?,?,?,?,?,'started',?)
                ON CONFLICT(provider, fetch_id) DO UPDATE SET
                  job_id=excluded.job_id, status='started', started_at=excluded.started_at,
                  artifact_ref=NULL, row_count=NULL, error_code=NULL, error_message=NULL,
                  finished_at=NULL
                """,
                (
                    provider,
                    fetch_id,
                    job_id,
                    Market(market).value,
                    Interval(interval).value,
                    self._enc_date(window.start),
                    self._enc_date(window.end),
                    self._enc_ts(started_at or _now()),
                ),
            )

    def finish_fetch(
        self,
        provider: str,
        fetch_id: str,
        status: str,
        *,
        artifact_ref: str | None = None,
        row_count: int | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
        finished_at: datetime | None = None,
    ) -> None:
        if status not in FETCH_STATUSES or status == "started":
            raise ValueError(f"invalid terminal fetch status {status!r}")
        with self._connect() as con:
            cur = con.execute(
                """
                UPDATE fetch_log SET status=?, artifact_ref=?, row_count=?, error_code=?,
                  error_message=?, finished_at=? WHERE provider=? AND fetch_id=?
                """,
                (
                    status,
                    artifact_ref,
                    row_count,
                    error_code,
                    error_message,
                    self._enc_ts(finished_at or _now()),
                    provider,
                    fetch_id,
                ),
            )
            if cur.rowcount == 0:
                raise KeyError(f"unknown fetch {provider}/{fetch_id}")

    def get_fetch(self, provider: str, fetch_id: str) -> FetchLogEntry | None:
        with self._connect() as con:
            row = con.execute(
                "SELECT * FROM fetch_log WHERE provider=? AND fetch_id=?", (provider, fetch_id)
            ).fetchone()
        return None if row is None else _fetch(row)

    def list_fetches(
        self,
        *,
        provider: str | None = None,
        status: str | None = None,
        market: Market | None = None,
        interval: Interval | None = None,
    ) -> list[FetchLogEntry]:
        sql, params = "SELECT * FROM fetch_log WHERE 1=1", []
        for column, value in (
            ("provider", provider),
            ("status", status),
            ("market", None if market is None else Market(market).value),
            ('"interval"', None if interval is None else Interval(interval).value),
        ):
            if value is not None:
                sql += f" AND {column} = ?"
                params.append(value)
        sql += " ORDER BY started_at, fetch_id"
        with self._connect() as con:
            return [_fetch(r) for r in con.execute(sql, params).fetchall()]


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, (str, bytes)) else value


def _job(row: Any) -> Job:
    return Job(
        job_id=row["job_id"],
        kind=JobKind(row["kind"]),
        provider=row["provider"],
        market=Market(row["market"]),
        interval=Interval(row["interval"]),
        instruments=tuple(_json(row["instruments"])),
        window=DateRange(_parse_date(row["window_start"]), _parse_date(row["window_end"])),
        priority=row["priority"],
        node=row["node"],
        state=JobState(row["state"]),
        attempts=row["attempts"],
        retry_at=_parse_ts(row["retry_at"]),
        last_error=row["last_error"],
    )


def _fetch(row: Any) -> FetchLogEntry:
    return FetchLogEntry(
        provider=row["provider"],
        fetch_id=row["fetch_id"],
        market=Market(row["market"]),
        interval=Interval(row["interval"]),
        window=DateRange(_parse_date(row["window_start"]), _parse_date(row["window_end"])),
        status=row["status"],
        started_at=_parse_ts(row["started_at"]),
        job_id=row["job_id"],
        artifact_ref=row["artifact_ref"],
        row_count=row["row_count"],
        error_code=row["error_code"],
        error_message=row["error_message"],
        finished_at=_parse_ts(row["finished_at"]),
    )


class SqliteStateStore(SqlStateStore):
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as con:
            con.execute("PRAGMA journal_mode=WAL")
            for statement in state_ddl("sqlite"):
                con.execute(statement)
            con.execute(
                "INSERT OR IGNORE INTO schema_meta(key, value) VALUES ('schema_version', ?)",
                (str(SCHEMA_VERSION),),
            )

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        con = sqlite3.connect(self.path, timeout=30)
        con.row_factory = sqlite3.Row
        try:
            with con:  # commit / rollback
                yield con
        finally:
            con.close()
