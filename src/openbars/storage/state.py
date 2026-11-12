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
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence

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

    # --- scheduling: submit / lease / transition -------------------------------------
    # ``updated_at`` doubles as the lease heartbeat of leased/running rows; ``retry_at`` is
    # only the earliest next attempt. Every lease write takes the caller's ``now`` so a
    # test clock and the stored timestamps agree.

    def _begin_write(self, con: Any) -> None:
        """Hook: take the write lock before a read-then-write sequence (SQLite)."""

    def _insert_job_sql(self) -> str:
        return """
            INSERT INTO jobs (job_id, kind, provider, market, "interval", instruments,
              window_start, window_end, priority, node, state, attempts, retry_at,
              last_error, created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(job_id) DO NOTHING
            """

    def _job_params(self, job: Job, now: str | None) -> tuple[Any, ...]:
        return (
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
        )

    def submit_jobs(self, jobs: Iterable[Job]) -> int:
        """Insert jobs that do not exist yet; existing rows (any state) are left untouched.

        Returns the number of newly inserted jobs.
        """
        now = self._enc_ts(_now())
        inserted = 0
        with self._connect() as con:
            self._begin_write(con)
            for job in jobs:
                cur = con.execute(self._insert_job_sql(), self._job_params(job, now))
                inserted += 1 if cur.rowcount == 1 else 0
        return inserted

    def submit_job(self, job: Job) -> bool:
        return self.submit_jobs([job]) == 1

    def claim_job(
        self,
        *,
        now: datetime,
        exclude_providers: Sequence[str] = (),
        providers: Sequence[str] | None = None,
    ) -> Job | None:
        """Atomically lease the best runnable job (pending, or failed with ``retry_at`` due).

        Order is ``priority, created_at, job_id``. The row becomes ``leased`` with
        ``updated_at = now``; the conditional UPDATE guarantees two workers never claim the
        same job, even where ``_begin_write`` is a no-op.
        """
        stamp = self._enc_ts(now)
        sql = "SELECT * FROM jobs WHERE state IN ('pending','failed') AND (retry_at IS NULL OR retry_at <= ?)"
        params: list[Any] = [stamp]
        if exclude_providers:
            sql += f" AND provider NOT IN ({', '.join('?' for _ in exclude_providers)})"
            params.extend(exclude_providers)
        if providers is not None:
            sql += f" AND provider IN ({', '.join('?' for _ in providers) or 'NULL'})"
            params.extend(providers)
        sql += " ORDER BY priority, created_at, job_id LIMIT 1"
        with self._connect() as con:
            self._begin_write(con)
            row = con.execute(sql, params).fetchone()
            if row is None:
                return None
            cur = con.execute(
                "UPDATE jobs SET state='leased', updated_at=? "
                "WHERE job_id=? AND state IN ('pending','failed')",
                (stamp, row["job_id"]),
            )
            if cur.rowcount != 1:
                return None
        return replace(_job(row), state=JobState.LEASED)

    def start_job(self, job_id: str, *, now: datetime) -> bool:
        """leased -> running (also refreshes the heartbeat)."""
        with self._connect() as con:
            cur = con.execute(
                "UPDATE jobs SET state='running', updated_at=? WHERE job_id=? AND state IN ('leased','running')",
                (self._enc_ts(now), job_id),
            )
            return cur.rowcount == 1

    def touch_job(self, job_id: str, *, now: datetime) -> bool:
        """Heartbeat for a leased/running job."""
        with self._connect() as con:
            cur = con.execute(
                "UPDATE jobs SET updated_at=? WHERE job_id=? AND state IN ('leased','running')",
                (self._enc_ts(now), job_id),
            )
            return cur.rowcount == 1

    def finish_job(
        self,
        job_id: str,
        state: JobState,
        *,
        now: datetime,
        attempts: int,
        retry_at: datetime | None = None,
        last_error: str | None = None,
    ) -> bool:
        """Leave leased/running for ``done`` / ``failed`` / ``dead`` (or back to ``pending``).

        A no-op returning False when the lease was lost (e.g. reaped) in the meantime.
        """
        with self._connect() as con:
            cur = con.execute(
                "UPDATE jobs SET state=?, attempts=?, retry_at=?, last_error=?, updated_at=? "
                "WHERE job_id=? AND state IN ('leased','running')",
                (
                    JobState(state).value,
                    attempts,
                    self._enc_ts(retry_at),
                    last_error,
                    self._enc_ts(now),
                    job_id,
                ),
            )
            return cur.rowcount == 1

    def reap_expired_leases(
        self, *, now: datetime, ttl: timedelta = timedelta(seconds=60), max_attempts: int | None = None
    ) -> int:
        """Recover leased/running rows whose heartbeat is older than ``ttl``.

        They return to ``pending`` with ``attempts + 1`` (a crashed worker counts as an
        attempt so a poison job cannot loop forever); once that reaches ``max_attempts``
        the job becomes ``dead``. Returns the number of recovered rows.
        """
        cutoff = self._enc_ts(now - ttl)
        limit = max_attempts if max_attempts is not None else 2**31 - 1
        with self._connect() as con:
            self._begin_write(con)
            cur = con.execute(
                """
                UPDATE jobs SET
                  state = CASE WHEN attempts + 1 >= ? THEN 'dead' ELSE 'pending' END,
                  attempts = attempts + 1,
                  retry_at = NULL,
                  last_error = 'lease expired (worker lost)',
                  updated_at = ?
                WHERE state IN ('leased','running') AND updated_at < ?
                """,
                (limit, self._enc_ts(now), cutoff),
            )
            return cur.rowcount

    def count_jobs(self) -> dict[JobState, int]:
        with self._connect() as con:
            rows = con.execute("SELECT state, COUNT(*) AS n FROM jobs GROUP BY state").fetchall()
        counts = {state: 0 for state in JobState}
        for row in rows:
            counts[JobState(row["state"])] = row["n"]
        return counts

    def list_checkpoints(self) -> list[Checkpoint]:
        with self._connect() as con:
            rows = con.execute("SELECT * FROM checkpoints ORDER BY job_id").fetchall()
        return [
            Checkpoint(job_id=r["job_id"], cursor=r["cursor"], updated_at=_parse_ts(r["updated_at"]))
            for r in rows
        ]

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

    def _begin_write(self, con: sqlite3.Connection) -> None:
        # Take the write lock up front so read-then-write claims cannot interleave.
        con.execute("BEGIN IMMEDIATE")

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        con = sqlite3.connect(self.path, timeout=30)
        con.row_factory = sqlite3.Row
        try:
            with con:  # commit / rollback
                yield con
        finally:
            con.close()
