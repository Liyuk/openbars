"""Run completion status files (``openbars.run-status`` schema v1, issue #219).

Every ``openbars run`` ends by writing three kinds of JSON documents under
``<storage.path>/_status/``:

* ``runs/<run_id>.json`` -- one document per invocation; never overwritten;
* ``latest.json`` -- a copy of the most recent document;
* ``sessions/<market>/<interval>/<session_date>.json`` -- the most recent result for that
  session, so a single session can be queried by date.

All writes are atomic (temp file in the same directory, ``fsync``, ``os.replace``): a reader
sees the previous document or the new one, never a truncated file. A write failure is reported
as a warning and never changes the exit code of ``run``.

The document is a published contract; see ``docs/schemas/run-status.v1.json`` and
``docs/run-status.md``. It contains no credentials, no DSNs, no absolute user paths and no
configured webhook URLs -- only counters, session outcomes and hashed webhook identifiers.
"""

from __future__ import annotations

import json
import secrets
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from openbars.core.atomic import atomic_write

SCHEMA = "openbars.run-status"
SCHEMA_VERSION = 1
STATUS_DIR = "_status"
# Only ``final`` exists today (provisional same-day data is Discussion #178, not implemented).
TIER_FINAL = "final"
SESSION_OUTCOMES = ("success", "failed")
RUN_OUTCOMES = ("success", "partial", "failed", "skipped", "noop")


def new_run_id(started_at: datetime, token: str | None = None) -> str:
    """``<UTC compact timestamp>-<6 hex>``, e.g. ``20261117T051502Z-ab12cd``."""
    moment = started_at.astimezone(UTC)
    suffix = token if token is not None else secrets.token_hex(3)
    return f"{moment.strftime('%Y%m%dT%H%M%SZ')}-{suffix}"


def iso_utc(moment: datetime) -> str:
    """Second-resolution UTC ISO 8601 with a trailing ``Z``."""
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class _Session:
    market: str
    interval: str
    session_date: date
    raw: int = 0
    canonical: int = 0
    failed: bool = False
    tier: str = TIER_FINAL
    providers: set[str] = field(default_factory=set)

    def as_dict(self) -> dict[str, Any]:
        return {
            "market": self.market,
            "interval": self.interval,
            "session_date": self.session_date.isoformat(),
            "tier": self.tier,
            "outcome": "failed" if self.failed else "success",
            "rows": {"raw": self.raw, "canonical": self.canonical},
            "providers": sorted(self.providers),
        }


def _text(value: object) -> str:
    """Enum-or-string to its plain value (``Market.US`` -> ``us``)."""
    return str(getattr(value, "value", value))


class RunStatusRecorder:
    """Thread-safe tally of what a single ``run`` wrote, turned into the status document.

    ``run`` feeds it in two places: the collector node records the raw bars it stored and the
    canonical sink records the rows it reconciled. Sessions the run planned but did not finish
    are recorded as ``failed`` by the CLI from the state store.
    """

    def __init__(
        self,
        *,
        started_at: datetime,
        storage_backend: str = "parquet_duckdb",
        command: str = "run",
        run_id: str | None = None,
    ) -> None:
        import threading

        self.started_at = started_at.astimezone(UTC)
        self.storage_backend = storage_backend
        self.command = command
        self.run_id = run_id or new_run_id(self.started_at)
        self._sessions: dict[tuple[str, str, date], _Session] = {}
        self._lock = threading.Lock()

    # -- recording ---------------------------------------------------------------------
    def _session(self, market: object, interval: object, session_date: date) -> _Session:
        key = (_text(market), _text(interval), session_date)
        entry = self._sessions.get(key)
        if entry is None:
            entry = _Session(key[0], key[1], session_date)
            self._sessions[key] = entry
        return entry

    def record_raw_bars(self, market: object, interval: object, bars: Iterable[Any]) -> None:
        """Count raw bars written by one fetch (their own ``interval`` / ``session_date`` win)."""
        with self._lock:
            for bar in bars:
                day = bar.session_date or bar.ts_start.date()
                entry = self._session(market, getattr(bar, "interval", interval), day)
                entry.raw += 1
                provider = getattr(bar, "provider", None)
                if provider:
                    entry.providers.add(str(provider))

    def record_canonical_rows(
        self, rows: Iterable[Any], *, market: object | None = None, interval: object | None = None
    ) -> None:
        """Count canonical rows written by one sink call.

        ``market`` / ``interval`` are the fallbacks for row types that only carry the session
        date (the reconcilers hand ``CanonicalDailyRow`` to a ``(rows, market)`` writer).
        """
        with self._lock:
            for row in rows:
                day = getattr(row, "session_date", None)
                row_market = getattr(row, "market", None) or market
                row_interval = getattr(row, "interval", None) or interval or "1d"
                if day is None or row_market is None:
                    continue
                entry = self._session(row_market, row_interval, day)
                entry.canonical += 1
                provider = getattr(row, "provider", None)
                if provider:
                    entry.providers.add(str(provider))

    def wrap_canonical_writer(self, writer: Callable[..., Any]) -> Callable[..., Any]:
        """Wrap a ``writer(rows, market)`` so it tallies before writing."""

        def wrapped(rows: Sequence[Any], market: Any) -> Any:
            self.record_canonical_rows(rows, market=market)
            return writer(rows, market)

        return wrapped

    def wrap_rows_writer(self, writer: Callable[..., Any]) -> Callable[..., Any]:
        """Wrap a single-argument ``writer(rows)`` so it tallies before writing."""

        def wrapped(rows: Sequence[Any]) -> Any:
            self.record_canonical_rows(rows)
            return writer(rows)

        return wrapped

    def mark_job_failed(
        self, market: object, interval: object, session_date: date, provider: object | None = None
    ) -> None:
        """Record that the jobs targeting ``session_date`` did not finish (dead or retrying)."""
        with self._lock:
            entry = self._session(market, interval, session_date)
            entry.failed = True
            if provider:
                entry.providers.add(str(provider))

    def sessions(self) -> list[dict[str, Any]]:
        with self._lock:
            ordered = sorted(self._sessions.values(), key=lambda s: (s.market, s.interval, s.session_date))
            return [entry.as_dict() for entry in ordered]

    # -- document ----------------------------------------------------------------------
    def document(
        self,
        *,
        finished_at: datetime,
        jobs: Mapping[str, int],
        error: str | None = None,
    ) -> dict[str, Any]:
        sessions = self.sessions()
        return {
            "schema": SCHEMA,
            "schema_version": SCHEMA_VERSION,
            "run_id": self.run_id,
            "command": self.command,
            "started_at": iso_utc(self.started_at),
            "finished_at": iso_utc(finished_at),
            "outcome": overall_outcome(jobs),
            "jobs": {
                "planned": int(jobs.get("planned", 0)),
                "succeeded": int(jobs.get("succeeded", 0)),
                "failed": int(jobs.get("failed", 0)),
                "skipped": int(jobs.get("skipped", 0)),
            },
            "sessions": sessions,
            "storage": {"backend": self.storage_backend},
            "error": error,
            # Filled in after delivery; the signed payload is the document without this key.
            "notifications": [],
        }


def overall_outcome(jobs: Mapping[str, int]) -> str:
    """``noop`` / ``skipped`` / ``success`` / ``partial`` / ``failed``.

    ``noop`` is a run with nothing planned. ``skipped`` is a run that planned work but
    executed none of it (for example the non-daily request budget was not accepted); it is
    deliberately not folded into ``noop``, because a consumer waiting for fresh data must be
    able to tell "nothing was due" from "work was blocked".
    """
    planned = int(jobs.get("planned", 0))
    succeeded = int(jobs.get("succeeded", 0))
    failed = int(jobs.get("failed", 0))
    skipped = int(jobs.get("skipped", 0))
    if planned == 0 and succeeded == 0 and failed == 0 and skipped == 0:
        return "noop"
    if failed == 0:
        if planned > 0 and succeeded == 0 and skipped >= planned:
            return "skipped"
        return "success"
    return "partial" if succeeded else "failed"


# -- serialisation ---------------------------------------------------------------------


def render_document(document: Mapping[str, Any]) -> bytes:
    """Canonical bytes for a document: UTF-8, indented, trailing newline.

    The single serialiser for the status files and for the webhook payload (see
    :func:`signature_payload`, which drops ``notifications`` before rendering).
    """
    return (json.dumps(document, indent=2, ensure_ascii=False) + "\n").encode("utf-8")


def signature_payload(document: Mapping[str, Any]) -> bytes:
    """The body that is signed and posted: the document without the ``notifications`` key."""
    return render_document({k: v for k, v in document.items() if k != "notifications"})


def _write(path: Path, body: bytes) -> Path:
    atomic_write(path, lambda handle: handle.write(body))
    return path


# -- locations -------------------------------------------------------------------------


def status_root(root: Path) -> Path:
    return Path(root) / STATUS_DIR


def latest_status_path(root: Path) -> Path:
    return status_root(root) / "latest.json"


def run_status_path(root: Path, run_id: str) -> Path:
    return status_root(root) / "runs" / f"{run_id}.json"


def session_status_path(root: Path, market: str, interval: str, session_date: str) -> Path:
    return status_root(root) / "sessions" / market / interval / f"{session_date}.json"


def session_document(document: Mapping[str, Any], session: Mapping[str, Any]) -> dict[str, Any]:
    """A session-scoped copy: the same fields with only that one session."""
    scoped = {key: value for key, value in document.items() if key not in ("sessions", "notifications")}
    scoped["sessions"] = [dict(session)]
    scoped["notifications"] = []
    return scoped


def write_status_copies(root: Path, document: Mapping[str, Any]) -> list[Path]:
    """Rewrite ``runs/<run_id>.json`` and ``latest.json`` (used after deliveries are known)."""
    body = render_document(document)
    return [
        _write(run_status_path(root, str(document["run_id"])), body),
        _write(latest_status_path(root), body),
    ]


def write_run_status(root: Path, document: Mapping[str, Any]) -> tuple[bytes, list[Path]]:
    """Write ``runs/<run_id>.json``, ``latest.json`` and the per-session copies.

    Returns the exact bytes of ``runs/<run_id>.json`` (the webhook payload) and the paths
    written, in write order. Raises ``OSError`` if the filesystem refuses a write; callers
    treat that as a warning.
    """
    body = render_document(document)
    written = [_write(run_status_path(root, str(document["run_id"])), body)]
    written.append(_write(latest_status_path(root), body))
    for session in document.get("sessions", ()):
        scoped = session_document(document, session)
        path = session_status_path(
            root, str(session["market"]), str(session["interval"]), str(session["session_date"])
        )
        written.append(_write(path, render_document(scoped)))
    return body, written


def read_latest_status(root: Path) -> dict[str, Any] | None:
    """The parsed ``latest.json``, or ``None`` when no run has written one yet."""
    path = latest_status_path(root)
    if not path.is_file():
        return None
    return json.loads(path.read_text(encoding="utf-8"))
