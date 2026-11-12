"""Job and checkpoint models."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from openbars.core.models import DateRange, Interval, Market


class JobKind(str, Enum):
    BACKFILL = "backfill"
    DAILY = "daily"
    REPAIR = "repair"
    VERIFY = "verify"


class JobState(str, Enum):
    PENDING = "pending"
    LEASED = "leased"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    DEAD = "dead"


WHOLE_MARKET = ("*",)


@dataclass(frozen=True)
class Job:
    job_id: str  # deterministic hash of the fields below -> idempotent
    kind: JobKind
    provider: str
    market: Market
    interval: Interval
    instruments: tuple[str, ...]  # WHOLE_MARKET for grouped endpoints
    window: DateRange
    priority: int = 100
    node: str | None = None
    state: JobState = JobState.PENDING
    attempts: int = 0
    retry_at: datetime | None = None
    last_error: str | None = None


@dataclass(frozen=True)
class Checkpoint:
    job_id: str
    cursor: str | None  # provider page token or last completed date
    updated_at: datetime


def make_job_id(
    kind: JobKind,
    provider: str,
    market: Market,
    interval: Interval,
    instruments: tuple[str, ...] | list[str],
    window: DateRange,
) -> str:
    """Deterministic job id: sha256 (first 24 hex chars) of a canonical JSON form.

    The same logical job always maps to the same id, which makes ``submit`` idempotent.
    """
    canonical = json.dumps(
        [
            JobKind(kind).value,
            provider,
            Market(market).value,
            Interval(interval).value,
            list(instruments),
            window.start.isoformat(),
            window.end.isoformat(),
        ],
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:24]
