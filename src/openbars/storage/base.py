"""StorageBackend contract."""

from __future__ import annotations

from datetime import datetime
from typing import Iterable, Protocol, Sequence

from openbars.core.models import CanonicalBar, DateRange, Interval, Market
from openbars.normalize.instruments import InstrumentMaster
from openbars.orchestrator.jobs import Checkpoint, Job
from openbars.raw.store import RawStore
from openbars.storage.state import FetchLogEntry


class CanonicalStore(Protocol):
    def upsert(self, bars: Sequence[CanonicalBar]) -> None: ...

    def scan(
        self, instrument_ids: Sequence[str], interval: Interval, window: DateRange
    ) -> Iterable[CanonicalBar]: ...


class StateStore(Protocol):
    """Jobs, checkpoints, quota buckets and fetch_log (SQLite by default)."""

    def save_job(self, job: Job) -> None: ...

    def save_checkpoint(self, checkpoint: Checkpoint) -> None: ...

    def load_checkpoint(self, job_id: str) -> Checkpoint | None: ...

    # fetch_log: one row per provider request (provider, fetch_id, window, status, artifact)
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
    ) -> None: ...

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
    ) -> None: ...

    def get_fetch(self, provider: str, fetch_id: str) -> FetchLogEntry | None: ...


class StorageBackend(Protocol):
    raw: RawStore
    canonical: CanonicalStore
    instruments: InstrumentMaster
    state: StateStore
