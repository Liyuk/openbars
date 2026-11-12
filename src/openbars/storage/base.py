"""StorageBackend contract."""

from __future__ import annotations

from typing import Iterable, Protocol, Sequence

from openbars.core.models import CanonicalBar, DateRange, Interval
from openbars.normalize.instruments import InstrumentMaster
from openbars.orchestrator.jobs import Checkpoint, Job
from openbars.raw.store import RawStore


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


class StorageBackend(Protocol):
    raw: RawStore
    canonical: CanonicalStore
    instruments: InstrumentMaster
    state: StateStore
