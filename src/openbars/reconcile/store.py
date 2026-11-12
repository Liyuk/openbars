"""Canonical-layer persistence interface and an in-memory implementation.

Real backends (Parquet/DuckDB, Postgres) live in ``openbars.storage``; this module only
defines what the reconciler needs and keeps everything in plain dicts for tests.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date
from typing import Protocol

from openbars.core.series import CanonicalDailyRow
from openbars.reconcile.audit import AuditRecord
from openbars.reconcile.gaps import BackfillJob, KnownGap

RowKey = tuple[str, date, str]  # (instrument_id, session_date, policy_version)


class CanonicalRowStore(Protocol):
    def get(self, key: RowKey) -> CanonicalDailyRow | None: ...

    def put(self, row: CanonicalDailyRow, audit: AuditRecord) -> None: ...

    def add_backfill_jobs(self, jobs: Iterable[BackfillJob]) -> None: ...

    def add_known_gaps(self, gaps: Iterable[KnownGap]) -> None: ...


class InMemoryCanonicalStore:
    """Dict-backed store. ``put`` overwrites; superseded rows are kept in ``history``."""

    def __init__(self) -> None:
        self._rows: dict[RowKey, CanonicalDailyRow] = {}
        self._history: dict[RowKey, list[CanonicalDailyRow]] = {}
        self._audit: list[AuditRecord] = []
        self._jobs: dict[tuple[str, date], BackfillJob] = {}
        self._gaps: dict[tuple[str, date], KnownGap] = {}

    def get(self, key: RowKey) -> CanonicalDailyRow | None:
        return self._rows.get(key)

    def put(self, row: CanonicalDailyRow, audit: AuditRecord) -> None:
        previous = self._rows.get(row.unique_key)
        if previous is not None:
            self._history.setdefault(row.unique_key, []).append(previous)
        self._rows[row.unique_key] = row
        self._audit.append(audit)
        # A selected bar closes any open gap/job for that session.
        self._jobs.pop((row.instrument_id, row.session_date), None)
        self._gaps.pop((row.instrument_id, row.session_date), None)

    def add_backfill_jobs(self, jobs: Iterable[BackfillJob]) -> None:
        for job in jobs:
            self._jobs[(job.instrument_id, job.session_date)] = job

    def add_known_gaps(self, gaps: Iterable[KnownGap]) -> None:
        for gap in gaps:
            self._gaps[(gap.instrument_id, gap.session_date)] = gap
            self._jobs.pop((gap.instrument_id, gap.session_date), None)

    def history(self, key: RowKey) -> tuple[CanonicalDailyRow, ...]:
        return tuple(self._history.get(key, ()))

    @property
    def rows(self) -> tuple[CanonicalDailyRow, ...]:
        return tuple(sorted(self._rows.values(), key=lambda r: (r.instrument_id, r.session_date)))

    @property
    def audit(self) -> tuple[AuditRecord, ...]:
        return tuple(self._audit)

    @property
    def backfill_jobs(self) -> tuple[BackfillJob, ...]:
        return tuple(sorted(self._jobs.values(), key=lambda j: (j.instrument_id, j.session_date)))

    @property
    def known_gaps(self) -> tuple[KnownGap, ...]:
        return tuple(sorted(self._gaps.values(), key=lambda g: (g.instrument_id, g.session_date)))
