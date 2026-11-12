"""Gap detection: expected XNYS sessions minus selected canonical bars."""

from __future__ import annotations

from collections.abc import Collection, Iterable
from dataclasses import dataclass
from datetime import date

from openbars.calendars.xnys import XNYSCalendar

GAP_REASON = "gap"
NO_SOURCE_COVERAGE = "no_source_coverage"


@dataclass(frozen=True)
class BackfillJob:
    """Ask the orchestrator to refetch one missing session."""

    instrument_id: str
    session_date: date
    reason: str = GAP_REASON


@dataclass(frozen=True)
class KnownGap:
    """A session that is missing and will not be refilled (refill failed / no source)."""

    instrument_id: str
    session_date: date
    reason: str = NO_SOURCE_COVERAGE


@dataclass(frozen=True)
class GapReport:
    backfill_jobs: tuple[BackfillJob, ...]
    known_gaps: tuple[KnownGap, ...]


def missing_sessions(
    start: date,
    end: date,
    present: Iterable[date],
    *,
    calendar: XNYSCalendar | None = None,
) -> tuple[date, ...]:
    have = set(present)
    cal = calendar or XNYSCalendar()
    return tuple(d for d in cal.sessions(start, end) if d not in have)


def detect_gaps(
    instrument_id: str,
    start: date,
    end: date,
    present: Iterable[date],
    *,
    calendar: XNYSCalendar | None = None,
    unfillable: Collection[date] = (),
    refill: bool = True,
    known_gap_reason: str = NO_SOURCE_COVERAGE,
) -> GapReport:
    """Missing sessions in ``[start, end]`` become backfill jobs or known gaps.

    A session in ``unfillable`` (refill already failed / no source covers it), or any
    session when ``refill`` is False, becomes a ``KnownGap`` instead of a job.
    Weekends and holidays are never expected.
    """
    jobs: list[BackfillJob] = []
    gaps: list[KnownGap] = []
    skip = set(unfillable)
    for day in missing_sessions(start, end, present, calendar=calendar):
        if not refill or day in skip:
            gaps.append(KnownGap(instrument_id, day, known_gap_reason))
        else:
            jobs.append(BackfillJob(instrument_id, day, GAP_REASON))
    return GapReport(tuple(jobs), tuple(gaps))
