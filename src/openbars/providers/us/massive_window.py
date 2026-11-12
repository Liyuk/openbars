"""Pure window rules for Massive grouped-daily requests (no I/O, no planner state).

A grouped-daily call returns the whole US market for one session, so the unit of work is
one session date. A session ``D`` can be requested only after 00:00 America/New_York on
``D + 1``. The public Basic tier also serves only a rolling ~2 year window.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from openbars.calendars import xnys
from openbars.core.models import DateRange

__all__ = ["ROLLING_WINDOW_DAYS", "massive_grouped_sessions", "session_is_ready"]

ROLLING_WINDOW_DAYS = 730
NEW_YORK = ZoneInfo("America/New_York")


def _new_york_date(now: datetime) -> date:
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    return now.astimezone(NEW_YORK).date()


def session_is_ready(session: date, now: datetime) -> bool:
    """True once it is 00:00 America/New_York on the calendar day after ``session``."""
    return _new_york_date(now) > session


def massive_grouped_sessions(
    window: DateRange,
    *,
    as_of: date,
    now: datetime,
    covered: Iterable[date] = (),
    sessions: Iterable[date] | None = None,
) -> list[date]:
    """Sessions of ``window`` to request as grouped-daily jobs, one date per job.

    Clipped to ``[as_of - 730 days, last ready session]``. Non-sessions, sessions that
    are not ready yet, and ``covered`` dates are omitted. Order is chronological; adjacent
    days are never merged. ``sessions`` defaults to the XNYS sessions inside the window.
    """
    last_ready = _new_york_date(now) - timedelta(days=1)
    start = max(window.start, as_of - timedelta(days=ROLLING_WINDOW_DAYS))
    end = min(window.end, last_ready)
    if end < start:
        return []
    candidates = xnys.sessions(start, end) if sessions is None else sessions
    done = set(covered)
    return sorted(
        {d for d in candidates if start <= d <= end and d not in done and session_is_ready(d, now)}
    )
