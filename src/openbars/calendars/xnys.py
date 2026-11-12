"""US equities (XNYS) trading calendar.

Holiday/half-day rules come from the ``exchange_calendars`` library (Apache-2.0)
rather than a vendored table. A default calendar is cached; ranges outside its
first/last session use a bounded calendar built for the requested range.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from typing import Any, NamedTuple
from zoneinfo import ZoneInfo

import exchange_calendars as xcals
import pandas as pd

from openbars.core.calendar import TradingSession
from openbars.core.models import Market

XNYS_CODE = "XNYS"
NY_TZ = ZoneInfo("America/New_York")
UTC = timezone.utc
DEFAULT_EPOCH = date(2000, 1, 1)
NON_SESSION_REASON = "non_session_date"
_LATEST_SESSION_LOOKBACK_DAYS = 14


@lru_cache(maxsize=1)
def _default_calendar() -> Any:
    return xcals.get_calendar(XNYS_CODE)


@lru_cache(maxsize=8)
def _bounded_calendar(start: date, end: date) -> Any:
    return xcals.get_calendar(XNYS_CODE, start=pd.Timestamp(start), end=pd.Timestamp(end))


def _calendar_for(start: date, end: date) -> Any:
    """Default calendar when it covers [start, end]; else a bounded one (+/- 1 day)."""
    default = _default_calendar()
    if default.first_session.date() <= start and end <= default.last_session.date():
        return default
    return _bounded_calendar(start - timedelta(days=1), end + timedelta(days=1))


def _label(day: date) -> pd.Timestamp:
    return pd.Timestamp(day)


def _to_date(value: date | datetime) -> date:
    return value.date() if isinstance(value, datetime) else value


def _session_open_close_utc(cal: Any, day: date) -> tuple[datetime, datetime]:
    label = _label(day)
    return (
        cal.session_open(label).to_pydatetime().astimezone(UTC),
        cal.session_close(label).to_pydatetime().astimezone(UTC),
    )


class XNYSCalendar:
    """``TradingCalendar`` implementation for the New York Stock Exchange."""

    market = Market.US
    timezone = "America/New_York"

    def sessions(self, start: date, end: date) -> list[date]:
        """Trading days in the inclusive range ``[start, end]``."""
        if end < start:
            raise ValueError(f"end ({end}) must not be before start ({start})")
        cal = _calendar_for(start, end)
        return [ts.date() for ts in cal.sessions_in_range(_label(start), _label(end))]

    def is_session(self, day: date) -> bool:
        if day.weekday() >= 5:
            return False
        return bool(_calendar_for(day, day).is_session(_label(day)))

    def session_bounds(self, day: date) -> tuple[tuple[datetime, datetime], ...]:
        """UTC ``(open, close)`` segments; empty for a non-session.

        XNYS has no lunch break, so a session has exactly one segment (half days
        simply close early).
        """
        if not self.is_session(day):
            return ()
        return (_session_open_close_utc(_calendar_for(day, day), day),)

    def trading_session(self, day: date) -> TradingSession | None:
        """Session with America/New_York open/close instants, or ``None``."""
        bounds = self.session_bounds(day)
        if not bounds:
            return None
        opens, closes = bounds[0]
        return TradingSession(day, opens.astimezone(NY_TZ), closes.astimezone(NY_TZ))

    def trading_sessions(self, start: date, end: date) -> list[TradingSession]:
        if end < start:
            raise ValueError(f"end ({end}) must not be before start ({start})")
        cal = _calendar_for(start, end)
        out: list[TradingSession] = []
        for ts in cal.sessions_in_range(_label(start), _label(end)):
            opens, closes = _session_open_close_utc(cal, ts.date())
            out.append(TradingSession(ts.date(), opens.astimezone(NY_TZ), closes.astimezone(NY_TZ)))
        return out


_CALENDAR = XNYSCalendar()


def sessions(start: date, end: date) -> list[date]:
    return _CALENDAR.sessions(start, end)


def is_session(day: date) -> bool:
    return _CALENDAR.is_session(day)


def session_bounds(day: date) -> tuple[tuple[datetime, datetime], ...]:
    return _CALENDAR.session_bounds(day)


def session_index(as_of: date, *, epoch: date = DEFAULT_EPOCH) -> int:
    """Zero-based index of the last XNYS session at or before ``as_of``.

    The first session on or after ``epoch`` has index 0; returns 0 when
    ``as_of < epoch`` or no session exists in ``[epoch, as_of]``.
    """
    if as_of < epoch:
        return 0
    count = len(_CALENDAR.sessions(epoch, as_of))
    return max(count - 1, 0)


def latest_completed_session(now: datetime, wait_minutes: int = 30) -> date | None:
    """Latest session whose close is at least ``wait_minutes`` before ``now``.

    ``now`` must be timezone-aware. Searches back ~14 calendar days; ``None`` if no
    session qualifies.
    """
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    if wait_minutes < 0:
        raise ValueError("wait_minutes must be >= 0")
    now_ny = now.astimezone(NY_TZ)
    today = now_ny.date()
    wait = timedelta(minutes=wait_minutes)
    for session in reversed(
        _CALENDAR.trading_sessions(today - timedelta(days=_LATEST_SESSION_LOOKBACK_DAYS), today)
    ):
        if now_ny >= session.closes_at.astimezone(NY_TZ) + wait:
            return session.session_date
    return None


def is_non_session_date(day: date) -> bool:
    """True when ``day`` is certainly not an XNYS session.

    Weekends always are. Holidays are only decided inside the exchange calendar's
    default range; weekdays before its first or after its last session are *not*
    treated as non-sessions, so long pre-range histories are not rejected.
    """
    day = _to_date(day)
    if day.weekday() >= 5:
        return True
    default = _default_calendar()
    if day < default.first_session.date() or day > default.last_session.date():
        return False
    return not bool(default.is_session(_label(day)))


class BarRejection(NamedTuple):
    bar: Any
    session_date: date
    reason: str


class BarFilterResult(NamedTuple):
    accepted: list[Any]
    rejected: list[BarRejection]


def reject_non_session_bars(
    bars: Iterable[Any], *, session_date_attr: str = "session_date"
) -> BarFilterResult:
    """Split ``bars`` into accepted and rejected (reason ``"non_session_date"``).

    Pure function: each bar only needs a ``session_date`` attribute (configurable).
    """
    accepted: list[Any] = []
    rejected: list[BarRejection] = []
    for bar in bars:
        raw = getattr(bar, session_date_attr)
        if not isinstance(raw, date):
            raise TypeError(f"{session_date_attr} must be a date, got {type(raw).__name__}")
        day = _to_date(raw)
        if is_non_session_date(day):
            rejected.append(BarRejection(bar, day, NON_SESSION_REASON))
        else:
            accepted.append(bar)
    return BarFilterResult(accepted, rejected)


__all__ = [
    "BarFilterResult",
    "BarRejection",
    "NON_SESSION_REASON",
    "NY_TZ",
    "TradingSession",
    "XNYSCalendar",
    "is_non_session_date",
    "is_session",
    "latest_completed_session",
    "reject_non_session_bars",
    "session_bounds",
    "session_index",
    "sessions",
]
