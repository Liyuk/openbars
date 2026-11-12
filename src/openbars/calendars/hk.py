"""Hong Kong (HKEX) trading calendar, Asia/Hong_Kong.

Holiday and early-close rules come from the ``exchange_calendars`` library's **XHKG**
calendar. Regular session (from 2011-03-07, as recorded by the library) is 09:30-12:00
and 13:00-16:00 local time. Half-day sessions (Christmas Eve, New Year's Eve, Lunar New
Year's Eve, and any other early close the library lists) have a single morning segment
ending at the library's session close (12:00 HKT in recent years) and no lunch break.
``session_bounds`` returns those UTC segments; ``TradingSession`` spans the first open
to the last close (the lunch break is not a gap in the session object).

``exchange_calendars`` 4.13.x ships a default XHKG session range of 2006-11-13 through
2027-11-11. Queries outside that range raise ``ValueError`` rather than inventing
sessions or holidays. Historical unscheduled closures that the library already records
(for example some typhoon / black-rain days in ``adhoc_holidays``) are used as-is.
Future weather closures cannot be predicted and are **not** handwritten here; upgrade
the package (or, later, ingest HKEX announcements) to extend coverage.
Design: https://github.com/Liyuk/openbars/discussions/68
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from functools import lru_cache
from typing import Any
from zoneinfo import ZoneInfo

import exchange_calendars as xcals
import pandas as pd

from openbars.core.calendar import TradingSession
from openbars.core.models import Market

XHKG_CODE = "XHKG"
HK_TZ = ZoneInfo("Asia/Hong_Kong")
_LATEST_SESSION_LOOKBACK_DAYS = 14


@lru_cache(maxsize=1)
def _default_calendar() -> Any:
    return xcals.get_calendar(XHKG_CODE)


def coverage() -> tuple[date, date]:
    """Inclusive first and last dates the bundled XHKG calendar can answer."""
    cal = _default_calendar()
    return cal.first_session.date(), cal.last_session.date()


def _require_covered(*days: date) -> None:
    first, last = coverage()
    for day in days:
        if day < first or day > last:
            raise ValueError(
                f"XHKG holiday coverage in exchange-calendars is {first.isoformat()}.."
                f"{last.isoformat()}; {day.isoformat()} is outside that range. "
                "Upgrade exchange-calendars rather than inventing holidays."
            )


def _calendar() -> Any:
    return _default_calendar()


def _label(day: date) -> pd.Timestamp:
    return pd.Timestamp(day)


def _utc(ts: pd.Timestamp) -> datetime:
    return ts.to_pydatetime().astimezone(UTC)


def _segments(cal: Any, day: date) -> tuple[tuple[datetime, datetime], ...]:
    label = _label(day)
    opens, closes = _utc(cal.session_open(label)), _utc(cal.session_close(label))
    break_start, break_end = (
        cal.session_break_start(label),
        cal.session_break_end(label),
    )
    if pd.isna(break_start) or pd.isna(break_end):
        return ((opens, closes),)
    return ((opens, _utc(break_start)), (_utc(break_end), closes))


class HKCalendar:
    """``TradingCalendar`` implementation for the Hong Kong Exchange."""

    market = Market.HK
    timezone = "Asia/Hong_Kong"

    def sessions(self, start: date, end: date) -> list[date]:
        if end < start:
            raise ValueError(f"end ({end}) must not be before start ({start})")
        _require_covered(start, end)
        return [
            ts.date()
            for ts in _calendar().sessions_in_range(_label(start), _label(end))
        ]

    def is_session(self, day: date) -> bool:
        _require_covered(day)
        if day.weekday() >= 5:
            return False
        return bool(_calendar().is_session(_label(day)))

    def session_bounds(self, day: date) -> tuple[tuple[datetime, datetime], ...]:
        """UTC ``(open, close)`` segments: two on a regular day, one on a half day."""
        if not self.is_session(day):
            return ()
        return _segments(_calendar(), day)

    def trading_session(self, day: date) -> TradingSession | None:
        bounds = self.session_bounds(day)
        if not bounds:
            return None
        return TradingSession(
            day, bounds[0][0].astimezone(HK_TZ), bounds[-1][1].astimezone(HK_TZ)
        )

    def trading_sessions(self, start: date, end: date) -> list[TradingSession]:
        if end < start:
            raise ValueError(f"end ({end}) must not be before start ({start})")
        _require_covered(start, end)
        cal = _calendar()
        out: list[TradingSession] = []
        for ts in cal.sessions_in_range(_label(start), _label(end)):
            segs = _segments(cal, ts.date())
            out.append(
                TradingSession(
                    ts.date(),
                    segs[0][0].astimezone(HK_TZ),
                    segs[-1][1].astimezone(HK_TZ),
                )
            )
        return out


XHKGCalendar = HKCalendar

_CALENDAR = HKCalendar()


def sessions(start: date, end: date) -> list[date]:
    return _CALENDAR.sessions(start, end)


def is_session(day: date) -> bool:
    return _CALENDAR.is_session(day)


def session_bounds(day: date) -> tuple[tuple[datetime, datetime], ...]:
    return _CALENDAR.session_bounds(day)


def trading_session(day: date) -> TradingSession | None:
    return _CALENDAR.trading_session(day)


def trading_sessions(start: date, end: date) -> list[TradingSession]:
    return _CALENDAR.trading_sessions(start, end)


def latest_completed_session(now: datetime, wait_minutes: int = 30) -> date | None:
    """Latest session whose close (16:00, or 12:00 on a half day) is at least ``wait_minutes`` before ``now``.

    ``now`` must be timezone-aware. The search stays inside the bundled coverage window.
    """
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    if wait_minutes < 0:
        raise ValueError("wait_minutes must be >= 0")
    now_hk = now.astimezone(HK_TZ)
    first, last = coverage()
    today = now_hk.date()
    end = min(today, last)
    if end < first:
        return None
    start = max(end - timedelta(days=_LATEST_SESSION_LOOKBACK_DAYS), first)
    wait = timedelta(minutes=wait_minutes)
    for session in reversed(_CALENDAR.trading_sessions(start, end)):
        if now_hk >= session.closes_at + wait:
            return session.session_date
    return None


__all__ = [
    "HK_TZ",
    "XHKG_CODE",
    "HKCalendar",
    "TradingSession",
    "XHKGCalendar",
    "coverage",
    "is_session",
    "latest_completed_session",
    "session_bounds",
    "sessions",
    "trading_session",
    "trading_sessions",
]
