"""China A-share (SSE/SZSE) trading calendar, Asia/Shanghai.

Holiday rules come from the ``exchange_calendars`` library's **XSHG** calendar. That
package has no XSHE calendar; SSE and SZSE publish the same annual holiday schedule, so
``XSHE`` is treated as an alias of ``XSHG`` (``XSHE_CODE`` / ``XSHECalendar``). The Beijing
exchange is out of scope.

Regular session 09:30-11:30 and 13:00-15:00 local time. ``session_bounds`` returns two UTC
segments (morning, afternoon) on a session day and nothing otherwise. ``TradingSession``
opens at 09:30 and closes at 15:00 (the lunch break is not a gap in the session object).
``exchange_calendars`` only records XSHG holidays up to a fixed year (2026 in 4.13.x); queries
beyond that raise ``ValueError`` from the library rather than assuming no holidays; upgrade the
package to extend coverage.
Design: https://github.com/Liyuk/openbars/discussions/54
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from typing import Any
from zoneinfo import ZoneInfo

import exchange_calendars as xcals
import pandas as pd

from openbars.core.calendar import TradingSession
from openbars.core.models import Market

XSHG_CODE = "XSHG"
XSHE_CODE = XSHG_CODE  # alias: no XSHE calendar in exchange_calendars
CN_TZ = ZoneInfo("Asia/Shanghai")
UTC = timezone.utc
_LATEST_SESSION_LOOKBACK_DAYS = 14


@lru_cache(maxsize=1)
def _default_calendar() -> Any:
    return xcals.get_calendar(XSHG_CODE)


@lru_cache(maxsize=8)
def _bounded_calendar(start: date, end: date) -> Any:
    return xcals.get_calendar(XSHG_CODE, start=pd.Timestamp(start), end=pd.Timestamp(end))


def _calendar_for(start: date, end: date) -> Any:
    """Default calendar when it covers [start, end]; else a bounded one (+/- 1 day)."""
    default = _default_calendar()
    if default.first_session.date() <= start and end <= default.last_session.date():
        return default
    return _bounded_calendar(start - timedelta(days=1), end + timedelta(days=1))


def _label(day: date) -> pd.Timestamp:
    return pd.Timestamp(day)


def _utc(ts: pd.Timestamp) -> datetime:
    return ts.to_pydatetime().astimezone(UTC)


def _segments(cal: Any, day: date) -> tuple[tuple[datetime, datetime], ...]:
    label = _label(day)
    opens, closes = _utc(cal.session_open(label)), _utc(cal.session_close(label))
    break_start, break_end = cal.session_break_start(label), cal.session_break_end(label)
    if pd.isna(break_start) or pd.isna(break_end):
        return ((opens, closes),)
    return ((opens, _utc(break_start)), (_utc(break_end), closes))


class CNCalendar:
    """``TradingCalendar`` implementation for the Shanghai/Shenzhen exchanges."""

    market = Market.CN
    timezone = "Asia/Shanghai"

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
        """UTC ``(open, close)`` segments: morning and afternoon; empty for a non-session."""
        if not self.is_session(day):
            return ()
        return _segments(_calendar_for(day, day), day)

    def trading_session(self, day: date) -> TradingSession | None:
        """Session spanning the day's first open to last close (Asia/Shanghai), or ``None``."""
        bounds = self.session_bounds(day)
        if not bounds:
            return None
        return TradingSession(day, bounds[0][0].astimezone(CN_TZ), bounds[-1][1].astimezone(CN_TZ))

    def trading_sessions(self, start: date, end: date) -> list[TradingSession]:
        if end < start:
            raise ValueError(f"end ({end}) must not be before start ({start})")
        cal = _calendar_for(start, end)
        out: list[TradingSession] = []
        for ts in cal.sessions_in_range(_label(start), _label(end)):
            segs = _segments(cal, ts.date())
            out.append(
                TradingSession(ts.date(), segs[0][0].astimezone(CN_TZ), segs[-1][1].astimezone(CN_TZ))
            )
        return out


# XSHE shares the XSHG schedule.
XSHGCalendar = CNCalendar
XSHECalendar = CNCalendar

_CALENDAR = CNCalendar()


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
    """Latest session whose 15:00 Shanghai close is at least ``wait_minutes`` before ``now``.

    ``now`` must be timezone-aware. Searches back ~14 calendar days (covers Golden Week);
    ``None`` if no session qualifies.
    """
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    if wait_minutes < 0:
        raise ValueError("wait_minutes must be >= 0")
    now_cn = now.astimezone(CN_TZ)
    today = now_cn.date()
    wait = timedelta(minutes=wait_minutes)
    for session in reversed(
        _CALENDAR.trading_sessions(today - timedelta(days=_LATEST_SESSION_LOOKBACK_DAYS), today)
    ):
        if now_cn >= session.closes_at + wait:
            return session.session_date
    return None


__all__ = [
    "CNCalendar",
    "CN_TZ",
    "TradingSession",
    "XSHECalendar",
    "XSHE_CODE",
    "XSHGCalendar",
    "XSHG_CODE",
    "is_session",
    "latest_completed_session",
    "session_bounds",
    "sessions",
    "trading_session",
    "trading_sessions",
]
