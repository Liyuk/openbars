"""Crypto 24/7 trading calendar with daily bars cut at UTC 00:00.

Every UTC calendar date is a session (no weekends, holidays or breaks). A session
is the half-open interval ``[00:00:00 UTC, next 00:00:00 UTC)``, so adjacent
sessions abut exactly and the close instant belongs to the next session.
Pure stdlib; no dependency on ``exchange_calendars``.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

from openbars.calendars.xnys import (
    NON_SESSION_REASON,
    BarFilterResult,
    BarRejection,
)
from openbars.core.calendar import TradingSession
from openbars.core.models import Market

UTC = timezone.utc
DEFAULT_EPOCH = date(2000, 1, 1)
_LATEST_SESSION_LOOKBACK_DAYS = 14
_ONE_DAY = timedelta(days=1)


def _to_date(value: date | datetime) -> date:
    return value.date() if isinstance(value, datetime) else value


def _midnight_utc(day: date) -> datetime:
    return datetime.combine(day, time(0, 0), tzinfo=UTC)


def _bounds(day: date) -> tuple[datetime, datetime]:
    return _midnight_utc(day), _midnight_utc(day + _ONE_DAY)


class CryptoCalendar:
    """``TradingCalendar`` implementation for 24/7 crypto markets (UTC days)."""

    market = Market.CRYPTO
    timezone = "UTC"

    def sessions(self, start: date, end: date) -> list[date]:
        """Every date in the inclusive range ``[start, end]``."""
        if end < start:
            raise ValueError(f"end ({end}) must not be before start ({start})")
        return [start + timedelta(days=i) for i in range((end - start).days + 1)]

    def is_session(self, day: date) -> bool:
        return True

    def session_bounds(self, day: date) -> tuple[tuple[datetime, datetime], ...]:
        """One UTC segment ``[00:00:00, next 00:00:00)`` (half-open)."""
        return (_bounds(day),)

    def trading_session(self, day: date) -> TradingSession:
        opens, closes = _bounds(day)
        return TradingSession(day, opens, closes)

    def trading_sessions(self, start: date, end: date) -> list[TradingSession]:
        return [self.trading_session(day) for day in self.sessions(start, end)]


_CALENDAR = CryptoCalendar()


def sessions(start: date, end: date) -> list[date]:
    return _CALENDAR.sessions(start, end)


def is_session(day: date) -> bool:
    return _CALENDAR.is_session(day)


def session_bounds(day: date) -> tuple[tuple[datetime, datetime], ...]:
    return _CALENDAR.session_bounds(day)


def session_index(as_of: date, *, epoch: date = DEFAULT_EPOCH) -> int:
    """Zero-based index of the last session at or before ``as_of``.

    Every day is a session, so this is ``(as_of - epoch).days``; 0 when
    ``as_of < epoch``.
    """
    if as_of < epoch:
        return 0
    return (as_of - epoch).days


def latest_completed_session(now: datetime, wait_minutes: int = 30) -> date | None:
    """Latest session whose close is at least ``wait_minutes`` before ``now``.

    ``now`` must be timezone-aware; compared in UTC. Searches back ~14 calendar
    days; ``None`` if no session qualifies.
    """
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    if wait_minutes < 0:
        raise ValueError("wait_minutes must be >= 0")
    now_utc = now.astimezone(UTC)
    today = now_utc.date()
    wait = timedelta(minutes=wait_minutes)
    for session in reversed(
        _CALENDAR.trading_sessions(today - timedelta(days=_LATEST_SESSION_LOOKBACK_DAYS), today)
    ):
        if now_utc >= session.closes_at + wait:
            return session.session_date
    return None


def is_non_session_date(day: date) -> bool:
    """Always False: crypto never rejects a UTC date."""
    return False


def reject_non_session_bars(
    bars: Iterable[Any], *, session_date_attr: str = "session_date"
) -> BarFilterResult:
    """Split ``bars`` into accepted and rejected; nothing is rejected for crypto.

    Same signature and result types as the XNYS helper; each bar still needs a
    ``session_date`` attribute (configurable) holding a ``date``.
    """
    accepted: list[Any] = []
    rejected: list[BarRejection] = []
    for bar in bars:
        raw = getattr(bar, session_date_attr)
        if not isinstance(raw, date):
            raise TypeError(f"{session_date_attr} must be a date, got {type(raw).__name__}")
        if is_non_session_date(_to_date(raw)):  # pragma: no cover - always False
            rejected.append(BarRejection(bar, _to_date(raw), NON_SESSION_REASON))
        else:
            accepted.append(bar)
    return BarFilterResult(accepted, rejected)


__all__ = [
    "BarFilterResult",
    "BarRejection",
    "CryptoCalendar",
    "NON_SESSION_REASON",
    "TradingSession",
    "is_non_session_date",
    "is_session",
    "latest_completed_session",
    "reject_non_session_bars",
    "session_bounds",
    "session_index",
    "sessions",
]
