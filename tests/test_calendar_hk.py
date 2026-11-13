"""HK (XHKG) calendar tests; local exchange_calendars data only, no network."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from importlib.metadata import entry_points
from zoneinfo import ZoneInfo

import pytest

from openbars.calendars import (
    HKCalendar,
    XHKGCalendar,
    hk_is_session,
    hk_latest_completed_session,
    hk_session_bounds,
    hk_sessions,
    hk_trading_session,
    hk_trading_sessions,
)
from openbars.calendars.hk import coverage
from openbars.core.calendar import TradingCalendar
from openbars.core.models import Market

HK = ZoneInfo("Asia/Hong_Kong")
CAL = HKCalendar()


def _hk(y, m, d, hh=0, mm=0) -> datetime:
    return datetime(y, m, d, hh, mm, tzinfo=HK)


def test_identity_and_protocol():
    assert CAL.market is Market.HK
    assert CAL.timezone == "Asia/Hong_Kong"
    _: TradingCalendar = CAL
    assert (
        callable(CAL.sessions)
        and callable(CAL.is_session)
        and callable(CAL.session_bounds)
    )
    assert XHKGCalendar is HKCalendar


def test_entry_point_loads_calendar():
    eps = {e.name: e for e in entry_points(group="openbars.calendars")}
    assert eps["hk"].load() is HKCalendar


def test_weekend_closed():
    assert not hk_is_session(date(2026, 3, 7))  # Saturday
    assert not hk_is_session(date(2026, 3, 8))  # Sunday
    assert hk_session_bounds(date(2026, 3, 7)) == ()
    assert hk_trading_session(date(2026, 3, 8)) is None
    assert hk_sessions(date(2026, 3, 7), date(2026, 3, 8)) == []


@pytest.mark.parametrize(
    "day",
    [
        date(2025, 12, 25),
        date(2025, 12, 26),
        date(2026, 1, 1),
    ],  # Christmas, Boxing Day, New Year
)
def test_holidays_closed(day):
    assert day.weekday() < 5
    assert not hk_is_session(day)
    assert hk_session_bounds(day) == ()
    assert hk_trading_session(day) is None
    assert day not in hk_sessions(day - timedelta(days=3), day + timedelta(days=3))


def test_library_adhoc_closure_is_used_not_handwritten():
    # 2024-09-06 is in the library's adhoc holidays (a historically unscheduled closure).
    # openbars must not invent extra weather days beyond what the library ships.
    day = date(2024, 9, 6)
    assert day.weekday() < 5
    assert not hk_is_session(day)
    assert hk_session_bounds(day) == ()


def test_ordinary_day_bounds_have_lunch_gap():
    day = date(2026, 3, 2)  # Monday
    bounds = hk_session_bounds(day)
    assert len(bounds) == 2
    (m_open, m_close), (a_open, a_close) = bounds
    assert m_open == _hk(2026, 3, 2, 9, 30).astimezone(UTC)
    assert m_close == _hk(2026, 3, 2, 12, 0).astimezone(UTC)
    assert a_open == _hk(2026, 3, 2, 13, 0).astimezone(UTC)
    assert a_close == _hk(2026, 3, 2, 16, 0).astimezone(UTC)
    session = hk_trading_session(day)
    assert session is not None
    assert session.opens_at == _hk(2026, 3, 2, 9, 30)
    assert session.closes_at == _hk(2026, 3, 2, 16, 0)


@pytest.mark.parametrize(
    "day", [date(2025, 12, 24), date(2025, 12, 31), date(2026, 2, 16)]
)
def test_half_day_is_a_single_morning_segment(day):
    bounds = hk_session_bounds(day)
    assert len(bounds) == 1
    open_, close = bounds[0]
    assert open_ == datetime(day.year, day.month, day.day, 9, 30, tzinfo=HK).astimezone(
        UTC
    )
    assert close == datetime(day.year, day.month, day.day, 12, 0, tzinfo=HK).astimezone(
        UTC
    )
    session = hk_trading_session(day)
    assert session is not None and session.closes_at.hour == 12


def test_latest_completed_session_uses_actual_close():
    # Regular day: close 16:00 + 30 min => 16:30 HKT
    assert (
        hk_latest_completed_session(_hk(2026, 3, 2, 16, 29)) == date(2026, 2, 27)
    )  # Friday before Mon 3/2? 2026-02-27 is Friday, 2/28-3/1 weekend, 3/2 is Monday not yet complete
    assert hk_latest_completed_session(_hk(2026, 3, 2, 16, 30)) == date(2026, 3, 2)
    # Half day 2025-12-24 close 12:00 + 30 min
    assert hk_latest_completed_session(_hk(2025, 12, 24, 12, 29)) != date(2025, 12, 24)
    assert hk_latest_completed_session(_hk(2025, 12, 24, 12, 30)) == date(2025, 12, 24)


def test_coverage_is_documented_and_out_of_range_raises():
    first, last = coverage()
    assert first <= date(2006, 11, 13)
    assert last >= date(2026, 11, 11)  # current year must be covered
    # Do not invent sessions past the bundled holiday table.
    past = last + timedelta(days=1)
    # skip weekend to still be a weekday when possible
    while past.weekday() >= 5:
        past += timedelta(days=1)
    with pytest.raises(ValueError, match="outside that range"):
        hk_is_session(past)
    with pytest.raises(ValueError, match="outside that range"):
        hk_sessions(past, past + timedelta(days=5))
    before = first - timedelta(days=1)
    with pytest.raises(ValueError, match="outside that range"):
        hk_is_session(before)


def test_module_functions_match_the_class():
    day = date(2026, 3, 2)
    assert hk_is_session(day) is CAL.is_session(day)
    assert hk_session_bounds(day) == CAL.session_bounds(day)
    assert hk_sessions(day, day) == CAL.sessions(day, day)
    assert hk_trading_sessions(day, day) == CAL.trading_sessions(day, day)
