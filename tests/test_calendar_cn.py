"""CN (XSHG/XSHE) calendar tests; local exchange_calendars data only, no network."""

from __future__ import annotations

from importlib.metadata import entry_points
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from openbars.calendars import (
    CNCalendar,
    TradingSession,
    XSHECalendar,
    XSHGCalendar,
    cn_is_session,
    cn_latest_completed_session,
    cn_session_bounds,
    cn_sessions,
    cn_trading_session,
    cn_trading_sessions,
)
from openbars.core.calendar import TradingCalendar
from openbars.core.models import Market

SH = ZoneInfo("Asia/Shanghai")
UTC = timezone.utc
CAL = CNCalendar()


def _sh(y, m, d, hh=0, mm=0) -> datetime:
    return datetime(y, m, d, hh, mm, tzinfo=SH)


def test_identity_and_protocol():
    assert CAL.market is Market.CN
    assert CAL.timezone == "Asia/Shanghai"
    _: TradingCalendar = CAL  # structural typing check
    assert callable(CAL.sessions) and callable(CAL.is_session) and callable(CAL.session_bounds)
    assert XSHGCalendar is CNCalendar and XSHECalendar is CNCalendar


def test_entry_point_loads_calendar():
    eps = {e.name: e for e in entry_points(group="openbars.calendars")}
    assert eps["cn"].load() is CNCalendar


def test_weekend_closed():
    assert not cn_is_session(date(2026, 3, 7))  # Saturday
    assert not cn_is_session(date(2026, 3, 8))  # Sunday
    assert cn_session_bounds(date(2026, 3, 7)) == ()
    assert cn_trading_session(date(2026, 3, 8)) is None
    assert cn_sessions(date(2026, 3, 7), date(2026, 3, 8)) == []


@pytest.mark.parametrize(
    "day",
    [date(2026, 1, 1), date(2026, 2, 17), date(2026, 10, 1)],  # New Year, Spring Festival, National Day
)
def test_holidays_closed(day):
    assert day.weekday() < 5  # genuine weekday closures
    assert not cn_is_session(day)
    assert cn_session_bounds(day) == ()
    assert cn_trading_session(day) is None
    assert day not in cn_sessions(day - timedelta(days=3), day + timedelta(days=3))


def test_makeup_workday_open_after_national_day():
    # Library says 2026-10-08 is a trading day (Thursday after the holiday week).
    assert cn_is_session(date(2026, 10, 8))
    assert date(2026, 10, 8) in cn_sessions(date(2026, 9, 28), date(2026, 10, 12))
    # Golden week 10-01..10-07 has no sessions.
    assert [d for d in cn_sessions(date(2026, 10, 1), date(2026, 10, 7))] == []


def test_ordinary_day_bounds_have_lunch_gap():
    day = date(2026, 3, 2)  # Monday
    bounds = cn_session_bounds(day)
    assert len(bounds) == 2
    (m_open, m_close), (a_open, a_close) = bounds
    assert all(t.tzinfo is not None and t.utcoffset() == timedelta(0) for t in bounds[0] + bounds[1])
    assert m_open.astimezone(SH) == _sh(2026, 3, 2, 9, 30)
    assert m_close.astimezone(SH) == _sh(2026, 3, 2, 11, 30)
    assert a_open.astimezone(SH) == _sh(2026, 3, 2, 13, 0)
    assert a_close.astimezone(SH) == _sh(2026, 3, 2, 15, 0)
    # UTC values
    assert m_open == datetime(2026, 3, 2, 1, 30, tzinfo=UTC)
    assert a_close == datetime(2026, 3, 2, 7, 0, tzinfo=UTC)
    # lunch gap 11:30-13:00 Shanghai
    assert a_open - m_close == timedelta(minutes=90)


def test_trading_session_spans_whole_day():
    s = cn_trading_session(date(2026, 3, 2))
    assert isinstance(s, TradingSession)
    assert s.session_date == date(2026, 3, 2)
    assert s.opens_at == _sh(2026, 3, 2, 9, 30) and s.closes_at == _sh(2026, 3, 2, 15, 0)
    assert s.opens_at.utcoffset() == timedelta(hours=8)
    assert s.closes_at.tzinfo is not None


def test_trading_sessions_range_matches_sessions():
    start, end = date(2026, 9, 28), date(2026, 10, 12)
    ts = cn_trading_sessions(start, end)
    assert [t.session_date for t in ts] == cn_sessions(start, end)
    assert all(t.opens_at.astimezone(SH).time().hour == 9 and t.closes_at.astimezone(SH).hour == 15 for t in ts)
    assert ts[0] == cn_trading_session(ts[0].session_date)


def test_range_validation():
    with pytest.raises(ValueError):
        cn_sessions(date(2026, 3, 3), date(2026, 3, 2))
    with pytest.raises(ValueError):
        cn_trading_sessions(date(2026, 3, 3), date(2026, 3, 2))


def test_range_past_library_holiday_data_fails_loudly():
    # exchange_calendars only records XSHG holidays through 2026 (4.13.x); we must not
    # silently treat later years as holiday-free.
    with pytest.raises(ValueError, match="holidays are only recorded"):
        cn_sessions(date(2035, 3, 5), date(2035, 3, 11))


def test_historical_range_before_default_start_uses_bounded_calendar():
    days = cn_sessions(date(2000, 3, 6), date(2000, 3, 10))
    assert days == [date(2000, 3, d) for d in range(6, 11)]


def test_latest_completed_session_around_close_plus_wait():
    mon = date(2026, 3, 2)
    fri = date(2026, 2, 27)
    # 15:29 Shanghai: still waiting -> previous session
    assert cn_latest_completed_session(_sh(2026, 3, 2, 15, 29)) == fri
    # 15:30 exactly: complete
    assert cn_latest_completed_session(_sh(2026, 3, 2, 15, 30)) == mon
    assert cn_latest_completed_session(_sh(2026, 3, 2, 23, 59)) == mon
    # Lunch break / before open: today not yet complete
    assert cn_latest_completed_session(_sh(2026, 3, 2, 12, 0)) == fri
    # Custom wait
    assert cn_latest_completed_session(_sh(2026, 3, 2, 15, 0), wait_minutes=0) == mon
    assert cn_latest_completed_session(_sh(2026, 3, 2, 15, 29), wait_minutes=0) == mon


def test_latest_completed_session_timezone_aware_and_weekend():
    # 2026-03-02 07:30 UTC == 15:30 Shanghai
    assert cn_latest_completed_session(datetime(2026, 3, 2, 7, 30, tzinfo=UTC)) == date(2026, 3, 2)
    assert cn_latest_completed_session(datetime(2026, 3, 2, 7, 29, tzinfo=UTC)) == date(2026, 2, 27)
    # Sunday evening -> Friday
    assert cn_latest_completed_session(_sh(2026, 3, 1, 20, 0)) == date(2026, 2, 27)


def test_latest_completed_session_across_golden_week():
    # Oct 8 morning: last completed is the last pre-holiday session
    expected = max(cn_sessions(date(2026, 9, 20), date(2026, 9, 30)))
    assert cn_latest_completed_session(_sh(2026, 10, 8, 9, 0)) == expected
    assert cn_latest_completed_session(_sh(2026, 10, 8, 16, 0)) == date(2026, 10, 8)


def test_latest_completed_session_validation():
    with pytest.raises(ValueError):
        cn_latest_completed_session(datetime(2026, 3, 2, 16, 0))
    with pytest.raises(ValueError):
        cn_latest_completed_session(_sh(2026, 3, 2, 16, 0), wait_minutes=-1)


def test_xshe_alias_uses_same_sessions():
    xshe, xshg = XSHECalendar(), XSHGCalendar()
    start, end = date(2026, 1, 1), date(2026, 12, 31)
    assert xshe.sessions(start, end) == xshg.sessions(start, end)
    for day in (date(2026, 2, 17), date(2026, 3, 2), date(2026, 10, 8)):
        assert xshe.session_bounds(day) == xshg.session_bounds(day)
    assert xshe.market is Market.CN


def test_xshe_code_alias_constant():
    from openbars.calendars.cn import XSHE_CODE, XSHG_CODE

    assert XSHE_CODE == XSHG_CODE == "XSHG"
