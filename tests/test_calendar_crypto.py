"""Crypto calendar tests: every UTC date is a session, bars cut at UTC 00:00."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from importlib.metadata import entry_points
from zoneinfo import ZoneInfo

import pytest

from openbars.calendars import CryptoCalendar, TradingSession
from openbars.calendars.crypto import (
    NON_SESSION_REASON,
    is_non_session_date,
    is_session,
    latest_completed_session,
    reject_non_session_bars,
    session_bounds,
    session_index,
    sessions,
)
from openbars.core.models import Market

UTC = timezone.utc
CAL = CryptoCalendar()


def test_market_and_timezone():
    assert CryptoCalendar.market is Market.CRYPTO
    assert CAL.market is Market.CRYPTO
    assert CAL.timezone == "UTC"


def test_entry_point_loads_calendar():
    eps = {e.name: e for e in entry_points(group="openbars.calendars")}
    assert eps["crypto"].load() is CryptoCalendar


def test_sessions_cross_weekend_and_leap_day():
    got = sessions(date(2020, 2, 27), date(2020, 3, 2))
    assert got == [date(2020, 2, 27), date(2020, 2, 28), date(2020, 2, 29), date(2020, 3, 1), date(2020, 3, 2)]
    assert date(2020, 2, 29) in got and date(2020, 2, 29).weekday() == 5
    assert date(2020, 3, 1).weekday() == 6


def test_sessions_single_day_and_method():
    d = date(2024, 5, 4)
    assert sessions(d, d) == [d]
    assert CAL.sessions(d, d + timedelta(days=2)) == [d, d + timedelta(days=1), d + timedelta(days=2)]


def test_sessions_end_before_start_raises():
    with pytest.raises(ValueError, match="must not be before start"):
        sessions(date(2024, 1, 2), date(2024, 1, 1))
    with pytest.raises(ValueError):
        CAL.trading_sessions(date(2024, 1, 2), date(2024, 1, 1))


@pytest.mark.parametrize(
    "day",
    [
        date(2024, 5, 15),  # Wednesday
        date(2024, 5, 18),  # Saturday
        date(2024, 5, 19),  # Sunday
        date(2024, 1, 1),
        date(2024, 12, 25),
        date(2020, 2, 29),
    ],
)
def test_is_session_always_true(day):
    assert is_session(day) is True
    assert CAL.is_session(day) is True


def test_session_bounds_utc_half_open_and_abutting():
    start = date(2020, 2, 27)
    prev_close = None
    for day in sessions(start, date(2020, 3, 3)):
        bounds = session_bounds(day)
        assert len(bounds) == 1
        opens, closes = bounds[0]
        assert opens.tzinfo is not None and opens.utcoffset() == timedelta(0)
        assert closes.tzinfo is not None and closes.utcoffset() == timedelta(0)
        assert opens == datetime(day.year, day.month, day.day, tzinfo=UTC)
        assert closes - opens == timedelta(hours=24)
        if prev_close is not None:
            assert opens == prev_close  # no gap, no overlap
        prev_close = closes


def test_session_bounds_year_end():
    ((opens, closes),) = CAL.session_bounds(date(2023, 12, 31))
    assert closes == datetime(2024, 1, 1, tzinfo=UTC)
    # the close instant belongs to the next session
    ((next_opens, _),) = CAL.session_bounds(date(2024, 1, 1))
    assert next_opens == closes


def test_trading_session_keeps_utc():
    ts = CAL.trading_session(date(2024, 3, 10))
    assert isinstance(ts, TradingSession)
    assert ts.session_date == date(2024, 3, 10)
    assert ts.opens_at == datetime(2024, 3, 10, tzinfo=UTC)
    assert ts.closes_at == datetime(2024, 3, 11, tzinfo=UTC)
    assert ts.opens_at.utcoffset() == timedelta(0)
    assert ts.closes_at.utcoffset() == timedelta(0)
    assert ts.closes_at > ts.opens_at


def test_trading_sessions_range():
    out = CAL.trading_sessions(date(2020, 2, 28), date(2020, 3, 1))
    assert [s.session_date for s in out] == [date(2020, 2, 28), date(2020, 2, 29), date(2020, 3, 1)]
    for a, b in zip(out, out[1:]):
        assert a.closes_at == b.opens_at


def test_session_index():
    epoch = date(2000, 1, 1)
    assert session_index(epoch) == 0
    assert session_index(epoch + timedelta(days=1)) == 1
    assert session_index(date(1999, 12, 31)) == 0
    assert session_index(date(2024, 1, 10), epoch=date(2024, 1, 1)) == 9
    assert session_index(date(2024, 1, 1), epoch=date(2024, 1, 10)) == 0


def test_latest_completed_at_next_midnight_no_wait():
    now = datetime(2024, 5, 19, 0, 0, tzinfo=UTC)  # Sunday midnight
    assert latest_completed_session(now, wait_minutes=0) == date(2024, 5, 18)


def test_latest_completed_wait_boundary():
    midnight = datetime(2024, 5, 19, 0, 0, tzinfo=UTC)
    assert latest_completed_session(midnight + timedelta(minutes=29), wait_minutes=30) == date(2024, 5, 17)
    assert latest_completed_session(midnight + timedelta(minutes=30), wait_minutes=30) == date(2024, 5, 18)
    assert latest_completed_session(midnight - timedelta(seconds=1), wait_minutes=0) == date(2024, 5, 17)


def test_latest_completed_compares_in_utc():
    # 2024-05-19 08:00 in +08:00 is 2024-05-19 00:00 UTC
    now = datetime(2024, 5, 19, 8, 0, tzinfo=timezone(timedelta(hours=8)))
    assert latest_completed_session(now, wait_minutes=0) == date(2024, 5, 18)
    ny = datetime(2024, 5, 18, 19, 59, tzinfo=ZoneInfo("America/New_York"))  # 23:59 UTC
    assert latest_completed_session(ny, wait_minutes=0) == date(2024, 5, 17)


def test_latest_completed_validation():
    with pytest.raises(ValueError, match="timezone-aware"):
        latest_completed_session(datetime(2024, 5, 19, 0, 0))
    with pytest.raises(ValueError, match="wait_minutes"):
        latest_completed_session(datetime(2024, 5, 19, tzinfo=UTC), wait_minutes=-1)


def test_is_non_session_date_false():
    assert is_non_session_date(date(2024, 5, 18)) is False
    assert is_non_session_date(date(2024, 12, 25)) is False


@dataclass(frozen=True)
class _Bar:
    session_date: date


def test_reject_non_session_bars_accepts_all():
    bars = [_Bar(date(2024, 5, 17)), _Bar(date(2024, 5, 18)), _Bar(date(2024, 5, 19)), _Bar(date(2024, 12, 25))]
    result = reject_non_session_bars(bars)
    assert result.accepted == bars
    assert result.rejected == []
    assert NON_SESSION_REASON == "non_session_date"


def test_reject_non_session_bars_type_check_and_custom_attr():
    with pytest.raises(TypeError):
        reject_non_session_bars([_Bar("2024-05-18")])  # type: ignore[arg-type]

    @dataclass
    class Other:
        day: date

    assert reject_non_session_bars([Other(date(2024, 5, 18))], session_date_attr="day").accepted
