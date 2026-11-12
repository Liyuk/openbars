"""XNYS calendar tests against independently computed, public calendar facts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from openbars.calendars import (
    TradingSession,
    XNYSCalendar,
    is_non_session_date,
    latest_completed_session,
    reject_non_session_bars,
    session_index,
)
from openbars.core.models import Market

NY = ZoneInfo("America/New_York")
UTC = timezone.utc
CAL = XNYSCalendar()


# --- independent expected values: official NYSE full-day closures -------------------


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    first = date(year, month, 1)
    return first + timedelta(days=(weekday - first.weekday()) % 7 + 7 * (n - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    nxt = date(year + (month == 12), month % 12 + 1, 1)
    last = nxt - timedelta(days=1)
    return last - timedelta(days=(last.weekday() - weekday) % 7)


def _easter(year: int) -> date:  # anonymous Gregorian algorithm
    a, b, c = year % 19, year // 100, year % 100
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    L = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * L) // 451
    month, day = divmod(h + L - 7 * m + 114, 31)
    return date(year, month, day + 1)


def _observed(day: date) -> date | None:
    if day.weekday() == 5:
        return day - timedelta(days=1)
    if day.weekday() == 6:
        return day + timedelta(days=1)
    return day


def full_day_closures(year: int) -> set[date]:
    closures: set[date] = set()
    new_year = date(year, 1, 1)
    # A Saturday New Year's Day is NOT observed on the preceding Friday (NYSE rule).
    if new_year.weekday() != 5:
        closures.add(_observed(new_year))  # type: ignore[arg-type]
    closures.add(_nth_weekday(year, 1, 0, 3))  # MLK
    closures.add(_nth_weekday(year, 2, 0, 3))  # Presidents Day
    closures.add(_easter(year) - timedelta(days=2))  # Good Friday
    closures.add(_last_weekday(year, 5, 0))  # Memorial Day
    if year >= 2022:
        closures.add(_observed(date(year, 6, 19)))  # type: ignore[arg-type]
    closures.add(_observed(date(year, 7, 4)))  # type: ignore[arg-type]
    closures.add(_nth_weekday(year, 9, 0, 1))  # Labor Day
    closures.add(_nth_weekday(year, 11, 3, 4))  # Thanksgiving
    closures.add(_observed(date(year, 12, 25)))  # type: ignore[arg-type]
    if year == 2018:
        closures.add(date(2018, 12, 5))  # National Day of Mourning
    if year == 2025:
        closures.add(date(2025, 1, 9))  # National Day of Mourning
    return {d for d in closures if d.year == year}


def expected_session_count(year: int) -> int:
    weekdays = sum(
        1
        for i in range((date(year, 12, 31) - date(year, 1, 1)).days + 1)
        if (date(year, 1, 1) + timedelta(days=i)).weekday() < 5
    )
    return weekdays - len(full_day_closures(year))


# Frozen cross-check of the rule-based computation above (public NYSE counts).
FROZEN_COUNTS = {
    2016: 252, 2017: 251, 2018: 251, 2019: 252, 2020: 253, 2021: 252,
    2022: 251, 2023: 250, 2024: 252, 2025: 250, 2026: 251,
}  # fmt: skip


@pytest.mark.parametrize("year", range(2016, 2027))
def test_session_count_matches_independent_expectation(year):
    expected = expected_session_count(year)
    assert expected == FROZEN_COUNTS[year]
    got = CAL.sessions(date(year, 1, 1), date(year, 12, 31))
    assert len(got) == expected
    assert not set(got) & full_day_closures(year)
    assert all(d.weekday() < 5 for d in got)


@pytest.mark.parametrize(
    "day,name",
    [
        (date(2016, 1, 1), "new year"),
        (date(2017, 1, 2), "new year observed (Sunday)"),
        (date(2024, 1, 15), "mlk"),
        (date(2024, 2, 19), "presidents"),
        (date(2024, 3, 29), "good friday"),
        (date(2016, 3, 25), "good friday"),
        (date(2024, 5, 27), "memorial"),
        (date(2024, 7, 4), "independence"),
        (date(2026, 7, 3), "independence observed (Saturday)"),
        (date(2024, 9, 2), "labor"),
        (date(2024, 11, 28), "thanksgiving"),
        (date(2024, 12, 25), "christmas"),
        (date(2021, 12, 24), "christmas observed (Saturday)"),
        (date(2022, 6, 20), "juneteenth 2022 (observed)"),
        (date(2025, 6, 19), "juneteenth"),
        (date(2025, 1, 9), "national day of mourning"),
        (date(2018, 12, 5), "national day of mourning"),
    ],
)
def test_known_closures(day, name):
    assert not CAL.is_session(day), name
    assert day not in CAL.sessions(day - timedelta(days=3), day + timedelta(days=3))
    assert CAL.session_bounds(day) == ()
    assert is_non_session_date(day)


@pytest.mark.parametrize("day", [date(2021, 12, 31), date(2021, 6, 18), date(2024, 1, 3)])
def test_known_open_days(day):
    # 2021-12-31: New Year's Day 2022 fell on a Saturday and is not observed on Friday.
    # 2021-06-18: Juneteenth was not yet a market holiday.
    assert CAL.is_session(day)
    assert not is_non_session_date(day)


def test_protocol_attributes_and_range_errors():
    assert CAL.market is Market.US
    assert CAL.timezone == "America/New_York"
    with pytest.raises(ValueError):
        CAL.sessions(date(2024, 1, 10), date(2024, 1, 9))
    assert CAL.sessions(date(2024, 1, 3), date(2024, 1, 3)) == [date(2024, 1, 3)]
    assert CAL.sessions(date(2024, 1, 6), date(2024, 1, 7)) == []


def test_calendar_registered_as_entry_point():
    from importlib.metadata import entry_points

    eps = {e.name: e for e in entry_points(group="openbars.calendars")}
    assert eps["xnys"].load() is XNYSCalendar


# --- bounds -------------------------------------------------------------------------


def test_regular_session_bounds_are_one_utc_segment():
    (segment,) = CAL.session_bounds(date(2024, 1, 3))  # EST
    assert segment == (datetime(2024, 1, 3, 14, 30, tzinfo=UTC), datetime(2024, 1, 3, 21, 0, tzinfo=UTC))
    assert segment[0].utcoffset() == timedelta(0)


@pytest.mark.parametrize("day", [date(2016, 11, 25), date(2024, 11, 29)])
def test_half_day_after_thanksgiving(day):
    ((opens, closes),) = CAL.session_bounds(day)
    assert opens == datetime(day.year, 11, day.day, 14, 30, tzinfo=UTC)  # 09:30 EST
    assert closes == datetime(day.year, 11, day.day, 18, 0, tzinfo=UTC)  # 13:00 EST
    assert closes.astimezone(NY).hour == 13
    session = CAL.trading_session(day)
    assert isinstance(session, TradingSession)
    assert session.closes_at == datetime(day.year, 11, day.day, 13, 0, tzinfo=NY)


def test_regular_close_is_sixteen_ny():
    session = CAL.trading_session(date(2024, 11, 27))
    assert session.opens_at == datetime(2024, 11, 27, 9, 30, tzinfo=NY)
    assert session.closes_at == datetime(2024, 11, 27, 16, 0, tzinfo=NY)


def test_dst_shift_2016():
    ((o1, c1),) = CAL.session_bounds(date(2016, 3, 11))  # EST
    ((o2, c2),) = CAL.session_bounds(date(2016, 3, 14))  # EDT
    assert (o1.hour, o1.minute, c1.hour) == (14, 30, 21)
    assert (o2.hour, o2.minute, c2.hour) == (13, 30, 20)


def test_trading_session_requires_aware_datetimes():
    with pytest.raises(ValueError):
        TradingSession(date(2024, 1, 3), datetime(2024, 1, 3, 9, 30), datetime(2024, 1, 3, 16, 0))


def test_non_session_has_no_trading_session():
    assert CAL.trading_session(date(2024, 7, 4)) is None


def test_ranges_outside_default_calendar_use_bounded_calendar():
    assert CAL.is_session(date(1998, 6, 3))
    assert not CAL.is_session(date(1998, 6, 6))
    assert len(CAL.sessions(date(1999, 1, 1), date(1999, 12, 31))) == 252
    ((o, c),) = CAL.session_bounds(date(1998, 6, 3))
    assert (o.hour, o.minute, c.hour) == (13, 30, 20)
    assert CAL.is_session(date(2035, 6, 4))


# --- session_index ------------------------------------------------------------------


def test_session_index():
    epoch = date(2024, 1, 1)
    assert session_index(date(2023, 12, 31), epoch=epoch) == 0  # before epoch
    assert session_index(date(2024, 1, 1), epoch=epoch) == 0  # holiday, nothing yet
    assert session_index(date(2024, 1, 2), epoch=epoch) == 0  # first session
    assert session_index(date(2024, 1, 3), epoch=epoch) == 1
    assert session_index(date(2024, 1, 6), epoch=epoch) == 3  # Saturday -> Friday's index
    assert session_index(date(2024, 12, 31), epoch=epoch) == 252 - 1
    assert session_index(date(1999, 12, 31)) == 0
    assert session_index(date(2000, 1, 3)) == 0
    assert session_index(date(2000, 1, 4)) == 1


# --- latest_completed_session -------------------------------------------------------


def test_latest_completed_session_wait_boundary():
    close = datetime(2024, 1, 3, 16, 0, tzinfo=NY)
    before = close + timedelta(minutes=29)
    at = close + timedelta(minutes=30)
    assert latest_completed_session(before, wait_minutes=30) == date(2024, 1, 2)
    assert latest_completed_session(at, wait_minutes=30) == date(2024, 1, 3)
    assert latest_completed_session(at.astimezone(UTC), wait_minutes=30) == date(2024, 1, 3)
    assert latest_completed_session(close, wait_minutes=0) == date(2024, 1, 3)


def test_latest_completed_session_half_day_and_weekend():
    half_close = datetime(2024, 11, 29, 13, 0, tzinfo=NY)
    assert latest_completed_session(half_close + timedelta(minutes=30)) == date(2024, 11, 29)
    assert latest_completed_session(half_close + timedelta(minutes=29)) == date(2024, 11, 27)
    sunday = datetime(2024, 1, 7, 12, 0, tzinfo=NY)
    assert latest_completed_session(sunday) == date(2024, 1, 5)


def test_latest_completed_session_validation():
    with pytest.raises(ValueError):
        latest_completed_session(datetime(2024, 1, 3, 22, 0))
    with pytest.raises(ValueError):
        latest_completed_session(datetime(2024, 1, 3, 22, 0, tzinfo=UTC), wait_minutes=-1)


# --- non-session rejection ----------------------------------------------------------


@dataclass(frozen=True)
class FakeBar:
    instrument_id: str
    session_date: date


def test_is_non_session_date():
    assert is_non_session_date(date(2024, 1, 6))  # Saturday
    assert is_non_session_date(date(2024, 1, 7))  # Sunday
    assert is_non_session_date(date(2024, 7, 4))  # holiday inside the default range
    assert not is_non_session_date(date(2024, 7, 5))
    assert not is_non_session_date(date(1998, 6, 3))  # weekday before default start
    assert is_non_session_date(date(1998, 6, 6))  # weekends always
    assert not is_non_session_date(date(2999, 6, 3))  # weekday after default end
    assert is_non_session_date(datetime(2024, 7, 4, 12, 0))  # datetime accepted


def test_reject_non_session_bars():
    holiday = FakeBar("TEST1", date(2024, 7, 4))
    weekend = FakeBar("TEST1", date(2024, 1, 6))
    regular = FakeBar("TEST1", date(2024, 1, 3))
    result = reject_non_session_bars([holiday, regular, weekend])
    assert result.accepted == [regular]
    assert [r.bar for r in result.rejected] == [holiday, weekend]
    assert {r.reason for r in result.rejected} == {"non_session_date"}
    assert result.rejected[0].session_date == date(2024, 7, 4)


def test_reject_non_session_bars_custom_attr_and_type_check():
    @dataclass(frozen=True)
    class Other:
        day: date

    assert reject_non_session_bars([Other(date(2024, 7, 4))], session_date_attr="day").rejected
    with pytest.raises(TypeError):
        reject_non_session_bars([Other("2024-07-04")], session_date_attr="day")
