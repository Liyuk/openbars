"""A-share limit up/down, suspension flags and missing sessions (#61). Synthetic data only."""

from __future__ import annotations

from datetime import date, datetime
from decimal import ROUND_HALF_EVEN, Decimal as D

import pytest

from openbars.calendars import cn as cn_calendar
from openbars.core.models import Interval, QualityFlag, RawBar
from openbars.normalize.cn_quality import (
    CnBoard,
    board_of_code,
    board_of_symbol,
    code_of,
    flags_for_cn_bar,
    flags_for_cn_series,
    ipo_session_number,
    is_suspended,
    limit_prices,
    limit_ratio,
    missing_sessions,
    suspended_dates,
)
from openbars.storage.rows import quality_status_for

DAY = date(2026, 3, 5)
LONG_AGO = date(2015, 1, 5)  # listed long ago: no IPO window


def rawbar(symbol, day, close, *, volume="1000", extra=None):
    c = D(close)
    return RawBar(
        provider="baostock", symbol_as_returned=symbol, fetch_id="f", interval=Interval.D1,
        ts_start=datetime(day.year, day.month, day.day, tzinfo=cn_calendar.CN_TZ),
        open=c, high=c, low=c, close=c, volume=D(volume), extra=extra or {}, session_date=day,
    )  # fmt: skip


# --- boards ------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("code", "board"),
    [
        ("688001", CnBoard.STAR), ("689009", CnBoard.STAR),
        ("300750", CnBoard.CHINEXT), ("301001", CnBoard.CHINEXT),
        ("600000", CnBoard.MAIN), ("601398", CnBoard.MAIN), ("603288", CnBoard.MAIN), ("605001", CnBoard.MAIN),
        ("000001", CnBoard.MAIN), ("001001", CnBoard.MAIN), ("002001", CnBoard.MAIN), ("003001", CnBoard.MAIN),
    ],
)  # fmt: skip
def test_board_of_code(code, board):
    assert board_of_code(code) is board


@pytest.mark.parametrize("code", ["830001", "430047", "920001", "900901", "200002", "399001", "12345", "60000a"])
def test_out_of_scope_codes_raise(code):
    with pytest.raises(ValueError):
        board_of_code(code)


def test_symbol_spellings_keep_leading_zeros():
    for text in ("sz.000001", "000001.SZ", "SZ000001", "cn:eq:xshe:000001", "cn:eq:xshe:000001:g2"):
        assert code_of(text) == "000001"
        assert board_of_symbol(text) is CnBoard.MAIN
    assert board_of_symbol("sh.688001") is board_of_symbol("688001.SH") is board_of_symbol("cn:eq:xshg:688001")
    assert board_of_symbol("sh.688001") is CnBoard.STAR
    with pytest.raises(ValueError):
        code_of("600000")  # a bare code has no exchange


# --- ratios and prices -----------------------------------------------------------------------------


def test_main_board_is_ten_percent():
    for day in (date(2018, 1, 2), date(2026, 3, 5)):
        assert limit_ratio(CnBoard.MAIN, day, listed_on=LONG_AGO) == D("0.1")


def test_chinext_is_ten_percent_before_and_twenty_from_the_reform_date():
    before, on = date(2020, 8, 21), date(2020, 8, 24)
    assert limit_ratio(CnBoard.CHINEXT, before, listed_on=LONG_AGO) == D("0.1")
    assert limit_ratio(CnBoard.CHINEXT, on, listed_on=LONG_AGO) == D("0.2")
    assert limit_ratio(CnBoard.CHINEXT, DAY, listed_on=LONG_AGO) == D("0.2")


def test_star_is_twenty_percent_from_launch():
    assert limit_ratio(CnBoard.STAR, DAY, listed_on=LONG_AGO) == D("0.2")
    assert limit_ratio(CnBoard.STAR, date(2019, 7, 22), listed_on=LONG_AGO) == D("0.2")
    assert limit_ratio(CnBoard.STAR, date(2019, 7, 19)) is None  # the board did not exist yet


def test_st_is_five_percent():
    assert limit_ratio(CnBoard.MAIN, DAY, is_st=True, listed_on=LONG_AGO) == D("0.05")
    assert limit_ratio(CnBoard.MAIN, DAY, is_st=True, listed_on=LONG_AGO, st_ratio=D("0.1")) == D("0.1")


def test_limit_prices_round_half_up_not_bankers():
    # 10.15 * 1.1 = 11.165 and 10.05 * 0.9 = 9.045: a tie on the third decimal
    up, down = limit_prices(D("10.15"), D("0.1"))
    assert up == D("11.17") and (D("10.15") * D("1.1")).quantize(D("0.01"), ROUND_HALF_EVEN) == D("11.16")
    assert down == D("9.14")
    up, down = limit_prices(D("10.05"), D("0.1"))
    assert down == D("9.05") and (D("10.05") * D("0.9")).quantize(D("0.01"), ROUND_HALF_EVEN) == D("9.04")
    assert up == D("11.06")  # 11.055 -> 11.06


def test_limit_prices_known_values_and_validation():
    assert limit_prices(D("10.00"), D("0.1")) == (D("11.00"), D("9.00"))
    assert limit_prices(D("20.00"), D("0.2")) == (D("24.00"), D("16.00"))
    assert limit_prices(D("10.00"), D("0.05")) == (D("10.50"), D("9.50"))
    with pytest.raises(TypeError):
        limit_prices(10.0, D("0.1"))  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        limit_prices(D("0"), D("0.1"))


# --- IPO windows ---------------------------------------------------------------------------------------


def test_ipo_session_number_counts_xshg_sessions_not_calendar_days():
    listed = date(2026, 3, 6)  # Friday
    assert ipo_session_number(listed, listed) == 1
    assert ipo_session_number(listed, date(2026, 3, 9)) == 2  # the weekend does not count
    assert ipo_session_number(listed, date(2026, 3, 5)) is None
    assert ipo_session_number(listed, date(2026, 3, 11), sessions=cn_calendar.sessions(listed, date(2026, 3, 11))) == 4


def test_star_first_five_sessions_have_no_limit_then_twenty():
    listed = date(2026, 3, 2)
    days = cn_calendar.sessions(listed, date(2026, 3, 10))
    ratios = [limit_ratio(CnBoard.STAR, d, listed_on=listed, sessions=days) for d in days]
    assert ratios == [None] * 5 + [D("0.2")] * 2  # 3/2..3/6, then 3/9, 3/10


def test_ipo_window_uses_sessions_across_a_holiday():
    listed = date(2026, 2, 12)  # Spring Festival 2026 closes the exchange 2/16-2/20 (and the week around it)
    days = cn_calendar.sessions(listed, date(2026, 3, 6))
    assert days[:2] == [date(2026, 2, 12), date(2026, 2, 13)]
    sixth = days[5]
    assert limit_ratio(CnBoard.STAR, days[4], listed_on=listed) is None  # via the calendar, no sessions given
    assert limit_ratio(CnBoard.STAR, sixth, listed_on=listed) == D("0.2")
    assert (sixth - listed).days > 5  # five calendar days would have ended the window much earlier


def test_chinext_ipo_rules_depend_on_the_listing_date():
    new = date(2026, 3, 2)
    assert [limit_ratio(CnBoard.CHINEXT, d, listed_on=new) for d in cn_calendar.sessions(new, date(2026, 3, 9))] == (
        [None] * 5 + [D("0.2")]
    )
    old = date(2020, 8, 18)  # before the reform: only the first session is special, then 10%
    days = cn_calendar.sessions(old, date(2020, 8, 21))
    assert [limit_ratio(CnBoard.CHINEXT, d, listed_on=old) for d in days] == [None, D("0.1"), D("0.1"), D("0.1")]


def test_main_board_ipo_rules_depend_on_the_registration_date():
    new = date(2023, 4, 10)
    days = cn_calendar.sessions(new, date(2023, 4, 18))
    assert [limit_ratio(CnBoard.MAIN, d, listed_on=new) for d in days] == [None] * 5 + [D("0.1")] * 2
    old = date(2023, 4, 7)
    days = cn_calendar.sessions(old, date(2023, 4, 12))
    assert [limit_ratio(CnBoard.MAIN, d, listed_on=old) for d in days] == [None, D("0.1"), D("0.1"), D("0.1")]


def test_unknown_listing_date_assumes_no_ipo_window():
    assert limit_ratio(CnBoard.STAR, DAY) == D("0.2")


# --- bar flags -----------------------------------------------------------------------------------------------


def test_close_at_the_limit_flags_up_and_down():
    pre = {"preclose": "10.00", "tradestatus": 1}
    assert flags_for_cn_bar(rawbar("sh.600000", DAY, "11.00", extra=pre), listed_on=LONG_AGO) is QualityFlag.LIMIT_UP
    assert flags_for_cn_bar(rawbar("sh.600000", DAY, "9.00", extra=pre), listed_on=LONG_AGO) is QualityFlag.LIMIT_DOWN


def test_one_tick_away_is_not_a_limit():
    pre = {"preclose": "10.00", "tradestatus": 1}
    for close in ("10.99", "9.01", "10.00", "11.01"):
        assert flags_for_cn_bar(rawbar("sh.600000", DAY, close, extra=pre), listed_on=LONG_AGO) is QualityFlag.NONE


def test_limit_follows_the_board_and_st_status():
    pre = {"preclose": "10.00", "tradestatus": 1}
    star = rawbar("sh.688001", DAY, "12.00", extra=pre)
    assert flags_for_cn_bar(star, listed_on=LONG_AGO) is QualityFlag.LIMIT_UP
    assert flags_for_cn_bar(rawbar("sh.600000", DAY, "12.00", extra=pre), listed_on=LONG_AGO) is QualityFlag.NONE
    assert flags_for_cn_bar(rawbar("sz.300750", DAY, "8.00", extra=pre), listed_on=LONG_AGO) is QualityFlag.LIMIT_DOWN
    st = {**pre, "isST": 1}
    assert flags_for_cn_bar(rawbar("sh.600000", DAY, "10.50", extra=st), listed_on=LONG_AGO) is QualityFlag.LIMIT_UP
    assert flags_for_cn_bar(rawbar("sh.600000", DAY, "10.50", extra=pre), listed_on=LONG_AGO) is QualityFlag.NONE
    assert (
        flags_for_cn_bar(rawbar("sh.600000", DAY, "10.50", extra=pre), listed_on=LONG_AGO, is_st=True)
        is QualityFlag.LIMIT_UP
    )  # the argument wins over extra


def test_rounding_boundary_decides_the_flag():
    pre = {"preclose": "10.15", "tradestatus": 1}
    assert flags_for_cn_bar(rawbar("sh.600000", DAY, "11.17", extra=pre), listed_on=LONG_AGO) is QualityFlag.LIMIT_UP
    assert flags_for_cn_bar(rawbar("sh.600000", DAY, "11.16", extra=pre), listed_on=LONG_AGO) is QualityFlag.NONE


def test_reference_prefers_extra_preclose_over_previous_close():
    ex_rights = {"preclose": "9.00", "tradestatus": 1}  # ex-rights reference below the last close
    bar = rawbar("sh.600000", DAY, "9.90", extra=ex_rights)
    assert flags_for_cn_bar(bar, previous_close=D("10.00"), listed_on=LONG_AGO) is QualityFlag.LIMIT_UP
    bare = rawbar("sh.600000", DAY, "11.00", extra={"tradestatus": 1})
    assert flags_for_cn_bar(bare, previous_close=D("10.00"), listed_on=LONG_AGO) is QualityFlag.LIMIT_UP
    assert flags_for_cn_bar(bare, listed_on=LONG_AGO) is QualityFlag.NONE  # no reference at all


def test_suspended_bar_is_suspended_never_limit():
    for status in (0, "0"):
        # the placeholder sits exactly on a "limit" (preclose 10, close 11) to prove LIMIT_* is skipped
        bar = rawbar("sh.600000", DAY, "11.00", volume="0", extra={"tradestatus": status, "preclose": "10.00"})
        assert is_suspended(bar)
        assert flags_for_cn_bar(bar, listed_on=LONG_AGO) is QualityFlag.SUSPENDED
    assert not is_suspended(rawbar("sh.600000", DAY, "11.00", extra={"tradestatus": 1}))
    assert not is_suspended(rawbar("sh.600000", DAY, "11.00"))


def test_ipo_window_never_sets_limit_flags_even_for_huge_moves():
    listed = date(2026, 3, 2)
    days = cn_calendar.sessions(listed, date(2026, 3, 9))
    huge = {"preclose": "10.00", "tradestatus": 1}
    for d in days[:5]:
        assert flags_for_cn_bar(rawbar("sh.688001", d, "44.00", extra=huge), listed_on=listed) is QualityFlag.NONE
        assert flags_for_cn_bar(rawbar("sz.300750", d, "5.00", extra=huge), listed_on=listed) is QualityFlag.NONE
    sixth = rawbar("sh.688001", days[5], "12.00", extra=huge)
    assert flags_for_cn_bar(sixth, listed_on=listed) is QualityFlag.LIMIT_UP


def test_out_of_scope_board_gets_no_limit_flag_but_keeps_suspension():
    extra = {"preclose": "10.00", "tradestatus": 1}
    assert flags_for_cn_bar(rawbar("sh.900901", DAY, "11.00", extra=extra)) is QualityFlag.NONE
    gone = rawbar("sh.900901", DAY, "10.00", volume="0", extra={"tradestatus": 0})
    assert flags_for_cn_bar(gone) is QualityFlag.SUSPENDED


def test_instrument_id_is_used_for_the_board_when_present():
    from dataclasses import replace

    bar = replace(rawbar("x", DAY, "12.00", extra={"preclose": "10.00", "tradestatus": 1}), instrument_id="cn:eq:xshg:688001")
    assert flags_for_cn_bar(bar, listed_on=LONG_AGO) is QualityFlag.LIMIT_UP


def test_quality_status_stays_complete_for_the_new_flags():
    for flag in (QualityFlag.LIMIT_UP, QualityFlag.LIMIT_DOWN, QualityFlag.SUSPENDED):
        assert quality_status_for(flag) == "complete"


def test_series_uses_previous_close_and_keeps_input_order():
    bars = [
        rawbar("sh.600000", date(2026, 3, 3), "11.00", extra={"tradestatus": 1}),  # +10% on 3/2's close
        rawbar("sh.600000", date(2026, 3, 2), "10.00", extra={"tradestatus": 1}),
        rawbar("sh.600000", date(2026, 3, 4), "11.00", volume="0", extra={"tradestatus": 0}),
        rawbar("sh.600000", date(2026, 3, 5), "9.90", extra={"tradestatus": 1}),  # -10% vs the placeholder
    ]
    flags = flags_for_cn_series(bars, listed_on=LONG_AGO)
    assert flags == [QualityFlag.LIMIT_UP, QualityFlag.NONE, QualityFlag.SUSPENDED, QualityFlag.LIMIT_DOWN]


# --- missing sessions ----------------------------------------------------------------------------------------------


def test_missing_sessions_ignores_holidays_and_suspensions():
    # Spring Festival 2026: 2/16-2/20 are weekdays without a session
    calendar = cn_calendar.sessions(date(2026, 2, 11), date(2026, 2, 25))
    assert date(2026, 2, 17) not in calendar and date(2026, 2, 13) in calendar and date(2026, 2, 24) in calendar
    absent, halted = date(2026, 2, 12), date(2026, 2, 13)
    present = [d for d in calendar if d not in (absent, halted)]
    assert missing_sessions(calendar, present, [halted]) == [absent]
    assert missing_sessions(calendar, present) == [absent, halted]  # without the mark, the halt looks missing
    assert missing_sessions(calendar, calendar) == []  # a bar on a holiday (not in the list) changes nothing
    assert missing_sessions(calendar, [*present, date(2026, 2, 17)], [halted]) == [absent]
    assert missing_sessions([], [DAY]) == []


def test_suspended_dates_from_raw_bars():
    halted = rawbar("sh.600000", date(2026, 3, 4), "10.15", volume="0", extra={"tradestatus": 0})
    traded_zero = rawbar("sh.600000", date(2026, 3, 5), "10.15", volume="0", extra={"tradestatus": 1})
    normal = rawbar("sh.600000", date(2026, 3, 6), "10.15", extra={"tradestatus": "1"})
    assert suspended_dates([halted, traded_zero, normal]) == {date(2026, 3, 4)}
    # persisted raw rows carry no ``extra``: the zero-volume placeholder signature is used
    stored_halt = rawbar("sh.600000", date(2026, 3, 4), "10.15", volume="0")
    assert suspended_dates([stored_halt, rawbar("sh.600000", date(2026, 3, 6), "10.15")]) == {date(2026, 3, 4)}
    assert suspended_dates([]) == set()
