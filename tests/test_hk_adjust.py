"""HK share adjustment: synthetic prices only. 00000 is not a live listing."""

from __future__ import annotations

from datetime import date
from decimal import Decimal as D

import pytest
from synth import utc

from openbars.core.models import Adjustment, Bar, CorporateActionKind, Interval, Session
from openbars.normalize.hk_adjust import (
    bonus_ratio,
    hk_adjust_rows,
    hk_bonus,
    hk_cash_dividend,
    hk_consolidation,
    hk_split,
    share_ratio,
)

IID = "hk:eq:xhkg:00000"
EX = date(2024, 6, 3)
PRE, POST = date(2024, 5, 31), date(2024, 6, 4)


def bar(day, close, *, volume="4000000", stored=Adjustment.SPLIT):
    c = D(close)
    return Bar(
        instrument_id=IID,
        interval=Interval.D1,
        ts_start=utc(day),
        session_date=day,
        session=Session.REGULAR,
        open=c - 1,
        high=c + 2,
        low=c - 2,
        close=c,
        volume=D(volume),
        source="yfinance",
        adjustment=stored,
    )


def series():
    # stored split-adjusted as delivered, same profile as US research-daily-v2
    return [bar(PRE, "125"), bar(EX, "130"), bar(POST, "131", volume="3000000")]


def test_share_and_bonus_ratios_are_exact():
    assert share_ratio(2, 1) == D(2)
    assert share_ratio(1, 5) == D("0.2")
    assert bonus_ratio(1, 2) == D("1.5")  # 1 bonus for every 2 held -> 3-for-2
    with pytest.raises(ValueError):
        share_ratio(0, 1)
    with pytest.raises(ValueError):
        bonus_ratio(1, 0)
    with pytest.raises(TypeError):
        share_ratio(True, 1)


def test_split_constructor_rejects_non_increasing():
    with pytest.raises(ValueError, match="increase"):
        hk_split(IID, EX, 1, 2, "yfinance")
    with pytest.raises(ValueError, match="increase"):
        hk_split(IID, EX, 1, 1, "yfinance")


def test_consolidation_constructor_rejects_non_decreasing():
    with pytest.raises(ValueError, match="decrease"):
        hk_consolidation(IID, EX, 2, 1, "yfinance")


def test_split_raw_undoes_later_2_for_1():
    act = hk_split(IID, EX, 2, 1, "yfinance")
    assert act.kind is CorporateActionKind.SPLIT and act.ratio == D(2)
    out = hk_adjust_rows(series(), [act], Adjustment.RAW).rows
    pre, ex, post = out
    assert pre.close == D("250") and pre.volume == D("2000000")
    assert (ex.close, post.close) == (D("130"), D("131"))
    # default SPLIT read is the stored series
    bars = series()
    stored = hk_adjust_rows(bars, [act], Adjustment.SPLIT).rows
    assert all(a is b for a, b in zip(stored, bars))


def test_consolidation_1_for_5_raw_undo():
    # stored prices already reflect the 1-for-5; undoing multiplies close by 0.2
    bars = [bar(PRE, "100"), bar(POST, "20")]
    act = hk_consolidation(IID, EX, 1, 5, "yfinance")
    assert act.kind is CorporateActionKind.SPLIT and act.ratio == D("0.2")
    out = hk_adjust_rows(bars, [act], Adjustment.RAW).rows
    assert out[0].close == D("20") and out[0].volume == D("20000000")
    assert out[1].close == D("20")


def test_bonus_is_skipped_by_default_and_applied_when_opted_in():
    # 1 bonus for 1 held -> ratio 2, same arithmetic as a 2-for-1 split
    act = hk_bonus(IID, EX, bonus=1, held=1, source="yfinance")
    assert (
        act.kind is CorporateActionKind.DIVIDEND
        and act.ratio == D(2)
        and act.amount is None
    )
    skipped = hk_adjust_rows(series(), [act], Adjustment.RAW)
    assert [b.close for b in skipped.rows] == [D("125"), D("130"), D("131")]
    assert [s.reason for s in skipped.skipped] == ["stock_dividend_not_applied"]
    applied = hk_adjust_rows(series(), [act], Adjustment.RAW, include_bonus=True)
    assert applied.rows[0].close == D("250") and not applied.skipped
    # alias of include_stock_dividends
    also = hk_adjust_rows(series(), [act], Adjustment.RAW, include_stock_dividends=True)
    assert also.rows[0].close == D("250")


def test_include_bonus_disagrees_with_stock_flag():
    act = hk_bonus(IID, EX, 1, 1, "yfinance")
    with pytest.raises(ValueError, match="disagree"):
        hk_adjust_rows(
            series(),
            [act],
            Adjustment.RAW,
            include_stock_dividends=True,
            include_bonus=False,
        )


def test_cash_dividend_uses_same_factor_as_us():
    bars = [
        bar(date(2024, 3, 1), "96"),
        bar(date(2024, 3, 4), "100"),
        bar(date(2024, 3, 5), "101"),
    ]
    act = hk_cash_dividend(IID, date(2024, 3, 5), "25", "yfinance")
    out = hk_adjust_rows(bars, [act], Adjustment.SPLIT_DIVIDEND).rows
    assert [b.close for b in out] == [D("76.8"), D("80"), D("101")]
    assert out[0].volume == D("4000000")
    assert hk_adjust_rows(bars, [act], Adjustment.SPLIT).rows[0].close == D("96")
