"""Query-time adjuster: synthetic prices only (TEST1/TEST2 are not real data).

Public split facts used for realism of the dates only: a 4-for-1 on 2020-08-31 and a
3-for-1 on 2022-08-25. All prices and volumes below are made up.
"""

from dataclasses import dataclass
from datetime import date
from decimal import Decimal as D

import pytest
from synth import utc

from openbars.core.models import (
    Adjustment,
    Bar,
    CorporateAction,
    CorporateActionKind,
    Interval,
    QualityFlag,
    Session,
)
from openbars.normalize.adjust import (
    AdjustmentError,
    MISSING_CORPORATE_ACTIONS,
    QueryTimeAdjuster,
    adjust_rows,
    check_adjustment_as_of,
    requires_derivation,
)

SPLIT = CorporateActionKind.SPLIT
DIV = CorporateActionKind.DIVIDEND


def bar(day, close, *, iid="TEST1", stored=Adjustment.SPLIT, volume="4000000", vwap=None):
    c = D(close)
    return Bar(
        instrument_id=iid, interval=Interval.D1, ts_start=utc(day), session_date=day,
        session=Session.REGULAR, open=c - 1, high=c + 2, low=c - 2, close=c,
        volume=None if volume is None else D(volume), source="prov_a", adjustment=stored,
        vwap=None if vwap is None else D(vwap),
    )


def split(day, ratio, *, iid="TEST1", source="prov_a"):
    return CorporateAction(iid, day, SPLIT, source, ratio=D(ratio))


def cash(day, amount, *, iid="TEST1", source="prov_a"):
    return CorporateAction(iid, day, DIV, source, amount=D(amount))


EX1 = date(2020, 8, 31)
PRE1, POST1 = date(2020, 8, 28), date(2020, 9, 1)


def series1():
    # stored split-adjusted as delivered: already post-split
    return [bar(PRE1, "125", vwap="124.5"), bar(EX1, "130"), bar(POST1, "131", volume="3000000")]


def test_split_as_of_none_is_stored_and_unchanged():
    bars = series1()
    out = adjust_rows(bars, [split(EX1, 4)], Adjustment.SPLIT).rows
    assert all(a is b for a, b in zip(out, bars))


def test_raw_undoes_later_split_exactly():
    out = adjust_rows(series1(), [split(EX1, 4)], Adjustment.RAW).rows
    pre, ex, post = out
    assert (pre.open, pre.high, pre.low, pre.close) == (D("496"), D("508"), D("492"), D("500"))
    assert pre.vwap == D("498") and pre.volume == D("1000000")  # volume / 4
    assert pre.adjustment is Adjustment.RAW
    # on/after the ex-date the stored value already is the raw value
    assert (ex.close, ex.volume) == (D("130"), D("4000000"))
    assert (post.close, post.volume) == (D("131"), D("3000000"))
    assert all(isinstance(getattr(pre, f), D) for f in ("open", "high", "low", "close", "volume", "vwap"))


def test_split_as_of_before_ex_date_matches_raw_for_that_day():
    raw = adjust_rows(series1(), [split(EX1, 4)], Adjustment.RAW).rows[0]
    asof = adjust_rows(series1(), [split(EX1, 4)], Adjustment.SPLIT, as_of=date(2020, 8, 30)).rows[0]
    assert (asof.open, asof.high, asof.low, asof.close, asof.volume, asof.vwap) == (
        raw.open, raw.high, raw.low, raw.close, raw.volume, raw.vwap
    )
    assert asof.adjustment is Adjustment.SPLIT  # same basis label as requested


def test_split_as_of_on_or_after_ex_date_matches_stored():
    bars = series1()
    for as_of in (EX1, date(2020, 9, 15)):
        out = adjust_rows(bars, [split(EX1, 4)], Adjustment.SPLIT, as_of=as_of).rows
        assert all(a is b for a, b in zip(out, bars))


def test_second_split_and_compounding_with_as_of_between():
    ex2 = date(2022, 8, 25)
    bars = [
        bar(date(2020, 8, 28), "10", iid="TEST2"),
        bar(date(2021, 6, 1), "12", iid="TEST2"),
        bar(date(2022, 9, 1), "15", iid="TEST2"),
    ]
    acts = [split(EX1, 4, iid="TEST2"), split(ex2, 3, iid="TEST2")]
    raw = adjust_rows(bars, acts, Adjustment.RAW).rows
    assert [b.close for b in raw] == [D("120"), D("36"), D("15")]  # x12, x3, x1
    assert [b.volume for b in raw] == [D("333333.33333333"), D("1333333.33333333"), D("4000000")]
    mid = adjust_rows(bars, acts, Adjustment.SPLIT, as_of=date(2021, 12, 31)).rows  # 3-for-1 not yet
    assert [b.close for b in mid] == [D("30"), D("36"), D("15")]


def test_reverse_split_ratio_below_one():
    bars = [bar(PRE1, "100"), bar(POST1, "400")]
    out = adjust_rows(bars, [split(EX1, "0.25")], Adjustment.RAW).rows
    assert out[0].close == D("25") and out[0].volume == D("16000000")
    assert out[1] is not None and out[1].close == D("400")


def test_stored_raw_bars():
    bars = [bar(PRE1, "500", stored=Adjustment.RAW, volume="1000000"), bar(POST1, "131", stored=Adjustment.RAW)]
    acts = [split(EX1, 4)]
    assert all(a is b for a, b in zip(adjust_rows(bars, acts, Adjustment.RAW).rows, bars))  # no-op
    applied = adjust_rows(bars, acts, Adjustment.SPLIT).rows  # apply later splits
    assert (applied[0].close, applied[0].volume, applied[0].adjustment) == (D("125"), D("4000000"), Adjustment.SPLIT)
    assert applied[1].close == D("131") and applied[1].adjustment is Adjustment.SPLIT
    before = adjust_rows(bars, acts, Adjustment.SPLIT, as_of=date(2020, 8, 30)).rows
    assert before[0].close == D("500") and before[0].volume == D("1000000")  # split not yet known


def test_requires_derivation():
    acts = [split(EX1, 4)]
    assert not requires_derivation(Adjustment.SPLIT)
    assert not requires_derivation(Adjustment.SPLIT, as_of=date(2020, 9, 1), actions=acts)
    assert requires_derivation(Adjustment.SPLIT, as_of=date(2020, 8, 30), actions=acts)
    assert not requires_derivation(Adjustment.SPLIT, as_of=date(2020, 8, 30), actions=[])
    assert requires_derivation(Adjustment.RAW)
    assert not requires_derivation(Adjustment.RAW, stored=Adjustment.RAW)
    assert requires_derivation(Adjustment.SPLIT, stored=Adjustment.RAW)
    assert requires_derivation(Adjustment.ALL)


# --- dividends ----------------------------------------------------------------------------


def div_series():
    return [bar(date(2024, 3, 1), "96"), bar(date(2024, 3, 4), "100"), bar(date(2024, 3, 5), "101")]


def test_cash_dividend_factor_uses_prior_close():
    # ex 2024-03-05, prior close 100, amount 25 -> factor 100/125 = 0.8 on earlier bars
    out = adjust_rows(div_series(), [cash(date(2024, 3, 5), "25")], Adjustment.SPLIT_DIVIDEND).rows
    assert [b.close for b in out] == [D("76.8"), D("80"), D("101")]
    assert [b.open for b in out] == [D("76"), D("79.2"), D("100")]
    assert out[0].volume == D("4000000")  # volume is not dividend adjusted
    assert all(b.adjustment is Adjustment.SPLIT_DIVIDEND for b in out)
    assert adjust_rows(div_series(), [cash(date(2024, 3, 5), "25")], Adjustment.ALL).rows[0].close == D("76.8")
    # SPLIT mode ignores dividends
    assert adjust_rows(div_series(), [cash(date(2024, 3, 5), "25")], Adjustment.SPLIT).rows[0].close == D("96")


def test_dividends_compound_and_respect_as_of():
    acts = [cash(date(2024, 3, 5), "25"), cash(date(2024, 3, 4), "100")]  # 100/125=0.8 ; 96/196
    out = adjust_rows(div_series(), acts, Adjustment.SPLIT_DIVIDEND).rows
    # 2024-03-04 dividend: prior close is 96 -> 96/196
    assert out[0].close == (D("96") * D("0.8") * D("96") / D("196")).quantize(D("1e-8"))
    # as_of 2024-03-04: the 03-05 dividend is not yet known
    early = adjust_rows(div_series(), acts, Adjustment.SPLIT_DIVIDEND, as_of=date(2024, 3, 4)).rows
    assert early[0].close == (D("96") * D("96") / D("196")).quantize(D("1e-8"))
    assert early[1].close == D("100")


def test_dividend_prior_close_is_unadjusted_across_a_later_split():
    # stored (split-adjusted) closes; 2-for-1 on 03-04, so the 03-01 raw close is 200.
    # amount 2 paid per (old) share on ex 03-04: factor 200/202 applied to bars before 03-04.
    bars = [bar(date(2024, 3, 1), "100"), bar(date(2024, 3, 5), "101")]
    acts = [split(date(2024, 3, 4), 2), cash(date(2024, 3, 4), "2")]
    out = adjust_rows(bars, acts, Adjustment.SPLIT_DIVIDEND).rows
    assert out[0].close == (D("100") * D("200") / D("202")).quantize(D("1e-8"))
    assert out[1].close == D("101")


def test_unappliable_actions_are_reported_not_guessed():
    stock = CorporateAction("TEST1", date(2024, 3, 5), DIV, "prov_a", ratio=D("1.05"))
    spin = CorporateAction("TEST1", date(2024, 3, 4), CorporateActionKind.SPINOFF, "prov_a", ratio=D("0.1"))
    bad = cash(date(2024, 3, 5), "-1")
    result = adjust_rows(div_series(), [stock, spin], Adjustment.SPLIT_DIVIDEND)
    assert [b.close for b in result.rows] == [D("96"), D("100"), D("101")]  # unchanged
    assert {(s.action.kind, s.reason) for s in result.skipped} == {
        (DIV, "stock_dividend_not_applied"),
        (CorporateActionKind.SPINOFF, "spinoff_not_adjusted"),
    }
    assert [s.reason for s in adjust_rows(div_series(), [bad], Adjustment.SPLIT_DIVIDEND).skipped] == [
        "invalid_dividend_amount"
    ]
    # opt-in: the stock dividend is a split-like ratio (new/old)
    as_split = adjust_rows(div_series(), [stock], Adjustment.RAW, include_stock_dividends=True)
    assert as_split.rows[0].close == D("100.8") and not as_split.skipped


def test_dividend_without_earlier_bar_changes_nothing():
    out = adjust_rows(div_series(), [cash(date(2024, 2, 1), "5")], Adjustment.SPLIT_DIVIDEND)
    assert [b.close for b in out.rows] == [D("96"), D("100"), D("101")] and not out.skipped


# --- sources, instruments, as_of consistency ------------------------------------------------


def test_duplicate_sources_are_not_double_counted_and_dispute_is_reported():
    acts = [split(EX1, 4, source="a"), split(EX1, 4, source="b")]
    result = adjust_rows(series1(), acts, Adjustment.RAW)
    assert result.rows[0].close == D("500") and not result.disputed
    disputed = adjust_rows(series1(), [split(EX1, 4, source="a"), split(EX1, 2, source="b")], Adjustment.RAW,
                           source_priority=["b"])
    assert disputed.rows[0].close == D("250")  # priority source's row
    assert len(disputed.disputed) == 1 and disputed.disputed[0].conflicts[0].field == "ratio"


def test_actions_only_touch_their_instrument():
    bars = series1() + [bar(PRE1, "20", iid="TEST2")]
    # TEST2 has corporate-action rows (none are splits): attested, so RAW == stored, scale 1
    acts = [split(EX1, 4), cash(date(2020, 1, 2), "1", iid="TEST2")]
    out = adjust_rows(bars, acts, Adjustment.RAW).rows
    assert out[0].close == D("500") and out[3].close == D("20") and out[3].adjustment is Adjustment.RAW


def test_raw_without_any_actions_raises_instead_of_relabelling():
    with pytest.raises(AdjustmentError) as exc:
        adjust_rows(series1(), [], Adjustment.RAW)
    assert exc.value.reason_code == MISSING_CORPORATE_ACTIONS == "missing_corporate_actions"
    # actions of *another* instrument do not attest this one
    with pytest.raises(AdjustmentError):
        adjust_rows(series1(), [split(EX1, 4, iid="TEST2")], Adjustment.RAW)
    # dividend modes need the actions too
    with pytest.raises(AdjustmentError):
        adjust_rows(series1(), [], Adjustment.SPLIT_DIVIDEND)
    with pytest.raises(AdjustmentError):
        adjust_rows([bar(PRE1, "500", stored=Adjustment.RAW)], [], Adjustment.SPLIT)
    with pytest.raises(ValueError):
        adjust_rows(series1(), [], Adjustment.RAW, on_missing_actions="ignore")


def test_requests_that_need_no_derivation_do_not_need_actions():
    bars = series1()
    assert all(a is b for a, b in zip(adjust_rows(bars, [], Adjustment.SPLIT).rows, bars))
    raw = [bar(PRE1, "500", stored=Adjustment.RAW)]
    assert adjust_rows(raw, [], Adjustment.RAW).rows == raw


def test_split_as_of_without_actions_is_flagged_not_raised():
    """An as-of SPLIT read keeps stored prices but cannot attest there were no later splits."""
    bars = series1()
    res = adjust_rows(bars, [], Adjustment.SPLIT, as_of=date(2020, 8, 30))  # default is raise
    assert res.unverified == ("TEST1",)
    assert [b.close for b in res.rows] == [b.close for b in bars]
    assert all(b.adjustment is Adjustment.SPLIT for b in res.rows)
    assert all(b.flags & QualityFlag.ADJUSTMENT_UNVERIFIED for b in res.rows)
    # a later as_of is the same: still no action rows, still unattested
    later = adjust_rows(bars, [], Adjustment.SPLIT, as_of=date(2020, 9, 15), on_missing_actions="raise")
    assert later.unverified == ("TEST1",)
    # attested once the instrument has any corporate-action row
    ok = adjust_rows(bars, [cash(date(2020, 1, 2), "1")], Adjustment.SPLIT, as_of=date(2020, 8, 30))
    assert ok.unverified == () and all(a is b for a, b in zip(ok.rows, bars))


def test_flag_mode_keeps_stored_basis_and_flags_only_unattested_instruments():
    bars = series1() + [bar(PRE1, "20", iid="TEST2")]
    res = adjust_rows(bars, [split(EX1, 4, iid="TEST2")], Adjustment.RAW, on_missing_actions="flag")
    assert res.unverified == ("TEST1",)
    t1, t2 = res.rows[:3], res.rows[3]
    assert [b.close for b in t1] == [D("125"), D("130"), D("131")]  # stored values, not relabelled
    assert all(b.adjustment is Adjustment.SPLIT for b in t1)
    assert all(b.flags & QualityFlag.ADJUSTMENT_UNVERIFIED for b in t1)
    assert t2.adjustment is Adjustment.RAW and t2.close == D("80") and not t2.flags
    # the adjuster class carries the option
    out = QueryTimeAdjuster(on_missing_actions="flag").adjust(series1(), [], Adjustment.RAW)
    assert {b.adjustment for b in out} == {Adjustment.SPLIT}


def test_adjustment_as_of_mixture_is_a_mismatch():
    @dataclass(frozen=True)
    class Row:
        instrument_id: str
        session_date: date
        ts_start: object
        open: D
        high: D
        low: D
        close: D
        volume: D | None
        vwap: D | None
        adjustment: str
        adjustment_as_of: date | None

    def row(day, as_of):
        c = D("10")
        return Row("TEST1", day, utc(day), c, c, c, c, D("1"), None, "split", as_of)

    rows = [row(PRE1, date(2024, 1, 2)), row(POST1, date(2024, 1, 3))]
    assert check_adjustment_as_of([r.adjustment_as_of for r in rows]).reason_code == "adjustment_cutoff_mismatch"
    with pytest.raises(AdjustmentError) as exc:
        adjust_rows(rows, [], Adjustment.RAW)
    assert exc.value.reason_code == "adjustment_cutoff_mismatch"
    same = [row(PRE1, date(2024, 1, 2)), row(POST1, date(2024, 1, 2))]
    assert check_adjustment_as_of(r.adjustment_as_of for r in same).comparable
    assert len(adjust_rows(same, [], Adjustment.SPLIT).rows) == 2


def test_stored_dividend_adjusted_series_cannot_be_rederived():
    bars = [bar(PRE1, "100", stored=Adjustment.ALL)]
    assert adjust_rows(bars, [], Adjustment.ALL).rows == bars
    with pytest.raises(AdjustmentError):
        adjust_rows(bars, [], Adjustment.RAW)


def test_adjuster_class_on_bar_models():
    adj = QueryTimeAdjuster()
    out = adj.adjust(series1(), [split(EX1, 4)], Adjustment.RAW)
    assert out[0].close == D("500") and isinstance(out[0], Bar)
    assert QueryTimeAdjuster(as_of=date(2020, 8, 30)).adjust(series1(), [split(EX1, 4)], Adjustment.SPLIT)[0].close == D("500")
