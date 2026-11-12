"""A-share QFQ / HFQ derived from stored RAW bars + adjust factors (#60). Synthetic data only."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal as D

import pytest

from openbars.core.models import Adjustment, Bar, Interval, Market, QualityFlag, Session
from openbars.normalize.adjust import AdjustmentError, adjust_rows
from openbars.normalize.cn_adjust import (
    AMBIGUOUS_FACTOR_SOURCE,
    MISSING_ADJUST_FACTORS,
    UNSUPPORTED_CN_ADJUSTMENT,
    AdjustFactor,
    adjust_cn_rows,
    from_baostock,
)
from openbars.providers.cn.baostock import BaoStockAdjustFactor
from openbars.reconcile import POLICY_VERSION_CN_DAILY_V1, policy_version_for_market
from openbars.storage.parquet_duckdb import open_backend
from openbars.storage.rows import CanonicalBarRow

IID = "cn:eq:xshg:600000"
OTHER = "cn:eq:xshe:000001"
D2, D3, D4, D5, D6 = (date(2026, 3, d) for d in (2, 3, 4, 5, 6))
EX1, EX2 = D4, D6  # ex-dates; back factors 1.25 from EX1 and 1.5 from EX2


def factor(day, back, *, iid=IID, source="baostock", method="pct_return"):
    return AdjustFactor(iid, day, source, method, D("1") / D("2"), D(back), D("0.8"))


FACTORS = [factor(EX1, "1.25"), factor(EX2, "1.5")]


def bar(day, close, *, iid=IID, adjustment=Adjustment.RAW, **kw):
    c = D(close)
    return Bar(
        instrument_id=iid, interval=Interval.D1, ts_start=datetime(day.year, day.month, day.day, tzinfo=UTC),
        session_date=day, session=Session.REGULAR, open=c - 1, high=c + 1, low=c - 2, close=c,
        volume=D("1000"), source="baostock", adjustment=adjustment, **kw,
    )  # fmt: skip


BARS = [bar(D2, "10"), bar(D3, "10"), bar(D4, "8"), bar(D5, "8"), bar(D6, "6")]


def closes(result):
    return [r.close for r in result.rows]


# --- the arithmetic --------------------------------------------------------------------------


def test_hfq_is_raw_times_back_factor():
    res = adjust_cn_rows(BARS, FACTORS, Adjustment.HFQ)
    # before the first event the factor is 1; 8 * 1.25 = 10; 6 * 1.5 = 9
    assert closes(res) == [D("10"), D("10"), D("10"), D("10"), D("9")]
    assert {r.adjustment for r in res.rows} == {Adjustment.HFQ}
    assert res.unverified == ()


def test_qfq_divides_by_latest_back_factor_and_leaves_volume_alone():
    res = adjust_cn_rows(BARS, FACTORS, Adjustment.QFQ)
    # ref = 1.5: 10/1.5, 8*1.25/1.5, 6*1.5/1.5
    assert closes(res) == [D("6.66666667")] * 4 + [D("6")]  # quantised once, half-even, 8 digits
    assert res.rows[-1].close == D("6")  # the latest session is unchanged
    assert all(r.volume == D("1000") for r in res.rows)
    assert {r.adjustment for r in res.rows} == {Adjustment.QFQ}


def test_ohlc_and_vwap_are_scaled_together():
    b = bar(D6, "6", vwap=D("5.5"))
    (row,) = adjust_cn_rows([b], FACTORS, Adjustment.HFQ).rows
    assert (row.open, row.high, row.low, row.close, row.vwap) == (D("7.5"), D("10.5"), D("6"), D("9"), D("8.25"))
    assert row.volume == D("1000")


def test_sessions_before_the_first_event_use_factor_one():
    res = adjust_cn_rows(BARS[:2], FACTORS, Adjustment.HFQ)
    assert closes(res) == [D("10"), D("10")]  # instrument has factors: attested, factor 1


def test_as_of_ignores_later_events():
    # known on D5: only EX1 is visible, so the qfq divisor is 1.25 (not 1.5)
    qfq = adjust_cn_rows(BARS, FACTORS, Adjustment.QFQ, as_of=D5)
    assert closes(qfq) == [D("8"), D("8"), D("8"), D("8"), D("6") * D("1.25") / D("1.25")]
    # EX2 is not visible: D6 uses the factor of EX1, hfq = 6 * 1.25
    hfq = adjust_cn_rows(BARS, FACTORS, Adjustment.HFQ, as_of=D5)
    assert hfq.rows[-1].close == D("7.5")


def test_no_visible_event_means_qfq_equals_hfq_equals_raw():
    for mode in (Adjustment.QFQ, Adjustment.HFQ):
        res = adjust_cn_rows(BARS, FACTORS, mode, as_of=date(2026, 3, 3))
        assert closes(res) == [b.close for b in BARS]


def test_event_on_the_session_itself_applies_that_day():
    res = adjust_cn_rows([bar(D3, "10"), bar(D4, "8")], FACTORS, Adjustment.HFQ)
    assert closes(res) == [D("10"), D("10")]  # D4 is the ex-date: 8 * 1.25


def test_raw_is_a_noop_and_other_modes_are_refused():
    res = adjust_cn_rows(BARS, [], Adjustment.RAW)
    assert res.rows == BARS
    for mode in (Adjustment.SPLIT, Adjustment.ALL, Adjustment.SPLIT_DIVIDEND):
        with pytest.raises(AdjustmentError) as e:
            adjust_cn_rows(BARS, FACTORS, mode)
        assert e.value.reason_code == UNSUPPORTED_CN_ADJUSTMENT


def test_already_adjusted_rows_cannot_be_rebased():
    with pytest.raises(AdjustmentError) as e:
        adjust_cn_rows([bar(D2, "10", adjustment=Adjustment.HFQ)], FACTORS, Adjustment.QFQ)
    assert e.value.reason_code == UNSUPPORTED_CN_ADJUSTMENT


def test_us_adjuster_refuses_qfq_hfq():
    with pytest.raises(AdjustmentError):
        adjust_rows(BARS, [], Adjustment.QFQ)


def test_instruments_are_adjusted_independently():
    other = [bar(D2, "20", iid=OTHER), bar(D6, "30", iid=OTHER)]
    res = adjust_cn_rows([*BARS, *other], [*FACTORS, factor(D5, "2", iid=OTHER)], Adjustment.HFQ)
    assert [r.close for r in res.rows if r.instrument_id == OTHER] == [D("20"), D("60")]
    assert res.rows[-3].close == D("9")  # input order kept: BARS[-1] before the OTHER rows


# --- sources ---------------------------------------------------------------------------------


def test_two_sources_without_source_raise_and_with_source_use_only_that_one():
    other = [factor(EX1, "9", source="other"), factor(EX2, "99", source="other")]
    both = [*FACTORS, *other]
    with pytest.raises(AdjustmentError) as e:
        adjust_cn_rows(BARS, both, Adjustment.HFQ)
    assert e.value.reason_code == AMBIGUOUS_FACTOR_SOURCE
    mine = adjust_cn_rows(BARS, both, Adjustment.HFQ, source="baostock")
    assert closes(mine) == closes(adjust_cn_rows(BARS, FACTORS, Adjustment.HFQ))
    theirs = adjust_cn_rows(BARS, both, Adjustment.HFQ, source="other")
    assert theirs.rows[-1].close == D("594")  # 6 * 99


def test_single_source_is_picked_automatically():
    res = adjust_cn_rows(BARS, [factor(EX1, "1.25", source="other")], Adjustment.HFQ)
    assert res.rows[2].close == D("10")


def test_source_without_rows_for_the_instrument_counts_as_missing():
    with pytest.raises(AdjustmentError) as e:
        adjust_cn_rows(BARS, FACTORS, Adjustment.HFQ, source="other")
    assert e.value.reason_code == MISSING_ADJUST_FACTORS


# --- missing factors -------------------------------------------------------------------------


@pytest.mark.parametrize("mode", [Adjustment.QFQ, Adjustment.HFQ])
def test_no_factor_rows_raise_by_default(mode):
    with pytest.raises(AdjustmentError) as e:
        adjust_cn_rows(BARS, [], mode)
    assert e.value.reason_code == MISSING_ADJUST_FACTORS
    assert IID in str(e.value)


def test_no_factor_rows_flag_returns_stored_raw_unverified():
    res = adjust_cn_rows(BARS, [], Adjustment.QFQ, on_missing_factors="flag")
    assert closes(res) == [b.close for b in BARS]
    assert {r.adjustment for r in res.rows} == {Adjustment.RAW}  # never relabelled
    assert all(QualityFlag.ADJUSTMENT_UNVERIFIED in r.flags for r in res.rows)
    assert res.unverified == (IID,)


def test_flag_only_marks_the_instrument_without_factors():
    rows = [*BARS, bar(D2, "20", iid=OTHER)]
    res = adjust_cn_rows(rows, FACTORS, Adjustment.HFQ, on_missing_factors="flag")
    assert res.unverified == (OTHER,)
    assert res.rows[-1].adjustment is Adjustment.RAW and res.rows[-1].close == D("20")
    assert res.rows[2].adjustment is Adjustment.HFQ and QualityFlag.ADJUSTMENT_UNVERIFIED not in res.rows[2].flags


def test_bad_on_missing_factors_value_is_rejected():
    with pytest.raises(ValueError):
        adjust_cn_rows(BARS, FACTORS, Adjustment.HFQ, on_missing_factors="ignore")


# --- the factor model ------------------------------------------------------------------------


def test_from_baostock_keeps_source_method_and_code():
    bs = BaoStockAdjustFactor("sh.600000", EX1, D("0.8"), D("1.25"), D("1.25"))
    f = from_baostock(bs, IID)
    assert (f.instrument_id, f.effective_from, f.source, f.method) == (IID, EX1, "baostock", "pct_return")
    assert (f.fore_adjust_factor, f.back_adjust_factor, f.adjust_factor, f.symbol) == (
        D("0.8"), D("1.25"), D("1.25"), "sh.600000",
    )  # fmt: skip


def test_adjust_factor_rejects_float_and_non_positive():
    with pytest.raises(TypeError):
        AdjustFactor(IID, EX1, "baostock", "pct_return", D("1"), 1.25, D("1"))  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        factor(EX1, "0")
    with pytest.raises(TypeError):
        AdjustFactor(IID, datetime(2026, 3, 4), "baostock", "pct_return", D("1"), D("1"), D("1"))  # type: ignore[arg-type]


# --- policy dispatch -------------------------------------------------------------------------


def test_policy_version_for_cn():
    assert POLICY_VERSION_CN_DAILY_V1 == "cn-daily-v1"
    assert policy_version_for_market(Market.CN) == "cn-daily-v1"
    assert policy_version_for_market(None, instrument_ids=[IID]) == "cn-daily-v1"
    assert policy_version_for_market(Market.CN, "research-daily-v2") == "research-daily-v2"  # override wins
    assert policy_version_for_market(None, instrument_ids=["us:eq:T1"]) == "research-daily-v2"


# --- Parquet round trip ----------------------------------------------------------------------


@pytest.fixture
def backend(tmp_path):
    return open_backend(tmp_path / "data")


def crow(iid, day, close, *, volume="1000"):
    c = D(close)
    return CanonicalBarRow(
        instrument_id=iid, market=Market.CN, interval=Interval.D1,
        ts_start=datetime(day.year, day.month, day.day, tzinfo=UTC), session_date=day,
        session=Session.REGULAR, provider="baostock", open=c - 1, high=c + 1, low=c - 2, close=c,
        volume=D(volume), price_source="baostock:ashare", reconcile_rule="primary_source",
        policy_version="cn-daily-v1", adjustment="raw", feed="ashare",
        generated_at=datetime(2026, 3, 1, 12, tzinfo=UTC),
    )  # fmt: skip


def seed(backend):
    backend.canonical.upsert_rows([crow(IID, b.session_date, b.close) for b in BARS])
    backend.instruments.upsert_adjust_factors(FACTORS)


def col(table, name):
    return table.column(name).to_pylist()


def test_factor_store_missing_file_is_empty_and_upsert_dedups(backend):
    store = backend.instruments
    assert store.adjust_factors() == []
    assert not (backend.ref_dir / "adjust_factors.parquet").exists()
    assert store.upsert_adjust_factors(FACTORS) == 2
    store.upsert_adjust_factors([factor(EX1, "1.30")])  # same (instrument, source, date): new wins
    got = {f.effective_from: f for f in store.adjust_factors()}
    assert len(got) == 2 and got[EX1].back_adjust_factor == D("1.30")
    assert got[EX2].fore_adjust_factor == D("0.5") and got[EX2].adjust_factor == D("0.8")  # exact Decimals


def test_factor_store_keeps_sources_side_by_side_and_filters(backend):
    store = backend.instruments
    store.upsert_adjust_factors([*FACTORS, factor(EX1, "3", source="other"), factor(EX1, "4", iid=OTHER)])
    assert len(store.adjust_factors()) == 4
    assert {f.source for f in store.adjust_factors([IID])} == {"baostock", "other"}
    assert [f.instrument_id for f in store.adjust_factors(source="baostock")] == [OTHER, IID, IID]  # sorted by key
    assert store.adjust_factors([OTHER], source="other") == []


def test_factor_store_does_not_touch_schema_version_and_is_viewable(backend):
    from openbars.storage.schema import SCHEMA_VERSION

    assert SCHEMA_VERSION == 1
    seed(backend)
    t = backend.query("SELECT instrument_id, effective_from, back_adjust_factor FROM adjust_factors ORDER BY 2")
    assert col(t, "back_adjust_factor") == [D("1.25"), D("1.5")]
    backend.rebuild_catalog()
    assert backend.query("SELECT count(*) AS n FROM adjust_factors").column("n").to_pylist() == [2]


def test_reader_default_and_raw_return_the_stored_series_for_cn(backend):
    seed(backend)
    stored = [b.close for b in BARS]
    for kwargs in ({}, {"adjustment": Adjustment.RAW}, {"market": Market.CN}, {"market": Market.CN, "adjustment": Adjustment.RAW}):
        t = backend.reader.bars([IID], **kwargs)
        assert col(t, "close") == stored, kwargs
        assert set(col(t, "adjustment")) == {"raw"}


def test_reader_hfq_and_qfq_are_derived_from_stored_raw(backend):
    seed(backend)
    hfq = backend.reader.bars([IID], adjustment=Adjustment.HFQ)
    assert col(hfq, "close") == [D("10"), D("10"), D("10"), D("10"), D("9")]
    assert set(col(hfq, "adjustment")) == {"hfq"}
    assert col(hfq, "volume") == [D("1000")] * 5
    qfq = backend.reader.bars([IID], adjustment=Adjustment.QFQ, market=Market.CN)
    assert col(qfq, "close")[-1] == D("6") and col(qfq, "close")[0] == D("6.66666667")
    assert set(col(qfq, "adjustment")) == {"qfq"}
    # nothing was persisted: the stored rows are still RAW
    assert col(backend.reader.bars([IID]), "close") == [b.close for b in BARS]


def test_reader_qfq_respects_as_of_and_window(backend):
    seed(backend)
    t = backend.reader.bars([IID], adjustment=Adjustment.QFQ, as_of=D5)
    assert col(t, "close") == [D("8"), D("8"), D("8"), D("8"), D("6")]  # EX2 not yet known
    win = backend.reader.bars([IID], adjustment=Adjustment.HFQ, start=D3, end=D4)
    assert col(win, "close") == [D("10"), D("10")]


def test_reader_missing_factors_raise_or_flag(backend):
    backend.canonical.upsert_rows([crow(IID, b.session_date, b.close) for b in BARS])
    with pytest.raises(AdjustmentError) as e:
        backend.reader.bars([IID], adjustment=Adjustment.QFQ)
    assert e.value.reason_code == MISSING_ADJUST_FACTORS
    t = backend.reader.bars([IID], adjustment=Adjustment.QFQ, on_missing_factors="flag")
    assert col(t, "close") == [b.close for b in BARS]
    assert set(col(t, "adjustment")) == {"raw"}
    assert all("ADJUSTMENT_UNVERIFIED" in flags for flags in col(t, "flags"))
    # on_missing_actions is reused when on_missing_factors is not given
    t2 = backend.reader.bars([IID], adjustment=Adjustment.HFQ, on_missing_actions="flag")
    assert all("ADJUSTMENT_UNVERIFIED" in flags for flags in col(t2, "flags"))


def test_reader_factor_source_selection(backend):
    seed(backend)
    backend.instruments.upsert_adjust_factors([factor(EX1, "9", source="other")])
    with pytest.raises(AdjustmentError) as e:
        backend.reader.bars([IID], adjustment=Adjustment.HFQ)
    assert e.value.reason_code == AMBIGUOUS_FACTOR_SOURCE
    t = backend.reader.bars([IID], adjustment=Adjustment.HFQ, factor_source="baostock")
    assert col(t, "close")[-1] == D("9")


def test_reader_never_applies_us_actions_to_cn_and_rejects_split_dividend(backend):
    seed(backend)
    with pytest.raises(AdjustmentError) as e:
        backend.reader.bars([IID], adjustment=Adjustment.ALL)
    assert e.value.reason_code == UNSUPPORTED_CN_ADJUSTMENT


def test_reader_us_and_cn_rows_mix_without_cross_effects(backend):
    seed(backend)
    us = "us:eq:T1"
    backend.canonical.upsert_rows([
        CanonicalBarRow(
            instrument_id=us, market=Market.US, interval=Interval.D1,
            ts_start=datetime(2026, 3, 2, tzinfo=UTC), session_date=D2, session=Session.REGULAR,
            provider="prov_a", open=D("99"), high=D("101"), low=D("98"), close=D("100"),
            volume=D("5"), price_source="prov_a", reconcile_rule="primary_wins", adjustment="split",
            generated_at=datetime(2026, 3, 1, 12, tzinfo=UTC),
        )
    ])  # fmt: skip
    t = backend.reader.bars([IID, us])  # default read: each series as stored
    assert sorted(set(col(t, "instrument_id"))) == [IID, us]
    assert dict(zip(col(t, "instrument_id"), col(t, "adjustment"))) == {IID: "raw", us: "split"}
    with pytest.raises(AdjustmentError):  # QFQ is not defined for the US row
        backend.reader.bars([IID, us], adjustment=Adjustment.QFQ)
    only_cn = backend.reader.bars([IID], adjustment=Adjustment.QFQ, factor_source="baostock")
    assert col(only_cn, "close")[-1] == D("6")
