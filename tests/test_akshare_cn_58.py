"""AKShare A-share adapter (#58): injected fake hist, synthetic rows, no network by default."""

from __future__ import annotations

import os
import sys
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from openbars.core.errors import ProviderError, QuotaExhausted
from openbars.core.models import Adjustment, DateRange, Interval, Market
from openbars.core.series import (
    AKSHARE_CN_EASTMONEY_RAW,
    AKSHARE_CN_SINA_RAW,
    AKSHARE_EASTMONEY_RAW,
    KNOWN_SOURCES,
    AdjustmentScope,
    BarSeriesCatalog,
    PriceBasis,
    SessionSemantics,
    VolumeBasis,
)
from openbars.providers.adapter import NullQuotaGate
from openbars.providers.base import BatchShape, FetchRequest
from openbars.providers.cn.akshare import (
    AKShareCNProvider,
    to_eastmoney_a_symbol,
    to_native_cn_symbol,
    to_sina_a_symbol,
)

NOW = datetime(2026, 3, 10, tzinfo=UTC)
SHANGHAI = ZoneInfo("Asia/Shanghai")

EASTMONEY_ROWS = [
    {
        "日期": date(2026, 3, 2),
        "股票代码": "600000",
        "开盘": 10.0,
        "收盘": 10.4,
        "最高": 10.5,
        "最低": 9.9,
        "成交量": 1234.0,  # lots (手)
        "成交额": 1_287_000.0,
        "振幅": 6.0,
        "涨跌幅": 4.0,
        "涨跌额": 0.4,
        "换手率": 0.5,
    },
    {
        "日期": date(2026, 3, 3),
        "股票代码": "600000",
        "开盘": 10.4,
        "收盘": 10.2,
        "最高": 10.6,
        "最低": 10.1,
        "成交量": 2345.0,
        "成交额": 2_405_000.0,
        "振幅": 4.8,
        "涨跌幅": -1.9,
        "涨跌额": -0.2,
        "换手率": 0.9,
    },
]

SINA_ROWS = [
    {
        "date": date(2026, 3, 2),
        "open": 10.0,
        "high": 10.5,
        "low": 9.9,
        "close": 10.4,
        "volume": 123_400.0,  # shares
        "amount": 1_287_000.0,
        "outstanding_share": 1_000_000_000.0,
        "turnover": 0.0001234,
    },
    {
        "date": date(2026, 3, 3),
        "open": 10.4,
        "high": 10.6,
        "low": 10.1,
        "close": 10.2,
        "volume": 234_500.0,
        "amount": 2_405_000.0,
        "outstanding_share": 1_000_000_000.0,
        "turnover": 0.0002345,
    },
]

SECOND_CODE_ROWS = [{**row, "股票代码": "000001"} for row in EASTMONEY_ROWS]


def cn_req(*symbols: str, start="2026-03-02", end="2026-03-06", **kw) -> FetchRequest:
    return FetchRequest(
        market=Market.CN,
        interval=Interval.D1,
        symbols=symbols or ("600000.SH",),
        window=DateRange(date.fromisoformat(start), date.fromisoformat(end)),
        **kw,
    )


class FakeHist:
    def __init__(self, data=None, errors=None):
        self.data = data or {}
        self.errors = errors or {}
        self.calls: list[tuple[str, dict]] = []

    def __call__(self, symbol, **kwargs):
        self.calls.append((symbol, kwargs))
        if symbol in self.errors:
            raise self.errors[symbol]
        return self.data.get(symbol, [])


class RecordingGate(NullQuotaGate):
    def __init__(self):
        self.acquired: list[str] = []

    def acquire(self, provider, weight=1, *, scope="default", timeout=None):
        self.acquired.append(provider)
        return super().acquire(provider, weight, scope=scope)


def provider(hist=None, **kw) -> AKShareCNProvider:
    feeds = (
        hist
        if hist is not None
        else {"eastmoney": FakeHist(), "sina": FakeHist()}
    )
    return AKShareCNProvider(feeds, clock=lambda: NOW, **kw)


def test_symbol_helpers():
    assert to_eastmoney_a_symbol("600000.SH") == "600000"
    assert to_eastmoney_a_symbol("sh.600000") == "600000"
    assert to_sina_a_symbol("600000.SH") == "sh600000"
    assert to_sina_a_symbol("000001.SZ") == "sz000001"
    assert to_native_cn_symbol("600000.SH") == "sh.600000"
    assert to_native_cn_symbol("sz.000001") == "sz.000001"
    for raw in ("600000", "", "xx.600000", "sh.60000"):
        with pytest.raises(ValueError):
            to_native_cn_symbol(raw)


def test_series_identities_are_distinct_from_hk_and_cny():
    assert AKSHARE_CN_EASTMONEY_RAW != AKSHARE_EASTMONEY_RAW
    assert AKSHARE_CN_SINA_RAW != AKSHARE_CN_EASTMONEY_RAW
    assert AKSHARE_CN_EASTMONEY_RAW.provider == "akshare_ashare"
    assert (AKSHARE_CN_EASTMONEY_RAW.feed, AKSHARE_CN_SINA_RAW.feed) == (
        "eastmoney",
        "sina",
    )
    assert AKSHARE_CN_EASTMONEY_RAW.adjustment == "raw"
    assert AKSHARE_CN_EASTMONEY_RAW not in KNOWN_SOURCES
    assert AKSHARE_CN_SINA_RAW not in KNOWN_SOURCES
    for identity in (AKSHARE_CN_EASTMONEY_RAW, AKSHARE_CN_SINA_RAW):
        series = BarSeriesCatalog.resolve(identity)
        assert series.currency == "CNY"
        assert series.price_basis is PriceBasis.RAW
        assert series.adjustment_scope is AdjustmentScope.NONE
        assert series.session_semantics is SessionSemantics.REGULAR
        assert series.volume_basis is VolumeBasis.AS_TRADED


def test_capabilities_and_quota_are_conservative():
    p = provider()
    (cap,) = p.capabilities()
    assert p.id == "akshare_ashare"
    assert (cap.provider, cap.market, cap.interval) == (
        "akshare_ashare",
        Market.CN,
        Interval.D1,
    )
    assert cap.batch is BatchShape.PER_SYMBOL
    assert cap.max_symbols_per_request == 1
    assert cap.adjusted_modes == frozenset({Adjustment.RAW})
    assert cap.feed == "eastmoney"
    assert cap.terms.personal_only and not cap.terms.redistribution_allowed
    assert cap.terms.url and "disclaimer" in cap.terms.url
    notes = cap.terms.notes or ""
    assert "Eastmoney" in notes and "Sina" in notes and "MIT" in notes
    assert "unadjusted" in notes.lower()
    policy = p.quota_policy()
    assert policy.provider == "akshare_ashare"
    assert policy.max_batch_symbols == 1
    assert (policy.limits[0].count, policy.limits[0].per) == (4, timedelta(minutes=1))
    assert policy.limits[1].count == 80
    assert policy.key_scope == "per_process"
    assert policy.effective_limit(policy.limits[0]) == 3


def test_eastmoney_fetch_requests_unadjusted_and_covers_lots_to_shares():
    hist = FakeHist({"600000": EASTMONEY_ROWS})
    batch = provider({"eastmoney": hist}).fetch_bars(cn_req("600000.SH"))
    assert hist.calls[0][0] == "600000"
    assert hist.calls[0][1] == {
        "period": "daily",
        "start_date": "20260302",
        "end_date": "20260306",
        "adjust": "",
    }
    assert [b.session_date.isoformat() for b in batch.bars] == [
        "2026-03-02",
        "2026-03-03",
    ]
    first = batch.bars[0]
    assert (first.provider, first.feed) == ("akshare_ashare", "eastmoney")
    assert first.symbol_as_returned == "sh.600000"
    assert first.series.source == AKSHARE_CN_EASTMONEY_RAW
    assert first.close == Decimal("10.4")
    assert first.volume == Decimal("123400")  # 1234 lots * 100
    assert first.extra["code"] == "sh.600000"
    assert first.extra["volume_lots"] == "1234"
    assert first.extra["amount"] == "1287000"
    assert first.extra["turnover_rate"] == "0.5"
    assert first.ts_start == datetime(2026, 3, 2, tzinfo=SHANGHAI)
    assert first.ts_start.utcoffset() == timedelta(hours=8)
    assert batch.error is None


def test_sina_feed_uses_prefixed_symbol_and_share_volume():
    hist = FakeHist({"sh600000": SINA_ROWS})
    batch = provider({"sina": hist}).fetch_bars(
        cn_req("600000.SH", feed="sina")
    )
    assert hist.calls[0][0] == "sh600000"
    assert hist.calls[0][1] == {
        "start_date": "20260302",
        "end_date": "20260306",
        "adjust": "",
    }
    first = batch.bars[0]
    assert first.feed == "sina" and first.series.source == AKSHARE_CN_SINA_RAW
    assert first.volume == Decimal("123400")
    assert "volume_lots" not in first.extra
    assert first.extra["outstanding_share"] == "1000000000"
    assert first.extra["turnover_ratio"] == "0.0001234"


def test_english_columns_and_spelling_dedupe():
    rows = [
        {
            "date": "2026-03-02",
            "open": 10,
            "high": 11,
            "low": 9,
            "close": 10.5,
            "volume": 100,
        }
    ]
    hist = FakeHist({"600000": rows})
    provider({"eastmoney": hist}).fetch_bars(
        cn_req("600000.SH", "sh.600000", "SH600000")
    )
    assert [c[0] for c in hist.calls] == ["600000"]


def test_skips_non_session_rows():
    rows = [
        *EASTMONEY_ROWS,
        {**EASTMONEY_ROWS[0], "日期": date(2026, 3, 7)},  # Saturday
        {**EASTMONEY_ROWS[0], "日期": date(2026, 3, 8)},  # Sunday
    ]
    result = provider({"eastmoney": FakeHist({"600000": rows})}).fetch_bars_result(
        cn_req("600000.SH", start="2026-03-02", end="2026-03-08")
    )
    assert [b.session_date.isoformat() for b in result.batch.bars] == [
        "2026-03-02",
        "2026-03-03",
    ]
    assert result.skipped.get("non_session", 0) == 2


def test_skips_rows_for_another_vendor_code():
    result = provider(
        {"eastmoney": FakeHist({"600000": EASTMONEY_ROWS, "000001": EASTMONEY_ROWS})}
    ).fetch_bars_result(cn_req("600000.SH", "000001.SZ"))
    assert [b.symbol_as_returned for b in result.batch.bars] == [
        "sh.600000",
        "sh.600000",
    ]
    assert result.skipped.get("other_code", 0) == 2
    assert result.batch.error is not None and result.batch.error.code == "empty_result"


def test_default_feed_is_eastmoney():
    hist = FakeHist({"600000": EASTMONEY_ROWS})
    batch = provider({"eastmoney": hist}).fetch_bars(cn_req("600000.SH"))
    assert hist.calls and batch.bars[0].feed == "eastmoney"


def test_rejects_vendor_adjustment_other_markets_and_unknown_feed():
    p = provider()
    with pytest.raises(ProviderError) as exc:
        p.fetch_bars(cn_req(adjustment="qfq"))
    assert exc.value.code == "invalid_request"
    with pytest.raises(ProviderError) as exc:
        p.fetch_bars(cn_req(adjustment="split"))
    assert exc.value.code == "invalid_request"
    with pytest.raises(ProviderError) as exc:
        p.fetch_bars(cn_req(feed="tencent"))
    assert exc.value.code == "invalid_request"
    with pytest.raises(ProviderError) as exc:
        p.fetch_bars(
            FetchRequest(
                market=Market.HK,
                interval=Interval.D1,
                symbols=("00700",),
                window=DateRange(date(2026, 3, 2), date(2026, 3, 6)),
            )
        )
    assert exc.value.code == "unsupported_request"


def test_bare_code_rejected_before_any_call():
    hist = FakeHist({"600000": EASTMONEY_ROWS})
    with pytest.raises(ProviderError) as err:
        provider({"eastmoney": hist}).fetch_bars(cn_req("600000"))
    assert err.value.code == "invalid_request" and not err.value.retryable
    assert hist.calls == []


def test_window_outside_calendar_coverage_raises():
    with pytest.raises(ProviderError) as exc:
        provider().fetch_bars(cn_req(start="2030-03-04", end="2030-03-08"))
    assert exc.value.code == "invalid_request"


def test_quota_gate_called_once_per_symbol():
    hist = FakeHist({"600000": EASTMONEY_ROWS, "000001": SECOND_CODE_ROWS})
    gate = RecordingGate()
    provider({"eastmoney": hist}, quota_gate=gate).fetch_bars(
        cn_req("600000.SH", "000001.SZ")
    )
    assert gate.acquired == ["akshare_ashare", "akshare_ashare"]
    assert [c[0] for c in hist.calls] == ["600000", "000001"]


def test_quota_exhausted_stops_with_cursor():
    hist = FakeHist({"600000": EASTMONEY_ROWS, "000001": SECOND_CODE_ROWS})

    class Once(RecordingGate):
        def acquire(self, provider, weight=1, *, scope="default", timeout=None):
            if self.acquired:
                raise QuotaExhausted("akshare_ashare", retry_at=NOW)
            return super().acquire(provider, weight, scope=scope)

    gate = Once()
    result = provider({"eastmoney": hist}, quota_gate=gate).fetch_bars_result(
        cn_req("600000.SH", "000001.SZ")
    )
    assert {b.symbol_as_returned for b in result.batch.bars} == {"sh.600000"}
    assert result.batch.next_cursor == "1"
    assert result.batch.error is not None and result.batch.error.retryable


def test_construction_does_not_import_akshare():
    sys.modules.pop("akshare", None)
    AKShareCNProvider({"eastmoney": FakeHist()})
    assert "akshare" not in sys.modules


def test_fetch_without_package_or_hist_raises(monkeypatch):
    import builtins

    real_import = builtins.__import__

    def blocked(name, *args, **kwargs):
        if name == "akshare" or name.startswith("akshare."):
            raise ImportError("blocked")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", blocked)
    with pytest.raises(ProviderError) as exc:
        AKShareCNProvider().fetch_bars(cn_req("600000.SH"))
    assert exc.value.code == "akshare_not_installed"


@pytest.mark.network
def test_optional_live_smoke_stays_in_memory():
    if os.environ.get("OPENBARS_AKSHARE_CN_LIVE") != "1":
        pytest.skip(
            "set OPENBARS_AKSHARE_CN_LIVE=1 to run the optional AKShare A-share smoke test"
        )
    pytest.importorskip("akshare")
    batch = AKShareCNProvider().fetch_bars(
        cn_req("600000.SH", start="2026-03-02", end="2026-03-06")
    )
    assert batch.provider == "akshare_ashare"
    assert batch.bars, "expected at least one in-memory bar"
    assert all(b.session_date.weekday() < 5 for b in batch.bars)


def test_pandas_dataframe_payload_is_parsed_and_nan_rows_skipped():
    import pandas as pd

    rows = [*EASTMONEY_ROWS, {**EASTMONEY_ROWS[0], "日期": date(2026, 3, 4),
                              "开盘": float("nan"), "收盘": float("nan"),
                              "最高": float("nan"), "最低": float("nan"),
                              "成交量": float("nan")}]  # fmt: skip
    frame = pd.DataFrame(rows).set_index("日期")
    result = provider({"eastmoney": FakeHist({"600000": frame})}).fetch_bars_result(
        cn_req("600000.SH", start="2026-03-02", end="2026-03-06")
    )
    assert [b.session_date.isoformat() for b in result.batch.bars] == [
        "2026-03-02",
        "2026-03-03",
    ]
    assert result.skipped.get("nan_row", 0) == 1
