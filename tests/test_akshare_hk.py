"""AKShare HK adapter: injected fake hist, no network by default."""

from __future__ import annotations

import os
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from openbars.core.errors import ProviderError, QuotaExhausted
from openbars.core.models import Adjustment, DateRange, Interval, Market
from openbars.core.series import PriceBasis
from openbars.providers.adapter import NullQuotaGate
from openbars.providers.base import BatchShape, FetchRequest
from openbars.providers.hk.akshare import (
    AKSHARE_EASTMONEY_RAW,
    AKShareHKProvider,
    to_akshare_hk_symbol,
)

NOW = datetime(2026, 3, 10, tzinfo=UTC)
_OHLCV = {
    "开盘": "10.10",
    "最高": "10.50",
    "最低": "10.00",
    "收盘": "10.40",
    "成交量": 1000,
}


def _row(day: str, **extra) -> dict:
    return {"日期": day, **_OHLCV, **extra}


def hk_req(*symbols: str, start="2026-03-02", end="2026-03-06", **kw) -> FetchRequest:
    return FetchRequest(
        market=Market.HK,
        interval=Interval.D1,
        symbols=symbols or ("00700",),
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


def provider(hist=None, **kw) -> AKShareHKProvider:
    return AKShareHKProvider(hist or FakeHist(), clock=lambda: NOW, **kw)


def test_symbol_mapping_uses_five_digits():
    assert to_akshare_hk_symbol("0700.HK") == "00700"
    assert to_akshare_hk_symbol("hk:eq:xhkg:00700") == "00700"
    assert to_akshare_hk_symbol("700") == "00700"


def test_capabilities_and_quota_are_conservative():
    p = provider()
    (cap,) = p.capabilities()
    assert (
        cap.provider == "akshare"
        and cap.market is Market.HK
        and cap.feed == "eastmoney"
    )
    assert cap.interval is Interval.D1
    assert cap.batch is BatchShape.PER_SYMBOL and cap.max_symbols_per_request == 1
    assert cap.adjusted_modes == frozenset({Adjustment.RAW})
    assert cap.terms.personal_only and not cap.terms.redistribution_allowed
    assert (
        "Eastmoney" in (cap.terms.notes or "")
        or "eastmoney" in (cap.terms.notes or "").lower()
    )
    assert cap.terms.url and "disclaimer" in cap.terms.url
    policy = p.quota_policy()
    assert policy.max_batch_symbols == 1
    assert (policy.limits[0].count, policy.limits[0].per) == (4, timedelta(minutes=1))
    assert policy.limits[1].count == 80


def test_fetch_maps_spellings_requests_unadjusted_and_keeps_symbol():
    hist = FakeHist({"00700": [_row("2026-03-02"), _row("2026-03-03")]})
    batch = provider(hist).fetch_bars(hk_req("hk:eq:xhkg:00700"))
    assert hist.calls[0][0] == "00700"
    assert hist.calls[0][1] == {
        "period": "daily",
        "start_date": "20260302",
        "end_date": "20260306",
        "adjust": "",
    }
    assert {b.symbol_as_returned for b in batch.bars} == {"hk:eq:xhkg:00700"}
    assert [b.session_date.isoformat() for b in batch.bars] == [
        "2026-03-02",
        "2026-03-03",
    ]
    assert batch.bars[0].close == Decimal("10.40")
    assert batch.bars[0].series is AKSHARE_EASTMONEY_RAW
    assert batch.bars[0].series.price_basis is PriceBasis.RAW
    assert batch.bars[0].feed == "eastmoney" and batch.provider == "akshare"


def test_english_columns_and_dedupe_by_code():
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
    hist = FakeHist({"00700": rows})
    provider(hist).fetch_bars(hk_req("0700.HK", "00700", "700"))
    assert [c[0] for c in hist.calls] == ["00700"]


def test_skips_non_session_days():
    rows = [
        _row("2025-12-23"),
        _row("2025-12-24"),
        _row("2025-12-25"),
        _row("2025-12-26"),
    ]
    result = provider(FakeHist({"00700": rows})).fetch_bars_result(
        hk_req("00700", start="2025-12-23", end="2025-12-26")
    )
    assert [b.session_date.isoformat() for b in result.batch.bars] == [
        "2025-12-23",
        "2025-12-24",
    ]
    assert result.skipped.get("non_session", 0) == 2


def test_rejects_vendor_adjustment_and_other_markets():
    p = provider(FakeHist({"00700": [_row("2026-03-02")]}))
    with pytest.raises(ProviderError) as exc:
        p.fetch_bars(hk_req(adjustment="qfq"))
    assert exc.value.code == "invalid_request"
    with pytest.raises(ProviderError) as exc:
        p.fetch_bars(hk_req(adjustment="split"))
    assert exc.value.code == "invalid_request"
    with pytest.raises(ProviderError) as exc:
        p.fetch_bars(
            FetchRequest(
                market=Market.CN,
                interval=Interval.D1,
                symbols=("00700",),
                window=DateRange(date(2026, 3, 2), date(2026, 3, 6)),
            )
        )
    assert exc.value.code == "unsupported_request"


def test_window_outside_coverage_raises():
    with pytest.raises(ProviderError) as exc:
        provider(FakeHist()).fetch_bars(
            hk_req("00700", start="1999-01-04", end="1999-01-08")
        )
    assert exc.value.code == "invalid_request"


def test_quota_gate_called_once_per_symbol():
    hist = FakeHist({"00700": [_row("2026-03-02")], "09988": [_row("2026-03-02")]})
    gate = RecordingGate()
    provider(hist, quota_gate=gate).fetch_bars(hk_req("00700", "09988"))
    assert gate.acquired == ["akshare", "akshare"]
    assert [c[0] for c in hist.calls] == ["00700", "09988"]


def test_quota_exhausted_stops_with_cursor():
    hist = FakeHist({"00700": [_row("2026-03-02")], "09988": [_row("2026-03-02")]})

    class Once(RecordingGate):
        def acquire(self, provider, weight=1, *, scope="default", timeout=None):
            if self.acquired:
                raise QuotaExhausted("akshare", retry_at=NOW)
            return super().acquire(provider, weight, scope=scope)

    gate = Once()
    result = provider(hist, quota_gate=gate).fetch_bars_result(hk_req("00700", "09988"))
    assert [b.symbol_as_returned for b in result.batch.bars] == ["00700"]
    assert result.batch.next_cursor == "1"
    assert result.batch.error is not None and result.batch.error.retryable


def test_construction_does_not_import_akshare():
    import sys

    sys.modules.pop("akshare", None)
    AKShareHKProvider(FakeHist())
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
        AKShareHKProvider().fetch_bars(hk_req("00700"))
    assert exc.value.code == "akshare_not_installed"


@pytest.mark.network
def test_optional_live_smoke_stays_in_memory():
    if os.environ.get("OPENBARS_AKSHARE_LIVE") != "1":
        pytest.skip(
            "set OPENBARS_AKSHARE_LIVE=1 to run the optional AKShare smoke test"
        )
    pytest.importorskip("akshare")
    batch = AKShareHKProvider().fetch_bars(
        hk_req("00700", start="2026-03-02", end="2026-03-06")
    )
    assert batch.provider == "akshare"
    assert batch.bars, "expected at least one in-memory bar"
    assert all(b.session_date.weekday() < 5 for b in batch.bars)
