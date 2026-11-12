"""#74 LongPort HK daily adapter: injected fake history, never the network, SDK or real keys."""

from __future__ import annotations

import builtins
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from openbars.core.errors import ProviderError, QuotaExhausted
from openbars.core.models import Adjustment, DateRange, Interval, Market
from openbars.providers.adapter import NullQuotaGate
from openbars.providers.base import FetchRequest
from openbars.providers.hk.longport import (
    CHUNK_DAYS,
    LONGPORT_OPENAPI_RAW,
    LongPortProvider,
    to_longport_hk_symbol,
)

pytestmark = pytest.mark.usefixtures("no_network")

NOW = datetime(2026, 3, 10, tzinfo=UTC)
FAKE_SECRET = "lp-" + "s3cr3t-value-0123456789"


def candle(day, close=Decimal("10.40"), tz=None):
    stamp = datetime.combine(day, datetime.min.time())
    if tz == "utc":  # midnight HKT expressed in UTC: the previous calendar day, 16:00
        stamp = (stamp - timedelta(hours=8)).replace(tzinfo=UTC)
    elif tz == "hkt":
        stamp = stamp.replace(tzinfo=timezone(timedelta(hours=8)))
    return SimpleNamespace(timestamp=stamp, open=Decimal("10.10"), high=Decimal("10.50"), low=Decimal("10.00"),
                           close=close, volume=1000, turnover=Decimal("10400"))  # fmt: skip


def req(*symbols, start="2026-03-02", end="2026-03-06", **kw):
    return FetchRequest(Market.HK, Interval.D1, symbols or ("00700",),
                        DateRange(date.fromisoformat(start), date.fromisoformat(end)), **kw)  # fmt: skip


class FakeHistory:
    def __init__(self, data=None, errors=None):
        self.data, self.errors, self.calls = data or {}, errors or {}, []

    def __call__(self, symbol, start, end):
        self.calls.append((symbol, start, end))
        if symbol in self.errors:
            raise self.errors[symbol]
        return [c for c in self.data.get(symbol, [])]


def make(history, **kw):
    return LongPortProvider(history, clock=lambda: NOW, **kw)


def test_symbol_and_capabilities():
    assert to_longport_hk_symbol("00700") == "700.HK" and to_longport_hk_symbol("09988") == "9988.HK"
    p = make(FakeHistory())
    (cap,) = p.capabilities()
    assert cap.market is Market.HK and cap.adjusted_modes == frozenset({Adjustment.RAW})
    assert cap.terms.personal_only and not cap.terms.redistribution_allowed
    (limit,) = p.quota_policy().limits
    assert limit.count <= 10 and limit.per == timedelta(seconds=1)


@pytest.mark.parametrize("tz", [None, "utc", "hkt"])
def test_fetch_parses_exact_decimals_and_hk_session_dates(tz):
    days = [date(2026, 3, 2), date(2026, 3, 3)]
    hist = FakeHistory({"700.HK": [candle(d, tz=tz) for d in days]})
    batch = make(hist).fetch_bars(req("0700.HK"))
    assert [b.session_date for b in batch.bars] == days
    b = batch.bars[0]
    assert (b.open, b.high, b.low, b.close, b.volume) == (Decimal("10.1"), Decimal("10.5"), Decimal("10"), Decimal("10.4"), Decimal("1000"))
    assert b.provider == "longport" and b.feed == "openapi" and b.series == LONGPORT_OPENAPI_RAW
    assert hist.calls == [("700.HK", date(2026, 3, 2), date(2026, 3, 6))]


def test_mapping_candles_and_skips():
    data = [
        {"timestamp": datetime(2026, 3, 1), "open": 1, "high": 2, "low": 1, "close": 2, "volume": 5},  # Sunday
        {"timestamp": datetime(2026, 3, 9), "open": 1, "high": 2, "low": 1, "close": 2, "volume": 5},  # outside
        {"timestamp": datetime(2026, 3, 4), "open": 1, "high": 2, "low": 1, "close": 2, "volume": 5},
    ]
    result = make(FakeHistory({"700.HK": data})).fetch_bars_result(req())
    assert [b.session_date for b in result.batch.bars] == [date(2026, 3, 4)]
    assert result.skipped == {"out_of_window": 2}


def test_long_windows_are_chunked():
    hist = FakeHistory({"700.HK": [candle(date(2020, 1, 2))]})
    make(hist).fetch_bars(req(start="2018-01-02", end="2026-03-06"))
    spans = [(s, e) for _, s, e in hist.calls]
    assert spans[0][0] == date(2018, 1, 2) and spans[-1][1] == date(2026, 3, 6)
    assert all((e - s).days < CHUNK_DAYS for s, e in spans)
    assert all(spans[i][1] + timedelta(days=1) == spans[i + 1][0] for i in range(len(spans) - 1))


@pytest.mark.parametrize(
    "exc, code, retryable",
    [
        (RuntimeError(f"OpenApiException: 301606 request rate limit {FAKE_SECRET}"), "rate_limited", True),
        (RuntimeError(f"no quote access for HK {FAKE_SECRET}"), "no_entitlement", False),
        (RuntimeError(f"401 unauthorized: invalid token {FAKE_SECRET}"), "auth_failed", False),
        (TimeoutError("read"), "provider_unavailable", True),
        (RuntimeError("symbol not found"), "provider_error", False),
    ],
)
def test_errors_are_classified_without_echoing_the_message(exc, code, retryable):
    with pytest.raises(ProviderError) as info:
        make(FakeHistory(errors={"700.HK": exc})).fetch_bars(req())
    assert info.value.code == code and info.value.retryable is retryable
    assert FAKE_SECRET not in str(info.value)


def test_quota_stop_sets_a_resumable_cursor():
    class Gate(NullQuotaGate):
        n = 0

        def acquire(self, provider, weight=1, *, scope="default", timeout=None):
            if self.n:
                raise QuotaExhausted(provider, retry_at=NOW)
            self.n += 1

    hist = FakeHistory({"700.HK": [candle(date(2026, 3, 2))], "5.HK": [candle(date(2026, 3, 2))]})
    batch = make(hist, quota_gate=Gate()).fetch_bars(req("00700", "00005"))
    assert [b.symbol_as_returned for b in batch.bars] == ["00700"]
    assert batch.next_cursor == "1" and batch.error.code == "quota_exhausted"


def test_missing_credentials_or_sdk_are_non_retryable(monkeypatch):
    with pytest.raises(ProviderError) as info:
        LongPortProvider(app_key="k", app_secret="", access_token="t").fetch_bars(req())
    assert info.value.code == "missing_credentials" and not info.value.retryable
    real_import = builtins.__import__

    def fake_import(name, *a, **kw):
        if name == "longport" or name.startswith("longport."):
            raise ImportError(name)
        return real_import(name, *a, **kw)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    p = LongPortProvider(app_key=FAKE_SECRET, app_secret=FAKE_SECRET, access_token=FAKE_SECRET)
    with pytest.raises(ProviderError) as info:
        p.fetch_bars(req("00700", "00005"))
    assert info.value.code == "longport_not_installed"
    assert FAKE_SECRET not in repr(p) and FAKE_SECRET not in str(info.value)


def test_registered_but_not_routed_by_planner():
    from openbars.cli.wiring import _PROVIDER_MARKETS
    from openbars.providers.registry import discover

    assert discover()["longport"] is LongPortProvider and "longport" not in _PROVIDER_MARKETS
