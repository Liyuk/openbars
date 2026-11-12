"""#73 Futu OpenD HK daily adapter: injected fake OpenD context, never the network or the SDK."""

from __future__ import annotations

import builtins
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from openbars.core.errors import ProviderError, QuotaExhausted
from openbars.core.models import Adjustment, DateRange, Interval, Market
from openbars.core.series import PriceBasis
from openbars.providers.adapter import NullQuotaGate
from openbars.providers.base import BatchShape, FetchRequest
from openbars.providers.hk.futu import AUTYPE_NONE, FUTU_OPEND_RAW, KTYPE_DAY, FutuOpenDProvider, to_futu_hk_code

pytestmark = pytest.mark.usefixtures("no_network")

NOW = datetime(2026, 3, 10, tzinfo=UTC)


def row(day: str, close="10.40", **kw):
    return {"code": "HK.00700", "time_key": f"{day} 00:00:00", "open": 10.1, "high": 10.5, "low": 10.0,
            "close": close, "volume": 1000, "turnover": 10400.0, **kw}  # fmt: skip


def req(*symbols, start="2026-03-02", end="2026-03-06", **kw):
    return FetchRequest(Market.HK, Interval.D1, symbols or ("00700",),
                        DateRange(date.fromisoformat(start), date.fromisoformat(end)), **kw)  # fmt: skip


class FakeContext:
    """Pages: code -> list of (ret, data, next_key) responses, consumed in order."""

    def __init__(self, pages):
        self.pages = {k: list(v) for k, v in pages.items()}
        self.calls = []
        self.closed = 0

    def request_history_kline(self, code, **kw):
        self.calls.append((code, kw))
        return self.pages[code].pop(0)

    def close(self):
        self.closed += 1


class Gate(NullQuotaGate):
    def __init__(self, fail_after=None):
        self.n = 0
        self.fail_after = fail_after

    def acquire(self, provider, weight=1, *, scope="default", timeout=None):
        if self.fail_after is not None and self.n >= self.fail_after:
            raise QuotaExhausted(provider, retry_at=NOW)
        self.n += 1
        return super().acquire(provider, weight, scope=scope)


def make(ctx, **kw):
    return FutuOpenDProvider(lambda: ctx, clock=lambda: NOW, **kw)


def test_capabilities_terms_and_quota():
    p = make(FakeContext({}))
    (cap,) = p.capabilities()
    assert (cap.market, cap.interval, cap.batch, cap.max_symbols_per_request) == (Market.HK, Interval.D1, BatchShape.PER_SYMBOL, 1)
    assert cap.adjusted_modes == frozenset({Adjustment.RAW})
    assert cap.terms.personal_only and not cap.terms.redistribution_allowed
    (limit,) = p.quota_policy().limits
    assert limit.count <= 60 and limit.per.total_seconds() == 30
    assert to_futu_hk_code("00700") == "HK.00700"
    assert FUTU_OPEND_RAW.price_basis is PriceBasis.RAW and FUTU_OPEND_RAW.currency == "HKD"


def test_fetch_pages_unadjusted_and_parses_exact_decimals():
    ctx = FakeContext({"HK.00700": [(0, [row("2026-03-02"), row("2026-03-03")], "k1"), (0, [row("2026-03-04", close=10.45)], None)]})
    batch = make(ctx).fetch_bars(req("0700.HK"))
    assert [b.session_date for b in batch.bars] == [date(2026, 3, 2), date(2026, 3, 3), date(2026, 3, 4)]
    first = batch.bars[0]
    assert (first.open, first.high, first.low, first.close, first.volume) == (
        Decimal("10.1"), Decimal("10.5"), Decimal("10"), Decimal("10.4"), Decimal("1000"))
    assert batch.bars[2].close == Decimal("10.45")
    assert first.provider == "futu" and first.feed == "opend" and first.symbol_as_returned == "0700.HK"
    assert first.series == FUTU_OPEND_RAW and first.ts_start == datetime(2026, 3, 2, tzinfo=UTC)
    code, kw = ctx.calls[0]
    assert code == "HK.00700" and kw["ktype"] == KTYPE_DAY and kw["autype"] == AUTYPE_NONE
    assert (kw["start"], kw["end"], kw["page_req_key"]) == ("2026-03-02", "2026-03-06", None)
    assert ctx.calls[1][1]["page_req_key"] == "k1"
    assert ctx.closed == 1


def test_non_sessions_out_of_window_and_nan_rows_are_skipped():
    rows = [row("2026-03-01"), row("2026-03-07"), row("2026-03-09"), row("2026-03-05", open=None, high=None,
            low=None, close=None, volume=None), row("2026-03-06")]  # fmt: skip
    result = make(FakeContext({"HK.00700": [(0, rows, None)]})).fetch_bars_result(req())
    assert [b.session_date for b in result.batch.bars] == [date(2026, 3, 6)]
    assert result.skipped == {"out_of_window": 3, "nan_row": 1}


@pytest.mark.parametrize(
    "message, code, retryable",
    [
        ("获取历史K线频率太高", "rate_limited", True),
        ("历史K线额度不足", "history_quota_exhausted", False),
        ("无权限获取港股行情", "no_entitlement", False),
        ("连接断开", "provider_unavailable", True),
        ("unknown stock", "provider_error", False),
    ],
)
def test_opend_errors_are_classified(message, code, retryable):
    ctx = FakeContext({"HK.00700": [(-1, message, None)]})
    with pytest.raises(ProviderError) as info:
        make(ctx).fetch_bars(req())
    assert info.value.code == code and info.value.retryable is retryable
    assert message not in str(info.value) or code == "provider_error"
    assert ctx.closed == 1


def test_partial_failure_keeps_good_symbols_and_quota_stop_sets_cursor():
    ctx = FakeContext({"HK.00700": [(0, [row("2026-03-02")], None)], "HK.09988": [(-1, "unknown stock", None)],
                       "HK.00005": [(0, [row("2026-03-02")], None)]})  # fmt: skip
    batch = make(ctx).fetch_bars(req("00700", "9988", "5"))
    assert [b.symbol_as_returned for b in batch.bars] == ["00700", "5"]
    assert batch.error is not None and "9988=provider_error" in str(batch.error) and batch.next_cursor is None
    ctx = FakeContext({"HK.00700": [(0, [row("2026-03-02")], None)], "HK.09988": [(0, [row("2026-03-02")], None)]})
    batch = make(ctx, quota_gate=Gate(fail_after=1)).fetch_bars(req("00700", "09988"))
    assert [b.symbol_as_returned for b in batch.bars] == ["00700"] and batch.next_cursor == "1"
    assert batch.error.code == "quota_exhausted" and batch.error.retryable
    resumed = make(FakeContext({"HK.09988": [(0, [row("2026-03-02")], None)]})).fetch_bars(req("00700", "09988", cursor="1"))
    assert [b.symbol_as_returned for b in resumed.bars] == ["09988"]


def test_rejects_unsupported_requests():
    p = make(FakeContext({}))
    for bad in (
        FetchRequest(Market.US, Interval.D1, ("AAPL",), DateRange(date(2026, 3, 2), date(2026, 3, 6))),
        req(adjustment=Adjustment.SPLIT),
        req(feed="lv2"),
        req("not-a-code"),
        req(start="1990-01-02", end="1990-01-05"),
    ):
        with pytest.raises(ProviderError) as info:
            p.fetch_bars(bad)
        assert not info.value.retryable


def test_missing_sdk_is_a_clear_non_retryable_error(monkeypatch):
    real_import = builtins.__import__

    def fake_import(name, *a, **kw):
        if name == "futu" or name.startswith("futu."):
            raise ImportError(name)
        return real_import(name, *a, **kw)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    with pytest.raises(ProviderError) as info:
        FutuOpenDProvider(host="127.0.0.1", port="11111").fetch_bars(req("00700", "00005"))
    assert info.value.code == "futu_not_installed" and not info.value.retryable


def test_port_must_be_an_integer():
    with pytest.raises(ProviderError):
        FutuOpenDProvider(port="eleven")


def test_registered_as_entry_point_but_not_routed_by_planner():
    from openbars.cli.wiring import _PROVIDER_MARKETS
    from openbars.providers.registry import discover

    assert discover()["futu"] is FutuOpenDProvider
    assert "futu" not in _PROVIDER_MARKETS
