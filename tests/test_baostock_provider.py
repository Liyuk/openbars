"""BaoStock adapter tests: injected fake client and synthetic fixtures (no network, no package)."""

from __future__ import annotations

import builtins
import json
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from openbars.core.errors import ProviderError, QuotaExhausted
from openbars.core.models import Adjustment, AssetClass, DateRange, Interval, Market
from openbars.core.series import (
    BAOSTOCK_ASHARE_RAW,
    KNOWN_SOURCES,
    AdjustmentScope,
    PriceBasis,
    SessionSemantics,
    VolumeBasis,
)
from openbars.providers.adapter import NullQuotaGate
from openbars.providers.base import BatchShape, FetchRequest
from openbars.providers.cn.baostock import (
    ADJUSTFLAG_RAW,
    DefaultBaoStockClient,
    BaoStockProvider,
    to_baostock_code,
)

FIXTURES = Path(__file__).parent / "fixtures" / "baostock"
SHANGHAI = ZoneInfo("Asia/Shanghai")


def load(name: str) -> list[dict[str, str]]:
    return json.loads((FIXTURES / name).read_text())


class FakeClient:
    def __init__(self, klines=None, factors=None, stocks=None, fail_login=None, fail_query=None):
        self.events: list[str] = []
        self.kline_calls: list[dict] = []
        self.factor_calls: list[tuple] = []
        self.klines = klines if klines is not None else load("klines_sh_test.json")
        self.factors = factors if factors is not None else load("adjust_factors_sh_test.json")
        self.stocks = stocks
        self.fail_login = fail_login
        self.fail_query = fail_query

    def login(self):
        self.events.append("login")
        if self.fail_login:
            raise self.fail_login

    def logout(self):
        self.events.append("logout")

    def query_history_k_data_plus(self, code, fields, start_date, end_date, frequency="d", adjustflag="3"):
        self.events.append("kline")
        self.kline_calls.append(
            dict(code=code, fields=fields, start=start_date, end=end_date, frequency=frequency, adjustflag=adjustflag)
        )
        if self.fail_query:
            raise self.fail_query
        return self.klines

    def query_adjust_factor(self, code, start_date, end_date):
        self.events.append("factor")
        self.factor_calls.append((code, start_date, end_date))
        return self.factors

    def query_all_stock(self, day):
        self.events.append("stocks")
        self.stock_day = day
        return self.stocks


class RecordingGate:
    def __init__(self, exhausted: bool = False) -> None:
        self.calls: list[tuple[str, int]] = []
        self.exhausted = exhausted

    def acquire(self, provider, weight=1, *, scope="default", timeout=None):
        self.calls.append((provider, weight))
        if self.exhausted:
            raise QuotaExhausted(provider)
        return NullQuotaGate().acquire(provider, weight, scope=scope)

    def penalize(self, provider, until):  # pragma: no cover
        pass

    def snapshot(self):  # pragma: no cover
        return {}


def request_for(symbol="sh.600000", start="2024-01-02", end="2024-01-04", **kw) -> FetchRequest:
    return FetchRequest(
        market=kw.pop("market", Market.CN),
        interval=kw.pop("interval", Interval.D1),
        symbols=(symbol,),
        window=DateRange(date.fromisoformat(start), date.fromisoformat(end)),
        **kw,
    )


def make(client=None, **kw) -> BaoStockProvider:
    return BaoStockProvider(client=client or FakeClient(), fetch_id_factory=lambda: "fetch-1", **kw)


# -- identity / capabilities -----------------------------------------------------------------
def test_source_identity_and_series_semantics():
    assert BAOSTOCK_ASHARE_RAW not in KNOWN_SOURCES
    batch = make().fetch_bars(request_for())
    series = batch.bars[0].series
    assert series.source == BAOSTOCK_ASHARE_RAW
    assert (series.provider, series.feed, series.legacy_adjustment) == (
        BAOSTOCK_ASHARE_RAW.provider,
        BAOSTOCK_ASHARE_RAW.feed,
        BAOSTOCK_ASHARE_RAW.adjustment,
    )
    assert series.price_basis is PriceBasis.RAW
    assert series.adjustment_scope is AdjustmentScope.NONE
    assert series.session_semantics is SessionSemantics.REGULAR
    assert series.volume_basis is VolumeBasis.AS_TRADED
    assert series.currency == "CNY"


def test_capabilities_and_policy():
    provider = make()
    (cap,) = provider.capabilities()
    assert provider.id == "baostock" and provider.secret_values() == ()
    assert (cap.market, cap.interval, cap.batch) == (Market.CN, Interval.D1, BatchShape.PER_SYMBOL)
    assert cap.adjusted_modes == frozenset({Adjustment.RAW})
    assert cap.asset_classes == frozenset({AssetClass.EQUITY})
    assert cap.max_symbols_per_request == 1
    assert cap.terms.personal_only and not cap.terms.redistribution_allowed
    policy = provider.quota_policy()
    assert policy.provider == "baostock" and policy.safety_margin == 0.9
    assert policy.key_scope == "per_process"
    assert policy.official_docs_url.startswith("https://www.baostock.com")
    assert all(policy.effective_limit(limit) <= 2 for limit in policy.limits)
    assert policy.min_interval() >= timedelta(seconds=0.5)


# -- symbols ---------------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("sh.600000", "sh.600000"),
        ("600000.SH", "sh.600000"),
        ("SH600000", "sh.600000"),
        ("sz.000001", "sz.000001"),
        ("000001.sz", "sz.000001"),
    ],
)
def test_symbol_helper(raw, expected):
    assert to_baostock_code(raw) == expected


@pytest.mark.parametrize("raw", ["600000", "000001", "xx.600000", "", "sh.60000"])
def test_symbol_helper_rejects(raw):
    with pytest.raises(ValueError):
        to_baostock_code(raw)


def test_bare_code_rejected_before_any_call():
    client = FakeClient()
    with pytest.raises(ProviderError) as err:
        make(client).fetch_bars(request_for("600000"))
    assert err.value.code == "invalid_request" and not err.value.retryable
    assert client.events == []


def test_alternate_spelling_talks_native_form():
    client = FakeClient()
    make(client).fetch_bars(request_for("600000.SH"))
    assert client.kline_calls[0]["code"] == "sh.600000"


# -- bars ------------------------------------------------------------------------------------
def test_bars_unadjusted_decimal_and_session_login_logout():
    client = FakeClient()
    batch = make(client).fetch_bars(request_for())
    assert client.events == ["login", "kline", "logout"]
    call = client.kline_calls[0]
    assert call["adjustflag"] == ADJUSTFLAG_RAW == "3"
    assert call["frequency"] == "d"
    assert (call["start"], call["end"]) == ("2024-01-02", "2024-01-04")
    for name in ("date", "code", "open", "high", "low", "close", "volume", "tradestatus", "isST", "preclose"):
        assert name in call["fields"].split(",")
    assert batch.provider == "baostock" and batch.fetch_id == "fetch-1" and batch.error is None
    assert [b.session_date for b in batch.bars] == [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)]
    first = batch.bars[0]
    assert (first.provider, first.symbol_as_returned, first.feed) == ("baostock", "sh.600000", "ashare")
    assert first.interval is Interval.D1
    assert first.ts_start == datetime(2024, 1, 2, tzinfo=SHANGHAI)
    assert first.ts_start.utcoffset() == timedelta(hours=8)
    for value in (first.open, first.high, first.low, first.close, first.volume):
        assert isinstance(value, Decimal)
    assert (first.open, first.close, first.volume) == (Decimal("10.0000"), Decimal("10.2000"), Decimal("1000000"))
    assert first.extra["preclose"] == "10.0000" and first.extra["amount"] == "10150000.0000"
    assert first.extra["tradestatus"] == 1 and first.extra["isST"] == 0
    assert first.extra["pctChg"] == "2.000000" and first.extra["adjustflag"] == "3"


def test_suspended_row_is_kept_in_raw():
    batch = make().fetch_bars(request_for())
    suspended = batch.bars[1]
    assert suspended.extra["tradestatus"] == 0 and suspended.extra["isST"] == 1
    assert suspended.volume == 0 and suspended.extra["amount"] == "0.0000"
    assert suspended.open == suspended.close == Decimal("10.2000")
    assert suspended.extra["preclose"] == "10.2000"
    assert batch.bars[2].extra["isST"] == 1  # ST state is preserved per row


def test_suspended_blank_volume_becomes_zero_but_trading_blank_volume_is_rejected():
    rows = load("klines_sh_test.json")
    rows[1]["volume"] = ""
    assert make(FakeClient(klines=rows)).fetch_bars(request_for()).bars[1].volume == 0
    rows[0]["volume"] = ""
    with pytest.raises(ProviderError) as err:
        make(FakeClient(klines=rows)).fetch_bars(request_for())
    assert err.value.code == "invalid_payload"


def test_rows_sorted_ascending_and_window_filtered():
    rows = list(reversed(load("klines_sh_test.json")))
    batch = make(FakeClient(klines=rows)).fetch_bars(request_for(start="2024-01-03", end="2024-01-04"))
    assert [b.session_date for b in batch.bars] == [date(2024, 1, 3), date(2024, 1, 4)]


def test_empty_result_is_an_empty_batch():
    assert make(FakeClient(klines=[])).fetch_bars(request_for()).bars == ()


@pytest.mark.parametrize(
    "mutate",
    [
        lambda rows: rows[0].update(close="abc"),
        lambda rows: rows[0].update(open="NaN"),
        lambda rows: rows[0].update(date="2024/01/02"),
        lambda rows: rows[0].update(code="sz.000001"),
        lambda rows: rows[0].update(adjustflag="2"),
        lambda rows: rows[0].update(tradestatus="x"),
        lambda rows: rows[0].update(high="1.0000", low="9.0000"),
        lambda rows: rows[1].update(date="2024-01-02"),  # duplicate
        lambda rows: rows[0].pop("close"),
    ],
)
def test_invalid_payload_rejected(mutate):
    rows = load("klines_sh_test.json")
    mutate(rows)
    with pytest.raises(ProviderError) as err:
        make(FakeClient(klines=rows)).fetch_bars(request_for())
    assert err.value.code == "invalid_payload" and not err.value.retryable


def test_non_list_payload_rejected():
    with pytest.raises(ProviderError) as err:
        make(FakeClient(klines={"oops": 1})).fetch_bars(request_for())
    assert err.value.code == "invalid_payload"


@pytest.mark.parametrize(
    "kw",
    [
        dict(market=Market.US),
        dict(interval=Interval.H1),
        dict(feed="other"),
        dict(adjustment="split"),
        dict(cursor="1"),
    ],
)
def test_unsupported_requests(kw):
    client = FakeClient()
    with pytest.raises(ProviderError) as err:
        make(client).fetch_bars(request_for(**kw))
    assert err.value.code == "invalid_request"
    assert client.events == []


def test_one_symbol_per_request():
    request = FetchRequest(Market.CN, Interval.D1, ("sh.600000", "sz.000001"), DateRange(date(2024, 1, 2), date(2024, 1, 3)))
    with pytest.raises(ProviderError) as err:
        make().fetch_bars(request)
    assert err.value.code == "invalid_request"


# -- adjust factors --------------------------------------------------------------------------
def test_adjust_factors_parsed_sorted_decimal():
    client = FakeClient()
    factors = make(client).fetch_adjust_factors(request_for(start="1990-01-01", end="2024-01-04"))
    assert client.events == ["login", "factor", "logout"]
    assert client.factor_calls == [("sh.600000", "1990-01-01", "2024-01-04")]
    assert [f.divid_operate_date for f in factors] == [date(2022, 6, 15), date(2023, 6, 20)]
    first = factors[0]
    assert first.code == "sh.600000" and first.source == "baostock" and first.method == "pct_return"
    assert (first.fore_adjust_factor, first.back_adjust_factor, first.adjust_factor) == (
        Decimal("0.900000"),
        Decimal("1.200000"),
        Decimal("1.200000"),
    )
    assert all(isinstance(f.adjust_factor, Decimal) for f in factors)


def test_adjust_factors_default_to_request_window():
    client = FakeClient(factors=[])
    assert make(client).fetch_adjust_factors(request_for(start="2024-01-02", end="2024-01-04")) == []
    assert client.factor_calls == [("sh.600000", "2024-01-02", "2024-01-04")]


@pytest.mark.parametrize(
    "mutate",
    [
        lambda rows: rows[0].update(adjustFactor="x"),
        lambda rows: rows[0].update(backAdjustFactor="0"),
        lambda rows: rows[0].update(dividOperateDate="bad"),
        lambda rows: rows[0].update(code="sz.000001"),
        lambda rows: rows[0].pop("foreAdjustFactor"),
        lambda rows: rows[1].update(dividOperateDate=rows[0]["dividOperateDate"]),
    ],
)
def test_invalid_factor_payload_rejected(mutate):
    rows = load("adjust_factors_sh_test.json")
    mutate(rows)
    with pytest.raises(ProviderError) as err:
        make(FakeClient(factors=rows)).fetch_adjust_factors(request_for())
    assert err.value.code == "invalid_payload"


def test_corporate_actions_are_empty():
    assert list(make().fetch_corporate_actions(request_for())) == []


# -- errors / quota --------------------------------------------------------------------------
def test_quota_gate_acquired_once_per_call():
    gate = RecordingGate()
    provider = make(quota_gate=gate)
    provider.fetch_bars(request_for())
    provider.fetch_adjust_factors(request_for())
    assert gate.calls == [("baostock", 1), ("baostock", 1)]


def test_quota_exhausted_stops_before_login():
    client = FakeClient()
    with pytest.raises(QuotaExhausted):
        make(client, quota_gate=RecordingGate(exhausted=True)).fetch_bars(request_for())
    assert client.events == []


def test_before_request_hook_runs():
    seen = []
    make(before_request=lambda: seen.append(1)).fetch_bars(request_for())
    assert seen == [1]


def test_login_failure_maps_to_provider_unavailable_without_retry():
    client = FakeClient(fail_login=OSError("boom secret-host"))
    with pytest.raises(ProviderError) as err:
        make(client).fetch_bars(request_for())
    assert err.value.code == "provider_unavailable"
    assert "secret-host" not in str(err.value)
    assert client.events == ["login"]  # one attempt, nothing else


def test_query_failure_logs_out_once_and_maps_error():
    client = FakeClient(fail_query=TimeoutError("slow"))
    with pytest.raises(ProviderError) as err:
        make(client).fetch_bars(request_for())
    assert err.value.code == "provider_unavailable" and err.value.retryable
    assert client.events == ["login", "kline", "logout"]


def test_client_provider_error_is_passed_through():
    rejected = ProviderError("provider_rejected_request", "bad param", retryable=False)
    client = FakeClient(fail_query=rejected)
    with pytest.raises(ProviderError) as err:
        make(client).fetch_bars(request_for())
    assert err.value is rejected


def test_logout_failure_does_not_mask_result():
    client = FakeClient()

    def bad_logout():
        raise OSError("closed")

    client.logout = bad_logout
    assert len(make(client).fetch_bars(request_for()).bars) == 3


# -- default client / optional package -------------------------------------------------------
def test_missing_package_raises_not_installed(monkeypatch):
    monkeypatch.setitem(sys.modules, "baostock", None)  # import -> ImportError
    provider = BaoStockProvider()  # default client
    with pytest.raises(ProviderError) as err:
        provider.fetch_bars(request_for())
    assert err.value.code == "baostock_not_installed" and not err.value.retryable
    assert "openbars[baostock]" in str(err.value)


def test_module_import_does_not_need_package(monkeypatch):
    real_import = builtins.__import__

    def guarded(name, *args, **kwargs):
        if name == "baostock":
            raise ImportError("blocked")
        return real_import(name, *args, **kwargs)

    monkeypatch.delitem(sys.modules, "openbars.providers.cn.baostock", raising=False)
    monkeypatch.setattr(builtins, "__import__", guarded)
    import importlib

    module = importlib.import_module("openbars.providers.cn.baostock")
    assert module.BaoStockProvider.id == "baostock"


def test_default_client_login_error_codes(monkeypatch):
    class Result:
        def __init__(self, code, msg=""):
            self.error_code, self.error_msg = code, msg

    class FakeBs:
        def __init__(self, result):
            self.result = result

        def login(self):
            return self.result

    for code, expected, retryable in (
        ("10002002", "provider_unavailable", True),
        ("10004006", "provider_rejected_request", False),
        ("10001002", "provider_unavailable", False),
    ):
        monkeypatch.setitem(sys.modules, "baostock", FakeBs(Result(code, "msg")))
        with pytest.raises(ProviderError) as err:
            DefaultBaoStockClient().login()
        assert (err.value.code, err.value.retryable) == (expected, retryable)


# -- listing ---------------------------------------------------------------------------------
def test_list_instruments_equities_only():
    stocks = [
        {"code": "sh.600000", "tradeStatus": "1", "code_name": "示例银行"},
        {"code": "sz.000001", "tradeStatus": "0", "code_name": "示例二号"},
        {"code": "sh.000001", "tradeStatus": "1", "code_name": "上证综合指数"},  # index
        {"code": "sz.399001", "tradeStatus": "1", "code_name": "深证成指"},  # index
        {"code": "sz.300001", "tradeStatus": "1", "code_name": ""},
    ]
    client = FakeClient(stocks=stocks)
    rows = list(make(client).list_instruments(Market.CN, day=date(2024, 1, 3)))
    assert client.events == ["login", "stocks", "logout"] and client.stock_day == "2024-01-03"
    assert [r.symbol for r in rows] == ["sh.600000", "sz.000001", "sz.300001"]
    assert [r.active for r in rows] == [True, False, True]
    assert [r.exchange for r in rows] == ["XSHG", "XSHE", "XSHE"]
    assert rows[0].market is Market.CN and rows[0].asset_class is AssetClass.EQUITY
    assert rows[0].native_id == "sh.600000" and rows[2].name is None


def test_list_instruments_default_day_is_latest_weekday():
    client = FakeClient(stocks=[])
    provider = make(client, clock=lambda: datetime(2024, 1, 7, 12, tzinfo=SHANGHAI))  # Sunday
    assert list(provider.list_instruments(Market.CN)) == []
    assert client.stock_day == "2024-01-05"


def test_list_instruments_other_market_rejected():
    with pytest.raises(ProviderError):
        make().list_instruments(Market.US)


# -- wiring ----------------------------------------------------------------------------------
def test_wiring_registers_no_key_provider():
    from openbars.cli import wiring

    assert wiring._CREDENTIALS["baostock"] == {}
    assert wiring._PROVIDER_MARKETS["baostock"] == frozenset({Market.CN})
