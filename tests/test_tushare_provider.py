"""Tushare free-tier adapter tests: injected fake client, synthetic rows, no network/token."""

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
    KNOWN_SOURCES,
    TUSHARE_ASHARE_RAW,
    AdjustmentScope,
    BarSeriesCatalog,
    PriceBasis,
    SessionSemantics,
    VolumeBasis,
)
from openbars.providers.adapter import NullQuotaGate
from openbars.providers.base import BatchShape, FetchRequest
from openbars.providers.cn.tushare import (
    DefaultTushareClient,
    TushareProvider,
    parse_daily,
    to_native_cn_symbol,
    to_tushare_code,
)

pytestmark = pytest.mark.usefixtures("no_network")

FIXTURES = Path(__file__).parent / "fixtures" / "tushare"
SHANGHAI = ZoneInfo("Asia/Shanghai")
CREDENTIAL = "unit-" + "test-value"
_UNSET = object()


def load(name: str = "daily_sh_test.json") -> list[dict]:
    return json.loads((FIXTURES / name).read_text())


class FakeClient:
    def __init__(self, data=_UNSET, error=None):
        self.data = load() if data is _UNSET else data
        self.error = error
        self.calls: list[dict] = []

    def daily(self, *, ts_code, start_date, end_date):
        self.calls.append(dict(ts_code=ts_code, start_date=start_date, end_date=end_date))
        if self.error is not None:
            raise self.error
        return self.data


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


def make(client=None, **kw) -> TushareProvider:
    return TushareProvider(
        CREDENTIAL,
        client=client or FakeClient(),
        fetch_id_factory=lambda: "fetch-1",
        **kw,
    )


# -- identity / capabilities -----------------------------------------------------------------
def test_source_identity_and_series_semantics():
    assert TUSHARE_ASHARE_RAW not in KNOWN_SOURCES
    series = BarSeriesCatalog.resolve(TUSHARE_ASHARE_RAW)
    assert (series.provider, series.feed, series.legacy_adjustment) == (
        "tushare",
        "ashare",
        "raw",
    )
    assert series.price_basis is PriceBasis.RAW
    assert series.adjustment_scope is AdjustmentScope.NONE
    assert series.session_semantics is SessionSemantics.REGULAR
    assert series.volume_basis is VolumeBasis.AS_TRADED
    assert series.currency == "CNY"


def test_capabilities_and_policy():
    provider = make()
    (cap,) = provider.capabilities()
    assert provider.id == "tushare"
    assert provider.secret_values() == (CREDENTIAL,)
    assert (cap.market, cap.interval, cap.batch) == (Market.CN, Interval.D1, BatchShape.PER_SYMBOL)
    assert cap.adjusted_modes == frozenset({Adjustment.RAW})
    assert cap.asset_classes == frozenset({AssetClass.EQUITY})
    assert cap.max_symbols_per_request == 1
    assert cap.max_bars_per_request == 6000
    assert cap.feed == "ashare"
    assert cap.terms.personal_only and not cap.terms.redistribution_allowed
    assert cap.terms.url and "doc_id=405" in cap.terms.url
    notes = cap.terms.notes or ""
    assert "数据来源：Tushare数据" in notes and "non-commercial" in notes
    policy = provider.quota_policy()
    assert policy.provider == "tushare"
    assert policy.key_scope == "per_key"
    assert policy.safety_margin == 0.9
    assert (policy.limits[0].count, policy.limits[0].per) == (50, timedelta(minutes=1))
    assert policy.limits[1].count == 8000
    assert policy.effective_limit(policy.limits[0]) == 45
    assert policy.official_docs_url and "tushare.pro" in policy.official_docs_url


# -- symbols ---------------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("raw", "tushare", "native"),
    [
        ("sh.600000", "600000.SH", "sh.600000"),
        ("600000.SH", "600000.SH", "sh.600000"),
        ("SH600000", "600000.SH", "sh.600000"),
        ("sz.000001", "000001.SZ", "sz.000001"),
        ("000001.sz", "000001.SZ", "sz.000001"),
    ],
)
def test_symbol_helpers(raw, tushare, native):
    assert to_tushare_code(raw) == tushare
    assert to_native_cn_symbol(raw) == native


@pytest.mark.parametrize("raw", ["600000", "000001", "xx.600000", "", "sh.60000"])
def test_symbol_helper_rejects(raw):
    with pytest.raises(ValueError):
        to_tushare_code(raw)


def test_bare_code_rejected_before_any_call():
    client = FakeClient()
    with pytest.raises(ProviderError) as err:
        make(client).fetch_bars(request_for("600000"))
    assert err.value.code == "invalid_request" and not err.value.retryable
    assert client.calls == []


def test_alternate_spelling_talks_tushare_form_but_stores_native_form():
    client = FakeClient()
    batch = make(client).fetch_bars(request_for("600000.SH"))
    assert client.calls[0]["ts_code"] == "600000.SH"
    assert batch.bars[0].symbol_as_returned == "sh.600000"


# -- bars ------------------------------------------------------------------------------------
def test_bars_unadjusted_decimal_and_quota_before_call():
    client = FakeClient()
    gate = RecordingGate()
    seen = []
    batch = make(client, quota_gate=gate, before_request=lambda: seen.append(1)).fetch_bars(request_for())
    assert gate.calls == [("tushare", 1)]
    assert seen == [1]
    assert client.calls == [dict(ts_code="600000.SH", start_date="20240102", end_date="20240104")]
    assert batch.provider == "tushare" and batch.fetch_id == "fetch-1" and batch.error is None
    assert [b.session_date for b in batch.bars] == [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4)]
    first = batch.bars[0]
    assert (first.provider, first.symbol_as_returned, first.feed) == ("tushare", "sh.600000", "ashare")
    assert first.interval is Interval.D1
    assert first.series.source == TUSHARE_ASHARE_RAW
    assert first.ts_start == datetime(2024, 1, 2, tzinfo=SHANGHAI)
    assert first.ts_start.utcoffset() == timedelta(hours=8)
    for value in (first.open, first.high, first.low, first.close, first.volume):
        assert isinstance(value, Decimal)
    assert (first.open, first.close) == (Decimal("10.0"), Decimal("10.2"))
    assert first.volume == Decimal("123400.0")  # 1234 lots * 100
    assert first.extra["volume_lots"] == "1234"
    assert first.extra["amount"] == "1287" and first.extra["amount_unit"] == "thousand_cny"
    assert first.extra["amount_cny"] == "1287000"
    assert first.extra["pre_close"] == "10" and first.extra["pct_chg"] == "2"
    assert first.extra["ts_code"] == "600000.SH" and first.extra["code"] == "600000.SH"


def test_zero_volume_row_is_kept():
    batch = make().fetch_bars(request_for())
    assert batch.bars[-1].volume == Decimal("0.0")
    assert batch.bars[-1].extra["amount"] == "0"


def test_rows_sorted_ascending_and_window_filtered():
    rows = list(reversed(load()))
    batch = make(FakeClient(rows)).fetch_bars(request_for(start="2024-01-03", end="2024-01-04"))
    assert [b.session_date for b in batch.bars] == [date(2024, 1, 3), date(2024, 1, 4)]


def test_non_session_rows_are_skipped():
    rows = [*load(), {**load()[0], "trade_date": "20240106"}]  # Saturday
    batch = make(FakeClient(rows)).fetch_bars(request_for(start="2024-01-02", end="2024-01-06"))
    assert [b.session_date for b in batch.bars] == [
        date(2024, 1, 2),
        date(2024, 1, 3),
        date(2024, 1, 4),
    ]


def test_empty_result_is_an_empty_batch():
    assert make(FakeClient([])).fetch_bars(request_for()).bars == ()
    assert make(FakeClient(None)).fetch_bars(request_for()).bars == ()


@pytest.mark.parametrize(
    "mutate",
    [
        lambda rows: rows[0].update(close="abc"),
        lambda rows: rows[0].update(open="NaN"),
        lambda rows: rows[0].update(trade_date="bad"),
        lambda rows: rows[0].update(ts_code="000001.SZ"),
        lambda rows: rows[0].update(vol=-1),
        lambda rows: rows[0].update(vol=""),
        lambda rows: rows[0].update(high="1.0", low="9.0"),
        lambda rows: rows[1].update(trade_date="20240102"),  # duplicate
        lambda rows: rows[0].pop("close"),
        lambda rows: rows[0].pop("vol"),
    ],
)
def test_invalid_payload_rejected(mutate):
    rows = load()
    mutate(rows)
    with pytest.raises(ProviderError) as err:
        make(FakeClient(rows)).fetch_bars(request_for())
    assert err.value.code == "invalid_payload" and not err.value.retryable


def test_non_list_payload_rejected():
    with pytest.raises(ProviderError) as err:
        make(FakeClient({"oops": 1})).fetch_bars(request_for())
    assert err.value.code == "invalid_payload"


def test_dataframe_payload_is_parsed():
    import pandas as pd

    frame = pd.DataFrame(load()).set_index("trade_date")
    batch = make(FakeClient(frame)).fetch_bars(request_for())
    assert [b.session_date for b in batch.bars] == [
        date(2024, 1, 2),
        date(2024, 1, 3),
        date(2024, 1, 4),
    ]


def test_parse_daily_accepts_date_objects_and_iso_strings():
    rows = load()
    rows[0]["trade_date"] = date(2024, 1, 2)
    rows[1]["trade_date"] = "2024-01-03"
    bars = parse_daily(
        rows,
        code="600000.SH",
        window_start=date(2024, 1, 2),
        window_end=date(2024, 1, 4),
        fetch_id="f",
        series=BarSeriesCatalog.resolve(TUSHARE_ASHARE_RAW),
    )
    assert [b.session_date for b in bars] == [
        date(2024, 1, 2),
        date(2024, 1, 3),
        date(2024, 1, 4),
    ]


@pytest.mark.parametrize(
    "kw",
    [
        dict(market=Market.US),
        dict(interval=Interval.H1),
        dict(feed="other"),
        dict(adjustment="qfq"),
        dict(adjustment="split"),
        dict(cursor="1"),
    ],
)
def test_unsupported_requests(kw):
    client = FakeClient()
    with pytest.raises(ProviderError) as err:
        make(client).fetch_bars(request_for(**kw))
    assert err.value.code == "invalid_request"
    assert client.calls == []


def test_one_symbol_per_request():
    request = FetchRequest(
        Market.CN,
        Interval.D1,
        ("sh.600000", "sz.000001"),
        DateRange(date(2024, 1, 2), date(2024, 1, 3)),
    )
    with pytest.raises(ProviderError) as err:
        make().fetch_bars(request)
    assert err.value.code == "invalid_request"


# -- errors / token / package ----------------------------------------------------------------
def test_quota_exhausted_stops_before_any_call():
    client = FakeClient()
    with pytest.raises(QuotaExhausted):
        make(client, quota_gate=RecordingGate(exhausted=True)).fetch_bars(request_for())
    assert client.calls == []


def test_missing_token_is_reported_without_calling_anything(monkeypatch):
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    provider = TushareProvider(fetch_id_factory=lambda: "f")
    with pytest.raises(ProviderError) as err:
        provider.fetch_bars(request_for())
    assert err.value.code == "tushare_token_missing" and not err.value.retryable
    assert "TUSHARE_TOKEN" in str(err.value)


def test_token_is_read_from_environment(monkeypatch):
    monkeypatch.setenv("TUSHARE_TOKEN", CREDENTIAL)
    assert TushareProvider().secret_values() == (CREDENTIAL,)


def test_client_error_does_not_leak_the_token():
    client = FakeClient(error=OSError(f"boom {CREDENTIAL} on host"))
    with pytest.raises(ProviderError) as err:
        make(client).fetch_bars(request_for())
    assert err.value.code == "provider_unavailable" and err.value.retryable
    assert CREDENTIAL not in str(err.value)


@pytest.mark.parametrize(
    ("error", "code", "retryable"),
    [
        (TimeoutError("slow"), "provider_unavailable", True),
        (Exception("没有权限"), "provider_rejected_request", False),
        (Exception("每分钟最多访问50次"), "rate_limited", True),
    ],
)
def test_client_errors_are_classified(error, code, retryable):
    with pytest.raises(ProviderError) as err:
        make(FakeClient(error=error)).fetch_bars(request_for())
    assert (err.value.code, err.value.retryable) == (code, retryable)


def test_client_provider_error_is_passed_through():
    rejected = ProviderError("provider_rejected_request", "bad param", retryable=False)
    with pytest.raises(ProviderError) as err:
        make(FakeClient(error=rejected)).fetch_bars(request_for())
    assert err.value is rejected


def test_missing_package_raises_not_installed(monkeypatch):
    monkeypatch.setitem(sys.modules, "tushare", None)  # import -> ImportError
    provider = TushareProvider(CREDENTIAL)
    with pytest.raises(ProviderError) as err:
        provider.fetch_bars(request_for())
    assert err.value.code == "tushare_not_installed" and not err.value.retryable
    assert "openbars[tushare]" in str(err.value)


def test_module_import_does_not_need_package(monkeypatch):
    real_import = builtins.__import__

    def guarded(name, *args, **kwargs):
        if name == "tushare":
            raise ImportError("blocked")
        return real_import(name, *args, **kwargs)

    monkeypatch.delitem(sys.modules, "openbars.providers.cn.tushare", raising=False)
    monkeypatch.setattr(builtins, "__import__", guarded)
    import importlib

    module = importlib.import_module("openbars.providers.cn.tushare")
    assert module.TushareProvider.id == "tushare"


def test_default_client_calls_pro_api_with_token_and_daily_kwargs(monkeypatch):
    seen = {}

    class Api:
        def daily(self, **kwargs):
            seen.update(kwargs)
            return load()

    class FakeModule:
        def pro_api(self, token):
            seen["token_arg"] = token
            return Api()

    monkeypatch.setitem(sys.modules, "tushare", FakeModule())
    batch = TushareProvider(CREDENTIAL, fetch_id_factory=lambda: "f").fetch_bars(request_for())
    assert seen == {
        "token_arg": CREDENTIAL,
        "ts_code": "600000.SH",
        "start_date": "20240102",
        "end_date": "20240104",
    }
    assert len(batch.bars) == 3


def test_corporate_actions_are_empty_and_no_adjust_factor_api():
    provider = make()
    assert list(provider.fetch_corporate_actions(request_for())) == []
    assert not hasattr(provider, "fetch_adjust_factors")
    with pytest.raises(NotImplementedError):
        list(provider.list_instruments(Market.CN))


# -- wiring ----------------------------------------------------------------------------------
def test_wiring_registers_token_provider_for_cn_only():
    from openbars.cli import wiring

    assert wiring._CREDENTIALS["tushare"] == {"token": "token"}
    assert wiring._PROVIDER_MARKETS["tushare"] == frozenset({Market.CN})


def test_build_providers_requires_the_token(monkeypatch, tmp_path):
    from openbars.cli.wiring import build_providers
    from openbars.config.loader import load_settings

    config = tmp_path / "config.yaml"
    config.write_text(
        "markets: [cn]\n"
        "providers:\n"
        "  tushare:\n"
        "    enabled: true\n"
        "    token: ${TUSHARE_TOKEN}\n"
    )
    settings = load_settings(config, env={})
    assert build_providers(settings) == {}
    built = build_providers(load_settings(config, env={"TUSHARE_TOKEN": CREDENTIAL}))
    assert set(built) == {"tushare"}
    assert built["tushare"].secret_values() == (CREDENTIAL,)
