"""Alpaca adapter tests: synthetic fixtures and httpx.MockTransport only (no network)."""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from openbars.core.errors import ProviderError, QuotaExhausted
from openbars.core.models import (
    Adjustment,
    CorporateActionKind,
    DateRange,
    Interval,
    Market,
)
from openbars.providers.adapter import HttpProviderAdapter, NullQuotaGate
from openbars.providers.base import BatchShape, FetchRequest
from openbars.providers.http import (
    TokenLoopGuard,
    decimal_value,
    digest_payload,
    integer_value,
    provider_error_from_response,
    redact_response_text,
    require_object,
)
from openbars.providers.us.alpaca import AlpacaProvider
from openbars.providers.us.alpaca_time import historical_end_parameter

FIXTURES = Path(__file__).parent / "fixtures" / "alpaca"
# Assembled so the source has no 16+ character credential assignment literals.
KEY = "test-key-" + "id-xx"
SECRET = "test-sec" + "ret-xx"
NOW = datetime(2024, 1, 5, 15, 30, 45, tzinfo=timezone.utc)  # Friday 10:30 in New York


def fixture(name: str) -> str:
    return (FIXTURES / name).read_text()


def request_for(*symbols: str, start="2024-01-02", end="2024-01-05", **kw) -> FetchRequest:
    return FetchRequest(
        market=Market.US,
        interval=Interval.D1,
        symbols=symbols or ("TEST1",),
        window=DateRange(date.fromisoformat(start), date.fromisoformat(end)),
        **kw,
    )


class RecordingGate:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int]] = []

    def acquire(self, provider, weight=1, *, scope="default", timeout=None):
        self.calls.append((provider, weight))
        return NullQuotaGate().acquire(provider, weight, scope=scope)

    def penalize(self, provider, until):  # pragma: no cover
        pass

    def snapshot(self):  # pragma: no cover
        return {}


class Server:
    """Routes mock responses; any unexpected URL fails the test."""

    def __init__(self, *responses: httpx.Response | Exception) -> None:
        self.responses = list(responses)
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        assert request.url.host == "data.alpaca.markets", f"unexpected host {request.url.host}"
        assert request.url.path in ("/v2/stocks/bars", "/v1/corporate-actions")
        assert self.responses, "more requests than prepared responses"
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def ok(name: str) -> httpx.Response:
    return httpx.Response(200, text=fixture(name))


def provider(server: Server, **kw) -> AlpacaProvider:
    kw.setdefault("api_key", KEY)
    kw.setdefault("api_secret", SECRET)
    kw.setdefault("clock", lambda: NOW)
    kw.setdefault("env", {})
    return AlpacaProvider(client=httpx.Client(transport=httpx.MockTransport(server)), **kw)


# -- bars ------------------------------------------------------------------------------------
def test_sip_split_page_parses_to_raw_bars():
    server = Server(ok("bars_sip_split.json"))
    batch = provider(server).fetch_bars(request_for("TEST1", "ZZZA"))
    assert batch.provider == "alpaca" and batch.next_cursor is None and batch.error is None
    assert len(batch.bars) == 3
    bar = batch.bars[0]
    assert (bar.provider, bar.symbol_as_returned, bar.feed) == ("alpaca", "TEST1", "sip")
    assert bar.session_date == date(2024, 1, 2)
    assert bar.ts_start == datetime(2024, 1, 2, 5, 0, tzinfo=timezone.utc)
    assert bar.open == Decimal("10.10") and isinstance(bar.close, Decimal)
    assert bar.volume == Decimal("120000")
    assert bar.vwap == Decimal("10.41234568")
    assert bar.extra == {"trade_count": 850}
    assert bar.payload_hash and bar.series.source.key == "alpaca/sip/split"

    req = server.requests[0]
    assert req.headers["APCA-API-KEY-ID"] == KEY
    assert req.headers["APCA-API-SECRET-KEY"] == SECRET
    params = dict(req.url.params)
    assert params == {
        "symbols": "TEST1,ZZZA",
        "timeframe": "1Day",
        "start": "2024-01-02",
        "end": "2024-01-05T15:14:45Z",
        "feed": "sip",
        "adjustment": "split",
        "limit": "10000",
    }


def test_zero_vwap_is_none():
    batch = provider(Server(ok("bars_sip_split.json"))).fetch_bars(request_for("TEST1", "ZZZA"))
    zzza = [b for b in batch.bars if b.symbol_as_returned == "ZZZA"][0]
    assert zzza.vwap is None


def test_pagination_returns_all_bars():
    server = Server(ok("bars_page1.json"), ok("bars_page2.json"))
    batch = provider(server).fetch_bars(request_for())
    assert [b.session_date for b in batch.bars] == [date(2024, 1, 2), date(2024, 1, 3)]
    assert "page_token" not in server.requests[0].url.params
    assert server.requests[1].url.params["page_token"] == "synthetic-page-token-0002"


def test_repeated_page_token_is_pagination_loop():
    server = Server(ok("bars_page1.json"), httpx.Response(200, text=fixture("bars_page1.json")))
    batch = provider(server).fetch_bars(request_for())  # partial: page 1 kept
    assert len(batch.bars) == 2  # both pages kept; the loop is detected on the repeated token
    assert batch.error is not None and batch.error.code == "pagination_loop"
    assert batch.error.retryable is False


def test_repeated_token_on_first_page_raises():
    page = json.dumps({"bars": {}, "next_page_token": "tok-aaaaaaaa"})
    server = Server(httpx.Response(200, text=page), httpx.Response(200, text=page))
    with pytest.raises(ProviderError) as info:
        provider(server).fetch_bars(request_for())
    assert info.value.code == "pagination_loop"


def test_token_loop_guard():
    guard = TokenLoopGuard()
    assert guard.accept("a-token") == "a-token"
    for bad in ("", None, 5, "a-token"):
        with pytest.raises(ProviderError) as info:
            guard.accept(bad)
        assert info.value.code == "pagination_loop" and not info.value.retryable
    with pytest.raises(ProviderError):
        TokenLoopGuard("seed").accept("seed")


def test_later_page_failure_returns_partial_batch_with_cursor():
    server = Server(ok("bars_page1.json"), httpx.Response(503, text="down"))
    batch = provider(server).fetch_bars(request_for())
    assert len(batch.bars) == 1
    assert batch.error.code == "provider_unavailable"
    assert batch.next_cursor == "0:synthetic-page-token-0002"
    # resuming from the cursor continues with that page token
    server2 = Server(ok("bars_page2.json"))
    rest = provider(server2).fetch_bars(request_for(cursor=batch.next_cursor))
    assert server2.requests[0].url.params["page_token"] == "synthetic-page-token-0002"
    assert len(rest.bars) == 1 and rest.next_cursor is None


def test_first_page_failure_raises():
    with pytest.raises(ProviderError) as info:
        provider(Server(httpx.Response(500, text="x"))).fetch_bars(request_for())
    assert info.value.code == "provider_unavailable"


@pytest.mark.parametrize(
    "status,code,retryable",
    [
        (429, "rate_limited", True),
        (500, "provider_unavailable", True),
        (503, "provider_unavailable", True),
        (400, "provider_rejected_request", False),
        (403, "provider_rejected_request", False),
    ],
)
def test_http_error_mapping_and_redaction(status, code, retryable):
    body = f"bad credentials {KEY} and {SECRET} and short"
    response = httpx.Response(status, text=body, headers={"x-request-id": "req-synthetic-1"})
    with pytest.raises(ProviderError) as info:
        provider(Server(response)).fetch_bars(request_for())
    err = info.value
    assert (err.code, err.retryable, err.status_code) == (code, retryable, status)
    assert err.request_id == "req-synthetic-1"
    assert "[REDACTED]" in err.raw_payload
    for secret in (KEY, SECRET):
        assert secret not in err.raw_payload
        assert secret not in str(err)
        assert secret not in repr(err.args)


def test_redact_response_text_ignores_short_secrets():
    assert redact_response_text("abc 12345678 xyz", "12345678", "abc", None, "") == (
        "abc [REDACTED] xyz"
    )


def test_provider_error_from_response_standalone():
    err = provider_error_from_response(httpx.Response(429, text="slow down"), secret_values=())
    assert err.code == "rate_limited" and err.retryable and err.raw_payload == "slow down"


def test_transport_and_timeout_errors():
    for exc, code in (
        (httpx.ReadTimeout("t"), "timeout"),
        (httpx.ConnectError("c"), "transport_error"),
    ):
        with pytest.raises(ProviderError) as info:
            provider(Server(exc)).fetch_bars(request_for())
        assert info.value.code == code and info.value.retryable


def test_malformed_payloads():
    cases = {
        "not json": httpx.Response(200, text="<html>"),
        "list": httpx.Response(200, text="[]"),
        "no bars member": httpx.Response(200, text='{"next_page_token": null}'),
        "bars not object": httpx.Response(200, text='{"bars": []}'),
        "symbol not array": httpx.Response(200, text='{"bars": {"TEST1": null}}'),
        "bad timestamp": httpx.Response(
            200,
            text='{"bars": {"TEST1": [{"t": "nope", "o": 1, "h": 1, "l": 1, "c": 1, "v": 1}]}}',
        ),
        "naive timestamp": httpx.Response(
            200,
            text='{"bars": {"TEST1": [{"t": "2024-01-02T05:00:00", "o": 1, "h": 1, "l": 1, "c": 1, "v": 1}]}}',
        ),
        "bad price": httpx.Response(
            200,
            text='{"bars": {"TEST1": [{"t": "2024-01-02T05:00:00Z", "o": "x", "h": 1, "l": 1, "c": 1, "v": 1}]}}',
        ),
        "unrequested symbol": ok("bars_unrequested_symbol.json"),
        "outside window": ok("bars_out_of_window.json"),
    }
    for label, response in cases.items():
        with pytest.raises(ProviderError) as info:
            provider(Server(response)).fetch_bars(request_for())
        assert info.value.code == "invalid_payload", label
        assert not info.value.retryable


def test_empty_bars_is_empty_result():
    with pytest.raises(ProviderError) as info:
        provider(Server(ok("bars_empty.json"))).fetch_bars(request_for())
    assert info.value.code == "empty_result" and not info.value.retryable


def test_iex_request_does_not_switch_to_sip():
    server = Server(ok("bars_page2.json"))
    batch = provider(server).fetch_bars(request_for(feed="iex", start="2024-01-03", end="2024-01-05"))
    params = server.requests[0].url.params
    assert params["feed"] == "iex"
    assert params["end"] == "2024-01-05"  # IEX never gets the SIP recent-data cutoff
    assert batch.bars[0].feed == "iex"
    assert batch.bars[0].series.source.key == "alpaca/iex/split"


def test_raw_adjustment_and_invalid_options():
    server = Server(ok("bars_page2.json"))
    provider(server).fetch_bars(request_for(adjustment="raw", start="2024-01-03"))
    assert server.requests[0].url.params["adjustment"] == "raw"
    for kw in ({"feed": "otc"}, {"adjustment": "all"}):
        with pytest.raises(ProviderError) as info:
            provider(Server()).fetch_bars(request_for(**kw))
        assert info.value.code == "invalid_request"
    with pytest.raises(ProviderError):
        provider(Server()).fetch_bars(
            FetchRequest(Market.CRYPTO, Interval.D1, ("TEST1",), DateRange(date(2024, 1, 2), date(2024, 1, 3)))
        )


def test_symbols_are_chunked_by_200():
    symbols = tuple(f"T{i}" for i in range(250))
    empty = '{"bars": {"T0": [{"t": "2024-01-02T05:00:00Z", "o": 1, "h": 1, "l": 1, "c": 1, "v": 1}]}}'
    server = Server(httpx.Response(200, text=empty), httpx.Response(200, text='{"bars": {}}'))
    batch = provider(server).fetch_bars(request_for(*symbols))
    assert [len(r.url.params["symbols"].split(",")) for r in server.requests] == [200, 50]
    assert len(batch.bars) == 1


def test_historical_end_parameter():
    assert historical_end_parameter(date(2024, 1, 5), feed="sip", now=NOW) == "2024-01-05T15:14:45Z"
    assert historical_end_parameter(date(2024, 1, 4), feed="sip", now=NOW) == "2024-01-04"
    assert historical_end_parameter(date(2024, 1, 5), feed="iex", now=NOW) == "2024-01-05"
    # 02:00 UTC on Jan 6 is still Jan 5 in New York
    late = datetime(2024, 1, 6, 2, 0, 0, tzinfo=timezone.utc)
    assert historical_end_parameter(date(2024, 1, 5), feed="sip", now=late) == "2024-01-06T01:44:00Z"
    assert historical_end_parameter(date(2024, 1, 6), feed="sip", now=late) == "2024-01-06"
    with pytest.raises(ValueError):
        historical_end_parameter(date(2024, 1, 5), feed="sip", now=datetime(2024, 1, 5, 12, 0))


# -- corporate actions ---------------------------------------------------------------------------
def test_corporate_actions_map_and_paginate():
    server = Server(ok("corporate_actions_page1.json"), ok("corporate_actions_page2.json"))
    actions = list(
        provider(server).fetch_corporate_actions(request_for("TEST1", "ZZZA", start="2024-01-01", end="2024-04-01"))
    )
    first = server.requests[0]
    assert first.url.path == "/v1/corporate-actions"
    assert dict(first.url.params) == {
        "symbols": "TEST1,ZZZA",
        "start": "2024-01-01",
        "end": "2024-04-01",
        "data_quality": "all",
        "limit": "1000",
    }
    assert server.requests[1].url.params["page_token"] == "synthetic-ca-token-0002"
    assert first.headers["APCA-API-KEY-ID"] == KEY

    split, dividend, reverse, rename = actions
    assert (split.instrument_id, split.kind, split.ex_date, split.ratio) == (
        "TEST1", CorporateActionKind.SPLIT, date(2024, 2, 1), Decimal("2"),
    )
    assert (dividend.instrument_id, dividend.kind, dividend.amount, dividend.ex_date) == (
        "ZZZA", CorporateActionKind.DIVIDEND, Decimal("0.25"), date(2024, 2, 9),
    )
    assert reverse.kind is CorporateActionKind.SPLIT and reverse.ratio == Decimal("0.25")
    assert rename.kind is CorporateActionKind.SYMBOL_CHANGE and rename.instrument_id == "TEST1"
    assert all(a.source == "alpaca" for a in actions)


def test_corporate_actions_result_reports_skips_and_partials():
    server = Server(ok("corporate_actions_page1.json"), ok("corporate_actions_page2.json"))
    result = provider(server).fetch_corporate_actions_result(request_for("TEST1", "ZZZA"))
    assert len(result.actions) == 4
    assert result.skipped == {"cash_mergers": 1, "rights_issues:unsupported_collection": 1}

    server = Server(ok("corporate_actions_page1.json"), httpx.Response(429, text="slow"))
    result = provider(server).fetch_corporate_actions_result(request_for("TEST1"))
    assert len(result.actions) == 2
    assert result.error.code == "rate_limited"
    assert result.next_cursor == "0:synthetic-ca-token-0002"
    with pytest.raises(ProviderError):
        provider(
            Server(ok("corporate_actions_page1.json"), httpx.Response(429, text="slow"))
        ).fetch_corporate_actions(request_for("TEST1"))


def test_corporate_actions_loop_and_errors():
    page = json.dumps({"corporate_actions": {}, "next_page_token": "tok-bbbbbbbb"})
    server = Server(*[httpx.Response(200, text=page)] * 2)
    with pytest.raises(ProviderError) as info:
        provider(server).fetch_corporate_actions(request_for())
    assert info.value.code == "pagination_loop"
    with pytest.raises(ProviderError) as info:
        provider(Server(httpx.Response(400, text=KEY))).fetch_corporate_actions(request_for())
    assert KEY not in info.value.raw_payload
    with pytest.raises(ProviderError) as info:
        provider(Server(httpx.Response(200, text="{}"))).fetch_corporate_actions(request_for())
    assert info.value.code == "invalid_payload"


# -- credentials ---------------------------------------------------------------------------------
def test_construct_without_credentials_fails_only_on_fetch():
    server = Server()
    p = AlpacaProvider(env={}, client=httpx.Client(transport=httpx.MockTransport(server)))
    for call in (p.fetch_bars, p.fetch_corporate_actions):
        with pytest.raises(ProviderError) as info:
            call(request_for())
        assert info.value.code == "not_configured" and not info.value.retryable
    assert server.requests == []


def test_credentials_from_env():
    server = Server(ok("bars_page2.json"))
    env = {"ALPACA_API_KEY_ID": KEY, "ALPACA_API_SECRET_KEY": SECRET}
    p = AlpacaProvider(
        env=env, clock=lambda: NOW, client=httpx.Client(transport=httpx.MockTransport(server))
    )
    p.fetch_bars(request_for(start="2024-01-03"))
    assert server.requests[0].headers["APCA-API-KEY-ID"] == KEY
    assert p.secret_values() == (KEY, SECRET)
    assert p.redact(f"x {KEY} {SECRET}") == "x [REDACTED] [REDACTED]"


def test_partial_credentials_not_configured():
    p = AlpacaProvider(api_key=KEY, env={}, client=httpx.Client(transport=httpx.MockTransport(Server())))
    with pytest.raises(ProviderError) as info:
        p.fetch_bars(request_for())
    assert info.value.code == "not_configured"


# -- quota gate ------------------------------------------------------------------------------------
def test_gate_called_once_per_http_get():
    gate = RecordingGate()
    server = Server(ok("bars_page1.json"), ok("bars_page2.json"))
    provider(server, quota_gate=gate).fetch_bars(request_for())
    assert gate.calls == [("alpaca", 1), ("alpaca", 1)]
    assert len(server.requests) == 2


def test_default_gate_is_null_gate_and_hook_runs():
    calls = []
    server = Server(ok("bars_page2.json"))
    p = provider(server, before_request=lambda: calls.append(1))
    assert isinstance(p._quota_gate, NullQuotaGate)
    p.fetch_bars(request_for(start="2024-01-03"))
    assert calls == [1]
    assert NullQuotaGate().acquire("alpaca").weight == 1


def test_gate_runs_before_request_and_exhaustion_blocks_http():
    class DenyGate(RecordingGate):
        def acquire(self, provider, weight=1, *, scope="default", timeout=None):
            raise QuotaExhausted(provider)

    server = Server()
    with pytest.raises(QuotaExhausted):
        provider(server, quota_gate=DenyGate()).fetch_bars(request_for())
    assert server.requests == []


def test_quota_exhausted_mid_pagination_returns_partial():
    class SecondDenied(RecordingGate):
        def acquire(self, provider, weight=1, *, scope="default", timeout=None):
            if self.calls:
                raise QuotaExhausted(provider)
            return super().acquire(provider, weight, scope=scope)

    batch = provider(Server(ok("bars_page1.json")), quota_gate=SecondDenied()).fetch_bars(request_for())
    assert len(batch.bars) == 1 and batch.error.code == "quota_exhausted" and batch.error.retryable


# -- capabilities / registry / helpers -----------------------------------------------------------
def test_capabilities_and_quota_policy():
    p = AlpacaProvider(env={})
    caps = {c.feed: c for c in p.capabilities()}
    assert set(caps) == {"alpaca:sip", "alpaca:iex"}
    sip = caps["alpaca:sip"]
    assert sip.market is Market.US and sip.interval is Interval.D1
    assert sip.history_start == date(2016, 1, 1)
    assert sip.batch is BatchShape.MULTI_SYMBOL
    assert (sip.max_symbols_per_request, sip.max_bars_per_request) == (200, 10000)
    assert Adjustment.SPLIT in sip.adjusted_modes and Adjustment.RAW in sip.adjusted_modes
    assert sip.terms.personal_only and not sip.terms.redistribution_allowed
    assert sip.terms.url.startswith("https://")
    limit = p.quota_policy().limits[0]
    assert (limit.count, limit.per) == (200, timedelta(minutes=1))
    assert p.id == "alpaca"


def test_entry_point_discovery():
    from openbars.providers.registry import discover

    assert discover()["alpaca"] is AlpacaProvider


def test_http_helpers():
    from decimal import Decimal as D

    assert decimal_value(1.1) == D("1.1") and decimal_value("2.50") == D("2.50")
    for bad in (True, None, "abc", float("nan"), [1]):
        with pytest.raises(ProviderError):
            decimal_value(bad)
    assert integer_value(5) == 5 and integer_value("7") == 7 and integer_value(D("3")) == 3
    for bad in (1.5, True, "x"):
        with pytest.raises(ProviderError):
            integer_value(bad)
    assert require_object({"a": 1}) == {"a": 1}
    with pytest.raises(ProviderError):
        require_object([])
    assert digest_payload({"a": 1, "b": 2}) == digest_payload({"b": 2, "a": 1})
    assert digest_payload({"a": 1}) != digest_payload({"a": 2})


def test_adapter_is_reusable_and_closes_own_client():
    class Plain(HttpProviderAdapter):
        provider_id = "plain"

    adapter = Plain()
    assert adapter.secret_values() == ()
    assert adapter.redact("x") == "x"
    adapter.close()


def test_provider_error_keeps_old_signature():
    err = ProviderError("c", "m", retryable=False)
    assert err.raw_payload is None
