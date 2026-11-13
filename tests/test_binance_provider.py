"""Binance adapter tests: synthetic fixtures and httpx.MockTransport only (no network)."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from openbars.core.errors import ProviderError
from openbars.core.models import (
    Adjustment,
    AssetClass,
    DateRange,
    Interval,
    Market,
)
from openbars.core.series import (
    BINANCE_SPOT_RAW,
    KNOWN_SOURCES,
    AdjustmentScope,
    PriceBasis,
    SessionSemantics,
    VolumeBasis,
)
from openbars.providers.adapter import NullQuotaGate
from openbars.providers.base import BatchShape, FetchRequest
from openbars.providers.crypto.binance import (
    DEFAULT_BASE_URL,
    EXCHANGE_INFO_WEIGHT,
    KLINES_WEIGHT,
    BinanceProvider,
)

FIXTURES = Path(__file__).parent / "fixtures" / "binance"
HOST = "data-api.binance.vision"
DAY_MS = 86_400_000
JAN_1_2024_MS = 1_704_067_200_000  # 2024-01-01T00:00:00Z


def fixture(name: str) -> str:
    return (FIXTURES / name).read_text()


def request_for(*symbols: str, start="2024-01-01", end="2024-01-03", **kw) -> FetchRequest:
    return FetchRequest(
        market=kw.pop("market", Market.CRYPTO),
        interval=kw.pop("interval", Interval.D1),
        symbols=symbols or ("TESTUSDT",),
        window=DateRange(date.fromisoformat(start), date.fromisoformat(end)),
        **kw,
    )


class RecordingGate:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int]] = []
        self.penalties: list[tuple[str, datetime]] = []

    def acquire(self, provider, weight=1, *, scope="default", timeout=None):
        self.calls.append((provider, weight))
        return NullQuotaGate().acquire(provider, weight, scope=scope)

    def penalize(self, provider, until):
        self.penalties.append((provider, until))

    def snapshot(self):  # pragma: no cover
        return {}


class Server:
    """Routes mock responses; any unexpected URL fails the test."""

    def __init__(self, *responses: httpx.Response | Exception, host: str = HOST) -> None:
        self.responses = list(responses)
        self.requests: list[httpx.Request] = []
        self.host = host

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        assert request.url.host == self.host, f"unexpected host {request.url.host}"
        assert request.url.path in ("/api/v3/klines", "/api/v3/exchangeInfo")
        assert self.responses, "more requests than prepared responses"
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def ok(name: str) -> httpx.Response:
    return httpx.Response(200, text=fixture(name))


def json_response(payload, status: int = 200, **kw) -> httpx.Response:
    return httpx.Response(status, text=json.dumps(payload), **kw)


def provider(server: Server, **kw) -> BinanceProvider:
    return BinanceProvider(client=httpx.Client(transport=httpx.MockTransport(server)), **kw)


def kline(open_ms: int, close: str = "105.00000000") -> list:
    return [
        open_ms, "100.00000000", "110.00000000", "90.00000000", close, "10.00000000",
        open_ms + DAY_MS - 1, "1000.00000000", 5, "4.0", "400.0", "0",
    ]


# -- bars ------------------------------------------------------------------------------------
def test_klines_parse_to_utc_daily_raw_bars():
    server = Server(ok("klines_testusdt_1d.json"))
    batch = provider(server, fetch_id_factory=lambda: "fetch-1").fetch_bars(request_for())
    assert batch.provider == "binance" and batch.fetch_id == "fetch-1"
    assert batch.next_cursor is None and batch.error is None
    assert len(batch.bars) == 3
    bar = batch.bars[0]
    assert (bar.provider, bar.symbol_as_returned, bar.feed) == ("binance", "TESTUSDT", "spot")
    assert bar.interval is Interval.D1
    assert bar.ts_start == datetime(2024, 1, 1, tzinfo=timezone.utc)
    assert bar.session_date == date(2024, 1, 1)
    assert (bar.open, bar.high, bar.low, bar.close) == (
        Decimal("100"), Decimal("110"), Decimal("90"), Decimal("105"),
    )
    assert bar.volume == Decimal("1234.5") and isinstance(bar.volume, Decimal)
    assert bar.vwap is None
    assert bar.extra == {"quote_volume": "129622.50000000", "trade_count": 42}
    assert bar.payload_hash
    assert [b.session_date for b in batch.bars] == [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)]
    assert all(b.ts_start.hour == 0 and b.ts_start.minute == 0 for b in batch.bars)
    assert batch.bars[1].high == Decimal("115.5") and batch.bars[2].volume == Decimal("0")

    series = bar.series
    assert series is not None
    assert series.source == BINANCE_SPOT_RAW
    assert series.legacy_adjustment == "raw" and series.currency is None
    assert series.price_basis is PriceBasis.RAW
    assert series.adjustment_scope is AdjustmentScope.NONE
    assert series.session_semantics is SessionSemantics.UTC_DAY
    assert series.volume_basis is VolumeBasis.AS_TRADED
    assert series.source not in KNOWN_SOURCES


def test_request_query_and_headers():
    server = Server(ok("klines_testusdt_1d.json"))
    gate = RecordingGate()
    provider(server, quota_gate=gate).fetch_bars(request_for("testusdt"))
    (req,) = server.requests
    assert req.url.path == "/api/v3/klines"
    assert dict(req.url.params) == {
        "symbol": "TESTUSDT",
        "interval": "1d",
        "startTime": str(JAN_1_2024_MS),
        "endTime": str(JAN_1_2024_MS + 3 * DAY_MS - 1),  # 2024-01-03T23:59:59.999Z
        "limit": "1000",
    }
    assert req.headers["User-Agent"].startswith("openbars/")
    assert "@" not in req.headers["User-Agent"]
    assert "x-mbx-apikey" not in req.headers and "authorization" not in req.headers
    assert gate.calls == [("binance", KLINES_WEIGHT)]


def test_default_base_url_is_vision_mirror():
    assert DEFAULT_BASE_URL == "https://data-api.binance.vision"
    assert "api.binance.com" not in DEFAULT_BASE_URL.replace("data-api.binance.vision", "")


def test_injected_base_url_is_used():
    server = Server(ok("klines_testusdt_1d.json"), host="mirror.test")
    batch = provider(server, base_url="https://mirror.test/").fetch_bars(request_for())
    assert len(batch.bars) == 3


def test_window_end_day_candle_is_included_and_out_of_window_rows_dropped():
    server = Server(ok("klines_testusdt_1d.json"))
    batch = provider(server).fetch_bars(request_for(start="2024-01-02", end="2024-01-02"))
    assert [b.session_date for b in batch.bars] == [date(2024, 1, 2)]


def test_pagination_concatenates_pages_and_advances_start():
    first = [kline(JAN_1_2024_MS + i * DAY_MS) for i in range(1000)]
    second_start = JAN_1_2024_MS + 1000 * DAY_MS
    second = [kline(second_start + i * DAY_MS) for i in range(5)]
    server = Server(json_response(first), json_response(second))
    gate = RecordingGate()
    end = date.fromordinal(date(2024, 1, 1).toordinal() + 1004)
    batch = provider(server, quota_gate=gate).fetch_bars(
        request_for(start="2024-01-01", end=end.isoformat())
    )
    assert len(batch.bars) == 1005 and batch.next_cursor is None
    assert len({b.session_date for b in batch.bars}) == 1005
    starts = [int(r.url.params["startTime"]) for r in server.requests]
    assert starts == [JAN_1_2024_MS, JAN_1_2024_MS + 999 * DAY_MS + 1]  # last openTime + 1 ms
    assert starts[1] > starts[0]
    assert server.requests[0].url.params["endTime"] == server.requests[1].url.params["endTime"]
    assert gate.calls == [("binance", KLINES_WEIGHT)] * 2


def test_stuck_cursor_cannot_spin():
    # A full page whose last openTime is before the window start (server ignoring startTime).
    stuck = [kline(JAN_1_2024_MS - DAY_MS * (1000 - i)) for i in range(1000)]
    server = Server(json_response(stuck), json_response(stuck), json_response(stuck))
    with pytest.raises(ProviderError) as exc:
        provider(server).fetch_bars(request_for(start="2024-01-01", end="2030-01-01"))
    assert exc.value.code == "pagination_loop"


def test_later_page_failure_returns_partial_batch_with_cursor():
    first = [kline(JAN_1_2024_MS + i * DAY_MS) for i in range(1000)]
    server = Server(json_response(first), httpx.Response(503, text="down"))
    batch = provider(server).fetch_bars(request_for(start="2024-01-01", end="2030-01-01"))
    assert len(batch.bars) == 1000
    assert batch.error is not None and batch.error.code == "provider_unavailable"
    assert batch.next_cursor == str(JAN_1_2024_MS + 999 * DAY_MS + 1)


def test_empty_history_is_an_empty_batch():
    batch = provider(Server(json_response([]))).fetch_bars(request_for())
    assert batch.bars == () and batch.error is None


def test_non_midnight_open_time_rejects_the_batch():
    # Documented choice: a mis-cut row means the cut semantics are not UTC-day, so the whole
    # payload is rejected rather than keeping a subset that looks valid.
    server = Server(ok("klines_midnight_mismatch.json"))
    with pytest.raises(ProviderError) as exc:
        provider(server).fetch_bars(request_for())
    assert exc.value.code == "invalid_payload" and not exc.value.retryable


@pytest.mark.parametrize(
    "payload",
    [
        {"code": 0, "msg": "not an array"},
        [[JAN_1_2024_MS, "1", "2", "0.5"]],  # short row
        ["not-a-row"],
        [[JAN_1_2024_MS, "abc", "110", "90", "105", "1", 0, "1", 1]],  # non-numeric price
        [[JAN_1_2024_MS, "100", "110", "90", "105", "-1", 0, "1", 1]],  # negative volume
        [[JAN_1_2024_MS, "100", "90", "110", "105", "1", 0, "1", 1]],  # high < low
        [kline(JAN_1_2024_MS + DAY_MS), kline(JAN_1_2024_MS)],  # not ascending
    ],
)
def test_invalid_payload(payload):
    with pytest.raises(ProviderError) as exc:
        provider(Server(json_response(payload))).fetch_bars(request_for())
    assert exc.value.code == "invalid_payload" and exc.value.retryable is False


def test_non_json_body_is_invalid_payload():
    with pytest.raises(ProviderError) as exc:
        provider(Server(httpx.Response(200, text="<html>"))).fetch_bars(request_for())
    assert exc.value.code == "invalid_payload"


# -- errors ----------------------------------------------------------------------------------
def test_429_is_rate_limited_and_penalizes_gate():
    gate = RecordingGate()
    server = Server(httpx.Response(429, text="slow down", headers={"Retry-After": "30"}))
    with pytest.raises(ProviderError) as exc:
        provider(server, quota_gate=gate).fetch_bars(request_for())
    err = exc.value
    assert err.code == "rate_limited" and err.retryable and err.status_code == 429
    assert err.retry_at is not None
    assert gate.penalties == [("binance", err.retry_at)]


def test_418_ip_ban_is_rate_limited_with_retry_at():
    gate = RecordingGate()
    server = Server(httpx.Response(418, text="banned", headers={"Retry-After": "120"}))
    with pytest.raises(ProviderError) as exc:
        provider(server, quota_gate=gate).fetch_bars(request_for())
    assert exc.value.code == "rate_limited" and exc.value.retryable
    assert gate.penalties and gate.penalties[0][1] == exc.value.retry_at


def test_451_region_unavailable_not_retryable():
    server = Server(httpx.Response(451, text="Service unavailable from a restricted location"))
    with pytest.raises(ProviderError) as exc:
        provider(server).fetch_bars(request_for())
    err = exc.value
    assert err.code == "provider_rejected_request" and err.retryable is False
    assert err.status_code == 451
    assert "region" in str(err) and "data-api.binance.vision" in str(err)


def test_5xx_is_provider_unavailable():
    with pytest.raises(ProviderError) as exc:
        provider(Server(httpx.Response(503, text="down"))).fetch_bars(request_for())
    assert exc.value.code == "provider_unavailable" and exc.value.retryable


def test_invalid_symbol_400_maps_to_provider_error():
    body = {"code": -1121, "msg": "Invalid symbol."}
    with pytest.raises(ProviderError) as exc:
        provider(Server(json_response(body, status=400))).fetch_bars(request_for("NOPEUSDT"))
    assert exc.value.code == "provider_rejected_request" and not exc.value.retryable
    assert exc.value.status_code == 400


def test_timeout_is_retryable_transport_error():
    server = Server(httpx.ReadTimeout("slow"))
    with pytest.raises(ProviderError) as exc:
        provider(server).fetch_bars(request_for())
    assert exc.value.code == "timeout" and exc.value.retryable


# -- invalid requests ------------------------------------------------------------------------
@pytest.mark.parametrize(
    "req",
    [
        request_for(market=Market.US),
        request_for(interval=Interval.H1),
        request_for("AAAUSDT", "BBBUSDT"),
        FetchRequest(Market.CRYPTO, Interval.D1, (), DateRange(date(2024, 1, 1), date(2024, 1, 2))),
        request_for("BAD/SYMBOL"),
        request_for(cursor="not-a-number"),
        request_for(cursor="1"),  # before the window
    ],
)
def test_invalid_request(req):
    server = Server()
    with pytest.raises(ProviderError) as exc:
        provider(server).fetch_bars(req)
    assert exc.value.code == "invalid_request" and exc.value.retryable is False
    assert server.requests == []


# -- instruments / actions -------------------------------------------------------------------
def test_list_instruments_from_exchange_info():
    server = Server(ok("exchange_info.json"))
    gate = RecordingGate()
    rows = list(provider(server, quota_gate=gate).list_instruments(Market.CRYPTO))
    assert server.requests[0].url.path == "/api/v3/exchangeInfo"
    assert gate.calls == [("binance", EXCHANGE_INFO_WEIGHT)]
    assert [r.symbol for r in rows] == ["TESTUSDT", "ZZZUSDT"]  # margin-only symbol excluded
    test, zzz = rows
    assert test.market is Market.CRYPTO and test.asset_class is AssetClass.CRYPTO_SPOT
    assert test.exchange == "BINANCE" and test.native_id == "TESTUSDT" and test.active is True
    assert zzz.active is False
    assert (test.base_asset, test.quote_asset) == ("TEST", "USDT")
    assert (zzz.base_asset, zzz.quote_asset) == ("ZZZ", "USDT")


def test_list_instruments_rejects_other_markets():
    server = Server()
    with pytest.raises(ProviderError) as exc:
        provider(server).list_instruments(Market.US)
    assert exc.value.code == "invalid_request"
    assert server.requests == []


def test_exchange_info_invalid_payload():
    with pytest.raises(ProviderError) as exc:
        provider(Server(json_response({"symbols": "nope"}))).list_instruments(Market.CRYPTO)
    assert exc.value.code == "invalid_payload"


def test_corporate_actions_are_empty():
    server = Server()
    assert list(provider(server).fetch_corporate_actions(request_for())) == []
    assert server.requests == []


# -- declarations ----------------------------------------------------------------------------
def test_capabilities_and_quota_policy():
    p = BinanceProvider()
    assert p.id == p.provider_id == "binance" and p.api_version == 1
    assert p.secret_values() == ()
    (cap,) = p.capabilities()
    assert (cap.market, cap.interval) == (Market.CRYPTO, Interval.D1)
    assert cap.asset_classes == frozenset({AssetClass.CRYPTO_SPOT})
    assert cap.batch is BatchShape.PER_SYMBOL and cap.max_symbols_per_request == 1
    assert cap.max_bars_per_request == 1000
    assert cap.adjusted_modes == frozenset({Adjustment.RAW})
    assert cap.history_start is None
    assert cap.feed == "spot"
    assert cap.terms.personal_only is False and cap.terms.url
    policy = p.quota_policy()
    assert policy.provider == "binance" and policy.key_scope == "per_ip"
    assert policy.safety_margin == 0.9
    (limit,) = policy.limits
    assert limit.count == 6000 and limit.per.total_seconds() == 60 and limit.kind == "rolling"
    assert policy.official_docs_url and policy.official_docs_url.startswith("https://")
    assert KLINES_WEIGHT == 2


def test_entry_point_discovery():
    from openbars.providers.registry import discover

    assert discover()["binance"] is BinanceProvider
