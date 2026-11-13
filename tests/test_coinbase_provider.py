"""Coinbase Exchange adapter tests: synthetic fixtures and httpx.MockTransport only (no network)."""

from __future__ import annotations

import json
import re
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
    COINBASE_EXCHANGE_RAW,
    KNOWN_SOURCES,
    AdjustmentScope,
    PriceBasis,
    SessionSemantics,
    SourceIdentity,
    VolumeBasis,
)
from openbars.providers.adapter import NullQuotaGate
from openbars.providers.base import BatchShape, FetchRequest
from openbars.providers.crypto.coinbase import (
    DEFAULT_BASE_URL,
    MAX_BARS_PER_REQUEST,
    REQUEST_WEIGHT,
    CoinbaseProvider,
)

FIXTURES = Path(__file__).parent / "fixtures" / "coinbase"
HOST = "api.exchange.coinbase.com"
PRODUCT = "TEST-USDT"
DAY = 86_400
JAN_1_2024 = 1_704_067_200  # 2024-01-01T00:00:00Z
CANDLES_PATH = f"/products/{PRODUCT}/candles"


def fixture(name: str) -> str:
    return (FIXTURES / name).read_text()


def request_for(*symbols: str, start="2024-01-01", end="2024-01-03", **kw) -> FetchRequest:
    return FetchRequest(
        market=kw.pop("market", Market.CRYPTO),
        interval=kw.pop("interval", Interval.D1),
        symbols=symbols or (PRODUCT,),
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
        assert request.url.path == "/products" or re.fullmatch(
            r"/products/[A-Z0-9]+-[A-Z0-9]+/candles", request.url.path
        ), f"unexpected path {request.url.path}"
        assert self.responses, "more requests than prepared responses"
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def ok(name: str) -> httpx.Response:
    return httpx.Response(200, text=fixture(name))


def json_response(payload, status: int = 200, **kw) -> httpx.Response:
    return httpx.Response(status, text=json.dumps(payload), **kw)


def provider(server: Server, **kw) -> CoinbaseProvider:
    return CoinbaseProvider(client=httpx.Client(transport=httpx.MockTransport(server)), **kw)


def candle(open_s: int, close: float = 105) -> list:
    """``[time, low, high, open, close, volume]``."""
    return [open_s, 90, 110, 100, close, 10]


def utc(seconds: int) -> str:
    return datetime.fromtimestamp(seconds, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# -- bars ------------------------------------------------------------------------------------
def test_candles_parse_to_utc_daily_raw_bars():
    server = Server(ok("candles_test_usdt_1d.json"))
    batch = provider(server, fetch_id_factory=lambda: "fetch-1").fetch_bars(request_for())
    assert batch.provider == "coinbase" and batch.fetch_id == "fetch-1"
    assert batch.next_cursor is None and batch.error is None
    # Fixture is newest-first and includes the next-midnight row (01-04), which is dropped.
    assert [b.session_date for b in batch.bars] == [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)]
    bar = batch.bars[0]
    assert (bar.provider, bar.symbol_as_returned, bar.feed) == ("coinbase", PRODUCT, "exchange")
    assert bar.interval is Interval.D1
    assert bar.ts_start == datetime(2024, 1, 1, tzinfo=timezone.utc)
    assert bar.session_date == date(2024, 1, 1)
    # Row layout is [time, low, high, open, close, volume].
    assert (bar.open, bar.high, bar.low, bar.close) == (
        Decimal("100"), Decimal("110"), Decimal("90"), Decimal("105"),
    )
    assert bar.volume == Decimal("1234.5") and isinstance(bar.volume, Decimal)
    assert bar.vwap is None and bar.payload_hash
    second, third = batch.bars[1], batch.bars[2]
    assert (second.open, second.high, second.low, second.close) == (
        Decimal("105"), Decimal("115.5"), Decimal("95.25"), Decimal("100.1"),
    )
    assert second.volume == Decimal("2000.25") and third.volume == Decimal("0")
    assert all(b.ts_start.hour == 0 and b.ts_start.minute == 0 for b in batch.bars)

    series = bar.series
    assert series is not None
    assert series.source == COINBASE_EXCHANGE_RAW
    assert series.source == SourceIdentity("coinbase", "exchange", "raw")
    assert series.legacy_adjustment == "raw" and series.currency is None
    assert series.price_basis is PriceBasis.RAW
    assert series.adjustment_scope is AdjustmentScope.NONE
    assert series.session_semantics is SessionSemantics.UTC_DAY
    assert series.volume_basis is VolumeBasis.AS_TRADED
    assert series.source not in KNOWN_SOURCES


def test_request_query_and_headers():
    server = Server(ok("candles_test_usdt_1d.json"))
    gate = RecordingGate()
    provider(server, quota_gate=gate).fetch_bars(request_for("test-usdt"))
    (req,) = server.requests
    assert req.url.path == CANDLES_PATH
    assert dict(req.url.params) == {
        "granularity": "86400",
        "start": "2024-01-01T00:00:00Z",
        "end": "2024-01-04T00:00:00Z",  # next midnight after window.end
    }
    assert req.headers["User-Agent"].startswith("openbars/")
    assert "@" not in req.headers["User-Agent"]
    for name in ("authorization", "cb-access-key", "cb-access-sign", "cb-access-passphrase"):
        assert name not in req.headers
    assert gate.calls == [("coinbase", REQUEST_WEIGHT)] and REQUEST_WEIGHT == 1


def test_default_base_url_is_exchange_not_advanced_trade():
    assert DEFAULT_BASE_URL == "https://api.exchange.coinbase.com"


def test_injected_base_url_is_used():
    server = Server(ok("candles_test_usdt_1d.json"), host="mirror.test")
    batch = provider(server, base_url="https://mirror.test/").fetch_bars(request_for())
    assert len(batch.bars) == 3


def test_out_of_window_rows_dropped():
    server = Server(ok("candles_test_usdt_1d.json"))
    batch = provider(server).fetch_bars(request_for(start="2024-01-02", end="2024-01-02"))
    assert [b.session_date for b in batch.bars] == [date(2024, 1, 2)]


def test_pagination_full_page_then_short_page():
    n = MAX_BARS_PER_REQUEST
    assert n == 300
    first = [candle(JAN_1_2024 + i * DAY) for i in range(n)][::-1]  # newest-first
    second_start = JAN_1_2024 + n * DAY
    second = [candle(second_start + i * DAY) for i in range(5)][::-1]
    server = Server(json_response(first), json_response(second))
    gate = RecordingGate()
    end = date.fromordinal(date(2024, 1, 1).toordinal() + n + 4)
    batch = provider(server, quota_gate=gate).fetch_bars(request_for(end=end.isoformat()))
    assert len(batch.bars) == n + 5 and batch.next_cursor is None and batch.error is None
    assert len({b.session_date for b in batch.bars}) == n + 5
    assert [b.ts_start for b in batch.bars] == sorted(b.ts_start for b in batch.bars)
    starts = [r.url.params["start"] for r in server.requests]
    assert starts == [utc(JAN_1_2024), utc(second_start)]
    ends = [r.url.params["end"] for r in server.requests]
    assert ends == [utc(second_start), utc(second_start + 5 * DAY)]
    assert gate.calls == [("coinbase", 1)] * 2


def test_later_page_failure_returns_partial_batch_with_cursor():
    n = MAX_BARS_PER_REQUEST
    first = [candle(JAN_1_2024 + i * DAY) for i in range(n)]
    server = Server(json_response(first), httpx.Response(503, text="down"))
    batch = provider(server).fetch_bars(request_for(end="2030-01-01"))
    assert len(batch.bars) == n
    assert batch.error is not None and batch.error.code == "provider_unavailable"
    assert batch.next_cursor == str(JAN_1_2024 + n * DAY)


def test_cursor_resumes_from_given_midnight():
    server = Server(ok("candles_test_usdt_1d.json"))
    provider(server).fetch_bars(request_for(cursor=str(JAN_1_2024 + DAY)))
    assert server.requests[0].url.params["start"] == "2024-01-02T00:00:00Z"


def test_overlapping_pages_are_invalid_payload():
    n = MAX_BARS_PER_REQUEST
    first = [candle(JAN_1_2024 + i * DAY) for i in range(n)]
    repeat = [candle(JAN_1_2024 + (n - 1) * DAY)]  # server repeats the last row
    server = Server(json_response(first), json_response(repeat))
    batch = provider(server).fetch_bars(request_for(end="2025-01-01"))
    # The first page is kept; the overlapping second page is reported as the batch error.
    assert len(batch.bars) == n
    assert batch.error is not None and batch.error.code == "invalid_payload"
    assert batch.next_cursor == str(JAN_1_2024 + n * DAY)


def test_empty_history_is_an_empty_batch():
    batch = provider(Server(json_response([]))).fetch_bars(request_for())
    assert batch.bars == () and batch.error is None


def test_non_midnight_time_rejects_the_batch():
    server = Server(ok("candles_midnight_mismatch.json"))
    with pytest.raises(ProviderError) as exc:
        provider(server).fetch_bars(request_for())
    assert exc.value.code == "invalid_payload" and not exc.value.retryable


@pytest.mark.parametrize(
    "payload",
    [
        {"message": "not an array"},
        [[JAN_1_2024, 1, 2, 0.5]],  # short row
        ["not-a-row"],
        [[JAN_1_2024, "abc", 110, 100, 105, 1]],  # non-numeric price
        [[JAN_1_2024, 90, 110, 100, 105, -1]],  # negative volume
        [[JAN_1_2024, 110, 90, 100, 105, 1]],  # high < low
        [[JAN_1_2024 + 3600, 90, 110, 100, 105, 1]],  # non-midnight
        [[JAN_1_2024, 90, 110, 100, 105, 1], [JAN_1_2024, 90, 110, 100, 105, 1]],  # duplicate time
        [[1.5, 90, 110, 100, 105, 1]],  # fractional time
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
    assert gate.penalties == [("coinbase", err.retry_at)]


def test_5xx_is_provider_unavailable():
    with pytest.raises(ProviderError) as exc:
        provider(Server(httpx.Response(503, text="down"))).fetch_bars(request_for())
    assert exc.value.code == "provider_unavailable" and exc.value.retryable


def test_unknown_product_404_is_provider_rejected_request():
    body = {"message": "NotFound"}
    with pytest.raises(ProviderError) as exc:
        provider(Server(json_response(body, status=404))).fetch_bars(request_for("NOPE-USDT"))
    assert exc.value.code == "provider_rejected_request" and not exc.value.retryable
    assert exc.value.status_code == 404


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
        request_for("AAA-USDT", "BBB-USDT"),
        FetchRequest(Market.CRYPTO, Interval.D1, (), DateRange(date(2024, 1, 1), date(2024, 1, 2))),
        request_for("BAD/SYMBOL"),
        request_for("TESTUSDT"),  # not a BASE-QUOTE product id
        request_for(cursor="not-a-number"),
        request_for(cursor="1"),  # not midnight
        request_for(cursor=str(JAN_1_2024 - DAY)),  # before the window
        request_for(cursor=str(JAN_1_2024 + 30 * DAY)),  # after the window
    ],
)
def test_invalid_request(req):
    server = Server()
    with pytest.raises(ProviderError) as exc:
        provider(server).fetch_bars(req)
    assert exc.value.code == "invalid_request" and exc.value.retryable is False
    assert server.requests == []


# -- instruments / actions -------------------------------------------------------------------
def test_list_instruments_from_products():
    server = Server(ok("products.json"))
    gate = RecordingGate()
    rows = list(provider(server, quota_gate=gate).list_instruments(Market.CRYPTO))
    assert server.requests[0].url.path == "/products"
    assert gate.calls == [("coinbase", 1)]
    assert [r.symbol for r in rows] == ["TEST-USDT", "ZZZ-USDT", "HALT-USDT", "BARE-USDT"]
    test, zzz, halt, bare = rows
    assert test.market is Market.CRYPTO and test.asset_class is AssetClass.CRYPTO_SPOT
    assert test.exchange == "COINBASE" and test.native_id == "TEST-USDT" and test.active is True
    assert (test.base_asset, test.quote_asset) == ("TEST", "USDT")
    assert zzz.active is False and (zzz.base_asset, zzz.quote_asset) == ("ZZZ", "USDT")
    assert halt.active is False  # online but trading_disabled
    assert bare.active is True and (bare.base_asset, bare.quote_asset) == (None, None)


def test_list_instruments_rejects_other_markets():
    server = Server()
    with pytest.raises(ProviderError) as exc:
        provider(server).list_instruments(Market.US)
    assert exc.value.code == "invalid_request"
    assert server.requests == []


@pytest.mark.parametrize("payload", [{"products": []}, ["x"], [{"status": "online"}]])
def test_products_invalid_payload(payload):
    with pytest.raises(ProviderError) as exc:
        provider(Server(json_response(payload))).list_instruments(Market.CRYPTO)
    assert exc.value.code == "invalid_payload"


def test_corporate_actions_are_empty():
    server = Server()
    assert list(provider(server).fetch_corporate_actions(request_for())) == []
    assert server.requests == []


# -- declarations ----------------------------------------------------------------------------
def test_capabilities_and_quota_policy():
    p = CoinbaseProvider()
    assert p.id == p.provider_id == "coinbase" and p.api_version == 1
    assert p.secret_values() == ()
    (cap,) = p.capabilities()
    assert (cap.market, cap.interval) == (Market.CRYPTO, Interval.D1)
    assert cap.asset_classes == frozenset({AssetClass.CRYPTO_SPOT})
    assert cap.batch is BatchShape.PER_SYMBOL and cap.max_symbols_per_request == 1
    assert cap.max_bars_per_request == 300
    assert cap.adjusted_modes == frozenset({Adjustment.RAW})
    assert cap.history_start is None
    assert cap.feed == "exchange"
    assert cap.terms.personal_only is False and cap.terms.url
    policy = p.quota_policy()
    assert policy.provider == "coinbase" and policy.key_scope == "per_ip"
    assert policy.safety_margin == 0.9
    (limit,) = policy.limits
    assert limit.count == 10 and limit.per.total_seconds() == 1 and limit.kind == "rolling"
    assert policy.official_docs_url and policy.official_docs_url.startswith("https://")


def test_entry_point_discovery():
    from openbars.providers.registry import discover

    assert discover()["coinbase"] is CoinbaseProvider
