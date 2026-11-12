"""Massive adapter tests: synthetic fixtures and httpx.MockTransport only (no network)."""

from __future__ import annotations

import base64
import json
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import httpx
import pytest

from openbars.core.errors import ProviderError
from openbars.core.models import CorporateActionKind, DateRange, Interval, Market
from openbars.providers.adapter import NullQuotaGate
from openbars.providers.base import BatchShape, FetchRequest
from openbars.providers.us.massive import (
    MassiveProvider,
    massive_grouped_sessions,
    session_is_ready,
)
from openbars.quota.defaults import MASSIVE as MASSIVE_POLICY

FIXTURES = Path(__file__).parent / "fixtures" / "massive"
# Assembled so the source has no 16+ character credential assignment literals.
KEY = "test-key-" + "id-xx"
NOW = datetime(2024, 1, 10, 15, 0, tzinfo=timezone.utc)
NY = ZoneInfo("America/New_York")
GROUPED = "/v2/aggs/grouped/locale/us/market/stocks/"


def fixture(name: str) -> str:
    return (FIXTURES / name).read_text()


def ok(name: str) -> httpx.Response:
    return httpx.Response(200, text=fixture(name))


def request_for(*symbols: str, start="2024-01-02", end="2024-01-02", **kw) -> FetchRequest:
    return FetchRequest(
        market=Market.US,
        interval=Interval.D1,
        symbols=symbols,
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
    """Serves prepared responses in order; any unexpected host fails the test."""

    def __init__(self, *responses: httpx.Response | Exception) -> None:
        self.responses = list(responses)
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        assert request.url.host == "api.massive.com", f"unexpected host {request.url.host}"
        assert request.url.scheme == "https"
        assert self.responses, "more requests than prepared responses"
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def provider(server: Server, **kw) -> MassiveProvider:
    kw.setdefault("api_key", KEY)
    kw.setdefault("clock", lambda: NOW)
    kw.setdefault("env", {})
    return MassiveProvider(client=httpx.Client(transport=httpx.MockTransport(server)), **kw)


class FakeMaster:
    def __init__(self, known: dict[str, str]) -> None:
        self.known = known
        self.calls: list[tuple] = []

    def resolve(self, symbol, *, on, provider, market):
        self.calls.append((symbol, on, provider, market))
        return self.known.get(symbol)

    def upsert(self, instruments, mappings):  # pragma: no cover
        pass


# -- capabilities ----------------------------------------------------------------------------
def test_capabilities_and_policy():
    (cap,) = provider(Server()).capabilities()
    assert cap.provider == "massive" and cap.market is Market.US and cap.interval is Interval.D1
    assert cap.batch is BatchShape.WHOLE_MARKET_PER_DAY
    assert cap.history_start is None and cap.rolling_window == timedelta(days=730)
    assert cap.latency == timedelta(hours=8) and cap.feed == "massive:eod"
    assert cap.terms.personal_only and not cap.terms.redistribution_allowed
    assert cap.terms.url.startswith("https://")
    assert provider(Server()).quota_policy() is MASSIVE_POLICY


def test_construction_without_key_succeeds_and_fetch_fails_not_configured():
    server = Server()
    p = MassiveProvider(
        client=httpx.Client(transport=httpx.MockTransport(server)), env={}, clock=lambda: NOW
    )
    with pytest.raises(ProviderError) as err:
        p.fetch_bars(request_for())
    assert err.value.code == "not_configured" and not err.value.retryable
    assert server.requests == []


def test_key_from_environment():
    server = Server(ok("grouped_2024-01-02.json"))
    p = provider(server, api_key=None, env={"MASSIVE_API_KEY": KEY})
    p.fetch_bars(request_for())
    assert server.requests[0].url.params["apiKey"] == KEY


@pytest.mark.parametrize("kw", [{"feed": "sip"}, {"adjustment": "raw"}])
def test_rejects_other_feed_or_adjustment(kw):
    with pytest.raises(ProviderError) as err:
        provider(Server()).fetch_bars(request_for(**kw))
    assert err.value.code == "invalid_request" and not err.value.retryable


def test_accepts_prefixed_feed():
    server = Server(ok("grouped_2024-01-02.json"))
    batch = provider(server).fetch_bars(request_for(feed="massive:eod", adjustment="split"))
    assert batch.bars[0].feed == "eod"


# -- grouped daily ----------------------------------------------------------------------------
def test_grouped_session_parses_to_raw_bars():
    server = Server(ok("grouped_2024-01-02.json"))
    master = FakeMaster({"TEST1": "inst-1"})
    batch = provider(server, instrument_master=master).fetch_bars(request_for())
    assert batch.provider == "massive" and batch.error is None and batch.next_cursor is None
    assert [b.symbol_as_returned for b in batch.bars] == ["TEST1", "ZZZA"]
    bar, other = batch.bars
    assert (bar.provider, bar.feed, bar.interval) == ("massive", "eod", Interval.D1)
    assert bar.series.source.key == "massive/eod/split"
    assert bar.session_date == date(2024, 1, 2)
    assert bar.ts_start == datetime(2024, 1, 2, 21, 0, tzinfo=timezone.utc)
    assert bar.open == Decimal("10.1") and isinstance(bar.close, Decimal)
    assert bar.volume == Decimal("120000") and bar.vwap == Decimal("10.41")
    assert bar.extra == {"trade_count": 850} and bar.payload_hash
    assert other.vwap is None  # zero vwap is "unknown"
    assert bar.instrument_id == "inst-1" and other.instrument_id is None
    assert master.calls[0] == ("TEST1", date(2024, 1, 2), "massive", Market.US)

    req = server.requests[0]
    assert req.url.path == GROUPED + "2024-01-02"
    assert dict(req.url.params) == {"adjusted": "true", "include_otc": "false", "apiKey": KEY}


def test_grouped_without_master_leaves_instrument_id_none():
    batch = provider(Server(ok("grouped_2024-01-02.json"))).fetch_bars(request_for())
    assert all(b.instrument_id is None for b in batch.bars)


def test_grouped_one_request_per_session_and_skips_non_sessions():
    # Sat/Sun 2024-01-06/07 are not XNYS sessions.
    server = Server(ok("grouped_2024-01-02.json"), ok("grouped_2024-01-03.json"))
    gate = RecordingGate()
    batch = provider(server, quota_gate=gate).fetch_bars(
        request_for(start="2024-01-02", end="2024-01-03")
    )
    assert [r.url.path[-10:] for r in server.requests] == ["2024-01-02", "2024-01-03"]
    assert len(batch.bars) == 4 and gate.calls == [("massive", 1)] * 2
    weekend = Server()
    with pytest.raises(ProviderError) as err:
        provider(weekend).fetch_bars(request_for(start="2024-01-06", end="2024-01-07"))
    assert err.value.code == "invalid_request" and weekend.requests == []


def test_grouped_later_session_failure_returns_partial():
    server = Server(ok("grouped_2024-01-02.json"), httpx.Response(500, text="boom"))
    batch = provider(server).fetch_bars(request_for(start="2024-01-02", end="2024-01-03"))
    assert len(batch.bars) == 2
    assert batch.error.code == "provider_unavailable" and batch.error.retryable
    assert batch.next_cursor == "2024-01-03"
    # Resume picks up at the failed session only.
    resume = Server(ok("grouped_2024-01-03.json"))
    again = provider(resume).fetch_bars(
        request_for(start="2024-01-02", end="2024-01-03", cursor=batch.next_cursor)
    )
    assert [r.url.path[-10:] for r in resume.requests] == ["2024-01-03"]
    assert again.error is None


def test_grouped_first_session_failure_raises():
    server = Server(httpx.Response(500, text="boom"))
    with pytest.raises(ProviderError) as err:
        provider(server).fetch_bars(request_for())
    assert err.value.code == "provider_unavailable"


def test_grouped_not_ready_sessions_are_not_requested():
    now = datetime(2024, 1, 3, 4, 59, tzinfo=timezone.utc)  # 23:59 ET on Jan 2
    server = Server()
    with pytest.raises(ProviderError) as err:
        provider(server, clock=lambda: now).fetch_bars(request_for())
    assert err.value.code == "data_not_ready" and err.value.retryable
    assert server.requests == []
    # Once ready, only the ready session is requested.
    server = Server(ok("grouped_2024-01-02.json"))
    ready = now + timedelta(minutes=1)
    batch = provider(server, clock=lambda: ready).fetch_bars(
        request_for(start="2024-01-02", end="2024-01-03")
    )
    assert len(server.requests) == 1 and len(batch.bars) == 2


def test_grouped_outside_rolling_window_is_not_requested():
    server = Server()
    with pytest.raises(ProviderError) as err:
        provider(server).fetch_bars(request_for(start="2020-01-02", end="2020-01-10"))
    assert err.value.code == "invalid_request" and not err.value.retryable
    assert server.requests == []


@pytest.mark.parametrize(
    ("name", "code", "retryable"),
    [
        ("grouped_empty.json", "empty_result", True),
        ("grouped_count_mismatch.json", "result_count_mismatch", True),
        ("grouped_unadjusted.json", "adjustment_mismatch", False),
        ("grouped_status_error.json", "provider_error", True),
        ("grouped_wrong_day.json", "invalid_payload", False),
    ],
)
def test_grouped_payload_errors(name, code, retryable):
    with pytest.raises(ProviderError) as err:
        provider(Server(ok(name))).fetch_bars(request_for())
    assert err.value.code == code and err.value.retryable is retryable


def test_grouped_missing_results_is_empty_result():
    body = json.dumps({"status": "OK", "adjusted": True, "request_id": "synthetic-request-9"})
    with pytest.raises(ProviderError) as err:
        provider(Server(httpx.Response(200, text=body))).fetch_bars(request_for())
    assert err.value.code == "empty_result" and err.value.retryable


def test_grouped_bad_bar_values_are_invalid_payload():
    body = json.loads(fixture("grouped_2024-01-02.json"))
    body["results"][0]["h"] = 1  # high below low
    with pytest.raises(ProviderError) as err:
        provider(Server(httpx.Response(200, json=body))).fetch_bars(request_for())
    assert err.value.code == "invalid_payload"


def test_403_end_of_day_is_data_not_ready_and_redacts_secrets():
    text = f"You requested today's data before end of day. key={KEY}"
    server = Server(httpx.Response(403, text=text, headers={"x-request-id": "synthetic-id-1"}))
    with pytest.raises(ProviderError) as err:
        provider(server).fetch_bars(request_for())
    exc = err.value
    assert exc.code == "data_not_ready" and exc.retryable and exc.status_code == 403
    assert KEY not in str(exc) and KEY not in (exc.raw_payload or "")
    assert "[REDACTED]" in exc.raw_payload


def test_plain_403_is_not_retryable():
    with pytest.raises(ProviderError) as err:
        provider(Server(httpx.Response(403, text="not entitled"))).fetch_bars(request_for())
    assert err.value.code == "provider_rejected_request" and not err.value.retryable


def test_http_status_mapping():
    cases = [(429, "rate_limited", True), (500, "provider_unavailable", True), (404, "provider_rejected_request", False)]
    for status, code, retryable in cases:
        with pytest.raises(ProviderError) as err:
            provider(Server(httpx.Response(status, text="x"))).fetch_bars(request_for())
        assert (err.value.code, err.value.retryable) == (code, retryable)


def test_retry_after_is_honoured():
    server = Server(httpx.Response(429, text="slow", headers={"Retry-After": "7"}))
    with pytest.raises(ProviderError) as err:
        provider(server).fetch_bars(request_for())
    assert err.value.retry_at is not None


def test_transport_error_never_leaks_key():
    server = Server(httpx.ConnectError(f"cannot reach {KEY}"))
    with pytest.raises(ProviderError) as err:
        provider(server).fetch_bars(request_for())
    assert err.value.code == "transport_error" and KEY not in str(err.value)


# -- ticker daily -----------------------------------------------------------------------------
def test_ticker_range_paginates_via_next_url():
    server = Server(ok("range_page1.json"), ok("range_page2.json"))
    gate = RecordingGate()
    batch = provider(server, quota_gate=gate).fetch_bars(
        request_for("TEST1", start="2024-01-02", end="2024-01-05")
    )
    assert [b.session_date.day for b in batch.bars] == [2, 3, 4, 5]
    bar = batch.bars[0]
    assert bar.series.source.key == "massive/eod/split" and bar.symbol_as_returned == "TEST1"
    assert bar.ts_start == datetime(2024, 1, 2, 5, 0, tzinfo=timezone.utc)
    assert isinstance(bar.open, Decimal)
    first, second = server.requests
    assert first.url.path == "/v2/aggs/ticker/TEST1/range/1/day/2024-01-02/2024-01-05"
    assert dict(first.url.params) == {
        "adjusted": "true", "sort": "asc", "limit": "50000", "apiKey": KEY,
    }
    assert dict(second.url.params) == {"cursor": "synthetic-cursor-1", "apiKey": KEY}
    assert len(gate.calls) == 2


def test_next_url_key_is_replaced_by_configured_key():
    page = json.loads(fixture("range_page1.json"))
    page["next_url"] += "&" + urlencode({"apiKey": "other-key-" + "value-1"})
    server = Server(httpx.Response(200, json=page), ok("range_page2.json"))
    provider(server).fetch_bars(request_for("TEST1", start="2024-01-02", end="2024-01-05"))
    query = server.requests[1].url.query.decode()
    assert query.count("apiKey") == 1 and "other-key" not in query and KEY in query


def test_ticker_multiple_symbols_one_chain_each():
    server = Server(ok("range_page2.json"), ok("range_zzza.json"))
    # range_page2 holds Jan 4-5; request spans the whole week.
    batch = provider(server).fetch_bars(request_for("TEST1", "ZZZA", start="2024-01-02", end="2024-01-05"))
    assert [b.symbol_as_returned for b in batch.bars] == ["TEST1", "TEST1", "ZZZA"]
    assert [r.url.path.split("/")[4] for r in server.requests] == ["TEST1", "ZZZA"]


def test_ticker_repeated_next_url_is_loop():
    page = json.loads(fixture("range_page1.json"))
    server = Server(httpx.Response(200, json=page), httpx.Response(200, json=page))
    # Earlier pages already produced bars -> partial batch rather than a raise.
    batch = provider(server).fetch_bars(request_for("TEST1", start="2024-01-02", end="2024-01-05"))
    assert batch.error.code == "pagination_loop" and not batch.error.retryable
    assert len(server.requests) == 2 and len(batch.bars) == 4


@pytest.mark.parametrize(
    "bad",
    [
        "http://api.massive.com/v2/aggs/ticker/TEST1/range/1/day/1/2?cursor=x",
        "https://evil.example/v2/aggs/ticker/TEST1/range/1/day/1/2?cursor=x",
        "https://api.massive.com.evil.example/v2/aggs/ticker/TEST1/range/1/day/1/2?cursor=x",
        "https://user:pw@api.massive.com/v2/aggs/ticker/TEST1/range/1/day/1/2?cursor=x",
        "https://api.massive.com:8443/v2/aggs/ticker/TEST1/range/1/day/1/2?cursor=x",
        "https://api.massive.com/v2/aggs/ticker/ZZZA/range/1/day/1/2?cursor=x",
        "https://api.massive.com/v3/reference/tickers?cursor=x",
        "https://api.massive.com/v2/aggs/ticker/TEST1/range/1/day/1/2/3?cursor=x",
        "https://api.massive.com:bad/v2/aggs/ticker/TEST1/range/1/day/1/2",
    ],
)
def test_untrusted_next_url_rejected(bad):
    page = json.loads(fixture("range_page1.json"))
    page["next_url"] = bad
    server = Server(httpx.Response(200, json=page))
    batch = provider(server).fetch_bars(request_for("TEST1", start="2024-01-02", end="2024-01-05"))
    assert batch.error.code == "invalid_pagination_url" and not batch.error.retryable
    assert len(server.requests) == 1 and len(batch.bars) == 2


def test_untrusted_next_url_on_first_page_raises():
    page = json.loads(fixture("range_empty.json"))
    page["next_url"] = "https://evil.example/v2/aggs/ticker/TEST1/range/1/day/1/2"
    with pytest.raises(ProviderError) as err:
        provider(Server(httpx.Response(200, json=page))).fetch_bars(
            request_for("TEST1", start="2024-01-02", end="2024-01-05")
        )
    assert err.value.code == "invalid_pagination_url"


def test_ticker_mismatch_and_count_mismatch_and_window():
    with pytest.raises(ProviderError) as err:
        provider(Server(ok("range_wrong_ticker.json"))).fetch_bars(request_for("TEST1"))
    assert err.value.code == "invalid_payload"

    page = json.loads(fixture("range_page2.json"))
    page["resultsCount"] = 5
    with pytest.raises(ProviderError) as err:
        provider(Server(httpx.Response(200, json=page))).fetch_bars(
            request_for("TEST1", start="2024-01-02", end="2024-01-05")
        )
    assert err.value.code == "result_count_mismatch"

    with pytest.raises(ProviderError) as err:  # Jan 4-5 outside a Jan 2-3 window
        provider(Server(ok("range_page2.json"))).fetch_bars(
            request_for("TEST1", start="2024-01-02", end="2024-01-03")
        )
    assert err.value.code == "invalid_payload"


def test_ticker_unadjusted_rejected():
    page = json.loads(fixture("range_page2.json"))
    page["adjusted"] = False
    with pytest.raises(ProviderError) as err:
        provider(Server(httpx.Response(200, json=page))).fetch_bars(
            request_for("TEST1", start="2024-01-02", end="2024-01-05")
        )
    assert err.value.code == "adjustment_mismatch" and not err.value.retryable


def test_ticker_all_empty_is_empty_result_not_retryable():
    server = Server(ok("range_empty.json"))
    with pytest.raises(ProviderError) as err:
        provider(server).fetch_bars(request_for("TEST1"))
    assert err.value.code == "empty_result" and not err.value.retryable


def test_ticker_empty_symbol_is_skipped_when_another_has_bars():
    empty_zzza = json.loads(fixture("range_empty.json"))
    empty_zzza["ticker"] = "ZZZA"
    server = Server(ok("range_page2.json"), httpx.Response(200, json=empty_zzza))
    batch = provider(server).fetch_bars(
        request_for("TEST1", "ZZZA", start="2024-01-02", end="2024-01-05")
    )
    assert len(batch.bars) == 2 and batch.error is None


def test_ticker_later_symbol_failure_is_partial_and_resumable():
    server = Server(ok("range_page2.json"), httpx.Response(500, text="boom"))
    batch = provider(server).fetch_bars(
        request_for("TEST1", "ZZZA", start="2024-01-02", end="2024-01-05")
    )
    assert len(batch.bars) == 2 and batch.error.code == "provider_unavailable"
    decoded = json.loads(base64.urlsafe_b64decode(batch.next_cursor + "=" * (-len(batch.next_cursor) % 4)))
    assert decoded == {"i": 1, "u": None}
    resume = Server(ok("range_zzza.json"))
    again = provider(resume).fetch_bars(
        request_for("TEST1", "ZZZA", start="2024-01-02", end="2024-01-05", cursor=batch.next_cursor)
    )
    assert [r.url.path.split("/")[4] for r in resume.requests] == ["ZZZA"]
    assert len(again.bars) == 1


def test_ticker_later_page_failure_cursor_keeps_clean_url():
    server = Server(ok("range_page1.json"), httpx.Response(429, text="slow"))
    batch = provider(server).fetch_bars(
        request_for("TEST1", start="2024-01-02", end="2024-01-05")
    )
    assert len(batch.bars) == 2 and batch.error.code == "rate_limited"
    decoded = json.loads(base64.urlsafe_b64decode(batch.next_cursor + "=" * (-len(batch.next_cursor) % 4)))
    assert decoded["i"] == 0 and "apiKey" not in decoded["u"] and KEY not in decoded["u"]
    resume = Server(ok("range_page2.json"))
    again = provider(resume).fetch_bars(
        request_for("TEST1", start="2024-01-02", end="2024-01-05", cursor=batch.next_cursor)
    )
    assert [b.session_date.day for b in again.bars] == [4, 5]
    assert resume.requests[0].url.params["cursor"] == "synthetic-cursor-1"


def test_malformed_cursor_rejected():
    with pytest.raises(ProviderError) as err:
        provider(Server()).fetch_bars(request_for("TEST1", cursor="not-a-cursor"))
    assert err.value.code == "invalid_request"


def test_ticker_applies_instrument_master_per_bar_date():
    master = FakeMaster({"TEST1": "inst-1"})
    server = Server(ok("range_page2.json"))
    batch = provider(server, instrument_master=master).fetch_bars(
        request_for("TEST1", start="2024-01-02", end="2024-01-05")
    )
    assert {b.instrument_id for b in batch.bars} == {"inst-1"}
    assert [c[1].day for c in master.calls] == [4, 5]


# -- window helpers -----------------------------------------------------------------------------
def test_session_is_ready_boundary_in_new_york():
    session = date(2024, 1, 2)
    before = datetime(2024, 1, 2, 23, 59, 59, tzinfo=NY)
    after = datetime(2024, 1, 3, 0, 0, 0, tzinfo=NY)
    assert not session_is_ready(session, before)
    assert session_is_ready(session, after)
    assert session_is_ready(session, after.astimezone(timezone.utc))
    assert not session_is_ready(session, datetime(2024, 1, 3, 4, 59, tzinfo=timezone.utc))
    with pytest.raises(ValueError):
        session_is_ready(session, datetime(2024, 1, 3))


def test_massive_grouped_sessions_one_job_per_in_window_ready_uncovered_day():
    as_of = date(2024, 1, 10)
    now = datetime(2024, 1, 10, 10, 0, tzinfo=NY)  # Jan 9 is ready, Jan 10 is not
    window = DateRange(date(2021, 12, 1), date(2024, 1, 12))
    jobs = massive_grouped_sessions(
        window, as_of=as_of, now=now, covered=[date(2024, 1, 3)]
    )
    floor = as_of - timedelta(days=730)
    assert jobs == sorted(jobs) and len(jobs) == len(set(jobs))
    assert jobs[0] >= floor and jobs[-1] == date(2024, 1, 9)
    assert date(2024, 1, 10) not in jobs and date(2024, 1, 3) not in jobs
    assert date(2021, 12, 1) not in jobs  # older than as_of - 730d
    assert date(2024, 1, 6) not in jobs and date(2024, 1, 1) not in jobs  # weekend / holiday
    assert date(2024, 1, 2) in jobs and date(2024, 1, 4) in jobs

    small = massive_grouped_sessions(
        DateRange(date(2024, 1, 2), date(2024, 1, 5)), as_of=as_of, now=now
    )
    assert small == [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4), date(2024, 1, 5)]


def test_massive_grouped_sessions_edge_cases():
    now = datetime(2024, 1, 10, 10, 0, tzinfo=NY)
    too_old = DateRange(date(2020, 1, 2), date(2020, 1, 10))
    assert massive_grouped_sessions(too_old, as_of=date(2024, 1, 10), now=now) == []
    future = DateRange(date(2024, 1, 10), date(2024, 1, 12))
    assert massive_grouped_sessions(future, as_of=date(2024, 1, 10), now=now) == []
    explicit = massive_grouped_sessions(
        DateRange(date(2024, 1, 2), date(2024, 1, 9)),
        as_of=date(2024, 1, 10),
        now=now,
        sessions=[date(2024, 1, 9), date(2024, 1, 4), date(2023, 12, 1)],
    )
    assert explicit == [date(2024, 1, 4), date(2024, 1, 9)]


# -- corporate actions ----------------------------------------------------------------------------
def test_splits_map_and_paginate():
    server = Server(ok("splits_page1.json"), ok("splits_page2.json"))
    gate = RecordingGate()
    result = provider(server, quota_gate=gate).fetch_corporate_actions_result(
        request_for(start="2024-01-01", end="2024-01-31")
    )
    actions = result.actions
    assert [(a.instrument_id, a.kind, a.ratio) for a in actions] == [
        ("TEST1", CorporateActionKind.SPLIT, Decimal("4")),
        ("ZZZA", CorporateActionKind.SPLIT, Decimal("0.1")),
        ("TEST1", CorporateActionKind.DIVIDEND, Decimal("1.05")),
    ]
    assert actions[0].ex_date == date(2024, 1, 3) and actions[0].source == "massive"
    assert dict(result.skipped) == {"something_else:unsupported_type": 1}
    first, second = server.requests
    assert first.url.path == "/stocks/v1/splits"
    assert dict(first.url.params) == {
        "execution_date.gte": "2024-01-01",
        "execution_date.lte": "2024-01-31",
        "limit": "1000",
        "sort": "execution_date.asc",
        "apiKey": KEY,
    }
    assert dict(second.url.params) == {"cursor": "synthetic-cursor-2", "limit": "1000", "apiKey": KEY}
    assert len(gate.calls) == 2


def test_parsed_splits_are_mapped_to_stable_ids_by_the_ingest_helper():
    from openbars.core.models import AssetClass, Instrument, Market, SymbolMapping
    from openbars.normalize.instruments import MemoryInstrumentMaster, ingest_corporate_actions

    actions = provider(Server(ok("splits_page1.json"), ok("splits_page2.json"))).fetch_corporate_actions_result(
        request_for(start="2024-01-01", end="2024-01-31")
    ).actions
    master = MemoryInstrumentMaster()
    master.upsert(
        [Instrument("us:eq:T1", Market.US, AssetClass.EQUITY, "USD")],
        [SymbolMapping("us:eq:T1", "TEST1", date(2020, 1, 1))],
    )
    summary = ingest_corporate_actions(actions, master)
    assert (summary.resolved, summary.unresolved) == (2, 1)  # two TEST1 rows, one unknown ZZZA row
    assert {a.instrument_id for a in master.corporate_actions()} == {"us:eq:T1"}
    assert [r.symbol for r in master.unresolved("corporate_action")] == ["ZZZA"]


def test_splits_per_symbol_and_missing_ratio():
    body = {
        "status": "OK",
        "results": [
            {"ticker": "TEST1", "execution_date": "2024-01-03", "adjustment_type": "forward_split"}
        ],
    }
    server = Server(httpx.Response(200, json=body), httpx.Response(200, json={"status": "OK"}))
    actions = list(
        provider(server).fetch_corporate_actions(request_for("TEST1", "ZZZA", start="2024-01-01", end="2024-01-31"))
    )
    assert len(actions) == 1 and actions[0].ratio is None
    assert [r.url.params["ticker"] for r in server.requests] == ["TEST1", "ZZZA"]


def test_splits_partial_result_keeps_first_page():
    server = Server(ok("splits_page1.json"), httpx.Response(500, text="boom"))
    result = provider(server).fetch_corporate_actions_result(request_for())
    assert len(result.actions) == 2 and result.error.code == "provider_unavailable"
    assert result.next_cursor
    with pytest.raises(ProviderError):
        provider(Server(ok("splits_page1.json"), httpx.Response(500, text="boom"))).fetch_corporate_actions(
            request_for()
        )


def test_splits_reject_untrusted_next_url():
    page = json.loads(fixture("splits_page1.json"))
    page["next_url"] = "https://api.massive.com/v2/aggs/ticker/TEST1/range/1/day/1/2"
    with pytest.raises(ProviderError) as err:
        list(provider(Server(httpx.Response(200, json=page))).fetch_corporate_actions(request_for()))
    assert err.value.code == "invalid_pagination_url"


def test_ticker_events_map_symbol_change():
    server = Server(ok("ticker_events.json"))
    actions = list(
        provider(server).fetch_corporate_actions(
            request_for("TEST1", start="2024-01-01", end="2024-01-31", feed="ticker_events")
        )
    )
    assert len(actions) == 1
    assert (actions[0].kind, actions[0].ex_date, actions[0].instrument_id) == (
        CorporateActionKind.SYMBOL_CHANGE, date(2024, 1, 4), "TEST1",
    )
    req = server.requests[0]
    assert req.url.path == "/vX/reference/tickers/TEST1/events"
    assert dict(req.url.params) == {"types": "ticker_change", "apiKey": KEY}


def test_ticker_events_need_symbols_and_dataset_must_be_known():
    for kw in ({"feed": "ticker_events"},):
        with pytest.raises(ProviderError) as err:
            list(provider(Server()).fetch_corporate_actions(request_for(**kw)))
        assert err.value.code == "invalid_request"
    with pytest.raises(ProviderError) as err:
        list(provider(Server()).fetch_corporate_actions(request_for("TEST1", feed="dividends")))
    assert err.value.code == "invalid_request"


def test_corporate_actions_missing_key_only_fails_on_fetch():
    p = MassiveProvider(client=httpx.Client(transport=httpx.MockTransport(Server())), env={})
    with pytest.raises(ProviderError) as err:
        list(p.fetch_corporate_actions(request_for()))
    assert err.value.code == "not_configured" and not err.value.retryable


# -- reference lists ---------------------------------------------------------------------------------
def test_list_instruments_two_pages():
    server = Server(ok("tickers_page1.json"), ok("tickers_page2.json"))
    rows = list(provider(server).list_instruments(Market.US, on=date(2024, 1, 2)))
    assert [(r.symbol, r.name, r.exchange, r.active) for r in rows] == [
        ("TEST1", "Test One Corp", "XNAS", True),
        ("ZZZA", "Zed Corp", "XNYS", True),
    ]
    assert all(r.market is Market.US for r in rows)
    first, second = server.requests
    assert first.url.path == "/v3/reference/tickers"
    assert dict(first.url.params) == {
        "market": "stocks", "locale": "us", "type": "CS", "active": "true", "order": "asc",
        "sort": "ticker", "limit": "1000", "date": "2024-01-02", "apiKey": KEY,
    }
    assert dict(second.url.params) == {"cursor": "synthetic-cursor-3", "limit": "1000", "apiKey": KEY}


def test_list_instruments_empty_is_retryable_and_market_checked():
    with pytest.raises(ProviderError) as err:
        provider(Server(ok("tickers_empty.json"))).list_instruments(Market.US)
    assert err.value.code == "empty_result" and err.value.retryable
    with pytest.raises(ProviderError) as err:
        provider(Server()).list_instruments(Market.CN)
    assert err.value.code == "unsupported_request"


def test_list_delisted_uses_active_false():
    server = Server(ok("tickers_page2.json"))
    rows = list(provider(server).list_delisted())
    assert server.requests[0].url.params["active"] == "false"
    assert rows[0].active is False


# -- plumbing ---------------------------------------------------------------------------------------
def test_default_gate_is_null_gate():
    assert isinstance(provider(Server())._quota_gate, NullQuotaGate)


def test_gate_called_once_per_grouped_get():
    gate = RecordingGate()
    provider(Server(ok("grouped_2024-01-02.json")), quota_gate=gate).fetch_bars(request_for())
    assert gate.calls == [("massive", 1)]


def test_entry_point_discovery():
    from openbars.providers.registry import discover

    assert discover()["massive"] is MassiveProvider
