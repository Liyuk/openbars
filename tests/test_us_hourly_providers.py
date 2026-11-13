"""US 1h adapters (#115): Massive, Alpaca, yfinance, Twelve Data. Synthetic data, no network."""

from __future__ import annotations

import base64
import json
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

import httpx
import pytest

from openbars.core.errors import ProviderError
from openbars.core.models import DateRange, Interval, Market
from openbars.core.series import SessionSemantics
from openbars.providers.adapter import NullQuotaGate
from openbars.providers.base import FetchRequest
from openbars.providers.us import twelve_data as twelve_data_module
from openbars.providers.us.alpaca import AlpacaProvider
from openbars.providers.us.hourly import HourlyGrid, HourlyOptions, clip_hourly_window
from openbars.providers.us.massive import MassiveProvider
from openbars.providers.us.twelve_data import TwelveDataProvider
from openbars.providers.us.yfinance import YFinanceProvider

NY = ZoneInfo("America/New_York")
UTC = timezone.utc
NOW = datetime(2024, 1, 10, 15, 0, tzinfo=UTC)
DAY = date(2024, 1, 2)
# Assembled so the source carries no credential-looking literal.
KEY = "hourly-" + "test-key"


def ny(day: date, hh: int, mm: int) -> datetime:
    return datetime(day.year, day.month, day.day, hh, mm, tzinfo=NY).astimezone(UTC)


def req(*symbols: str, start=DAY, end=DAY, **kw) -> FetchRequest:
    return FetchRequest(
        market=Market.US, interval=Interval.H1, symbols=symbols, window=DateRange(start, end), **kw
    )


def half_hours(day: date, start=(4, 0), end=(20, 0)):
    """Synthetic 30-minute aggregates: price rises by 1 cent per bucket, volume 100."""
    t = ny(day, *start)
    stop = ny(day, *end)
    i = 0
    while t < stop:
        price = Decimal("100") + Decimal(i) / 100
        yield t, {
            "o": str(price),
            "h": str(price + Decimal("0.5")),
            "l": str(price - Decimal("0.5")),
            "c": str(price + Decimal("0.01")),
            "v": 100,
            "vw": str(price),
            "n": 3,
        }
        t += timedelta(minutes=30)
        i += 1


# -- shared rules -------------------------------------------------------------------------------
def test_clip_window_rolling_and_history_start():
    w = clip_hourly_window(
        DateRange(date(2021, 1, 1), date(2030, 1, 1)), provider="x", now=NOW, rolling_days=730
    )
    assert w == DateRange(date(2022, 1, 10), date(2024, 1, 10))
    w = clip_hourly_window(
        DateRange(date(2010, 1, 1), DAY), provider="x", now=NOW, history_start=date(2016, 1, 1)
    )
    assert w.start == date(2016, 1, 1)
    with pytest.raises(ProviderError) as err:
        clip_hourly_window(DateRange(date(2020, 1, 1), date(2020, 2, 1)), provider="x", now=NOW, rolling_days=730)
    assert err.value.code == "outside_free_window" and not err.value.retryable


def test_grid_regular_default_and_early_close():
    grid = HourlyGrid(DateRange(date(2024, 7, 3), date(2024, 7, 3)), HourlyOptions())
    starts = [s.ts_start.astimezone(NY).strftime("%H:%M") for s in grid.slots]
    assert starts == ["09:30", "10:30", "11:30", "12:30"]  # half day closes 13:00
    assert grid.slots[-1].ts_end == ny(date(2024, 7, 3), 13, 0)


# -- Massive ------------------------------------------------------------------------------------
class MassiveServer:
    def __init__(self, days=(DAY,), fail_for=()):
        self.days = days
        self.fail_for = set(fail_for)
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        symbol = request.url.path.split("/")[4]
        if symbol in self.fail_for:
            return httpx.Response(429, text="slow down")
        results = [{"t": int(t.timestamp() * 1000), **o} for d in self.days for t, o in half_hours(d)]
        body = {
            "ticker": symbol,
            "adjusted": True,
            "status": "OK",
            "resultsCount": len(results),
            "results": results,
        }
        return httpx.Response(200, json=body)


def massive(server, now=NOW) -> MassiveProvider:
    return MassiveProvider(
        api_key=KEY,
        client=httpx.Client(transport=httpx.MockTransport(server)),
        clock=lambda: now,
        fetch_id_factory=lambda: "f1",
    )


def test_massive_hourly_session_open_regular_only():
    server = MassiveServer()
    batch = massive(server).fetch_bars(req("ZZZA"))
    (request,) = server.requests
    assert "/v2/aggs/ticker/ZZZA/range/30/minute/2024-01-02/2024-01-02" in str(request.url)
    assert request.url.params["adjusted"] == "true"
    bars = batch.bars
    assert [b.ts_start.astimezone(NY).strftime("%H:%M") for b in bars] == [
        "09:30", "10:30", "11:30", "12:30", "13:30", "14:30", "15:30"
    ]
    first, last = bars[0], bars[-1]
    assert first.interval is Interval.H1 and first.session_date == DAY
    assert (first.provider, first.feed) == ("massive", "aggs")
    assert first.series.source.key == "massive/aggs/split"
    assert first.series.session_semantics is SessionSemantics.REGULAR
    # 09:30 and 10:00 buckets are indices 11 and 12 counted from 04:00
    assert first.open == Decimal("100.11") and first.close == Decimal("100.13")
    assert first.high == Decimal("100.62") and first.low == Decimal("99.61")
    assert first.volume == Decimal(200) and first.vwap == Decimal("100.115")
    assert first.extra == {
        "session": "regular",
        "ts_end": ny(DAY, 10, 30).isoformat(),
        "trade_count": 6,
        "source_bars": 2,
    }
    assert last.volume == Decimal(100)  # 15:30-16:00 is one bucket
    assert KEY not in repr(bars)


def test_massive_hourly_clock_hour_and_extended():
    batch = massive(MassiveServer()).fetch_bars(
        req("ZZZA", hourly_alignment="clock_hour", include_extended_hours=True)
    )
    starts = [b.ts_start.astimezone(NY).strftime("%H:%M") for b in batch.bars]
    assert starts[:7] == ["04:00", "05:00", "06:00", "07:00", "08:00", "09:00", "09:30"]
    assert starts[-1] == "19:00" and len(starts) == 17
    sessions = [b.extra["session"] for b in batch.bars]
    assert sessions[5] == "pre" and sessions[6] == "regular" and sessions[-1] == "post"
    assert batch.bars[5].volume == Decimal(100)  # 09:00-09:30 stub
    assert batch.bars[0].series.session_semantics is SessionSemantics.EXTENDED


def test_massive_hourly_outside_window_sends_nothing_and_clips():
    server = MassiveServer()
    with pytest.raises(ProviderError) as err:
        massive(server).fetch_bars(req("ZZZA", start=date(2020, 1, 1), end=date(2020, 1, 31)))
    assert err.value.code == "outside_free_window" and server.requests == []
    massive(server).fetch_bars(req("ZZZA", start=date(2021, 1, 1), end=DAY))
    assert "/range/30/minute/2022-01-10/2024-01-02" in str(server.requests[0].url)


def test_massive_hourly_needs_symbols_and_drops_unfinished_slots():
    with pytest.raises(ProviderError) as err:
        massive(MassiveServer()).fetch_bars(req())
    assert err.value.code == "invalid_request"
    during = ny(DAY, 12, 0)  # 11:30 slot is still open
    batch = massive(MassiveServer(), now=during).fetch_bars(req("ZZZA", end=DAY))
    assert [b.ts_start.astimezone(NY).strftime("%H:%M") for b in batch.bars] == ["09:30", "10:30"]


def test_massive_hourly_partial_resumes_at_symbol():
    server = MassiveServer(fail_for={"ZZZB"})
    batch = massive(server).fetch_bars(req("ZZZA", "ZZZB"))
    assert {b.symbol_as_returned for b in batch.bars} == {"ZZZA"}
    assert batch.error is not None and batch.error.code == "rate_limited"
    cursor = json.loads(base64.urlsafe_b64decode(batch.next_cursor + "=="))
    assert cursor == {"i": 1, "u": None}
    resumed = massive(MassiveServer()).fetch_bars(req("ZZZA", "ZZZB", cursor=batch.next_cursor))
    assert {b.symbol_as_returned for b in resumed.bars} == {"ZZZB"}


# -- Alpaca -------------------------------------------------------------------------------------
class AlpacaServer:
    def __init__(self, page_size=10):
        self.page_size = page_size
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        symbols = request.url.params["symbols"].split(",")
        rows = [
            (s, {"t": t.strftime("%Y-%m-%dT%H:%M:%SZ"), **o})
            for s in symbols
            for t, o in half_hours(DAY)
        ]
        page = int(request.url.params.get("page_token") or 0)
        chunk = rows[page * self.page_size : (page + 1) * self.page_size]
        bars: dict[str, list] = {}
        for s, o in chunk:
            bars.setdefault(s, []).append(o)
        more = (page + 1) * self.page_size < len(rows)
        return httpx.Response(200, json={"bars": bars, "next_page_token": str(page + 1) if more else None})


class RecordingGate(NullQuotaGate):
    def __init__(self):
        self.calls = 0

    def acquire(self, provider, weight=1, *, scope="default", timeout=None):
        self.calls += 1
        return super().acquire(provider, weight, scope=scope)


def alpaca(server, now=NOW, gate=None) -> AlpacaProvider:
    return AlpacaProvider(
        api_key=KEY,
        api_secret=KEY + "-secret",
        client=httpx.Client(transport=httpx.MockTransport(server)),
        clock=lambda: now,
        quota_gate=gate,
        fetch_id_factory=lambda: "f1",
    )


def test_alpaca_hourly_pages_and_aggregates_per_symbol():
    server, gate = AlpacaServer(), RecordingGate()
    batch = alpaca(server, gate=gate).fetch_bars(req("ZZZA", "ZZZB"))
    params = server.requests[0].url.params
    assert params["timeframe"] == "30Min" and params["feed"] == "sip"
    assert params["start"] == "2024-01-02T14:30:00Z" and params["end"] == "2024-01-02T21:00:00Z"
    assert gate.calls == len(server.requests) > 1  # every page took a quota token
    by = {}
    for b in batch.bars:
        by.setdefault(b.symbol_as_returned, []).append(b)
    assert {k: len(v) for k, v in by.items()} == {"ZZZA": 7, "ZZZB": 7}
    bar = by["ZZZB"][0]
    assert bar.series.source.key == "alpaca/sip/split" and bar.series.interval is Interval.H1
    assert bar.volume == Decimal(200) and bar.extra["session"] == "regular"


def test_alpaca_hourly_skips_unrequested_bars_and_reports_count(caplog):
    rows = [{"t": t.isoformat(), **row} for t, row in half_hours(DAY)]
    def server(request):
        return httpx.Response(200, json={"bars": {"ZZZA": rows, "ZZZX": rows[:2]}})
    batch = alpaca(server).fetch_bars(req("ZZZA"))
    assert len(batch.bars) == 7
    assert {bar.symbol_as_returned for bar in batch.bars} == {"ZZZA"}
    assert "2" in caplog.text and "ZZZX" not in caplog.text


@pytest.mark.parametrize("items", [None, [{"t": "invalid"}]])
def test_alpaca_hourly_rejects_malformed_unrequested_bars(items):
    def server(request):
        return httpx.Response(200, json={"bars": {"ZZZX": items}})
    with pytest.raises(ProviderError) as info:
        alpaca(server).fetch_bars(req("ZZZA"))
    assert info.value.code == "invalid_payload"


def test_alpaca_hourly_sip_delay_limits_end():
    during = ny(DAY, 12, 0)
    server = AlpacaServer(page_size=1000)
    batch = alpaca(server, now=during).fetch_bars(req("ZZZA"))
    assert server.requests[0].url.params["end"] == "2024-01-02T16:44:00Z"
    assert [b.ts_start.astimezone(NY).strftime("%H:%M") for b in batch.bars] == ["09:30", "10:30"]


# -- yfinance -----------------------------------------------------------------------------------
class FakeHourlyDownload:
    def __init__(self):
        self.calls = []

    def __call__(self, ticker, **kwargs):
        self.calls.append((ticker, kwargs))
        rows = []
        for hh, mm in [(9, 0), (9, 30), (10, 30), (15, 30), (16, 0)]:
            t = datetime(DAY.year, DAY.month, DAY.day, hh, mm, tzinfo=NY)
            rows.append({"Datetime": t, "Open": 10.1, "High": 10.5, "Low": 9.9, "Close": 10.2, "Volume": 1000})
        return rows


def test_yfinance_hourly_keeps_grid_bars_only():
    download = FakeHourlyDownload()
    p = YFinanceProvider(download, clock=lambda: NOW, fetch_id_factory=lambda: "f1")
    result = p.fetch_bars_result(req("ZZZ.A"))
    ticker, kwargs = download.calls[0]
    assert ticker == "ZZZ-A" and kwargs["interval"] == "1h" and kwargs["prepost"] is False
    assert [b.ts_start.astimezone(NY).strftime("%H:%M") for b in result.batch.bars] == [
        "09:30", "10:30", "15:30"
    ]
    assert result.skipped == {"off_grid": 2}
    bar = result.batch.bars[0]
    assert bar.interval is Interval.H1 and bar.series.source.key == "yfinance/yahoo/split"
    assert bar.open == Decimal("10.1") and bar.extra["session"] == "regular"


def test_yfinance_hourly_rejects_clock_hour_and_old_windows():
    p = YFinanceProvider(FakeHourlyDownload(), clock=lambda: NOW)
    with pytest.raises(ProviderError) as err:
        p.fetch_bars(req("ZZZA", hourly_alignment="clock_hour"))
    assert err.value.code == "unsupported_request"
    with pytest.raises(ProviderError) as err:
        p.fetch_bars(req("ZZZA", start=date(2021, 1, 1), end=date(2021, 6, 1)))
    assert err.value.code == "outside_free_window"


@pytest.mark.parametrize("failure", [ProviderError("timeout", "timeout", retryable=True), TimeoutError()])
def test_yfinance_hourly_first_retryable_failure_raises(failure):
    download = FakeHourlyDownload()

    def fail_first(ticker, **kwargs):
        if ticker == "ZZZA":
            raise failure
        return download(ticker, **kwargs)

    p = YFinanceProvider(fail_first, clock=lambda: NOW)
    with pytest.raises(ProviderError) as err:
        p.fetch_bars_result(req("ZZZA", "ZZZB"))
    assert err.value.retryable
    assert download.calls == []


@pytest.mark.parametrize("failure", [ProviderError("timeout", "timeout", retryable=True), TimeoutError()])
def test_yfinance_hourly_retryable_failure_resumes_failed_symbol(failure):
    download = FakeHourlyDownload()

    def fail_second(ticker, **kwargs):
        if ticker == "ZZZB":
            raise failure
        return download(ticker, **kwargs)

    p = YFinanceProvider(fail_second, clock=lambda: NOW)
    result = p.fetch_bars_result(req("ZZZA", "ZZZB"))
    assert {b.symbol_as_returned for b in result.batch.bars} == {"ZZZA"}
    assert result.batch.next_cursor == "1"
    assert result.symbol_errors["ZZZB"].retryable

    resumed_download = FakeHourlyDownload()
    resumed = YFinanceProvider(resumed_download, clock=lambda: NOW).fetch_bars_result(
        req("ZZZA", "ZZZB", cursor=result.batch.next_cursor)
    )
    assert {b.symbol_as_returned for b in resumed.batch.bars} == {"ZZZB"}
    assert [ticker for ticker, _ in resumed_download.calls] == ["ZZZB"]


@pytest.mark.parametrize("failure", [None, ValueError("unavailable"), ProviderError("provider_unavailable", "unavailable", retryable=False)])
def test_yfinance_hourly_nonretryable_failure_continues(failure):
    download = FakeHourlyDownload()

    def fail_first(ticker, **kwargs):
        if ticker == "ZZZA":
            if failure is not None:
                raise failure
            return []
        return download(ticker, **kwargs)

    result = YFinanceProvider(fail_first, clock=lambda: NOW).fetch_bars_result(req("ZZZA", "ZZZB"))
    assert {b.symbol_as_returned for b in result.batch.bars} == {"ZZZB"}
    assert not result.symbol_errors["ZZZA"].retryable
    assert result.batch.next_cursor is None


# -- Twelve Data --------------------------------------------------------------------------------
class TwelveServer:
    def __init__(self, error=None):
        self.error = error
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.error is not None:
            return httpx.Response(200, json=self.error)
        start = datetime.fromisoformat(request.url.params["start_date"])
        values = []
        for hh, mm in [(9, 30), (10, 30), (11, 30), (12, 30), (13, 30), (14, 30), (15, 30)]:
            t = datetime(DAY.year, DAY.month, DAY.day, hh, mm)
            if t >= start:
                values.append({
                    "datetime": t.strftime("%Y-%m-%d %H:%M:%S"),
                    "open": "50.1", "high": "50.5", "low": "49.9", "close": "50.2", "volume": "700",
                })
        limit = int(request.url.params["outputsize"])
        meta = {"symbol": request.url.params["symbol"], "interval": "1h"}
        return httpx.Response(200, json={"meta": meta, "values": values[:limit], "status": "ok"})


def twelve(server, env=None) -> TwelveDataProvider:
    return TwelveDataProvider(
        api_key=None if env is not None else KEY,
        env=env if env is not None else {},
        client=httpx.Client(transport=httpx.MockTransport(server)),
        clock=lambda: NOW,
        fetch_id_factory=lambda: "f1",
    )


def test_twelve_data_hourly_bars_and_pagination(monkeypatch):
    monkeypatch.setattr(twelve_data_module, "OUTPUT_SIZE", 3)
    server = TwelveServer()
    batch = twelve(server).fetch_bars(req("zzza"))
    assert len(server.requests) == 3  # 3 + 3 + 1 bars
    params = server.requests[0].url.params
    assert params["interval"] == "1h" and params["adjust"] == "splits"
    assert params["timezone"] == "America/New_York" and params["apikey"] == KEY
    assert len(batch.bars) == 7
    bar = batch.bars[0]
    assert bar.ts_start == ny(DAY, 9, 30) and bar.symbol_as_returned == "ZZZA"
    assert bar.series.source.key == "twelve_data/intraday/split"
    assert KEY not in repr(batch.bars)


def test_twelve_data_free_tier_guards_and_errors():
    with pytest.raises(ProviderError) as err:
        twelve(TwelveServer()).fetch_bars(req("ZZZA", include_extended_hours=True))
    assert err.value.code == "unsupported_request"
    with pytest.raises(ProviderError) as err:
        twelve(TwelveServer(), env={}).fetch_bars(req("ZZZA"))
    assert err.value.code == "not_configured"
    limited = {"code": 429, "message": f"limit for key {KEY}", "status": "error"}
    with pytest.raises(ProviderError) as err:
        twelve(TwelveServer(error=limited)).fetch_bars(req("ZZZA"))
    assert err.value.code == "rate_limited" and err.value.retryable and KEY not in str(err.value)
    with pytest.raises(ProviderError) as err:
        twelve(TwelveServer()).fetch_bars(
            FetchRequest(market=Market.US, interval=Interval.D1, symbols=("ZZZA",), window=DateRange(DAY, DAY))
        )
    assert err.value.code == "unsupported_request"


def test_twelve_data_capability_and_discovery():
    from openbars.providers.registry import discover

    assert discover()["twelve_data"] is TwelveDataProvider
    (cap,) = TwelveDataProvider(env={}).capabilities()
    assert cap.interval is Interval.H1 and cap.market is Market.US and cap.feed == "twelve_data:intraday"
    assert TwelveDataProvider(env={}).quota_policy().provider == "twelve_data"
