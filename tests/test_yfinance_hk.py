"""Hong Kong yfinance adapter: injected fake download, no network by default."""

from __future__ import annotations

import os
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from openbars.core.errors import ProviderError
from openbars.core.models import Adjustment, DateRange, Interval, Market
from openbars.core.series import SourceIdentity
from openbars.normalize.hk_instruments import from_yahoo_hk_ticker
from openbars.providers.base import FetchRequest
from openbars.providers.us.yfinance import (
    YFinanceProvider,
    chunk_hk_symbols,
    to_yahoo_hk_ticker,
)

NOW = datetime(2026, 3, 10, tzinfo=UTC)

# A regular XHKG week in 2026: Mon 3/2 – Fri 3/6. 3/7 is Saturday.
_OHLCV = {"Open": "10.10", "High": "10.50", "Low": "10.00", "Close": "10.40", "Volume": 1000}


def _row(day: str, **extra) -> dict:
    return {"Date": day, **_OHLCV, **extra}


def hk_req(*symbols: str, start="2026-03-02", end="2026-03-06", **kw) -> FetchRequest:
    return FetchRequest(
        market=Market.HK,
        interval=Interval.D1,
        symbols=symbols or ("00700",),
        window=DateRange(date.fromisoformat(start), date.fromisoformat(end)),
        **kw,
    )


class FakeDownload:
    def __init__(self, data=None, errors=None):
        self.data = data or {}
        self.errors = errors or {}
        self.calls: list[tuple[str, dict]] = []

    def __call__(self, ticker, **kwargs):
        self.calls.append((ticker, kwargs))
        if ticker in self.errors:
            raise self.errors[ticker]
        return self.data.get(ticker, [])


def provider(download=None, **kw) -> YFinanceProvider:
    return YFinanceProvider(download or FakeDownload(), clock=lambda: NOW, **kw)


def test_capabilities_include_hk():
    caps = {c.market: c for c in provider().capabilities()}
    cap = caps[Market.HK]
    assert cap.provider == "yfinance" and cap.feed == "yahoo"
    assert cap.interval is Interval.D1
    assert Adjustment.SPLIT in cap.adjusted_modes
    assert cap.terms.personal_only and not cap.terms.redistribution_allowed
    assert "HKEX" in (cap.terms.notes or "")


def test_maps_five_digit_and_instrument_id_to_yahoo_ticker():
    rows = [_row("2026-03-02"), _row("2026-03-03")]
    download = FakeDownload({"0700.HK": rows})
    batch = provider(download).fetch_bars(hk_req("hk:eq:xhkg:00700"))
    assert download.calls[0][0] == "0700.HK"
    assert download.calls[0][1]["auto_adjust"] is False
    assert {b.symbol_as_returned for b in batch.bars} == {"hk:eq:xhkg:00700"}
    assert [b.session_date.isoformat() for b in batch.bars] == [
        "2026-03-02",
        "2026-03-03",
    ]
    assert all(
        b.series.source == SourceIdentity("yfinance", "yahoo", "split")
        for b in batch.bars
    )
    assert batch.bars[0].close == Decimal("10.40")


def test_maps_yahoo_spelling_and_dedupes_by_code():
    rows = [_row("2026-03-02")]
    download = FakeDownload({"0700.HK": rows})
    provider(download).fetch_bars(hk_req("0700.HK", "00700", "700"))
    assert [c[0] for c in download.calls] == ["0700.HK"]


def test_chunk_hk_symbols_unique_by_code():
    chunks = chunk_hk_symbols(["0700.HK", "00700", "09988", "9988.HK"], size=10)
    assert chunks == [("0700.HK", "09988")]
    assert to_yahoo_hk_ticker("09988") == "9988.HK"
    assert from_yahoo_hk_ticker("0700.HK") == "00700"


def test_skips_non_session_days_keeps_half_day():
    # 2025-12-24 is a half day (session); 2025-12-25 Christmas is closed.
    rows = [
        _row("2025-12-23"),
        _row("2025-12-24"),
        _row("2025-12-25"),
        _row("2025-12-26"),  # Boxing Day, closed
    ]
    download = FakeDownload({"0700.HK": rows})
    result = provider(download).fetch_bars_result(
        hk_req("00700", start="2025-12-23", end="2025-12-26")
    )
    days = [b.session_date.isoformat() for b in result.batch.bars]
    assert days == ["2025-12-23", "2025-12-24"]
    assert result.skipped.get("non_session", 0) == 2


def test_skips_library_adhoc_closure():
    rows = [_row("2024-09-05"), _row("2024-09-06"), _row("2024-09-09")]
    download = FakeDownload({"0700.HK": rows})
    result = provider(download).fetch_bars_result(
        hk_req("00700", start="2024-09-05", end="2024-09-09")
    )
    assert [b.session_date.isoformat() for b in result.batch.bars] == [
        "2024-09-05",
        "2024-09-09",
    ]
    assert result.skipped.get("non_session", 0) == 1


def test_window_outside_coverage_raises():
    with pytest.raises(ProviderError) as exc:
        provider(FakeDownload()).fetch_bars(
            hk_req("00700", start="1999-01-04", end="1999-01-08")
        )
    assert exc.value.code == "invalid_request"
    assert "outside that range" in str(exc.value)


def test_unsupported_market_or_adjustment():
    p = provider(FakeDownload({"0700.HK": [_row("2026-03-02")]}))
    with pytest.raises(ProviderError) as exc:
        p.fetch_bars(hk_req(adjustment="raw"))
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


def test_invalid_symbol_is_invalid_request():
    with pytest.raises(ProviderError) as exc:
        provider(FakeDownload()).fetch_bars(hk_req("NOTATICKER"))
    assert exc.value.code == "invalid_request"


@pytest.mark.network
def test_optional_live_smoke_stays_in_memory():
    """Hits Yahoo only when OPENBARS_YF_LIVE=1; nothing is written to disk."""
    if os.environ.get("OPENBARS_YF_LIVE") != "1":
        pytest.skip("set OPENBARS_YF_LIVE=1 to run the optional Yahoo smoke test")
    pytest.importorskip("yfinance")
    batch = YFinanceProvider().fetch_bars(
        hk_req("0700.HK", start="2026-03-02", end="2026-03-06")
    )
    assert batch.provider == "yfinance"
    assert batch.bars, "expected at least one in-memory bar"
    assert all(b.session_date.weekday() < 5 for b in batch.bars)
    # The result is only in this process; no store is opened.
