"""#152: yfinance 1h -- a legitimate "no rows" answer is an empty success, real errors still fail."""

from __future__ import annotations

import sys
from datetime import date
from types import ModuleType, SimpleNamespace

import pytest
from test_us_hourly_providers import DAY, NOW, FakeHourlyDownload, req

from openbars.core.errors import ProviderError
from openbars.core.models import DateRange, Interval, Market
from openbars.providers.base import FetchRequest
from openbars.providers.us.yfinance import (
    YFinanceNoData,
    YFinanceProvider,
    _yfinance_download,
    is_no_data_error,
)

pytestmark = pytest.mark.usefixtures("no_network")


class Mixed(FakeHourlyDownload):
    def __init__(self, raises=None):
        super().__init__()
        self.raises = raises or {}

    def __call__(self, ticker, **kwargs):
        if ticker in self.raises:
            self.calls.append((ticker, kwargs))
            raise self.raises[ticker]
        return super().__call__(ticker, **kwargs)


def test_no_data_symbol_is_skipped_not_an_error():
    p = YFinanceProvider(Mixed({"ZZZB": YFinanceNoData("ZZZB")}), clock=lambda: NOW)
    result = p.fetch_bars_result(req("ZZZA", "ZZZB"))
    assert {b.symbol_as_returned for b in result.batch.bars} == {"ZZZA"}
    assert result.batch.error is None and result.symbol_errors == {}
    assert result.skipped["no_data"] == 1


def test_all_symbols_without_data_give_an_empty_successful_batch():
    p = YFinanceProvider(Mixed({"ZZZA": YFinanceNoData("ZZZA"), "ZZZB": YFinanceNoData("ZZZB")}), clock=lambda: NOW)
    batch = p.fetch_bars(req("ZZZA", "ZZZB"))
    assert batch.bars == () and batch.error is None and batch.next_cursor is None


def test_real_errors_still_fail():
    p = YFinanceProvider(Mixed({"ZZZA": RuntimeError("Too Many Requests. Rate limited.")}), clock=lambda: NOW)
    with pytest.raises(ProviderError) as info:
        p.fetch_bars(req("ZZZA"))
    assert info.value.retryable and info.value.code == "rate_limited"
    p = YFinanceProvider(lambda t, **k: [], clock=lambda: NOW)  # empty frame without a no-data signal
    with pytest.raises(ProviderError) as info:
        p.fetch_bars(req("ZZZA"))
    assert info.value.code == "empty_result"


def test_daily_behaviour_is_unchanged():
    def download(ticker, **kwargs):
        raise YFinanceNoData(ticker)

    daily = FetchRequest(Market.US, Interval.D1, ("ZZZA",), DateRange(date(2024, 1, 2), date(2024, 1, 5)))
    with pytest.raises(ProviderError) as info:
        YFinanceProvider(download, clock=lambda: NOW).fetch_bars(daily)
    assert info.value.code == "provider_unavailable" and not info.value.retryable
    with pytest.raises(ProviderError) as info:
        YFinanceProvider(lambda t, **k: [], clock=lambda: NOW).fetch_bars(daily)
    assert info.value.code == "empty_result"


# -- default download path against a fake yfinance module ---------------------------------------


class YFException(Exception):
    pass


class YFTickerMissingError(YFException):
    pass


class YFPricesMissingError(YFTickerMissingError):
    def __init__(self, ticker, debug_info, yahoo_reason=None):
        self.yahoo_reason = yahoo_reason
        super().__init__(f"${ticker}: " + (yahoo_reason or f"possibly delisted; no price data found {debug_info}"))


class YFTzMissingError(YFTickerMissingError):
    def __init__(self, ticker):
        super().__init__(f"${ticker}: possibly delisted; no timezone found")


class YFRateLimitError(YFException):
    def __init__(self):
        super().__init__("Too Many Requests. Rate limited. Try after a while.")


def fake_yfinance(outcome):
    module = ModuleType("yfinance")
    module.exceptions = SimpleNamespace(YFPricesMissingError=YFPricesMissingError, YFTzMissingError=YFTzMissingError)
    calls = []

    class Ticker:
        def __init__(self, symbol):
            self.symbol = symbol

        def history(self, **kwargs):
            calls.append(("history", self.symbol, kwargs))
            if isinstance(outcome, BaseException):
                raise outcome
            return outcome

    def download(ticker, **kwargs):
        calls.append(("download", ticker, kwargs))
        return []

    module.Ticker, module.download, module.calls = Ticker, download, calls
    return module


@pytest.mark.parametrize("error, no_data", [
    (YFPricesMissingError("ZZZA", "(1h 2024-01-08 -> 2024-01-09)"), True),
    (YFPricesMissingError("ZZZA", "", yahoo_reason="Data doesn't exist for startDate = 1, endDate = 2"), True),
    (YFPricesMissingError("ZZZA", "", yahoo_reason="No data found, symbol may be delisted"), True),
    (YFPricesMissingError("ZZZA", "(1h ...)(Yahoo status_code = 500)"), False),
    (YFPricesMissingError("ZZZA", "", yahoo_reason="Invalid input - interval=1h is not supported"), False),
    (YFTzMissingError("ZZZA"), False),
    (YFRateLimitError(), False),
])  # fmt: skip
def test_default_hourly_path_tells_no_data_from_errors(monkeypatch, error, no_data):
    module = fake_yfinance(error)
    monkeypatch.setitem(sys.modules, "yfinance", module)
    kwargs = {"start": "2024-01-08", "end": "2024-01-09", "interval": "1h", "prepost": False, "auto_adjust": False,
              "actions": False, "progress": False, "threads": False, "timeout": 10, "group_by": "ticker"}  # fmt: skip
    if no_data:
        with pytest.raises(YFinanceNoData):
            _yfinance_download("ZZZA", **kwargs)
    else:
        with pytest.raises(type(error)):
            _yfinance_download("ZZZA", **kwargs)
    ((kind, symbol, passed),) = module.calls
    assert kind == "history" and symbol == "ZZZA" and passed["raise_errors"] is True
    assert "group_by" not in passed and "threads" not in passed and passed["interval"] == "1h"


def test_default_path_through_the_adapter(monkeypatch):
    monkeypatch.setitem(sys.modules, "yfinance", fake_yfinance(YFPricesMissingError("ZZZA", "")))
    batch = YFinanceProvider(clock=lambda: NOW).fetch_bars(req("ZZZA"))
    assert batch.bars == () and batch.error is None
    monkeypatch.setitem(sys.modules, "yfinance", fake_yfinance(YFTzMissingError("ZZZA")))
    with pytest.raises(ProviderError) as info:
        YFinanceProvider(clock=lambda: NOW).fetch_bars(req("ZZZA"))
    assert not info.value.retryable
    monkeypatch.setitem(sys.modules, "yfinance", fake_yfinance(YFRateLimitError()))
    with pytest.raises(ProviderError) as info:
        YFinanceProvider(clock=lambda: NOW).fetch_bars(req("ZZZA"))
    assert info.value.retryable


def test_daily_default_path_still_uses_download(monkeypatch):
    module = fake_yfinance([])
    monkeypatch.setitem(sys.modules, "yfinance", module)
    _yfinance_download("ZZZA", start="2024-01-02", end="2024-01-06", interval="1d")
    assert [c[0] for c in module.calls] == ["download"]


def test_real_yfinance_exception_shapes():
    exceptions = pytest.importorskip("yfinance.exceptions")
    assert is_no_data_error(exceptions.YFPricesMissingError("ZZZA", "(1h 2024-01-08 -> 2024-01-09)"))
    assert not is_no_data_error(exceptions.YFPricesMissingError("ZZZA", "(1h)(Yahoo status_code = 404)"))
    assert is_no_data_error(exceptions.YFPricesMissingError("ZZZA", "", yahoo_reason="No data found, symbol may be delisted"))


def test_history_frame_shape_parses(monkeypatch):
    pd = pytest.importorskip("pandas")
    index = pd.DatetimeIndex(
        [pd.Timestamp(DAY.isoformat() + f" {h}", tz="America/New_York") for h in ("09:30", "10:30", "11:30")],
        name="Datetime",
    )
    frame = pd.DataFrame({"Open": [10.1] * 3, "High": [10.5] * 3, "Low": [9.9] * 3, "Close": [10.2] * 3,
                          "Adj Close": [10.2] * 3, "Volume": [1000] * 3}, index=index)  # fmt: skip
    monkeypatch.setitem(sys.modules, "yfinance", fake_yfinance(frame))
    batch = YFinanceProvider(clock=lambda: NOW).fetch_bars(req("ZZZA"))
    assert len(batch.bars) == 3 and batch.error is None
