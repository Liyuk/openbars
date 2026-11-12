"""yfinance adapter tests: injected fake download, no network, yfinance package not required."""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_EVEN, Decimal
from pathlib import Path

import pytest

from openbars.core.errors import ProviderError
from openbars.core.models import Adjustment, DateRange, Interval, Market
from openbars.providers.adapter import NullQuotaGate
from openbars.providers.base import BatchShape, FetchRequest
from openbars.providers.us import yfinance as yf_mod
from openbars.providers.us.yfinance import (
    YFinanceProvider,
    chunk_symbols,
    should_request_yfinance,
)

DATA = json.loads((Path(__file__).parent / "fixtures" / "yfinance" / "bars.json").read_text())
NOW = datetime(2024, 1, 10, tzinfo=timezone.utc)


def req(*symbols: str, start="2024-01-02", end="2024-01-05", **kw) -> FetchRequest:
    return FetchRequest(
        market=Market.US,
        interval=Interval.D1,
        symbols=symbols or ("TEST1",),
        window=DateRange(date.fromisoformat(start), date.fromisoformat(end)),
        **kw,
    )


class FakeDownload:
    def __init__(self, data=None, errors=None):
        self.data = DATA if data is None else data
        self.errors = errors or {}
        self.calls: list[tuple[str, dict]] = []

    def __call__(self, ticker, **kwargs):
        self.calls.append((ticker, kwargs))
        if ticker in self.errors:
            raise self.errors[ticker]
        return self.data.get(ticker, [])


class RecordingGate(NullQuotaGate):
    def __init__(self):
        self.acquired: list[str] = []
        self.penalized: list[tuple[str, datetime]] = []

    def acquire(self, provider, weight=1, *, scope="default", timeout=None):
        self.acquired.append(provider)
        return super().acquire(provider, weight, scope=scope)

    def penalize(self, provider, until):
        self.penalized.append((provider, until))


def provider(download=None, **kw) -> YFinanceProvider:
    return YFinanceProvider(download or FakeDownload(), clock=lambda: NOW, **kw)


# -- capabilities ---------------------------------------------------------------------------
def test_capabilities_and_quota():
    p = provider()
    (cap,) = p.capabilities()
    assert (p.id, cap.provider, cap.feed) == ("yfinance", "yfinance", "yahoo")
    assert cap.market is Market.US and cap.interval is Interval.D1
    assert cap.batch is BatchShape.MULTI_SYMBOL and cap.max_symbols_per_request == 50
    assert cap.history_start is None
    assert cap.terms.personal_only and not cap.terms.redistribution_allowed and cap.terms.url
    assert Adjustment.SPLIT in cap.adjusted_modes
    policy = p.quota_policy()
    assert policy.key_scope == "per_ip"
    assert (policy.limits[0].count, policy.limits[0].per) == (30, timedelta(minutes=1))


# -- bars -------------------------------------------------------------------------------------
def test_fetch_bars_identity_decimals_and_window():
    download = FakeDownload()
    batch = provider(download).fetch_bars(req("test1", "ZZZA"))
    assert batch.error is None and batch.provider == "yfinance"
    # window is inclusive: 01-05 kept; 01-06 (end + 1) dropped; 2023-12-29 dropped
    keys = [(b.symbol_as_returned, b.session_date.isoformat()) for b in batch.bars]
    assert keys == [
        ("TEST1", "2024-01-02"),
        ("TEST1", "2024-01-03"),
        ("TEST1", "2024-01-05"),
        ("ZZZA", "2024-01-02"),
    ]
    first = batch.bars[0]
    assert first.feed == "yahoo" and first.provider == "yfinance"
    assert first.series.source.key == "yfinance/yahoo/split"
    assert (first.open, first.high, first.low, first.close, first.volume) == (
        Decimal("10.10"), Decimal("10.50"), Decimal("10.00"), Decimal("10.40"), Decimal("1000"),
    )
    assert batch.bars[1].open == Decimal("10.4")  # float input goes through str, not float math
    assert first.ts_start == datetime(2024, 1, 2, tzinfo=timezone.utc)


def test_download_kwargs_and_exclusive_end():
    download = FakeDownload()
    provider(download).fetch_bars(req("TEST1"))
    ticker, kwargs = download.calls[0]
    assert ticker == "TEST1"
    assert kwargs == {
        "start": "2024-01-02",
        "end": "2024-01-06",
        "auto_adjust": False,
        "actions": False,
        "progress": False,
        "threads": False,
        "timeout": 10,
        "group_by": "ticker",
    }


def test_defaults_and_unsupported_values():
    p = provider()
    assert p.fetch_bars(req(feed="yahoo", adjustment="split")).bars
    with pytest.raises(ProviderError) as e:
        p.fetch_bars(req(adjustment="raw"))
    assert e.value.code == "invalid_request"
    with pytest.raises(ProviderError):
        p.fetch_bars(req(feed="sip"))


def test_dot_symbol_maps_to_dash_and_keeps_original():
    download = FakeDownload()
    batch = provider(download).fetch_bars(req("ZZZ.A"))
    assert download.calls[0][0] == "ZZZ-A"
    assert {b.symbol_as_returned for b in batch.bars} == {"ZZZ.A"}


def test_symbols_are_stripped_uppercased_unique():
    download = FakeDownload()
    provider(download).fetch_bars(req(" test1 ", "TEST1", "zzza"))
    assert [c[0] for c in download.calls] == ["TEST1", "ZZZA"]


def test_chunking_51_symbols():
    symbols = [f"ZZ{i:02d}" for i in range(51)]
    chunks = chunk_symbols(symbols)
    assert [len(c) for c in chunks] == [50, 1]
    assert [len(c) for c in provider(batch_size=10).chunk_symbols(symbols)] == [10] * 5 + [1]
    with pytest.raises(ValueError):
        YFinanceProvider(FakeDownload(), batch_size=51)


def test_fetch_bars_with_more_than_batch_size_symbols():
    symbols = [f"ZZ{i:02d}" for i in range(51)]
    data = {"ZZ00": DATA["TEST1"], "ZZ50": DATA["ZZZA"]}
    download = FakeDownload(data)
    batch = provider(download).fetch_bars(req(*symbols))
    assert len(download.calls) == 51
    assert {b.symbol_as_returned for b in batch.bars} == {"ZZ00", "ZZ50"}
    assert batch.error is not None and batch.error.code == "empty_result"


def test_invalid_and_nan_rows_skipped_neighbours_kept():
    rows = [
        {"Date": "2024-01-02", "Open": 1, "High": 2, "Low": 1, "Close": 2, "Volume": 5},
        {"Date": "2024-01-03", "Open": float("nan"), "High": float("nan"), "Low": float("nan"),
         "Close": float("nan"), "Volume": float("nan")},
        {"Date": "2024-01-04", "Open": 1, "High": 0.5, "Low": 1, "Close": 1, "Volume": 5},  # high < low
        {"Date": "2024-01-05", "Open": 2, "High": 3, "Low": 2, "Close": 3, "Volume": 6},
    ]
    result = provider(FakeDownload({"TEST1": rows})).fetch_bars_result(req())
    assert [b.session_date.day for b in result.batch.bars] == [2, 5]
    assert result.skipped == {"nan_row": 1, "invalid_row": 1}


def test_column_oriented_and_tuple_keys():
    cols = {
        "Date": [date(2024, 1, 2), date(2024, 1, 3)],
        ("TEST1", "Open"): [1, 2], ("TEST1", "High"): [2, 3], ("TEST1", "Low"): [1, 2],
        ("TEST1", "Close"): [2, 3], ("TEST1", "Volume"): [5, 6],
    }
    batch = provider(FakeDownload({"TEST1": cols})).fetch_bars(req())
    assert [b.close for b in batch.bars] == [Decimal(2), Decimal(3)]


NOISY = [
    # (open, high, low, close, volume): IEEE floats whose repr has > 8 fractional digits
    (10.123456789012, 10.987654321098, 9.876543210987, 10.555555555555, 1234.123456789012),
    (11.111111111111, 12.222222222222, 10.000000000001, 11.555555555555, 2000.2500000001),
    (0.123456785, 0.123456795, 0.123456775, 0.123456785, 2.000000005),  # half-even ties on repr
]


def _noisy_rows(dates=("2024-01-02", "2024-01-03", "2024-01-04")):
    return [
        {"Date": d, "Open": o, "High": h, "Low": lo, "Close": c, "Volume": v}
        for d, (o, h, lo, c, v) in zip(dates, NOISY)
    ]


def _q8(value: float) -> Decimal:
    return Decimal(repr(value)).quantize(Decimal("1e-8"), rounding=ROUND_HALF_EVEN)


def _assert_noisy_quantized(bars):
    assert len(bars) == len(NOISY)
    for bar, inputs in zip(bars, NOISY):
        got = (bar.open, bar.high, bar.low, bar.close, bar.volume)
        for value, raw in zip(got, inputs):
            assert isinstance(value, Decimal)
            assert value.as_tuple().exponent >= -8
            assert value == _q8(raw)


def test_noisy_float_prices_are_quantized_not_empty_result():
    assert len(repr(NOISY[0][0]).split(".")[1]) > 8
    result = provider(FakeDownload({"TEST1": _noisy_rows()})).fetch_bars_result(req())
    assert result.batch.error is None and not result.symbol_errors and not result.skipped
    assert [b.session_date.isoformat() for b in result.batch.bars] == [
        "2024-01-02", "2024-01-03", "2024-01-04",
    ]
    _assert_noisy_quantized(result.batch.bars)
    assert result.batch.bars[0].open == Decimal("10.12345679")
    assert result.batch.bars[2].open == Decimal("0.12345678")  # tie -> even
    assert result.batch.bars[2].high == Decimal("0.1234568")  # tie -> even


def test_noisy_two_session_inclusive_window():
    download = FakeDownload({"TEST1": _noisy_rows(("2024-01-02", "2024-01-03"))[:2]})
    batch = provider(download).fetch_bars(req(start="2024-01-02", end="2024-01-03"))
    assert download.calls[0][1]["end"] == "2024-01-04"
    assert [b.session_date.isoformat() for b in batch.bars] == ["2024-01-02", "2024-01-03"]


def test_exact_str_decimal_inputs_with_too_many_digits_stay_invalid():
    rows = [
        {"Date": "2024-01-02", "Open": "1.123456789", "High": "2", "Low": "1", "Close": "1", "Volume": 1},
        {"Date": "2024-01-03", "Open": Decimal("1.123456789"), "High": 2, "Low": 1, "Close": 1, "Volume": 1},
    ]
    with pytest.raises(ProviderError) as e:
        provider(FakeDownload({"TEST1": rows})).fetch_bars(req())
    assert e.value.code == "empty_result"


def test_float_still_invalid_after_quantization_is_skipped():
    rows = _noisy_rows()[:1] + [
        {"Date": "2024-01-03", "Open": 1.5, "High": 1.0000000001, "Low": 1.9999999999,
         "Close": 1.5, "Volume": 1.0},  # high < low even after quantization
        {"Date": "2024-01-04", "Open": float("inf"), "High": 2.0, "Low": 1.0, "Close": 1.0, "Volume": 1.0},
    ]
    result = provider(FakeDownload({"TEST1": rows})).fetch_bars_result(req())
    assert [b.session_date.day for b in result.batch.bars] == [2]
    assert result.skipped == {"invalid_row": 2}


def test_rate_limited_symbol_is_retryable_and_keeps_other_bars():
    download = FakeDownload(errors={"TEST1": RuntimeError("Too Many Requests. Rate limited")})
    result = provider(download).fetch_bars_result(req("TEST1", "ZZZA"))
    assert {b.symbol_as_returned for b in result.batch.bars} == {"ZZZA"}
    assert result.symbol_errors["TEST1"].code == "rate_limited"
    assert result.symbol_errors["TEST1"].retryable
    assert result.batch.error.code == "rate_limited" and result.batch.error.retryable


@pytest.mark.parametrize(
    "exc", [TimeoutError("x"), ConnectionError("x"), RuntimeError("service temporarily down")]
)
def test_transient_exceptions_are_retryable(exc):
    result = provider(FakeDownload(errors={"TEST1": exc})).fetch_bars_result(req("TEST1", "ZZZA"))
    assert result.symbol_errors["TEST1"].code == "rate_limited"


def test_other_exception_is_provider_unavailable():
    result = provider(FakeDownload(errors={"TEST1": ValueError("boom")})).fetch_bars_result(
        req("TEST1", "ZZZA")
    )
    err = result.symbol_errors["TEST1"]
    assert err.code == "provider_unavailable" and "boom" not in str(err)
    assert result.batch.bars


def test_retry_at_penalizes_gate():
    class Blocked(Exception):
        retry_after = 120

    gate = RecordingGate()
    p = provider(FakeDownload(errors={"TEST1": Blocked("rate limit")}), quota_gate=gate)
    batch = p.fetch_bars(req("TEST1", "ZZZA"))
    assert gate.penalized == [("yfinance", NOW + timedelta(seconds=120))]
    assert batch.error.retry_at == NOW + timedelta(seconds=120)


def test_all_symbols_failing_raises():
    with pytest.raises(ProviderError) as e:
        provider(FakeDownload(errors={"TEST1": TimeoutError("t")})).fetch_bars(req("TEST1"))
    assert e.value.code == "rate_limited" and e.value.retryable


def test_empty_symbol_is_empty_result_not_retryable():
    with pytest.raises(ProviderError) as e:
        provider().fetch_bars(req("EMPTY"))
    assert e.value.code == "empty_result" and not e.value.retryable
    result = provider().fetch_bars_result(req("EMPTY", "TEST1"))
    assert result.symbol_errors["EMPTY"].code == "empty_result"
    assert not result.symbol_errors["EMPTY"].retryable
    assert not result.batch.error.retryable and result.batch.bars


def test_quota_gate_and_hook_called_once_per_symbol():
    gate = RecordingGate()
    hooks: list[int] = []
    provider(quota_gate=gate, before_request=lambda: hooks.append(1)).fetch_bars(
        req("TEST1", "ZZZA", "EMPTY")
    )
    assert gate.acquired == ["yfinance"] * 3 and len(hooks) == 3


def test_quota_exhausted_stops_with_cursor():
    from openbars.core.errors import QuotaExhausted

    class Gate(RecordingGate):
        def acquire(self, provider, weight=1, *, scope="default", timeout=None):
            if len(self.acquired) == 1:
                raise QuotaExhausted(provider)
            return super().acquire(provider, weight, scope=scope)

    batch = provider(quota_gate=Gate()).fetch_bars(req("TEST1", "ZZZA"))
    assert batch.next_cursor == "1" and batch.error.code == "quota_exhausted"
    rest = provider().fetch_bars(req("TEST1", "ZZZA", cursor=batch.next_cursor))
    assert {b.symbol_as_returned for b in rest.bars} == {"ZZZA"}


# -- optional dependency ----------------------------------------------------------------------
def test_construction_does_not_import_yfinance():
    code = (
        "import sys; from openbars.providers.us.yfinance import YFinanceProvider; "
        "YFinanceProvider(); import openbars.providers.us.alpaca; "
        "assert 'yfinance' not in sys.modules"
    )
    subprocess.run([sys.executable, "-c", code], check=True)


def test_fetch_without_package_or_download_raises(monkeypatch):
    monkeypatch.setitem(sys.modules, "yfinance", None)  # makes `import yfinance` fail
    with pytest.raises(ProviderError) as e:
        YFinanceProvider().fetch_bars(req())
    assert e.value.code == "yfinance_not_installed" and not e.value.retryable


# -- pandas path (only when pandas is available) ------------------------------------------------
def test_pandas_dataframe_with_multiindex_columns():
    pd = pytest.importorskip("pandas")
    frame = pd.DataFrame(
        {
            ("TEST1", "Open"): [1.5, 2.5], ("TEST1", "High"): [2.0, 3.0], ("TEST1", "Low"): [1.0, 2.0],
            ("TEST1", "Close"): [1.75, 2.75], ("TEST1", "Volume"): [10, 20],
        },
        index=pd.DatetimeIndex(["2024-01-02", "2024-01-03"], name="Date"),
    )
    batch = provider(FakeDownload({"TEST1": frame})).fetch_bars(req())
    assert [b.close for b in batch.bars] == [Decimal("1.75"), Decimal("2.75")]


def test_pandas_yfinance_18_group_by_ticker_shape_with_noisy_floats():
    pd = pytest.importorskip("pandas")
    columns = pd.MultiIndex.from_product(
        [["TEST1"], ["Open", "High", "Low", "Close", "Volume"]], names=["Ticker", "Price"]
    )
    values = [list(row) for row in NOISY]
    frame = pd.DataFrame(
        values, columns=columns, index=pd.DatetimeIndex(["2024-01-02", "2024-01-03", "2024-01-04"], name="Date")
    )
    assert ("Date", "") in frame.reset_index().columns  # the shape under test
    result = provider(FakeDownload({"TEST1": frame})).fetch_bars_result(req())
    assert result.batch.error is None and not result.skipped
    assert [b.session_date.isoformat() for b in result.batch.bars] == [
        "2024-01-02", "2024-01-03", "2024-01-04",
    ]
    _assert_noisy_quantized(result.batch.bars)


def test_pandas_numpy_float32_scalars_are_quantized():
    np = pytest.importorskip("numpy")
    row = {"Date": "2024-01-02", "Open": np.float32(1.1), "High": np.float32(2.2),
           "Low": np.float32(1.0), "Close": np.float32(1.5), "Volume": np.float32(7)}
    (bar,) = provider(FakeDownload({"TEST1": [row]})).fetch_bars(req()).bars
    assert bar.open.as_tuple().exponent >= -8 and bar.volume == Decimal(7)


# -- routing ----------------------------------------------------------------------------------
def test_routing_pre_2016_window_requested():
    window = DateRange(date(2014, 1, 2), date(2015, 12, 31))
    out = should_request_yfinance(window, primary_covered=None)
    assert out == [window]
    # even when "covered" it is still pre-alpaca history
    assert should_request_yfinance(window, primary_covered=window) == [window]


def test_routing_covered_2017_window_not_requested():
    window = DateRange(date(2017, 3, 1), date(2017, 3, 31))
    assert should_request_yfinance(window, primary_covered=window) == []
    all_days = [date(2017, 3, 1) + timedelta(days=i) for i in range(31)]
    assert should_request_yfinance(window, primary_covered=all_days) == []


def test_routing_2017_gap_day_requested():
    window = DateRange(date(2017, 3, 1), date(2017, 3, 10))
    covered = [d for d in (date(2017, 3, 1) + timedelta(days=i) for i in range(10)) if d != date(2017, 3, 8)]
    assert should_request_yfinance(window, primary_covered=covered) == [
        DateRange(date(2017, 3, 8), date(2017, 3, 8))
    ]


def test_routing_straddling_window_pre_2016_plus_gaps():
    window = DateRange(date(2015, 12, 28), date(2016, 1, 8))
    covered = [date(2016, 1, 4), date(2016, 1, 5), date(2016, 1, 7), date(2016, 1, 8)]  # 01-06 is a gap
    assert should_request_yfinance(window, primary_covered=covered) == [
        DateRange(date(2015, 12, 28), date(2015, 12, 31)),
        DateRange(date(2016, 1, 6), date(2016, 1, 6)),
    ]
    clean = DateRange(date(2016, 1, 4), date(2016, 1, 8))
    assert should_request_yfinance(window, primary_covered=clean) == [
        DateRange(date(2015, 12, 28), date(2015, 12, 31))
    ]


def test_routing_no_primary_coverage_requests_all_sessions():
    window = DateRange(date(2024, 1, 2), date(2024, 1, 5))
    assert should_request_yfinance(window, primary_covered=None) == [window]
    assert yf_mod.ALPACA_HISTORY_START == date(2016, 1, 1)
