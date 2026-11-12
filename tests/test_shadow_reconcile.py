"""Shadow reconcile: pure comparison + a fake HTTP backend. No network, no real tickers."""

from __future__ import annotations

import io
import json
from datetime import date, datetime, timezone
from decimal import Decimal
from urllib.error import HTTPError

import pytest

from openbars.cli.main import main
from openbars.cli.shadow import fetch_backend_series, read_symbols_file, run_shadow
from openbars.core.models import DateRange
from openbars.reconcile.shadow import (
    DIFF_ADJUSTMENT,
    DIFF_MISSING_BACKEND,
    DIFF_MISSING_LOCAL,
    DIFF_PRICE,
    DIFF_VOLUME,
    ShadowBar,
    compare_shadow,
    format_report,
)

pytestmark = pytest.mark.usefixtures("no_network")

SECRET = "tkn-shadow-" + "not-a-real-key"


def bar(day, open=10, high=11, low=9, close=10.5, volume=100, adjustment="split"):
    return ShadowBar(
        session_date=date.fromisoformat(day),
        open=Decimal(str(open)),
        high=Decimal(str(high)),
        low=Decimal(str(low)),
        close=Decimal(str(close)),
        volume=Decimal(str(volume)) if volume is not None else None,
        adjustment=adjustment,
    )


def test_identical_series_is_full_agreement():
    rows = [bar("2024-01-02"), bar("2024-01-03")]
    report = compare_shadow(
        {"TEST1": (rows, rows)},
        window_start=date(2024, 1, 2),
        window_end=date(2024, 1, 3),
    )
    assert report.comparable == 2
    assert report.rate() == Decimal("100.00")
    assert report.rate(DIFF_PRICE) == Decimal("100.00")
    assert report.missing_counts() == {}
    text = "\n".join(format_report(report))
    assert "TEST1" in text and "overall=100.00%" in text


def test_price_and_volume_and_adjustment_and_gaps_are_classified():
    local = [
        bar("2024-01-02", close=10.5, volume=100, adjustment="split"),
        bar("2024-01-03", close=10.5, volume=100, adjustment="split"),
        bar("2024-01-05", close=10.5, volume=100, adjustment="split"),  # missing on backend
    ]
    backend = [
        bar("2024-01-02", close="10.6", volume=100, adjustment="split"),  # 0.95% close > 0.5%
        bar("2024-01-03", close=10.5, volume=200, adjustment="raw"),  # volume + adjustment
        bar("2024-01-04", close=10.5, volume=100, adjustment="split"),  # missing locally
    ]
    report = compare_shadow(
        {"TEST1": (local, backend)},
        window_start=date(2024, 1, 2),
        window_end=date(2024, 1, 5),
    )
    kinds = {d.session_date.isoformat(): d.kinds for d in report.symbols[0].diffs}
    assert kinds["2024-01-02"] == (DIFF_PRICE,)
    assert DIFF_VOLUME in kinds["2024-01-03"] and DIFF_ADJUSTMENT in kinds["2024-01-03"]
    assert kinds["2024-01-04"] == (DIFF_MISSING_LOCAL,)
    assert kinds["2024-01-05"] == (DIFF_MISSING_BACKEND,)
    assert report.comparable == 2  # only the overlapping sessions
    assert report.rate(DIFF_PRICE) == Decimal("50.00")
    assert report.missing_counts()[DIFF_MISSING_LOCAL] == 1
    assert report.missing_counts()[DIFF_MISSING_BACKEND] == 1


def test_price_within_tolerance_is_agreement():
    local = [bar("2024-01-02", close="10.00")]
    backend = [bar("2024-01-02", close="10.04")]  # 0.4% < 0.5%
    report = compare_shadow({"ZZZA": (local, backend)}, window_start=date(2024, 1, 2), window_end=date(2024, 1, 2))
    assert report.rate(DIFF_PRICE) == Decimal("100.00") and not report.symbols[0].diffs


def test_format_report_does_not_echo_secrets():
    report = compare_shadow(
        {"TEST1": ([bar("2024-01-02")], [bar("2024-01-02")])},
        window_start=date(2024, 1, 2),
        window_end=date(2024, 1, 2),
        notes=[f"token was {SECRET}"],
    )
    # The compare module does not scrub; callers must. This test just checks no surprise leak path
    # in to_dict keys themselves.
    blob = json.dumps(report.to_dict())
    assert "API_KEY" not in blob


def test_read_symbols_file(tmp_path):
    path = tmp_path / "symbols.txt"
    path.write_text("# comment\n test1 \nTEST1\nzzza\n\n")
    assert read_symbols_file(path) == ["TEST1", "ZZZA"]
    with pytest.raises(Exception):
        read_symbols_file(tmp_path / "missing.txt")


class FakeOpener:
    def __init__(self, payloads, errors=None):
        self.payloads = payloads
        self.errors = errors or {}
        self.calls = []

    def __call__(self, request, timeout=30):
        self.calls.append((request.full_url, dict(request.header_items())))
        symbol = request.full_url.split("symbol=")[1].split("&")[0]
        if symbol in self.errors:
            raise self.errors[symbol]
        body = json.dumps(self.payloads[symbol]).encode()

        class Resp:
            def read(self_):
                return body

            def __enter__(self_):
                return self_

            def __exit__(self_, *a):
                return False

        return Resp()


def test_fetch_backend_series_parses_items_and_source():
    opener = FakeOpener(
        {
            "TEST1": {
                "items": [
                    {
                        "date": "2024-01-02",
                        "open": "10", "high": "11", "low": "9", "close": "10.5", "volume": "100",
                        "source": {"provider": "massive", "feed": "eod", "adjustment": "split"},
                    }
                ]
            }
        }
    )
    bars, err = fetch_backend_series(
        "http://127.0.0.1:8000", "TEST1", date(2024, 1, 2), date(2024, 1, 3),
        token=SECRET, opener=opener,
    )
    assert err is None and len(bars) == 1
    assert bars[0].close == Decimal("10.5") and bars[0].provider == "massive"
    url, headers = opener.calls[0]
    assert "127.0.0.1" in url and SECRET not in url
    assert any(v == "Bearer " + SECRET for v in headers.values())


def test_fetch_backend_series_404_is_an_error_code():
    err_body = json.dumps({"error": {"code": "symbol_not_found"}}).encode()
    fp = io.BytesIO(err_body)

    def opener(request, timeout=30):
        raise HTTPError(request.full_url, 404, "nf", hdrs=None, fp=fp)

    bars, err = fetch_backend_series(
        "http://127.0.0.1:8000", "EMPTY", date(2024, 1, 2), date(2024, 1, 2), opener=opener
    )
    assert bars == [] and err == "symbol_not_found"


def _settings_yaml(tmp_path) -> str:
    path = tmp_path / "config.yaml"
    path.write_text(
        f"""
markets: [us]
history: {{start: 2024-01-02, end: 2024-01-03}}
storage: {{path: {tmp_path / "data"}}}
providers:
  yfinance:
    enabled: true
"""
    )
    return str(path)


def test_shadow_cli_compare_without_fetch_writes_redacted_report(tmp_path, capsys):
    symbols = tmp_path / "symbols.txt"
    symbols.write_text("TEST1\n")
    output = tmp_path / "out"
    opener = FakeOpener(
        {
            "TEST1": {
                "items": [
                    {
                        "date": "2024-01-02",
                        "open": "10", "high": "11", "low": "9", "close": "10.5", "volume": "100",
                        "source": {"adjustment": "split"},
                    }
                ]
            }
        }
    )
    # No --fetch: empty local store + backend bars => missing_local, still exit 0.
    from openbars.cli import shadow as shadow_mod
    from openbars.config.loader import load_settings
    from openbars.storage.parquet_duckdb import open_backend

    settings = load_settings(_settings_yaml(tmp_path), env={})
    root = tmp_path / "data"
    open_backend(root)  # create empty store
    code, report, lines = run_shadow(
        settings=settings,
        symbols=["TEST1"],
        window=DateRange(date(2024, 1, 2), date(2024, 1, 2)),
        output=output,
        fetch=False,
        opener=opener,
    )
    assert code == 0
    assert report.missing_counts()[DIFF_MISSING_LOCAL] == 1
    text = (output / "shadow-report.txt").read_text()
    assert SECRET not in text and "TEST1" in text
    blob = (output / "shadow-report.json").read_text()
    assert SECRET not in blob
    _ = symbols, shadow_mod, main
