"""CLI-level Hong Kong: ``markets: [hk]`` runs a synthetic yfinance fake and ``verify`` reads it back.

Offline: ``HkFake`` (yfinance-shaped, weekday bars) is injected through ``provider_factory``;
Parquet + SQLite under tmp_path. Synthetic code ``00000`` only (not a live listing).
"""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest
from cn_fakes import CnFake
from fakes import FakeProvider

from openbars.cli.main import main
from openbars.core.models import Market
from openbars.core.series import YFINANCE_YAHOO_RAW, BarSeriesCatalog

pytestmark = pytest.mark.usefixtures("no_network")

NOW = datetime(2026, 3, 6, 17, 0, tzinfo=ZoneInfo("Asia/Hong_Kong"))  # an hour after Friday's close
IID = "hk:eq:xhkg:00000"
SERIES = BarSeriesCatalog.resolve(YFINANCE_YAHOO_RAW, currency="HKD")


class HkFake(FakeProvider):
    """Weekday bars for Hong Kong; the fake's default series is replaced by yfinance RAW / HKD."""

    def __init__(self) -> None:
        super().__init__("yfinance", feed="yahoo")

    def capabilities(self):
        return tuple(replace(c, market=Market.HK) for c in super().capabilities())

    def bar(self, symbol, day, fetch_id):
        base = super().bar(symbol, day, fetch_id)
        return replace(base, series=SERIES, extra={"session_date": day, "adjustment": "raw"})


def config(tmp_path, markets="[hk]") -> str:
    path = tmp_path / "config.yaml"
    path.write_text(
        f"""
version: 1
markets: {markets}
intervals: [1d]
history: {{start: 2026-03-02, end: null}}
universe:
  hk:
    symbols: ["00000"]
  cn:
    symbols: [sh.600000]
storage: {{backend: parquet_duckdb, path: {tmp_path / "data"}}}
providers:
  yfinance: {{enabled: true}}
  baostock: {{enabled: true}}
"""
    )
    return str(path)


def cli(argv, tmp_path, factory, markets="[hk]", now=NOW):
    return main(
        ["--config", config(tmp_path, markets), *argv], provider_factory=factory, clock=lambda: now, env={}
    )


def test_hk_run_then_verify_reports_xhkg_raw(tmp_path, capsys):
    hk = HkFake()
    factory = lambda settings, gate: {"yfinance": hk}  # noqa: E731
    assert cli(["run"], tmp_path, factory) == 0
    assert hk.requests
    capsys.readouterr()
    assert cli(["verify", "--json", "reports/verify.json"], tmp_path, factory) == 0
    captured = capsys.readouterr()
    out = captured.out
    assert "us market only" not in captured.err + out
    assert "market hk" in out and "hk-daily-v1" in out and "XHKG" in out
    assert "2026-03-02..2026-03-06" in out and "expected sessions 5" in out and "universe 1" in out
    assert "stored RAW" in out and "yfinance/yahoo/" in out
    assert "missing 0" in out

    report = json.loads((tmp_path / "data" / "reports" / "verify.json").read_text())
    assert report["market"] == "hk" and report["policy_version"] == "hk-daily-v1"
    cov = report["periods"][0]["coverage"]
    assert cov["expected_cells"] == 5 and cov["canonical_cells"] == 5 and cov["suspended_cells"] == 0


def test_two_markets_print_both_reports_and_keep_json_keyed_by_market(tmp_path, capsys):
    hk, cn = HkFake(), CnFake()
    factory = lambda settings, gate: {"yfinance": hk, "baostock": cn}  # noqa: E731
    assert cli(["run"], tmp_path, factory, markets="[hk, cn]") == 0
    capsys.readouterr()
    assert cli(["verify", "--json", "reports/verify.json"], tmp_path, factory, markets="[hk, cn]") == 0
    out = capsys.readouterr().out
    assert "== market cn: calendar XSHG, policy cn-daily-v1 ==" in out
    assert "== market hk: calendar XHKG, policy hk-daily-v1 ==" in out
    assert out.index("market cn") < out.index("market hk")  # fixed order: us, crypto, cn, hk
    document = json.loads((tmp_path / "data" / "reports" / "verify.json").read_text())
    assert set(document["markets"]) == {"cn", "hk"}
    assert document["markets"]["cn"]["policy_version"] == "cn-daily-v1"
    assert document["markets"]["hk"]["calendar"] == "XHKG"


def test_configured_market_without_bars_is_a_note_not_an_error(tmp_path, capsys):
    hk = HkFake()
    factory = lambda settings, gate: {"yfinance": hk}  # noqa: E731
    assert cli(["run"], tmp_path, factory) == 0  # hk only
    capsys.readouterr()
    assert cli(["verify"], tmp_path, factory, markets="[hk, cn]") == 0
    out = capsys.readouterr().out
    assert "market hk" in out and "note: market cn: no stored bars" in out


def test_no_stored_bars_in_any_market_still_errors(tmp_path, capsys):
    (tmp_path / "data").mkdir()
    assert cli(["verify"], tmp_path, lambda s, g: {}, markets="[hk, cn]") == 1
    assert "no stored bars" in capsys.readouterr().err
