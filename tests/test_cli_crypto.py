"""CLI-level crypto: ``markets: [crypto]`` ingests pairs, writes crypto-daily-v1 bars, reads them back.

Offline (fake venues, Parquet + SQLite under tmp_path). Drives ``openbars.cli.main`` ``run`` /
``plan``; the read side uses ``DuckDbBarReader`` (auto policy, ticker resolve).
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest
from crypto_fakes import CryptoFake
from fakes import FakeProvider

from openbars.cli.main import main
from openbars.core.models import DateRange, Interval, Market
from openbars.providers.base import ProviderInstrument
from openbars.storage.parquet_duckdb import DuckDbBarReader, open_backend

NOW = datetime(2026, 3, 10, 6, 0, tzinfo=UTC)  # latest ready UTC session: Mon 2026-03-09
START, END = date(2026, 3, 5), date(2026, 3, 9)
DAYS = [START + timedelta(days=i) for i in range(5)]
BIN = "crypto:spot:binance:TEST-USDT"
CB = "crypto:spot:coinbase:TEST-USDT"
YEAR = DateRange(date(2026, 1, 1), date(2026, 12, 31))
PAIRS = {"binance": ("TESTUSDT", "BINANCE"), "coinbase": ("TEST-USDT", "COINBASE")}


class ListingFake(CryptoFake):
    """CryptoFake that also lists its TEST pair (and counts / can fail ``list_instruments``)."""

    def __init__(self, name: str, *, list_error: Exception | None = None, **kw) -> None:
        super().__init__(name, **kw)
        self.list_calls: list[Market] = []
        self.list_error = list_error

    def list_instruments(self, market):
        self.list_calls.append(market)
        if self.list_error is not None:
            raise self.list_error
        symbol, exchange = PAIRS[self.id]
        return [
            ProviderInstrument(symbol, Market.CRYPTO, None, exchange=exchange, base_asset="TEST", quote_asset="USDT"),
            ProviderInstrument("BADROW", Market.CRYPTO, None, exchange=exchange),  # no base/quote: skipped
        ]


def config(tmp_path) -> str:
    path = tmp_path / "config.yaml"
    path.write_text(
        f"""
version: 1
markets: [crypto]
intervals: [1d]
history: {{start: 2026-03-05, end: null}}
universe:
  crypto:
    symbols: [TESTUSDT, TEST-USDT]
storage: {{backend: parquet_duckdb, path: {tmp_path / "data"}}}
providers:
  binance: {{enabled: true}}
  coinbase: {{enabled: true}}
"""
    )
    return str(path)


def make_factory(**kw):
    fakes = {n: ListingFake(n, base_price=p, **kw.get(n, {})) for n, p in (("binance", 100), ("coinbase", 500))}
    return fakes, lambda settings, gate: fakes


def cli(argv, tmp_path, factory):
    return main(["--config", config(tmp_path), *argv], provider_factory=factory, clock=lambda: NOW, env={})


def test_run_ingests_pairs_and_writes_crypto_canonical(tmp_path, capsys):
    fakes, factory = make_factory()
    assert cli(["run"], tmp_path, factory) == 0
    out = capsys.readouterr().out
    assert "crypto pairs (binance, coinbase): 2 created, 0 reused, 2 unresolved" in out  # BADROW x2

    backend = open_backend(tmp_path / "data")
    master = backend.instruments
    assert {BIN, CB} <= {i.instrument_id for i in master.instruments()}
    assert {(m.provider, m.symbol) for m in master.mappings()} == {("binance", "TESTUSDT"), ("coinbase", "TEST-USDT")}
    assert all(f.list_calls == [Market.CRYPTO] for f in fakes.values())

    for name in PAIRS:
        assert list(backend.raw.scan(name, Market.CRYPTO, Interval.D1, YEAR))
    rows = backend.canonical.scan_rows([BIN, CB], Interval.D1, YEAR, policy_version=None)
    assert len(rows) == 2 * len(DAYS)
    assert {r.policy_version for r in rows} == {"crypto-daily-v1"}
    for iid, venue in ((BIN, "binance"), (CB, "coinbase")):  # venues never merged
        mine = [r for r in rows if r.instrument_id == iid]
        assert sorted(r.session_date for r in mine) == DAYS and {r.provider for r in mine} == {venue}


def test_second_run_is_idempotent(tmp_path, capsys):
    fakes, factory = make_factory()
    assert cli(["run"], tmp_path, factory) == 0
    backend = open_backend(tmp_path / "data")
    before = backend.canonical.scan_rows([BIN, CB], Interval.D1, YEAR, policy_version=None)
    requests = {n: len(f.requests) for n, f in fakes.items()}
    capsys.readouterr()
    assert cli(["run"], tmp_path, factory) == 0
    assert "0 created, 2 reused" in capsys.readouterr().out
    assert {n: len(f.requests) for n, f in fakes.items()} == requests  # raw coverage: nothing refetched
    after = open_backend(tmp_path / "data").canonical.scan_rows([BIN, CB], Interval.D1, YEAR, policy_version=None)
    assert after == before
    master = open_backend(tmp_path / "data").instruments
    assert len([i for i in master.instruments() if i.instrument_id.startswith("crypto:")]) == 2


def test_dry_run_plan_does_not_list_or_write_master(tmp_path, capsys):
    fakes, factory = make_factory()
    assert cli(["plan"], tmp_path, factory) == 0
    out = capsys.readouterr().out
    assert "plan:" in out and "nothing written" in out
    assert all(f.list_calls == [] for f in fakes.values())
    assert open_backend(tmp_path / "data").instruments.mappings() == []


def test_plan_submit_ingests_then_plans_from_master(tmp_path, capsys):
    fakes, factory = make_factory()
    assert cli(["plan", "--submit"], tmp_path, factory) == 0
    assert "crypto pairs" in capsys.readouterr().out
    assert all(f.list_calls == [Market.CRYPTO] for f in fakes.values())
    assert len(open_backend(tmp_path / "data").instruments.mappings()) == 2
    # a later dry run may use the master: still no listing
    assert cli(["plan"], tmp_path, factory) == 0
    assert all(f.list_calls == [Market.CRYPTO] for f in fakes.values())


def test_listing_failure_is_a_note_not_a_crash(tmp_path, capsys):
    fakes, factory = make_factory(coinbase={"list_error": RuntimeError("boom")})
    assert cli(["run"], tmp_path, factory) == 0
    out = capsys.readouterr().out
    assert "note: crypto pair listing failed for coinbase: RuntimeError" in out
    assert "boom" not in out
    backend = open_backend(tmp_path / "data")
    assert {m.provider for m in backend.instruments.mappings()} == {"binance"}
    # binance pair resolves and is written; coinbase has no pair in the master: raw only
    assert backend.canonical.scan_rows([BIN], Interval.D1, YEAR, policy_version=None)
    assert backend.canonical.scan_rows([CB], Interval.D1, YEAR, policy_version=None) == []


def test_us_only_settings_never_list_instruments(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text(
        f"""
version: 1
markets: [us]
intervals: [1d]
history: {{start: 2026-03-05, end: null}}
universe:
  us:
    symbols: [TEST1]
storage: {{backend: parquet_duckdb, path: {tmp_path / "data"}}}
providers:
  massive: {{enabled: true, api_key: k}}
"""
    )
    spy = FakeProvider("massive", feed="eod", max_symbols=10**6)
    calls: list[Market] = []
    spy.list_instruments = lambda market: calls.append(market) or ()  # type: ignore[method-assign]
    assert main(["--config", str(path), "plan", "--submit"], provider_factory=lambda s, g: {"massive": spy},
                clock=lambda: NOW, env={}) == 0
    assert calls == []


# --- read path -------------------------------------------------------------------------------


@pytest.fixture
def written(tmp_path):
    _, factory = make_factory()
    assert cli(["run"], tmp_path, factory) == 0
    return DuckDbBarReader(open_backend(tmp_path / "data"))


def test_reader_auto_policy_for_crypto_market_and_ids(written):
    for kw in ({"market": Market.CRYPTO}, {}):
        table = written.bars([BIN, CB], start=START, end=END, **kw)
        assert table.num_rows == 2 * len(DAYS)
        assert set(table.column("policy_version").to_pylist()) == {"crypto-daily-v1"}
    assert written.bars(market=Market.CRYPTO, start=START, end=END).num_rows == 2 * len(DAYS)


def test_reader_explicit_policy_wins(written):
    assert written.bars([BIN], policy_version="research-daily-v2", start=START, end=END).num_rows == 0
    assert written.bars([BIN], policy_version="crypto-daily-v1", start=START, end=END).num_rows == len(DAYS)


def test_reader_ticker_resolves_through_provider_specific_mappings(written):
    for symbol, iid in (("TESTUSDT", BIN), ("TEST-USDT", CB)):
        for market in (Market.CRYPTO, None):
            table = written.bars([symbol], market=market, start=START, end=END)
            assert set(table.column("instrument_id").to_pylist()) == {iid}
            assert table.num_rows == len(DAYS)
            assert set(table.column("policy_version").to_pylist()) == {"crypto-daily-v1"}
    assert written.bars(["NOPEUSDT"], market=Market.CRYPTO, start=START, end=END).num_rows == 0


def test_policy_version_for_market():
    from openbars.reconcile import policy_version_for_market as pv
    from openbars.storage.schema import DEFAULT_POLICY_VERSION

    assert pv(Market.CRYPTO) == "crypto-daily-v1"
    assert pv(Market.US) == pv(None) == DEFAULT_POLICY_VERSION == "research-daily-v2"
    assert pv(None, instrument_ids=[BIN]) == "crypto-daily-v1"
    assert pv(Market.CRYPTO, "custom-v9") == "custom-v9"
    assert pv(Market.US, "crypto-daily-v1") == "crypto-daily-v1"


def _us_row(day: date, close: str):
    from decimal import Decimal as D

    from openbars.core.models import Session
    from openbars.storage.rows import CanonicalBarRow

    c = D(close)
    return CanonicalBarRow(
        instrument_id="us:eq:T1", market=Market.US, interval=Interval.D1,
        ts_start=datetime(day.year, day.month, day.day, tzinfo=UTC), session_date=day,
        session=Session.REGULAR, provider="prov_a", open=c - 1, high=c + 2, low=c - 2, close=c,
        volume=D("4000000"), price_source="prov_a", reconcile_rule="primary_wins", adjustment="split",
        generated_at=datetime(day.year, day.month, day.day, 21, tzinfo=UTC),
    )  # fmt: skip


def test_us_bars_without_policy_version_still_research_daily_v2(written):
    backend = written.backend
    backend.canonical.upsert_rows([_us_row(date(2026, 3, 6), "120")])
    us = written.bars(["us:eq:T1"])
    assert us.column("policy_version").to_pylist() == ["research-daily-v2"]
    only_us = written.bars(start=START, end=END)  # no ids, no market: US default, crypto excluded
    assert set(only_us.column("instrument_id").to_pylist()) == {"us:eq:T1"}
    # mixed ids without a market: each id gets its own policy; crypto rows are never "adjusted"
    mixed = written.bars(["us:eq:T1", BIN], start=START, end=END)
    pairs = set(zip(mixed.column("instrument_id").to_pylist(), mixed.column("policy_version").to_pylist()))
    assert pairs == {("us:eq:T1", "research-daily-v2"), (BIN, "crypto-daily-v1")}
    assert mixed.num_rows == 1 + len(DAYS)


def test_crypto_read_ignores_adjustment_mode(written):
    from openbars.core.models import Adjustment

    for mode in (Adjustment.RAW, Adjustment.SPLIT, Adjustment.ALL, None):
        table = written.bars([BIN], adjustment=mode, start=START, end=END)
        assert table.num_rows == len(DAYS)
