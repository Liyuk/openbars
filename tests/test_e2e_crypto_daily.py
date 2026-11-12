"""End-to-end crypto daily path with synthetic providers: plan -> run -> canonical.

No network, no real prices: two fake venues (Binance ``TESTUSDT``, Coinbase ``TEST-USDT``),
a short UTC window that spans a weekend, Parquet + SQLite under tmp_path. Drives the planner and
``LocalOrchestrator`` directly (not ``openbars.cli.main``).
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest
from crypto_fakes import CryptoFake

from openbars.cli.wiring import build_providers
from openbars.config.loader import load_settings
from openbars.config.schema import Selection
from openbars.core.models import DateRange, Interval, Market
from openbars.normalize.crypto_pairs import CryptoPairListing, ingest_crypto_pairs
from openbars.orchestrator.finalize import (
    CryptoReconcilingSink,
    DispatchingSink,
    ReconcilingSink,
    parquet_canonical_writer,
)
from openbars.orchestrator.jobs import JobState
from openbars.orchestrator.nodes import LocalNode
from openbars.orchestrator.planner import DefaultPlanner, RawStoreCoverage
from openbars.orchestrator.runner import LocalOrchestrator
from openbars.reconcile.reconciler import MemoryReconciler
from openbars.storage.parquet_duckdb import open_backend

pytestmark = pytest.mark.usefixtures("no_network")

NOW = datetime(2026, 3, 10, 6, 0, tzinfo=UTC)  # latest ready UTC session: Mon 2026-03-09
START, END = date(2026, 3, 5), date(2026, 3, 9)  # Thu .. Mon, Sat/Sun included
DAYS = [START + timedelta(days=i) for i in range(5)]
BIN = "crypto:spot:binance:TEST-USDT"
CB = "crypto:spot:coinbase:TEST-USDT"
YEAR = DateRange(date(2026, 1, 1), date(2026, 12, 31))


def selection() -> Selection:
    return Selection(frozenset({Market.CRYPTO}), frozenset({Interval.D1}), START, None)


class World:
    def __init__(self, tmp_path, clock) -> None:
        self.backend = open_backend(tmp_path / "data")
        self.master = self.backend.instruments
        ingest_crypto_pairs(
            self.master,
            [
                CryptoPairListing("binance", "TEST", "USDT", "binance", "TESTUSDT"),
                CryptoPairListing("coinbase", "TEST", "USDT", "coinbase", "TEST-USDT"),
            ],
            as_of=date(2026, 1, 1),
        )
        self.providers = {
            "binance": CryptoFake("binance", base_price=100),
            "coinbase": CryptoFake("coinbase", base_price=500),
        }
        sink = DispatchingSink(
            us=ReconcilingSink(MemoryReconciler(), parquet_canonical_writer(self.backend.canonical)),
            crypto=CryptoReconcilingSink(
                self.master, parquet_canonical_writer(self.backend.canonical),
                raw=self.backend.raw, other_providers=tuple(self.providers),
            ),
        )  # fmt: skip
        self.node = LocalNode(
            self.providers, store=self.backend.state, raw=self.backend.raw, sink=sink, clock=clock
        )
        self.orch = LocalOrchestrator(
            self.backend.state, self.node,
            policies={n: p.quota_policy() for n, p in self.providers.items()},
            lock_path=tmp_path / "scheduler.lock", clock=clock, sleep=clock.sleep,
        )  # fmt: skip

    def plan(self):
        return DefaultPlanner().plan(
            selection(), RawStoreCoverage(self.backend.raw), now=NOW,
            universe={Market.CRYPTO: ["TESTUSDT", "TEST-USDT"]}, providers=self.providers,
        )  # fmt: skip


@pytest.fixture
def world(tmp_path, clock):
    return World(tmp_path, clock)


def test_plan_run_writes_raw_and_per_venue_canonical(world):
    plan = world.plan()
    assert not [n for n in plan.notes if n.code == "no_calendar"]
    assert world.orch.submit(plan.jobs) == len(plan.jobs) > 0
    report = world.orch.run(until_idle=True)
    assert (report.failed, report.dead) == (0, 0) and report.done == len(plan.jobs)
    assert all(world.backend.state.load_job(j.job_id).state is JobState.DONE for j in plan.jobs)

    # raw: one row per venue and UTC day (weekend included), as returned (no instrument id yet)
    for name, symbol in (("binance", "TESTUSDT"), ("coinbase", "TEST-USDT")):
        raw = list(world.backend.raw.scan(name, Market.CRYPTO, Interval.D1, YEAR))
        assert sorted(b.ts_start.date() for b in raw) == DAYS
        assert {b.symbol_as_returned for b in raw} == {symbol}

    rows = world.backend.canonical.scan_rows([BIN, CB], Interval.D1, YEAR, policy_version=None)
    assert len(rows) == 2 * len(DAYS)
    assert {r.policy_version for r in rows} == {"crypto-daily-v1"}
    for iid, venue, price in ((BIN, "binance", 100), (CB, "coinbase", 500)):
        mine = sorted((r for r in rows if r.instrument_id == iid), key=lambda r: r.session_date)
        assert [r.session_date for r in mine] == DAYS
        assert {r.provider for r in mine} == {venue}  # never mixed with the other venue
        assert [r.close for r in mine] == [Decimal(price + d.day + 1) for d in DAYS]


def test_second_plan_after_run_has_nothing_to_do_and_new_day_adds_only_it(world, clock):
    world.orch.submit(world.plan().jobs)
    world.orch.run(until_idle=True)
    assert world.plan().jobs == ()
    later = NOW + timedelta(days=1)
    plan = DefaultPlanner().plan(
        selection(), RawStoreCoverage(world.backend.raw), now=later,
        universe={Market.CRYPTO: ["TESTUSDT", "TEST-USDT"]}, providers=world.providers,
    )  # fmt: skip
    assert {(j.provider, j.window) for j in plan.jobs} == {
        ("binance", DateRange(date(2026, 3, 10), date(2026, 3, 10))),
        ("coinbase", DateRange(date(2026, 3, 10), date(2026, 3, 10))),
    }


def test_unresolved_symbol_stays_raw_only(tmp_path, clock):
    w = World(tmp_path, clock)
    sink = CryptoReconcilingSink(open_backend(tmp_path / "other").instruments, lambda rows, m: 1 / 0)
    node = LocalNode(w.providers, store=w.backend.state, raw=w.backend.raw, sink=sink, clock=clock)
    orch = LocalOrchestrator(
        w.backend.state, node, lock_path=tmp_path / "l2.lock", clock=clock, sleep=clock.sleep
    )
    orch.submit(w.plan().jobs)
    report = orch.run(until_idle=True)  # empty master: nothing resolves, so the writer is never called
    assert report.failed == report.dead == 0
    assert list(w.backend.raw.scan("binance", Market.CRYPTO, Interval.D1, YEAR))
    assert w.backend.canonical.scan_rows([BIN, CB], Interval.D1, YEAR, policy_version=None) == []


def test_us_sink_leaves_crypto_jobs_alone(world):
    us_only = ReconcilingSink(MemoryReconciler(), parquet_canonical_writer(world.backend.canonical))
    world.node.sink = us_only
    world.orch.submit(world.plan().jobs)
    world.orch.run(until_idle=True)
    assert world.backend.canonical.scan_rows([BIN, CB], Interval.D1, YEAR, policy_version=None) == []
    assert list(world.backend.raw.scan("binance", Market.CRYPTO, Interval.D1, YEAR))


def test_cross_venue_raw_never_corroborates_other_instrument(world):
    """A wrong-venue bar on the other venue's instrument id is rejected, not used."""
    from dataclasses import replace

    from openbars.orchestrator.jobs import Job, JobKind, make_job_id

    bar = replace(CryptoFake("coinbase").bar("TEST-USDT", START, "x"), instrument_id=BIN)
    job = Job(
        make_job_id(JobKind.DAILY, "coinbase", Market.CRYPTO, Interval.D1, ("TEST-USDT",), DateRange(START, START)),
        JobKind.DAILY, "coinbase", Market.CRYPTO, Interval.D1, ("TEST-USDT",), DateRange(START, START),
    )  # fmt: skip
    world.node.sink.crypto.after_fetch(job, [bar])
    assert world.backend.canonical.scan_rows([BIN, CB], Interval.D1, YEAR, policy_version=None) == []


def test_make_sink_factory_dispatches_by_market(tmp_path):
    from openbars.cli.wiring import make_sink

    sink = make_sink(open_backend(tmp_path / "data"), {})
    assert isinstance(sink, DispatchingSink)
    assert isinstance(sink.us, ReconcilingSink) and isinstance(sink.crypto, CryptoReconcilingSink)


def test_build_providers_constructs_crypto_providers_without_keys(tmp_path):
    cfg = tmp_path / "config.yaml"
    cfg.write_text(
        f"""
version: 1
markets: [crypto]
intervals: [1d]
universe:
  crypto:
    symbols: [TESTUSDT, TEST-USDT]
storage: {{backend: parquet_duckdb, path: {tmp_path / "data"}}}
providers:
  binance: {{enabled: true, base_url: "https://binance.example.invalid"}}
  coinbase: {{enabled: true, base_url: "https://coinbase.example.invalid"}}
"""
    )
    settings = load_settings(cfg, env={})
    providers = build_providers(settings)
    assert sorted(providers) == ["binance", "coinbase"]
    assert providers["binance"]._base_url == "https://binance.example.invalid"
    plan = DefaultPlanner().plan(
        settings.selection, RawStoreCoverage(open_backend(tmp_path / "data").raw), now=NOW,
        universe=settings.universe, providers=providers,
    )  # fmt: skip
    assert {j.provider for j in plan.jobs} == {"binance", "coinbase"}
    assert {j.instruments for j in plan.jobs if j.provider == "binance"} == {("TESTUSDT",)}
    assert {j.instruments for j in plan.jobs if j.provider == "coinbase"} == {("TEST-USDT",)}
