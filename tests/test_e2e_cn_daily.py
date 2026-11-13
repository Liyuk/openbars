"""End-to-end A-share daily path with a synthetic provider: plan -> run -> canonical (#62).

No network, no real prices: a fake BaoStock-shaped provider on one real XSHG week
(2026-03-02 Mon .. 03-06 Fri) with a suspended Wednesday and a limit-up Thursday, Parquet + SQLite
under tmp_path. Drives the planner and ``LocalOrchestrator`` directly (not ``openbars.cli.main``).
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal as D
from zoneinfo import ZoneInfo

import pytest
from cn_fakes import PRICES, WEEK, CnFake

from openbars.calendars import cn as cn_calendar
from openbars.cli.wiring import build_providers, make_sink
from openbars.config.loader import load_settings
from openbars.config.schema import Selection
from openbars.core.errors import ProviderError
from openbars.core.models import Adjustment, DateRange, Interval, Market, QualityFlag
from openbars.normalize.cn_ingest import ensure_cn_instruments
from openbars.normalize.cn_quality import missing_sessions, suspended_dates
from openbars.orchestrator.finalize import (
    CnCanonicalSink,
    DispatchingSink,
    ReconcilingSink,
    parquet_canonical_writer,
)
from openbars.orchestrator.jobs import Job, JobKind, JobState, make_job_id
from openbars.orchestrator.nodes import LocalNode
from openbars.orchestrator.planner import DefaultPlanner, RawStoreCoverage
from openbars.orchestrator.runner import LocalOrchestrator
from openbars.providers.cn.baostock import BaoStockAdjustFactor
from openbars.reconcile.reconciler import MemoryReconciler
from openbars.storage.parquet_duckdb import open_backend

pytestmark = pytest.mark.usefixtures("no_network")

NOW = datetime(2026, 3, 6, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai"))  # an hour after Friday's close
START = date(2026, 3, 2)
SUSPENDED_DAY, LIMIT_DAY = date(2026, 3, 4), date(2026, 3, 5)
IID = "cn:eq:xshg:600000"
YEAR = DateRange(date(2026, 1, 1), date(2026, 12, 31))
FACTOR = BaoStockAdjustFactor("sh.600000", LIMIT_DAY, D("0.8"), D("1.25"), D("1.25"))  # ex-date: the limit-up day


def selection() -> Selection:
    return Selection(frozenset({Market.CN}), frozenset({Interval.D1}), START, None)


class World:
    def __init__(self, tmp_path, clock, provider=None) -> None:
        self.backend = open_backend(tmp_path / "data")
        self.provider = provider or CnFake(factors={"sh.600000": [FACTOR]})
        self.providers = {"baostock": self.provider}
        self.sink = make_sink(self.backend, self.providers)
        self.node = LocalNode(
            self.providers, store=self.backend.state, raw=self.backend.raw, sink=self.sink, clock=clock
        )
        self.orch = LocalOrchestrator(
            self.backend.state, self.node,
            policies={n: p.quota_policy() for n, p in self.providers.items()},
            lock_path=tmp_path / "scheduler.lock", clock=clock, sleep=clock.sleep,
        )  # fmt: skip

    def plan(self, universe=("sh.600000",)):
        return DefaultPlanner().plan(
            selection(), RawStoreCoverage(self.backend.raw), now=NOW,
            universe={Market.CN: list(universe)}, providers=self.providers,
        )  # fmt: skip

    def run(self, universe=("sh.600000",)):
        plan = self.plan(universe)
        assert self.orch.submit(plan.jobs) == len(plan.jobs) > 0
        report = self.orch.run(until_idle=True)
        return plan, report

    def canonical(self):
        return self.backend.canonical.scan_rows([IID], Interval.D1, YEAR, policy_version=None)


@pytest.fixture
def world(tmp_path, clock):
    return World(tmp_path, clock)


def test_plan_run_writes_raw_and_canonical_with_cn_policy(world):
    plan, report = world.run()
    assert not [n for n in plan.notes if n.code == "no_calendar"]
    assert (report.failed, report.dead) == (0, 0) and report.done == len(plan.jobs)
    assert all(world.backend.state.load_job(j.job_id).state is JobState.DONE for j in plan.jobs)

    raw = list(world.backend.raw.scan_rows("baostock", Market.CN, Interval.D1, YEAR))
    assert [b.session_date for b in raw] == WEEK  # the Shanghai trading day, not the UTC date of 00:00 CST
    assert {b.symbol_as_returned for b in raw} == {"sh.600000"}
    halted = next(b for b in raw if b.session_date == SUSPENDED_DAY)
    assert halted.volume == 0  # the raw layer keeps the suspension placeholder

    rows = sorted(world.canonical(), key=lambda r: r.session_date)
    assert [r.session_date for r in rows] == [d for d in WEEK if d != SUSPENDED_DAY]  # no placeholder bar
    assert {r.policy_version for r in rows} == {"cn-daily-v1"}
    assert {(r.adjustment, r.market, r.provider, r.feed) for r in rows} == {("raw", Market.CN, "baostock", "ashare")}
    assert {r.quality_status for r in rows} == {"complete"}
    by_day = {r.session_date: r for r in rows}
    assert [by_day[d].close for d in by_day] == [D(PRICES[d][3]) for d in by_day]
    assert by_day[LIMIT_DAY].close == D("11.17") and by_day[LIMIT_DAY].flags == ("LIMIT_UP",)
    assert all(r.flags == () for d, r in by_day.items() if d != LIMIT_DAY)
    assert all(r.volume > 0 for r in rows)


def test_suspended_session_is_not_a_missing_session(world):
    world.run()
    raw = list(world.backend.raw.scan("baostock", Market.CN, Interval.D1, YEAR))
    present = {r.session_date for r in world.canonical()}
    sessions = cn_calendar.sessions(START, date(2026, 3, 8))  # the weekend adds no session
    assert sessions == WEEK
    assert suspended_dates(raw) == {SUSPENDED_DAY}
    assert missing_sessions(sessions, present, suspended_dates(raw)) == []
    assert missing_sessions(sessions, present) == [SUSPENDED_DAY]  # canonical alone shows a hole


def test_instruments_are_registered_with_deterministic_generation_one_ids(world):
    world.run()
    inst = world.backend.instruments.get(IID)
    assert (inst.market, inst.currency, inst.exchange) == (Market.CN, "CNY", "XSHG")
    master = world.backend.instruments
    for symbol, provider in (("sh.600000", "baostock"), ("sh.600000", "*"), ("600000.SH", "*")):
        assert master.resolve(symbol, on=date(2026, 3, 5), provider=provider, market=Market.CN) == IID


def test_second_plan_after_run_has_nothing_to_do(world):
    world.run()
    assert world.plan().jobs == ()
    assert world.plan(universe=("600000.SH",)).jobs == ()  # another spelling of the same stock


def test_adjust_factors_are_ingested_and_qfq_hfq_read_back(world):
    world.run()
    stored = world.backend.instruments.adjust_factors([IID])
    assert [(f.effective_from, f.source, f.method, f.back_adjust_factor, f.symbol) for f in stored] == [
        (LIMIT_DAY, "baostock", "pct_return", D("1.25"), "sh.600000")
    ]
    reader = world.backend.reader
    default = reader.bars([IID], market=Market.CN)
    assert default.column("close").to_pylist()[-1] == D("11.00")
    assert set(default.column("adjustment").to_pylist()) == {"raw"}
    hfq = reader.bars([IID], adjustment=Adjustment.HFQ)
    assert hfq.column("close").to_pylist() == [D("10.05"), D("10.15"), D("13.9625"), D("13.75")]
    qfq = reader.bars(["sh.600000"], adjustment=Adjustment.QFQ, start=START, end=date(2026, 3, 6), market=Market.CN)
    assert qfq.column("close").to_pylist() == [D("8.04"), D("8.12"), D("11.17"), D("11")]
    assert qfq.column("volume").to_pylist() == [D(PRICES[d][4]) for d in WEEK if d != SUSPENDED_DAY]  # as traded
    assert qfq.column("flags").to_pylist()[2] == ["LIMIT_UP"]  # marks survive the derivation
    assert set(qfq.column("adjustment").to_pylist()) == {"qfq"}


def test_provider_without_ex_dates_gets_a_seed_factor_so_qfq_is_the_identity(tmp_path, clock):
    w = World(tmp_path, clock, provider=CnFake())  # the provider knows no ex-dates
    w.run()
    (seed,) = w.backend.instruments.adjust_factors()  # one seed (a successful empty full history)
    assert (seed.instrument_id, seed.effective_from, seed.source, seed.method) == (
        IID, date(2026, 3, 6), "baostock", "pct_return",
    )  # watermark = job end, not 1990-01-01
    assert {seed.fore_adjust_factor, seed.back_adjust_factor, seed.adjust_factor} == {D(1)}
    assert w.backend.canonical.scan_rows([IID], Interval.D1, YEAR, policy_version=None)  # no CorporateAction needed
    raw = w.backend.reader.bars([IID])
    qfq = w.backend.reader.bars([IID], adjustment=Adjustment.QFQ)
    assert qfq.column("close").to_pylist() == raw.column("close").to_pylist()
    assert set(qfq.column("adjustment").to_pylist()) == {"qfq"}
    assert not any("ADJUSTMENT_UNVERIFIED" in f for f in qfq.column("flags").to_pylist())


def test_without_factors_qfq_raises_or_flags(tmp_path, clock):
    class Down(CnFake):  # every factor fetch fails: nothing stored, nothing attested
        def fetch_adjust_factors(self, request):
            raise ProviderError("provider_unavailable", "down", retryable=True)

    w = World(tmp_path, clock, provider=Down())
    w.run()
    assert w.backend.instruments.adjust_factors() == []
    from openbars.normalize.adjust import AdjustmentError

    with pytest.raises(AdjustmentError) as e:
        w.backend.reader.bars([IID], adjustment=Adjustment.QFQ)
    assert e.value.reason_code == "missing_adjust_factors"
    t = w.backend.reader.bars([IID], adjustment=Adjustment.QFQ, on_missing_factors="flag")
    assert set(t.column("adjustment").to_pylist()) == {"raw"}
    assert all("ADJUSTMENT_UNVERIFIED" in f for f in t.column("flags").to_pylist())


def test_factor_outage_does_not_fail_the_job(tmp_path, clock):
    class Broken(CnFake):
        def fetch_adjust_factors(self, request):
            raise ProviderError("provider_unavailable", "down", retryable=True)

    w = World(tmp_path, clock, provider=Broken())
    _, report = w.run()
    assert (report.failed, report.dead) == (0, 0)
    assert len(w.canonical()) == 4  # bars are written
    assert w.sink.cn.factor_errors and {c for _, c in w.sink.cn.factor_errors} == {"provider_unavailable"}


def test_first_run_pulls_full_history_then_factor_runs_are_incremental(tmp_path, clock):
    w = World(tmp_path, clock)
    w.run()
    first = {r.window.start for r in w.provider.factor_requests}
    assert date(1990, 1, 1) in first  # nothing stored yet: from 1990-01-01 (no listing date known)
    assert {r.window.end for r in w.provider.factor_requests} <= {r.window.end for r in w.provider.requests}
    assert [f.effective_from for f in w.backend.instruments.adjust_factors([IID])] == [LIMIT_DAY]
    # a later job: incremental from the last stored effective_from (inclusive), not from 1990
    w.provider.factor_requests.clear()
    raw = list(w.backend.raw.scan("baostock", Market.CN, Interval.D1, YEAR))
    w.sink.cn.after_fetch(_job(), raw)
    assert [r.window for r in w.provider.factor_requests] == [DateRange(LIMIT_DAY, date(2026, 3, 6))]


def test_first_run_starts_at_the_listing_date_when_known(tmp_path, clock):
    w = World(tmp_path, clock)
    listed = date(1999, 11, 10)
    ensure_cn_instruments(w.backend.instruments, ["sh.600000"], as_of=START, listed_on=listed)
    w.run()
    assert min(r.window.start for r in w.provider.factor_requests) == listed


def test_factor_start_overrides_the_window_start(tmp_path, clock):
    w = World(tmp_path, clock)
    w.sink.cn.factor_start = date(2000, 1, 1)
    w.run()
    assert {r.window.start for r in w.provider.factor_requests} == {date(2000, 1, 1)}
    assert {r.window.end for r in w.provider.factor_requests} <= {r.window.end for r in w.provider.requests}


def test_us_sink_and_dispatcher_without_cn_leave_cn_jobs_raw_only(world):
    us_only = ReconcilingSink(MemoryReconciler(), parquet_canonical_writer(world.backend.canonical))
    world.node.sink = DispatchingSink(us=us_only)  # no cn= sink: raw only, never the XNYS reconciler
    world.orch.submit(world.plan().jobs)
    world.orch.run(until_idle=True)
    assert world.canonical() == []
    raw = list(world.backend.raw.scan("baostock", Market.CN, Interval.D1, YEAR))
    assert len(raw) == len(WEEK)
    us_only.after_fetch(_job(), raw)  # a US sink handed CN bars directly ignores them too
    assert world.canonical() == []


def _job(day=START, end=date(2026, 3, 6), symbol="sh.600000"):
    win = DateRange(day, end)
    return Job(
        make_job_id(JobKind.DAILY, "baostock", Market.CN, Interval.D1, (symbol,), win),
        JobKind.DAILY, "baostock", Market.CN, Interval.D1, (symbol,), win,
    )  # fmt: skip


def test_zero_volume_trading_day_is_kept_but_suspended_is_dropped(world):
    provider = world.provider
    traded = provider.bar("sh.600000", date(2026, 3, 2), "x")
    from dataclasses import replace
    from decimal import Decimal

    quiet = replace(traded, volume=Decimal(0), extra={**traded.extra, "tradestatus": 1})
    halted = replace(provider.bar("sh.600000", SUSPENDED_DAY, "x"), volume=Decimal(0))
    world.sink.cn.after_fetch(_job(), [quiet, halted])
    rows = world.canonical()
    assert [(r.session_date, r.volume) for r in rows] == [(date(2026, 3, 2), D(0))]


def test_unrecognised_symbol_stays_raw_only_and_flags_come_from_the_previous_close(world):
    from dataclasses import replace

    provider = world.provider
    bare = replace(provider.bar("sh.600000", date(2026, 3, 2), "x"), symbol_as_returned="600000")
    world.sink.cn.after_fetch(_job(), [bare])
    assert world.canonical() == [] and world.backend.instruments.instruments() == []
    # no extra["preclose"]: the previous raw close (10.00 -> 11.00 is +10%) is the reference
    a = replace(provider.bar("sh.600000", date(2026, 3, 2), "x"), close=D("10.00"), open=D("10.00"), high=D("10.00"), low=D("10.00"), extra={"tradestatus": 1})
    b = replace(provider.bar("sh.600000", date(2026, 3, 3), "x"), close=D("11.00"), open=D("11.00"), high=D("11.00"), low=D("11.00"), extra={"tradestatus": 1})
    world.sink.cn.after_fetch(_job(), [a, b])
    assert {r.session_date: r.flags for r in world.canonical()} == {date(2026, 3, 2): (), date(2026, 3, 3): ("LIMIT_UP",)}


def test_new_flags_are_distinct_bits():
    flags = [QualityFlag.LIMIT_UP, QualityFlag.LIMIT_DOWN, QualityFlag.SUSPENDED]
    assert len({f.value for f in flags}) == 3
    assert all(f not in QualityFlag.DIVERGENT | QualityFlag.GAP | QualityFlag.ADJUSTMENT_UNVERIFIED for f in flags)


def test_make_sink_factory_wires_the_cn_sink(tmp_path):
    sink = make_sink(open_backend(tmp_path / "data"), {})
    assert isinstance(sink, DispatchingSink) and isinstance(sink.cn, CnCanonicalSink)


def test_config_markets_cn_with_baostock_enabled_plans_jobs(tmp_path):
    cfg = tmp_path / "config.yaml"
    cfg.write_text(
        f"""
version: 1
markets: [cn]
intervals: [1d]
universe:
  cn:
    symbols: [sh.600000, 000001.SZ]
storage: {{backend: parquet_duckdb, path: {tmp_path / "data"}}}
providers:
  baostock: {{enabled: true}}
"""
    )
    settings = load_settings(cfg, env={})
    providers = build_providers(settings)
    assert sorted(providers) == ["baostock"]
    plan = DefaultPlanner().plan(
        Selection(settings.selection.markets, settings.selection.intervals, START, None),
        RawStoreCoverage(open_backend(tmp_path / "data").raw), now=NOW,
        universe=settings.universe, providers=providers,
    )  # fmt: skip
    assert not [n for n in plan.notes if n.code == "no_calendar"]
    assert {j.provider for j in plan.jobs} == {"baostock"}
    assert {s for j in plan.jobs for s in j.instruments} == {"sh.600000", "000001.SZ"}
