"""CLI-level A-share: ``markets: [cn]`` plans / runs BaoStock jobs, writes cn-daily-v1 bars.

Offline: ``CnFake`` (BaoStock-shaped, one real XSHG week with a suspended Wednesday and a limit-up
Thursday) is injected through ``provider_factory``; Parquet + SQLite under tmp_path. Drives
``openbars.cli.main`` ``plan`` / ``run``; the read side uses the backend's bar reader (QFQ / HFQ).
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal as D
from zoneinfo import ZoneInfo

import pytest
from cn_fakes import PRICES, WEEK, CnFake

from openbars.cli.main import main
from openbars.core.models import Adjustment, DateRange, Interval, Market
from openbars.normalize.cn_quality import suspended_dates
from openbars.providers.cn.baostock import BaoStockAdjustFactor
from openbars.storage.parquet_duckdb import open_backend

pytestmark = pytest.mark.usefixtures("no_network")

NOW = datetime(2026, 3, 6, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai"))  # an hour after Friday's close
SUSPENDED_DAY, LIMIT_DAY = date(2026, 3, 4), date(2026, 3, 5)
IID = "cn:eq:xshg:600000"
YEAR = DateRange(date(2026, 1, 1), date(2026, 12, 31))
FACTOR = BaoStockAdjustFactor("sh.600000", LIMIT_DAY, D("0.8"), D("1.25"), D("1.25"))  # ex-date: the limit-up day


def config(tmp_path) -> str:
    path = tmp_path / "config.yaml"
    path.write_text(
        f"""
version: 1
markets: [cn]
intervals: [1d]
history: {{start: 2026-03-02, end: null}}
universe:
  cn:
    symbols: [sh.600000]
storage: {{backend: parquet_duckdb, path: {tmp_path / "data"}}}
providers:
  baostock: {{enabled: true}}
"""
    )
    return str(path)


def make_factory(**kw):
    fake = CnFake(factors={"sh.600000": [FACTOR]}, **kw)
    return fake, lambda settings, gate: {"baostock": fake}


def cli(argv, tmp_path, factory):
    return main(["--config", config(tmp_path), *argv], provider_factory=factory, clock=lambda: NOW, env={})


def test_plan_dry_run_prints_jobs_and_writes_nothing(tmp_path, capsys):
    fake, factory = make_factory()
    assert cli(["plan"], tmp_path, factory) == 0
    out = capsys.readouterr().out
    assert "plan: 2 jobs (2 new, 0 already queued)" in out and "baostock" in out
    assert "nothing written" in out
    assert fake.requests == [] and fake.factor_requests == []  # a dry run never fetches
    assert list((tmp_path / "data").rglob("*.parquet")) == []  # no raw / canonical / instruments
    assert open_backend(tmp_path / "data").state.list_jobs() == []  # no jobs queued


def test_run_writes_raw_and_cn_canonical_with_flags(tmp_path, capsys):
    fake, factory = make_factory()
    assert cli(["run"], tmp_path, factory) == 0
    out = capsys.readouterr().out
    assert "run: claimed 2, done 2, failed 0, dead 0" in out and "no usable provider" not in out

    backend = open_backend(tmp_path / "data")
    raw = list(backend.raw.scan("baostock", Market.CN, Interval.D1, YEAR))
    assert [b.session_date for b in raw] == WEEK
    halted = next(b for b in raw if b.session_date == SUSPENDED_DAY)
    assert halted.volume == 0 and halted.extra["tradestatus"] == 0  # kept in raw, with tradestatus (#94)
    assert {b.session_date: b.extra["tradestatus"] for b in raw} == {d: PRICES[d][-1] for d in WEEK}
    assert suspended_dates(raw) == {SUSPENDED_DAY}

    rows = sorted(backend.canonical.scan_rows([IID], Interval.D1, YEAR, policy_version=None), key=lambda r: r.session_date)
    assert [r.session_date for r in rows] == [d for d in WEEK if d != SUSPENDED_DAY]  # the suspended day is dropped
    assert {r.policy_version for r in rows} == {"cn-daily-v1"} and {r.adjustment for r in rows} == {"raw"}
    flags = {r.session_date: r.flags for r in rows}
    assert flags[LIMIT_DAY] == ("LIMIT_UP",)
    assert all(f == () for d, f in flags.items() if d != LIMIT_DAY)


def test_run_ingests_factors_from_the_full_history_start_and_qfq_hfq_work(tmp_path):
    fake, factory = make_factory()
    assert cli(["run"], tmp_path, factory) == 0
    assert date(1990, 1, 1) in {r.window.start for r in fake.factor_requests}  # first run: full history

    backend = open_backend(tmp_path / "data")
    assert [f.effective_from for f in backend.instruments.adjust_factors([IID])] == [LIMIT_DAY]
    reader = backend.reader
    hfq = reader.bars([IID], adjustment=Adjustment.HFQ)
    assert hfq.column("close").to_pylist() == [D("10.05"), D("10.15"), D("13.9625"), D("13.75")]
    qfq = reader.bars(["sh.600000"], adjustment=Adjustment.QFQ, market=Market.CN)
    assert qfq.column("close").to_pylist() == [D("8.04"), D("8.12"), D("11.17"), D("11")]
    assert set(qfq.column("adjustment").to_pylist()) == {"qfq"}


def test_second_run_refetches_nothing(tmp_path, capsys):
    fake, factory = make_factory()
    assert cli(["run"], tmp_path, factory) == 0
    bars, factors = len(fake.requests), len(fake.factor_requests)
    capsys.readouterr()
    assert cli(["run"], tmp_path, factory) == 0
    assert "plan: 0 jobs" in capsys.readouterr().out
    assert (len(fake.requests), len(fake.factor_requests)) == (bars, factors)


def test_verify_cn_only_config_reports_xshg_raw_limit_up_and_the_halt(tmp_path, capsys):
    fake, factory = make_factory()
    assert cli(["run"], tmp_path, factory) == 0
    capsys.readouterr()
    assert cli(["verify", "--json", "reports/verify.json"], tmp_path, factory) == 0
    captured = capsys.readouterr()
    out = captured.out
    assert "us market only" not in captured.err + out
    assert "market cn" in out and "cn-daily-v1" in out and "XSHG" in out
    assert "2026-03-02..2026-03-06" in out and "expected sessions 5" in out
    assert "stored RAW" in out  # QFQ / HFQ are not what verify scores
    assert "suspended 1 (expected)" in out and "missing 0" in out  # the halted Wednesday is no ordinary hole
    assert "LIMIT_UP=1" in out
    assert "canonical status: complete=4" in out

    import json

    report = json.loads((tmp_path / "data" / "reports" / "verify.json").read_text())
    assert report["market"] == "cn" and report["policy_version"] == "cn-daily-v1"  # single market: plain object
    whole = report["periods"][0]
    assert whole["coverage"]["raw_cells"] == 5 and whole["coverage"]["canonical_cells"] == 4
    assert whole["coverage"]["suspended_cells"] == 1 and whole["coverage"]["missing_cells"] == 0
    assert whole["quality"]["flags"] == {"LIMIT_UP": 1}
