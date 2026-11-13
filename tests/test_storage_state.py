import sqlite3
from datetime import UTC, date, datetime

import pytest

from openbars.core.models import DateRange, Interval, Market
from openbars.orchestrator.jobs import WHOLE_MARKET, Checkpoint, Job, JobKind, JobState
from openbars.storage.schema import STATE_TABLES
from openbars.storage.state import SqliteStateStore

WINDOW = DateRange(date(2024, 1, 2), date(2024, 1, 5))


@pytest.fixture
def state(tmp_path):
    return SqliteStateStore(tmp_path / "data" / "state.db")


def make_job(**kw):
    base = dict(
        job_id="job-1", kind=JobKind.BACKFILL, provider="prov_a", market=Market.US,
        interval=Interval.D1, instruments=WHOLE_MARKET, window=WINDOW,
    )
    base.update(kw)
    return Job(**base)


def test_tables_and_no_quota_tables(state):
    con = sqlite3.connect(state.path)
    names = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {t.name for t in STATE_TABLES} <= names
    assert not any(n.startswith("quota_") for n in names)
    version = con.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()[0]
    assert version == "1"


def test_job_roundtrip_and_update(state):
    job = make_job(instruments=("TEST1", "TEST2"), node="local")
    state.save_job(job)
    assert state.load_job("job-1") == job
    retry = datetime(2024, 1, 6, 12, tzinfo=UTC)
    updated = make_job(
        instruments=("TEST1", "TEST2"), node="local", state=JobState.FAILED, attempts=2,
        retry_at=retry, last_error="429",
    )
    state.save_job(updated)
    assert state.load_job("job-1") == updated
    assert state.load_job("missing") is None
    state.save_job(make_job(job_id="job-2", priority=1))
    assert [j.job_id for j in state.list_jobs()] == ["job-2", "job-1"]
    assert [j.job_id for j in state.list_jobs(state=JobState.FAILED)] == ["job-1"]


def test_checkpoint_roundtrip(state, tmp_path):
    assert state.load_checkpoint("job-1") is None
    t1 = datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)
    state.save_checkpoint(Checkpoint("job-1", "page-2", t1))
    assert state.load_checkpoint("job-1") == Checkpoint("job-1", "page-2", t1)
    t2 = datetime(2024, 1, 3, tzinfo=UTC)
    state.save_checkpoint(Checkpoint("job-1", None, t2))
    assert state.load_checkpoint("job-1") == Checkpoint("job-1", None, t2)
    # survives a reopen (restart after kill)
    assert SqliteStateStore(state.path).load_checkpoint("job-1").updated_at == t2


def test_fetch_log_lifecycle(state):
    state.start_fetch(
        provider="prov_a", fetch_id="f1", market=Market.US, interval=Interval.D1,
        window=WINDOW, job_id="job-1", started_at=datetime(2024, 1, 2, tzinfo=UTC),
    )
    entry = state.get_fetch("prov_a", "f1")
    assert entry.status == "started" and entry.finished_at is None and entry.window == WINDOW
    state.finish_fetch("prov_a", "f1", "ok", artifact_ref="provider=prov_a/x.json", row_count=4)
    entry = state.get_fetch("prov_a", "f1")
    assert (entry.status, entry.artifact_ref, entry.row_count) == ("ok", "provider=prov_a/x.json", 4)
    assert entry.finished_at is not None and entry.job_id == "job-1"
    state.start_fetch(provider="prov_b", fetch_id="f1", market=Market.US, interval=Interval.D1, window=WINDOW)
    state.finish_fetch("prov_b", "f1", "error", error_code="http_429", error_message="slow down")
    assert [e.provider for e in state.list_fetches(status="error")] == ["prov_b"]
    assert len(state.list_fetches(market=Market.US, interval=Interval.D1)) == 2
    assert [e.fetch_id for e in state.list_fetches(provider="prov_a")] == ["f1"]
    with pytest.raises(ValueError):
        state.finish_fetch("prov_a", "f1", "started")
    with pytest.raises(KeyError):
        state.finish_fetch("prov_a", "nope", "ok")
    # restarting a fetch resets it
    state.start_fetch(provider="prov_a", fetch_id="f1", market=Market.US, interval=Interval.D1, window=WINDOW)
    assert state.get_fetch("prov_a", "f1").status == "started"
