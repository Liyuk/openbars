"""#151: dead / failed jobs are only re-queued by an explicit ``openbars jobs retry``."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from fakes import always_fail
from test_cli_plan_run_status import NOW, SECRET, cli, factory, state

from openbars.core.errors import ProviderError
from openbars.core.models import DateRange, Interval, Market
from openbars.orchestrator.jobs import Job, JobKind, JobState
from openbars.storage.state import SqliteStateStore

pytestmark = pytest.mark.usefixtures("no_network")

PERMANENT = ProviderError("invalid_request", "window rejected by provider", retryable=False, status_code=400)


def test_dead_jobs_stay_dead_without_an_explicit_retry(tmp_path, capsys):
    assert cli(["run"], tmp_path, factory(behavior=always_fail(PERMANENT))) == 0
    dead = state(tmp_path).list_jobs(state=JobState.DEAD)
    assert len(dead) == 6
    # re-planning the same missing sessions yields the same job ids, which stay dead
    assert cli(["plan", "--submit"], tmp_path) == 0
    assert cli(["run"], tmp_path) == 0
    assert state(tmp_path).count_jobs()[JobState.DEAD] == 6
    assert state(tmp_path).count_jobs()[JobState.DONE] == 0


def test_explicit_retry_requeues_and_the_next_run_executes_them(tmp_path, capsys):
    assert cli(["run"], tmp_path, factory(behavior=always_fail(PERMANENT))) == 0
    capsys.readouterr()
    assert cli(["jobs", "retry", "--dry-run"], tmp_path) == 0
    out = capsys.readouterr().out
    assert "dry run: 6 dead jobs would be reset to pending" in out and "window rejected by provider" in out
    assert state(tmp_path).count_jobs()[JobState.DEAD] == 6
    assert cli(["jobs", "retry"], tmp_path) == 0
    assert "retry: 6 dead jobs reset to pending" in capsys.readouterr().out
    jobs = state(tmp_path).list_jobs()
    assert {j.state for j in jobs} == {JobState.PENDING} and {j.attempts for j in jobs} == {0}
    assert all(j.retry_at is None and j.last_error for j in jobs)  # the old error is kept for the record
    assert cli(["run"], tmp_path) == 0
    assert "done 6" in capsys.readouterr().out
    assert state(tmp_path).count_jobs()[JobState.DONE] == 6
    assert cli(["jobs", "retry"], tmp_path) == 0
    assert "retry: 0 dead jobs" in capsys.readouterr().out


def test_retry_does_not_print_secrets_and_needs_a_state_db(tmp_path, capsys):
    assert cli(["jobs", "retry"], tmp_path) == 1
    assert "no state database" in capsys.readouterr().err
    leak = ProviderError("invalid_request", f"bad key {SECRET}", retryable=False)
    assert cli(["run"], tmp_path, factory(behavior=always_fail(leak))) == 0
    capsys.readouterr()
    assert cli(["jobs", "retry", "--dry-run"], tmp_path) == 0
    assert SECRET not in capsys.readouterr().out
    assert cli(["jobs"], tmp_path) == 2


def _job(job_id, provider="massive", market=Market.US, interval=Interval.D1, state=JobState.DEAD, error="boom"):
    day = datetime(2026, 3, 2, tzinfo=UTC).date()
    return Job(job_id, JobKind.DAILY, provider, market, interval, ("TEST1",), DateRange(day, day),
               state=state, attempts=5, last_error=error,
               retry_at=NOW + timedelta(hours=1) if state is JobState.FAILED else None)  # fmt: skip


def test_store_filters_and_state_guard(tmp_path):
    store = SqliteStateStore(tmp_path / "state.db")
    for job in (
        _job("a"), _job("b", provider="alpaca", error="response contains a symbol that was not requested"),
        _job("c", interval=Interval.H1, error="50% off_limits"), _job("d", state=JobState.FAILED),
        _job("e", state=JobState.DONE), _job("f", state=JobState.PENDING),
    ):  # fmt: skip
        store.save_job(job)
    ids = lambda jobs: sorted(j.job_id for j in jobs)  # noqa: E731
    assert ids(store.retry_jobs(provider="alpaca", dry_run=True)) == ["b"]
    assert ids(store.retry_jobs(interval=Interval.H1, dry_run=True)) == ["c"]
    assert ids(store.retry_jobs(error_contains="SYMBOL THAT", dry_run=True)) == ["b"]
    assert ids(store.retry_jobs(error_contains="50%", dry_run=True)) == ["c"]  # LIKE wildcards are literal
    assert ids(store.retry_jobs(error_contains="f_l", dry_run=True)) == ["c"]
    assert ids(store.retry_jobs(error_contains="0% o", dry_run=True)) == ["c"]
    assert ids(store.retry_jobs(error_contains="%", dry_run=True)) == ["c"]
    assert ids(store.retry_jobs(market=Market.CRYPTO, dry_run=True)) == []
    assert ids(store.retry_jobs(states=[JobState.FAILED], dry_run=True)) == ["d"]
    with pytest.raises(ValueError):
        store.retry_jobs(states=[JobState.DONE])
    with pytest.raises(ValueError):
        store.retry_jobs(states=[])
    assert ids(store.retry_jobs(states=[JobState.DEAD, JobState.FAILED], now=NOW)) == ["a", "b", "c", "d"]
    after = {j.job_id: j for j in store.list_jobs()}
    assert {k for k, j in after.items() if j.state is JobState.PENDING} == {"a", "b", "c", "d", "f"}
    assert after["d"].retry_at is None and after["d"].attempts == 0
    assert after["e"].state is JobState.DONE and after["e"].attempts == 5


@pytest.mark.postgres_live
def test_postgres_state_store_retry(pg_dsn):
    from pg_helpers import throwaway_schema

    from openbars.storage.postgres import PostgresBackend

    with throwaway_schema(pg_dsn) as schema:
        store = PostgresBackend(pg_dsn, mode="native", write=True, schema=schema).state
        for job in (_job("a"), _job("b", error="50% off"), _job("e", state=JobState.DONE)):
            store.save_job(job)
        assert [j.job_id for j in store.retry_jobs(error_contains="50%", dry_run=True)] == ["b"]
        assert [j.job_id for j in store.retry_jobs(error_contains="OFF", dry_run=True)] == ["b"]
        assert sorted(j.job_id for j in store.retry_jobs(now=NOW)) == ["a", "b"]
        assert store.count_jobs()[JobState.PENDING] == 2 and store.count_jobs()[JobState.DONE] == 1


def test_postgres_cli_opens_the_backend_writable_only_for_a_real_retry(tmp_path, monkeypatch, capsys):
    """Review fix: a read-only PG backend cannot run the UPDATE, so only ``--dry-run`` opens it
    read-only; a real retry honours the configured write flag (``write=None``)."""
    import argparse
    from types import SimpleNamespace

    import openbars.cli.main as main_mod
    from openbars.cli.jobs import run_jobs

    store = SqliteStateStore(tmp_path / "state.db")
    store.save_job(_job("a"))
    seen = []

    def fake_open(settings, root, *, write=None):
        seen.append(write)
        return SimpleNamespace(state=store)

    monkeypatch.setattr(main_mod, "_open_backend", fake_open)
    monkeypatch.setattr("openbars.cli.wiring.data_dir", lambda settings, data: tmp_path)
    settings = SimpleNamespace(storage_backend="postgres")
    base = dict(jobs_action="retry", data=None, dead=False, failed=False, provider=None, market=None,
                interval=None, error=None)  # fmt: skip
    out = SimpleNamespace(line=lambda *a: None, err=lambda *a: None)
    assert run_jobs(argparse.Namespace(**base, dry_run=True), settings, out, lambda: NOW) == 0
    assert run_jobs(argparse.Namespace(**base, dry_run=False), settings, out, lambda: NOW) == 0
    assert seen == [False, None]
    assert store.list_jobs()[0].state is JobState.PENDING


@pytest.mark.postgres_live
def test_postgres_read_only_state_lists_but_refuses_a_real_retry(pg_dsn):
    from pg_helpers import throwaway_schema

    from openbars.storage.errors import ReadOnlyError
    from openbars.storage.postgres import PostgresBackend

    with throwaway_schema(pg_dsn) as schema:
        PostgresBackend(pg_dsn, mode="native", write=True, schema=schema).state.save_job(_job("a"))
        ro = PostgresBackend(pg_dsn, mode="native", write=False, schema=schema).state
        assert [j.job_id for j in ro.retry_jobs(dry_run=True)] == ["a"]
        with pytest.raises(ReadOnlyError):
            ro.retry_jobs(now=NOW)
