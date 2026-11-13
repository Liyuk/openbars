from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
from fakes import (
    FakeProvider,
    MemoryRawStore,
    SqliteSideStore,
    always_fail,
    fail_first,
)

from openbars.core.errors import ProviderError, QuotaExhausted
from openbars.core.models import DateRange, Interval, Market
from openbars.orchestrator.jobs import WHOLE_MARKET, Job, JobKind, JobState, make_job_id
from openbars.orchestrator.lock import SchedulerLock, SchedulerLocked
from openbars.orchestrator.nodes import LocalNode, RemoteNode
from openbars.orchestrator.runner import LocalOrchestrator, next_data_ready_time
from openbars.providers.base import BatchShape, RawBatch
from openbars.quota.policy import BackoffPolicy
from openbars.storage.state import SqliteStateStore

pytestmark = pytest.mark.usefixtures("no_network")


DAY = date(2026, 3, 3)


def job(symbol: str = "TEST1", *, provider: str = "fake", day: date = DAY, end: date | None = None,
        priority: int = 100, kind: JobKind = JobKind.BACKFILL) -> Job:  # fmt: skip
    window = DateRange(day, end or day)
    ins = (symbol,)
    return Job(make_job_id(kind, provider, Market.US, Interval.D1, ins, window), kind, provider,
               Market.US, Interval.D1, ins, window, priority)  # fmt: skip


class Env:
    def __init__(self, tmp_path: Path, clock, providers, *, workers: int = 1, ttl: float = 60.0, sink=None) -> None:
        self.clock = clock
        self.store = SqliteStateStore(tmp_path / "state.db")
        self.raw = MemoryRawStore()
        self.providers = {p.id: p for p in providers}
        self.node = LocalNode(self.providers, store=self.store, raw=self.raw, clock=clock, sink=sink)
        self.orch = LocalOrchestrator(
            self.store, self.node,
            policies={n: p.quota_policy() for n, p in self.providers.items()},
            workers=workers, lease_ttl=timedelta(seconds=ttl),
            lock_path=tmp_path / "scheduler.lock", clock=clock, sleep=clock.sleep,
        )  # fmt: skip

    def state(self, j: Job) -> Job:
        return self.store.load_job(j.job_id)


@pytest.fixture
def make_env(tmp_path, clock):
    def make(*providers, **kw):
        return Env(tmp_path, clock, providers, **kw)

    return make


def test_success_writes_raw_once_and_marks_done(make_env):
    fake = FakeProvider()
    env = make_env(fake)
    j = job(end=date(2026, 3, 5))
    assert env.orch.submit([j]) == 1 and env.orch.submit([j]) == 0
    report = env.orch.run()
    assert (report.done, report.failed, report.dead, report.requests) == (1, 0, 0, 1)
    done = env.state(j)
    assert done.state is JobState.DONE and done.last_error is None and done.attempts == 0
    assert len(env.raw.rows) == 3  # Mar 3, 4, 5
    cp = env.store.load_checkpoint(j.job_id)
    assert cp is not None and cp.cursor is None
    (entry,) = env.store.list_fetches(provider="fake")
    assert entry.status == "ok" and entry.row_count == 3 and entry.job_id == j.job_id
    # A finished job is not executed again.
    assert env.orch.run().claimed == 0 and len(fake.requests) == 1


def test_request_is_built_from_the_job(make_env):
    fake = FakeProvider("alpaca")
    env = make_env(fake)
    env.node.feeds["alpaca"] = "sip"
    j = job("TEST1", provider="alpaca")
    env.orch.submit([j])
    env.orch.run()
    (req,) = fake.requests
    assert (req.market, req.interval, req.symbols, req.window, req.feed, req.cursor) == (
        Market.US, Interval.D1, ("TEST1",), j.window, "sip", None,
    )  # fmt: skip


def test_whole_market_job_sends_no_symbols(make_env):
    fake = FakeProvider("massive")
    env = make_env(fake)
    j = Job(make_job_id(JobKind.DAILY, "massive", Market.US, Interval.D1, WHOLE_MARKET, DateRange(DAY, DAY)),
            JobKind.DAILY, "massive", Market.US, Interval.D1, WHOLE_MARKET, DateRange(DAY, DAY))  # fmt: skip
    env.orch.submit([j])
    env.orch.run()
    assert fake.requests[0].symbols == () and env.state(j).state is JobState.DONE


def test_pagination_checkpoint_resumes_after_failure(make_env, clock):
    def behavior(req, n):  # call 1 ok (page 1, next "1"); call 2 raises; later calls succeed
        if n == 2:
            raise ProviderError("timeout", "slow", retryable=True)

    fake = FakeProvider(page_size=1, behavior=behavior)
    env = make_env(fake)
    j = job(end=date(2026, 3, 5))
    env.orch.submit([j])
    env.orch.run()
    failed = env.state(j)
    assert failed.state is JobState.FAILED and failed.attempts == 1
    assert env.store.load_checkpoint(j.job_id).cursor == "1"
    assert len(env.raw.rows) == 1
    clock.advance(seconds=5)
    env.orch.run()
    assert env.state(j).state is JobState.DONE
    assert [r.cursor for r in fake.requests] == [None, "1", "1", "2"]  # resumed, not restarted
    assert len(env.raw.rows) == 3


def test_partial_batch_with_error_saves_cursor_and_retries(make_env, clock):
    class Partial(FakeProvider):
        def fetch_bars(self, request):
            self.requests.append(request)
            if len(self.requests) == 1:
                full = super().bar("TEST1", DAY, "p1")
                err = ProviderError("rate_limited", "later", retryable=True)
                return RawBatch(self.id, "p1", request, (full,), next_cursor="resume-7", error=err)
            return RawBatch(self.id, "p2", request, (super().bar("TEST1", DAY + timedelta(days=1), "p2"),))

    fake = Partial()
    env = make_env(fake)
    j = job(end=date(2026, 3, 4))
    env.orch.submit([j])
    env.orch.run()
    assert env.state(j).state is JobState.FAILED
    assert env.store.load_checkpoint(j.job_id).cursor == "resume-7"
    assert env.store.list_fetches(status="partial")
    clock.advance(minutes=1)
    env.orch.run()
    assert fake.requests[1].cursor == "resume-7" and env.state(j).state is JobState.DONE


def test_retryable_error_sets_retry_at_then_succeeds(make_env, clock):
    fake = FakeProvider(behavior=fail_first(1, ProviderError("timeout", "slow", retryable=True)))
    env = make_env(fake)
    j = job()
    env.orch.submit([j])
    first = env.orch.run()
    assert (first.failed, first.done) == (1, 0)
    s = env.state(j)
    assert s.state is JobState.FAILED and s.attempts == 1 and "slow" in s.last_error
    assert s.retry_at == clock.now + timedelta(seconds=1)
    assert env.orch.run().claimed == 0  # not due yet
    clock.advance(seconds=2)
    second = env.orch.run()
    assert second.done == 1
    s = env.state(j)
    assert s.state is JobState.DONE and s.last_error is None and s.attempts == 1
    assert len(env.raw.rows) == 1


def test_provider_retry_at_hint_only_delays_further(make_env, clock):
    hint = clock.now + timedelta(minutes=5)
    fake = FakeProvider(behavior=fail_first(1, ProviderError("rate_limited", "429", retryable=True, retry_at=hint)))
    env = make_env(fake)
    j = job()
    env.orch.submit([j])
    env.orch.run()
    assert env.state(j).retry_at == hint


def test_quota_exhausted_is_retryable_with_retry_at(make_env, clock):
    reset = clock.now + timedelta(minutes=10)

    def behavior(req, n):
        if n == 1:
            raise QuotaExhausted("fake", reset)

    env = make_env(FakeProvider(behavior=behavior))
    j = job()
    env.orch.submit([j])
    env.orch.run()
    s = env.state(j)
    assert s.state is JobState.FAILED and s.retry_at == reset
    clock.advance(minutes=11)
    assert env.orch.run().done == 1


def test_data_not_ready_retries_at_next_0015_new_york(make_env, clock):
    env = make_env(FakeProvider(behavior=fail_first(1, ProviderError("data_not_ready", "not yet", retryable=True))))
    j = job()
    env.orch.submit([j])
    env.orch.run()
    # fixture clock: 2026-10-15 12:00 UTC = 08:00 EDT -> next 00:15 EDT is 2026-10-16 04:15 UTC
    assert env.state(j).retry_at == datetime(2026, 10, 16, 4, 15, tzinfo=UTC)
    assert next_data_ready_time(datetime(2026, 10, 16, 4, 14, tzinfo=UTC)) == datetime(2026, 10, 16, 4, 15, tzinfo=UTC)


def test_non_retryable_error_goes_dead(make_env, clock):
    err = ProviderError("invalid_request", "bad window", retryable=False, status_code=400)
    fake = FakeProvider(behavior=always_fail(err))
    env = make_env(fake)
    j = job()
    env.orch.submit([j])
    report = env.orch.run()
    assert (report.dead, report.failed, report.done) == (1, 0, 0)
    s = env.state(j)
    assert s.state is JobState.DEAD and s.attempts == 1 and "bad window" in s.last_error
    clock.advance(days=1)
    assert env.orch.run().claimed == 0 and len(fake.requests) == 1  # dead jobs stay dead
    assert env.store.list_jobs(state=JobState.DEAD) == [s]


def test_attempts_exhausted_goes_dead(make_env, clock):
    backoff = BackoffPolicy(base_seconds=1, max_seconds=4, max_attempts=3, circuit_break_after=100)
    fake = FakeProvider(behavior=always_fail(ProviderError("timeout", "slow", retryable=True)), backoff=backoff)
    env = make_env(fake)
    j = job()
    env.orch.submit([j])
    for _ in range(5):
        env.orch.run()
        clock.advance(minutes=1)
    s = env.state(j)
    assert s.state is JobState.DEAD and s.attempts == 3 and len(fake.requests) == 3


def test_unexpected_exception_does_not_crash_the_run(make_env):
    def boom(req, n):
        raise RuntimeError("secret-looking-detail")

    env = make_env(FakeProvider(behavior=boom))
    j = job()
    env.orch.submit([j])
    env.orch.run()
    s = env.state(j)
    assert s.state is JobState.FAILED and "RuntimeError" in s.last_error and "secret" not in s.last_error


def test_unknown_provider_job_is_dead(make_env):
    env = make_env(FakeProvider())
    j = job(provider="nowhere")
    env.orch.submit([j])
    env.orch.run()
    assert env.state(j).state is JobState.DEAD


def test_max_jobs_limits_claims(make_env):
    env = make_env(FakeProvider())
    env.orch.submit([job(f"S{i}") for i in range(5)])
    assert env.orch.run(max_jobs=2).claimed == 2
    assert env.store.count_jobs()[JobState.PENDING] == 3
    assert env.orch.run().claimed == 3


def test_priority_order(make_env):
    fake = FakeProvider()
    env = make_env(fake)
    env.orch.submit([job("LOW", priority=50), job("HIGH", priority=0), job("MID", priority=20)])
    env.orch.run()
    assert [r.symbols[0] for r in fake.requests] == ["HIGH", "MID", "LOW"]


def test_worker_pool_runs_every_job_once(make_env):
    fake = FakeProvider(delay=0.001)
    env = make_env(fake, workers=4)
    jobs = [job(f"S{i:03d}") for i in range(60)]
    env.orch.submit(jobs)
    report = env.orch.run()
    assert report.done == 60
    assert sorted(r.symbols[0] for r in fake.requests) == sorted(j.instruments[0] for j in jobs)
    assert env.store.count_jobs()[JobState.DONE] == 60


# --- circuit breaker -----------------------------------------------------------------------


def test_open_circuit_on_one_provider_does_not_block_another(make_env, clock):
    backoff = BackoffPolicy(base_seconds=1, max_seconds=60, max_attempts=5, circuit_break_after=3)
    a = FakeProvider("fa", behavior=always_fail(ProviderError("http_500", "down", retryable=True)), backoff=backoff)
    b = FakeProvider("fb", backoff=backoff)
    env = make_env(a, b)
    a_jobs = [job(f"A{i:02d}", provider="fa", priority=10) for i in range(10)]
    b_jobs = [job(f"B{i:02d}", provider="fb", priority=50) for i in range(10)]
    env.orch.submit(a_jobs + b_jobs)
    report = env.orch.run()
    assert report.open_circuits == ("fa",)
    assert len(a.requests) == 3  # the circuit opened after 3 consecutive failures
    assert all(env.state(j).state is JobState.DONE for j in b_jobs)
    a_states = [env.state(j).state for j in a_jobs]
    assert a_states.count(JobState.FAILED) == 3 and a_states.count(JobState.PENDING) == 7
    assert report.done == 10 and report.waiting == 10
    # After cooldown the provider is probed again (and re-opens on a failed probe).
    clock.advance(seconds=61)
    again = env.orch.run(max_jobs=5)
    assert len(a.requests) > 3 and again.open_circuits in ((), ("fa",))


def test_circuit_closes_after_success(make_env, clock):
    backoff = BackoffPolicy(base_seconds=1, max_seconds=30, max_attempts=9, circuit_break_after=2)
    a = FakeProvider("fa", behavior=fail_first(2, ProviderError("http_500", "down", retryable=True)), backoff=backoff)
    env = make_env(a)
    env.orch.submit([job(f"A{i}", provider="fa") for i in range(4)])
    assert env.orch.run().open_circuits == ("fa",)
    clock.advance(seconds=31)
    report = env.orch.run()
    assert report.open_circuits == () and env.store.count_jobs()[JobState.DONE] == 4


# --- leases / claims -----------------------------------------------------------------------


def test_claim_is_exclusive_across_threads(tmp_path):
    store = SqliteStateStore(tmp_path / "state.db")
    store.submit_jobs([job(f"S{i:03d}") for i in range(80)])
    now = datetime(2026, 1, 1, tzinfo=UTC)
    claimed: list[str] = []
    lock = threading.Lock()

    def worker() -> None:
        while (j := store.claim_job(now=now)) is not None:
            with lock:
                claimed.append(j.job_id)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert len(claimed) == 80 and len(set(claimed)) == 80


def test_reap_expired_leases(tmp_path):
    store = SqliteStateStore(tmp_path / "state.db")
    j = job()
    store.submit_job(j)
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    claimed = store.claim_job(now=t0)
    assert claimed.state is JobState.LEASED and store.claim_job(now=t0) is None
    assert store.reap_expired_leases(now=t0 + timedelta(seconds=30), ttl=timedelta(seconds=60)) == 0
    store.start_job(j.job_id, now=t0 + timedelta(seconds=40))  # heartbeat keeps it alive
    assert store.reap_expired_leases(now=t0 + timedelta(seconds=90), ttl=timedelta(seconds=60)) == 0
    assert store.reap_expired_leases(now=t0 + timedelta(seconds=101), ttl=timedelta(seconds=60)) == 1
    back = store.load_job(j.job_id)
    assert back.state is JobState.PENDING and back.attempts == 1
    # a lost lease can no longer finish the job
    assert not store.finish_job(j.job_id, JobState.DONE, now=t0, attempts=1)


def test_reap_marks_dead_when_attempts_exhausted(tmp_path):
    store = SqliteStateStore(tmp_path / "state.db")
    j = job()
    store.submit_job(j)
    t0 = datetime(2026, 1, 1, tzinfo=UTC)
    store.claim_job(now=t0)
    store.reap_expired_leases(now=t0 + timedelta(hours=1), ttl=timedelta(seconds=60), max_attempts=1)
    assert store.load_job(j.job_id).state is JobState.DEAD


def test_run_waits_out_stale_leases_from_a_lost_worker(make_env, clock):
    env = make_env(FakeProvider(), ttl=10)
    j = job()
    env.orch.submit([j])
    assert env.store.claim_job(now=clock.now).state is JobState.LEASED  # a worker that then vanished
    report = env.orch.run()
    assert report.done == 1 and env.state(j).attempts == 1
    assert clock.sleeps  # waited on the injected sleep, not real time


def test_scheduler_lock_blocks_a_second_run(make_env, tmp_path):
    fake = FakeProvider()
    env = make_env(fake)
    env.orch.submit([job()])
    with SchedulerLock(tmp_path / "scheduler.lock"):
        with pytest.raises(SchedulerLocked):
            env.orch.run()
    assert fake.requests == [] and env.store.count_jobs()[JobState.PENDING] == 1
    assert env.orch.run().done == 1  # free again once released


def test_scheduler_lock_is_reentrant_for_one_holder(make_env):
    env = make_env(FakeProvider())
    env.orch.submit([job()])
    with env.orch:  # e.g. a CLI command holding the lock across plan + run
        assert env.orch.run().done == 1


def test_remote_node_is_a_stub():
    node = RemoteNode("cn", "https://node.example", markets={Market.CN})
    assert node.markets() == {Market.CN}
    with pytest.raises(NotImplementedError):
        node.execute(job())


# --- raw idempotence / canonical -------------------------------------------------------------


def test_raw_writes_are_idempotent_on_parquet(tmp_path, clock):
    from openbars.storage.parquet_duckdb import open_backend

    backend = open_backend(tmp_path / "data")
    fake = FakeProvider()
    node = LocalNode({"fake": fake}, store=backend.state, raw=backend.raw, clock=clock)
    orch = LocalOrchestrator(backend.state, node, clock=clock, sleep=clock.sleep)
    j = job(end=date(2026, 3, 5))
    orch.submit([j])
    orch.run()
    window = DateRange(DAY, date(2026, 3, 5))
    assert len(list(backend.raw.scan("fake", Market.US, Interval.D1, window))) == 3
    # Force the same job to run again (e.g. a replay after a crash): no duplicate rows.
    from dataclasses import replace

    backend.state.save_job(replace(backend.state.load_job(j.job_id), state=JobState.PENDING))
    orch.run()
    assert len(fake.requests) == 2
    assert len(list(backend.raw.scan("fake", Market.US, Interval.D1, window))) == 3


def test_reconciler_hook_writes_canonical_rows(tmp_path, clock):
    from openbars.orchestrator.finalize import ReconcilingSink, parquet_canonical_writer
    from openbars.reconcile.reconciler import MemoryReconciler
    from openbars.storage.parquet_duckdb import open_backend

    backend = open_backend(tmp_path / "data")
    fake = FakeProvider("massive", batch=BatchShape.WHOLE_MARKET_PER_DAY, feed="eod")
    sink = ReconcilingSink(
        MemoryReconciler(), parquet_canonical_writer(backend.canonical),
        raw=backend.raw, other_providers=("alpaca",),
    )  # fmt: skip
    node = LocalNode({"massive": fake}, store=backend.state, raw=backend.raw, sink=sink, clock=clock)
    orch = LocalOrchestrator(backend.state, node, clock=clock, sleep=clock.sleep)
    j = Job(make_job_id(JobKind.DAILY, "massive", Market.US, Interval.D1, WHOLE_MARKET, DateRange(DAY, DAY)),
            JobKind.DAILY, "massive", Market.US, Interval.D1, WHOLE_MARKET, DateRange(DAY, DAY))  # fmt: skip
    orch.submit([j])
    assert orch.run().done == 1
    rows = backend.canonical.scan_rows(["TEST1", "ZZZA"], Interval.D1, DateRange(DAY, DAY))
    assert sorted(r.instrument_id for r in rows) == ["TEST1", "ZZZA"]  # symbol used while unresolved
    assert {r.provider for r in rows} == {"massive"}


def test_sink_failure_is_a_retryable_job_failure_raw_is_kept(make_env, clock):
    class BadSink:
        def after_fetch(self, job, bars):
            raise ValueError("reconcile exploded")

    env = make_env(FakeProvider(), sink=BadSink())
    j = job()
    env.orch.submit([j])
    env.orch.run()
    s = env.state(j)
    assert s.state is JobState.FAILED and "post-fetch" in s.last_error
    assert len(env.raw.rows) == 1


# --- kill -9 resume ----------------------------------------------------------------------------

N_JOBS = 1000
WORKER = Path(__file__).with_name("kill_worker.py")


def _spawn(state_db: Path, side_db: Path, ttl: float, delay: float) -> subprocess.Popen:
    env = dict(os.environ, PYTHONPATH=os.pathsep.join([str(WORKER.parent), os.environ.get("PYTHONPATH", "")]))
    return subprocess.Popen(
        [sys.executable, str(WORKER), str(state_db), str(side_db), str(ttl), str(delay)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env,
    )  # fmt: skip


def test_kill_9_mid_run_resumes_without_duplicates_or_losses(tmp_path):
    state_db, side_db = tmp_path / "state.db", tmp_path / "side.db"
    store = SqliteStateStore(state_db)
    jobs = [job(f"S{i:04d}") for i in range(N_JOBS)]
    assert store.submit_jobs(jobs) == N_JOBS
    key = {j.job_id: f"{j.instruments[0]}|{j.window.start.isoformat()}" for j in jobs}

    child = _spawn(state_db, side_db, ttl=0.5, delay=0.001)
    deadline = time.monotonic() + 60
    try:
        while store.count_jobs()[JobState.DONE] < 100:
            assert child.poll() is None, child.stderr.read()
            assert time.monotonic() < deadline, "worker made no progress"
            time.sleep(0.005)
    finally:
        child.kill()  # SIGKILL: no cleanup, no lock release by the process itself
        child.wait()
    after_kill = store.list_jobs()
    done_before = {j.job_id for j in after_kill if j.state is JobState.DONE}
    in_flight = {j.job_id for j in after_kill if j.state in (JobState.LEASED, JobState.RUNNING)}
    assert 100 <= len(done_before) < N_JOBS, "worker finished before it could be killed"

    second = _spawn(state_db, side_db, ttl=0.5, delay=0.0)
    out, err = second.communicate(timeout=120)
    assert second.returncode == 0, err

    final = store.list_jobs()
    assert len(final) == N_JOBS
    assert {j.state for j in final} == {JobState.DONE}  # nothing pending/leased/running/dead
    side = SqliteSideStore(side_db)
    counts = side.exec_counts()
    assert set(counts) == set(key.values())  # no job missing
    for job_id, k in key.items():
        if job_id in done_before:
            assert counts[k] == 1, "a job finished before the crash ran again"
        elif job_id in in_flight:
            assert counts[k] <= 2  # claimed-but-unfinished work may be retried once
        else:
            assert counts[k] == 1
    assert sum(1 for c in counts.values() if c == 2) <= 1  # one worker thread => at most one in flight
    assert side.raw_count() == N_JOBS  # replays never duplicate raw rows


def test_hourly_runner_passes_configured_options_after_lease(tmp_path, clock):
    fake = FakeProvider(interval=Interval.H1)
    store = SqliteStateStore(tmp_path / "state.db")
    node = LocalNode({fake.id: fake}, store=store, raw=MemoryRawStore(), clock=clock,
                     hourly_alignment="clock_hour", include_extended_hours=True)
    runner = LocalOrchestrator(store, node, clock=clock, sleep=clock.sleep)
    window = DateRange(DAY, DAY)
    j = Job(make_job_id(JobKind.BACKFILL, fake.id, Market.US, Interval.H1, ("TEST1",), window),
            JobKind.BACKFILL, fake.id, Market.US, Interval.H1, ("TEST1",), window)
    assert runner.submit([j]) == 1
    assert runner.run().done == 1
    assert fake.requests[0].hourly_alignment == "clock_hour"
    assert fake.requests[0].include_extended_hours is True
