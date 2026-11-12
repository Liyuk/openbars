from datetime import UTC, date, datetime

from fakes import FakeProvider, MemoryRawStore
from openbars.config.schema import Selection
from openbars.core.models import DateRange, Interval, Market
from openbars.orchestrator.planner import DefaultPlanner, RawStoreCoverage, StaticCoverage
from openbars.storage.state import SqliteStateStore


def test_hourly_multi_session_window_with_state(tmp_path):
    state = SqliteStateStore(tmp_path / 'state.db')
    coverage = RawStoreCoverage(MemoryRawStore(), state=state)
    provider = FakeProvider('alpaca', interval=Interval.H1, max_symbols=200)
    selection = Selection(frozenset({Market.US}), frozenset({Interval.H1}), date(2026, 2, 23), date(2026, 3, 6))
    plan = DefaultPlanner().plan(selection, coverage, now=datetime(2026, 3, 10, tzinfo=UTC), universe=['TEST1', 'TEST2', 'TEST3'], providers={'alpaca': provider})
    assert len(plan.jobs) == 1
    assert plan.jobs[0].window == DateRange(date(2026, 2, 23), date(2026, 3, 6))
    assert plan.budget.total_requests == 1


def test_successful_empty_and_existing_queue_reserve_sessions(tmp_path):
    from dataclasses import replace
    from openbars.orchestrator.jobs import Job, JobKind, JobState

    state = SqliteStateStore(tmp_path / 'state.db')
    first, second = date(2026, 3, 2), date(2026, 3, 3)
    job = Job('one', JobKind.BACKFILL, 'alpaca', Market.US, Interval.H1, ('TEST1',), DateRange(first, first), state=JobState.DONE)
    state.save_job(job)
    state.start_fetch(provider='alpaca', fetch_id='f', job_id='one', market=Market.US, interval=Interval.H1, window=job.window, started_at=datetime(2026, 3, 4, tzinfo=UTC))
    state.finish_fetch('alpaca', 'f', 'empty', row_count=0)
    state.save_job(replace(job, job_id='two', window=DateRange(second, second), state=JobState.PENDING))
    coverage = RawStoreCoverage(MemoryRawStore(), state=state)
    assert coverage.covered('alpaca', Market.US, Interval.H1, DateRange(first, second), symbol='TEST1') == {first, second}
    state.save_job(replace(job, job_id='two', window=DateRange(second, second), state=JobState.DEAD))
    coverage = RawStoreCoverage(MemoryRawStore(), state=state)
    assert coverage.covered('alpaca', Market.US, Interval.H1, DateRange(first, second), symbol='TEST1') == {first}


def test_covered_middle_session_splits_windows(tmp_path):
    raw = MemoryRawStore()
    state = SqliteStateStore(tmp_path / 'state.db')
    coverage = RawStoreCoverage(raw, state=state)
    middle = date(2026, 3, 4)
    base = coverage.covered
    coverage.covered = lambda provider, market, interval, window, *, symbol=None: base(provider, market, interval, window, symbol=symbol) | {middle}
    provider = FakeProvider('alpaca', interval=Interval.H1, max_symbols=200)
    selection = Selection(frozenset({Market.US}), frozenset({Interval.H1}), date(2026, 3, 2), date(2026, 3, 6))
    plan = DefaultPlanner().plan(selection, coverage, now=datetime(2026, 3, 10, tzinfo=UTC), universe=['TEST1'], providers={'alpaca': provider})
    assert {job.window for job in plan.jobs} == {DateRange(date(2026, 3, 2), date(2026, 3, 3)), DateRange(date(2026, 3, 5), date(2026, 3, 6))}


def test_daily_planning_ignores_hourly_attempt_history(tmp_path):
    from openbars.orchestrator.planner import EmptyCoverage

    provider = FakeProvider('alpaca', interval=Interval.D1, max_symbols=200)
    selection = Selection(frozenset({Market.US}), frozenset({Interval.D1}), date(2026, 3, 2), date(2026, 3, 6))
    kwargs = dict(now=datetime(2026, 3, 10, tzinfo=UTC), universe=['TEST1'], providers={'alpaca': provider})
    expected = DefaultPlanner().plan(selection, EmptyCoverage(), **kwargs)
    actual = DefaultPlanner().plan(selection, RawStoreCoverage(MemoryRawStore(), state=SqliteStateStore(tmp_path / 'state.db')), **kwargs)
    assert actual.jobs == expected.jobs
    assert actual.budget == expected.budget
