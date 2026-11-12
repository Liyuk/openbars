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


def test_attempt_index_matches_brute_force():
    from types import SimpleNamespace
    from openbars.calendars import xnys
    from openbars.orchestrator.jobs import Job, JobKind, JobState
    from openbars.orchestrator.planner import NEW_YORK

    window = DateRange(date(2026, 3, 2), date(2026, 3, 6))
    states = [JobState.PENDING, JobState.LEASED, JobState.RUNNING, JobState.DONE, JobState.DEAD]
    jobs = [Job(str(i), JobKind.BACKFILL, 'alpaca' if i % 2 else 'other', Market.US, Interval.H1,
                ('TEST1', 'TEST2') if i % 3 else ('TEST2',), window, state=state)
            for i, state in enumerate(states)]
    fetches = [SimpleNamespace(job_id='3', status=status, started_at=started, window=span)
               for status, started, span in [
                   ('ok', datetime(2026, 3, 4, 3, tzinfo=UTC), window),
                   ('empty', datetime(2026, 3, 7, tzinfo=UTC), DateRange(date(2026, 3, 5), date(2026, 3, 5))),
                   ('error', datetime(2026, 3, 8, tzinfo=UTC), window),
                   ('ok', None, window),
               ]]
    state = SimpleNamespace(list_jobs=lambda: jobs, list_fetches=lambda **kwargs: fetches)
    coverage = RawStoreCoverage(MemoryRawStore(), state=state)
    for provider in ('alpaca', 'other', 'missing'):
        for symbol in ('TEST1', 'TEST2', 'missing'):
            for query in (window, DateRange(date(2026, 3, 3), date(2026, 3, 5))):
                expected = set()
                for job in jobs:
                    if job.provider != provider or symbol not in job.instruments:
                        continue
                    for day in xnys.sessions(max(query.start, job.window.start), min(query.end, job.window.end)):
                        if job.state in {JobState.PENDING, JobState.LEASED, JobState.RUNNING} or (
                            job.state is JobState.DONE and any(
                                f.job_id == job.job_id and f.status in {'ok', 'empty'} and f.started_at is not None
                                and f.window.start <= day <= f.window.end
                                and f.started_at.astimezone(NEW_YORK).date() > day for f in fetches
                            )
                        ):
                            expected.add(day)
                assert coverage.covered(provider, Market.US, Interval.H1, query, symbol=symbol) == expected


def test_attempt_index_avoids_per_symbol_job_scans():
    from types import SimpleNamespace
    from openbars.orchestrator.jobs import Job, JobKind, JobState

    class Instruments(tuple):
        checks = 0

        def __contains__(self, item):
            type(self).checks += 1
            return super().__contains__(item)

    window = DateRange(date(2026, 3, 2), date(2026, 3, 2))
    jobs = [Job(str(i), JobKind.BACKFILL, 'alpaca', Market.US, Interval.H1,
                Instruments((f'TEST{i}',)), window, state=JobState.PENDING) for i in range(2000)]
    state = SimpleNamespace(list_jobs=lambda: jobs, list_fetches=lambda **kwargs: [])
    coverage = RawStoreCoverage(MemoryRawStore(), state=state)
    for i in range(2000):
        assert coverage.covered('alpaca', Market.US, Interval.H1, window, symbol=f'TEST{i}') == {window.start}
    assert Instruments.checks < 4000


# --- #144 review: coverage on the expected hourly grid ------------------------------------

from dataclasses import replace as _replace

from openbars.calendars import xnys as _xnys
from openbars.core.models import HourlyAlignment
from openbars.orchestrator.jobs import Job, JobKind, JobState
from openbars.providers.base import FetchRequest, RawBatch

MON, TUE = date(2026, 3, 2), date(2026, 3, 3)
EARLY = date(2025, 11, 28)  # day after Thanksgiving: 13:00 close


def _store(day, slots, symbol="TEST1", raw=None):
    raw = raw or MemoryRawStore()
    provider = FakeProvider("alpaca", interval=Interval.H1)
    bars = tuple(_replace(provider.bar(symbol, day, "f"), interval=Interval.H1, ts_start=s.ts_start) for s in slots)
    raw.append(RawBatch("alpaca", "f", FetchRequest(Market.US, Interval.H1, (symbol,), DateRange(day, day)), bars))
    return raw


def _covered(raw, day, state=None, symbol="TEST1"):
    return RawStoreCoverage(raw, state=state).covered("alpaca", Market.US, Interval.H1, DateRange(day, day), symbol=symbol)


def test_partial_hours_are_not_coverage_and_a_full_grid_is():
    slots = _xnys.hourly_slots(MON)
    assert len(slots) == 7
    assert _covered(_store(MON, slots[:6]), MON) == set()
    assert _covered(_store(MON, slots[1:]), MON) == set()
    assert _covered(_store(MON, slots), MON) == {MON}


def test_clock_hour_grid_and_early_close_count_as_complete():
    clock = _xnys.hourly_slots(MON, alignment=HourlyAlignment.CLOCK_HOUR)
    assert _covered(_store(MON, clock), MON) == {MON}
    early = _xnys.hourly_slots(EARLY)
    assert len(early) == 4
    assert _covered(_store(EARLY, early), EARLY) == {EARLY}
    assert _covered(_store(EARLY, early[:3]), EARLY) == set()


def test_mixed_alignments_do_not_add_up_to_a_full_session():
    so = _xnys.hourly_slots(MON)
    ch = _xnys.hourly_slots(MON, alignment=HourlyAlignment.CLOCK_HOUR)
    assert _covered(_store(MON, so[:4] + ch[4:]), MON) == set()


def test_instrument_id_and_symbol_both_resolve_grid_coverage():
    raw = MemoryRawStore()
    provider = FakeProvider("alpaca", interval=Interval.H1)
    bars = tuple(_replace(provider.bar("TEST1", MON, "f"), interval=Interval.H1, ts_start=s.ts_start,
                          instrument_id="us:eq:TEST1") for s in _xnys.hourly_slots(MON))
    raw.append(RawBatch("alpaca", "f", FetchRequest(Market.US, Interval.H1, ("TEST1",), DateRange(MON, MON)), bars))
    assert _covered(raw, MON, symbol="us:eq:TEST1") == {MON} == _covered(raw, MON)


def _done_job(state, job_id, day, status, started_at, job_state=JobState.DONE):
    job = Job(job_id, JobKind.BACKFILL, "alpaca", Market.US, Interval.H1, ("TEST1",), DateRange(day, day), state=job_state)
    state.save_job(job)
    state.start_fetch(provider="alpaca", fetch_id=job_id, job_id=job_id, market=Market.US, interval=Interval.H1,
                      window=job.window, started_at=started_at)
    state.finish_fetch("alpaca", job_id, status, row_count=0)


def test_partial_session_is_replanned_unless_a_successful_fetch_answered_it(tmp_path):
    state = SqliteStateStore(tmp_path / "state.db")
    raw = _store(MON, _xnys.hourly_slots(MON)[:3])
    provider = FakeProvider("alpaca", interval=Interval.H1, max_symbols=200)
    selection = Selection(frozenset({Market.US}), frozenset({Interval.H1}), MON, TUE)
    kwargs = dict(now=datetime(2026, 3, 10, tzinfo=UTC), universe=["TEST1"], providers={"alpaca": provider})
    # partial data (e.g. from an interrupted fetch whose job died): Monday is fetched again
    _done_job(state, "dead", MON, "error", datetime(2026, 3, 4, tzinfo=UTC), JobState.DEAD)
    plan = DefaultPlanner().plan(selection, RawStoreCoverage(raw, state=state), **kwargs)
    assert [j.window for j in plan.jobs] == [DateRange(MON, TUE)]
    # a successful fetch after the close answered Monday: missing hours are no-trade hours
    _done_job(state, "ok", MON, "ok", datetime(2026, 3, 4, tzinfo=UTC))
    plan = DefaultPlanner().plan(selection, RawStoreCoverage(raw, state=state), **kwargs)
    assert [j.window for j in plan.jobs] == [DateRange(TUE, TUE)]


def test_a_fetch_started_before_the_session_closed_does_not_cover_it(tmp_path):
    state = SqliteStateStore(tmp_path / "state.db")
    raw = _store(MON, _xnys.hourly_slots(MON)[:3])
    _done_job(state, "early", MON, "ok", datetime(2026, 3, 2, 17, tzinfo=UTC))  # 12:00 New York, intraday
    assert _covered(raw, MON, state=state) == set()
