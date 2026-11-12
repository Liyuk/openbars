from datetime import UTC, date, datetime
from decimal import Decimal as D

import pytest

from openbars.core.models import Interval, Market, RawBar
from openbars.orchestrator.finalize import ReconcilingSink, parquet_canonical_writer
from openbars.orchestrator.jobs import Job
from openbars.reconcile.reconciler import MemoryReconciler


def bar(hour, provider='massive', feed='aggs', close='10', session='regular'):
    return RawBar(provider=provider, feed=feed, symbol_as_returned='TEST1', fetch_id='x',
                  instrument_id='us:TEST1', interval=Interval.H1,
                  ts_start=datetime(2025, 1, 2, hour, tzinfo=UTC), session_date=date(2025, 1, 2),
                  open=D('10'), high=D('20'), low=D('5'), close=D(close), volume=D('100'),
                  extra={'session': session})


def test_hourly_selection_and_daily_comparison():
    from openbars.reconcile.hourly import reconcile_hourly, compare_hourly_to_daily
    rows = reconcile_hourly([bar(15), bar(16), bar(15, 'alpaca', 'sip', '15')])
    assert len(rows) == 2
    assert rows[0].provider == 'massive'
    assert rows[0].quality_status == 'divergent'
    daily = rows[0]
    from dataclasses import replace
    daily = replace(daily, interval=Interval.D1, volume=D('200'))
    report = compare_hourly_to_daily(rows + (replace(rows[0], session='pre'),), daily)
    assert report.differences == ()
    assert report.aggregate.volume == D('200')
    assert compare_hourly_to_daily(rows, replace(daily, close=D('15'))).differences == ('close',)


def test_hourly_sink_round_trip_and_priority(tmp_path):
    from openbars.core.models import DateRange
    from openbars.orchestrator.jobs import JobKind
    from openbars.storage.parquet_duckdb import open_backend
    backend = open_backend(tmp_path)
    sink = ReconcilingSink(MemoryReconciler(), parquet_canonical_writer(backend.canonical))
    day = date(2025, 1, 2)
    job = Job('job', JobKind.BACKFILL, 'massive', Market.US, Interval.H1,
              ('us:TEST1',), DateRange(day, day))
    sink.after_fetch(job, [bar(15), bar(16, session='post')])
    rows = backend.canonical.scan_rows(['us:TEST1'], Interval.H1, DateRange(day, day))
    assert {r.ts_start.hour for r in rows} == {15, 16}
    assert {r.session.value for r in rows} == {'regular', 'post'}
    assert backend.canonical.scan_rows(['us:TEST1'], Interval.D1, DateRange(day, day)) == []


def test_secondary_fill_and_quality_rules():
    from dataclasses import replace
    from openbars.reconcile.hourly import reconcile_hourly
    from openbars.reconcile.overrides import SelectionOverride
    from openbars.reconcile.projection import ACTION_MARK_SUSPECT
    sip = bar(15, 'alpaca', 'sip')
    assert reconcile_hourly([sip])[0].provider == 'alpaca'
    assert reconcile_hourly([bar(15, 'yfinance', 'yahoo')])[0].provider == 'yfinance'
    assert reconcile_hourly([bar(15, 'twelve_data', 'intraday')])[0].provider == 'twelve_data'
    assert reconcile_hourly([bar(15, 'alpaca', 'iex')]) == ()
    missing = replace(bar(15), volume=D('0'))
    assert reconcile_hourly([missing])[0].quality_status == 'volume_missing'
    marked = reconcile_hourly([bar(15)], overrides={('us:TEST1', bar(15).ts_start): SelectionOverride(ACTION_MARK_SUSPECT)})
    assert marked[0].quality_status == 'spike_suspect'


@pytest.mark.parametrize("with_series", [True, False])
def test_raw_store_cross_source_arrival_orders_and_interval_coexistence(tmp_path, with_series):
    from dataclasses import replace
    from openbars.core.models import DateRange
    from openbars.core.series import BarSeriesCatalog, SourceIdentity
    from openbars.orchestrator.jobs import JobKind
    from openbars.providers.base import FetchRequest, RawBatch
    from openbars.storage.parquet_duckdb import open_backend

    day = date(2025, 1, 2)
    window = DateRange(day, day)
    results = []
    for order in [('massive', 'alpaca'), ('alpaca', 'massive')]:
        backend = open_backend(tmp_path / order[0])
        sink = ReconcilingSink(MemoryReconciler(), parquet_canonical_writer(backend.canonical),
                               backend.raw, ('massive', 'alpaca'))
        for provider in order:
            feed = 'aggs' if provider == 'massive' else 'sip'
            b = bar(15, provider, feed, '10' if provider == 'massive' else '15')
            if with_series:
                b = replace(b, series=BarSeriesCatalog.resolve(SourceIdentity(provider, feed, 'split'), interval=Interval.H1))
            request = FetchRequest(Market.US, Interval.H1, ('TEST1',), window)
            backend.raw.append(RawBatch(provider, 'x', request, (b,)))
            scanned = list(backend.raw.scan(provider, Market.US, Interval.H1, window))[0]
            assert scanned.instrument_id == b.instrument_id
            assert scanned.series == b.series
            job = Job('job', JobKind.BACKFILL, provider, Market.US, Interval.H1, ('us:TEST1',), window)
            sink.after_fetch(job, [b, bar(16)])
        hourly = backend.canonical.scan_rows(['us:TEST1'], Interval.H1, window)
        daily = replace(hourly[0], interval=Interval.D1)
        backend.canonical.upsert_rows([daily])
        backend.canonical.upsert_rows([daily, *hourly])
        assert len(backend.canonical.scan_rows(['us:TEST1'], Interval.D1, window)) == 1
        assert len(backend.canonical.scan_rows(['us:TEST1'], Interval.H1, window)) == 2
        row = next(r for r in hourly if r.ts_start.hour == 15)
        results.append((row.provider, row.close, row.quality_status))
    assert results == [('massive', D('10'), 'divergent')] * 2


def test_daily_stored_instrument_id_does_not_change_symbol_reconciliation():
    from dataclasses import replace
    from openbars.core.models import DateRange
    from openbars.orchestrator.finalize import observation_from_raw
    from openbars.providers.base import FetchRequest, RawBatch
    from openbars.storage.rows import raw_rows_from_batch

    day = date(2025, 1, 2)
    daily = replace(bar(15, feed="eod"), interval=Interval.D1, instrument_id=None,
                    extra={'instrument_id': 'us:TEST1', 'adjustment': 'split'})
    request = FetchRequest(Market.US, Interval.D1, ('TEST1',), DateRange(day, day))
    row = raw_rows_from_batch(RawBatch('massive', 'x', request, (daily,)))[0]
    assert row.instrument_id == 'us:TEST1'
    restored = row.to_raw_bar()
    observation = observation_from_raw(restored)
    assert restored.instrument_id is None
    assert observation.key == 'TEST1'
    result = MemoryReconciler().reconcile_range('TEST1', day, day, [observation])
    assert result.canonical[0].instrument_id == 'TEST1'


@pytest.mark.parametrize('order', [('massive', 'alpaca'), ('alpaca', 'massive')])
def test_hourly_mixed_identity_reconciles(order):
    from dataclasses import replace
    from openbars.core.models import DateRange
    from openbars.orchestrator.jobs import JobKind

    day = date(2025, 1, 2)
    window = DateRange(day, day)
    stored = {}
    writes = []

    class Raw:
        def scan(self, provider, market, interval, window):
            return stored.get(provider, [])

    sink = ReconcilingSink(MemoryReconciler(), lambda rows, market: writes.append(rows),
                           Raw(), ('massive', 'alpaca'))
    for provider in order:
        b = bar(15, provider, 'aggs' if provider == 'massive' else 'sip',
                '10' if provider == 'massive' else '15')
        if provider == 'alpaca':
            b = replace(b, instrument_id=None)
        stored[provider] = [b]
        job = Job('job', JobKind.BACKFILL, provider, Market.US, Interval.H1, ('TEST1',), window)
        sink.after_fetch(job, [b])
    if order[0] == 'alpaca':
        # An unresolved first write uses the symbol until another source supplies an ID.
        assert writes[0][0].instrument_id == 'TEST1'
    assert len(writes[-1]) == 1
    assert writes[-1][0].instrument_id == 'us:TEST1'
    assert writes[-1][0].quality_status == 'divergent'
    assert set(writes[-1][0].sources_seen) == {'massive', 'alpaca'}


def test_symbol_daily_and_hourly_ignore_available_master(tmp_path):
    from dataclasses import replace
    from openbars.cli.wiring import make_sink
    from openbars.core.models import DateRange
    from openbars.orchestrator.jobs import JobKind
    from openbars.reconcile.hourly import compare_hourly_to_daily
    from openbars.storage.parquet_duckdb import open_backend

    backend = open_backend(tmp_path)
    class Master:
        def resolve(self, *args, **kwargs):
            return 'us:TEST1'
    backend.instruments = Master()
    sink = make_sink(backend, {})
    day = date(2025, 1, 2)
    window = DateRange(day, day)
    job = Job('job', JobKind.BACKFILL, 'massive', Market.US, Interval.H1, ('TEST1',), window)
    sink.after_fetch(job, [replace(bar(15), instrument_id=None),
                           replace(bar(16, 'alpaca', 'sip'), instrument_id=None)])
    rows = backend.canonical.scan_rows(['TEST1'], Interval.H1, window)
    assert len(rows) == 2
    daily = replace(rows[0], interval=Interval.D1, volume=D('200'))
    backend.canonical.upsert_rows([daily])
    stored_daily = backend.canonical.scan_rows(['TEST1'], Interval.D1, window)[0]
    assert compare_hourly_to_daily(rows, stored_daily).differences == ()
    assert compare_hourly_to_daily(rows, replace(stored_daily, close=D('15'))).differences == ('close',)


def test_conflicting_ids_are_not_merged():
    from dataclasses import replace
    from openbars.core.models import DateRange
    from openbars.orchestrator.jobs import JobKind

    day = date(2025, 1, 2)
    writes = []
    sink = ReconcilingSink(MemoryReconciler(), lambda rows, market: writes.extend(rows))
    job = Job('job', JobKind.BACKFILL, 'massive', Market.US, Interval.H1,
              ('TEST1',), DateRange(day, day))
    sink.after_fetch(job, [bar(15), replace(bar(15, 'alpaca', 'sip', '15'), instrument_id='us:OTHER'),
                           replace(bar(15, 'yfinance', 'yahoo'), instrument_id=None)])
    assert {row.instrument_id for row in writes} == {'us:TEST1', 'us:OTHER', 'TEST1'}
    assert all(len(row.sources_seen) == 1 for row in writes)


def test_daily_aggregation_uses_regular_hours_and_preserves_daily():
    from dataclasses import replace
    from openbars.reconcile.hourly import compare_hourly_to_daily, reconcile_hourly

    first = replace(bar(15), open=D('8'), high=D('12'), low=D('7'), close=D('11'), volume=D('30'))
    last = replace(bar(16), open=D('11'), high=D('15'), low=D('9'), close=D('14'), volume=D('70'))
    extended = replace(bar(14, session='pre'), high=D('99'), volume=D('900'))
    rows = reconcile_hourly([last, extended, first])
    daily = replace(rows[0], interval=Interval.D1, open=D('8'), high=D('15'), low=D('7'),
                    close=D('14'), volume=D('100'))
    report = compare_hourly_to_daily(rows, daily)
    assert report.differences == ()
    assert (report.aggregate.open, report.aggregate.high, report.aggregate.low,
            report.aggregate.close, report.aggregate.volume) == (D('8'), D('15'), D('7'), D('14'), D('100'))
    assert compare_hourly_to_daily(rows, replace(daily, volume=D('200'))).differences == ('volume',)
    assert daily.volume == D('100')
