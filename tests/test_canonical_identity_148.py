from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from openbars.core.models import DateRange, Interval, Market, RawBar
from openbars.orchestrator.finalize import ReconcilingSink, parquet_canonical_writer
from openbars.orchestrator.jobs import Job, JobKind
from openbars.providers.base import FetchRequest, RawBatch
from openbars.reconcile.reconciler import MemoryReconciler
from openbars.storage.parquet_duckdb import open_backend


@pytest.mark.parametrize('order', [('alpaca', 'massive'), ('massive', 'alpaca')])
def test_hourly_identity_upgrade_persists_one_row_and_preserves_daily(tmp_path, order):
    day = date(2025, 1, 2)
    window = DateRange(day, day)
    backend = open_backend(tmp_path)
    sink = ReconcilingSink(MemoryReconciler(), parquet_canonical_writer(backend.canonical),
                           backend.raw, ('massive', 'alpaca'), canonical=backend.canonical)
    for provider in order:
        bars = tuple(RawBar(
            provider=provider, feed='aggs' if provider == 'massive' else 'sip',
            symbol_as_returned='TEST1', fetch_id='synthetic',
            instrument_id='us:TEST1' if provider == 'massive' else None,
            interval=Interval.H1, ts_start=datetime(2025, 1, 2, hour, tzinfo=UTC),
            session_date=day, open=Decimal('10'), high=Decimal('20'), low=Decimal('5'),
            close=Decimal('10' if provider == 'massive' else '15'), volume=Decimal('100'),
        ) for hour in (15, 16))
        request = FetchRequest(Market.US, Interval.H1, ('TEST1',), window)
        backend.raw.append(RawBatch(provider, 'synthetic', request, bars))
        job = Job('job', JobKind.BACKFILL, provider, Market.US, Interval.H1, ('TEST1',), window)
        sink.after_fetch(job, bars)
        if provider == order[0]:
            first = backend.canonical.scan_rows(None, Interval.H1, window)[0]
            daily = replace(first, instrument_id='TEST1', interval=Interval.D1)
            backend.canonical.upsert_rows([daily])
    hourly = backend.canonical.scan_rows(None, Interval.H1, window)
    assert len(hourly) == 2
    assert {row.instrument_id for row in hourly} == ({'TEST1'} if order[0] == 'alpaca' else {'us:TEST1'})
    assert {row.provider for row in hourly} == {'massive'}
    assert {row.ts_start.hour for row in hourly} == {15, 16}
    assert {row.quality_status for row in hourly} == {'divergent'}
    assert backend.canonical.scan_rows(None, Interval.D1, window) == [daily]
