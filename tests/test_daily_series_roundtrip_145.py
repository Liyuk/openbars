from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from openbars.core.models import DateRange, Interval, Market, RawBar
from openbars.core.series import BarSeriesCatalog, SourceIdentity
from openbars.orchestrator.finalize import observation_from_raw
from openbars.providers.base import FetchRequest, RawBatch
from openbars.reconcile.reconciler import MemoryReconciler
from openbars.storage.parquet_duckdb import open_backend
from openbars.storage.rows import raw_rows_from_batch

DAY = date(2025, 1, 2)
WINDOW = DateRange(DAY, DAY)


def make_bar(provider, feed, adjustment, *, series=True, extra=None, currency="USD"):
    identity = BarSeriesCatalog.resolve(SourceIdentity(provider, feed, adjustment), currency=currency) if series else None
    return RawBar(provider=provider, feed=feed, symbol_as_returned='TEST1', fetch_id='test',
                  interval=Interval.D1, ts_start=datetime(2025, 1, 2, tzinfo=UTC),
                  session_date=DAY, open=Decimal('10'), high=Decimal('11'), low=Decimal('9'),
                  close=Decimal('10'), volume=Decimal('100'), series=identity, extra=extra or {})


def reload_bar(backend, bar, market):
    request = FetchRequest(market, Interval.D1, ('TEST1',), WINDOW)
    backend.raw.append(RawBatch(bar.provider, 'test', request, (bar,)))
    return list(backend.raw.scan(bar.provider, market, Interval.D1, WINDOW))[0]


@pytest.mark.parametrize('provider,feed,adjustment,market', [
    ('massive', 'eod', 'split', Market.US),
    ('alpaca', 'sip', 'split', Market.US),
    ('alpaca', 'iex', 'raw', Market.US),
    ('yfinance', 'yahoo', 'split', Market.US),
    ('yfinance', 'yahoo', 'raw', Market.HK),
    ('binance', 'spot', 'raw', Market.CRYPTO),
    ('coinbase', 'exchange', 'raw', Market.CRYPTO),
    ('baostock', 'ashare', 'raw', Market.CN),
    ('akshare', 'eastmoney', 'raw', Market.HK),
])
def test_provider_shapes_preserve_observation_identity(tmp_path, provider, feed, adjustment, market):
    currency = {Market.CRYPTO: None, Market.CN: 'CNY', Market.HK: 'HKD'}.get(market, 'USD')
    bar = make_bar(provider, feed, adjustment, currency=currency)
    restored = reload_bar(open_backend(tmp_path), bar, market)
    before, after = observation_from_raw(bar), observation_from_raw(restored)
    assert restored.series == bar.series
    assert (after.feed, after.adjustment, after.series) == (before.feed, before.adjustment, before.series)


@pytest.mark.parametrize('extra', [{}, {'adjustment': 'raw'}, {'adjustment': 'split'}])
def test_seriesless_adjustment_identity_roundtrips(tmp_path, extra):
    bar = make_bar('massive', 'eod', 'split', series=False, extra=extra)
    restored = reload_bar(open_backend(tmp_path), bar, Market.US)
    before, after = observation_from_raw(bar), observation_from_raw(restored)
    assert restored.series is None
    assert (after.feed, after.adjustment, after.series) == (before.feed, before.adjustment, before.series)


@pytest.mark.parametrize('first', ['massive', 'alpaca'])
@pytest.mark.parametrize('reverse', [False, True])
def test_reconciliation_same_when_either_source_reloaded(tmp_path, first, reverse):
    backend = open_backend(tmp_path)
    bars = [make_bar('massive', 'eod', 'split'), make_bar('alpaca', 'sip', 'split')]
    mixed = [reload_bar(backend, b, Market.US) if b.provider == first else b for b in bars]
    if reverse:
        bars.reverse()
        mixed.reverse()

    def reconcile(items):
        observations = [observation_from_raw(b) for b in items]
        return MemoryReconciler().reconcile_range('TEST1', DAY, DAY, observations,
                                                   generated_at=datetime(2025, 1, 3, tzinfo=UTC))
    expected = reconcile(bars)
    assert expected.canonical
    assert reconcile(mixed) == expected


def test_legacy_row_without_series_loads():
    bar = make_bar('massive', 'eod', 'split', series=False, extra={'adjustment': 'raw'})
    request = FetchRequest(Market.US, Interval.D1, ('TEST1',), WINDOW)
    row = raw_rows_from_batch(RawBatch(bar.provider, 'test', request, (bar,)))[0]
    assert not row.extra or '_bar_series' not in row.extra
    restored = row.to_raw_bar()
    assert restored.series is None
    assert observation_from_raw(restored).adjustment == 'raw'


@pytest.mark.parametrize('fresh_provider', ['yfinance', 'akshare'])
def test_hk_both_arrival_orders_preserve_reconciliation(tmp_path, fresh_provider):
    from dataclasses import replace
    from openbars.reconcile.hk import reconcile_hk

    backend = open_backend(tmp_path)
    bars = [make_bar('yfinance', 'yahoo', 'raw', currency='HKD'),
            make_bar('akshare', 'eastmoney', 'raw', currency='HKD')]
    mixed = [b if b.provider == fresh_provider else reload_bar(backend, b, Market.HK) for b in bars]
    iid = 'hk:equity:TEST1'
    def reconcile(items):
        obs = [replace(observation_from_raw(b), instrument_id=iid) for b in items]
        return reconcile_hk(iid, DAY, DAY, obs, generated_at=datetime(2025, 1, 3, tzinfo=UTC)).result
    expected = reconcile(bars)
    assert expected.canonical
    assert reconcile(mixed) == expected


@pytest.mark.parametrize('fresh_feed', ['spot', 'archive'])
def test_binance_same_venue_sources_both_arrival_orders(tmp_path, fresh_feed):
    from dataclasses import replace
    from openbars.reconcile.crypto import reconcile_crypto

    backend = open_backend(tmp_path)
    bars = [make_bar('binance', feed, 'raw', currency=None) for feed in ('spot', 'archive')]
    mixed = [b if b.feed == fresh_feed else reload_bar(backend, b, Market.CRYPTO) for b in bars]
    iid = 'crypto:spot:binance:TEST-USDT'
    def reconcile(items):
        obs = [replace(observation_from_raw(b), instrument_id=iid) for b in items]
        return reconcile_crypto(iid, DAY, DAY, obs, generated_at=datetime(2025, 1, 3, tzinfo=UTC)).result
    expected = reconcile(bars)
    assert expected.canonical
    assert reconcile(mixed) == expected


def test_fresh_sink_output_matches_original_projection(tmp_path, monkeypatch):
    """The original daily projection and the current one yield identical fresh rows."""
    from dataclasses import replace
    import openbars.orchestrator.finalize as finalize
    from openbars.orchestrator.jobs import Job, JobKind

    current = finalize.observation_from_raw
    def original(bar):
        obs = current(bar)
        if obs is None:
            return None
        adjustment = (bar.series.legacy_adjustment if bar.series is not None
                      else str(bar.extra.get('adjustment') or 'split'))
        return replace(obs, adjustment=adjustment)

    backend = open_backend(tmp_path)
    cases = [
        (Market.US, make_bar('massive', 'eod', 'split')),
        (Market.US, make_bar('massive', 'eod', 'split', series=False)),
        (Market.CRYPTO, replace(make_bar('binance', 'spot', 'raw', currency=None),
                                instrument_id='crypto:spot:binance:TEST-USDT')),
        (Market.CRYPTO, replace(make_bar('coinbase', 'exchange', 'raw', currency=None),
                                instrument_id='crypto:spot:coinbase:TEST-USDT')),
        (Market.CN, replace(make_bar('baostock', 'ashare', 'raw', currency='CNY'),
                            instrument_id='cn:equity:TEST1')),
    ]
    for market, bar in cases:
        job = Job('test', JobKind.DAILY, bar.provider, market, Interval.D1, ('TEST1',), WINDOW)
        def run(projection):
            monkeypatch.setattr(finalize, 'observation_from_raw', projection)
            output = []
            if market is Market.US:
                sink = finalize.ReconcilingSink(MemoryReconciler(), lambda rows, m: output.extend(rows))
            elif market is Market.CRYPTO:
                sink = finalize.CryptoReconcilingSink(backend.instruments, lambda rows, m: output.extend(rows))
            else:
                sink = finalize.CnCanonicalSink(backend.instruments, lambda rows: output.extend(rows))
            sink.after_fetch(job, [bar])
            return [replace(row, generated_at=datetime(2025, 1, 3, tzinfo=UTC)) for row in output]
        before, after = run(original), run(current)
        assert before
        assert after == before
