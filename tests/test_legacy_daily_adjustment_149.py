"""#149: pre-#145 daily raw rows stored ``adjustment=raw`` for split sources; reads normalize them.

Real-storage regression: a legacy ``massive/eod/raw`` row next to the current ``massive/eod/split``
row must not change canonical selection. Parquet always; Postgres native with OPENBARS_TEST_PG_DSN.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from openbars.core.models import DateRange, Interval, Market, RawBar
from openbars.core.series import BarSeriesCatalog, SourceIdentity
from openbars.orchestrator.finalize import ReconcilingSink
from openbars.orchestrator.jobs import Job, JobKind
from openbars.providers.base import FetchRequest, RawBatch
from openbars.reconcile.reconciler import MemoryReconciler
from openbars.storage.parquet_duckdb import open_backend
from openbars.storage.rows import is_legacy_daily_row, normalize_legacy_daily, raw_rows_from_batch

DAYS = [date(2025, 1, 2), date(2025, 1, 3)]
WINDOW = DateRange(DAYS[0], DAYS[-1])


def bar(provider, feed, day, close="10", *, legacy=False, market_currency="USD", adjustment="split"):
    series = None if legacy else BarSeriesCatalog.resolve(SourceIdentity(provider, feed, adjustment), currency=market_currency)
    return RawBar(provider=provider, feed=feed, symbol_as_returned="TEST1", fetch_id="legacy" if legacy else "new",
                  interval=Interval.D1, ts_start=datetime.combine(day, datetime.min.time(), tzinfo=UTC),
                  session_date=day, open=Decimal("10"), high=Decimal("11"), low=Decimal("9"), close=Decimal(close),
                  volume=Decimal("100"), series=series, extra={"adjustment": "raw"} if legacy else {})  # fmt: skip


def rows(bars, market=Market.US):
    request = FetchRequest(market, Interval.D1, ("TEST1",), WINDOW)
    return raw_rows_from_batch(RawBatch(bars[0].provider, "t", request, tuple(bars)))


def legacy_rows(provider="massive", feed="eod", close="10"):
    out = rows([bar(provider, feed, d, close, legacy=True) for d in DAYS])
    assert all(r.adjustment == "raw" and is_legacy_daily_row(r) for r in out)
    return out


def canonical(raw_store, alpaca_bars):
    output = []
    sink = ReconcilingSink(MemoryReconciler(), lambda rs, m: output.extend(rs), raw=raw_store,
                           other_providers=("massive", "alpaca"))  # fmt: skip
    job = Job("j", JobKind.DAILY, "alpaca", Market.US, Interval.D1, ("TEST1",), WINDOW)
    sink.after_fetch(job, alpaca_bars)
    return [replace(r, generated_at=datetime(2025, 1, 6, tzinfo=UTC), available_at=None) for r in output]


def scenario(raw_store, *, legacy: bool, current: bool, legacy_close="10"):
    if legacy:
        raw_store.append_rows(legacy_rows(close=legacy_close))
    if current:
        raw_store.append_rows(rows([bar("massive", "eod", d) for d in DAYS]))
    return canonical(raw_store, [bar("alpaca", "sip", d) for d in DAYS])


def test_normalize_rewrites_and_drops_superseded_legacy_rows():
    old = legacy_rows()
    new = rows([bar("massive", "eod", d) for d in DAYS[:1]])
    out = normalize_legacy_daily([*old, *new])
    assert [(r.session_date, r.adjustment, r.fetch_id) for r in out] == [(DAYS[1], "split", "legacy"), (DAYS[0], "split", "new")]
    assert normalize_legacy_daily(new) == new


@pytest.mark.parametrize("provider, feed, market, currency, expected", [
    ("massive", "eod", Market.US, "USD", True),
    ("alpaca", "iex", Market.US, "USD", True),
    ("alpaca", "sip", Market.US, "USD", True),
    ("yfinance", "yahoo", Market.US, "USD", True),
    ("yfinance", "yahoo", Market.HK, "HKD", False),  # HK yfinance really is raw
    ("akshare", "eastmoney", Market.HK, "HKD", False),
    ("binance", "spot", Market.CRYPTO, None, False),
    ("baostock", "ashare", Market.CN, "CNY", False),
    ("akshare_ashare", "eastmoney", Market.CN, "CNY", False),
    ("akshare_ashare", "sina", Market.CN, "CNY", False),
])  # fmt: skip
def test_only_known_split_sources_are_normalized(provider, feed, market, currency, expected):
    (row,) = rows([bar(provider, feed, DAYS[0], legacy=True)], market)
    assert row.adjustment == "raw"
    assert is_legacy_daily_row(row) is expected
    (current,) = rows([bar(provider, feed, DAYS[0], market_currency=currency,
                           adjustment="split" if expected else "raw")], market)  # fmt: skip
    assert not is_legacy_daily_row(current)


def test_hourly_rows_are_untouched():
    (row,) = legacy_rows()[:1]
    hourly = replace(row, interval=Interval.H1)
    assert not is_legacy_daily_row(hourly)


def _check_store(make_store):
    expected = scenario(make_store("only-current"), legacy=False, current=True)
    assert expected and {r.provider for r in expected} <= {"massive", "alpaca"}
    assert scenario(make_store("both"), legacy=True, current=True) == expected
    assert scenario(make_store("only-legacy"), legacy=True, current=False) == expected  # identity recovered
    # a revised legacy value is superseded by the current row for the same key
    assert scenario(make_store("revised"), legacy=True, current=True, legacy_close="10.75") == expected
    store = make_store("scan")
    store.append_rows(legacy_rows())
    store.append_rows(rows([bar("massive", "eod", d) for d in DAYS]))
    got = store.scan_rows("massive", Market.US, Interval.D1, WINDOW)
    assert [(r.session_date, r.adjustment) for r in got] == [(d, "split") for d in DAYS]


def _check_conflicting_coexistence(make_store):
    """The legacy raw identity must not count as independent corroboration.

    Massive is the primary and disagrees with Alpaca. One independent source supports
    Massive; a second apparent independent vote must not be manufactured by the legacy
    Massive row retaining its old ``raw`` identity.
    """
    store = make_store("conflicting-coexistence")
    store.append_rows(legacy_rows(close="10"))
    store.append_rows(rows([bar("massive", "eod", d, close="10") for d in DAYS]))
    store.append_rows(rows([bar("yfinance", "yahoo", d, close="10") for d in DAYS]))

    output = []
    sink = ReconcilingSink(
        MemoryReconciler(), lambda rs, m: output.extend(rs), raw=store,
        other_providers=("massive", "yfinance", "alpaca"),
    )
    # Alpaca is the fetched secondary and diverges from Massive. With normalization,
    # only yfinance independently backs Massive, below the corroboration quorum of 2.
    job = Job("j", JobKind.DAILY, "alpaca", Market.US, Interval.D1, ("TEST1",), WINDOW)
    sink.after_fetch(job, [bar("alpaca", "sip", d, close="10.2") for d in DAYS])

    assert len(output) == len(DAYS)
    assert {(r.provider, r.feed, r.adjustment) for r in output} == {("massive", "eod", "split")}
    assert {r.quality_status for r in output} == {"divergent"}
    assert {r.selection_reason for r in output} == {"primary_source"}


def test_parquet_selection_is_unchanged_by_legacy_rows(tmp_path):
    _check_store(lambda name: open_backend(tmp_path / name).raw)
    _check_conflicting_coexistence(lambda name: open_backend(tmp_path / name).raw)


@pytest.mark.postgres_live
def test_postgres_native_selection_is_unchanged_by_legacy_rows(pg_dsn):
    from contextlib import ExitStack

    from pg_helpers import throwaway_schema

    from openbars.storage.postgres import PostgresBackend

    with ExitStack() as stack:
        def make(name):
            schema = stack.enter_context(throwaway_schema(pg_dsn))
            return PostgresBackend(pg_dsn, mode="native", write=True, schema=schema).raw

        _check_store(make)
        _check_conflicting_coexistence(make)
