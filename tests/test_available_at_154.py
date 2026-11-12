"""Canonical ``available_at`` and the point-in-time ``available_by`` read (#154).

Offline tests need no database. The ``postgres_live`` test needs OPENBARS_TEST_PG_DSN and uses a
throwaway schema; it is skipped by the default check.sh run.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from openbars.api.queries import QueryError, load_canonical
from openbars.core.models import DateRange, Interval, Market, RawBar, Session
from openbars.orchestrator.finalize import (
    CnCanonicalSink,
    ReconcilingSink,
    observation_from_raw,
    parquet_canonical_writer,
)
from openbars.orchestrator.jobs import Job, JobKind
from openbars.providers.base import FetchRequest, RawBatch
from openbars.reconcile.hourly import reconcile_hourly
from openbars.reconcile.reconciler import MemoryReconciler
from openbars.storage.bar_reader import StoreBarReader
from openbars.storage.parquet_duckdb import open_backend
from openbars.storage.postgres import (
    _CANON_DAILY_SQL,
    _CANON_INTRADAY_SQL,
    NATIVE_REQUIRED,
    PgCanonicalStore,
    ColumnInfo,
    check_native,
    native_ddl,
)
from openbars.storage.rows import CanonicalBarRow
from openbars.storage.schema import CANONICAL_BARS_SCHEMA, SCHEMA_VERSION, TS

IID = "us:eq:TEST1"
OTHER = "us:eq:TEST2"
D1, D2 = date(2025, 1, 2), date(2025, 1, 3)
WINDOW = DateRange(D1, D2)
T0 = datetime(2025, 1, 2, 22, 0, tzinfo=UTC)


def at(hours: float) -> datetime:
    return T0 + timedelta(hours=hours)


def row(day: date = D1, *, iid: str = IID, close: str = "10", revision: int = 1,
        available_at: datetime | None = None, freshness: str = "final", provider: str = "massive",
        interval: Interval = Interval.D1,
        hour: int = 0) -> CanonicalBarRow:
    ts = datetime(day.year, day.month, day.day, hour, tzinfo=UTC)
    return CanonicalBarRow(
        instrument_id=iid, market=Market.US, interval=interval, ts_start=ts, session_date=day,
        session=Session.REGULAR, provider=provider, open=Decimal("10"), high=Decimal("12"),
        low=Decimal("9"), close=Decimal(close), volume=Decimal("100"), price_source="massive:eod",
        reconcile_rule="primary_source", feed="eod", adjustment="split", revision=revision,
        generated_at=T0, available_at=available_at, freshness=freshness,
    )


def closes(table: pa.Table) -> list[tuple[str, date, str, int]]:
    return [
        (r["instrument_id"], r["session_date"], format(r["close"].normalize(), "f"), r["revision"])
        for r in table.to_pylist()
    ]


# --- schema ----------------------------------------------------------------------------


def test_available_at_is_an_additive_nullable_utc_column():
    field = CANONICAL_BARS_SCHEMA.field("available_at")
    assert field.nullable and field.type == TS == pa.timestamp("us", tz="UTC")
    assert SCHEMA_VERSION == 1
    assert CANONICAL_BARS_SCHEMA.field("freshness").nullable
    assert CANONICAL_BARS_SCHEMA.names[-2:] == ["available_at", "freshness"]


def test_freshness_is_independent_and_legacy_null_defaults_to_final(tmp_path):
    backend = open_backend(tmp_path)
    with pytest.raises(ValueError, match="freshness"):
        row(freshness="stale")
    backend.canonical.upsert_rows([row(freshness="provisional", available_at=at(1))])
    assert backend.canonical.scan_rows([IID], Interval.D1, WINDOW)[0].freshness == "provisional"
    _write_pre_154_part(backend, [row(D2)])
    assert {r.session_date: r.freshness for r in backend.canonical.scan_rows([IID], Interval.D1, WINDOW)} == {
        D1: "provisional", D2: "final"
    }


def test_naive_available_at_is_rejected():
    from openbars.storage.rows import InvalidBarError

    with pytest.raises(InvalidBarError):
        row(available_at=datetime(2025, 1, 2, 22))


# --- Parquet: write, read back, old files ----------------------------------------------


def test_parquet_write_and_read_back(tmp_path):
    backend = open_backend(tmp_path)
    backend.canonical.upsert_rows([row(available_at=at(1)), row(D2, available_at=None)])
    got = {r.session_date: r.available_at for r in backend.canonical.scan_rows([IID], Interval.D1, WINDOW)}
    assert got == {D1: at(1), D2: None}
    table = backend.reader.bars([IID], start=D1, end=D2)
    assert table.schema.field("available_at").type == TS
    assert table.column("available_at").to_pylist() == [at(1), None]


def _write_pre_154_part(backend, rows):
    """A canonical part exactly as a pre-#154 writer left it (no ``available_at`` column)."""
    from openbars.storage.rows import canonical_rows_to_table

    table = canonical_rows_to_table(rows)
    table = table.drop_columns(["available_at", "freshness"]).replace_schema_metadata(CANONICAL_BARS_SCHEMA.metadata)
    directory = backend.root / "canonical" / "market=us" / "interval=1d" / "year=2025"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "part-0000000000000000001-old00000.parquet"
    pq.write_table(table, path)
    assert "available_at" not in pq.read_schema(path).names
    assert "freshness" not in pq.read_schema(path).names
    return path


def test_old_files_read_as_null_and_are_hidden_by_available_by(tmp_path):
    backend = open_backend(tmp_path)
    _write_pre_154_part(backend, [row(), row(D2)])
    assert [r.available_at for r in backend.canonical.scan_rows([IID], Interval.D1, WINDOW)] == [None, None]
    table = backend.reader.bars([IID], start=D1, end=D2)
    assert table.num_rows == 2 and table.column("available_at").null_count == 2
    assert table.column("freshness").to_pylist() == ["final", "final"]
    assert backend.reader.bars([IID], start=D1, end=D2, available_by=at(1000)).num_rows == 0
    assert [r["session_date"] for r in load_canonical(backend, [IID], start=D1, end=D2)] == ["2025-01-02", "2025-01-03"]


def test_old_and_new_files_mix_and_a_null_is_filled_for_the_same_source_bar(tmp_path):
    backend = open_backend(tmp_path)
    _write_pre_154_part(backend, [row(), row(D2)])
    # the same source bar, now with a known availability time: null -> value is a change, so a
    # new part fills it (the old file is untouched; merge-on-read shows the new row)
    assert backend.canonical.upsert_rows([row(available_at=at(2))]) == 1
    got = {r.session_date: r.available_at for r in backend.canonical.scan_rows([IID], Interval.D1, WINDOW)}
    assert got == {D1: at(2), D2: None}
    assert closes(backend.reader.bars([IID], start=D1, end=D2, available_by=at(2))) == [(IID, D1, "10", 1)]
    backend.canonical.compact()
    got = {r.session_date: r.available_at for r in backend.canonical.scan_rows([IID], Interval.D1, WINDOW)}
    assert got == {D1: at(2), D2: None}


# --- available_by -------------------------------------------------------------------------


def test_available_by_selects_latest_available_revision_and_compaction_keeps_history(tmp_path):
    backend = open_backend(tmp_path)
    reader = backend.reader
    backend.canonical.upsert_rows([row(available_at=at(1), freshness="provisional"), row(D2, available_at=at(1)),
                                   row(iid=OTHER, available_at=at(1))])
    # A different provider supplies the final bar later.
    backend.canonical.upsert_rows([row(close="11", revision=2, available_at=at(5),
                                       provider="alpaca", freshness="final")])

    def read(by):
        return sorted(closes(reader.bars([IID, OTHER], start=D1, end=D2, available_by=by)))

    assert read(at(0)) == []
    assert read(at(1)) == sorted([(OTHER, D1, "10", 1), (IID, D1, "10", 1), (IID, D2, "10", 1)])
    assert read(at(5)) == sorted([(IID, D1, "11", 2), (OTHER, D1, "10", 1), (IID, D2, "10", 1)])
    selected = reader.bars([IID], start=D1, end=D1, available_by=at(1)).to_pylist()[0]
    assert selected["freshness"] == "provisional" and selected["available_at"] == at(1)
    selected = reader.bars([IID], start=D1, end=D1, available_by=at(5)).to_pylist()[0]
    assert selected["freshness"] == "final" and selected["available_at"] == at(5)
    # no available_by = today's behaviour (newest row, whatever its availability)
    assert sorted(closes(reader.bars([IID, OTHER], start=D1, end=D2))) == read(at(5))
    # the boundary is inclusive and any time zone is accepted
    tokyo = at(5).astimezone(__import__("zoneinfo").ZoneInfo("Asia/Tokyo"))
    assert read(tokyo) == read(at(5))
    # the same answer after compaction (superseded parts are gone, nothing changes)
    backend.canonical.compact()
    assert read(at(1)) == sorted([(OTHER, D1, "10", 1), (IID, D1, "10", 1), (IID, D2, "10", 1)])
    assert read(at(5)) == sorted([(IID, D1, "11", 2), (OTHER, D1, "10", 1), (IID, D2, "10", 1)])


def assert_latest_write_wins_with_reversed_availability(backend):
    store = backend.canonical
    store.upsert_rows([row(available_at=at(5), revision=9)])
    store.upsert_rows([row(close="11", revision=2, available_at=at(1), provider="alpaca")])
    for by in (at(3), at(6), None):
        (selected,) = backend.reader.bars([IID], start=D1, end=D2, available_by=by).to_pylist()
        assert (selected["close"], selected["revision"], selected["available_at"]) == (Decimal("11"), 2, at(1))


def test_parquet_pit_uses_write_order_when_availability_is_reversed(tmp_path):
    backend = open_backend(tmp_path)
    assert_latest_write_wins_with_reversed_availability(backend)
    backend.canonical.compact()
    for by in (at(3), at(6), None):
        (selected,) = backend.reader.bars([IID], start=D1, end=D2, available_by=by).to_pylist()
        assert (selected["close"], selected["revision"], selected["available_at"]) == (Decimal("11"), 2, at(1))


@pytest.mark.postgres_live
def test_postgres_pit_uses_write_order_when_availability_is_reversed(pg_dsn):
    from pg_helpers import throwaway_schema
    from openbars.storage.postgres import PostgresBackend

    with throwaway_schema(pg_dsn) as schema:
        backend = PostgresBackend(pg_dsn, mode="native", write=True, schema=schema)
        assert_latest_write_wins_with_reversed_availability(backend)


def test_available_by_must_be_timezone_aware(tmp_path):
    backend = open_backend(tmp_path)
    backend.canonical.upsert_rows([row(available_at=at(1))])
    with pytest.raises(ValueError):
        backend.reader.bars([IID], available_by=datetime(2025, 1, 3))
    with pytest.raises(QueryError):
        load_canonical(backend, [IID], available_by=datetime(2025, 1, 3))


def test_load_canonical_passes_available_by_for_daily_and_hourly(tmp_path):
    backend = open_backend(tmp_path)
    backend.canonical.upsert_rows([row(available_at=at(1)), row(D2, available_at=at(3))])
    hourly = [row(interval=Interval.H1, hour=15, available_at=at(1)),
              row(interval=Interval.H1, hour=16, available_at=at(3))]
    hourly = [replace(r, policy_version="research-daily-v2") for r in hourly]
    backend.canonical.upsert_rows(hourly)
    daily = load_canonical(backend, [IID], start=D1, end=D2, available_by=at(2))
    assert [r["session_date"] for r in daily] == ["2025-01-02"]
    assert len(load_canonical(backend, [IID], start=D1, end=D2)) == 2
    h1 = load_canonical(backend, [IID], interval=Interval.H1, start=D1, end=D1,
                        adjustment=__import__("openbars.core.models", fromlist=["Adjustment"]).Adjustment.SPLIT,
                        available_by=at(2), market=Market.US)
    assert [r["ts_start"] for r in h1] == ["2025-01-02T15:00:00+00:00"]


# --- write rule: earliest availability of the same source bar ---------------------------


def test_same_source_bar_keeps_its_earliest_available_at(tmp_path):
    backend = open_backend(tmp_path)
    store = backend.canonical

    def stored():
        return store.scan_rows([IID], Interval.D1, WINDOW)[0].available_at

    store.upsert_rows([row(available_at=at(2))])
    assert store.upsert_rows([row(available_at=at(9))]) == 0  # re-run: no new part, stays at(2)
    assert stored() == at(2)
    assert store.upsert_rows([row(available_at=at(1))]) == 1  # an earlier sighting wins
    assert stored() == at(1)
    assert store.upsert_rows([row(available_at=None)]) == 0
    assert stored() == at(1)
    # a different source bar (new revision / values) takes its own time
    store.upsert_rows([row(close="11", revision=2, available_at=at(7))])
    assert stored() == at(7)


def corrected(base: CanonicalBarRow, **changes) -> CanonicalBarRow:
    """Same key, revision and ``source_fingerprint`` as *base*: the daily path fingerprints
    without a source bar id, so a source switch or a price correction can share it (#154 review)."""
    return replace(base, source_fingerprint=base.source_fingerprint, **changes)


SAME_FINGERPRINT_CHANGES = [
    {"close": Decimal("11")},
    {"provider": "alpaca", "feed": "sip", "price_source": "alpaca:sip"},
    {"volume": Decimal("150")},
    {"volume_source": "alpaca:sip"},
]


@pytest.mark.parametrize("changes", SAME_FINGERPRINT_CHANGES)
def test_changed_source_or_price_with_the_same_fingerprint_takes_its_own_time(tmp_path, changes):
    store = open_backend(tmp_path).canonical
    first = row(available_at=at(2))
    store.upsert_rows([first])
    later = corrected(first, available_at=at(7), **changes)
    assert later.source_fingerprint == first.source_fingerprint
    store.upsert_rows([later])
    (stored,) = store.scan_rows([IID], Interval.D1, WINDOW)
    assert stored.available_at == at(7)
    reader = open_backend(tmp_path).reader
    for when, expected in ((at(5), first), (at(7), later), (None, later)):
        (selected,) = reader.bars([IID], start=D1, end=D1, available_by=when).to_pylist()
        for field in ("close", "provider", "feed", "price_source", "volume", "volume_source", "available_at"):
            assert selected[field] == getattr(expected, field)


# --- producers ------------------------------------------------------------------------------


def _raw_bar(provider="massive", feed="eod", *, interval=Interval.D1, hour=0, instrument_id=None):
    return RawBar(
        provider=provider, symbol_as_returned="TEST1", fetch_id="f", interval=interval,
        ts_start=datetime(2025, 1, 2, hour, tzinfo=UTC), session_date=D1, feed=feed,
        open=Decimal("10"), high=Decimal("12"), low=Decimal("9"), close=Decimal("10"),
        volume=Decimal("100"), instrument_id=instrument_id,
    )


def test_reloaded_raw_bar_carries_ingested_at_into_the_observation(tmp_path):
    backend = open_backend(tmp_path)
    request = FetchRequest(Market.US, Interval.D1, ("TEST1",), DateRange(D1, D1))
    backend.raw.append(RawBatch("massive", "f", request, (_raw_bar(),)))
    stored = backend.raw.scan_rows("massive", Market.US, Interval.D1, DateRange(D1, D1))[0]
    reloaded = next(backend.raw.scan("massive", Market.US, Interval.D1, DateRange(D1, D1)))
    assert observation_from_raw(reloaded).available_at == stored.ingested_at
    assert observation_from_raw(_raw_bar()).available_at is None  # straight from a provider


def test_raw_observation_tier_changes_after_local_session_day():
    same_day = replace(_raw_bar(), extra={"market": "us", "ingested_at": at(1)})
    next_day = replace(_raw_bar(), extra={"market": "us", "ingested_at": at(8)})
    assert observation_from_raw(same_day).freshness == "provisional"
    assert observation_from_raw(next_day).freshness == "final"


def test_us_daily_sink_uses_stored_ingested_at_or_the_write_time(tmp_path):
    backend = open_backend(tmp_path)
    sink = ReconcilingSink(MemoryReconciler(), parquet_canonical_writer(backend.canonical),
                           backend.raw, ("massive", "alpaca"))
    window = DateRange(D1, D1)
    # alpaca arrived earlier and is stored; massive (the primary) is fetched now
    old = _raw_bar("alpaca", "sip")
    backend.raw.append(RawBatch("alpaca", "f", FetchRequest(Market.US, Interval.D1, ("TEST1",), window), (old,)))
    before = datetime.now(UTC)
    fresh = _raw_bar()
    backend.raw.append(RawBatch("massive", "f", FetchRequest(Market.US, Interval.D1, ("TEST1",), window), (fresh,)))
    sink.after_fetch(Job("j", JobKind.DAILY, "massive", Market.US, Interval.D1, ("TEST1",), window), [fresh])
    (canon,) = backend.canonical.scan_rows(None, Interval.D1, window)
    assert canon.provider == "massive"
    raw_massive = backend.raw.scan_rows("massive", Market.US, Interval.D1, window)[0]
    # the fresh bar has no stored ingested_at in memory: the write time, never before the raw row
    assert raw_massive.ingested_at <= canon.available_at and before <= canon.available_at <= datetime.now(UTC)


def test_cn_and_hourly_producers_set_available_at(tmp_path):
    backend = open_backend(tmp_path)
    window = DateRange(D1, D1)
    rows = []
    sink = CnCanonicalSink(backend.instruments, rows.extend)
    cn_bar = replace(_raw_bar("baostock", "ashare"), symbol_as_returned="sh.600000")
    sink.after_fetch(Job("j", JobKind.DAILY, "baostock", Market.CN, Interval.D1, ("sh.600000",), window), [cn_bar])
    assert rows and all(r.available_at is not None for r in rows)

    hourly = _raw_bar(interval=Interval.H1, hour=15, instrument_id=IID, feed="aggs")
    request = FetchRequest(Market.US, Interval.H1, ("TEST1",), window)
    backend.raw.append(RawBatch("massive", "f", request, (hourly,)))
    stored = backend.raw.scan_rows("massive", Market.US, Interval.H1, window)[0]
    reloaded = list(backend.raw.scan("massive", Market.US, Interval.H1, window))
    (canon,) = reconcile_hourly(reloaded)
    assert canon.available_at == stored.ingested_at
    (fresh,) = reconcile_hourly([hourly])
    assert fresh.available_at is None  # the writer stamps it (parquet_canonical_writer)


# --- Postgres native (offline) ---------------------------------------------------------------


def test_postgres_native_without_history_filters_current_rows_by_availability():
    queries = []

    class Cursor:
        def fetchone(self):
            return {"name": None}

        def fetchall(self):
            return []

    class Connection:
        def execute(self, sql, params=()):
            queries.append((sql, params))
            return Cursor()

    @contextmanager
    def session():
        yield Connection()

    store = PgCanonicalStore(session, False, lambda _: Market.US)
    store.scan_rows([IID], Interval.D1, WINDOW, available_by=at(1))
    sql, params = queries[-1]
    # SQL's <= comparison excludes both later timestamps and NULL.
    assert "available_at <= ?" in sql
    assert at(1) in params

    queries.clear()
    store.scan_rows([IID], Interval.D1, WINDOW)
    sql, params = queries[-1]
    assert "available_at" not in sql
    assert at(1) not in params


def test_postgres_native_ddl_and_schema_check_treat_pit_fields_as_additive():
    from test_storage_postgres import fake_catalog

    ddl = native_ddl()
    create = next(s for s in ddl if s.startswith('CREATE TABLE IF NOT EXISTS "canonical_bars"'))
    assert '"available_at" TIMESTAMPTZ' in create
    assert '"freshness" TEXT' in create
    assert 'ALTER TABLE "canonical_bars" ADD COLUMN IF NOT EXISTS available_at TIMESTAMPTZ' in ddl
    assert 'ALTER TABLE "canonical_bars" ADD COLUMN IF NOT EXISTS freshness TEXT' in ddl
    assert any('"canonical_bars_history"' in s for s in ddl)
    catalog = fake_catalog(NATIVE_REQUIRED)
    check_native(catalog)
    del catalog["canonical_bars"]["available_at"]  # pre-#154 database
    del catalog["canonical_bars"]["freshness"]  # pre-#180 database
    check_native(catalog)
    catalog["canonical_bars"]["available_at"] = ColumnInfo("text")
    from openbars.storage.errors import SchemaMismatchError

    with pytest.raises(SchemaMismatchError):
        check_native(catalog)
    for sql in (_CANON_DAILY_SQL, _CANON_INTRADAY_SQL):
        for column in ("source_fingerprint", "price_source", "close", "volume_source"):
            assert f'"canonical_bars"."{column}" IS NOT DISTINCT FROM EXCLUDED."{column}"' in sql
        assert 'LEAST("canonical_bars"."available_at", EXCLUDED."available_at")' in sql
        assert '"freshness" = EXCLUDED."freshness"' in sql


class _FakeCanonical:
    def __init__(self, rows):
        self.rows = rows

    def scan_rows(self, ids, interval, window, *, market=None, policy_version=None):
        return [r for r in self.rows if r.instrument_id in ids and r.interval is interval]


class _FakeMaster:
    def get(self, instrument_id):
        return None

    def corporate_actions(self, ids):
        return []


class _FakeBackend:
    def __init__(self, rows):
        self.canonical = _FakeCanonical(rows)
        self.instruments = _FakeMaster()


def test_store_bar_reader_applies_the_same_available_by_rule():
    rows = [row(close="11", revision=2, available_at=at(5)), row(D2, available_at=at(1)),
            row(iid=OTHER, available_at=None)]
    reader = StoreBarReader(_FakeBackend(rows))
    got = reader.bars([IID, OTHER], start=D1, end=D2, market=Market.US, available_by=at(1))
    assert [(r.instrument_id, r.session_date) for r in got] == [(IID, D2)]
    assert len(reader.bars([IID, OTHER], start=D1, end=D2, market=Market.US)) == 3
    with pytest.raises(ValueError):
        reader.bars([IID], available_by=datetime(2025, 1, 3))


@pytest.mark.postgres_live
def test_live_postgres_native_matches_parquet(pg_dsn, tmp_path):
    from pg_helpers import throwaway_schema

    from openbars.storage.postgres import PostgresBackend

    with throwaway_schema(pg_dsn) as schema:
        pg = PostgresBackend(pg_dsn, mode="native", write=True, schema=schema)
        pq_backend = open_backend(tmp_path)
        for backend in (pg, pq_backend):
            store = backend.canonical
            store.upsert_rows([row(available_at=at(2)), row(D2, available_at=at(1)),
                               row(iid=OTHER, available_at=None)])
            store.upsert_rows([row(available_at=at(9))])  # same source bar: stays at(2)
            store.upsert_rows([row(D2, close="11", revision=2, available_at=at(5))])
        results = []
        for backend in (pg, pq_backend):
            stored = {(r.instrument_id, r.session_date): (r.available_at, r.revision)
                      for r in backend.canonical.scan_rows([IID, OTHER], Interval.D1, WINDOW)}
            got = {}
            for by in (at(0), at(2), at(5)):
                rows = load_canonical(backend, [IID, OTHER], start=D1, end=D2, market=Market.US, available_by=by)
                got[by] = sorted((r["instrument_id"], r["session_date"], r["close"]) for r in rows)
            results.append((stored, got))
        assert results[0] == results[1]
        stored, got = results[0]
        assert stored[(IID, D1)] == (at(2), 1) and stored[(OTHER, D1)] == (None, 1)
        assert stored[(IID, D2)] == (at(5), 2)
        assert got[at(0)] == [] and got[at(2)] == [(IID, "2025-01-02", "10.00000000")]
        assert got[at(5)] == [(IID, "2025-01-02", "10.00000000"), (IID, "2025-01-03", "11.00000000")]


@pytest.mark.postgres_live
def test_live_pre_154_postgres_reads_null_and_write_open_adds_the_column(pg_dsn):
    import psycopg
    from pg_helpers import throwaway_schema

    from openbars.storage.postgres import PostgresBackend

    with throwaway_schema(pg_dsn) as schema:
        PostgresBackend(pg_dsn, mode="native", write=True, schema=schema)
        with psycopg.connect(dsn := pg_dsn, autocommit=True) as conn:
            conn.execute(f'ALTER TABLE "{schema}".canonical_bars DROP COLUMN available_at')
        ro = PostgresBackend(dsn, mode="native", write=False, schema=schema)  # pre-#154 table is accepted
        assert ro.canonical.scan_rows([IID], Interval.D1, WINDOW) == []
        rw = PostgresBackend(dsn, mode="native", write=True, schema=schema)  # ADD COLUMN IF NOT EXISTS
        rw.canonical.upsert_rows([row(available_at=at(1))])
        assert [r.available_at for r in ro.canonical.scan_rows([IID], Interval.D1, WINDOW)] == [at(1)]


@pytest.mark.postgres_live
def test_live_postgres_changed_source_with_the_same_fingerprint(pg_dsn):
    from pg_helpers import throwaway_schema

    from openbars.storage.postgres import PostgresBackend

    with throwaway_schema(pg_dsn) as schema:
        store = PostgresBackend(pg_dsn, mode="native", write=True, schema=schema).canonical
        first = row(available_at=at(2))
        store.upsert_rows([first])
        store.upsert_rows([corrected(first, available_at=at(9))])  # identical bar: stays at(2)
        assert store.scan_rows([IID], Interval.D1, WINDOW)[0].available_at == at(2)
        for i, changes in enumerate(SAME_FINGERPRINT_CHANGES):
            when = at(10 + i)
            store.upsert_rows([corrected(first, available_at=when, **changes)])
            assert store.scan_rows([IID], Interval.D1, WINDOW)[0].available_at == when
            store.upsert_rows([first])  # back to the original bar for the next case
