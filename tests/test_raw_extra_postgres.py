"""Postgres native keeps leftover RawBar.extra in a nullable JSONB column (#122).

Offline tests need no database. Live tests (marker postgres_live) need OPENBARS_TEST_PG_DSN
and use a throwaway schema; they are skipped by the default check.sh run.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import date
from decimal import Decimal as D

import pytest
from cn_fakes import PRICES, WEEK, CnFake

from openbars.core.models import DateRange, Interval, Market
from openbars.normalize.cn_quality import suspended_dates
from openbars.providers.base import FetchRequest, RawBatch
from openbars.storage.errors import SchemaMismatchError
from openbars.storage.postgres import (
    NATIVE_REQUIRED,
    ColumnInfo,
    PostgresBackend,
    check_native,
    native_ddl,
)
from openbars.storage.schema import RAW_BARS_SCHEMA, SCHEMA_VERSION, TABLE_SCHEMAS

WINDOW = DateRange(WEEK[0], WEEK[-1])
HALTED = date(2026, 3, 4)


def week_batch(provider: CnFake | None = None) -> RawBatch:
    return (provider or CnFake()).fetch_bars(FetchRequest(Market.CN, Interval.D1, ("sh.600000",), WINDOW))


def test_schema_version_and_shared_raw_schema_are_unchanged():
    assert SCHEMA_VERSION == 1
    assert "extra" not in RAW_BARS_SCHEMA.names
    assert "extra" not in TABLE_SCHEMAS["raw_bars"].names
    assert "extra" not in NATIVE_REQUIRED["raw_bars"]
    ddl = native_ddl()
    create = next(s for s in ddl if s.startswith('CREATE TABLE IF NOT EXISTS "raw_bars"'))
    assert "extra" not in create  # CREATE stays 1:1 with RAW_BARS_SCHEMA
    assert any(s == 'ALTER TABLE "raw_bars" ADD COLUMN IF NOT EXISTS extra JSONB' for s in ddl)


def test_check_native_allows_a_missing_extra_column_and_rejects_a_wrong_type():
    from test_storage_postgres import fake_catalog

    catalog = fake_catalog(NATIVE_REQUIRED)
    check_native(catalog)  # no extra column: pre-#122 database
    catalog["raw_bars"]["extra"] = ColumnInfo("jsonb")
    check_native(catalog)
    catalog["raw_bars"]["extra"] = ColumnInfo("text")
    with pytest.raises(SchemaMismatchError) as exc:
        check_native(catalog)
    problem = next(p for p in exc.value.problems if p.column == "extra")
    assert (problem.kind, problem.expected, problem.actual) == ("type_mismatch", "jsonb", "text")


def _scan(backend):
    return {b.session_date: b for b in backend.raw.scan("baostock", Market.CN, Interval.D1, WINDOW)}


@pytest.mark.postgres_live
def test_live_tradestatus_isst_preclose_survive_the_postgres_round_trip(pg_dsn):
    from pg_helpers import throwaway_schema

    with throwaway_schema(pg_dsn) as schema:
        rw = PostgresBackend(pg_dsn, mode="native", write=True, schema=schema)
        assert rw.raw.append(week_batch()) == len(WEEK)
        bars = _scan(rw)
        assert sorted(bars) == WEEK
        for day, bar in bars.items():
            *_, preclose, status = PRICES[day]
            assert bar.extra["tradestatus"] == status and bar.extra["isST"] == 0
            assert bar.extra["preclose"] == preclose and bar.extra["code"] == "sh.600000"
        assert suspended_dates(bars.values()) == {HALTED}

        empty = replace(CnFake().bar("sh.600000", WEEK[-1], "x"), extra={})
        # empty leftover is NULL; first-class fields still merge back on to_raw_bar
        rw.raw.append(RawBatch("baostock", "f-empty", week_batch().request, (empty,)))
        back = _scan(rw)[WEEK[-1]]
        assert back.extra.get("tradestatus") is None
        # typed leftovers
        extra = {
            "tradestatus": 0,
            "isST": True,
            "preclose": D("10.15"),
            "listed": date(2020, 1, 2),
            "tags": ["a", 1],
            "x": None,
        }
        typed = replace(CnFake().bar("sh.600000", WEEK[0], "x"), extra={**CnFake().bar("sh.600000", WEEK[0], "x").extra, **extra})
        rw.raw.append(RawBatch("baostock", "f-typed", week_batch().request, (typed,)))
        got = _scan(rw)[WEEK[0]].extra
        for key, value in extra.items():
            assert got[key] == value and type(got[key]) is type(value)


@pytest.mark.postgres_live
def test_live_pre_migration_catalog_is_readable_and_write_adds_the_column(pg_dsn):
    import psycopg
    from pg_helpers import throwaway_schema

    with throwaway_schema(pg_dsn) as schema:
        rw = PostgresBackend(pg_dsn, mode="native", write=True, schema=schema)
        first = replace(CnFake().bar("sh.600000", WEEK[0], "legacy"), extra={})
        assert rw.raw.append(RawBatch("baostock", "legacy", week_batch().request, (first,))) == 1
        with psycopg.connect(pg_dsn, autocommit=True) as admin:
            admin.execute(f'SET search_path TO "{schema}"')
            admin.execute("ALTER TABLE raw_bars DROP COLUMN extra")

        ro = PostgresBackend(pg_dsn, mode="native", write=False, schema=schema)
        bars = _scan(ro)
        assert list(bars) == [WEEK[0]]
        assert bars[WEEK[0]].extra.get("tradestatus") is None  # missing column -> no leftover

        migrated = PostgresBackend(pg_dsn, mode="native", write=True, schema=schema)
        old = _scan(migrated)[WEEK[0]]
        assert old.extra.get("tradestatus") is None  # existing row is NULL leftover
        halted = CnFake().bar("sh.600000", HALTED, "new")
        assert migrated.raw.append(RawBatch("baostock", "new", week_batch().request, (halted,))) == 1
        back = _scan(migrated)
        assert back[WEEK[0]].extra.get("tradestatus") is None
        assert back[HALTED].extra["tradestatus"] == 0
        assert suspended_dates(back.values()) == {HALTED}
