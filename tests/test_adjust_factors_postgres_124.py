"""Postgres native stores and reads back A-share adjust factors (#124).

Offline tests need no database. Live tests (marker postgres_live) need OPENBARS_TEST_PG_DSN and
use a throwaway schema; the default check.sh run skips them.
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import date
from decimal import Decimal as D

import pytest
from cn_fakes import WEEK, CnFake

from openbars.core.models import Adjustment, DateRange, Interval, Market
from openbars.normalize.cn_adjust import AdjustFactor
from openbars.orchestrator.finalize import CnCanonicalSink
from openbars.orchestrator.jobs import Job, JobKind
from openbars.providers.base import FetchRequest
from openbars.providers.cn.baostock import BaoStockAdjustFactor
from openbars.storage.errors import SchemaMismatchError
from openbars.storage.postgres import (
    ADJUST_FACTORS_REQUIRED,
    NATIVE_REQUIRED,
    ColumnInfo,
    check_native,
    native_ddl,
)
from openbars.storage.schema import ADJUST_FACTORS_SCHEMA, REF_PARQUET_SCHEMAS, SCHEMA_VERSION, TABLE_SCHEMAS

IID = "cn:eq:xshg:600000"
OTHER = "cn:eq:xshe:000001"
FACTORS = [
    AdjustFactor(IID, date(2020, 1, 2), "baostock", "pct_return", D("1"), D("1"), D("1"), "sh.600000"),
    AdjustFactor(IID, date(2021, 6, 1), "baostock", "pct_return", D("0.812345"), D("1.230999"), D("1.230999"), "sh.600000"),
    AdjustFactor(IID, date(2021, 6, 1), "other", "ratio", D("0.5"), D("2"), D("2")),
    AdjustFactor(OTHER, date(2019, 3, 4), "baostock", "pct_return", D("0.123456789"), D("8.1"), D("8.1")),
]


def test_adjust_factors_table_is_additive_and_outside_the_shared_schemas():
    assert SCHEMA_VERSION == 1
    assert "adjust_factors" not in TABLE_SCHEMAS and "adjust_factors" not in NATIVE_REQUIRED
    assert REF_PARQUET_SCHEMAS["adjust_factors"] is ADJUST_FACTORS_SCHEMA  # Parquet unchanged
    create = next(s for s in native_ddl() if s.startswith('CREATE TABLE IF NOT EXISTS "adjust_factors"'))
    assert '"fore_adjust_factor" NUMERIC(20,8) NOT NULL' in create
    assert '"symbol" TEXT,' in create  # nullable
    assert 'PRIMARY KEY ("instrument_id", "source", "effective_from")' in create
    assert set(ADJUST_FACTORS_REQUIRED["adjust_factors"]) == set(ADJUST_FACTORS_SCHEMA.names)


def test_check_native_accepts_a_missing_table_and_rejects_a_wrong_one():
    from test_storage_postgres import fake_catalog

    catalog = fake_catalog(NATIVE_REQUIRED)
    check_native(catalog)  # pre-#124 database: no adjust_factors table
    catalog.update(fake_catalog(ADJUST_FACTORS_REQUIRED))
    check_native(catalog)
    catalog["adjust_factors"]["back_adjust_factor"] = ColumnInfo("numeric", 38, 2)
    with pytest.raises(SchemaMismatchError) as exc:
        check_native(catalog)
    assert {(p.table, p.column) for p in exc.value.problems} == {("adjust_factors", "back_adjust_factor")}


def _expected(factors):
    """The Parquet master's view of the same rows (key-merged, 8-digit half-even, key order)."""
    from openbars.storage.parquet_duckdb import _factor_record

    merged = {}
    for f in factors:
        merged[(f.instrument_id, f.source, f.effective_from)] = _factor_record(f)
    return [merged[k] for k in sorted(merged)]


def _as_records(factors):
    return [
        {"instrument_id": f.instrument_id, "source": f.source, "method": f.method,
         "effective_from": f.effective_from, "fore_adjust_factor": f.fore_adjust_factor,
         "back_adjust_factor": f.back_adjust_factor, "adjust_factor": f.adjust_factor, "symbol": f.symbol}
        for f in factors
    ]


@pytest.mark.postgres_live
def test_live_native_adjust_factors_round_trip_like_parquet(pg_dsn, tmp_path):
    from pg_helpers import throwaway_schema

    from openbars.storage.parquet_duckdb import open_backend
    from openbars.storage.postgres import PostgresBackend

    with throwaway_schema(pg_dsn) as schema:
        pg = PostgresBackend(pg_dsn, mode="native", write=True, schema=schema).instruments
        pq_master = open_backend(tmp_path).instruments
        for master in (pg, pq_master):
            assert master.adjust_factors() == []
            assert master.upsert_adjust_factors(FACTORS) == 4
            # new wins on the key; another source on the same day stays separate
            assert master.upsert_adjust_factors([AdjustFactor(IID, date(2021, 6, 1), "baostock", "pct_return",
                                                              D("0.8"), D("1.25"), D("1.25"), "sh.600000")]) == 1
        later = [*FACTORS, AdjustFactor(IID, date(2021, 6, 1), "baostock", "pct_return", D("0.8"), D("1.25"), D("1.25"), "sh.600000")]
        expected = _expected(later)
        got = _as_records(pg.adjust_factors())
        assert got == expected == _as_records(pq_master.adjust_factors())
        assert all(isinstance(r["back_adjust_factor"], D) for r in got)
        (other,) = [r for r in got if r["instrument_id"] == OTHER]
        assert other["fore_adjust_factor"] == D("0.12345679")  # 9 digits -> 8, half-even
        assert _as_records(pg.adjust_factors([OTHER])) == _as_records(pq_master.adjust_factors([OTHER]))
        assert _as_records(pg.adjust_factors([IID], source="other")) == [r for r in expected if r["source"] == "other"]
        assert pg.adjust_factors(["cn:eq:none"]) == []


@pytest.mark.postgres_live
def test_live_pre_124_database_reads_empty_then_write_open_adds_the_table(pg_dsn):
    import psycopg
    from pg_helpers import throwaway_schema

    from openbars.storage.postgres import PostgresBackend

    with throwaway_schema(pg_dsn) as schema:
        PostgresBackend(pg_dsn, mode="native", write=True, schema=schema)
        with psycopg.connect(pg_dsn, autocommit=True) as conn:
            conn.execute(f'DROP TABLE "{schema}".adjust_factors')
        ro = PostgresBackend(pg_dsn, mode="native", write=False, schema=schema)
        assert ro.instruments.adjust_factors() == [] and ro.instruments.adjust_factors([IID]) == []
        rw = PostgresBackend(pg_dsn, mode="native", write=True, schema=schema)
        assert rw.instruments.upsert_adjust_factors(FACTORS[:1]) == 1
        assert ro.instruments.adjust_factors() == FACTORS[:1]


@pytest.mark.postgres_live
def test_live_cn_sink_writes_factors_and_qfq_matches_parquet(pg_dsn, tmp_path):
    from pg_helpers import throwaway_schema

    from openbars.storage.parquet_duckdb import open_backend
    from openbars.storage.postgres import PostgresBackend

    window = DateRange(WEEK[0], WEEK[-1])
    factor = BaoStockAdjustFactor("sh.600000", date(2026, 3, 5), D("0.8"), D("1.25"), D("1.25"))
    with throwaway_schema(pg_dsn) as schema:
        pg = PostgresBackend(pg_dsn, mode="native", write=True, schema=schema)
        pq_backend = open_backend(tmp_path)
        out = []
        for backend in (pg, pq_backend):
            fake = CnFake(factors={"sh.600000": [factor]})
            sink = CnCanonicalSink(backend.instruments, backend.canonical.upsert_rows,
                                   providers={"baostock": fake}, factor_store=backend.instruments)
            batch = fake.fetch_bars(FetchRequest(Market.CN, Interval.D1, ("sh.600000",), window))
            backend.raw.append(batch)
            sink.after_fetch(Job("j", JobKind.DAILY, "baostock", Market.CN, Interval.D1, ("sh.600000",), window), batch.bars)
            assert sink.factor_errors == []
            factors = backend.instruments.adjust_factors([IID])
            assert factors, "the sink stored the provider's factors"
            reads = {}
            for mode in (Adjustment.RAW, Adjustment.QFQ, Adjustment.HFQ):
                result = backend.reader.bars([IID], start=WEEK[0], end=WEEK[-1], adjustment=mode, market=Market.CN)
                rows = result.to_pylist() if hasattr(result, "to_pylist") else [asdict(r) for r in result]
                reads[mode] = [(r["session_date"], r["close"], r["adjustment"]) for r in rows]
            out.append((_as_records(factors), reads))
        assert out[0] == out[1]
        qfq = out[0][1][Adjustment.QFQ]
        assert qfq and {a for *_, a in qfq} == {"qfq"}
        assert out[0][1][Adjustment.RAW] != qfq
