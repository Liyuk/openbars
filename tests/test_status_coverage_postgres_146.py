"""#146: ``openbars status`` raw coverage summary on the Postgres backend.

Offline tests use a fake session; live tests (marker postgres_live, OPENBARS_TEST_PG_DSN) use a
throwaway schema and compare against the Parquet store on the same synthetic rows.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from synth import raw_batch, raw_batch_h1, weekdays

from openbars.cli.main import main
from openbars.core.models import Interval, Market
from openbars.storage.errors import CoverageTimeout
from openbars.storage.postgres import PgRawStore

pytestmark = pytest.mark.usefixtures("no_network")

DAYS = weekdays(date(2026, 3, 2), 5)
SECRET = "tkn-0123" + "456789abcdef"


class QueryCanceled(Exception):  # stands in for psycopg.errors.QueryCanceled
    pass


class FakeConn:
    def __init__(self, record=None, cancel=False):
        self.record, self.cancel, self.sql = record, cancel, []

    def execute(self, sql, params=()):
        self.sql.append((sql, list(params)))
        if self.cancel and not sql.startswith("SET LOCAL"):
            raise QueryCanceled("canceling statement due to statement timeout")
        return self

    def fetchone(self):
        return self.record


def _store(conn):
    @contextmanager
    def session():
        yield conn

    return PgRawStore(session, write=False)


def test_native_summary_is_one_bounded_aggregate():
    conn = FakeConn({"first": DAYS[0], "last": DAYS[-1], "sessions": 5, "bars": 10})
    assert _store(conn).coverage_summary("prov_a", Market.US, Interval.D1, timeout_ms=2500) == (DAYS[0], DAYS[-1], 5, 10)
    (set_sql, _), (sql, params) = conn.sql
    assert set_sql == "SET LOCAL statement_timeout = 2500"
    assert "count(DISTINCT session_date)" in sql and 'provider = ? AND market = ? AND "interval" = ?' in sql
    assert params == ["prov_a", "us", "1d"]
    empty = FakeConn({"first": None, "last": None, "sessions": 0, "bars": 0})
    assert _store(empty).coverage_summary("prov_a", Market.US, Interval.H1) == (None, None, 0, 0)


def test_timeout_becomes_coverage_timeout():
    with pytest.raises(CoverageTimeout, match="exceeded 1s"):
        _store(FakeConn(cancel=True)).coverage_summary("prov_a", Market.US, Interval.D1, timeout_ms=1000)


def test_cli_status_prints_unavailable_on_timeout(tmp_path, capsys, monkeypatch):
    from types import SimpleNamespace

    import openbars.cli.main as cli_main

    def summary(provider, market, interval):
        if interval is Interval.H1:
            raise CoverageTimeout("coverage summary exceeded 15s")
        return DAYS[0], DAYS[-1], 5, 10

    fake = SimpleNamespace(raw=SimpleNamespace(coverage_summary=summary), state=None)
    monkeypatch.setattr(cli_main, "_open_backend", lambda *a, **k: fake)
    monkeypatch.setattr(cli_main, "_job_state", lambda backend, db: _EmptyState())
    config = _config(tmp_path, "native", "s", intervals="[1d, 1h]")
    assert main(["--config", config, "status"], env={"TEST_API_KEY": SECRET, "OPENBARS_PG_DSN": "postgresql://u:pw-secret-123@h/db"}) == 0
    out = capsys.readouterr().out
    assert "massive us/1d: 2026-03-02..2026-03-06 (10 bars, 5 sessions)" in out
    assert "massive us/1h: unavailable (coverage summary exceeded 15s)" in out
    assert "not available for this backend" not in out
    assert "pw-secret-123" not in out and SECRET not in out


class _EmptyState:
    def count_jobs(self):
        from openbars.orchestrator.jobs import JobState

        return {s: 0 for s in JobState}

    def count_jobs_by_provider(self):
        return {}

    def list_jobs(self, *a, **k):
        return []

    def __getattr__(self, name):
        return lambda *a, **k: {} if name.startswith("count") else []


def _config(tmp_path: Path, mode: str, schema: str, intervals: str = "[1d]") -> str:
    path = tmp_path / "config.yaml"
    path.write_text(
        f"""
version: 1
markets: [us]
intervals: {intervals}
history: {{start: 2026-03-02, end: null}}
universe:
  us:
    symbols: [TEST1]
storage:
  backend: postgres
  path: {tmp_path / "data"}
  postgres:
    dsn: ${{OPENBARS_PG_DSN}}
    mode: {mode}
    write: false
    schema: {schema}
providers:
  massive:
    enabled: true
    api_key: ${{TEST_API_KEY}}
  prov_a:
    enabled: true
"""
    )
    return str(path)


def _write(raw):
    for i in range(2):  # the same keys twice: Parquet keeps both parts, Postgres upserts
        raw.append(raw_batch(["TEST1", "TEST2"], DAYS, base=Decimal("11.25") + i, fetch_id=f"d{i}"))
    raw.append(raw_batch_h1(["TEST1"], DAYS[:2]))


@pytest.mark.postgres_live
def test_live_native_summary_matches_parquet_and_status_prints_it(pg_dsn, tmp_path, capsys):
    from pg_helpers import throwaway_schema

    from openbars.storage.parquet_duckdb import open_backend
    from openbars.storage.postgres import PostgresBackend

    with throwaway_schema(pg_dsn) as schema:
        pg = PostgresBackend(pg_dsn, mode="native", write=True, schema=schema)
        pq = open_backend(tmp_path / "pq")
        _write(pg.raw)
        _write(pq.raw)
        for market, interval in ((Market.US, Interval.D1), (Market.US, Interval.H1), (Market.CRYPTO, Interval.D1)):
            assert pg.raw.coverage_summary("prov_a", market, interval) == pq.raw.coverage_summary("prov_a", market, interval)
        assert pg.raw.coverage_summary("prov_a", Market.US, Interval.D1) == (DAYS[0], DAYS[-1], 5, 10)
        assert pg.raw.coverage_summary("nobody", Market.US, Interval.D1) == (None, None, 0, 0)
        ro = PostgresBackend(pg_dsn, mode="native", write=False, schema=schema)
        assert ro.raw.coverage_summary("prov_a", Market.US, Interval.H1)[3] == 7 * 2
        config = _config(tmp_path, "native", schema, intervals="[1d, 1h]")
        assert main(["--config", config, "status"], env={"TEST_API_KEY": SECRET, "OPENBARS_PG_DSN": pg_dsn}) == 0
        out = capsys.readouterr().out
        assert "prov_a us/1d: 2026-03-02..2026-03-06 (10 bars, 5 sessions)" in out
        assert "prov_a us/1h: 2026-03-02..2026-03-03 (14 bars, 2 sessions)" in out
        assert "massive us/1d: none" in out
        assert pg_dsn not in out and "not available for this backend" not in out


@pytest.mark.postgres_live
def test_live_compat_summary_reads_daily_bar(pg_dsn, tmp_path):
    import psycopg
    from test_cli_postgres import _compat_schema

    from openbars.storage.postgres import PostgresBackend

    with _compat_schema(pg_dsn, "openbars_cov") as schema:
        with psycopg.connect(pg_dsn, autocommit=True, options=f"-c search_path={schema}") as conn:
            conn.execute("INSERT INTO instrument (id, asset_class, security_type) VALUES (10, 'equity', 'common_stock')")
            for i, day in enumerate(DAYS[:3]):
                for revision in (1, 2):  # two revisions of a day count as two bars, one session
                    conn.execute(
                        "INSERT INTO daily_bar (instrument_id, session_date, provider, feed, adjustment, revision, "
                        "open, high, low, close, volume) VALUES (10, %s, 'massive', 'eod', 'split', %s, 1, 2, 1, 2, 5)",
                        [day, revision],
                    )
        pg = PostgresBackend(pg_dsn, mode="upstream_compat", write=False, schema=schema, state_path=tmp_path / "state.db")
        assert pg.raw.coverage_summary("massive", Market.US, Interval.D1) == (DAYS[0], DAYS[2], 3, 6)
        assert pg.raw.coverage_summary("alpaca", Market.US, Interval.D1) == (None, None, 0, 0)
        assert pg.raw.coverage_summary("massive", Market.US, Interval.H1) == (None, None, 0, 0)
        assert pg.raw.coverage_summary("massive", Market.CRYPTO, Interval.D1) == (None, None, 0, 0)
