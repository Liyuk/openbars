"""CLI plan/run against storage.backend: postgres. Live cases need OPENBARS_TEST_PG_DSN."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

import pytest
from test_cli_plan_run_status import NOW, SECRET, factory

from openbars.cli.main import main
from openbars.orchestrator.jobs import JobState
from openbars.storage.state import SqliteStateStore

pytestmark = pytest.mark.usefixtures("no_network")

_DDL = (Path(__file__).parent / "compat" / "upstream_schema.sql").read_text()


def _config(tmp_path: Path, schema: str, *, write: bool = True, symbols: str = "[TEST1, ZZZA]", quota: str = "sqlite") -> str:
    path = tmp_path / "config.yaml"
    path.write_text(
        f"""
version: 1
markets: [us]
intervals: [1d]
history: {{start: 2026-03-02, end: null}}
universe:
  us:
    symbols: {symbols}
storage:
  backend: postgres
  path: {tmp_path / "data"}
  postgres:
    dsn: ${{OPENBARS_PG_DSN}}
    mode: upstream_compat
    write: {"true" if write else "false"}
    schema: {schema}
    quota: {quota}
providers:
  massive:
    enabled: true
    api_key: ${{TEST_API_KEY}}
"""
    )
    return str(path)


def _cli(argv, tmp_path, schema, *, dsn: str, provider_factory=None, write: bool = True, symbols: str = "[TEST1, ZZZA]", quota: str = "sqlite"):
    return main(
        ["--config", _config(tmp_path, schema, write=write, symbols=symbols, quota=quota), *argv],
        provider_factory=provider_factory or factory(),
        clock=lambda: NOW,
        env={"TEST_API_KEY": SECRET, "OPENBARS_PG_DSN": dsn},
    )


@contextmanager
def _compat_schema(dsn: str, prefix: str):
    import psycopg

    schema = f"{prefix}_{datetime.now(UTC):%H%M%S%f}"
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    try:
        with psycopg.connect(dsn, autocommit=True, options=f"-c search_path={schema}") as conn:
            conn.execute(_DDL)
        yield schema
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(f'DROP SCHEMA "{schema}" CASCADE')


def _counts(dsn: str, schema: str) -> tuple[int, int, int]:
    import psycopg

    with psycopg.connect(dsn, options=f"-c search_path={schema}") as conn:
        daily = conn.execute("SELECT count(*) FROM daily_bar").fetchone()[0]
        research = conn.execute("SELECT count(*) FROM research_daily_bar").fetchone()[0]
        missing = conn.execute(
            "SELECT count(*) FROM research_daily_bar WHERE selected_source_bar_id IS NULL"
        ).fetchone()[0]
    return int(daily), int(research), int(missing)


@pytest.mark.postgres_live
def test_live_cli_plan_dry_run_does_not_write(pg_dsn, tmp_path, capsys):
    with _compat_schema(pg_dsn, "openbars_cli_dry") as schema:
        assert _cli(["plan"], tmp_path, schema, dsn=pg_dsn) == 0
        out = capsys.readouterr().out
        assert "dry run" in out and "plan: 6 jobs" in out
        assert not (tmp_path / "data" / "state.db").exists() or (
            SqliteStateStore(tmp_path / "data" / "state.db").list_jobs() == []
        )
        daily, research, _ = _counts(pg_dsn, schema)
        assert daily == 0 and research == 0


@pytest.mark.postgres_live
def test_live_cli_plan_submit_and_run_write_upstream_compat(pg_dsn, tmp_path, capsys):
    with _compat_schema(pg_dsn, "openbars_cli_run") as schema:
        assert _cli(["plan"], tmp_path, schema, dsn=pg_dsn, write=True) == 0
        capsys.readouterr()
        assert _cli(["plan", "--submit"], tmp_path, schema, dsn=pg_dsn) == 0
        out = capsys.readouterr().out
        assert "submitted 6 new jobs" in out
        state = SqliteStateStore(tmp_path / "data" / "state.db")
        assert state.count_jobs()[JobState.PENDING] == 6
        daily, research, _ = _counts(pg_dsn, schema)
        assert daily == 0 and research == 0  # submit queues jobs only

        assert _cli(["status"], tmp_path, schema, dsn=pg_dsn) == 0
        assert "pending=6" in capsys.readouterr().out

        assert _cli(["run"], tmp_path, schema, dsn=pg_dsn) == 0
        out = capsys.readouterr().out
        assert "done 6" in out and "dead 0" in out
        assert state.count_jobs()[JobState.DONE] == 6
        daily, research, missing = _counts(pg_dsn, schema)
        assert daily >= 12 and research == 12 and missing == 0


@pytest.mark.postgres_live
def test_live_cli_run_prints_symbol_ambiguity_report(pg_dsn, tmp_path, capsys):
    import psycopg

    with _compat_schema(pg_dsn, "openbars_cli_amb") as schema:
        with psycopg.connect(pg_dsn, autocommit=True, options=f"-c search_path={schema}") as conn:
            conn.execute("INSERT INTO instrument (id, asset_class, security_type) VALUES (10, 'equity', 'common_stock')")
            conn.execute("INSERT INTO instrument (id, asset_class, security_type) VALUES (11, 'equity', 'common_stock')")
            conn.execute(
                "INSERT INTO symbol_alias (instrument_id, symbol, exchange, valid_from, valid_from_basis, verification_status) "
                "VALUES (10, 'TEST1', 'TESTX', '2010-01-01', 'listing', 'verified'), "
                "(11, 'TEST1', 'TESTX', '2020-01-01', 'listing', 'verified')"
            )
        code = _cli(["run"], tmp_path, schema, dsn=pg_dsn, symbols="[TEST1]")
        captured = capsys.readouterr()
        assert code == 0  # jobs are marked dead; the scheduler itself succeeds
        text = captured.out + captured.err
        assert "dead " in captured.out
        assert "instrument_id=10" in text and "instrument_id=11" in text
        assert "pass a numeric instrument_id" in text
        assert "TEST1" in text


@pytest.mark.postgres_live
def test_live_cli_run_uses_postgres_quota_gate(pg_dsn, tmp_path, capsys):
    import psycopg

    with _compat_schema(pg_dsn, "openbars_cli_quota") as schema:
        assert _cli(["run"], tmp_path, schema, dsn=pg_dsn, quota="postgres") == 0
        out = capsys.readouterr().out
        assert "done 6" in out and "dead 0" in out
        with psycopg.connect(pg_dsn, options=f"-c search_path={schema}") as conn:
            usage = conn.execute("SELECT count(*) FROM provider_request_usage").fetchone()[0]
            gates = conn.execute("SELECT provider FROM provider_request_gate").fetchall()
        assert int(usage) >= 6
        assert "massive" in {row[0] for row in gates}
        # default sqlite file should not be required when the postgres gate is selected
        # (it may still exist if something else created data/, but quota.db should be unused)
        assert not (tmp_path / "data" / "quota.db").exists()
