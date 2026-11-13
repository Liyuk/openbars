"""Generic Postgres export plugin (#203): a reference layout, no caller foreign keys.

Offline tests use a fake connection to inspect the SQL and parameters; the live tests
(marker ``postgres_live``) need ``OPENBARS_TEST_PG_DSN`` and only ever touch a throwaway
schema created and dropped by the test.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from openbars.cli.main import main
from openbars.export_postgres import REFERENCE_COLUMNS, REFERENCE_KEY, REFERENCE_TABLE, reference_ddl
from openbars.storage.schema import CANONICAL_BARS_SCHEMA

DAY_CLOSES = [(date(2026, 1, 1), "10.12345678"), (date(2026, 1, 3), "11.00000001"), (date(2026, 1, 5), "12.00000002")]


def _bar(day: date, close: str, **extra) -> dict:
    row = {
        "instrument_id": "us:eq:TEST1", "market": "us", "interval": "1d",
        "ts_start": datetime.combine(day, datetime.min.time(), UTC), "session_date": day,
        "session": "regular", "policy_version": "research-daily-v2", "provider": "fake",
        "feed": "sip", "adjustment": "raw", "revision": 0,
        "open": Decimal(close), "high": Decimal(close), "low": Decimal(close), "close": Decimal(close),
        "volume": Decimal("123.00000001"), "vwap": None, "trade_count": 10,
        "flags": ["DIVERGENT"], "selection_reason": "primary_source", "quality_status": "complete",
        "capabilities": ["currency:usd"], "source_fingerprint": "fingerprint-1", "price_source": "fake",
        "volume_source": None, "sources_seen": ["fake"], "reconcile_rule": "test",
        "generated_at": datetime(2026, 1, 5, tzinfo=UTC), "available_at": datetime(2026, 1, 6, tzinfo=UTC),
        "freshness": "provisional",
    }
    row.update(extra)
    return row


def _source(path, *, contract: bool = False) -> None:
    """Write a canonical Parquet file; ``contract`` adds the #202 ``source_fetch_id`` column."""
    path.mkdir(parents=True, exist_ok=True)
    schema = CANONICAL_BARS_SCHEMA
    rows = [_bar(day, close) for day, close in DAY_CLOSES]
    if contract:
        schema = schema.append(pa.field("source_fetch_id", pa.string(), nullable=True))
        for row in rows:
            row["source_fetch_id"] = "fetch-1"
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path / "part-0.parquet")


class FakeCursor:
    def __init__(self, conn): self.conn = conn
    def __enter__(self): return self
    def __exit__(self, *exc): return False
    def execute(self, sql, params=None): self.conn.statements.append((sql, params))
    def executemany(self, sql, params):
        self.conn.statements.extend((sql, row) for row in params)
    def close(self): pass


class FakeConnection:
    def __init__(self): self.statements = []; self.commits = 0
    def cursor(self): return FakeCursor(self)
    def commit(self): self.commits += 1
    def rollback(self): pass
    def close(self): pass


def _inserts(statements):
    return [(sql, params) for sql, params in statements if sql.lstrip().upper().startswith("INSERT")]


def _values(params) -> dict:
    return {column: value for (column, _, _), value in zip(REFERENCE_COLUMNS, params)}


def test_dry_run_reports_window_and_never_connects(tmp_path, monkeypatch, capsys):
    source = tmp_path / "source"; _source(source)
    import openbars.export_postgres as exporter
    monkeypatch.setattr(exporter, "_connect", lambda dsn: (_ for _ in ()).throw(AssertionError("dry-run connected")))
    assert main(["export-postgres", "--source", str(source), "--dsn", "postgresql://unused",
                 "--since", "2026-01-03"]) == 0
    out = capsys.readouterr().out
    assert "2 rows" in out and "dry-run" in out and REFERENCE_TABLE in out


def test_dry_run_on_empty_source_never_connects(tmp_path, monkeypatch, capsys):
    import openbars.export_postgres as exporter
    monkeypatch.setattr(exporter, "_connect", lambda dsn: pytest.fail("empty source connected"))
    assert main(["export-postgres", "--source", str(tmp_path), "--dsn", "unused", "--apply"]) == 0
    out = capsys.readouterr().out
    assert "0 rows" in out and "nothing to write" in out


def test_apply_writes_window_rows_with_source_information(tmp_path, monkeypatch, capsys):
    source = tmp_path / "source"; _source(source)
    import openbars.export_postgres as exporter
    conn = FakeConnection()
    monkeypatch.setattr(exporter, "_connect", lambda dsn: conn)
    argv = ["export-postgres", "--source", str(source), "--dsn", "postgresql://unused", "--days", "3", "--apply"]
    assert main(argv) == 0
    inserts = _inserts(conn.statements)
    assert len(inserts) == 2  # only 2026-01-03 and 2026-01-05 fall inside the 3-day window
    sql = inserts[0][0]
    assert sql.startswith(f'INSERT INTO "{REFERENCE_TABLE}"')
    key = ", ".join(f'"{column}"' for column in REFERENCE_KEY)
    assert f"ON CONFLICT ({key})" in sql
    # no caller provenance foreign keys and no integer upstream id remapping
    for forbidden in ("selected_source_bar_id", "source_artifact_id", "upstream_id"):
        assert forbidden not in sql
    for _, params in inserts:
        values = _values(params)
        assert values["instrument_id"] == "us:eq:TEST1"  # openbars id, written as text
        assert values["provider"] == "fake" and values["feed"] == "sip"
        assert values["session_date"] in {date(2026, 1, 3), date(2026, 1, 5)}
        assert values["freshness"] == "provisional"
        assert values["available_at"] == datetime(2026, 1, 6, tzinfo=UTC)
        assert values["source_fingerprint"] == "fingerprint-1"
        assert values["generated_at"] == datetime(2026, 1, 5, tzinfo=UTC)
        assert values["source_fetch_id"] is None
        assert isinstance(values["open"], Decimal) and values["volume"] == Decimal("123.00000001")
        assert json.loads(values["capabilities"]) == ["currency:usd"]
        assert json.loads(values["flags"]) == ["DIVERGENT"]
    assert conn.commits == 1
    assert "applied 2 idempotent upserts" in capsys.readouterr().out


def test_apply_keeps_fetch_batch_pointer_when_canonical_carries_it(tmp_path, monkeypatch):
    source = tmp_path / "source"; _source(source, contract=True)
    import openbars.export_postgres as exporter
    conn = FakeConnection()
    monkeypatch.setattr(exporter, "_connect", lambda dsn: conn)
    assert main(["export-postgres", "--source", str(source), "--dsn", "unused", "--apply"]) == 0
    values = _values(_inserts(conn.statements)[0][1])
    assert values["source_fetch_id"] == "fetch-1"


def test_replay_reuses_same_conflict_keys(tmp_path, monkeypatch):
    source = tmp_path / "source"; _source(source)
    import openbars.export_postgres as exporter
    conn = FakeConnection()
    monkeypatch.setattr(exporter, "_connect", lambda dsn: conn)
    argv = ["export-postgres", "--source", str(source), "--dsn", "unused", "--apply"]
    assert main(argv) == 0
    first = _inserts(conn.statements)
    assert main(argv) == 0
    second = _inserts(conn.statements[len(first):])
    assert [sql for sql, _ in second] == [sql for sql, _ in first]
    assert [params for _, params in second] == [params for _, params in first]


def test_reference_ddl_matches_the_insert_layout():
    ddl = reference_ddl()
    lines = {line.strip().rstrip(",") for line in ddl.splitlines()}
    for column, kind, nullable in REFERENCE_COLUMNS:
        assert f'"{column}" {kind}' + ("" if nullable else " NOT NULL") in lines
    key = ", ".join(f'"{column}"' for column in REFERENCE_KEY)
    assert f"PRIMARY KEY ({key})" in lines
    with pytest.raises(ValueError):
        reference_ddl("schema.table")


def test_doc_ddl_matches_reference_ddl():
    from pathlib import Path

    doc = Path(__file__).resolve().parents[1] / "docs" / "postgres-export.md"
    assert f"```sql\n{reference_ddl()}\n```" in doc.read_text(encoding="utf-8")


def test_core_cli_survives_missing_postgres_extra(monkeypatch, capsys):
    import sys
    monkeypatch.setitem(sys.modules, "psycopg", None)
    with pytest.raises(SystemExit) as exit_info:
        main(["--version"])
    assert exit_info.value.code == 0
    assert "openbars" in capsys.readouterr().out


def test_apply_without_psycopg_prints_extra_install_hint(monkeypatch, capsys):
    import sys
    import openbars.export_postgres as exporter
    monkeypatch.setitem(sys.modules, "psycopg", None)
    monkeypatch.setattr(exporter, "_rows", lambda *args: [_bar(date(2026, 1, 5), "1")])
    result = main(["export-postgres", "--source", "unused", "--dsn", "postgresql://unused", "--apply"])
    assert result == 1
    assert "openbars[postgres-export]" in capsys.readouterr().err


def test_caller_specific_layout_flags_are_gone(tmp_path, capsys):
    with pytest.raises(SystemExit) as exit_info:
        main(["export-postgres", "--source", str(tmp_path), "--dsn", "unused",
              "--table", "daily_bar", "--table-layout", "daily_bar", "--mapping", "ids.csv"])
    assert exit_info.value.code == 2
    err = capsys.readouterr().err
    assert "--table-layout" in err


@pytest.mark.parametrize("table", ["bad;drop", "schema.table", "", "a" * 64])
def test_invalid_table_rejected_before_connecting(tmp_path, monkeypatch, capsys, table):
    source = tmp_path / "source"; _source(source)
    import openbars.export_postgres as exporter
    monkeypatch.setattr(exporter, "_connect", lambda dsn: pytest.fail("unexpected connection"))
    assert main(["export-postgres", "--source", str(source), "--dsn", "unused", "--table", table, "--apply"]) != 0
    assert "table" in capsys.readouterr().err


def test_dsn_password_is_not_echoed(tmp_path, monkeypatch, capsys):
    source = tmp_path / "source"; _source(source)
    import openbars.export_postgres as exporter
    dsn = "postgresql://openbars:verysecret@127.0.0.1:55432/test"
    monkeypatch.setattr(exporter, "_connect", lambda dsn: (_ for _ in ()).throw(RuntimeError(f"cannot reach {dsn}")))
    assert main(["export-postgres", "--source", str(source), "--dsn", dsn, "--apply"]) == 1
    err = capsys.readouterr().err
    assert "verysecret" not in err and "[REDACTED]" in err


# --- live database: throwaway schema, never a shared / production database -----------------


def _insert_dump(dsn: str, schema: str):
    import psycopg

    with psycopg.connect(_with_search_path(dsn, schema)) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f'SELECT "instrument_id", "session_date", "provider", "feed", "freshness", '
                f'"available_at", "source_fingerprint", "generated_at", "volume", "capabilities" '
                f'FROM "{REFERENCE_TABLE}" ORDER BY "session_date"'
            )
            return cur.fetchall()


def _with_search_path(dsn: str, schema: str) -> str:
    separator = "&" if "?" in dsn else "?"
    return f"{dsn}{separator}options=-csearch_path%3D{schema}"


@pytest.mark.postgres_live
def test_live_export_apply_window_and_idempotent_replay(pg_dsn, tmp_path):
    import psycopg
    from pg_helpers import throwaway_schema

    source = tmp_path / "source"; _source(source)
    with throwaway_schema(pg_dsn) as schema:
        with psycopg.connect(_with_search_path(pg_dsn, schema), autocommit=True) as conn:
            conn.execute(reference_ddl())
        dsn = _with_search_path(pg_dsn, schema)

        # dry run: table stays empty and the DSN is never needed to write
        assert main(["export-postgres", "--source", str(source), "--dsn", dsn, "--since", "2026-01-03"]) == 0
        assert _insert_dump(dsn, schema) == []

        # --days 3 => 2026-01-03 and 2026-01-05, never 2026-01-01
        argv = ["export-postgres", "--source", str(source), "--dsn", dsn, "--days", "3", "--apply"]
        assert main(argv) == 0
        rows = _insert_dump(dsn, schema)
        assert [row[1] for row in rows] == [date(2026, 1, 3), date(2026, 1, 5)]
        first = rows[0]
        assert first[2] == "fake" and first[3] == "sip"  # data source
        assert first[4] == "provisional"  # provisional vs final
        assert first[5] == datetime(2026, 1, 6, tzinfo=UTC)  # availability time
        assert first[6] == "fingerprint-1" and first[7] == datetime(2026, 1, 5, tzinfo=UTC)
        assert first[8] == Decimal("123.00000001")
        assert first[9] == ["currency:usd"]

        # replay is idempotent: same row count and same primary keys
        assert main(argv) == 0
        replay = _insert_dump(dsn, schema)
        assert [row[:2] for row in replay] == [row[:2] for row in rows]

        # a narrower window only touches rows inside it
        assert main(["export-postgres", "--source", str(source), "--dsn", dsn, "--since", "2026-01-05", "--apply"]) == 0
        assert [row[1] for row in _insert_dump(dsn, schema)] == [date(2026, 1, 3), date(2026, 1, 5)]
