from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from openbars.cli.main import main


def _source(path):
    path.mkdir(parents=True)
    schema = pa.schema([
        ("instrument_id", pa.string()), ("market", pa.string()), ("interval", pa.string()),
        ("ts_start", pa.timestamp("us", tz="UTC")), ("session_date", pa.date32()),
        ("session", pa.string()), ("policy_version", pa.string()), ("provider", pa.string()),
        ("feed", pa.string()), ("adjustment", pa.string()), ("revision", pa.int32()),
        ("open", pa.decimal128(20, 8)), ("high", pa.decimal128(20, 8)),
        ("low", pa.decimal128(20, 8)), ("close", pa.decimal128(20, 8)),
        ("volume", pa.decimal128(28, 8)), ("vwap", pa.decimal128(20, 8)),
        ("trade_count", pa.int64()), ("selection_reason", pa.string()),
        ("quality_status", pa.string()), ("capabilities", pa.list_(pa.string())),
        ("source_fingerprint", pa.string()), ("price_source", pa.string()),
        ("volume_source", pa.string()), ("sources_seen", pa.list_(pa.string())),
        ("reconcile_rule", pa.string()), ("generated_at", pa.timestamp("us", tz="UTC")),
        ("available_at", pa.timestamp("us", tz="UTC")), ("freshness", pa.string()),
    ])
    rows = []
    for day, close in [(date(2026, 1, 1), "10.12345678"), (date(2026, 1, 3), "11.00000001"), (date(2026, 1, 5), "12.00000002")]:
        rows.append({"instrument_id":"local:1", "market":"us", "interval":"1d",
            "ts_start":datetime.combine(day, datetime.min.time(), UTC), "session_date":day,
            "session":"regular", "policy_version":"research-daily-v2", "provider":"fake",
            "feed":"sip", "adjustment":"raw", "revision":0, "open":Decimal(close),
            "high":Decimal(close), "low":Decimal(close), "close":Decimal(close),
            "volume":Decimal("123.00000001"), "vwap":None, "trade_count":10,
            "selection_reason":"primary_source", "quality_status":"complete", "capabilities":[],
            "source_fingerprint":"f", "price_source":"fake", "volume_source":"fake",
            "sources_seen":["fake"], "reconcile_rule":"test", "generated_at":datetime(2026,1,5,tzinfo=UTC),
            "available_at":None, "freshness":"final"})
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
    def __init__(self): self.statements=[]; self.commits=0
    def cursor(self): return FakeCursor(self)
    def commit(self): self.commits += 1
    def rollback(self): pass
    def close(self): pass


def test_export_postgres_dry_run_filters_and_never_connects(tmp_path, monkeypatch, capsys):
    source = tmp_path / "source"; _source(source)
    mapping = tmp_path / "ids.csv"; mapping.write_text("instrument_id,upstream_id\nlocal:1,42\n")
    import openbars.export_postgres as exporter
    monkeypatch.setattr(exporter, "_connect", lambda dsn: (_ for _ in ()).throw(AssertionError("dry-run connected")))
    assert main(["export-postgres", "--source", str(source), "--dsn", "postgresql://unused", "--table", "daily_bar",
        "--mapping", str(mapping), "--since", "2026-01-03"]) == 0
    out = capsys.readouterr().out
    assert "2 rows" in out and "dry-run" in out


def test_export_postgres_apply_upserts_decimal_rows_and_uses_integer_ids(tmp_path, monkeypatch):
    source = tmp_path / "source"; _source(source)
    mapping = tmp_path / "ids.csv"; mapping.write_text("instrument_id,upstream_id\nlocal:1,42\n")
    import openbars.export_postgres as exporter
    conn = FakeConnection()
    monkeypatch.setattr(exporter, "_connect", lambda dsn: conn)
    argv = ["export-postgres", "--source", str(source), "--dsn", "postgresql://unused", "--table", "research_daily_bar",
        "--mapping", str(mapping), "--days", "3", "--apply"]
    assert main(argv) == 0
    first = list(conn.statements)
    assert len([sql for sql, _ in first if sql.lstrip().upper().startswith("INSERT")]) == 2
    inserts = [(sql, params) for sql, params in first if sql.lstrip().upper().startswith("INSERT")]
    assert all(params[0] == 42 and isinstance(params[8], Decimal) for _, params in inserts)
    assert all("ON CONFLICT" in sql.upper() for sql, _ in inserts)
    assert not any("instrument" in sql.lower() and not "instrument_id" in sql.lower() for sql, _ in first)
    assert conn.commits == 1
    assert main(argv) == 0
    second_inserts = [x for x in conn.statements[len(first):] if x[0].lstrip().upper().startswith("INSERT")]
    assert len(second_inserts) == len(inserts)  # replay uses the same conflict keys


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
    monkeypatch.setattr(exporter, "_rows", lambda *args: [])
    result = main(["export-postgres", "--source", "unused", "--dsn", "postgresql://unused",
        "--table", "daily_bar", "--mapping", "unused.csv", "--apply"])
    assert result == 1
    assert "openbars[postgres-export]" in capsys.readouterr().err
