"""#127: storage maintenance CLI -- compact plan/apply, retention, usage, backup."""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, date, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest
from synth import canonical_bars, canonical_bars_h1, raw_batch, raw_batch_h1, weekdays

from openbars.cli.main import main
from openbars.cli.storage import run_storage
from openbars.config.loader import load_settings
from openbars.core.models import DateRange, Interval, Market
from openbars.storage.maintenance import compact_plan, retention_candidates, usage
from openbars.config.schema import RetentionRule
from openbars.storage.parquet_duckdb import open_backend

pytestmark = pytest.mark.usefixtures("no_network")

NOW = datetime(2026, 3, 10, 15, 0, tzinfo=UTC)
SECRET = "tkn-0123" + "456789abcdef"
OLD = weekdays(date(2024, 1, 2), 5)
NEW = weekdays(date(2026, 3, 2), 5)


def config(tmp_path, retention: str = "") -> str:
    path = tmp_path / "config.yaml"
    path.write_text(
        f"""
version: 1
markets: [us]
intervals: [1d, 1h]
history: {{start: 2026-01-02, end: null}}
universe:
  us:
    symbols: [TEST1]
storage:
  backend: parquet_duckdb
  path: {tmp_path / "data"}
{retention}
providers:
  massive:
    enabled: true
    api_key: ${{TEST_API_KEY}}
"""
    )
    return str(path)


def cli(tmp_path, *argv, retention: str = ""):
    return main(["--config", config(tmp_path, retention), *argv], clock=lambda: NOW, env={"TEST_API_KEY": SECRET})


def populate(tmp_path):
    backend = open_backend(tmp_path / "data")
    for i in range(3):  # three overlapping parts per daily raw partition
        backend.raw.append(raw_batch(["TEST1", "TEST2"], OLD, base=Decimal("11.25") + i, fetch_id=f"o{i}"))
        backend.raw.append(raw_batch(["TEST1", "TEST2"], NEW, base=Decimal("11.25") + i, fetch_id=f"n{i}"))
    backend.raw.append(raw_batch_h1(["TEST1"], NEW[:2]))
    backend.raw.append(raw_batch_h1(["TEST1"], NEW[:2], base=Decimal("20")))
    backend.canonical.upsert(canonical_bars(["us:eq:TEST1"], OLD))
    backend.canonical.upsert(canonical_bars(["us:eq:TEST1"], OLD, base=Decimal("12")))
    backend.canonical.upsert(canonical_bars_h1(["us:eq:TEST1"], NEW[:2]))
    return backend


def snapshot(root):
    return {p.relative_to(root).as_posix(): (p.stat().st_size, p.stat().st_mtime_ns) for p in root.rglob("*") if p.is_file()}


def reads(backend):
    out = {}
    for provider in ("prov_a",):
        for interval in (Interval.D1, Interval.H1):
            out[(provider, interval)] = backend.raw.scan_table(provider, Market.US, interval, DateRange(OLD[0], NEW[-1]))
    for interval in (Interval.D1, Interval.H1):
        out[("canonical", interval)] = backend.canonical.scan_rows(Market.US, interval, DateRange(OLD[0], NEW[-1]))
    return out


# --- compact ------------------------------------------------------------------------------


def test_compact_without_apply_changes_nothing(tmp_path, capsys):
    populate(tmp_path)
    root = tmp_path / "data"
    before = snapshot(root)
    assert cli(tmp_path, "compact") == 0
    out = capsys.readouterr().out
    assert snapshot(root) == before
    assert "raw/provider=prov_a/market=us/interval=1d/year=2024: 3 parts -> 1, 30 rows -> 10 visible" in out
    assert "canonical/market=us/interval=1d/year=2024: 2 parts -> 1" in out
    assert "dry run: nothing changed" in out
    assert str(tmp_path) not in out and SECRET not in out


def test_compact_apply_honours_min_parts_and_keeps_reads(tmp_path, capsys):
    backend = populate(tmp_path)
    expected = reads(backend)
    # min_parts=3: only the 3-part daily raw partitions qualify
    plan = compact_plan(tmp_path / "data", "all", min_parts=3)
    assert {c.partition for c in plan} == {
        "raw/provider=prov_a/market=us/interval=1d/year=2024",
        "raw/provider=prov_a/market=us/interval=1d/year=2026",
    }
    assert cli(tmp_path, "compact", "--min-parts", "3", "--apply") == 0
    assert "compacted 2 partitions" in capsys.readouterr().out
    parts = lambda rel: sorted((tmp_path / "data" / rel).rglob("part-*.parquet"))  # noqa: E731
    assert len(parts("raw/provider=prov_a/market=us/interval=1d/year=2024")) == 1
    assert len(parts("canonical/market=us/interval=1d/year=2024")) == 2  # below min_parts: untouched
    h1 = [p for p in parts("raw/provider=prov_a/market=us/interval=1h") ]
    assert len(h1) == 2
    assert reads(open_backend(tmp_path / "data")) == expected
    # then everything, layer by layer
    assert cli(tmp_path, "compact", "--layer", "canonical", "--apply") == 0
    assert len(parts("canonical/market=us/interval=1d/year=2024")) == 1
    assert len(parts("raw/provider=prov_a/market=us/interval=1h")) == 2  # raw not requested
    assert cli(tmp_path, "compact", "--apply") == 0
    assert len(parts("raw/provider=prov_a/market=us/interval=1h")) == 1
    assert reads(open_backend(tmp_path / "data")) == expected
    assert compact_plan(tmp_path / "data") == []


def test_compact_rejects_min_parts_below_two(tmp_path, capsys):
    populate(tmp_path)
    assert cli(tmp_path, "compact", "--min-parts", "1") == 2


def test_compact_dry_run_on_missing_dir_creates_nothing(tmp_path, capsys):
    assert cli(tmp_path, "compact") == 1
    assert not (tmp_path / "data").exists()


# --- retention ----------------------------------------------------------------------------


RAW_KEEP_1 = """  retention:
    - {layer: raw, keep_years: 1}
"""


def test_retention_is_off_by_default(tmp_path, capsys):
    populate(tmp_path)
    before = snapshot(tmp_path / "data")
    assert cli(tmp_path, "retention", "--apply") == 0
    assert "retention: off" in capsys.readouterr().out
    assert snapshot(tmp_path / "data") == before


@pytest.mark.parametrize("layer", ["canonical", "ref", "raw-archive", ""])
def test_retention_rejects_layers_other_than_raw(tmp_path, capsys, layer):
    rule = f"  retention:\n    - {{layer: {layer!r}, keep_years: 1}}\n"
    assert cli(tmp_path, "retention", retention=rule) == 2
    assert "always kept" in capsys.readouterr().err


@pytest.mark.parametrize("years", ["0", "-1", "true", "1.5", "'2'"])
def test_retention_rejects_bad_keep_years(tmp_path, capsys, years):
    rule = f"  retention:\n    - {{layer: raw, keep_years: {years}}}\n"
    assert cli(tmp_path, "config", retention=rule) == 2


def test_retention_lists_candidates_before_deleting(tmp_path, capsys):
    populate(tmp_path)
    root = tmp_path / "data"
    before = snapshot(root)
    assert cli(tmp_path, "retention", retention=RAW_KEEP_1) == 0
    out = capsys.readouterr().out
    assert "raw/provider=prov_a/market=us/interval=1d/year=2024: 3 files" in out
    assert "year=2026" not in out.split("candidates")[1]
    assert "dry run: nothing deleted" in out
    assert snapshot(root) == before
    assert cli(tmp_path, "retention", "--apply", retention=RAW_KEEP_1) == 0
    out = capsys.readouterr().out
    assert "candidates (1)" in out and "deleted 1 partitions" in out
    assert not (root / "raw/provider=prov_a/market=us/interval=1d/year=2024").exists()
    assert (root / "raw/provider=prov_a/market=us/interval=1d/year=2026").is_dir()
    assert (root / "raw/provider=prov_a/market=us/interval=1h").is_dir()
    # canonical and ref are kept, whatever their age
    assert list((root / "canonical/market=us/interval=1d/year=2024").rglob("part-*.parquet"))
    assert (root / "ref").is_dir()


def test_retention_rule_filters(tmp_path):
    populate(tmp_path)
    root = tmp_path / "data"
    assert retention_candidates(root, [RetentionRule("raw", 1, market="crypto")], now=NOW) == []
    assert retention_candidates(root, [RetentionRule("raw", 1, interval="1h")], now=NOW) == []
    assert retention_candidates(root, [RetentionRule("raw", 3)], now=NOW) == []  # 2024..2026 kept
    found = retention_candidates(root, [RetentionRule("raw", 1, provider="prov_a", interval="1d")], now=NOW)
    assert [c.partition for c in found] == ["raw/provider=prov_a/market=us/interval=1d/year=2024"]
    settings = load_settings(config(tmp_path, RAW_KEEP_1), env={"TEST_API_KEY": SECRET})
    assert settings.retention == (RetentionRule("raw", 1),)


# --- usage --------------------------------------------------------------------------------


def test_usage_splits_daily_and_hourly(tmp_path, capsys):
    populate(tmp_path)
    assert cli(tmp_path, "usage", "--json") == 0
    out = capsys.readouterr().out
    report = json.loads(out)
    by = {(r["layer"], r.get("interval")): r for r in report["by_layer_interval"]}
    assert by[("raw", "1d")]["files"] == 6 and by[("raw", "1h")]["files"] == 2
    assert by[("canonical", "1d")]["files"] == 2 and by[("canonical", "1h")]["files"] == 1
    assert all(r["bytes"] > 0 for r in report["by_layer_interval"])
    raw_years = {(p["provider"], p["interval"], p["year"]) for p in report["partitions"] if p["layer"] == "raw"}
    assert raw_years == {("prov_a", "1d", "2024"), ("prov_a", "1d", "2026"), ("prov_a", "1h", "2026")}
    assert report["total"]["files"] == sum(e.files for e in usage(tmp_path / "data"))
    assert str(tmp_path) not in out
    assert cli(tmp_path, "usage") == 0
    text = capsys.readouterr().out
    assert "raw              1h" in text and "canonical        1d" in text and str(tmp_path) not in text


# --- backup -------------------------------------------------------------------------------


def test_backup_copies_data_without_lock_catalog_or_secrets(tmp_path, capsys):
    backend = populate(tmp_path)
    expected = reads(backend)
    root = tmp_path / "data"
    (root / ".env").write_text(f"TEST_API_KEY={SECRET}\n")  # stray dotfile must not be copied
    con = sqlite3.connect(root / "state.db")
    con.execute("CREATE TABLE IF NOT EXISTS t (x)")
    con.execute("INSERT INTO t VALUES (1)")
    con.commit()
    con.close()
    dest = tmp_path / "bk"
    assert cli(tmp_path, "backup", str(dest)) == 0
    out = capsys.readouterr().out
    assert "backup:" in out and SECRET not in out
    assert not (dest / ".lock").exists() and not (dest / ".env").exists()
    assert not (dest / "openbars.duckdb").exists()
    for path in dest.rglob("*"):
        if path.is_file():
            assert SECRET.encode() not in path.read_bytes()
    manifest = json.loads((dest / "backup-manifest.json").read_text())
    assert manifest["schema_version"] == 1 and all(not f["path"].startswith("/") for f in manifest["files"])
    assert str(tmp_path) not in (dest / "backup-manifest.json").read_text()
    assert sqlite3.connect(dest / "state.db").execute("SELECT x FROM t").fetchall() == [(1,)]
    assert reads(open_backend(dest)) == expected  # restore = point storage.path at the copy


def test_backup_filters_and_refuses_bad_destinations(tmp_path, capsys):
    populate(tmp_path)
    root = tmp_path / "data"
    dest = tmp_path / "bk"
    assert cli(tmp_path, "backup", str(dest), "--interval", "1h") == 0
    copied = {p.relative_to(dest).as_posix() for p in dest.rglob("part-*.parquet")}
    assert copied and all("interval=1h" in p for p in copied)
    assert (dest / "ref").is_dir() or not (root / "ref").exists() or not any((root / "ref").iterdir())
    assert cli(tmp_path, "backup", str(dest)) == 1  # not empty
    assert "not an empty directory" in capsys.readouterr().err
    assert cli(tmp_path, "backup", str(root / "inside")) == 1
    assert cli(tmp_path, "backup", str(tmp_path)) == 1  # would contain the data dir
    assert not (root / "inside").exists()


def test_maintenance_commands_need_the_parquet_backend(capsys):
    settings = SimpleNamespace(storage_backend="postgres")
    args = SimpleNamespace(command="usage", data=None)
    assert run_storage(args, settings, SimpleNamespace(err=print, line=print), lambda: NOW) == 2
    assert "parquet_duckdb backend only" in capsys.readouterr().out
