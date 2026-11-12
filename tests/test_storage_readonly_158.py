import shutil
from pathlib import Path

import pytest

from openbars.core.models import DateRange, Interval, Market
from openbars.storage.errors import ReadOnlyError
from openbars.storage.parquet_duckdb import open_backend
from synth import raw_batch, weekdays
from datetime import date


def _files(root: Path) -> set[str]:
    return {p.relative_to(root).as_posix() for p in root.rglob("*")}


def test_read_only_opens_unwritable_tree_without_changes(tmp_path):
    root = tmp_path / "data"
    writer = open_backend(root)
    days = weekdays(date(2024, 1, 2), 2)
    writer.raw.append(raw_batch(["TEST1"], days))
    expected = writer.raw.scan_rows("prov_a", Market.US, Interval.D1, DateRange(days[0], days[-1]))
    writer.close()
    before = _files(root)
    paths = [root, *root.rglob("*")]
    modes = {p: p.stat().st_mode for p in paths}
    try:
        for p in paths:
            p.chmod(modes[p] & ~0o222)
        backend = open_backend(root, read_only=True, index_dir=tmp_path / "cache")
        actual = backend.raw.scan_rows("prov_a", Market.US, Interval.D1, DateRange(days[0], days[-1]))
        assert actual == expected
        assert _files(root) == before
        with pytest.raises(ReadOnlyError, match="read-only"):
            backend.raw.append(raw_batch(["TEST2"], days))
        assert _files(root) == before
    finally:
        for p, mode in modes.items():
            p.chmod(mode)


def test_catalog_signature_is_path_independent_and_tracks_changes(tmp_path):
    source = tmp_path / "source"
    first = open_backend(source)
    days = weekdays(date(2024, 1, 2), 2)
    first.raw.append(raw_batch(["TEST1"], days))
    signature = first._catalog_signature()
    other = tmp_path / "other"
    shutil.copytree(source, other, ignore=shutil.ignore_patterns("openbars.duckdb", ".lock"))
    second = open_backend(other)
    assert second._catalog_signature() == signature
    second.raw.append(raw_batch(["TEST2"], days))
    assert second._catalog_signature() != signature



def test_read_only_cache_reused_after_copy_and_reads_match(tmp_path, monkeypatch):
    from decimal import Decimal
    from openbars.core.models import Adjustment, CorporateAction, CorporateActionKind
    from synth import canonical_bars

    root = tmp_path / "source"
    writer = open_backend(root)
    days = weekdays(date(2024, 1, 2), 2)
    window = DateRange(days[0], days[-1])
    writer.raw.append(raw_batch(["TEST1"], days))
    writer.canonical.upsert(canonical_bars(["us:eq:T1"], days))
    writer.instruments.upsert_corporate_actions([
        CorporateAction("us:eq:T1", days[1], CorporateActionKind.SPLIT, "prov_a", ratio=Decimal("2"))
    ])
    sql = "SELECT instrument_id, close FROM canonical_bars ORDER BY session_date"
    expected_query = writer.query(sql).to_pylist()
    expected_raw = writer.raw.scan_rows("prov_a", Market.US, Interval.D1, window)
    expected_canonical = writer.canonical.scan_rows(["us:eq:T1"], Interval.D1, window)
    expected_adjusted = writer.reader.bars(["us:eq:T1"], adjustment=Adjustment.RAW).to_pylist()
    assert expected_query and expected_raw and expected_canonical and expected_adjusted
    # The stored bars are split-adjusted: undo the 2-for-1 split before its ex-date.
    assert [row["close"] for row in expected_adjusted] == [Decimal("23.50"), Decimal("12.00")]
    cache = tmp_path / "cache"
    first = open_backend(root, read_only=True, index_dir=cache)
    assert first.query(sql).to_pylist() == expected_query
    copied = tmp_path / "copied"
    shutil.copytree(root, copied)
    signature = first._catalog_signature()
    shutil.rmtree(root)
    second = open_backend(copied, read_only=True, index_dir=cache)
    assert second._catalog_signature() == signature
    assert second.catalog_path == first.catalog_path
    def fail_rebuild():
        pytest.fail("a matching cache must not be rebuilt")
    monkeypatch.setattr(second, "rebuild_catalog", fail_rebuild)
    assert second.query(sql).to_pylist() == expected_query
    assert second.raw.scan_rows("prov_a", Market.US, Interval.D1, window) == expected_raw
    assert second.canonical.scan_rows(["us:eq:T1"], Interval.D1, window) == expected_canonical
    assert second.reader.bars(["us:eq:T1"], adjustment=Adjustment.RAW).to_pylist() == expected_adjusted
