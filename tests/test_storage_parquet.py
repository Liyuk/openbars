import dataclasses
import subprocess
import sys
import textwrap
import time
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from synth import canonical_bars, raw_batch, utc, weekdays

from openbars.core.atomic import atomic_write
from openbars.core.models import (
    AssetClass,
    CorporateAction,
    CorporateActionKind,
    DateRange,
    Instrument,
    Interval,
    Market,
    QualityFlag,
    SymbolMapping,
)
from openbars.storage.parquet_duckdb import CANONICAL_LAYER, RAW_LAYER, open_backend
from openbars.storage.rows import CanonicalBarRow, InvalidBarError, RawBarRow
from openbars.storage.schema import (
    CANONICAL_BARS_SCHEMA,
    METADATA_VERSION_KEY,
    RAW_BARS_SCHEMA,
    SCHEMA_VERSION,
)

DAYS = weekdays(date(2024, 1, 2), 5)
WINDOW = DateRange(DAYS[0], DAYS[-1])


@pytest.fixture
def backend(tmp_path):
    return open_backend(tmp_path / "data")


def raw_parts(backend):
    return sorted((backend.root / "raw").rglob("part-*.parquet"))


# --- schema ---


def test_schema_decimal_types_and_version():
    for name in ("open", "high", "low", "close", "vwap"):
        assert RAW_BARS_SCHEMA.field(name).type == pa.decimal128(20, 8)
        assert CANONICAL_BARS_SCHEMA.field(name).type == pa.decimal128(20, 8)
    assert RAW_BARS_SCHEMA.field("volume").type == pa.decimal128(28, 8)
    assert CANONICAL_BARS_SCHEMA.field("volume").type == pa.decimal128(28, 8)
    assert SCHEMA_VERSION == 1
    assert RAW_BARS_SCHEMA.field("instrument_id").nullable
    assert RAW_BARS_SCHEMA.field("volume_basis").type == pa.string()


# --- raw ---


def test_raw_roundtrip_is_exact_decimal(backend):
    batch = raw_batch(["TEST1", "TEST2"], DAYS)
    assert backend.raw.append(batch) == 10
    rows = backend.raw.scan_rows("prov_a", Market.US, Interval.D1, WINDOW)
    assert len(rows) == 10
    first = rows[0]
    assert first.symbol_as_returned == "TEST1"
    assert first.open == Decimal("11.25") and isinstance(first.open, Decimal)
    assert first.volume == Decimal("1000000.5")
    assert first.volume_basis == "unknown"  # default when not supplied
    assert first.instrument_id is None
    assert first.feed == "feed1" and first.currency == "USD"
    assert first.ts_start == utc(DAYS[0])
    table = backend.raw.scan_table("prov_a", Market.US, Interval.D1, WINDOW)
    assert table.schema.field("open").type == pa.decimal128(20, 8)
    assert table.schema.field("volume").type == pa.decimal128(28, 8)


def test_raw_protocol_scan_returns_legacy_models_with_exact_extras(backend):
    backend.raw.append(raw_batch(["TEST1"], DAYS))
    bars = list(backend.raw.scan("prov_a", Market.US, Interval.D1, WINDOW))
    assert [b.open for b in bars][:2] == [Decimal("11.25"), Decimal("11.50")]
    assert bars[0].extra["open_exact"] == Decimal("11.25")
    assert bars[0].feed == "feed1"


def test_floats_are_never_expanded_in_binary():
    row = RawBarRow(
        provider="p", market=Market.US, symbol_as_returned="TEST1", interval=Interval.D1,
        ts_start=utc(DAYS[0]), session_date=DAYS[0], open=Decimal("0.1"), high=Decimal("0.3"),
        low=Decimal("0.1"), close=Decimal("0.2"), fetch_id="f",
    )
    assert row.open == Decimal("0.10000000")
    assert row.high == Decimal("0.30000000")  # shortest repr then 8dp, not 0.3000000000000000444


def test_exact_decimal_inputs_are_not_silently_rounded():
    kwargs = dict(
        provider="p", market=Market.US, symbol_as_returned="TEST1", interval=Interval.D1,
        ts_start=utc(DAYS[0]), session_date=DAYS[0], fetch_id="f", high=2, low=1, close=1,
    )
    with pytest.raises(InvalidBarError, match="fractional"):
        RawBarRow(open=Decimal("1.123456789"), **kwargs)
    with pytest.raises(InvalidBarError):
        RawBarRow(open=Decimal("1" + "0" * 13), **kwargs)  # exceeds decimal(20,8)
    ok = RawBarRow(open=Decimal("1.12345678"), **kwargs)
    assert ok.open == Decimal("1.12345678")


def test_invalid_bar_rejected_and_naive_ts_rejected(backend):
    batch = raw_batch(["TEST1"], DAYS[:1])
    # Model constructors validate OHLCV and timezone themselves.
    with pytest.raises(ValueError, match="high"):
        dataclasses.replace(
            batch.bars[0], high=Decimal("5"), low=Decimal("9"), open=Decimal("7"), close=Decimal("7")
        )
    with pytest.raises(ValueError, match="timezone-aware"):
        dataclasses.replace(batch.bars[0], ts_start=datetime(2024, 1, 2))
    # Storage rows collect OHLCV errors; naive timestamps are rejected immediately.
    row = RawBarRow(
        provider="prov_a", market=Market.US, symbol_as_returned="TEST1", interval=Interval.D1,
        ts_start=utc(DAYS[0]), session_date=DAYS[0], fetch_id="f",
        open=Decimal("7"), high=Decimal("5"), low=Decimal("9"), close=Decimal("7"),
    )
    assert "high < low" in row.validation_errors()
    with pytest.raises(InvalidBarError, match="naive"):
        RawBarRow(
            provider="prov_a", market=Market.US, symbol_as_returned="TEST1", interval=Interval.D1,
            ts_start=datetime(2024, 1, 2), session_date=DAYS[0], fetch_id="f",
            open=Decimal("7"), high=Decimal("8"), low=Decimal("6"), close=Decimal("7"),
        )
    assert raw_parts(backend) == []


def test_duplicate_append_does_not_duplicate_or_write(backend):
    batch = raw_batch(["TEST1", "TEST2"], DAYS)
    assert backend.raw.append(batch) == 10
    assert backend.raw.append(batch) == 0  # identical content -> no new part
    again = raw_batch(["TEST1", "TEST2"], DAYS, fetch_id="fetch-2")  # only lineage differs
    assert backend.raw.append(again) == 0
    assert len(raw_parts(backend)) == 1
    rows = backend.raw.scan_rows("prov_a", Market.US, Interval.D1, WINDOW)
    assert len(rows) == 10
    count = backend.query("SELECT count(*) AS n FROM raw_bars").column("n")[0].as_py()
    assert count == 10


def test_same_key_last_write_wins_and_new_revision_is_new_row(backend):
    backend.raw.append(raw_batch(["TEST1"], DAYS, base=Decimal("11.25")))
    backend.raw.append(raw_batch(["TEST1"], DAYS, base=Decimal("20"), fetch_id="fetch-2"))
    rows = backend.raw.scan_rows("prov_a", Market.US, Interval.D1, WINDOW)
    assert len(rows) == 5 and rows[0].open == Decimal("20") and rows[0].fetch_id == "fetch-2"
    assert len(raw_parts(backend)) == 2
    assert backend.query("SELECT count(*) AS n FROM raw_bars").column("n")[0].as_py() == 5
    # a different revision is a different identity
    rev = raw_batch(["TEST1"], DAYS[:1], base=30.0, fetch_id="fetch-3")
    rev = dataclasses.replace(
        rev, bars=tuple(dataclasses.replace(b, extra={**b.extra, "revision": 1}) for b in rev.bars)
    )
    backend.raw.append(rev)
    assert len(backend.raw.scan_rows("prov_a", Market.US, Interval.D1, WINDOW)) == 6


def test_raw_identity_prefers_instrument_id(backend):
    batch = raw_batch(["TEST1"], DAYS[:1])
    with_id = dataclasses.replace(
        batch, bars=tuple(dataclasses.replace(b, extra={**b.extra, "instrument_id": "us:eq:T1"}) for b in batch.bars)
    )
    backend.raw.append(batch)
    backend.raw.append(with_id)  # different entity key (instrument id vs symbol)
    rows = backend.raw.scan_rows("prov_a", Market.US, Interval.D1, WINDOW)
    assert sorted(r.instrument_id or "" for r in rows) == ["", "us:eq:T1"]


def test_partition_layout_and_zstd_and_metadata(backend):
    backend.raw.append(raw_batch(["TEST1"], DAYS))
    (part,) = raw_parts(backend)
    rel = part.relative_to(backend.root).parts
    assert rel[:5] == ("raw", "provider=prov_a", "market=us", "interval=1d", "year=2024")
    meta = pq.ParquetFile(part).metadata
    assert meta.row_group(0).column(0).compression == "ZSTD"
    assert pq.read_schema(part).metadata[METADATA_VERSION_KEY] == b"1"


def test_intraday_partitions_by_month(backend):
    batch = raw_batch(["TEST1"], DAYS[:1])
    intraday = dataclasses.replace(
        batch,
        request=dataclasses.replace(batch.request, interval=Interval.M5),
        bars=tuple(dataclasses.replace(b, interval=Interval.M5) for b in batch.bars),
    )
    backend.raw.append(intraday)
    (part,) = raw_parts(backend)
    assert "year=2024" in part.parts and "month=01" in part.parts
    assert len(backend.raw.scan_rows("prov_a", Market.US, Interval.M5, WINDOW)) == 1


# --- atomic write / interruption ---


def test_failed_replace_leaves_existing_data_and_no_tmp(backend, monkeypatch):
    backend.raw.append(raw_batch(["TEST1"], DAYS))
    before = raw_parts(backend)
    import openbars.core.atomic as atomic

    def boom(*a, **k):
        raise OSError("simulated crash before rename")

    monkeypatch.setattr(atomic.os, "replace", boom)
    with pytest.raises(OSError):
        backend.raw.append(raw_batch(["TEST1"], DAYS, base=50.0))
    monkeypatch.undo()
    assert raw_parts(backend) == before
    assert list(backend.root.rglob("*.tmp")) == []
    rows = backend.raw.scan_rows("prov_a", Market.US, Interval.D1, WINDOW)
    assert rows[0].open == Decimal("11.25")


def test_killed_writer_leaves_only_ignored_tmp(backend, tmp_path):
    backend.raw.append(raw_batch(["TEST1"], DAYS))
    (part,) = raw_parts(backend)
    # a real process that dies mid-write (no finally blocks run)
    script = textwrap.dedent(
        f"""
        import os, sys
        from pathlib import Path
        from openbars.core.atomic import atomic_write
        target = Path({str(part.parent / "part-9999999999999999999-dead.parquet")!r})
        def writer(fh):
            fh.write(b"PAR1 truncated garbage")
            fh.flush()
            os._exit(9)
        atomic_write(target, writer)
        """
    )
    proc = subprocess.run([sys.executable, "-c", script], capture_output=True)
    assert proc.returncode == 9
    leftovers = list(backend.root.rglob(".*.tmp"))
    assert len(leftovers) == 1
    assert not (part.parent / "part-9999999999999999999-dead.parquet").exists()
    # readers / catalog only see complete parquet files
    assert len(backend.raw.scan_rows("prov_a", Market.US, Interval.D1, WINDOW)) == 5
    backend.rebuild_catalog()
    assert backend.query("SELECT count(*) AS n FROM raw_bars").column("n")[0].as_py() == 5
    # compaction sweeps stale tmp files
    backend.compact(stale_tmp_seconds=0)
    assert list(backend.root.rglob(".*.tmp")) == []


def test_atomic_write_cleans_up_on_writer_error(tmp_path):
    target = tmp_path / "x.bin"

    def writer(fh):
        fh.write(b"partial")
        raise RuntimeError("nope")

    with pytest.raises(RuntimeError):
        atomic_write(target, writer)
    assert list(tmp_path.iterdir()) == []


# --- compaction ---


def test_compaction_preserves_rows_and_keys(backend):
    for i in range(4):  # 4 parts, overlapping keys: later parts overwrite day 0..i
        backend.raw.append(raw_batch(["TEST1", "TEST2"], DAYS, base=Decimal("11.25") + i, fetch_id=f"f{i}"))
    assert len(raw_parts(backend)) == 4
    before = backend.raw.scan_table("prov_a", Market.US, Interval.D1, WINDOW)
    results = backend.compact("raw")
    assert len(results) == 1
    res = results[0]
    assert (res.parts_before, res.rows_before, res.rows_after) == (4, 40, 10)
    assert len(raw_parts(backend)) == 1
    after = backend.raw.scan_table("prov_a", Market.US, Interval.D1, WINDOW)
    assert after.equals(before)
    # new data written after compaction still wins over the compacted part
    backend.raw.append(raw_batch(["TEST1"], DAYS[:1], base=99.0, fetch_id="late"))
    rows = backend.raw.scan_rows("prov_a", Market.US, Interval.D1, WINDOW)
    assert len(rows) == 10 and rows[0].open == Decimal("99")
    assert backend.compact("raw", min_parts=2)[0].rows_after == 10
    assert backend.compact("raw") == []  # single part: nothing to do


def test_crash_between_compact_write_and_delete_is_consistent(backend):
    for i in range(3):
        backend.raw.append(raw_batch(["TEST1"], DAYS, base=Decimal("11.25") + i, fetch_id=f"f{i}"))
    files = raw_parts(backend)
    expected = backend.raw.scan_table("prov_a", Market.US, Interval.D1, WINDOW)
    # emulate "compacted part written, inputs not yet deleted"
    table = backend.raw._table.query(files)
    backend.raw._table.write_part(files[0].parent, table, floor_ts=int(files[-1].name.split("-")[1]) - 1, suffix="c")
    assert len(raw_parts(backend)) == 4
    assert backend.raw.scan_table("prov_a", Market.US, Interval.D1, WINDOW).equals(expected)
    assert backend.compact("raw")[0].rows_after == 5


# --- catalog / reader ---


def test_rebuild_catalog_and_year_query_whole_market(backend, capsys):
    days = weekdays(date(2024, 1, 2), 252)
    symbols = [f"TEST{i}" for i in range(20)]
    ids = [f"us:eq:T{i}" for i in range(20)]
    backend.raw.append(raw_batch(symbols, days))
    backend.canonical.upsert(canonical_bars(ids, days))
    assert backend.canonical.upsert(canonical_bars(ids, days)) == 0
    backend.rebuild_catalog()
    assert backend.catalog_path.exists()

    start = time.perf_counter()
    table = backend.reader.bars(
        interval=Interval.D1, start=days[0], end=days[-1], market=Market.US
    )
    elapsed = time.perf_counter() - start
    print(f"whole-market year query: {table.num_rows} rows in {elapsed * 1000:.1f} ms")
    assert table.num_rows == 252 * 20
    assert table.schema.field("close").type == pa.decimal128(20, 8)
    assert backend.query("SELECT count(*) AS n FROM raw_bars").column("n")[0].as_py() == 252 * 20
    one = backend.reader.bars(["us:eq:T3"], start=days[0], end=days[9])
    assert one.num_rows == 10

    # views are disposable: drop the catalog file and everything still works
    backend.catalog_path.unlink()
    assert backend.query("SELECT count(*) AS n FROM canonical_bars").column("n")[0].as_py() == 252 * 20
    con = duckdb.connect(str(backend.catalog_path))
    con.execute("DROP VIEW canonical_bars")
    con.close()
    backend.rebuild_catalog()
    assert backend.query("SELECT count(*) AS n FROM canonical_bars").column("n")[0].as_py() == 252 * 20


def test_catalog_on_empty_tree_and_picks_up_first_files(backend):
    assert backend.query("SELECT count(*) AS n FROM raw_bars").column("n")[0].as_py() == 0
    assert backend.query("SELECT count(*) AS n FROM instruments").column("n")[0].as_py() == 0
    backend.raw.append(raw_batch(["TEST1"], DAYS))
    assert backend.query("SELECT count(*) AS n FROM raw_bars").column("n")[0].as_py() == 5


def test_reader_resolves_symbols_and_filters(backend):
    inst = Instrument("us:eq:T1", Market.US, AssetClass.EQUITY, "USD", name="Test One")
    backend.instruments.upsert([inst], [SymbolMapping("us:eq:T1", "TEST1", date(2020, 1, 1))])
    backend.canonical.upsert(canonical_bars(["us:eq:T1", "us:eq:T2"], DAYS))
    t = backend.reader.bars(["TEST1"], start=DAYS[0], end=DAYS[-1])
    assert set(t.column("instrument_id").to_pylist()) == {"us:eq:T1"}
    assert backend.reader.bars(["us:eq:T2"]).num_rows == 5
    assert backend.reader.bars(interval=Interval.D1, adjustment=None).num_rows == 10
    from openbars.core.models import Adjustment

    # RAW is now derived at query time from the stored split-adjusted bars (no splits recorded
    # here, so the prices equal the stored ones); before normalize.adjust it filtered the
    # stored column and returned nothing.
    raw = backend.reader.bars(adjustment=Adjustment.RAW)
    assert raw.num_rows == 10 and set(raw.column("adjustment").to_pylist()) == {"raw"}
    assert backend.reader.bars(as_of=date(2000, 1, 1)).num_rows == 0
    assert backend.reader.bars(policy_version="other-policy").num_rows == 0


# --- canonical ---


def test_canonical_roundtrip_flags_and_decimals(backend):
    bars = canonical_bars(["us:eq:T1"], DAYS, flags=QualityFlag.DIVERGENT | QualityFlag.SINGLE_SOURCE)
    assert backend.canonical.upsert(bars, capabilities=["price_basis:split_adjusted"], selection_reason="primary_source") == 5
    rows = backend.canonical.scan_rows(["us:eq:T1"], Interval.D1, WINDOW)
    assert len(rows) == 5
    r = rows[0]
    assert r.open == Decimal("11.25") and r.volume == Decimal("1000000.5")
    assert r.flags == ("DIVERGENT", "SINGLE_SOURCE")
    assert r.quality_status == "divergent"
    assert r.policy_version == "research-daily-v2"
    assert r.capabilities == ("price_basis:split_adjusted",)
    assert r.sources_seen == ("prov_a", "prov_b") and r.price_source == "prov_a"
    assert r.reconcile_rule == "primary_wins" and r.selection_reason == "primary_source"
    assert len(r.source_fingerprint) == 64
    legacy = list(backend.canonical.scan(["us:eq:T1"], Interval.D1, WINDOW))
    assert legacy[0].bar.flags == QualityFlag.DIVERGENT | QualityFlag.SINGLE_SOURCE
    assert legacy[0].bar.open == Decimal("11.25") and legacy[0].revision == 1
    assert legacy[0].sources_seen == ("prov_a", "prov_b")


def test_canonical_duplicate_upsert_and_update(backend):
    bars = canonical_bars(["us:eq:T1", "us:eq:T2"], DAYS)
    assert backend.canonical.upsert(bars) == 10
    assert backend.canonical.upsert(bars) == 0
    assert len(list((backend.root / "canonical").rglob("part-*.parquet"))) == 1
    changed = canonical_bars(["us:eq:T1"], DAYS, base=40.0)
    assert backend.canonical.upsert(changed) == 5
    rows = backend.canonical.scan_rows(None, Interval.D1, WINDOW, market=Market.US)
    assert len(rows) == 10
    t1 = [r for r in rows if r.instrument_id == "us:eq:T1"]
    assert t1[0].open == Decimal("40")
    # the same bar under another policy is a separate row
    backend.canonical.upsert_rows([dataclasses.replace(rows[0], policy_version="openbars-daily-v1")])
    assert len(backend.canonical.scan_rows(None, Interval.D1, WINDOW, policy_version=None)) == 11
    assert len(backend.canonical.scan_rows(None, Interval.D1, WINDOW)) == 10


def test_canonical_market_resolution(backend):
    bars = canonical_bars(["opaque-id"], DAYS[:1])
    from openbars.storage.errors import StorageError

    with pytest.raises(StorageError, match="market"):
        backend.canonical.upsert(bars)
    backend.instruments.upsert([Instrument("opaque-id", Market.HK, AssetClass.EQUITY, "HKD")], [])
    backend.canonical.upsert(bars)
    assert (backend.root / "canonical" / "market=hk").exists()


def test_canonical_rejects_invalid_ohlc(backend):
    (bar,) = canonical_bars(["us:eq:T1"], DAYS[:1])
    row = CanonicalBarRow.from_canonical(bar, market=Market.US)
    bad = dataclasses.replace(row, high=row.low - 1)
    with pytest.raises(InvalidBarError):
        backend.canonical.upsert_rows([bad])


# --- instrument master ---


def test_instrument_resolve_across_symbol_mapping_window(backend):
    a = Instrument("us:eq:A", Market.US, AssetClass.EQUITY, "USD", name="Alpha", external_ids={"figi": "TESTFIGI1"})
    b = Instrument("us:eq:B", Market.US, AssetClass.EQUITY, "USD")
    other = Instrument("hk:eq:C", Market.HK, AssetClass.EQUITY, "HKD")
    maps = [
        SymbolMapping("us:eq:A", "ZZZA", date(2015, 1, 1), date(2019, 12, 31)),
        SymbolMapping("us:eq:B", "ZZZA", date(2020, 1, 1), None),  # ticker reused
        SymbolMapping("hk:eq:C", "ZZZA", date(2015, 1, 1)),
        SymbolMapping("us:eq:A", "ZZZA", date(2021, 1, 1), None, provider="prov_x"),
    ]
    backend.instruments.upsert([a, b, other], maps)
    r = lambda on, provider="prov_a", market=Market.US: backend.instruments.resolve(  # noqa: E731
        "ZZZA", on=on, provider=provider, market=market
    )
    assert r(date(2014, 12, 31)) is None
    assert r(date(2015, 1, 1)) == "us:eq:A"
    assert r(date(2019, 12, 31)) == "us:eq:A"  # valid_to inclusive
    assert r(date(2020, 1, 1)) == "us:eq:B"
    assert r(date(2024, 6, 1)) == "us:eq:B"
    assert r(date(2024, 6, 1), provider="prov_x") == "us:eq:A"  # provider-specific wins
    assert r(date(2024, 6, 1), market=Market.HK) == "hk:eq:C"
    assert r(date(2024, 6, 1), market=Market.CRYPTO) is None
    assert backend.instruments.resolve("NOPE", on=date(2024, 1, 1), provider="prov_a", market=Market.US) is None
    # ambiguity -> None
    backend.instruments.upsert([], [SymbolMapping("us:eq:A", "ZZZA", date(2020, 6, 1), None)])
    assert r(date(2024, 6, 1)) is None
    # persisted & upsert is idempotent / merges
    backend.instruments.upsert([dataclasses.replace(a, name="Alpha 2")], [])
    again = open_backend(backend.root)
    assert again.instruments.get("us:eq:A").name == "Alpha 2"
    assert again.instruments.get("us:eq:A").external_ids == {"figi": "TESTFIGI1"}
    assert len(again.instruments.instruments()) == 3
    assert len(again.instruments.mappings()) == 5
    assert again.instruments.resolve("ZZZA", on=date(2019, 1, 1), provider="p", market=Market.US) == "us:eq:A"


def test_corporate_actions_roundtrip(backend):
    acts = [
        CorporateAction("us:eq:A", date(2022, 6, 1), CorporateActionKind.SPLIT, "prov_a", ratio=Decimal("4")),
        CorporateAction("us:eq:A", date(2022, 7, 1), CorporateActionKind.DIVIDEND, "prov_a", amount=Decimal("0.125")),
    ]
    backend.instruments.upsert_corporate_actions(acts)
    backend.instruments.upsert_corporate_actions(acts)
    got = backend.instruments.corporate_actions(["us:eq:A"])
    assert [(a.kind, a.ratio, a.amount) for a in got] == [
        (CorporateActionKind.SPLIT, Decimal("4"), None),
        (CorporateActionKind.DIVIDEND, None, Decimal("0.125")),
    ]
    table = backend.instruments.corporate_actions_table()
    assert table.column("ratio").to_pylist()[0] == Decimal("4")
    assert backend.instruments.corporate_actions(["nope"]) == []
    assert backend.query("SELECT count(*) AS n FROM corporate_actions").column("n")[0].as_py() == 2


# --- state & archive via the backend ---


def test_backend_wires_state_and_archive(backend):
    assert (backend.root / "state.db").exists()
    from openbars.orchestrator.jobs import Checkpoint

    backend.state.save_checkpoint(Checkpoint("j1", "cursor-1", datetime(2024, 1, 2, tzinfo=UTC)))
    assert open_backend(backend.root).state.load_checkpoint("j1").cursor == "cursor-1"
    res = backend.archive.write(
        provider="prov_a", market=Market.US, interval=Interval.D1, window=WINDOW,
        pages=['{"results": []}'], license_tag="personal_only",
    )
    assert (backend.root / "raw-archive" / res.ref).exists()
    batch = raw_batch(["TEST1"], DAYS)
    backend.raw.append(batch, source_artifact_ref=res.ref, ingest_run_id="run-1")
    row = backend.raw.scan_rows("prov_a", Market.US, Interval.D1, WINDOW)[0]
    assert row.source_artifact_ref == res.ref and row.ingest_run_id == "run-1"
