"""Parquet raw layer keeps ``RawBar.extra`` leftovers (tradestatus / isST / preclose, #94).

Synthetic BaoStock-shaped bars, tmp_path only. The ``extra`` column is Parquet-only and optional:
files written before it existed (no such column) must keep reading, with ``extra`` empty.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import date
from decimal import Decimal as D

import pyarrow.parquet as pq
import pytest
from cn_fakes import PRICES, WEEK, CnFake

from openbars.core.models import DateRange, Interval, Market
from openbars.normalize.cn_quality import suspended_dates
from openbars.providers.base import FetchRequest, RawBatch
from openbars.storage.parquet_duckdb import RAW_LAYER, open_backend
from openbars.storage.rows import (
    decode_extra,
    encode_extra,
    raw_rows_from_batch,
    raw_rows_to_table,
)
from openbars.storage.schema import (
    RAW_BARS_PARQUET_SCHEMA,
    RAW_BARS_SCHEMA,
    SCHEMA_VERSION,
    TABLE_SCHEMAS,
)

WINDOW = DateRange(WEEK[0], WEEK[-1])
HALTED = date(2026, 3, 4)


def week_batch(provider: CnFake | None = None) -> RawBatch:
    return (provider or CnFake()).fetch_bars(FetchRequest(Market.CN, Interval.D1, ("sh.600000",), WINDOW))


def scan(backend):
    return {b.session_date: b for b in backend.raw.scan("baostock", Market.CN, Interval.D1, WINDOW)}


@pytest.fixture
def backend(tmp_path):
    return open_backend(tmp_path / "data")


def test_schema_is_additive_and_parquet_only():
    assert SCHEMA_VERSION == 1
    assert "extra" not in RAW_BARS_SCHEMA.names and "extra" not in TABLE_SCHEMAS["raw_bars"].names
    assert RAW_BARS_PARQUET_SCHEMA.names == [*RAW_BARS_SCHEMA.names, "extra"]
    assert RAW_BARS_PARQUET_SCHEMA.field("extra").nullable
    assert RAW_LAYER.schema is RAW_BARS_PARQUET_SCHEMA


def test_tradestatus_isst_preclose_survive_the_parquet_round_trip(backend):
    backend.raw.append(week_batch())
    bars = scan(backend)
    assert sorted(bars) == WEEK
    for day, bar in bars.items():
        *_, preclose, status = PRICES[day]
        assert bar.extra["tradestatus"] == status and bar.extra["isST"] == 0
        assert bar.extra["preclose"] == preclose and bar.extra["code"] == "sh.600000"
        assert bar.extra["market"] == "cn"  # first-class columns are still merged in
    assert suspended_dates(bars.values()) == {HALTED}


def test_tradestatus_beats_the_zero_volume_heuristic(backend):
    traded = CnFake().bar("sh.600000", WEEK[0], "x")
    quiet = replace(traded, volume=D(0), extra={**traded.extra, "tradestatus": 1})  # traded, no volume
    halted = replace(CnFake().bar("sh.600000", HALTED, "x"), volume=D(0))
    backend.raw.append(RawBatch("baostock", "f", week_batch().request, (quiet, halted)))
    assert suspended_dates(scan(backend).values()) == {HALTED}


def test_json_values_round_trip_with_their_types(backend):
    extra = {"tradestatus": 0, "isST": True, "preclose": D("10.15"), "listed": date(2020, 1, 2), "tags": ["a", 1], "x": None}
    bar = replace(CnFake().bar("sh.600000", WEEK[0], "x"), extra={**CnFake().bar("sh.600000", WEEK[0], "x").extra, **extra})
    backend.raw.append(RawBatch("baostock", "f", week_batch().request, (bar,)))
    back = scan(backend)[WEEK[0]].extra
    for key, value in extra.items():
        assert back[key] == value and type(back[key]) is type(value)


def test_encode_decode_is_deterministic_and_lossless():
    extra = {"b": D("1.10"), "a": date(2026, 3, 4)}
    text = encode_extra(extra)
    assert text == encode_extra({"a": date(2026, 3, 4), "b": D("1.10")})  # key order never matters
    assert decode_extra(text) == extra and str(decode_extra(text)["b"]) == "1.10"
    assert encode_extra({}) is None and encode_extra(None) is None and decode_extra(None) is None


def test_re_append_is_a_no_op_and_extra_only_change_rewrites(backend):
    batch = week_batch()
    assert backend.raw.append(batch) == len(WEEK)
    assert backend.raw.append(batch) == 0
    bar = batch.bars[0]
    changed = replace(bar, extra={**bar.extra, "isST": 1})
    assert backend.raw.append(RawBatch("baostock", "f2", batch.request, (changed,))) == 1
    assert scan(backend)[WEEK[0]].extra["isST"] == 1


def write_legacy_part(backend, batch: RawBatch):
    """A raw part written before the ``extra`` column existed (``RAW_BARS_SCHEMA`` only)."""
    rows = raw_rows_from_batch(batch)
    table = raw_rows_to_table(rows)
    assert "extra" not in table.column_names
    directory = backend.raw._table.partition_dir(rows[0])
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "part-0000000000000000001-legacy.parquet"
    pq.write_table(table, path)
    return path


def test_old_files_without_the_extra_column_still_read(backend):
    legacy = write_legacy_part(backend, week_batch())
    assert "extra" not in pq.read_schema(legacy).names
    backend.rebuild_catalog()
    bars = scan(backend)
    assert sorted(bars) == WEEK
    assert all("tradestatus" not in b.extra for b in bars.values())  # nothing was stored: extra is empty
    assert suspended_dates(bars.values()) == {HALTED}  # falls back to the volume == 0 placeholder
    assert backend.raw.scan_table("baostock", Market.CN, Interval.D1, WINDOW).schema.names == RAW_BARS_PARQUET_SCHEMA.names
    assert backend.query("SELECT count(*) AS n FROM raw_bars").column("n")[0].as_py() == len(WEEK)


def test_new_data_over_old_files_backfills_extra_and_compacts(backend):
    batch = week_batch()
    write_legacy_part(backend, batch)
    assert backend.raw.append(batch) == len(WEEK)  # same prices, but now with extra: rows differ -> rewritten
    assert backend.raw.append(batch) == 0
    assert {d: b.extra["tradestatus"] for d, b in scan(backend).items()} == {d: PRICES[d][-1] for d in WEEK}
    results = backend.raw.compact()
    assert results and results[0].rows_after == len(WEEK)
    assert {d: b.extra["tradestatus"] for d, b in scan(backend).items()} == {d: PRICES[d][-1] for d in WEEK}
    names = pq.read_schema(next((backend.root / "raw").rglob("part-*.parquet"))).names
    assert names[-1] == "extra"


def test_bars_without_leftover_extras_store_null(backend):
    bar = replace(CnFake().bar("sh.600000", WEEK[0], "x"), extra={})
    backend.raw.append(RawBatch("baostock", "f", week_batch().request, (bar,)))
    assert backend.query("SELECT extra FROM raw_bars").column("extra").to_pylist() == [None]
    assert "tradestatus" not in scan(backend)[WEEK[0]].extra
