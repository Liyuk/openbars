"""Generic bar source-info contract (#202): canonical -> raw fetch batch pointer.

The adapter-facing contract is documented in ``docs/source-contract.md``. These offline tests
pin the machine-checkable parts: ``source_fetch_id`` is an additive nullable canonical column,
the native producers copy the selected raw row's ``fetch_id`` into it, and a pre-#202 canonical
part (no column) still reads back with a null value.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from openbars.core.models import DateRange, Interval, Market, RawBar
from openbars.core.schema import CANONICAL_DAILY_FIELDS, FieldSpec
from openbars.core.series import CanonicalDailyRow
from openbars.orchestrator.finalize import (
    CnCanonicalSink,
    ReconcilingSink,
    observation_from_raw,
    parquet_canonical_writer,
)
from openbars.orchestrator.jobs import Job, JobKind
from openbars.providers.base import FetchRequest, RawBatch
from openbars.reconcile.hourly import reconcile_hourly
from openbars.reconcile.reconciler import MemoryReconciler
from openbars.storage.parquet_duckdb import open_backend
from openbars.storage.postgres import compat_canonical_row, native_ddl
from openbars.storage.rows import CanonicalBarRow, InvalidBarError, canonical_rows_to_table
from openbars.storage.schema import (
    CANONICAL_BARS_SCHEMA,
    CANONICAL_LINEAGE_COLUMNS,
    SCHEMA_VERSION,
)

IID = "us:eq:TEST1"
D1 = date(2025, 1, 2)
WINDOW = DateRange(D1, D1)
T0 = datetime(2025, 1, 2, 22, 0, tzinfo=UTC)


def _raw_bar(
    provider: str = "massive",
    feed: str = "eod",
    *,
    fetch_id: str = "fetch-1",
    interval: Interval = Interval.D1,
    hour: int = 0,
    instrument_id: str | None = None,
    close: str = "10",
) -> RawBar:
    return RawBar(
        provider=provider, symbol_as_returned="TEST1", fetch_id=fetch_id, interval=interval,
        ts_start=datetime(2025, 1, 2, hour, tzinfo=UTC), session_date=D1, feed=feed,
        open=Decimal("10"), high=Decimal("12"), low=Decimal("9"), close=Decimal(close),
        volume=Decimal("100"), instrument_id=instrument_id,
    )  # fmt: skip


def _canon_row(**kw) -> CanonicalBarRow:
    base = dict(
        instrument_id=IID, market=Market.US, interval=Interval.D1, ts_start=T0, session_date=D1,
        session="regular", provider="massive", open=Decimal("10"), high=Decimal("12"),
        low=Decimal("9"), close=Decimal("10"), volume=Decimal("100"), price_source="massive:eod",
        reconcile_rule="primary_source", feed="eod", adjustment="split", revision=1,
        generated_at=T0,
    )
    base.update(kw)
    return CanonicalBarRow(**base)


# --- schema / model -------------------------------------------------------------------


def test_source_fetch_id_is_an_additive_nullable_column():
    field = CANONICAL_BARS_SCHEMA.field("source_fetch_id")
    assert field.nullable and field.type == pa.string()
    assert CANONICAL_BARS_SCHEMA.names[-1] == "source_fetch_id"
    assert SCHEMA_VERSION == 1
    # lineage, not content: an unchanged bar re-fetched under a new batch does not rewrite
    assert "source_fetch_id" in CANONICAL_LINEAGE_COLUMNS
    assert [f for f in CANONICAL_DAILY_FIELDS if f.name == "source_fetch_id"] == [
        FieldSpec("source_fetch_id", "string", True)
    ]


def test_source_fetch_id_is_optional_and_validated():
    assert _canon_row().source_fetch_id is None
    assert _canon_row(source_fetch_id="fetch-1").source_fetch_id == "fetch-1"
    with pytest.raises(InvalidBarError, match="source_fetch_id"):
        _canon_row(source_fetch_id="")
    with pytest.raises(ValueError, match="source_fetch_id"):
        CanonicalDailyRow(
            instrument_id=IID, session_date=D1, policy_version="research-daily-v2",
            selected_source_bar_id=None, provider="massive", feed="eod", adjustment="split",
            revision=1, open=Decimal("10"), high=Decimal("12"), low=Decimal("9"),
            close=Decimal("10"), volume=Decimal("100"), selection_reason="primary_source",
            quality_status="complete", capabilities=(), source_fingerprint="x", generated_at=T0,
            source_fetch_id="",
        )


# --- native producers -----------------------------------------------------------------


def test_us_daily_sink_copies_the_selected_raw_fetch_id(tmp_path):
    backend = open_backend(tmp_path)
    request = FetchRequest(Market.US, Interval.D1, ("TEST1",), WINDOW)
    bar = _raw_bar(fetch_id="fetch-us-1")
    backend.raw.append(RawBatch("massive", "fetch-us-1", request, (bar,)))
    sink = ReconcilingSink(
        MemoryReconciler(), parquet_canonical_writer(backend.canonical), backend.raw,
        ("massive", "alpaca"),
    )
    sink.after_fetch(Job("j", JobKind.DAILY, "massive", Market.US, Interval.D1, ("TEST1",), WINDOW), [bar])

    (canon,) = backend.canonical.scan_rows(["TEST1"], Interval.D1, WINDOW)
    (raw,) = backend.raw.scan_rows("massive", Market.US, Interval.D1, WINDOW)
    assert canon.source_fetch_id == raw.fetch_id == "fetch-us-1"
    arrow = backend.reader.bars(["TEST1"], start=D1, end=D1)
    assert arrow.column("source_fetch_id").to_pylist() == ["fetch-us-1"]


def test_cn_daily_sink_copies_the_raw_fetch_id(tmp_path):
    backend = open_backend(tmp_path)
    rows: list[CanonicalBarRow] = []
    sink = CnCanonicalSink(backend.instruments, rows.extend, canonical=backend.canonical)
    bar = replace(
        _raw_bar("baostock", "ashare", fetch_id="fetch-cn-1", instrument_id="cn:eq:600000.SH"),
        symbol_as_returned="sh.600000",
        extra={"market": "cn", "tradestatus": 1, "freshness": "final"},
    )
    sink.after_fetch(
        Job("cn", JobKind.DAILY, "baostock", Market.CN, Interval.D1, ("sh.600000",), WINDOW), [bar]
    )
    assert rows and rows[0].source_fetch_id == "fetch-cn-1"


def test_hourly_rows_copy_the_raw_fetch_id(tmp_path):
    backend = open_backend(tmp_path)
    request = FetchRequest(Market.US, Interval.H1, ("TEST1",), WINDOW)
    bar = _raw_bar("massive", "aggs", fetch_id="fetch-h1-1", interval=Interval.H1, hour=15)
    backend.raw.append(RawBatch("massive", "fetch-h1-1", request, (bar,)))
    reloaded = list(backend.raw.scan("massive", Market.US, Interval.H1, WINDOW))
    (canon,) = reconcile_hourly(reloaded)
    assert canon.source_fetch_id == "fetch-h1-1"
    assert reconcile_hourly([bar])[0].source_fetch_id == "fetch-h1-1"


def test_observation_and_row_carry_the_fetch_id():
    obs = observation_from_raw(_raw_bar(fetch_id="fetch-abc"))
    assert obs is not None and obs.fetch_id == "fetch-abc"


# --- old files / compat ---------------------------------------------------------------


def test_pre_202_canonical_part_reads_a_null_source_fetch_id(tmp_path):
    backend = open_backend(tmp_path)
    table = canonical_rows_to_table([_canon_row()]).drop_columns(["source_fetch_id"])
    table = table.replace_schema_metadata(CANONICAL_BARS_SCHEMA.metadata)
    directory = backend.root / "canonical" / "market=us" / "interval=1d" / "year=2025"
    directory.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, directory / "part-0000000000000000001-old00000.parquet")
    assert "source_fetch_id" not in pq.read_schema(directory / "part-0000000000000000001-old00000.parquet").names
    (row,) = backend.canonical.scan_rows([IID], Interval.D1, WINDOW)
    assert row.source_fetch_id is None
    arrow = backend.reader.bars([IID], start=D1, end=D1)
    assert arrow.column("source_fetch_id").null_count == 1


def test_native_ddl_adds_the_optional_column():
    ddl = "\n".join(native_ddl())
    assert '"source_fetch_id" TEXT' in ddl
    assert 'ADD COLUMN IF NOT EXISTS source_fetch_id TEXT' in ddl


def test_compat_rows_expose_the_same_fetch_id_as_the_compat_raw_view():
    row = compat_canonical_row({
        "instrument_id": 42, "session_date": D1, "policy_version": "research-daily-v2",
        "selected_source_bar_id": 7, "provider": "massive", "feed": "eod", "adjustment": "split",
        "revision": 1, "open": Decimal("10"), "high": Decimal("12"), "low": Decimal("9"),
        "close": Decimal("10"), "volume": Decimal("100"), "selection_reason": "primary_source",
        "quality_status": "complete", "capabilities": [], "source_fingerprint": "x",
        "generated_at": T0,
    })  # fmt: skip
    assert row.source_fetch_id == "compat:7"

    missing = compat_canonical_row({
        "instrument_id": 42, "session_date": D1, "policy_version": "research-daily-v2",
        "selected_source_bar_id": None, "provider": "massive", "feed": "eod",
        "adjustment": "split", "revision": 1, "open": Decimal("10"), "high": Decimal("12"),
        "low": Decimal("9"), "close": Decimal("10"), "volume": Decimal("100"),
        "selection_reason": "primary_source", "quality_status": "complete", "capabilities": [],
        "source_fingerprint": "x", "generated_at": T0,
    })  # fmt: skip
    assert missing.source_fetch_id is None
