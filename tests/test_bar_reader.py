"""DuckDbBarReader: query-time adjustment, point-in-time ticker resolve, schema-version guard.

Synthetic data only (TEST/ZZZ names, made-up prices).
"""

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal as D

import pyarrow.parquet as pq
import pytest
from synth import weekdays

from openbars.core.models import (
    Adjustment,
    AssetClass,
    CorporateAction,
    CorporateActionKind,
    Instrument,
    Interval,
    Market,
    Session,
    SymbolMapping,
)
from openbars.normalize.instruments import MemoryInstrumentMaster, resolve_symbol_windows
from openbars.storage.errors import SchemaVersionError, StorageError
from openbars.storage.parquet_duckdb import open_backend, parquet_schema_version
from openbars.storage.rows import CanonicalBarRow
from openbars.storage.schema import METADATA_VERSION_KEY, SCHEMA_VERSION

T1 = "us:eq:T1"


@pytest.fixture
def backend(tmp_path):
    return open_backend(tmp_path / "data")


def row(iid, day, close, *, stored="split", volume="4000000", generated_at=None):
    c = D(close)
    return CanonicalBarRow(
        instrument_id=iid, market=Market.US, interval=Interval.D1,
        ts_start=datetime(day.year, day.month, day.day, tzinfo=UTC), session_date=day,
        session=Session.REGULAR, provider="prov_a", open=c - 1, high=c + 2, low=c - 2, close=c,
        volume=D(volume), price_source="prov_a", reconcile_rule="primary_wins", adjustment=stored,
        generated_at=generated_at or datetime(day.year, day.month, day.day, 21, tzinfo=UTC),
    )


SPLIT_EX = date(2020, 8, 31)  # synthetic 4-for-1 on a publicly known date; prices below are made up
D27, D28, D31, SEP1 = date(2020, 8, 27), date(2020, 8, 28), SPLIT_EX, date(2020, 9, 1)


def seed_split(backend):
    backend.canonical.upsert_rows([
        row(T1, D27, "120"), row(T1, D28, "125"), row(T1, D31, "100"), row(T1, SEP1, "101"),
    ])
    backend.instruments.upsert_corporate_actions(
        [CorporateAction(T1, SPLIT_EX, CorporateActionKind.SPLIT, "prov_a", ratio=D("4"))]
    )


def closes(table):
    return [(d, c) for d, c in zip(table.column("session_date").to_pylist(), table.column("close").to_pylist())]


# --- adjustment ---------------------------------------------------------------------------


def test_default_split_read_is_the_stored_series(backend):
    seed_split(backend)
    t = backend.reader.bars([T1])
    assert closes(t) == [(D27, D("120")), (D28, D("125")), (D31, D("100")), (SEP1, D("101"))]
    assert set(t.column("adjustment").to_pylist()) == {"split"}


def test_raw_read_undoes_the_later_split(backend):
    seed_split(backend)
    t = backend.reader.bars([T1], adjustment=Adjustment.RAW)
    assert closes(t) == [(D27, D("480")), (D28, D("500")), (D31, D("100")), (SEP1, D("101"))]
    assert t.column("volume").to_pylist()[:2] == [D("1000000"), D("1000000")]
    assert set(t.column("adjustment").to_pylist()) == {"raw"}
    assert t.schema.field("close").type == backend.canonical.scan_table([T1], Interval.D1, _w()).schema.field("close").type


def _w():
    from openbars.core.models import DateRange

    return DateRange(D27, SEP1)


def test_split_as_of_is_knowledge_date_hides_and_undoes(backend):
    seed_split(backend)
    # as of 08-28: rows generated later are hidden, and the not-yet-known split is undone
    t = backend.reader.bars([T1], as_of=D28)
    assert closes(t) == [(D27, D("480")), (D28, D("500"))]
    # as of the ex-date itself the split is known: stored values verbatim
    assert closes(backend.reader.bars([T1], as_of=SPLIT_EX))[0] == (D27, D("120"))
    # a later knowledge date sees everything, stored
    assert closes(backend.reader.bars([T1], as_of=date(2020, 9, 30)))[:1] == [(D27, D("120"))]


def test_split_dividend_loads_past_end_and_trims(backend):
    seed_split(backend)
    # cash dividend ex 09-01: prior close (08-31, raw = stored = 100), amount 25 -> factor 0.8
    backend.instruments.upsert_corporate_actions(
        [CorporateAction(T1, SEP1, CorporateActionKind.DIVIDEND, "prov_a", amount=D("25"))]
    )
    t = backend.reader.bars([T1], end=D28, adjustment=Adjustment.SPLIT_DIVIDEND)
    assert closes(t) == [(D27, D("96")), (D28, D("100"))]
    assert set(t.column("adjustment").to_pylist()) == {"split_dividend"}
    # as_of before the dividend's ex-date: not applied (rows after as_of are hidden too)
    t = backend.reader.bars([T1], as_of=D31, adjustment=Adjustment.SPLIT_DIVIDEND)
    assert closes(t) == [(D27, D("120")), (D28, D("125")), (D31, D("100"))]
    # the stored column filter is untouched: the default read has no dividend adjustment
    assert closes(backend.reader.bars([T1], end=D28)) == [(D27, D("120")), (D28, D("125"))]


def test_raw_stored_profile_reads_verbatim_and_split_applies_later_splits(backend):
    backend.canonical.upsert_rows([
        row(T1, D28, "500", stored="raw", volume="1000000"), row(T1, SEP1, "101", stored="raw"),
    ], )
    backend.instruments.upsert_corporate_actions(
        [CorporateAction(T1, SPLIT_EX, CorporateActionKind.SPLIT, "prov_a", ratio=D("4"))]
    )
    kw = dict(stored_adjustment=Adjustment.RAW)
    raw = backend.reader.bars([T1], adjustment=Adjustment.RAW, **kw)
    assert closes(raw) == [(D28, D("500")), (SEP1, D("101"))]
    split = backend.reader.bars([T1], adjustment=Adjustment.SPLIT, **kw)
    assert closes(split) == [(D28, D("125")), (SEP1, D("101"))]
    assert split.column("volume").to_pylist()[0] == D("4000000")


# --- point-in-time ticker resolve -----------------------------------------------------------


def seed_reuse(backend):
    a, b = "us:eq:ZZA1", "us:eq:ZZB1"
    backend.instruments.upsert(
        [Instrument(a, Market.US, AssetClass.EQUITY, "USD"), Instrument(b, Market.US, AssetClass.EQUITY, "USD")],
        [
            SymbolMapping(a, "ZZZA", date(2020, 1, 1), date(2020, 1, 10)),  # valid_to inclusive
            SymbolMapping(b, "ZZZA", date(2020, 1, 15), None),
        ],
    )
    days = weekdays(date(2020, 1, 6), 15)  # 01-06 .. 01-24; both instruments have bars every day
    backend.canonical.upsert_rows(
        [row(a, d, "10") for d in days] + [row(b, d, "50") for d in days]
    )
    return a, b, days


def test_reused_ticker_resolves_per_day_across_the_window(backend):
    a, b, days = seed_reuse(backend)
    t = backend.reader.bars(["ZZZA"], start=days[0], end=days[-1])
    got = list(zip(t.column("session_date").to_pylist(), t.column("instrument_id").to_pylist()))
    expect = [(d, a) for d in days if d <= date(2020, 1, 10)] + [(d, b) for d in days if d >= date(2020, 1, 15)]
    assert got == expect
    gap = {date(2020, 1, 13), date(2020, 1, 14)}
    assert gap <= set(days) and not gap & {d for d, _ in got}  # days the ticker pointed nowhere
    by_id = {iid: {d for d, i in got if i == iid} for iid in (a, b)}
    assert max(by_id[a]) == date(2020, 1, 10) and min(by_id[b]) == date(2020, 1, 15)
    assert t.column("close").to_pylist()[0] == D("10") and t.column("close").to_pylist()[-1] == D("50")
    # open-ended window: same answer, nothing before either holder's window
    t2 = backend.reader.bars(["ZZZA"])
    assert list(zip(t2.column("session_date").to_pylist(), t2.column("instrument_id").to_pylist())) == expect


def test_ticker_window_subsets_and_ids_are_unclipped(backend):
    a, b, days = seed_reuse(backend)
    only_a = backend.reader.bars(["ZZZA"], start=date(2020, 1, 6), end=date(2020, 1, 8))
    assert set(only_a.column("instrument_id").to_pylist()) == {a}
    only_b = backend.reader.bars(["ZZZA"], start=date(2020, 1, 13), end=date(2020, 1, 17))
    assert set(only_b.column("instrument_id").to_pylist()) == {b}
    assert only_b.column("session_date").to_pylist()[0] == date(2020, 1, 15)
    assert backend.reader.bars(["ZZZA"], start=date(2020, 1, 13), end=date(2020, 1, 14)).num_rows == 0
    # an id is queried as is: all of its bars, even on days the ticker pointed elsewhere
    assert backend.reader.bars([a]).num_rows == len(days)
    both = backend.reader.bars([a, "ZZZA"], start=date(2020, 1, 20), end=date(2020, 1, 24))
    assert set(both.column("instrument_id").to_pylist()) == {a, b}


def test_resolve_windows_follows_resolve_rules():
    m = MemoryInstrumentMaster()
    m.upsert(
        [Instrument("us:eq:U1", Market.US, AssetClass.EQUITY, "USD")],
        [SymbolMapping("us:eq:U1", "TEST1", date(2020, 1, 1), date(2020, 6, 30))],
    )
    kw = dict(provider="*", market=Market.US)
    # unique history: the fallback covers the days before the first window, but nothing after the close
    assert m.resolve_windows("TEST1", start=date(2019, 12, 1), end=date(2020, 12, 31), **kw) == [
        ("us:eq:U1", date(2019, 12, 1), date(2020, 6, 30))
    ]
    assert m.resolve_windows("TEST1", start=date(2020, 7, 1), end=date(2020, 7, 31), **kw) == []
    assert m.resolve_windows("TEST1", start=None, end=None, **kw) == [("us:eq:U1", None, date(2020, 6, 30))]
    assert m.resolve_windows("NOPE", start=None, end=None, **kw) == []
    for d in (date(2019, 12, 31), date(2020, 3, 3), date(2020, 6, 30), date(2020, 7, 1)):
        expect = m.resolve("TEST1", on=d, **kw)
        segs = m.resolve_windows("TEST1", start=d, end=d, **kw)
        assert [s[0] for s in segs] == ([expect] if expect else [])
    assert resolve_symbol_windows([], lambda i: None, start=date(2020, 2, 1), end=date(2020, 1, 1),
                                  provider="*", market=Market.US) == []


# --- schema version guard -------------------------------------------------------------------


def stamp(path, version):
    table = pq.read_table(path)
    meta = dict(table.schema.metadata or {})
    if version is None:
        meta.pop(METADATA_VERSION_KEY, None)
    else:
        meta[METADATA_VERSION_KEY] = str(version).encode()
    pq.write_table(table.replace_schema_metadata(meta), path)


def a_part(backend):
    return sorted((backend.root / "canonical").rglob("part-*.parquet"))[0]


def test_newer_schema_version_part_is_rejected_everywhere(backend):
    seed_split(backend)
    backend.canonical.upsert_rows([row(T1, D27, "121")])  # a second part, so compact() has work
    assert backend.reader.bars([T1]).num_rows == 4
    part = a_part(backend)
    assert parquet_schema_version(part) == SCHEMA_VERSION
    stamp(part, SCHEMA_VERSION + 1)
    assert parquet_schema_version(part) == SCHEMA_VERSION + 1
    with pytest.raises(SchemaVersionError, match="schema version 2"):
        backend.reader.bars([T1])
    with pytest.raises(SchemaVersionError):
        backend.query("SELECT 1")
    with pytest.raises(SchemaVersionError):
        backend.canonical.scan_rows([T1], Interval.D1, _w())
    with pytest.raises(SchemaVersionError):
        backend.rebuild_catalog()
    with pytest.raises(SchemaVersionError):
        backend.compact()
    with pytest.raises(SchemaVersionError):  # a write that has to compare against existing parts
        backend.canonical.upsert_rows([row(T1, D27, "121")])
    with pytest.raises(SchemaVersionError):
        open_backend(backend.root)
    assert issubclass(SchemaVersionError, StorageError)
    # nothing was truncated or rewritten
    assert pq.read_table(part).num_rows == 4  # untouched
    stamp(part, SCHEMA_VERSION)  # back to a readable version
    assert open_backend(backend.root).reader.bars([T1]).num_rows == 4


def test_v1_and_unstamped_files_still_read(backend):
    seed_split(backend)
    part = a_part(backend)
    stamp(part, None)  # key missing = v1
    assert parquet_schema_version(part) == 1
    assert open_backend(backend.root).reader.bars([T1]).num_rows == 4
    stamp(part, 1)
    assert backend.reader.bars([T1]).num_rows == 4


def test_raw_layer_ref_tables_and_garbage_versions_are_rejected(backend):
    from synth import raw_batch

    backend.raw.append(raw_batch(["TEST1"], weekdays(date(2024, 1, 2), 3)))
    seed_split(backend)
    raw_part = next((backend.root / "raw").rglob("part-*.parquet"))
    stamp(raw_part, 2)
    from openbars.core.models import DateRange

    with pytest.raises(SchemaVersionError):
        backend.raw.scan_rows("prov_a", Market.US, Interval.D1, DateRange(date(2024, 1, 1), date(2024, 1, 31)))
    stamp(raw_part, 1)
    ca = backend.ref_dir / "corporate_actions.parquet"
    stamp(ca, 3)
    with pytest.raises(SchemaVersionError):
        backend.instruments.corporate_actions([T1])
    with pytest.raises(SchemaVersionError):
        backend.instruments.corporate_actions_table()
    with pytest.raises(SchemaVersionError):
        backend.instruments.upsert_corporate_actions(
            [CorporateAction(T1, SEP1, CorporateActionKind.SPLIT, "s", ratio=D("2"))]
        )
    stamp(ca, "banana")
    with pytest.raises(SchemaVersionError):
        backend.instruments.corporate_actions()
    stamp(ca, 1)
    assert len(backend.instruments.corporate_actions()) == 1


def test_corporate_actions_roundtrip_exact_decimal_types(backend):
    third = CorporateAction(T1, SPLIT_EX, CorporateActionKind.SPLIT, "prov_a", ratio=D(1) / D(3))
    backend.instruments.upsert_corporate_actions([third, CorporateAction(
        T1, SEP1, CorporateActionKind.DIVIDEND, "prov_a", amount=D("0.123456785"))])
    got = backend.instruments.corporate_actions([T1])
    assert all(isinstance(a.ratio, D) or a.ratio is None for a in got)
    assert not any(isinstance(v, float) for a in got for v in (a.ratio, a.amount))
    assert got[0].ratio == D("0.33333333")  # decimal(20,8) storage: rounded half-even, stated
    assert got[1].amount == D("0.12345678")
