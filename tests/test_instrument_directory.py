"""Instrument directory: stable ids, ticker reuse, rename, delisting, unresolved queue.

Synthetic symbols/FIGIs only; runs against the in-memory store and the Parquet master.
"""

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from openbars.core.models import AssetClass, Instrument, Market, RawBar, Interval, SymbolMapping
from openbars.normalize.directory import IngestPolicy, InstrumentDirectory, ListingRecord
from openbars.normalize.instruments import (
    MemoryInstrumentMaster,
    assign_instrument,
    assign_instruments,
    figi_instrument_id,
    new_instrument_id,
    resolve_symbol,
)
from openbars.storage.parquet_duckdb import open_backend

D = date


@pytest.fixture(params=["memory", "parquet", pytest.param("postgres", marks=pytest.mark.postgres_live)])
def store(request, tmp_path):
    if request.param == "memory":
        yield MemoryInstrumentMaster()
    elif request.param == "parquet":
        yield open_backend(tmp_path / "data").instruments
    else:  # native Postgres in a throwaway schema (skipped without OPENBARS_TEST_PG_DSN)
        from pg_helpers import throwaway_schema

        from openbars.storage.postgres import PostgresBackend

        dsn = request.getfixturevalue("pg_dsn")
        with throwaway_schema(dsn) as schema:
            yield PostgresBackend(dsn, mode="native", write=True, schema=schema).instruments


@pytest.fixture
def directory(store):
    counter = iter(range(1, 10_000))
    return InstrumentDirectory(
        store,
        id_factory=lambda market: f"{market.value}:eq:SEQ{next(counter):04d}",
        clock=lambda: datetime(2024, 1, 1, tzinfo=UTC),
    )


def rec(symbol="ZZZA", provider="prov_a", **kw) -> ListingRecord:
    kw.setdefault("ticker_type", "CS")
    kw.setdefault("exchange", "XNAS")
    return ListingRecord(provider=provider, symbol=symbol, **kw)


def res(store, symbol, on, provider="prov_x", market=Market.US):
    return store.resolve(symbol, on=on, provider=provider, market=market)


def bar(symbol, day, provider="prov_x"):
    from datetime import datetime as dt

    return RawBar(
        provider=provider, symbol_as_returned=symbol, fetch_id="f1", interval=Interval.D1,
        ts_start=dt(day.year, day.month, day.day, 5, tzinfo=UTC), open=Decimal("1"), high=Decimal("2"),
        low=Decimal("1"), close=Decimal("2"), volume=Decimal("10"), session_date=day,
    )


# --- stable ids ----------------------------------------------------------------------------------


def test_stable_id_rules():
    a = figi_instrument_id("TESTFIGI1")
    assert a == figi_instrument_id("testfigi1") != figi_instrument_id("TESTFIGI2")
    prefix, kind, ulid = a.split(":")
    assert (prefix, kind) == ("us", "eq") and len(ulid) == 26 and ulid[0] in "01234567"
    seen = {new_instrument_id() for _ in range(200)}
    assert len(seen) == 200 and all(len(i.split(":")[2]) == 26 for i in seen)
    assert figi_instrument_id("TESTFIGI1", Market.HK).startswith("hk:eq:")
    # time-ordered: a later millisecond sorts after an earlier one
    assert new_instrument_id(now_ms=lambda: 1_000) < new_instrument_id(now_ms=lambda: 2_000)


# --- new identity --------------------------------------------------------------------------------


def test_new_identity_creates_instrument_and_open_mapping(directory, store):
    s = directory.ingest(
        [
            rec(share_class_figi="TESTFIGI1", cik="0000000001", isin="TESTISIN0001",
                provider_instrument_id="native-1", list_date=D(2015, 1, 2), name="Alpha"),
            rec("ZZZB", ticker_type="ETF", provider_instrument_id="native-2"),
        ],
        as_of=D(2024, 1, 5),
    )
    assert s.counts["created"] == 2 and s.unresolved_count == 0
    a = store.get(figi_instrument_id("TESTFIGI1"))
    assert a.asset_class is AssetClass.EQUITY and a.listed_on == D(2015, 1, 2) and a.name == "Alpha"
    assert dict(a.external_ids) == {
        "figi": "TESTFIGI1", "cik": "0000000001", "isin": "TESTISIN0001", "native:prov_a": "native-1",
    }
    [ma] = [m for m in store.mappings() if m.symbol == "ZZZA"]
    assert (ma.instrument_id, ma.valid_from, ma.valid_to, ma.provider) == (a.instrument_id, D(2015, 1, 2), None, "*")
    [b] = [i for i in store.instruments() if i.asset_class is AssetClass.ETF]
    [mb] = [m for m in store.mappings() if m.symbol == "ZZZB"]
    assert mb.valid_from == D(2024, 1, 5)  # first observed (no list date)
    assert b.instrument_id.startswith("us:eq:SEQ")  # no FIGI -> minted id
    assert res(store, "ZZZA", D(2020, 1, 1)) == a.instrument_id


def test_reingest_is_idempotent(directory, store):
    r = [rec(share_class_figi="TESTFIGI1", list_date=D(2015, 1, 2))]
    directory.ingest(r, as_of=D(2024, 1, 5))
    before = (store.instruments(), store.mappings())
    s = directory.ingest(r, as_of=D(2024, 1, 8))
    assert (store.instruments(), store.mappings()) == before
    assert not s.counts["created"] and store.unresolved_count() == 0


def test_universe_filter_defaults_and_extra_config(store):
    rows = [
        rec("ZZZA", ticker_type="CS", provider_instrument_id="n1"),
        rec("ZZZB", ticker_type="ETF", provider_instrument_id="n2"),
        rec("ZZZC", ticker_type="ADRC", provider_instrument_id="n3"),
        rec("ZZZD", ticker_type="WARRANT", provider_instrument_id="n4"),
    ]
    s = InstrumentDirectory(store).ingest(rows, as_of=D(2024, 1, 5))
    assert s.counts["created"] == 2 and s.counts["skipped_type"] == 2
    assert {m.symbol for m in store.mappings()} == {"ZZZA", "ZZZB"}  # warrant never written as equity
    s = InstrumentDirectory(store, policy=IngestPolicy(frozenset({"adr"}))).ingest(rows, as_of=D(2024, 1, 5))
    assert s.counts["created"] == 1 and s.counts["skipped_type"] == 1
    assert {i.asset_class for i in store.instruments()} == {AssetClass.EQUITY, AssetClass.ETF, AssetClass.ADR}


# --- ticker reuse ----------------------------------------------------------------------------------


def test_ticker_reuse_with_listing_dates_keeps_histories_apart(directory, store):
    directory.ingest(
        [rec(share_class_figi="TESTFIGI1", list_date=D(2015, 1, 2))], as_of=D(2019, 1, 2)
    )
    s = directory.ingest(
        [rec(share_class_figi="TESTFIGI2", list_date=D(2021, 3, 1))], as_of=D(2022, 6, 1)
    )
    assert s.counts["reused"] == 1 and s.unresolved_count == 0
    a, b = figi_instrument_id("TESTFIGI1"), figi_instrument_id("TESTFIGI2")
    old = next(m for m in store.mappings() if m.instrument_id == a)
    assert (old.valid_from, old.valid_to) == (D(2015, 1, 2), D(2021, 2, 28))  # day before the new listing
    assert res(store, "ZZZA", D(2018, 5, 1)) == a
    assert res(store, "ZZZA", D(2021, 2, 28)) == a
    assert res(store, "ZZZA", D(2021, 3, 1)) == b
    assert res(store, "ZZZA", D(2024, 1, 1)) == b
    assert a != b

    bars = [bar("ZZZA", D(2018, 5, 1)), bar("ZZZA", D(2018, 5, 2)), bar("ZZZA", D(2023, 5, 1))]
    out = assign_instruments(bars, store)
    assert [x.instrument_id for x in out.resolved] == [a, a, b] and not out.unresolved


def test_ticker_reuse_without_listing_date_closes_at_last_observed(directory, store):
    directory.ingest([rec(provider_instrument_id="native-1")], as_of=D(2019, 1, 2))
    directory.ingest([rec(provider_instrument_id="native-1")], as_of=D(2020, 1, 10))
    s = directory.ingest([rec(provider_instrument_id="native-2")], as_of=D(2022, 3, 1))
    assert s.counts["reused"] == 1
    first = next(i for i in store.instruments() if i.external_ids["native:prov_a"] == "native-1")
    second = next(i for i in store.instruments() if i.external_ids["native:prov_a"] == "native-2")
    old = next(m for m in store.mappings() if m.instrument_id == first.instrument_id)
    assert (old.valid_from, old.valid_to) == (D(2019, 1, 2), D(2020, 1, 10))
    assert res(store, "ZZZA", D(2020, 1, 10)) == first.instrument_id
    assert res(store, "ZZZA", D(2022, 3, 1)) == second.instrument_id
    # the gap is not guessable: the symbol was held by two instruments -> never a fallback
    assert res(store, "ZZZA", D(2021, 6, 1)) is None
    assert res(store, "ZZZA", D(2018, 1, 1)) is None
    out = assign_instruments([bar("ZZZA", D(2021, 6, 1))], store)
    assert not out.resolved and out.records[0].reason_code == "symbol_reused_uncovered_date"
    assert store.unresolved_count("bar") == 1


def test_open_alias_on_other_instrument_without_dates_is_unresolved(directory, store):
    s = directory.ingest(
        [rec(provider="prov_a", provider_instrument_id="a-1"), rec(provider="prov_b", provider_instrument_id="b-1")],
        as_of=D(2024, 1, 5),
    )
    assert s.counts["created"] == 1 and s.unresolved_count == 1
    assert s.unresolved[0].reason_code == "symbol_has_open_alias"
    assert len(store.instruments()) == 1 and len(store.mappings()) == 1  # nothing guessed
    assert store.unresolved_count("listing") == 1


def test_no_overlap_when_listing_predates_open_alias(directory, store):
    directory.ingest([rec(share_class_figi="TESTFIGI1", list_date=D(2018, 1, 1))], as_of=D(2024, 1, 5))
    s = directory.ingest([rec(share_class_figi="TESTFIGI2", list_date=D(2017, 1, 1))], as_of=D(2024, 1, 6))
    assert s.unresolved[0].reason_code == "symbol_has_open_alias"
    assert len(store.instruments()) == 1


def test_historic_delisted_listing_fits_before_current_holder(directory, store):
    directory.ingest([rec(share_class_figi="TESTFIGI2", list_date=D(2018, 1, 1))], as_of=D(2024, 1, 5))
    s = directory.ingest(
        [rec(share_class_figi="TESTFIGI1", list_date=D(2010, 1, 1), delisted_on=D(2015, 6, 30))],
        as_of=D(2024, 1, 5),
    )
    assert s.counts["created"] == 1 and s.unresolved_count == 0
    a, b = figi_instrument_id("TESTFIGI1"), figi_instrument_id("TESTFIGI2")
    assert store.get(a).delisted_on == D(2015, 6, 30)
    assert res(store, "ZZZA", D(2012, 1, 1)) == a and res(store, "ZZZA", D(2020, 1, 1)) == b
    assert res(store, "ZZZA", D(2016, 1, 1)) is None  # nobody held it


def test_delisted_listing_overlapping_current_holder_is_unresolved(directory, store):
    directory.ingest([rec(share_class_figi="TESTFIGI2", list_date=D(2018, 1, 1))], as_of=D(2024, 1, 5))
    s = directory.ingest(
        [rec(share_class_figi="TESTFIGI1", list_date=D(2010, 1, 1), delisted_on=D(2019, 6, 30))],
        as_of=D(2024, 1, 5),
    )
    assert s.unresolved[0].reason_code == "symbol_window_overlap"


# --- rename ----------------------------------------------------------------------------------------


def test_rename_keeps_one_instrument_and_continuous_history(directory, store):
    directory.ingest([rec(share_class_figi="TESTFIGI1", list_date=D(2015, 1, 2))], as_of=D(2022, 1, 3))
    s = directory.ingest(
        [rec("ZZZB", share_class_figi="TESTFIGI1", list_date=D(2022, 5, 2))], as_of=D(2022, 5, 3)
    )
    assert s.counts["renamed"] == 1 and s.counts["created"] == 0
    [inst] = store.instruments()
    maps = sorted(store.mappings(), key=lambda m: m.valid_from)
    assert [(m.symbol, m.valid_from, m.valid_to) for m in maps] == [
        ("ZZZA", D(2015, 1, 2), D(2022, 5, 1)),
        ("ZZZB", D(2022, 5, 2), None),
    ]  # contiguous, no gap, no overlap
    assert res(store, "ZZZA", D(2022, 5, 1)) == inst.instrument_id == res(store, "ZZZB", D(2022, 5, 2))
    # the old ticker after the rename is not claimed by the instrument any more
    assert res(store, "ZZZA", D(2023, 1, 1)) is None


def test_rename_by_provider_native_id_closes_at_last_observed(directory, store):
    directory.ingest([rec(provider_instrument_id="native-1")], as_of=D(2022, 1, 3))
    directory.ingest([rec(provider_instrument_id="native-1")], as_of=D(2022, 2, 1))
    directory.ingest([rec("ZZZB", provider_instrument_id="native-1")], as_of=D(2022, 6, 1))
    [inst] = store.instruments()
    maps = {m.symbol: m for m in store.mappings()}
    assert (maps["ZZZA"].valid_to, maps["ZZZB"].valid_from, maps["ZZZB"].valid_to) == (D(2022, 2, 1), D(2022, 6, 1), None)
    assert res(store, "ZZZB", D(2022, 7, 1)) == inst.instrument_id == res(store, "ZZZA", D(2022, 1, 15))


def test_rename_then_old_ticker_reused_by_other_company(directory, store):
    directory.ingest([rec(share_class_figi="TESTFIGI1", list_date=D(2015, 1, 2))], as_of=D(2022, 1, 3))
    directory.ingest([rec("ZZZB", share_class_figi="TESTFIGI1", list_date=D(2022, 5, 2))], as_of=D(2022, 5, 3))
    s = directory.ingest([rec(share_class_figi="TESTFIGI2", list_date=D(2023, 1, 2))], as_of=D(2023, 1, 3))
    assert s.counts["created"] == 1 and s.counts["reused"] == 0 and s.unresolved_count == 0
    a, b = figi_instrument_id("TESTFIGI1"), figi_instrument_id("TESTFIGI2")
    assert res(store, "ZZZA", D(2020, 1, 1)) == a and res(store, "ZZZA", D(2023, 6, 1)) == b
    assert res(store, "ZZZB", D(2023, 6, 1)) == a


# --- delisting ------------------------------------------------------------------------------------


def test_delisting_closes_alias_window(directory, store):
    directory.ingest([rec(share_class_figi="TESTFIGI1", list_date=D(2015, 1, 2))], as_of=D(2023, 1, 3))
    s = directory.ingest(
        [rec(share_class_figi="TESTFIGI1", delisted_on=D(2023, 3, 15), active=False)], as_of=D(2023, 4, 1)
    )
    a = figi_instrument_id("TESTFIGI1")
    assert s.counts["delisted"] == 1
    assert store.get(a).delisted_on == D(2023, 3, 15)
    [m] = store.mappings()
    assert m.valid_to == D(2023, 3, 15)
    assert res(store, "ZZZA", D(2023, 3, 15)) == a
    assert res(store, "ZZZA", D(2023, 3, 16)) is None  # no fallback after the closed window


def test_symbol_only_delisting_closes_the_unambiguous_alias(directory, store):
    directory.ingest([rec(share_class_figi="TESTFIGI1", cik="0000000001", list_date=D(2015, 1, 2))], as_of=D(2023, 1, 3))
    s = directory.ingest(
        [rec(provider="prov_b", ticker_type=None, cik="0000000009", delisted_on=D(2023, 3, 15), active=False)],
        as_of=D(2023, 4, 1),
    )
    assert s.unresolved[0].reason_code == "issuer_mismatch"
    assert store.mappings()[0].valid_to is None
    directory.ingest(
        [rec(provider="prov_b", ticker_type=None, cik="0000000001", delisted_on=D(2023, 3, 15), active=False)],
        as_of=D(2023, 4, 2),
    )
    assert store.mappings()[0].valid_to == D(2023, 3, 15)
    # a delisting that predates the open alias is not applied
    directory.ingest([rec(share_class_figi="TESTFIGI2", list_date=D(2024, 1, 2))], as_of=D(2024, 1, 3))
    s = directory.ingest(
        [rec(provider="prov_b", ticker_type=None, delisted_on=D(2020, 1, 1), active=False)], as_of=D(2024, 1, 4)
    )
    assert s.unresolved[0].reason_code == "delisting_before_open_alias"
    assert next(m for m in store.mappings() if m.valid_from == D(2024, 1, 2)).valid_to is None


def test_ticker_change_reported_as_delisting_does_not_delist_the_instrument(directory, store):
    directory.ingest([rec(share_class_figi="TESTFIGI1", list_date=D(2015, 1, 2))], as_of=D(2022, 1, 3))
    directory.ingest([rec("ZZZB", share_class_figi="TESTFIGI1", list_date=D(2022, 5, 2))], as_of=D(2022, 5, 3))
    s = directory.ingest(
        [rec(share_class_figi="TESTFIGI1", delisted_on=D(2022, 5, 1), active=False)], as_of=D(2022, 6, 1)
    )
    assert s.counts["ticker_changed"] == 1 and s.counts["delisted"] == 0
    assert store.get(figi_instrument_id("TESTFIGI1")).delisted_on is None
    assert res(store, "ZZZB", D(2022, 6, 1)) == figi_instrument_id("TESTFIGI1")


# --- symbol-only / conflicts / unresolved queue ----------------------------------------------------


def test_symbol_only_rows_confirm_but_never_create(directory, store):
    s = directory.ingest([rec(ticker_type=None, provider="prov_b")], as_of=D(2024, 1, 5))
    assert s.unresolved[0].reason_code == "symbol_only_evidence" and not store.instruments()
    directory.ingest([rec(share_class_figi="TESTFIGI1", list_date=D(2015, 1, 2))], as_of=D(2024, 1, 5))
    s = directory.ingest([rec(ticker_type=None, provider="prov_b")], as_of=D(2024, 2, 5))
    assert s.counts["confirmed"] == 1 and s.unresolved_count == 0
    # confirmation pushes the last-observed date a later reuse would close at
    directory.ingest([rec(share_class_figi="TESTFIGI2")], as_of=D(2024, 6, 1))
    old = next(m for m in store.mappings() if m.instrument_id == figi_instrument_id("TESTFIGI1"))
    assert old.valid_to == D(2024, 2, 5)
    # and the earlier unresolved listing row was cleared once the symbol became confirmable
    assert store.unresolved_count("listing") == 0


def test_conflicting_identities_are_disputed_not_merged(directory, store):
    directory.ingest(
        [rec("ZZZA", share_class_figi="TESTFIGI1"), rec("ZZZB", provider_instrument_id="native-2")],
        as_of=D(2024, 1, 5),
    )
    # one row names the FIGI of instrument 1 and the native id of instrument 2
    s = directory.ingest(
        [rec("ZZZB", share_class_figi="TESTFIGI1", provider_instrument_id="native-2")], as_of=D(2024, 2, 5)
    )
    assert s.status["disputed"] == 1 and s.unresolved[0].reason_code == "conflicting_stable_identity"
    assert len(store.instruments()) == 2
    # issuer / class mismatch on the same FIGI
    directory.ingest([rec("ZZZA", share_class_figi="TESTFIGI1", cik="0000000001")], as_of=D(2024, 2, 6))
    s = directory.ingest([rec("ZZZA", share_class_figi="TESTFIGI1", cik="0000000002")], as_of=D(2024, 2, 7))
    assert s.unresolved[0].reason_code == "issuer_mismatch"
    s = directory.ingest([rec("ZZZA", ticker_type="ETF", share_class_figi="TESTFIGI1")], as_of=D(2024, 2, 8))
    assert s.unresolved[0].reason_code == "asset_class_mismatch"


def test_corroborating_provider_verifies(directory):
    directory.ingest([rec(share_class_figi="TESTFIGI1", provider="prov_a")], as_of=D(2024, 1, 5))
    s = directory.ingest([rec(share_class_figi="TESTFIGI1", provider="prov_b")], as_of=D(2024, 1, 6))
    assert s.status["verified"] == 1


def test_unclassified_row_cannot_create_but_can_match(directory, store):
    s = directory.ingest([rec(ticker_type=None, provider_instrument_id="n1")], as_of=D(2024, 1, 5))
    assert s.unresolved[0].reason_code == "unclassified_security_type" and not store.instruments()
    directory.ingest([rec(provider_instrument_id="n1")], as_of=D(2024, 1, 6))
    s = directory.ingest([rec(ticker_type=None, provider_instrument_id="n1")], as_of=D(2024, 1, 7))
    assert s.unresolved_count == 0 and len(store.instruments()) == 1


def test_resolved_listing_clears_its_queue_entry(directory, store):
    directory.ingest([rec(provider_instrument_id="a-1")], as_of=D(2024, 1, 5))
    directory.ingest([rec(provider="prov_b", provider_instrument_id="b-1")], as_of=D(2024, 1, 5))
    assert store.unresolved_count("listing") == 1
    # the second company's row now carries a listing date -> binds, the queue entry goes away
    both = [rec(provider_instrument_id="a-1"), rec(provider="prov_b", provider_instrument_id="b-1")]
    directory.ingest(both, as_of=D(2024, 1, 8))  # the old holder is still listed
    assert store.unresolved_count("listing") == 1  # still one row for the symbol, not one per snapshot
    directory.ingest(
        [rec(provider="prov_b", provider_instrument_id="b-1", list_date=D(2024, 1, 10))], as_of=D(2024, 1, 12)
    )  # a listing date later than the old holder's last sighting settles the order
    assert store.unresolved_count("listing") == 0
    assert len(store.instruments()) == 2
    assert res(store, "ZZZA", D(2024, 1, 9)) != res(store, "ZZZA", D(2024, 1, 10))


# --- resolution rules -------------------------------------------------------------------------------


def test_unique_history_fallback_only_when_symbol_never_reused(store):
    store.upsert(
        [Instrument("us:eq:A", Market.US, AssetClass.EQUITY, "USD")],
        [SymbolMapping("us:eq:A", "ZZZA", D(2022, 6, 1), None)],
    )
    assert res(store, "ZZZA", D(2022, 6, 1)) == "us:eq:A"
    # first-observed is later than the true listing: unique history still resolves
    assert res(store, "ZZZA", D(2021, 1, 1)) == "us:eq:A"
    # a second instrument ever bound to the symbol disables the fallback entirely
    store.upsert(
        [Instrument("us:eq:B", Market.US, AssetClass.EQUITY, "USD")],
        [SymbolMapping("us:eq:A", "ZZZA", D(2022, 6, 1), D(2023, 1, 1)),
         SymbolMapping("us:eq:B", "ZZZA", D(2023, 6, 1), None)],
    )
    assert res(store, "ZZZA", D(2021, 1, 1)) is None
    assert res(store, "ZZZA", D(2023, 3, 1)) is None


def test_no_fallback_after_the_last_closed_window(store):
    store.upsert(
        [Instrument("us:eq:A", Market.US, AssetClass.EQUITY, "USD")],
        [SymbolMapping("us:eq:A", "ZZZA", D(2020, 1, 1), D(2021, 1, 1))],
    )
    assert res(store, "ZZZA", D(2019, 1, 1)) == "us:eq:A"
    assert res(store, "ZZZA", D(2021, 1, 2)) is None  # a recycled ticker would land here
    detail = store.resolve_detailed("ZZZA", on=D(2021, 1, 2), provider="p", market=Market.US)
    assert detail.reason_code == "outside_mapping_window"


def test_ambiguity_and_missing_map_are_unresolved_and_counted(store):
    store.upsert(
        [Instrument("us:eq:A", Market.US, AssetClass.EQUITY, "USD"), Instrument("us:eq:B", Market.US, AssetClass.EQUITY, "USD")],
        [SymbolMapping("us:eq:A", "ZZZA", D(2020, 1, 1)), SymbolMapping("us:eq:B", "ZZZA", D(2020, 1, 1))],
    )
    assert res(store, "ZZZA", D(2024, 1, 1)) is None
    bars = [bar("ZZZA", D(2024, 1, 2)), bar("NOPE", D(2024, 1, 2)), bar("ZZZA", D(2024, 1, 3))]
    out = assign_instruments(bars, store)
    assert out.resolved == () and len(out.unresolved) == 3
    assert all(b.instrument_id is None for b in out.unresolved)
    assert sorted(r.reason_code for r in out.records) == ["ambiguous_mapping", "ambiguous_mapping", "no_mapping"]
    assert store.unresolved_count() == 3 and store.unresolved_count("bar") == 3
    # one queue row per (provider, symbol, day); re-running does not inflate the count
    assign_instruments(bars, store)
    assert store.unresolved_count() == 3
    # a known id passes through and a bar can be assigned singly
    assert assign_instrument(bar("NOPE", D(2024, 1, 4)), store, record=False).instrument_id is None
    assert store.unresolved_count() == 3
    pre = bar("ZZZA", D(2024, 1, 2))
    import dataclasses
    assert assign_instrument(dataclasses.replace(pre, instrument_id="us:eq:A"), store).instrument_id == "us:eq:A"


def test_assign_instrument_fills_id_and_session_date(store):
    store.upsert(
        [Instrument("us:eq:A", Market.US, AssetClass.EQUITY, "USD")],
        [SymbolMapping("us:eq:A", "ZZZA", D(2020, 1, 1), D(2020, 12, 31))],
    )
    import dataclasses
    raw = dataclasses.replace(bar("ZZZA", D(2020, 6, 1)), session_date=None)
    got = assign_instrument(raw, store)
    assert got.instrument_id == "us:eq:A" and got.session_date == D(2020, 6, 1)
    late = assign_instrument(bar("ZZZA", D(2021, 6, 1)), store)
    assert late.instrument_id is None and store.unresolved_count("bar") == 1
    # a different market never resolves
    assert assign_instrument(bar("ZZZA", D(2020, 6, 1)), store, market=Market.HK, record=False).instrument_id is None


def test_resolve_symbol_pure_provider_specific_beats_generic():
    inst = {"A": Market.US, "B": Market.US}
    maps = [
        SymbolMapping("A", "ZZZA", D(2020, 1, 1), None, "*"),
        SymbolMapping("B", "ZZZA", D(2020, 1, 1), None, "prov_x"),
    ]
    r = lambda p: resolve_symbol(maps, inst.get, on=D(2021, 1, 1), provider=p, market=Market.US)  # noqa: E731
    assert r("prov_x").instrument_id == "B" and r("prov_y").instrument_id == "A"
    assert r("prov_x").via == "window"


# --- upstream compat & persistence ------------------------------------------------------------------


def test_upstream_id_link(directory, store):
    directory.ingest([rec(share_class_figi="TESTFIGI1"), rec("ZZZB", share_class_figi="TESTFIGI2")], as_of=D(2024, 1, 5))
    a, b = figi_instrument_id("TESTFIGI1"), figi_instrument_id("TESTFIGI2")
    directory.link_upstream_id(a, 101)
    directory.link_upstream_id(a, 101)  # idempotent
    assert directory.upstream_id(a) == 101 and directory.instrument_for_upstream_id(101) == a
    assert directory.upstream_id(b) is None and directory.instrument_for_upstream_id(7) is None
    with pytest.raises(ValueError):
        directory.link_upstream_id(a, 102)
    with pytest.raises(ValueError):
        directory.link_upstream_id(b, 101)
    with pytest.raises(KeyError):
        directory.link_upstream_id("us:eq:nope", 5)


def test_parquet_identity_state_persists(tmp_path):
    backend = open_backend(tmp_path / "data")
    d = InstrumentDirectory(backend.instruments)
    d.ingest([rec(share_class_figi="TESTFIGI1", list_date=D(2015, 1, 2))], as_of=D(2019, 1, 2))
    d.ingest([rec(share_class_figi="TESTFIGI2")], as_of=D(2024, 1, 2))  # reuse w/o list date -> closes at last obs
    d.ingest([rec(provider="prov_b", provider_instrument_id="b-1")], as_of=D(2024, 1, 2))  # dateless dup holder
    d.link_upstream_id(figi_instrument_id("TESTFIGI1"), 5)
    assert (tmp_path / "data" / "ref" / "unresolved.parquet").exists()
    again = open_backend(tmp_path / "data")
    assert again.instruments.unresolved_count() == 1
    [u] = again.instruments.unresolved("listing")
    assert (u.reason_code, u.symbol, u.provider, u.session_date) == ("symbol_has_open_alias", "ZZZA", "prov_b", D(2024, 1, 2))
    assert again.instruments.upstream_ids() == {figi_instrument_id("TESTFIGI1"): 5}
    assert any(o[:3] == (figi_instrument_id("TESTFIGI1"), "ZZZA", "prov_a") for o in again.instruments.observations())
    # removed (re-dated) mapping rows really go away
    assert len(again.instruments.mappings()) == 2
