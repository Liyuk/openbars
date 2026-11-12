"""Crypto trading-pair master data (synthetic assets only; no network)."""

from datetime import date

import pytest

from openbars.core.models import AssetClass, Instrument, Market
from openbars.normalize.crypto_pairs import (
    INVALID_PAIR,
    SYMBOL_CONFLICT,
    CryptoPair,
    CryptoPairListing,
    crypto_instrument_id,
    ingest_crypto_pairs,
    listing_from_provider_instrument,
    pair_assets,
    pair_venue,
    parse_crypto_instrument_id,
)
from openbars.normalize.instruments import MemoryInstrumentMaster
from openbars.providers.base import ProviderInstrument
from openbars.storage.parquet_duckdb import open_backend

D = date
ASOF = D(2024, 1, 10)


@pytest.fixture(params=["memory", "parquet", pytest.param("postgres", marks=pytest.mark.postgres_live)])
def master(request, tmp_path):
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


def binance(base="TEST", quote="USDT", **kw):
    return CryptoPairListing("BINANCE", base, quote, "binance", f"{base}{quote}", **kw)


def coinbase(base="TEST", quote="USDT", **kw):
    return CryptoPairListing("coinbase", base, quote, "coinbase", f"{base}-{quote}", **kw)


def test_deterministic_id():
    assert crypto_instrument_id("BINANCE", "test", "usdt") == "crypto:spot:binance:TEST-USDT"
    assert crypto_instrument_id("Binance", "TEST", "USDT") == crypto_instrument_id("binance", "test", "usdt")
    p = parse_crypto_instrument_id("crypto:spot:coinbase:ZZZ-USDC")
    assert (p.venue, p.base, p.quote, p.display) == ("coinbase", "ZZZ", "USDC", "ZZZ-USDC")
    assert p.instrument_id == "crypto:spot:coinbase:ZZZ-USDC"


@pytest.mark.parametrize(
    "venue,base,quote",
    [
        ("", "TEST", "USDT"),
        ("bin-ance", "TEST", "USDT"),
        ("bin:ance", "TEST", "USDT"),
        ("binance", "", "USDT"),
        ("binance", "TEST", ""),
        ("binance", "TE-ST", "USDT"),
        ("binance", "TEST", "US:DT"),
        ("binance", "TEST/X", "USDT"),
        ("binance", "TÉST", "USDT"),
    ],
)
def test_invalid_codes_rejected(venue, base, quote):
    with pytest.raises(ValueError):
        crypto_instrument_id(venue, base, quote)


@pytest.mark.parametrize("bad", ["us:eq:ABC", "crypto:spot:binance:TESTUSDT", "crypto:spot:Binance:TEST-USDT"])
def test_parse_rejects_non_pair_ids(bad):
    with pytest.raises(ValueError):
        parse_crypto_instrument_id(bad)


def test_two_venues_are_two_instruments(master):
    m = master
    s = ingest_crypto_pairs(m, [binance(), coinbase()], as_of=ASOF)
    assert (s.created, s.reused, s.unresolved) == (2, 0, 0)
    a, b = (m.get(f"crypto:spot:{v}:TEST-USDT") for v in ("binance", "coinbase"))
    assert a is not None and b is not None and a.instrument_id != b.instrument_id
    assert pair_assets(a) == pair_assets(b) == ("TEST", "USDT")
    assert (pair_venue(a), pair_venue(b)) == ("binance", "coinbase")
    assert (a.exchange, b.exchange) == ("BINANCE", "COINBASE")
    assert a.market is Market.CRYPTO and a.asset_class is AssetClass.CRYPTO_SPOT
    assert a.currency == "USDT"
    assert a.external_ids["base"] == "TEST" and a.external_ids["quote"] == "USDT"
    assert a.external_ids["venue"] == "binance" and a.external_ids["provider_symbol"] == "TESTUSDT"


def test_same_venue_different_quote_are_two_instruments(master):
    m = master
    ingest_crypto_pairs(m, [binance(quote="USDT"), binance(quote="USDC")], as_of=ASOF)
    ids = sorted(i.instrument_id for i in m.instruments())
    assert ids == ["crypto:spot:binance:TEST-USDC", "crypto:spot:binance:TEST-USDT"]
    assert {pair_assets(i) for i in m.instruments()} == {("TEST", "USDT"), ("TEST", "USDC")}
    assert {i.currency for i in m.instruments()} == {"USDT", "USDC"}


def test_pair_read_back_from_instrument_not_symbol(master):
    m = master
    # venue-native symbol deliberately does not contain a splittable base/quote
    ingest_crypto_pairs(m, [CryptoPairListing("binance", "ZZZ", "USDC", "binance", "Q1X9")], as_of=ASOF)
    inst = m.get("crypto:spot:binance:ZZZ-USDC")
    assert pair_assets(inst) == ("ZZZ", "USDC")
    pair = CryptoPair.from_instrument(inst)
    assert pair == CryptoPair("binance", "ZZZ", "USDC") and pair.instrument_id == inst.instrument_id
    # from the id alone when external_ids are absent
    bare = Instrument(inst.instrument_id, Market.CRYPTO, AssetClass.CRYPTO_SPOT, "USDC")
    assert pair_assets(bare) == ("ZZZ", "USDC") and pair_venue(bare) == "binance"
    bad = Instrument(inst.instrument_id, Market.CRYPTO, AssetClass.CRYPTO_SPOT, "USDC",
                     external_ids={"base": "TEST", "quote": "USDC", "venue": "binance"})
    with pytest.raises(ValueError):
        pair_assets(bad)


def test_reingest_is_idempotent(master):
    m = master
    rows = [binance(name="Test"), coinbase()]
    ingest_crypto_pairs(m, rows, as_of=ASOF)
    before_i, before_m = sorted(m.instruments(), key=lambda i: i.instrument_id), sorted(
        m.mappings(), key=lambda x: (x.instrument_id, x.valid_from))
    s = ingest_crypto_pairs(m, rows, as_of=D(2024, 2, 1))
    assert (s.created, s.reused, s.unresolved) == (0, 2, 0)
    assert sorted(m.instruments(), key=lambda i: i.instrument_id) == before_i
    assert sorted(m.mappings(), key=lambda x: (x.instrument_id, x.valid_from)) == before_m
    assert len(before_i) == 2 and len(before_m) == 2


def test_duplicate_rows_in_one_batch_reuse(master):
    m = master
    s = ingest_crypto_pairs(m, [binance(), binance(name="Test")], as_of=ASOF)
    assert (s.created, s.reused) == (1, 1)
    assert len(m.instruments()) == 1 and len(m.mappings()) == 1
    assert m.instruments()[0].name == "Test"


def test_listed_on_earlier_wins_and_mapping_start(master):
    m = master
    ingest_crypto_pairs(m, [binance(listed_on=D(2023, 5, 1))], as_of=ASOF)
    assert m.mappings()[0].valid_from == D(2023, 5, 1)
    ingest_crypto_pairs(m, [binance(listed_on=D(2023, 6, 1))], as_of=ASOF)
    ingest_crypto_pairs(m, [binance()], as_of=ASOF)
    assert m.instruments()[0].listed_on == D(2023, 5, 1)
    ingest_crypto_pairs(m, [binance(listed_on=D(2023, 1, 1))], as_of=ASOF)
    assert m.instruments()[0].listed_on == D(2023, 1, 1)


def test_resolve_is_venue_specific_and_no_generic_mapping(master):
    m = master
    ingest_crypto_pairs(m, [binance(), coinbase()], as_of=ASOF)
    on = D(2024, 3, 1)
    assert m.resolve("TESTUSDT", on=on, provider="binance", market=Market.CRYPTO) == "crypto:spot:binance:TEST-USDT"
    assert m.resolve("TEST-USDT", on=on, provider="coinbase", market=Market.CRYPTO) == "crypto:spot:coinbase:TEST-USDT"
    # Coinbase's form does not resolve for binance, and nothing is generic.
    assert m.resolve("TEST-USDT", on=on, provider="binance", market=Market.CRYPTO) is None
    assert m.resolve("TESTUSDT", on=on, provider="coinbase", market=Market.CRYPTO) is None
    assert m.resolve("TEST-USDT", on=on, provider="*", market=Market.CRYPTO) is None
    assert all(x.provider != "*" for x in m.mappings())
    assert m.resolve("TESTUSDT", on=on, provider="binance", market=Market.US) is None


def test_delisting_closes_mapping_window(master):
    m = master
    ingest_crypto_pairs(m, [binance(listed_on=D(2023, 1, 1))], as_of=ASOF)
    s = ingest_crypto_pairs(m, [binance(active=False, delisted_on=D(2024, 2, 1))], as_of=D(2024, 2, 5))
    assert s.delisted == 1 and s.created == 0
    inst = m.get("crypto:spot:binance:TEST-USDT")
    assert inst.delisted_on == D(2024, 2, 1)
    (mp,) = m.mappings()
    assert mp.valid_to == D(2024, 2, 1)
    r = lambda d: m.resolve("TESTUSDT", on=d, provider="binance", market=Market.CRYPTO)
    assert r(D(2024, 2, 1)) == inst.instrument_id  # inclusive
    assert r(D(2023, 6, 1)) == inst.instrument_id
    assert r(D(2024, 2, 2)) is None  # not via an open window, no fallback after the closed one
    # repeating the delisting changes nothing
    s = ingest_crypto_pairs(m, [binance(active=False)], as_of=D(2024, 3, 1))
    assert s.delisted == 0 and m.mappings() == [mp]


def test_inactive_without_date_uses_as_of(master):
    m = master
    ingest_crypto_pairs(m, [binance(listed_on=D(2023, 1, 1))], as_of=ASOF)
    ingest_crypto_pairs(m, [binance(active=False)], as_of=D(2024, 4, 1))
    assert m.instruments()[0].delisted_on == D(2024, 4, 1)
    assert m.mappings()[0].valid_to == D(2024, 4, 1)


def test_new_inactive_pair_gets_closed_window(master):
    m = master
    ingest_crypto_pairs(m, [binance(active=False, listed_on=D(2023, 1, 1), delisted_on=D(2023, 9, 1))], as_of=ASOF)
    (mp,) = m.mappings()
    assert (mp.valid_from, mp.valid_to) == (D(2023, 1, 1), D(2023, 9, 1))
    assert m.resolve("TESTUSDT", on=D(2024, 1, 1), provider="binance", market=Market.CRYPTO) is None


def test_relisting_reuses_id_with_new_window(master):
    m = master
    ingest_crypto_pairs(m, [binance(listed_on=D(2023, 1, 1))], as_of=ASOF)
    ingest_crypto_pairs(m, [binance(active=False, delisted_on=D(2023, 6, 30))], as_of=ASOF)
    ingest_crypto_pairs(m, [binance()], as_of=D(2024, 1, 1))
    assert len(m.instruments()) == 1 and m.instruments()[0].delisted_on is None
    r = lambda d: m.resolve("TESTUSDT", on=d, provider="binance", market=Market.CRYPTO)
    assert r(D(2023, 3, 1)) == r(D(2024, 2, 1)) == "crypto:spot:binance:TEST-USDT"
    # two non-overlapping windows on the same id (the gap is only the unique-history fallback)
    assert sorted((x.valid_from, x.valid_to) for x in m.mappings()) == [
        (D(2023, 1, 1), D(2023, 6, 30)), (D(2024, 1, 1), None)]


def test_recycled_symbol_does_not_inherit_old_id(master):
    m = master
    old = CryptoPairListing("binance", "TEST", "USDT", "binance", "SYM1", listed_on=D(2023, 1, 1))
    ingest_crypto_pairs(m, [old], as_of=ASOF)
    ingest_crypto_pairs(m, [CryptoPairListing("binance", "TEST", "USDT", "binance", "SYM1",
                                              active=False, delisted_on=D(2023, 12, 31))], as_of=ASOF)
    new = CryptoPairListing("binance", "ZZZ", "USDT", "binance", "SYM1", listed_on=D(2024, 3, 1))
    s = ingest_crypto_pairs(m, [new], as_of=D(2024, 3, 1))
    assert s.unresolved == 0
    r = lambda d: m.resolve("SYM1", on=d, provider="binance", market=Market.CRYPTO)
    assert r(D(2023, 6, 1)) == "crypto:spot:binance:TEST-USDT"
    assert r(D(2024, 4, 1)) == "crypto:spot:binance:ZZZ-USDT"
    assert r(D(2024, 2, 1)) is None


def test_symbol_conflict_in_batch_is_unresolved_not_merged(master):
    m = master
    rows = [
        CryptoPairListing("binance", "TEST", "USDT", "binance", "SYMX"),
        CryptoPairListing("binance", "ZZZ", "USDT", "binance", "SYMX"),
    ]
    s = ingest_crypto_pairs(m, rows, as_of=ASOF)
    assert (s.created, s.unresolved, s.reasons) == (1, 1, {SYMBOL_CONFLICT: 1})
    assert [i.instrument_id for i in m.instruments()] == ["crypto:spot:binance:TEST-USDT"]
    assert m.resolve("SYMX", on=ASOF, provider="binance", market=Market.CRYPTO) == "crypto:spot:binance:TEST-USDT"
    assert [r.reason_code for r in m.unresolved("listing")] == [SYMBOL_CONFLICT]


def test_symbol_open_on_other_pair_in_master_is_unresolved(master):
    m = master
    ingest_crypto_pairs(m, [CryptoPairListing("binance", "TEST", "USDT", "binance", "SYMX")], as_of=ASOF)
    s = ingest_crypto_pairs(m, [CryptoPairListing("binance", "ZZZ", "USDT", "binance", "SYMX")], as_of=ASOF)
    assert s.unresolved == 1 and s.created == 0 and len(m.instruments()) == 1


def test_same_symbol_text_on_different_providers_is_fine(master):
    m = master
    rows = [
        CryptoPairListing("binance", "TEST", "USDT", "binance", "TESTUSDT"),
        CryptoPairListing("coinbase", "ZZZ", "USDT", "coinbase", "TESTUSDT"),
    ]
    s = ingest_crypto_pairs(m, rows, as_of=ASOF)
    assert s.unresolved == 0 and s.created == 2
    assert m.resolve("TESTUSDT", on=ASOF, provider="coinbase", market=Market.CRYPTO) == "crypto:spot:coinbase:ZZZ-USDT"


def test_invalid_rows_are_unresolved_batch_continues(master):
    m = master
    rows = [
        CryptoPairListing("binance", "TE-ST", "USDT", "binance", "BAD1"),
        CryptoPairListing("", "TEST", "USDT", "binance", "BAD2"),
        CryptoPairListing("binance", "TEST", "USDT", "binance", ""),
        CryptoPairListing("binance", "TEST", "USDT", "binance", "BAD3", listed_on=D(2024, 1, 2), delisted_on=D(2024, 1, 1)),
        binance(),
    ]
    s = ingest_crypto_pairs(m, rows, as_of=ASOF)
    assert (s.created, s.unresolved, s.reasons) == (1, 4, {INVALID_PAIR: 4})
    assert len(m.instruments()) == 1 and len(m.unresolved("listing")) == 4


def test_us_instruments_untouched(master):
    m = master
    us = Instrument("us:eq:SEQ0001", Market.US, AssetClass.EQUITY, "USD", name="Acme", exchange="XNAS")
    from openbars.core.models import SymbolMapping

    m.upsert([us], [SymbolMapping("us:eq:SEQ0001", "TESTUSDT", D(2020, 1, 1), None, "alpaca")])
    ingest_crypto_pairs(m, [binance(), coinbase()], as_of=ASOF)
    got = m.get("us:eq:SEQ0001")
    assert got is not None
    assert (got.instrument_id, got.market, got.asset_class, got.currency, got.name, got.exchange) == (
        us.instrument_id, us.market, us.asset_class, us.currency, us.name, us.exchange)
    assert m.resolve("TESTUSDT", on=ASOF, provider="alpaca", market=Market.US) == "us:eq:SEQ0001"
    assert m.resolve("TESTUSDT", on=ASOF, provider="binance", market=Market.CRYPTO) == "crypto:spot:binance:TEST-USDT"


def test_master_roundtrip(master):
    store = master
    s = ingest_crypto_pairs(store, [binance(listed_on=D(2023, 1, 1)), coinbase(), binance(quote="USDC")], as_of=ASOF)
    assert s.created == 3
    assert ingest_crypto_pairs(store, [binance(listed_on=D(2023, 1, 1)), coinbase()], as_of=ASOF).created == 0
    assert len(store.instruments()) == 3
    got = store.get("crypto:spot:binance:TEST-USDT")
    assert pair_assets(got) == ("TEST", "USDT") and got.currency == "USDT" and got.exchange == "BINANCE"
    assert store.resolve("TESTUSDT", on=ASOF, provider="binance", market=Market.CRYPTO) == got.instrument_id
    assert store.resolve("TEST-USDT", on=ASOF, provider="coinbase", market=Market.CRYPTO) == "crypto:spot:coinbase:TEST-USDT"


def test_listing_from_provider_instrument_roundtrip():
    row = ProviderInstrument(
        symbol="TESTUSDT",
        market=Market.CRYPTO,
        asset_class=AssetClass.CRYPTO_SPOT,
        name="Test pair",
        exchange="BINANCE",
        active=True,
        base_asset="TEST",
        quote_asset="USDT",
    )
    listing = listing_from_provider_instrument(row, provider="binance")
    assert (listing.venue, listing.base, listing.quote) == ("BINANCE", "TEST", "USDT")
    assert (listing.provider, listing.symbol, listing.name, listing.active) == (
        "binance",
        "TESTUSDT",
        "Test pair",
        True,
    )
    master = MemoryInstrumentMaster()
    summary = ingest_crypto_pairs(master, [listing], as_of=ASOF)
    assert summary.created == 1 and summary.unresolved == 0
    assert pair_assets(master.get("crypto:spot:binance:TEST-USDT")) == ("TEST", "USDT")


def test_listing_from_provider_instrument_requires_assets_and_venue():
    base = dict(symbol="TESTUSDT", market=Market.CRYPTO, asset_class=AssetClass.CRYPTO_SPOT)
    with pytest.raises(ValueError):
        listing_from_provider_instrument(ProviderInstrument(**base, exchange="BINANCE"), provider="binance")
    with pytest.raises(ValueError):
        listing_from_provider_instrument(
            ProviderInstrument(**base, exchange="BINANCE", base_asset="TEST", quote_asset=" "), provider="binance"
        )
    row = ProviderInstrument(**base, base_asset="TEST", quote_asset="USDT")
    with pytest.raises(ValueError):
        listing_from_provider_instrument(row, provider="binance")
    assert listing_from_provider_instrument(row, provider="binance", venue="binance").venue == "binance"
