"""Ingest-time mapping of provider-returned corporate actions to stable instrument ids.

Synthetic tickers/prices only (ZZZA, TEST1). Memory and Parquet masters share one contract.
"""

from datetime import date
from decimal import Decimal as D

import pytest

from openbars.core.models import (
    AssetClass,
    CorporateAction,
    CorporateActionKind,
    Instrument,
    Market,
    SymbolMapping,
)
from openbars.normalize.instruments import (
    CORPORATE_ACTION_KIND,
    MemoryInstrumentMaster,
    UnresolvedRecord,
    assign_corporate_actions,
    ingest_corporate_actions,
)
from openbars.storage.parquet_duckdb import open_backend

A, B = "us:eq:ZZA1", "us:eq:ZZB1"
SPLIT, DIV = CorporateActionKind.SPLIT, CorporateActionKind.DIVIDEND
IN_A, IN_GAP, IN_B = date(2020, 1, 8), date(2020, 1, 13), date(2020, 1, 20)


@pytest.fixture(params=["memory", "parquet", pytest.param("postgres", marks=pytest.mark.postgres_live)])
def master(request, tmp_path):
    if request.param == "memory":
        yield from _seeded(MemoryInstrumentMaster())
    elif request.param == "parquet":
        yield from _seeded(open_backend(tmp_path / "data").instruments)
    else:  # native Postgres in a throwaway schema (skipped without OPENBARS_TEST_PG_DSN)
        from pg_helpers import throwaway_schema

        from openbars.storage.postgres import PostgresBackend

        dsn = request.getfixturevalue("pg_dsn")
        with throwaway_schema(dsn) as schema:
            yield from _seeded(PostgresBackend(dsn, mode="native", write=True, schema=schema).instruments)


def _seeded(m):
    m.upsert(
        [Instrument(A, Market.US, AssetClass.EQUITY, "USD"), Instrument(B, Market.US, AssetClass.EQUITY, "USD")],
        [
            SymbolMapping(A, "ZZZA", date(2020, 1, 1), date(2020, 1, 10)),
            SymbolMapping(B, "ZZZA", date(2020, 1, 15), None),
        ],
    )
    yield m


def act(ticker, day, kind=SPLIT, source="prov_a", **kw):
    return CorporateAction(ticker, day, kind, source, **kw)


def test_reused_ticker_actions_land_on_the_holder_of_that_date(master):
    summary = ingest_corporate_actions(
        [
            act("ZZZA", IN_A, SPLIT, ratio=D("4")),
            act("ZZZA", IN_B, DIV, amount=D("0.5")),
            act("ZZZA", IN_GAP, SPLIT, ratio=D("2")),  # between the two holders
        ],
        master,
    )
    assert (summary.resolved, summary.unresolved, summary.mapped_from_ticker, summary.already_mapped) == (2, 1, 2, 0)
    assert summary.reasons == {"symbol_reused_uncovered_date": 1}
    stored = {(a.instrument_id, a.ex_date, a.kind) for a in master.corporate_actions()}
    assert stored == {(A, IN_A, SPLIT), (B, IN_B, DIV)}  # nothing for the gap, nothing under the ticker
    assert [a for a in master.corporate_actions() if a.instrument_id == A][0].ratio == D("4")
    (queued,) = master.unresolved(CORPORATE_ACTION_KIND)
    assert (queued.symbol, queued.provider, queued.session_date, queued.detail) == ("ZZZA", "prov_a", IN_GAP, "split")
    assert queued.reason_code == "symbol_reused_uncovered_date"


def test_known_stable_id_is_unchanged(master):
    summary = ingest_corporate_actions([act(A, IN_GAP, SPLIT, ratio=D("3"))], master)  # gap date, but an id
    assert (summary.resolved, summary.unresolved, summary.already_mapped, summary.mapped_from_ticker) == (1, 0, 1, 0)
    assert [(a.instrument_id, a.ex_date) for a in master.corporate_actions()] == [(A, IN_GAP)]
    assert master.unresolved_count() == 0


def test_reingesting_an_unresolved_event_does_not_inflate_the_queue(master):
    batch = [act("ZZZA", IN_GAP, SPLIT, ratio=D("2")), act("NOPE", IN_A, DIV, amount=D("1"))]
    for _ in range(3):
        ingest_corporate_actions(batch, master)
    assert master.unresolved_count(CORPORATE_ACTION_KIND) == 2
    assert master.corporate_actions() == []
    # a split and a dividend of the same symbol/day/provider are different events
    ingest_corporate_actions([act("ZZZA", IN_GAP, DIV, amount=D("1"))], master)
    assert master.unresolved_count(CORPORATE_ACTION_KIND) == 3
    # a different source is a different row too
    ingest_corporate_actions([act("ZZZA", IN_GAP, SPLIT, source="prov_b", ratio=D("2"))], master)
    assert master.unresolved_count(CORPORATE_ACTION_KIND) == 4
    reasons = {r.symbol: r.reason_code for r in master.unresolved(CORPORATE_ACTION_KIND)}
    assert reasons["NOPE"] == "no_mapping"


def test_provider_specific_mapping_beats_generic_and_ambiguity_is_unresolved(master):
    c = "us:eq:ZZC1"
    master.upsert(
        [Instrument(c, Market.US, AssetClass.EQUITY, "USD")],
        [
            SymbolMapping(c, "ZZZA", date(2020, 1, 1), None, provider="prov_special"),
            SymbolMapping(B, "ZZZA", date(2020, 1, 1), None, provider="prov_special"),
        ],
    )
    # prov_special has two instruments on that date -> ambiguous, never guessed
    ingest_corporate_actions([act("ZZZA", IN_B, SPLIT, source="prov_special", ratio=D("2"))], master)
    assert master.corporate_actions() == []
    (q,) = master.unresolved(CORPORATE_ACTION_KIND)
    assert q.reason_code == "ambiguous_mapping"
    master.remove_mappings([SymbolMapping(B, "ZZZA", date(2020, 1, 1), None, provider="prov_special")])
    ingest_corporate_actions([act("ZZZA", IN_B, SPLIT, source="prov_special", ratio=D("2"))], master)
    assert [a.instrument_id for a in master.corporate_actions()] == [c]  # specific beats the "*" holder B


def test_unique_history_fallback_only_where_it_applies():
    m = MemoryInstrumentMaster()
    m.upsert(
        [Instrument("us:eq:U1", Market.US, AssetClass.EQUITY, "USD")],
        [SymbolMapping("us:eq:U1", "TEST1", date(2020, 1, 1), date(2020, 6, 30))],
    )
    before = act("TEST1", date(2019, 12, 1), SPLIT, ratio=D("2"))  # before the first window: fallback
    after = act("TEST1", date(2020, 7, 15), SPLIT, ratio=D("2"))  # after the closed window: no
    summary = ingest_corporate_actions([before, after], m)
    assert (summary.resolved, summary.unresolved, summary.reasons) == (1, 1, {"outside_mapping_window": 1})
    assert [a.instrument_id for a in m.corporate_actions()] == ["us:eq:U1"]


def test_unresolved_event_that_later_resolves_leaves_the_queue(master):
    gap = act("ZZZA", IN_GAP, SPLIT, ratio=D("2"))
    ingest_corporate_actions([gap], master)
    assert master.unresolved_count(CORPORATE_ACTION_KIND) == 1
    master.upsert([], [SymbolMapping(A, "ZZZA", date(2020, 1, 11), date(2020, 1, 14))])  # an operator closes the gap
    summary = ingest_corporate_actions([gap], master)
    assert (summary.resolved, summary.cleared) == (1, 1)
    assert master.unresolved_count() == 0
    assert [(a.instrument_id, a.ex_date) for a in master.corporate_actions()] == [(A, IN_GAP)]


def test_assign_without_recording_and_listing_dedupe_is_unchanged(master):
    out = assign_corporate_actions([act("ZZZA", IN_GAP, SPLIT, ratio=D("2"))], master, record=False)
    assert len(out.unresolved) == 1 and out.unresolved[0].instrument_id == "ZZZA" and out.resolved == ()
    assert master.unresolved_count() == 0
    # listings: one row per provider+symbol, whatever the date; bars: per day
    master.record_unresolved([
        UnresolvedRecord("listing", "no_mapping", "ZZZA", "p", date(2020, 1, 1)),
        UnresolvedRecord("listing", "no_mapping", "ZZZA", "p", date(2020, 1, 2), detail="x"),
        UnresolvedRecord("bar", "no_mapping", "ZZZA", "p", date(2020, 1, 1)),
        UnresolvedRecord("bar", "no_mapping", "ZZZA", "p", date(2020, 1, 2)),
    ])
    assert master.unresolved_count("listing") == 1 and master.unresolved_count("bar") == 2
