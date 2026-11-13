"""R1-R3 data-fix rules on synthetic instruments (TEST1 / ZZZA / TEST2)."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from openbars.core.models import AssetClass, Instrument, Market
from openbars.core.series import CanonicalDailyRow
from openbars.normalize.instruments import MemoryInstrumentMaster
from openbars.reconcile import rules
from openbars.reconcile.rules import (
    ACTION_CLASSIFY,
    ACTION_CLOSE_ALIAS,
    ACTION_MERGE,
    BREAKER_CAPS,
    DUPLICATE_BASIS,
    PLACEHOLDER_BASIS,
    SEC_PLACEHOLDER_SOURCE,
    AliasSnapshot,
    TickerTypeObservation,
    plan_r1,
    plan_r2,
    plan_r3,
    r3_apply_enabled,
    run_r1,
    run_r2,
    run_r3,
    security_type_of,
)
from openbars.core.models import SymbolMapping

OBS_DAY = date(2024, 6, 3)
T0 = date(2020, 1, 1)


def inst(iid: str, security_type: str | None = None, asset_class=AssetClass.EQUITY) -> Instrument:
    ext = {} if security_type is None else {"security_type": security_type}
    return Instrument(iid, Market.US, asset_class, "USD", external_ids=ext)


def alias(iid, symbol="TEST1", exchange=None, valid_from=T0, **kw) -> AliasSnapshot:
    return AliasSnapshot(iid, symbol, valid_from, exchange=exchange, **kw)


def ob(symbol="TEST1", exchange="XNAS", typ="CS", on=OBS_DAY) -> TickerTypeObservation:
    return TickerTypeObservation(symbol, exchange, typ, on)


def master(instruments, aliases) -> MemoryInstrumentMaster:
    m = MemoryInstrumentMaster()
    m.upsert(
        instruments,
        [SymbolMapping(a.instrument_id, a.symbol, a.valid_from, a.valid_to) for a in aliases],
    )
    return m


# --- R1 -------------------------------------------------------------------------------------


def r1_world():
    instruments = [inst("us:eq:U1", "unknown")]
    aliases = [alias("us:eq:U1", "TEST1", "XNAS")]
    return instruments, aliases


def test_r1_classifies_unique_unknown_instrument():
    instruments, aliases = r1_world()
    plan = plan_r1(instruments, aliases, [ob(typ="CS")])
    assert plan.status == "ok" and not plan.breaker_tripped
    (change,) = plan.changes
    assert change.action == ACTION_CLASSIFY
    assert change.instrument_id == "us:eq:U1"
    assert change.security_type == "common_stock"
    assert change.asset_class is AssetClass.EQUITY


@pytest.mark.parametrize("obs_exchange,alias_exchange", [("", "XNAS"), ("UNKNOWN", "XNAS"),
                                                         (None, "XNAS"), ("XNAS", None), ("XNAS", "")])
def test_r1_wildcard_exchange_matches(obs_exchange, alias_exchange):
    plan = plan_r1([inst("us:eq:U1", "unknown")], [alias("us:eq:U1", exchange=alias_exchange)],
                   [ob(exchange=obs_exchange)])
    assert len(plan.changes) == 1


def test_r1_exchange_mismatch_does_not_match():
    plan = plan_r1([inst("us:eq:U1", "unknown")], [alias("us:eq:U1", exchange="XNYS")],
                   [ob(exchange="XNAS")])
    assert plan.changes == ()


def test_r1_ambiguous_candidates_skipped():
    instruments = [inst("us:eq:U1", "unknown"), inst("us:eq:U2", "unknown")]
    aliases = [alias("us:eq:U1", exchange="XNAS"), alias("us:eq:U2", exchange=None)]
    plan = plan_r1(instruments, aliases, [ob()])
    assert plan.changes == ()
    assert plan.skipped["ambiguousCandidates"] == 1


def test_r1_ambiguous_instrument_types_skipped():
    instruments, aliases = r1_world()
    plan = plan_r1(instruments, aliases, [ob(typ="CS"), ob(typ="ETF")])
    assert plan.changes == ()
    assert plan.skipped["ambiguousInstrumentTypes"] == 1


def test_r1_same_mapped_type_from_different_raw_codes_is_fine():
    instruments, aliases = r1_world()
    plan = plan_r1(instruments, aliases, [ob(typ="ADRC"), ob(typ="ADRP")])
    (change,) = plan.changes
    assert change.security_type == "adr" and change.asset_class is AssetClass.ADR


def test_r1_unmapped_type_skipped():
    instruments, aliases = r1_world()
    plan = plan_r1(instruments, aliases, [ob(typ="ZZ")])
    assert plan.changes == ()
    assert plan.skipped["unmappedType:ZZ"] == 1


def test_r1_no_observations_needs_first_weekly_pull():
    instruments, aliases = r1_world()
    plan = plan_r1(instruments, aliases, [])
    assert plan.status == "needs_first_weekly_pull"
    assert plan.changes == ()


def test_r1_uses_only_latest_observation_day():
    instruments, aliases = r1_world()
    old = ob(typ="ETF", on=date(2024, 5, 1))
    plan = plan_r1(instruments, aliases, [old, ob(typ="CS")])
    (change,) = plan.changes
    assert change.security_type == "common_stock"


def test_r1_ignores_typed_instruments():
    plan = plan_r1([inst("us:eq:T1")], [alias("us:eq:T1", exchange="XNAS")], [ob()])
    assert plan.changes == ()


def test_r1_type_without_asset_class_keeps_asset_class():
    instruments, aliases = r1_world()
    store = master(instruments, aliases)
    out = run_r1(instruments, aliases, [ob(typ="PFD")], mode="apply", store=store)
    (change,) = out.applied_changes
    assert change.security_type == "preferred" and change.asset_class is None
    updated = store.get("us:eq:U1")
    assert updated.asset_class is AssetClass.EQUITY
    assert updated.external_ids["security_type"] == "preferred"


def test_r1_preview_does_not_upsert():
    instruments, aliases = r1_world()
    store = master(instruments, aliases)
    out = run_r1(instruments, aliases, [ob(typ="ETF")], store=store)
    assert out.mode == "preview" and not out.applied
    assert store.get("us:eq:U1").external_ids["security_type"] == "unknown"
    assert store.get("us:eq:U1").asset_class is AssetClass.EQUITY


def test_r1_apply_upserts():
    instruments, aliases = r1_world()
    store = master(instruments, aliases)
    out = run_r1(instruments, aliases, [ob(typ="ETF")], mode="apply", store=store)
    assert out.applied
    got = store.get("us:eq:U1")
    assert got.asset_class is AssetClass.ETF
    assert got.external_ids["security_type"] == "etf"
    assert security_type_of(got) == "etf"


def test_r1_breaker_trips_and_apply_is_noop():
    instruments = [inst(f"us:eq:U{n}", "unknown") for n in range(3)]
    aliases = [alias(f"us:eq:U{n}", f"ZZ{n}", "XNAS") for n in range(3)]
    obs = [ob(f"ZZ{n}") for n in range(3)]
    store = master(instruments, aliases)
    plan = plan_r1(instruments, aliases, obs, cap=2)
    assert plan.breaker_tripped and plan.breaker_cap == 2 and plan.status == "breaker_tripped"
    out = run_r1(instruments, aliases, obs, mode="apply", store=store, cap=2)
    assert out.aborted and not out.applied and out.abort_reason
    assert all(i.external_ids["security_type"] == "unknown" for i in store.instruments())


def test_r1_at_cap_does_not_trip():
    instruments = [inst(f"us:eq:U{n}", "unknown") for n in range(2)]
    aliases = [alias(f"us:eq:U{n}", f"ZZ{n}", "XNAS") for n in range(2)]
    plan = plan_r1(instruments, aliases, [ob(f"ZZ{n}") for n in range(2)], cap=2)
    assert not plan.breaker_tripped


def test_security_type_derivation():
    assert security_type_of(inst("a")) == "common_stock"
    assert security_type_of(inst("a", asset_class=AssetClass.ADR)) == "adr"
    assert security_type_of(inst("a", asset_class=AssetClass.INDEX)) == "unknown"
    assert security_type_of(inst("a", "unknown")) == "unknown"


# --- R2 -------------------------------------------------------------------------------------


def r2_world(placeholder_source=SEC_PLACEHOLDER_SOURCE, placeholder_from=date(2023, 5, 1)):
    instruments = [inst("us:eq:P1", "unknown"), inst("us:eq:T1", "common_stock")]
    aliases = [
        alias("us:eq:P1", "TEST1", None, placeholder_from, source=placeholder_source, alias_id=7),
        alias("us:eq:T1", "TEST1", "XNAS"),
    ]
    return instruments, aliases


def test_r2_closes_placeholder_without_bars():
    instruments, aliases = r2_world()
    store = master(instruments, aliases)
    out = run_r2(instruments, aliases, mode="apply", store=store, has_bars={"us:eq:T1"})
    assert out.applied
    (change,) = out.applied_changes
    assert change.action == ACTION_CLOSE_ALIAS and change.alias_id == 7
    assert change.details["valid_to"] == date(2023, 5, 1)
    assert change.details["valid_to_basis"] == PLACEHOLDER_BASIS == "superseded_placeholder"
    closed = [m for m in store.mappings() if m.instrument_id == "us:eq:P1"]
    assert [(m.valid_from, m.valid_to) for m in closed] == [(date(2023, 5, 1), date(2023, 5, 1))]
    typed = [m for m in store.mappings() if m.instrument_id == "us:eq:T1"]
    assert typed[0].valid_to is None


def test_r2_adr_counts_as_typed_holder():
    instruments, aliases = r2_world()
    instruments[1] = inst("us:eq:T1", asset_class=AssetClass.ADR)
    assert len(plan_r2(instruments, aliases).changes) == 1


def test_r2_skips_when_unknown_instrument_has_bars():
    instruments, aliases = r2_world()
    plan = plan_r2(instruments, aliases, has_bars={"us:eq:P1"})
    assert plan.changes == ()
    assert plan.skipped["hasBars"] == 1
    assert plan_r2(instruments, aliases, has_bars=lambda i: i == "us:eq:P1").changes == ()


def test_r2_does_not_close_non_placeholder_alias():
    instruments, aliases = r2_world(placeholder_source="listing")
    assert plan_r2(instruments, aliases).changes == ()


def test_r2_does_not_close_already_closed_alias():
    instruments, aliases = r2_world()
    aliases[0] = alias("us:eq:P1", "TEST1", None, date(2023, 5, 1),
                       source=SEC_PLACEHOLDER_SOURCE, valid_to=date(2023, 6, 1))
    assert plan_r2(instruments, aliases).changes == ()


def test_r2_requires_typed_holder_and_unknown_instrument():
    instruments, aliases = r2_world()
    assert plan_r2(instruments[:1], aliases[:1]).changes == ()
    # a typed (non-unknown) instrument carrying a placeholder alias is left alone
    typed_placeholder = [inst("us:eq:P1", "common_stock"), instruments[1]]
    assert plan_r2(typed_placeholder, aliases).changes == ()
    # a typed holder on another symbol does not count
    other = [aliases[0], alias("us:eq:T1", "ZZZA", "XNAS")]
    assert plan_r2(instruments, other).changes == ()


def test_r2_preview_is_noop():
    instruments, aliases = r2_world()
    store = master(instruments, aliases)
    before = store.mappings()
    out = run_r2(instruments, aliases, store=store)
    assert not out.applied and len(out.plan.changes) == 1
    assert store.mappings() == before


def test_r2_breaker():
    instruments, aliases = r2_world()
    store = master(instruments, aliases)
    assert BREAKER_CAPS["R2"] == 500
    out = run_r2(instruments, aliases, mode="apply", store=store, cap=0)
    assert out.aborted and not out.applied
    assert all(m.valid_to is None for m in store.mappings())


# --- R3 -------------------------------------------------------------------------------------

WIN_FROM = date(2022, 1, 3)
LOSE_FROM = date(2021, 1, 4)
GEN = datetime(2024, 6, 1, tzinfo=timezone.utc)


def r3_world(*, loser_from=LOSE_FROM):
    instruments = [inst("us:eq:W", "common_stock"), inst("us:eq:L", "common_stock")]
    aliases = [
        alias("us:eq:W", "TEST1", "XNAS", WIN_FROM),
        alias("us:eq:L", "TEST1", "XNYS", loser_from),
    ]
    return instruments, aliases


def row(iid, day, *, provider="massive", feed="eod", adjustment="split", revision=1):
    px = Decimal("10")
    return CanonicalDailyRow(
        instrument_id=iid, session_date=day, policy_version="research-daily-v2",
        selected_source_bar_id=None, provider=provider, feed=feed, adjustment=adjustment,
        revision=revision, open=px, high=px, low=px, close=px, volume=Decimal(1),
        selection_reason="primary_source", quality_status="complete", capabilities=(),
        source_fingerprint="x", generated_at=GEN,
    )  # fmt: skip


def test_r3_plans_merge_for_unique_authority():
    instruments, aliases = r3_world()
    plan = plan_r3(instruments, aliases, [ob(exchange="XNAS", typ="CS")])
    (change,) = plan.changes
    assert change.action == ACTION_MERGE
    assert change.instrument_id == "us:eq:L"
    assert change.winner_instrument_id == "us:eq:W"
    assert not plan.skipped


def test_r3_ignores_untyped_observations_for_authority():
    instruments, aliases = r3_world()
    obs = [ob(exchange="XNAS", typ="CS"), ob(exchange="XNYS", typ="WARRANT")]
    assert len(plan_r3(instruments, aliases, obs).changes) == 1


def test_r3_single_holder_is_not_a_duplicate():
    instruments, aliases = r3_world()
    plan = plan_r3(instruments[:1], aliases[:1], [ob()])
    assert plan.changes == () and not plan.skipped


@pytest.mark.parametrize(
    "obs",
    [
        [],
        [ob(typ="WARRANT")],
        [ob(exchange="XNAS"), ob(exchange="XNYS")],
    ],
)
def test_r3_authority_missing_or_ambiguous(obs):
    instruments, aliases = r3_world()
    plan = plan_r3(instruments, aliases, obs)
    assert plan.changes == ()
    assert plan.skipped["authorityMissingOrAmbiguous"] == 1


def test_r3_no_exact_match():
    instruments, aliases = r3_world()
    plan = plan_r3(instruments, aliases, [ob(exchange="ARCX")])
    assert plan.changes == () and plan.skipped["noExactMatch"] == 1


def test_r3_multiple_matches():
    instruments = [inst("us:eq:A", "common_stock"), inst("us:eq:B", "adr"), inst("us:eq:C")]
    aliases = [alias(i, "TEST1", "XNAS") for i in ("us:eq:A", "us:eq:B")] + [
        alias("us:eq:C", "TEST1", "XNYS")
    ]
    plan = plan_r3(instruments, aliases, [ob(exchange="XNAS")])
    assert plan.changes == () and plan.skipped["multipleMatches"] == 1


def test_r3_loser_with_references_skipped():
    instruments, aliases = r3_world()
    plan = plan_r3(instruments, aliases, [ob()], has_other_references=lambda i: i == "us:eq:L")
    assert plan.changes == () and plan.skipped["loserHasReferences"] == 1


def test_r3_ignores_closed_aliases_and_unknown_instruments():
    instruments, aliases = r3_world()
    closed = [aliases[0], alias("us:eq:L", "TEST1", "XNYS", LOSE_FROM, valid_to=date(2021, 6, 1))]
    assert plan_r3(instruments, closed, [ob()]).changes == ()
    instruments[1] = inst("us:eq:L", "unknown")
    assert plan_r3(instruments, aliases, [ob()]).changes == ()


def test_r3_preview_and_default_apply_do_not_mutate(monkeypatch):
    monkeypatch.delenv("DATA_FIX_R3_APPLY", raising=False)
    instruments, aliases = r3_world()
    store = master(instruments, aliases)
    bars = [row("us:eq:L", date(2021, 2, 1))]
    before_maps, before_bars = store.mappings(), list(bars)
    preview = run_r3(instruments, aliases, [ob()], store=store, bars=bars)
    assert not preview.applied and len(preview.plan.changes) == 1
    default_apply = run_r3(instruments, aliases, [ob()], mode="apply", store=store, bars=bars)
    assert not default_apply.applied and default_apply.report_only
    assert store.mappings() == before_maps and bars == before_bars


def test_r3_apply_enabled_helper():
    assert not r3_apply_enabled(env={})
    assert r3_apply_enabled(env={"DATA_FIX_R3_APPLY": "1"})
    assert not r3_apply_enabled(env={"DATA_FIX_R3_APPLY": "0"})
    assert r3_apply_enabled(True, env={}) and not r3_apply_enabled(False, env={"DATA_FIX_R3_APPLY": "1"})


def test_r3_env_enables_apply(monkeypatch):
    monkeypatch.setenv("DATA_FIX_R3_APPLY", "1")
    instruments, aliases = r3_world()
    store = master(instruments, aliases)
    out = run_r3(instruments, aliases, [ob()], mode="apply", store=store)
    assert out.applied and not out.report_only


def test_r3_apply_closes_loser_extends_winner_and_moves_bars():
    instruments, aliases = r3_world()
    store = master(instruments, aliases)
    moved_a, moved_b = date(2021, 2, 1), date(2021, 3, 1)
    conflict = date(2022, 2, 1)
    bars = [
        row("us:eq:L", moved_a), row("us:eq:L", moved_b),
        row("us:eq:L", conflict), row("us:eq:W", conflict),
        row("us:eq:L", date(2021, 4, 1), provider="alpaca"),  # different provider: no conflict
    ]  # fmt: skip
    out = run_r3(instruments, aliases, [ob()], mode="apply", store=store, bars=bars, apply_r3=True)
    assert out.applied
    (change,) = out.applied_changes
    assert change.details["valid_to_basis"] == DUPLICATE_BASIS == "superseded_duplicate"
    assert change.details["movedBars"] == 3 and change.details["conflictingBars"] == 1
    assert change.details["movedFrom"] == moved_a
    assert change.details["movedTo"] == date(2021, 4, 1)
    assert change.details["mappingsClosed"] == 1
    assert change.details["winnerValidFrom"] == LOSE_FROM

    maps = {m.instrument_id: m for m in store.mappings()}
    assert (maps["us:eq:L"].valid_from, maps["us:eq:L"].valid_to) == (LOSE_FROM, LOSE_FROM)
    assert (maps["us:eq:W"].valid_from, maps["us:eq:W"].valid_to) == (LOSE_FROM, None)

    on_loser = [r.session_date for r in bars if r.instrument_id == "us:eq:L"]
    on_winner = sorted(r.session_date for r in bars if r.instrument_id == "us:eq:W")
    assert on_loser == [conflict]
    assert on_winner == sorted([moved_a, moved_b, conflict, date(2021, 4, 1)])
    assert len(bars) == 5


def test_r3_winner_not_extended_when_loser_started_later():
    instruments, aliases = r3_world(loser_from=date(2023, 1, 2))
    store = master(instruments, aliases)
    out = run_r3(instruments, aliases, [ob()], mode="apply", store=store, apply_r3=True)
    (change,) = out.applied_changes
    assert "winnerValidFrom" not in change.details
    maps = {m.instrument_id: m for m in store.mappings()}
    assert maps["us:eq:W"].valid_from == WIN_FROM


def test_r3_breaker_trips_and_apply_is_noop():
    instruments, aliases = r3_world()
    store = master(instruments, aliases)
    bars = [row("us:eq:L", date(2021, 2, 1))]
    plan = plan_r3(instruments, aliases, [ob()], cap=0)
    assert plan.breaker_tripped
    out = run_r3(instruments, aliases, [ob()], mode="apply", store=store, bars=bars,
                 apply_r3=True, cap=0)
    assert out.aborted and not out.applied
    assert bars[0].instrument_id == "us:eq:L"
    assert all(m.valid_to is None for m in store.mappings())
    assert BREAKER_CAPS["R3"] == 50


def test_plan_functions_are_side_effect_free():
    instruments, aliases = r3_world()
    store = master(instruments, aliases)
    before = store.mappings()
    plan_r3(instruments, aliases, [ob()])
    plan_r2(instruments, aliases)
    plan_r1(instruments, aliases, [ob()])
    assert store.mappings() == before


def test_invalid_mode_rejected():
    instruments, aliases = r3_world()
    with pytest.raises(ValueError):
        run_r1(instruments, aliases, [], mode="nope")
    with pytest.raises(ValueError):
        run_r2(instruments, aliases, mode="nope")
    with pytest.raises(ValueError):
        run_r3(instruments, aliases, [], mode="nope")


def test_frozen_caps_and_constants_unchanged():
    assert dict(rules.BREAKER_CAPS) == {"R1": 15000, "R2": 500, "R3": 50, "R4": 200}
    assert rules.TYPED_STOCKS == frozenset({"common_stock", "adr"})
