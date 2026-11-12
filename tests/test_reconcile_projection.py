"""research-daily-v2 projection cases on synthetic data (TEST1 / ZZZA)."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest
from reconcile_helpers import ALPHA, DAY, IEX, MASSIVE, SIP, TWELVE, YAHOO, obs

from openbars.core.series import SourceIdentity
from openbars.reconcile import (
    DEFAULT_POLICY,
    MemoryReconciler,
    ReconcilePolicy,
    SelectionOverride,
    is_priority_source,
    project_canonical_bars,
    source_fingerprint,
)

GOLDEN = Path(__file__).parent / "golden" / "research_daily_v2"
VOL = {"consolidated_volume", "split_adjusted_volume"}


def one(*bars, **kw):
    out = project_canonical_bars(bars, **kw)
    assert len(out) == 1
    return out[0]


def has_volume_caps(sel) -> bool:
    return VOL <= set(sel.capabilities)


def test_default_policy_matches_research_daily_v2():
    p = DEFAULT_POLICY
    assert p.policy_version == "research-daily-v2"
    assert p.price_tolerance_pct == Decimal("0.5")
    assert p.volume_tolerance_pct == Decimal("5")
    assert isinstance(p.price_tolerance_pct, Decimal)
    assert p.primary == MASSIVE
    assert p.secondary_sources == (SIP,)
    assert [f.identity for f in p.fill_sources] == [YAHOO, TWELVE]
    assert p.corroboration_quorum == 2


def test_policy_rejects_float_tolerance():
    with pytest.raises(TypeError):
        ReconcilePolicy(price_tolerance_pct=0.5)  # type: ignore[arg-type]


def test_sip_only_is_compatible_gap_fill():
    sel = one(obs(SIP, "100"))
    assert sel.selection_reason == "compatible_gap_fill"
    assert sel.quality_status == "complete"
    assert has_volume_caps(sel)
    assert sel.price_source == SIP


def test_massive_vs_disagreeing_sip_is_divergent():
    sel = one(obs(MASSIVE, "100", 1_000_000), obs(SIP, "100", 1_200_000))
    assert sel.price_source == MASSIVE
    assert sel.selection_reason == "primary_source"
    assert sel.quality_status == "divergent"
    assert not (VOL & set(sel.capabilities))
    assert "price_ohlc" in sel.capabilities


def test_massive_vs_agreeing_sip_is_complete():
    sel = one(obs(MASSIVE, "100", 1_000_000), obs(SIP, "100.3", 1_040_000))
    assert (sel.selection_reason, sel.quality_status) == ("primary_source", "complete")
    assert has_volume_caps(sel)


def test_iex_only_is_empty():
    assert project_canonical_bars([obs(IEX, "100")]) == ()


def test_iex_plus_alpha_vantage_raw_is_empty():
    assert project_canonical_bars([obs(IEX, "100"), obs(ALPHA, "100")]) == ()


def test_yfinance_only_is_fill_with_volume_caps():
    sel = one(obs(YAHOO, "100"))
    assert sel.selection_reason == "fill_yfinance"
    assert sel.quality_status == "complete"
    assert has_volume_caps(sel)
    assert "volume_basis:split_adjusted" in sel.capabilities
    assert "currency:USD" in sel.capabilities


def test_twelve_data_only_is_price_only_fill():
    sel = one(obs(TWELVE, "100"))
    assert sel.selection_reason == "fill_twelve_data"
    assert sel.quality_status == "complete"
    assert not (VOL & set(sel.capabilities))
    assert "volume_basis:unknown" in sel.capabilities
    assert "price_ohlc" in sel.capabilities


def test_yfinance_beats_twelve_data():
    sel = one(obs(TWELVE, "100"), obs(YAHOO, "100"))
    assert sel.price_source == YAHOO
    assert sel.selection_reason == "fill_yfinance"


def test_fills_never_replace_sip():
    sel = one(obs(SIP, "100"), obs(YAHOO, "100"), obs(TWELVE, "100"))
    assert sel.price_source == SIP
    assert sel.selection_reason == "compatible_gap_fill"


def test_massive_sip_yahoo_picks_massive_and_without_massive_picks_sip():
    all3 = [obs(MASSIVE, "100"), obs(SIP, "100"), obs(YAHOO, "100")]
    assert one(*all3).price_source == MASSIVE
    assert one(*all3[1:]).price_source == SIP


def test_is_priority_source():
    assert is_priority_source(MASSIVE) and is_priority_source(SIP)
    assert not any(is_priority_source(s) for s in (YAHOO, IEX, TWELVE))


# --- quorum ------------------------------------------------------------------------------


def test_two_independents_agreeing_with_primary_corroborate():
    sel = one(
        obs(MASSIVE, "100", 1_000_000),
        obs(SIP, "100", 1_200_000),
        obs(YAHOO, "100", 1_000_000),
        obs(IEX, "100", 1_000_000),
    )
    assert sel.price_source == MASSIVE
    assert sel.selection_reason == "primary_source_corroborated"
    assert sel.quality_status == "complete"
    assert has_volume_caps(sel)


def test_one_independent_is_not_enough():
    sel = one(
        obs(MASSIVE, "100", 1_000_000),
        obs(SIP, "100", 1_200_000),
        obs(YAHOO, "100", 1_000_000),
    )
    assert (sel.selection_reason, sel.quality_status) == ("primary_source", "divergent")
    assert not (VOL & set(sel.capabilities))


def test_independents_agreeing_with_secondary_are_divergent():
    sel = one(
        obs(MASSIVE, "100", 1_000_000),
        obs(SIP, "100", 1_200_000),
        obs(YAHOO, "100", 1_200_000),
        obs(IEX, "100", 1_200_000),
    )
    assert (sel.selection_reason, sel.quality_status) == ("primary_source", "divergent")


def test_independent_agreeing_with_both_sides_blocks_corroboration():
    # Within tolerance of both primary and secondary price-wise, volume sits between.
    sel = one(
        obs(MASSIVE, "100", 1_000_000),
        obs(SIP, "100", 1_080_000),
        obs(YAHOO, "100", 1_000_000),
        obs(IEX, "100", 1_040_000),
        obs(TWELVE, "100", 1_040_000),
    )
    assert sel.quality_status == "divergent"


def test_quorum_is_configurable():
    policy = ReconcilePolicy(corroboration_quorum=1)
    sel = one(
        obs(MASSIVE, "100", 1_000_000),
        obs(SIP, "100", 1_200_000),
        obs(YAHOO, "100", 1_000_000),
        policy=policy,
    )
    assert sel.selection_reason == "primary_source_corroborated"


# --- volume_missing ----------------------------------------------------------------------


def test_zero_volume_on_moved_sip_bar_is_volume_missing():
    sel = one(obs(SIP, "100", 0))
    assert sel.selection_reason == "compatible_gap_fill"
    assert sel.quality_status == "volume_missing"
    assert not (VOL & set(sel.capabilities))


def test_flat_zero_volume_massive_bar_stays_complete():
    sel = one(obs(MASSIVE, "100", 0, flat=True))
    assert sel.quality_status == "complete"
    assert has_volume_caps(sel)


def test_divergent_plus_zero_volume_stays_divergent():
    sel = one(obs(MASSIVE, "100", 0), obs(SIP, "100", 1_000_000))
    assert sel.quality_status == "divergent"


# --- fill vs gap-fill conflicts ------------------------------------------------------------


def test_sip_gap_fill_with_yahoo_close_disagreement_is_divergent():
    sel = one(obs(SIP, "100"), obs(YAHOO, "101"))
    assert sel.selection_reason == "compatible_gap_fill"
    assert sel.quality_status == "divergent"
    assert not (VOL & set(sel.capabilities))


def test_sip_gap_fill_close_within_tolerance_stays_complete():
    sel = one(obs(SIP, "100"), obs(YAHOO, "100.4"))
    assert sel.quality_status == "complete"
    assert has_volume_caps(sel)


def test_volume_only_yahoo_difference_is_not_a_conflict():
    sel = one(obs(SIP, "100", 1_000_000), obs(YAHOO, "100", 3_000_000))
    assert sel.quality_status == "complete"


def test_twelve_data_close_disagreement_flags_sip_gap_fill():
    sel = one(obs(SIP, "100"), obs(TWELVE, "103"))
    assert sel.quality_status == "divergent"


def test_massive_primary_not_divergent_because_yahoo_disagrees():
    sel = one(obs(MASSIVE, "100"), obs(YAHOO, "130"))
    assert (sel.selection_reason, sel.quality_status) == ("primary_source", "complete")


def test_stale_split_looking_day_is_divergent_neighbours_are_not():
    days = [date(2024, 1, 3), date(2024, 1, 4), date(2024, 1, 5), date(2024, 1, 8)]
    bars = [
        obs(SIP, "7.30", day=days[0]), obs(YAHOO, "7.31", day=days[0]),
        obs(SIP, "0.0698", day=days[1]), obs(YAHOO, "7.32", day=days[1]),
        obs(SIP, "7.35", day=days[2]), obs(YAHOO, "7.34", day=days[2]),
        obs(YAHOO, "7.40", day=days[3]),
    ]  # fmt: skip
    out = {s.session_date: s for s in project_canonical_bars(bars)}
    assert out[days[0]].quality_status == "complete"
    assert out[days[1]].quality_status == "divergent"
    assert out[days[2]].quality_status == "complete"
    assert out[days[3]].selection_reason == "fill_yfinance"
    assert out[days[3]].quality_status == "complete"


# --- whole-bar selection -------------------------------------------------------------------


def test_volume_is_never_taken_from_a_different_feed():
    # volume_priority prefers SIP, yet the selected bar is massive: volume stays massive's.
    policy = ReconcilePolicy(volume_priority=(SIP, MASSIVE))
    massive = obs(MASSIVE, "100", 1_000_000)
    sel = one(massive, obs(SIP, "100", 1_200_000), policy=policy)
    assert sel.selected == massive
    assert sel.selected.volume == Decimal(1_000_000)
    assert sel.price_source == sel.volume_source == MASSIVE
    row = sel.to_row(generated_at=datetime(2024, 1, 3, 23, tzinfo=timezone.utc))
    assert (row.provider, row.feed, row.volume) == ("massive", "eod", Decimal(1_000_000))


def test_latest_revision_per_source_wins():
    sel = one(
        obs(MASSIVE, "100", revision=1, source_bar_id="7"),
        obs(MASSIVE, "101", revision=2, source_bar_id="3"),
    )
    assert sel.selected.revision == 2
    assert sel.selected.close == Decimal("101")


def test_missing_source_bar_id_counts_as_zero_for_ties():
    sel = one(
        obs(MASSIVE, "100", revision=1, source_bar_id=None),
        obs(MASSIVE, "101", revision=1, source_bar_id="5"),
    )
    assert sel.selected.close == Decimal("101")


def test_sources_seen_lists_every_identity():
    sel = one(obs(SIP, "100"), obs(MASSIVE, "100"), obs(IEX, "100"))
    assert sel.sources_seen == (MASSIVE, SIP, IEX)


def test_observation_rejects_float_prices():
    with pytest.raises(TypeError):
        replace(obs(MASSIVE, "100"), close=100.0)  # type: ignore[arg-type]


def test_instrument_id_falls_back_to_symbol():
    bar = replace(obs(MASSIVE, "100"), instrument_id=None, symbol="ZZZA")
    assert project_canonical_bars([bar])[0].instrument_id == "ZZZA"


# --- overrides -----------------------------------------------------------------------------


def test_replace_bar_override():
    bars = [obs(MASSIVE, "16", 1_000_000), obs(SIP, "10", 1_000_000)]
    ov = {("TEST1", DAY): SelectionOverride("replace_bar", "alpaca", "sip", "split")}
    sel = one(*bars, overrides=ov)
    assert sel.selection_reason == "spike_replaced"
    assert sel.quality_status == "complete"
    assert sel.price_source == SIP
    assert sel.selected.close == Decimal("10")


def test_replace_bar_with_zero_volume_becomes_volume_missing():
    bars = [obs(MASSIVE, "16"), obs(SIP, "10", 0)]
    ov = {("TEST1", DAY): SelectionOverride("replace_bar", "alpaca", "sip", "split")}
    assert one(*bars, overrides=ov).quality_status == "volume_missing"


def test_mark_suspect_override_unless_divergent():
    ov = {("TEST1", DAY): SelectionOverride("mark_suspect")}
    assert one(obs(MASSIVE, "16"), overrides=ov).quality_status == "spike_suspect"
    divergent = one(obs(MASSIVE, "100", 1_000_000), obs(SIP, "100", 1_200_000), overrides=ov)
    assert divergent.quality_status == "divergent"


def test_override_for_absent_source_is_ignored():
    ov = {("TEST1", DAY): SelectionOverride("replace_bar", "alpaca", "sip", "split")}
    assert one(obs(MASSIVE, "16"), overrides=ov).selection_reason == "primary_source"


def test_override_validation():
    with pytest.raises(ValueError):
        SelectionOverride("replace_bar")
    with pytest.raises(ValueError):
        SelectionOverride("nope")


# --- fingerprint, outputs, golden ----------------------------------------------------------


def test_source_fingerprint_matches_documented_hash():
    payload = '["TEST1","2024-01-03","42",2,"research-daily-v2"]'
    expected = hashlib.sha256(payload.encode()).hexdigest()
    assert source_fingerprint("TEST1", DAY, "42", 2, "research-daily-v2") == expected
    null_payload = '["TEST1","2024-01-03",null,1,"research-daily-v2"]'
    assert source_fingerprint("TEST1", DAY, None, 1, "research-daily-v2") == (
        hashlib.sha256(null_payload.encode()).hexdigest()
    )
    assert source_fingerprint("TEST1", DAY, "42", 3, "research-daily-v2") != expected


def test_every_output_exposes_sources_via_selection_and_audit():
    rec = MemoryReconciler()
    result = rec.reconcile_range(
        "TEST1", DAY, DAY, [obs(MASSIVE, "100"), obs(SIP, "100"), obs(YAHOO, "100")]
    )
    (audit,) = result.audit
    assert audit.instrument_id == "TEST1" and audit.session_date == DAY
    assert audit.rule == "primary_source" and audit.quality_status == "complete"
    assert audit.price_source == MASSIVE
    assert audit.volume_source == audit.price_source
    assert audit.sources_seen == (MASSIVE, SIP, YAHOO)
    assert audit.revised is False
    (row,) = result.canonical
    assert (row.provider, row.feed, row.adjustment) == ("massive", "eod", "split")


def _snapshot(sel) -> dict:
    return {
        "selection_reason": sel.selection_reason,
        "quality_status": sel.quality_status,
        "price_source": sel.price_source.key,
        "volume_source": sel.volume_source.key,
        "capabilities": list(sel.capabilities),
        "sources_seen": [s.key for s in sel.sources_seen],
    }


def _golden_cases():
    path = GOLDEN / "projection_cases.json"
    return json.loads(path.read_text())


@pytest.mark.parametrize("case", _golden_cases(), ids=lambda c: c["name"])
def test_golden_projection_cases(case):
    bars = [
        obs(SourceIdentity.parse(b["source"]), b["close"], b["volume"], instrument_id="ZZZA")
        for b in case["bars"]
    ]
    out = project_canonical_bars(bars)
    assert [_snapshot(s) for s in out] == [case["expected"]]
