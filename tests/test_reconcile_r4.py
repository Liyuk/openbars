"""R4 spike planning on synthetic series (TEST1)."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from reconcile_helpers import DAY, MASSIVE, SIP, YAHOO, obs

from openbars.calendars.xnys import XNYSCalendar
from openbars.core.series import CanonicalDailyRow
from openbars.reconcile import project_canonical_bars
from openbars.reconcile import rules
from openbars.reconcile.rules import plan_r4, run_r4

GEN = datetime(2024, 6, 1, tzinfo=timezone.utc)
SESSIONS = XNYSCalendar().sessions(date(2024, 1, 2), date(2024, 3, 29))
SPIKE_DAY = SESSIONS[25]


def make_rows(closes: dict[date, str], *, volume: int = 1_000_000, instrument_id="TEST1", days=None):
    days = days or SESSIONS[:30]
    rows = []
    for d in days:
        close = Decimal(closes.get(d, "10"))
        rows.append(
            CanonicalDailyRow(
                instrument_id=instrument_id, session_date=d, policy_version="research-daily-v2",
                selected_source_bar_id=None, provider="massive", feed="eod", adjustment="split",
                revision=1, open=close, high=close, low=close, close=close,
                volume=Decimal(closes.get(("v", d), volume)),
                selection_reason="primary_source", quality_status="complete",
                capabilities=(), source_fingerprint="x", generated_at=GEN,
            )
        )  # fmt: skip
    return rows


BASE = {SPIKE_DAY: "16", SESSIONS[26]: "10.5"}


def plan(rows, others, **kw):
    return plan_r4(rows, others, since=SESSIONS[0], until=SESSIONS[-1], **kw)


def test_frozen_parameters():
    assert rules.SPIKE_MOVE == Decimal("0.50") and rules.SPIKE_REVERT == Decimal("0.15")
    assert rules.CORROBORATION_TOLERANCE == Decimal("0.005")
    assert rules.VOLUME_MULTIPLE == Decimal("2.0") and rules.VOLUME_LOOKBACK == 20
    assert rules.MAX_NEIGHBOUR_GAP_DAYS == 5
    assert dict(rules.BREAKER_CAPS) == {"R1": 15000, "R2": 500, "R3": 50, "R4": 200}
    assert rules.R4_SCOPE_TYPES == {"common_stock", "adr", "etf"}


def test_secondary_near_previous_close_replaces_bar():
    p = plan(make_rows(BASE), [obs(SIP, "10.02", day=SPIKE_DAY)])
    (d,) = p.decisions
    assert (d.instrument_id, d.session_date, d.outcome) == ("TEST1", SPIKE_DAY, "replace_bar")
    assert d.replacement == SIP
    assert p.changes == (d,)


def test_secondary_agreeing_with_spike_is_corroborated_and_not_counted():
    p = plan(make_rows(BASE), [obs(SIP, "16.01", day=SPIKE_DAY)])
    (d,) = p.decisions
    assert d.outcome == "corroborated"
    assert p.changes == () and p.planned_change_count == 0


def test_conflicting_sources_are_skipped():
    p = plan(make_rows(BASE), [obs(SIP, "16", day=SPIKE_DAY), obs(YAHOO, "10", day=SPIKE_DAY)])
    (d,) = p.decisions
    assert (d.outcome, d.skip_reason) == ("skipped", "sourcesConflict")


def test_no_other_source_low_volume_marks_suspect():
    p = plan(make_rows(BASE, volume=1_000_000), [])
    (d,) = p.decisions
    assert d.outcome == "mark_suspect"


def test_high_volume_spike_is_skipped():
    rows = make_rows({**BASE, ("v", SPIKE_DAY): 2_500_000})
    (d,) = plan(rows, []).decisions
    assert (d.outcome, d.skip_reason) == ("skipped", "highVolumeSpike")


def test_neighbour_gap_over_five_days_is_skipped():
    base = list(SESSIONS[:24])
    last = base[-1]

    def decisions(next_gap: int):
        spike = last + timedelta(days=1)
        nxt = spike + timedelta(days=next_gap)
        rows = make_rows({spike: "16", nxt: "10.5"}, days=[*base, spike, nxt])
        return plan(rows, []).decisions

    assert len(decisions(1)) == 1  # control: neighbours adjacent
    assert len(decisions(5)) == 1  # boundary: exactly 5 days is allowed
    assert decisions(6) == ()  # next neighbour too far
    far_prev = last + timedelta(days=7)
    spike = far_prev + timedelta(days=7)
    rows = make_rows({spike: "16", spike + timedelta(days=1): "10.5"},
                     days=[*base, far_prev, spike, spike + timedelta(days=1)])  # fmt: skip
    assert plan(rows, []).decisions == ()  # previous neighbour too far


def test_not_a_spike_when_price_does_not_revert_or_move_is_small():
    assert plan(make_rows({SPIKE_DAY: "16", SESSIONS[26]: "16"}), []).decisions == ()
    assert plan(make_rows({SPIKE_DAY: "14", SESSIONS[26]: "10.5"}), []).decisions == ()


def test_needs_volume_lookback():
    rows = make_rows({SESSIONS[10]: "16", SESSIONS[11]: "10.5"})
    assert plan(rows, []).decisions == ()


def test_window_and_already_evaluated_filters():
    rows = make_rows(BASE)
    assert plan_r4(rows, [], since=SPIKE_DAY + timedelta(days=1), until=SESSIONS[-1]).decisions == ()
    assert plan(rows, [], already_evaluated=[("TEST1", SPIKE_DAY)]).decisions == ()


def test_asset_class_scope():
    rows = make_rows(BASE)
    assert plan(rows, [], asset_classes={"TEST1": "fund"}).decisions == ()
    assert len(plan(rows, [], asset_classes={"TEST1": "etf"}).decisions) == 1
    assert len(plan(rows, [], asset_classes={"OTHER": "fund"}).decisions) == 1


def test_default_mode_is_preview_and_applies_nothing():
    out = run_r4(make_rows(BASE), [obs(SIP, "10.02", day=SPIKE_DAY)], since=SESSIONS[0], until=SESSIONS[-1])
    assert out.mode == "preview" and out.applied is False and dict(out.overrides) == {}
    assert len(out.plan.changes) == 1


def test_invalid_mode():
    with pytest.raises(ValueError):
        run_r4([], [], since=DAY, until=DAY, mode="dry")


def test_apply_overrides_feed_projection():
    others = [obs(MASSIVE, "16", day=SPIKE_DAY), obs(SIP, "10.02", day=SPIKE_DAY)]
    out = run_r4(
        make_rows(BASE), others, since=SESSIONS[0], until=SESSIONS[-1], mode="apply"
    )
    assert out.applied
    (sel,) = project_canonical_bars(others, overrides=out.overrides)
    assert sel.selection_reason == "spike_replaced" and sel.selected.close == Decimal("10.02")

    out = run_r4(make_rows(BASE), [], since=SESSIONS[0], until=SESSIONS[-1], mode="apply")
    (sel,) = project_canonical_bars([obs(MASSIVE, "16", day=SPIKE_DAY)], overrides=out.overrides)
    assert sel.quality_status == "spike_suspect"


def _many_spike_rows(count: int):
    rows = []
    for n in range(count):
        closes = {SPIKE_DAY: "16", SESSIONS[26]: "10.5"}
        rows += make_rows(closes, instrument_id=f"ZZ{n:03d}")
    return rows


def test_circuit_breaker_aborts_apply():
    cap = rules.BREAKER_CAPS["R4"]
    rows = _many_spike_rows(cap + 1)
    out = run_r4(rows, [], since=SESSIONS[0], until=SESSIONS[-1], mode="apply")
    assert out.plan.planned_change_count == cap + 1
    assert out.plan.breaker_tripped is True
    assert out.plan.changes == ()
    assert out.applied is False and dict(out.overrides) == {}


def test_at_cap_still_applies():
    cap = rules.BREAKER_CAPS["R4"]
    out = run_r4(_many_spike_rows(cap), [], since=SESSIONS[0], until=SESSIONS[-1], mode="apply")
    assert out.plan.breaker_tripped is False
    assert out.applied and len(out.overrides) == cap
