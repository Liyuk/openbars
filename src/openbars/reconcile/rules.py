"""Deterministic data-fix rules R1-R4 (frozen parameters).

Only R4 (isolated one-day spike repair) is pure logic and is implemented here:
``plan_r4`` decides, ``run_r4`` additionally honours the preview/apply mode and the
circuit breaker. R1-R3 depend on the instrument master and are exposed as constants
plus Protocol stubs only (no SQL or registry lookups are invented here).
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Collection, Iterable, Mapping
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Protocol

from openbars.core.series import CanonicalDailyRow, SourceIdentity
from openbars.reconcile.overrides import (
    ACTION_MARK_SUSPECT,
    ACTION_REPLACE_BAR,
    OverrideMap,
    SelectionOverride,
)
from openbars.reconcile.policy import DEFAULT_POLICY, ReconcilePolicy
from openbars.reconcile.projection import SourceObservation

# --- frozen R4 parameters --------------------------------------------------------------

SPIKE_MOVE = Decimal("0.50")  # |close / prev_close - 1| at or above this is a spike candidate
SPIKE_REVERT = Decimal("0.15")  # |next_close / prev_close - 1| below this: price came back
CORROBORATION_TOLERANCE = Decimal("0.005")  # another source "agrees" within this relative diff
VOLUME_MULTIPLE = Decimal("2.0")  # a spike on >= this multiple of avg volume is left alone
VOLUME_LOOKBACK = 20  # prior sessions used for the average volume
MAX_NEIGHBOUR_GAP_DAYS = 5  # prev/next bars must be within this many calendar days

# Per-rule cap on changes applied in one run; exceeding it aborts the whole rule.
BREAKER_CAPS: Mapping[str, int] = {"R1": 15000, "R2": 500, "R3": 50, "R4": 200}

R4_SCOPE_TYPES = frozenset({"common_stock", "adr", "etf"})

MODE_PREVIEW = "preview"
MODE_APPLY = "apply"
MODES = (MODE_PREVIEW, MODE_APPLY)

OUTCOME_CORROBORATED = "corroborated"
OUTCOME_SKIPPED = "skipped"
SKIP_SOURCES_CONFLICT = "sourcesConflict"
SKIP_HIGH_VOLUME_SPIKE = "highVolumeSpike"


# --- R1-R3: instrument-master dependent (not implemented here) ---------------------------


class _InstrumentMasterRule(Protocol):
    """Shape shared by the instrument-master dependent rules.

    Implementations arrive together with the instrument master; until then only the
    frozen circuit-breaker caps are defined. ``plan`` must be side-effect free and the
    apply step must abort without changes when the planned count exceeds the cap.
    """

    def plan(self, *, mode: str = MODE_PREVIEW) -> object: ...


class R1Rule(_InstrumentMasterRule, Protocol):
    """R1: deferred (instrument-master dependent). Cap: ``BREAKER_CAPS["R1"]``."""


class R2Rule(_InstrumentMasterRule, Protocol):
    """R2: deferred (instrument-master dependent). Cap: ``BREAKER_CAPS["R2"]``."""


class R3Rule(_InstrumentMasterRule, Protocol):
    """R3: deferred (instrument-master dependent). Cap: ``BREAKER_CAPS["R3"]``."""


# --- R4 ------------------------------------------------------------------------------------


@dataclass(frozen=True)
class R4Decision:
    instrument_id: str
    session_date: date
    outcome: str  # replace_bar | mark_suspect | corroborated | skipped
    skip_reason: str | None = None
    replacement: SourceIdentity | None = None  # set for replace_bar

    @property
    def counts_toward_cap(self) -> bool:
        return self.outcome in (ACTION_REPLACE_BAR, ACTION_MARK_SUSPECT)


@dataclass(frozen=True)
class R4Plan:
    decisions: tuple[R4Decision, ...]
    breaker_cap: int
    breaker_tripped: bool

    @property
    def changes(self) -> tuple[R4Decision, ...]:
        """Decisions that would be applied; empty when the circuit breaker tripped."""
        if self.breaker_tripped:
            return ()
        return tuple(d for d in self.decisions if d.counts_toward_cap)

    @property
    def planned_change_count(self) -> int:
        return sum(1 for d in self.decisions if d.counts_toward_cap)


@dataclass(frozen=True)
class R4Outcome:
    mode: str
    plan: R4Plan
    applied: bool
    overrides: OverrideMap  # non-empty only in apply mode and when not aborted


def _rel_diff(a: Decimal, b: Decimal) -> Decimal:
    if a == 0 and b == 0:
        return Decimal(0)
    return abs(a - b) / max(abs(a), abs(b))


def _ratio_move(value: Decimal, base: Decimal) -> Decimal:
    return abs(value / base - 1)


def _in_scope(instrument_id: str, asset_classes: Mapping[str, str] | None) -> bool:
    if asset_classes is None:
        return True
    kind = asset_classes.get(instrument_id)
    return kind is None or kind in R4_SCOPE_TYPES


def _same_source(row: CanonicalDailyRow, identity: SourceIdentity) -> bool:
    return (row.provider, row.feed, row.adjustment) == (
        identity.provider,
        identity.feed,
        identity.adjustment,
    )


def plan_r4(
    canonical_rows: Iterable[CanonicalDailyRow],
    observations: Iterable[SourceObservation],
    *,
    since: date,
    until: date,
    already_evaluated: Collection[tuple[str, date]] = (),
    asset_classes: Mapping[str, str] | None = None,
    policy: ReconcilePolicy | None = None,
) -> R4Plan:
    """Plan R4 repairs for isolated one-day spikes with session_date in ``[since, until]``.

    ``asset_classes`` (instrument_id -> type) is optional; instruments without an entry
    are in scope, instruments whose type is not in ``R4_SCOPE_TYPES`` are skipped.
    Only priority and fill sources of ``policy`` count as "other sources"; the circuit
    breaker trips when planned replace/mark changes exceed ``BREAKER_CAPS["R4"]``.
    """
    policy = policy or DEFAULT_POLICY
    eligible = {*policy.priority_sources, *(f.identity for f in policy.fill_sources)}

    by_instrument: dict[str, list[CanonicalDailyRow]] = defaultdict(list)
    for row in canonical_rows:
        by_instrument[row.instrument_id].append(row)

    latest: dict[tuple[str, date, SourceIdentity], SourceObservation] = {}
    for obs in observations:
        k = (obs.key, obs.session_date, obs.identity)
        cur = latest.get(k)
        if cur is None or obs.revision > cur.revision:
            latest[k] = obs
    day_sources: dict[tuple[str, date], list[SourceObservation]] = defaultdict(list)
    for (inst, day, _), obs in latest.items():
        day_sources[(inst, day)].append(obs)

    evaluated = set(already_evaluated)
    decisions: list[R4Decision] = []
    for instrument_id in sorted(by_instrument):
        if not _in_scope(instrument_id, asset_classes):
            continue
        rows = sorted(by_instrument[instrument_id], key=lambda r: r.session_date)
        for i in range(1, len(rows) - 1):
            row, prev, nxt = rows[i], rows[i - 1], rows[i + 1]
            if not since <= row.session_date <= until:
                continue
            if (instrument_id, row.session_date) in evaluated:
                continue
            if (row.session_date - prev.session_date).days > MAX_NEIGHBOUR_GAP_DAYS:
                continue
            if (nxt.session_date - row.session_date).days > MAX_NEIGHBOUR_GAP_DAYS:
                continue
            if i < VOLUME_LOOKBACK:
                continue
            if _ratio_move(row.close, prev.close) < SPIKE_MOVE:
                continue
            if _ratio_move(nxt.close, prev.close) >= SPIKE_REVERT:
                continue
            window = rows[i - VOLUME_LOOKBACK : i]
            avg_volume = sum((r.volume for r in window), Decimal(0)) / VOLUME_LOOKBACK
            decisions.append(
                _decide(
                    instrument_id, row, prev, avg_volume,
                    day_sources.get((instrument_id, row.session_date), []), eligible, policy,
                )  # fmt: skip
            )

    cap = BREAKER_CAPS["R4"]
    planned = sum(1 for d in decisions if d.counts_toward_cap)
    return R4Plan(tuple(decisions), cap, planned > cap)


def _decide(
    instrument_id: str,
    row: CanonicalDailyRow,
    prev: CanonicalDailyRow,
    avg_volume: Decimal,
    sources: list[SourceObservation],
    eligible: set[SourceIdentity],
    policy: ReconcilePolicy,
) -> R4Decision:
    others = sorted(
        (s for s in sources if s.identity in eligible and not _same_source(row, s.identity)),
        key=lambda s: (policy.precedence(s.identity), s.provider, s.feed),
    )
    agreeing = [s for s in others if _rel_diff(s.close, row.close) <= CORROBORATION_TOLERANCE]
    replacing = [
        s
        for s in others
        if s not in agreeing and _ratio_move(s.close, prev.close) < SPIKE_REVERT
    ]
    day = row.session_date
    if agreeing and replacing:
        return R4Decision(instrument_id, day, OUTCOME_SKIPPED, SKIP_SOURCES_CONFLICT)
    if agreeing:
        return R4Decision(instrument_id, day, OUTCOME_CORROBORATED)
    if replacing:
        return R4Decision(
            instrument_id, day, ACTION_REPLACE_BAR, replacement=replacing[0].identity
        )
    if row.volume < VOLUME_MULTIPLE * avg_volume:
        return R4Decision(instrument_id, day, ACTION_MARK_SUSPECT)
    return R4Decision(instrument_id, day, OUTCOME_SKIPPED, SKIP_HIGH_VOLUME_SPIKE)


def run_r4(
    canonical_rows: Iterable[CanonicalDailyRow],
    observations: Iterable[SourceObservation],
    *,
    since: date,
    until: date,
    mode: str = MODE_PREVIEW,
    already_evaluated: Collection[tuple[str, date]] = (),
    asset_classes: Mapping[str, str] | None = None,
    policy: ReconcilePolicy | None = None,
) -> R4Outcome:
    """Plan R4 and, in ``apply`` mode, turn the changes into selection overrides.

    ``preview`` (default) never produces overrides. If the circuit breaker tripped,
    nothing is applied in either mode. Feed ``outcome.overrides`` to
    ``project_canonical_bars(..., overrides=...)`` to re-apply the results.
    """
    if mode not in MODES:
        raise ValueError(f"mode must be one of {MODES}, got {mode!r}")
    plan = plan_r4(
        canonical_rows, observations, since=since, until=until,
        already_evaluated=already_evaluated, asset_classes=asset_classes, policy=policy,
    )  # fmt: skip
    overrides: dict[tuple[str, date], SelectionOverride] = {}
    applied = mode == MODE_APPLY and not plan.breaker_tripped
    if applied:
        for d in plan.changes:
            if d.outcome == ACTION_REPLACE_BAR and d.replacement is not None:
                overrides[(d.instrument_id, d.session_date)] = SelectionOverride(
                    ACTION_REPLACE_BAR,
                    d.replacement.provider, d.replacement.feed, d.replacement.adjustment,
                )  # fmt: skip
            else:
                overrides[(d.instrument_id, d.session_date)] = SelectionOverride(
                    ACTION_MARK_SUSPECT
                )
    return R4Outcome(mode, plan, applied, overrides)
