"""Deterministic data-fix rules R1-R4 (frozen parameters).

Every rule is a pure plan over in-memory snapshots (``plan_r1`` .. ``plan_r4``); the
matching ``run_r*`` additionally honours the preview/apply mode and the circuit breaker.

* R1 classifies ``unknown`` instruments from the latest ticker-type observations.
* R2 closes superseded placeholder aliases.
* R3 reports (and, only when explicitly enabled, applies) duplicate-instrument merges.
* R4 repairs isolated one-day spikes.

No SQL or registry lookups are invented here; stores and bars are injected.
"""

from __future__ import annotations

import dataclasses
import os
from collections import Counter, defaultdict
from collections.abc import Callable, Collection, Iterable, Mapping, MutableSequence, Sequence
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal

from openbars.core.models import AssetClass, Instrument
from openbars.core.series import CanonicalDailyRow, SourceIdentity
from openbars.normalize.instruments import InstrumentStore
from openbars.normalize.security_types import (
    UNKNOWN as SECURITY_UNKNOWN,
    asset_class_for,
    security_type_for,
    security_type_of_asset_class,
)
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


# --- R1-R3: instrument-master data fixes -------------------------------------------------------
#
# Each rule is a pure plan over in-memory snapshots (instruments, aliases, ticker-type
# observations); ``run_r1/r2/r3`` add the preview/apply mode and the circuit breaker and,
# in apply mode, mutate an ``InstrumentStore`` (and, for R3, a mutable bar collection).

PLACEHOLDER_BASIS = "superseded_placeholder"
DUPLICATE_BASIS = "superseded_duplicate"
ACTION_CLASSIFY = "classify"
ACTION_CLOSE_ALIAS = "close_alias"
ACTION_MERGE = "merge_duplicate"
SEC_PLACEHOLDER_SOURCE = "sec:source-observation"
TYPED_STOCKS = frozenset({"common_stock", "adr"})

STATUS_OK = "ok"
STATUS_NEEDS_FIRST_WEEKLY_PULL = "needs_first_weekly_pull"
STATUS_BREAKER_TRIPPED = "breaker_tripped"

SKIP_AMBIGUOUS_CANDIDATES = "ambiguousCandidates"
SKIP_AMBIGUOUS_INSTRUMENT_TYPES = "ambiguousInstrumentTypes"
SKIP_UNMAPPED_TYPE = "unmappedType"  # reported as ``unmappedType:<raw>``
SKIP_HAS_BARS = "hasBars"
SKIP_AUTHORITY = "authorityMissingOrAmbiguous"
SKIP_NO_EXACT_MATCH = "noExactMatch"
SKIP_MULTIPLE_MATCHES = "multipleMatches"
SKIP_LOSER_HAS_REFERENCES = "loserHasReferences"

R3_APPLY_ENV = "DATA_FIX_R3_APPLY"

_WILDCARD_EXCHANGES = frozenset({"", "UNKNOWN"})


@dataclass(frozen=True)
class TickerTypeObservation:
    """One provider ticker-type row (``massive_type`` is the raw provider code, e.g. ``CS``)."""

    symbol: str
    exchange: str | None
    massive_type: str
    observed_on: date


@dataclass(frozen=True)
class AliasSnapshot:
    """A symbol alias of an instrument, with the metadata ``SymbolMapping`` does not carry."""

    instrument_id: str
    symbol: str
    valid_from: date
    valid_to: date | None = None
    exchange: str | None = None
    source: str | None = None
    valid_to_basis: str | None = None
    alias_id: int | str | None = None

    @property
    def is_open(self) -> bool:
        return self.valid_to is None


@dataclass(frozen=True)
class RuleChange:
    """One planned change. ``details`` carries rule-specific facts (and apply results)."""

    rule: str
    action: str  # classify | close_alias | merge_duplicate
    instrument_id: str
    symbol: str
    alias_id: int | str | None = None
    security_type: str | None = None  # classify
    asset_class: AssetClass | None = None  # classify
    winner_instrument_id: str | None = None  # merge_duplicate
    details: Mapping[str, object] = field(default_factory=dict)

    @property
    def counts_toward_cap(self) -> bool:
        return self.action in (ACTION_CLASSIFY, ACTION_CLOSE_ALIAS, ACTION_MERGE)


@dataclass(frozen=True)
class RulePlan:
    rule: str
    changes: tuple[RuleChange, ...]
    skipped: Counter
    status: str
    breaker_cap: int
    breaker_tripped: bool

    @property
    def planned_change_count(self) -> int:
        return sum(1 for c in self.changes if c.counts_toward_cap)


@dataclass(frozen=True)
class RuleOutcome:
    rule: str
    mode: str
    plan: RulePlan
    applied: bool
    applied_changes: tuple[RuleChange, ...] = ()  # with apply results in ``details``
    aborted: bool = False  # circuit breaker tripped; nothing was applied
    abort_reason: str | None = None
    report_only: bool = False  # R3 in apply mode without explicit enablement


HasBars = Collection[str] | Callable[[str], bool] | None
ReferenceCheck = Callable[[str], bool]


def r3_apply_enabled(apply_r3: bool | None = None, env: Mapping[str, str] | None = None) -> bool:
    """R3 merges are report-only unless ``apply_r3=True`` or ``DATA_FIX_R3_APPLY=1``."""
    if apply_r3 is not None:
        return bool(apply_r3)
    source = os.environ if env is None else env
    return source.get(R3_APPLY_ENV, "").strip() == "1"


def security_type_of(instrument: Instrument) -> str:
    """``external_ids["security_type"]`` if set, else derived from the asset class."""
    explicit = (instrument.external_ids.get("security_type") or "").strip().lower()
    if explicit:
        return explicit
    return security_type_of_asset_class(instrument.asset_class)


def _is_unknown(instrument: Instrument) -> bool:
    return security_type_of(instrument) == SECURITY_UNKNOWN


def _norm_exchange(value: str | None) -> str:
    return (value or "").strip().upper()


def _exchange_matches(a: str | None, b: str | None) -> bool:
    na, nb = _norm_exchange(a), _norm_exchange(b)
    if na in _WILDCARD_EXCHANGES or nb in _WILDCARD_EXCHANGES:
        return True
    return na == nb


def _symbol_key(symbol: str) -> str:
    return symbol.strip().upper()


def _latest_observations(
    observations: Iterable[TickerTypeObservation],
) -> list[TickerTypeObservation]:
    obs = list(observations)
    if not obs:
        return []
    latest = max(o.observed_on for o in obs)
    return [o for o in obs if o.observed_on == latest]


def _resolve_cap(rule: str, cap: int | None) -> int:
    return BREAKER_CAPS[rule] if cap is None else cap


def _finish(
    rule: str, changes: list[RuleChange], skipped: Counter, cap: int | None, status: str = STATUS_OK
) -> RulePlan:
    limit = _resolve_cap(rule, cap)
    planned = sum(1 for c in changes if c.counts_toward_cap)
    tripped = planned > limit
    if tripped:
        status = STATUS_BREAKER_TRIPPED
    return RulePlan(rule, tuple(changes), skipped, status, limit, tripped)


def _has_bars_fn(has_bars: HasBars) -> Callable[[str], bool]:
    if has_bars is None:
        return lambda _i: False
    if callable(has_bars):
        return has_bars
    held = frozenset(has_bars)
    return lambda i: i in held


# --- R1: classify unknown instruments from ticker-type observations -----------------------------


def plan_r1(
    instruments: Iterable[Instrument],
    aliases: Iterable[AliasSnapshot],
    observations: Iterable[TickerTypeObservation],
    *,
    cap: int | None = None,
) -> RulePlan:
    """Classify ``unknown`` instruments from the latest ticker-type observations."""
    latest = _latest_observations(observations)
    if not latest:
        return _finish("R1", [], Counter(), cap, STATUS_NEEDS_FIRST_WEEKLY_PULL)

    unknown_ids = {i.instrument_id for i in instruments if _is_unknown(i)}
    open_aliases = [a for a in aliases if a.is_open and a.instrument_id in unknown_ids]
    by_symbol: dict[str, list[AliasSnapshot]] = defaultdict(list)
    for a in open_aliases:
        by_symbol[_symbol_key(a.symbol)].append(a)

    skipped: Counter = Counter()
    excluded: set[str] = set()  # instruments involved in an ambiguous candidate match
    raw_types: dict[str, set[str]] = defaultdict(set)  # instrument -> raw observed types
    symbols: dict[str, str] = {}
    seen: set[tuple[str, str, str]] = set()
    for o in sorted(latest, key=lambda o: (o.symbol, _norm_exchange(o.exchange), o.massive_type)):
        key = (_symbol_key(o.symbol), _norm_exchange(o.exchange), o.massive_type.strip().upper())
        if key in seen:
            continue
        seen.add(key)
        matched = {
            a.instrument_id: a
            for a in by_symbol.get(key[0], ())
            if _exchange_matches(a.exchange, o.exchange)
        }
        if not matched:
            continue
        if len(matched) > 1:
            skipped[SKIP_AMBIGUOUS_CANDIDATES] += 1
            excluded.update(matched)
            continue
        (instrument_id, alias), = matched.items()
        raw_types[instrument_id].add(key[2])
        symbols[instrument_id] = alias.symbol

    changes: list[RuleChange] = []
    for instrument_id in sorted(raw_types):
        if instrument_id in excluded:
            continue
        raws = sorted(raw_types[instrument_id])
        mapped = {security_type_for(r) for r in raws}
        if len(mapped) > 1:
            skipped[SKIP_AMBIGUOUS_INSTRUMENT_TYPES] += 1
            continue
        (security_type,) = mapped
        if security_type == SECURITY_UNKNOWN:
            skipped[f"{SKIP_UNMAPPED_TYPE}:{raws[0]}"] += 1
            continue
        alias_id = next(
            (a.alias_id for a in open_aliases if a.instrument_id == instrument_id), None
        )
        changes.append(
            RuleChange(
                "R1", ACTION_CLASSIFY, instrument_id, symbols[instrument_id], alias_id,
                security_type=security_type, asset_class=asset_class_for(raws[0]),
                details={"massiveTypes": tuple(raws)},
            )
        )  # fmt: skip
    return _finish("R1", changes, skipped, cap)


def _instrument_lookup(
    store: InstrumentStore | None, snapshot: Iterable[Instrument]
) -> dict[str, Instrument]:
    found = {i.instrument_id: i for i in snapshot}
    if store is not None:
        found.update({i.instrument_id: i for i in store.instruments()})
    return found


def _validate_mode(mode: str) -> None:
    if mode not in MODES:
        raise ValueError(f"mode must be one of {MODES}, got {mode!r}")


def _aborted(rule: str, mode: str, plan: RulePlan) -> RuleOutcome:
    reason = f"circuit breaker: {plan.planned_change_count} changes > cap {plan.breaker_cap}"
    return RuleOutcome(rule, mode, plan, applied=False, aborted=True, abort_reason=reason)


def run_r1(
    instruments: Iterable[Instrument],
    aliases: Iterable[AliasSnapshot],
    observations: Iterable[TickerTypeObservation],
    *,
    mode: str = MODE_PREVIEW,
    store: InstrumentStore | None = None,
    cap: int | None = None,
) -> RuleOutcome:
    """Plan R1 and, in ``apply`` mode with a store and no breaker trip, write the types."""
    _validate_mode(mode)
    instruments = list(instruments)
    plan = plan_r1(instruments, aliases, observations, cap=cap)
    if plan.breaker_tripped:
        return _aborted("R1", mode, plan)
    if mode != MODE_APPLY or store is None:
        return RuleOutcome("R1", mode, plan, applied=False)
    current = _instrument_lookup(store, instruments)
    updated: list[Instrument] = []
    for c in plan.changes:
        instrument = current.get(c.instrument_id)
        if instrument is None or c.security_type is None:
            continue
        updated.append(
            dataclasses.replace(
                instrument,
                asset_class=c.asset_class or instrument.asset_class,
                external_ids={**instrument.external_ids, "security_type": c.security_type},
            )
        )
    store.upsert(updated, [])
    return RuleOutcome("R1", mode, plan, applied=True, applied_changes=plan.changes)


# --- R2: close superseded placeholder aliases -----------------------------------------------------


def plan_r2(
    instruments: Iterable[Instrument],
    aliases: Iterable[AliasSnapshot],
    *,
    has_bars: HasBars = None,
    cap: int | None = None,
) -> RulePlan:
    """Close open placeholder aliases of bar-less ``unknown`` instruments whose symbol a typed
    instrument now holds."""
    by_id = {i.instrument_id: i for i in instruments}
    alias_list = [a for a in aliases if a.is_open]
    typed_symbols = {
        _symbol_key(a.symbol)
        for a in alias_list
        if a.instrument_id in by_id and security_type_of(by_id[a.instrument_id]) in TYPED_STOCKS
    }
    bars = _has_bars_fn(has_bars)
    skipped: Counter = Counter()
    changes: list[RuleChange] = []
    for a in sorted(alias_list, key=lambda a: (a.symbol, a.instrument_id, a.valid_from)):
        if a.source != SEC_PLACEHOLDER_SOURCE:
            continue
        instrument = by_id.get(a.instrument_id)
        if instrument is None or not _is_unknown(instrument):
            continue
        if _symbol_key(a.symbol) not in typed_symbols:
            continue
        if bars(a.instrument_id):
            skipped[SKIP_HAS_BARS] += 1
            continue
        changes.append(
            RuleChange(
                "R2", ACTION_CLOSE_ALIAS, a.instrument_id, a.symbol, a.alias_id,
                details={"valid_to": a.valid_from, "valid_to_basis": PLACEHOLDER_BASIS,
                         "valid_from": a.valid_from},
            )
        )  # fmt: skip
    return _finish("R2", changes, skipped, cap)


def _close_mappings(
    store: InstrumentStore, instrument_id: str, symbol: str, valid_from: date
) -> int:
    """Close the open store mappings matching ``(instrument, symbol, valid_from)``."""
    targets = [
        m
        for m in store.mappings()
        if m.instrument_id == instrument_id
        and m.symbol == symbol
        and m.valid_from == valid_from
        and m.valid_to is None
    ]
    if not targets:
        return 0
    store.remove_mappings(targets)
    store.upsert([], [dataclasses.replace(m, valid_to=m.valid_from) for m in targets])
    return len(targets)


def run_r2(
    instruments: Iterable[Instrument],
    aliases: Iterable[AliasSnapshot],
    *,
    mode: str = MODE_PREVIEW,
    store: InstrumentStore | None = None,
    has_bars: HasBars = None,
    cap: int | None = None,
) -> RuleOutcome:
    """Plan R2 and, in ``apply`` mode with a store and no breaker trip, close the aliases."""
    _validate_mode(mode)
    plan = plan_r2(instruments, aliases, has_bars=has_bars, cap=cap)
    if plan.breaker_tripped:
        return _aborted("R2", mode, plan)
    if mode != MODE_APPLY or store is None:
        return RuleOutcome("R2", mode, plan, applied=False)
    applied: list[RuleChange] = []
    for c in plan.changes:
        closed = _close_mappings(
            store, c.instrument_id, c.symbol, c.details["valid_from"]  # type: ignore[arg-type]
        )
        applied.append(dataclasses.replace(c, details={**c.details, "mappingsClosed": closed}))
    return RuleOutcome("R2", mode, plan, applied=True, applied_changes=tuple(applied))


# --- R3: merge duplicate typed instruments --------------------------------------------------------


def plan_r3(
    instruments: Iterable[Instrument],
    aliases: Iterable[AliasSnapshot],
    observations: Iterable[TickerTypeObservation],
    *,
    has_other_references: ReferenceCheck | None = None,
    cap: int | None = None,
) -> RulePlan:
    """Plan duplicate merges for symbols with several open typed-instrument mappings."""
    referenced = has_other_references or (lambda _i: False)
    latest = _latest_observations(observations)
    by_id = {i.instrument_id: i for i in instruments}
    by_symbol: dict[str, list[AliasSnapshot]] = defaultdict(list)
    for a in aliases:
        inst = by_id.get(a.instrument_id)
        if a.is_open and inst is not None and security_type_of(inst) in TYPED_STOCKS:
            by_symbol[_symbol_key(a.symbol)].append(a)

    skipped: Counter = Counter()
    changes: list[RuleChange] = []
    for symbol in sorted(by_symbol):
        group = by_symbol[symbol]
        holders = sorted({a.instrument_id for a in group})
        if len(holders) < 2:
            continue
        authority = {
            _norm_exchange(o.exchange)
            for o in latest
            if _symbol_key(o.symbol) == symbol
            and security_type_for(o.massive_type) in TYPED_STOCKS
            and _norm_exchange(o.exchange) not in _WILDCARD_EXCHANGES
        }
        if len(authority) != 1:
            skipped[SKIP_AUTHORITY] += 1
            continue
        (exchange,) = authority
        winners = sorted({a.instrument_id for a in group if _norm_exchange(a.exchange) == exchange})
        if not winners:
            skipped[SKIP_NO_EXACT_MATCH] += 1
            continue
        if len(winners) > 1:
            skipped[SKIP_MULTIPLE_MATCHES] += 1
            continue
        winner = winners[0]
        for loser in holders:
            if loser == winner:
                continue
            if referenced(loser):
                skipped[SKIP_LOSER_HAS_REFERENCES] += 1
                continue
            loser_aliases = [a for a in group if a.instrument_id == loser]
            changes.append(
                RuleChange(
                    "R3", ACTION_MERGE, loser, loser_aliases[0].symbol,
                    loser_aliases[0].alias_id, winner_instrument_id=winner,
                    details={
                        "authorityExchange": exchange,
                        "valid_to_basis": DUPLICATE_BASIS,
                        "loserValidFrom": min(a.valid_from for a in loser_aliases),
                        "winnerAliasValidFrom": min(
                            a.valid_from for a in group if a.instrument_id == winner
                        ),
                    },
                )
            )  # fmt: skip
    return _finish("R3", changes, skipped, cap)


def _bar_key(row: object) -> tuple:
    return (
        row.session_date, row.provider, row.feed, row.adjustment, row.revision  # type: ignore[attr-defined]
    )


def _move_bars(bars: MutableSequence, loser: str, winner: str) -> tuple[int, int, date | None, date | None]:
    """Move ``loser`` rows to ``winner`` in place; rows that collide with an existing winner
    row on (session, provider, feed, adjustment, revision) stay. Returns
    ``(moved, left_behind, first_session, last_session)``."""
    existing = {_bar_key(r) for r in bars if r.instrument_id == winner}
    moved = left = 0
    first: date | None = None
    last: date | None = None
    for idx, row in enumerate(list(bars)):
        if row.instrument_id != loser:
            continue
        if _bar_key(row) in existing:
            left += 1
            continue
        bars[idx] = dataclasses.replace(row, instrument_id=winner)
        existing.add(_bar_key(row))
        moved += 1
        day = row.session_date
        first = day if first is None or day < first else first
        last = day if last is None or day > last else last
    return moved, left, first, last


def run_r3(
    instruments: Iterable[Instrument],
    aliases: Iterable[AliasSnapshot],
    observations: Iterable[TickerTypeObservation],
    *,
    mode: str = MODE_PREVIEW,
    store: InstrumentStore | None = None,
    bars: MutableSequence | None = None,
    has_other_references: ReferenceCheck | None = None,
    apply_r3: bool | None = None,
    cap: int | None = None,
) -> RuleOutcome:
    """Plan R3; merges are only applied in ``apply`` mode when R3 apply is explicitly enabled.

    ``bars`` is a mutable sequence of dataclass rows exposing ``instrument_id``,
    ``session_date``, ``provider``, ``feed``, ``adjustment`` and ``revision``. No planner or
    rebuild job is scheduled; the window of moved bars is returned in each applied change's
    ``details`` (``movedFrom`` / ``movedTo``) so a caller can schedule it.
    """
    _validate_mode(mode)
    alias_list = list(aliases)
    plan = plan_r3(
        instruments, alias_list, observations, has_other_references=has_other_references, cap=cap
    )
    if plan.breaker_tripped:
        return _aborted("R3", mode, plan)
    if mode != MODE_APPLY:
        return RuleOutcome("R3", mode, plan, applied=False)
    if not r3_apply_enabled(apply_r3):
        return RuleOutcome("R3", mode, plan, applied=False, report_only=True)

    applied: list[RuleChange] = []
    for c in plan.changes:
        winner = c.winner_instrument_id
        assert winner is not None
        details: dict[str, object] = dict(c.details)
        closed = 0
        if store is not None:
            loser_from = c.details["loserValidFrom"]
            for a in alias_list:
                if a.is_open and a.instrument_id == c.instrument_id and _symbol_key(a.symbol) == _symbol_key(c.symbol):
                    closed += _close_mappings(
                        store, a.instrument_id, a.symbol, a.valid_from
                    )
            if _extend_winner(store, winner, c.symbol, loser_from):  # type: ignore[arg-type]
                details["winnerValidFrom"] = loser_from
        details["mappingsClosed"] = closed
        if bars is not None:
            moved, left, first, last = _move_bars(bars, c.instrument_id, winner)
            details.update(
                movedBars=moved, conflictingBars=left, movedFrom=first, movedTo=last
            )
        applied.append(dataclasses.replace(c, details=details))
    return RuleOutcome("R3", mode, plan, applied=True, applied_changes=tuple(applied))


def _extend_winner(store: InstrumentStore, winner: str, symbol: str, valid_from: date) -> bool:
    """Move the winner's earliest open mapping on ``symbol`` back to ``valid_from`` if it
    starts later. Returns whether a mapping was changed."""
    open_maps = [
        m for m in store.mappings()
        if m.instrument_id == winner and _symbol_key(m.symbol) == _symbol_key(symbol)
        and m.valid_to is None
    ]  # fmt: skip
    if not open_maps:
        return False
    earliest = min(m.valid_from for m in open_maps)
    if earliest <= valid_from:
        return False
    targets = [m for m in open_maps if m.valid_from == earliest]
    store.remove_mappings(targets)
    store.upsert([], [dataclasses.replace(m, valid_from=valid_from) for m in targets])
    return True
