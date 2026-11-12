"""research-daily-v2 whole-bar selection (pure logic, no I/O).

For every ``(instrument, session_date)`` exactly one source observation is selected;
price and volume always come from that same observation (never spliced across feeds).
"""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, replace
from datetime import date, datetime
from decimal import Decimal

from openbars.core.ohlcv import validate_ohlcv
from openbars.core.series import (
    QUALITY_COMPLETE,
    QUALITY_DIVERGENT,
    QUALITY_SPIKE_SUSPECT,
    QUALITY_VOLUME_MISSING,
    SELECTION_COMPATIBLE_GAP_FILL,
    SELECTION_FILL_PREFIX,
    SELECTION_PRIMARY_SOURCE,
    SELECTION_PRIMARY_SOURCE_CORROBORATED,
    SELECTION_SPIKE_REPLACED,
    BarSeriesCatalog,
    BarSeriesIdentity,
    CanonicalDailyRow,
    PriceBasis,
    SourceIdentity,
    VolumeBasis,
)
from openbars.reconcile.compatibility import compare_series
from openbars.reconcile.overrides import (
    ACTION_MARK_SUSPECT,
    ACTION_REPLACE_BAR,
    OverrideMap,
    SelectionOverride,
)
from openbars.reconcile.policy import (
    CAP_CONSOLIDATED_VOLUME,
    CAP_PRICE_OHLC,
    CAP_SPLIT_ADJUSTED_VOLUME,
    DEFAULT_POLICY,
    VOLUME_CAPABILITIES,
    ReconcilePolicy,
)

_UNTRUSTED_PRICE_BASIS = {PriceBasis.UNKNOWN, PriceBasis.PROVIDER_DEFINED}
_UNTRUSTED_VOLUME_BASIS = {VolumeBasis.UNKNOWN, VolumeBasis.PROVIDER_DEFINED}
_HUNDRED = Decimal(100)


@dataclass(frozen=True)
class SourceObservation:
    """One source bar as seen by the selector."""

    instrument_id: str | None
    symbol: str
    session_date: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    provider: str
    feed: str
    adjustment: str
    revision: int = 1
    source_bar_id: str | None = None
    series: BarSeriesIdentity | None = None  # None: resolved via BarSeriesCatalog

    def __post_init__(self) -> None:
        if type(self.session_date) is not date:
            raise TypeError("session_date must be a date (not datetime)")
        if isinstance(self.revision, bool) or not isinstance(self.revision, int):
            raise TypeError("revision must be int")
        if self.revision < 1:
            raise ValueError(f"revision must be >= 1, got {self.revision}")
        if self.instrument_id is None and not self.symbol:
            raise ValueError("either instrument_id or symbol is required")
        if self.volume is None:
            raise ValueError("volume is required")
        o, h, low, c, v, _ = validate_ohlcv(self.open, self.high, self.low, self.close, self.volume)
        for name, value in (("open", o), ("high", h), ("low", low), ("close", c), ("volume", v)):
            object.__setattr__(self, name, value)
        identity = SourceIdentity(self.provider, self.feed, self.adjustment)
        if self.series is not None and self.series.source != identity:
            raise ValueError("series does not match provider/feed/adjustment")

    @property
    def identity(self) -> SourceIdentity:
        return SourceIdentity(self.provider, self.feed, self.adjustment)

    @property
    def key(self) -> str:
        """Grouping id: instrument id, falling back to the symbol."""
        return self.instrument_id or self.symbol

    @property
    def resolved_series(self) -> BarSeriesIdentity:
        return self.series if self.series is not None else BarSeriesCatalog.resolve(self.identity)


@dataclass(frozen=True)
class CanonicalSelection:
    """The selected observation for one ``(instrument, session_date)`` plus its labels."""

    instrument_id: str
    session_date: date
    policy_version: str
    selected: SourceObservation
    selection_reason: str
    quality_status: str
    capabilities: tuple[str, ...]
    sources_seen: tuple[SourceIdentity, ...]
    source_fingerprint: str

    @property
    def price_source(self) -> SourceIdentity:
        return self.selected.identity

    @property
    def volume_source(self) -> SourceIdentity:
        """Always the same observation as the price (whole-bar selection)."""
        return self.selected.identity

    def to_row(self, *, generated_at: datetime, revision: int | None = None) -> CanonicalDailyRow:
        bar = self.selected
        return CanonicalDailyRow(
            instrument_id=self.instrument_id,
            session_date=self.session_date,
            policy_version=self.policy_version,
            selected_source_bar_id=bar.source_bar_id,
            provider=bar.provider,
            feed=bar.feed,
            adjustment=bar.adjustment,
            revision=bar.revision if revision is None else revision,
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=bar.volume,
            selection_reason=self.selection_reason,
            quality_status=self.quality_status,
            capabilities=self.capabilities,
            source_fingerprint=self.source_fingerprint,
            generated_at=generated_at,
        )


# --- helpers ---------------------------------------------------------------------------


def is_priority_source(
    identity: SourceIdentity, *, policy: ReconcilePolicy | None = None
) -> bool:
    """True for the primary and secondary priority sources (fill sources are not)."""
    return (policy or DEFAULT_POLICY).is_priority_source(identity)


def source_fingerprint(
    instrument_id: str,
    session_date: date,
    source_bar_id: str | None,
    revision: int,
    policy_version: str,
) -> str:
    payload = json.dumps(
        [instrument_id, session_date.isoformat(), source_bar_id, revision, policy_version],
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _difference_pct(a: Decimal, b: Decimal) -> Decimal:
    if a == 0 and b == 0:
        return Decimal(0)
    return abs(a - b) / max(abs(a), abs(b)) * _HUNDRED


def _price_diff_pct(a: SourceObservation, b: SourceObservation) -> Decimal:
    return max(
        _difference_pct(a.open, b.open),
        _difference_pct(a.high, b.high),
        _difference_pct(a.low, b.low),
        _difference_pct(a.close, b.close),
    )


def _bars_agree(a: SourceObservation, b: SourceObservation, policy: ReconcilePolicy) -> bool:
    return (
        _price_diff_pct(a, b) <= policy.price_tolerance_pct
        and _difference_pct(a.volume, b.volume) <= policy.volume_tolerance_pct
    )


def _capabilities(bar: SourceObservation) -> tuple[str, ...]:
    series = bar.resolved_series
    caps = [
        f"price_basis:{series.price_basis.value}",
        f"adjustment_scope:{series.adjustment_scope.value}",
        f"session_semantics:{series.session_semantics.value}",
        f"volume_basis:{series.volume_basis.value}",
    ]
    if series.currency:
        caps.append(f"currency:{series.currency}")
    if series.adjustment_as_of is not None:
        caps.append(f"adjustment_as_of:{series.adjustment_as_of.isoformat()}")
    if series.price_basis not in _UNTRUSTED_PRICE_BASIS:
        caps.append(CAP_PRICE_OHLC)
    if series.volume_basis not in _UNTRUSTED_VOLUME_BASIS:
        caps.extend((CAP_CONSOLIDATED_VOLUME, CAP_SPLIT_ADJUSTED_VOLUME))
    return tuple(caps)


def _without_volume_caps(caps: Iterable[str]) -> tuple[str, ...]:
    return tuple(c for c in caps if c not in VOLUME_CAPABILITIES)


def volume_is_missing(bar: SourceObservation) -> bool:
    """Zero volume on a bar whose price moved (a flat zero-volume bar is legitimate)."""
    return bar.volume == 0 and (bar.open != bar.close or bar.high != bar.low)


def _rank(bar: SourceObservation, policy: ReconcilePolicy) -> tuple[int, str, str, int]:
    return (policy.precedence(bar.identity), bar.provider, bar.feed, bar.revision)


def _latest_per_source(group: Sequence[SourceObservation]) -> list[SourceObservation]:
    latest: dict[SourceIdentity, SourceObservation] = {}
    for bar in group:
        current = latest.get(bar.identity)
        if current is None or _version(bar) > _version(current):
            latest[bar.identity] = bar
    return list(latest.values())


def _version(bar: SourceObservation) -> tuple[int, int, str]:
    """``(revision, numeric source_bar_id or 0, id text)``; a missing id counts as 0."""
    sid = bar.source_bar_id
    return (bar.revision, int(sid) if sid is not None and sid.isdigit() else 0, sid or "")


# --- selection -------------------------------------------------------------------------


@dataclass(frozen=True)
class _Pick:
    bar: SourceObservation
    reason: str
    quality: str
    capabilities: tuple[str, ...]


def _select_research_bars(
    group: Sequence[SourceObservation], policy: ReconcilePolicy
) -> _Pick | None:
    instrument_key = group[0].key
    bars = sorted(group, key=lambda b: _rank(b, policy))
    by_identity = {b.identity: b for b in bars}
    primary = by_identity.get(policy.primary)

    if primary is not None:
        quality = QUALITY_COMPLETE
        reason = SELECTION_PRIMARY_SOURCE
        caps = _capabilities(primary)
        primary_series = primary.resolved_series
        independents = [b for b in bars if not policy.is_priority_source(b.identity)]
        for secondary_id in policy.secondary_sources:
            secondary = by_identity.get(secondary_id)
            if secondary is None:
                continue
            gate = compare_series(
                instrument_key, primary_series, instrument_key, secondary.resolved_series
            )
            if not gate.comparable:
                continue
            if (
                _price_diff_pct(primary, secondary) > policy.price_tolerance_pct
                or _difference_pct(primary.volume, secondary.volume) > policy.volume_tolerance_pct
            ):
                backing_primary = [b for b in independents if _bars_agree(b, primary, policy)]
                backing_secondary = [b for b in independents if _bars_agree(b, secondary, policy)]
                if len(backing_primary) >= policy.corroboration_quorum and not backing_secondary:
                    reason = SELECTION_PRIMARY_SOURCE_CORROBORATED
                    continue
                quality = QUALITY_DIVERGENT
                caps = _without_volume_caps(caps)
                break
        # Upstream: a divergent primary is reported as plain primary_source.
        return _Pick(
            primary, reason if quality == QUALITY_COMPLETE else SELECTION_PRIMARY_SOURCE, quality, caps
        )

    # No primary: first compatible priority source, else the fill sources by trust.
    reference = BarSeriesCatalog.resolve(policy.primary)
    fill_ids = {f.identity for f in policy.fill_sources}
    fills = [b for b in bars if b.identity in fill_ids]
    for secondary_id in policy.secondary_sources:
        secondary = by_identity.get(secondary_id)
        if secondary is None:
            continue
        series = secondary.resolved_series
        # With no primary bar there is no cutoff to compare against: use the secondary's own.
        gate = compare_series(
            instrument_key,
            replace(reference, adjustment_as_of=series.adjustment_as_of),
            instrument_key,
            series,
        )
        if not gate.comparable:
            continue
        quality = QUALITY_COMPLETE
        caps = _capabilities(secondary)
        for fill in fills:
            if _difference_pct(fill.close, secondary.close) > policy.price_tolerance_pct:
                quality = QUALITY_DIVERGENT
                caps = _without_volume_caps(caps)
                break
        return _Pick(secondary, SELECTION_COMPATIBLE_GAP_FILL, quality, caps)

    for fill_source in policy.fill_sources:
        bar = by_identity.get(fill_source.identity)
        if bar is not None:
            return _Pick(
                bar,
                f"{SELECTION_FILL_PREFIX}{bar.provider}",
                QUALITY_COMPLETE,
                fill_source.capabilities,
            )
    return None


def _without_missing_volume(pick: _Pick) -> _Pick:
    if pick.quality == QUALITY_DIVERGENT or not volume_is_missing(pick.bar):
        return pick
    return replace(
        pick, quality=QUALITY_VOLUME_MISSING, capabilities=_without_volume_caps(pick.capabilities)
    )


def _to_selection(
    key: str,
    pick: _Pick,
    seen: Sequence[SourceObservation],
    policy: ReconcilePolicy,
) -> CanonicalSelection:
    bar = pick.bar
    ranked = sorted(seen, key=lambda b: _rank(b, policy))
    sources_seen = tuple(dict.fromkeys(b.identity for b in ranked))
    return CanonicalSelection(
        instrument_id=key,
        session_date=bar.session_date,
        policy_version=policy.policy_version,
        selected=bar,
        selection_reason=pick.reason,
        quality_status=pick.quality,
        capabilities=pick.capabilities,
        sources_seen=sources_seen,
        source_fingerprint=source_fingerprint(
            key, bar.session_date, bar.source_bar_id, bar.revision, policy.policy_version
        ),
    )


def _apply_override(
    pick: _Pick,
    group: Sequence[SourceObservation],
    override: SelectionOverride,
    policy: ReconcilePolicy,
) -> _Pick:
    if override.action == ACTION_REPLACE_BAR:
        target = SourceIdentity(
            override.provider or "", override.feed or "", override.adjustment or ""
        )
        replacement = next((b for b in group if b.identity == target), None)
        if replacement is None:
            return pick
        fill = policy.fill_for(replacement.identity)
        caps = fill.capabilities if fill is not None else _capabilities(replacement)
        return _without_missing_volume(
            _Pick(replacement, SELECTION_SPIKE_REPLACED, QUALITY_COMPLETE, caps)
        )
    if override.action == ACTION_MARK_SUSPECT and pick.quality != QUALITY_DIVERGENT:
        return replace(pick, quality=QUALITY_SPIKE_SUSPECT)
    return pick


def project_canonical_bars(
    bars: Iterable[SourceObservation],
    *,
    policy: ReconcilePolicy | None = None,
    overrides: OverrideMap | None = None,
) -> tuple[CanonicalSelection, ...]:
    """Select one canonical observation per ``(instrument, session_date)``.

    Output is sorted by ``(instrument_id, session_date)``. ``overrides`` (optional)
    re-applies ``replace_bar`` / ``mark_suspect`` results from the data-fix rules.
    """
    policy = policy or DEFAULT_POLICY
    grouped: dict[tuple[str, date], list[SourceObservation]] = defaultdict(list)
    for bar in bars:
        grouped[(bar.key, bar.session_date)].append(bar)

    out: list[CanonicalSelection] = []
    for (key, day) in sorted(grouped):
        latest = _latest_per_source(grouped[(key, day)])
        pick = _select_research_bars(latest, policy)
        if pick is None:
            continue
        pick = _without_missing_volume(pick)
        override = (overrides or {}).get((key, day))
        if override is not None:
            pick = _apply_override(pick, latest, override, policy)
        out.append(_to_selection(key, pick, latest, policy))
    return tuple(out)
