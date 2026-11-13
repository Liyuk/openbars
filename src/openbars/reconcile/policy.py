"""Reconcile policy. The default constructor is the ``research-daily-v2`` profile.

Tolerances are expressed in **percent** as ``Decimal`` (``Decimal("0.5")`` == 0.5%).
Changing anything that alters selection results of the default profile requires a new
``policy_version``; build a new ``ReconcilePolicy`` with a different version instead.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from openbars.core.series import (
    ALPACA_SIP_SPLIT,
    MASSIVE_EOD_SPLIT,
    POLICY_VERSION_RESEARCH_DAILY_V2,
    TWELVE_DATA_DAILY_SPLIT,
    YFINANCE_YAHOO_SPLIT,
    SourceIdentity,
)

PRICE_TOLERANCE_PCT = Decimal("0.5")
VOLUME_TOLERANCE_PCT = Decimal("5")
CORROBORATION_QUORUM = 2

# Capability strings (see docs/architecture.md section 11.2).
CAP_PRICE_OHLC = "price_ohlc"
CAP_CONSOLIDATED_VOLUME = "consolidated_volume"
CAP_SPLIT_ADJUSTED_VOLUME = "split_adjusted_volume"
VOLUME_CAPABILITIES = frozenset({CAP_CONSOLIDATED_VOLUME, CAP_SPLIT_ADJUSTED_VOLUME})

_FILL_BASE_CAPABILITIES = (
    "price_basis:split_adjusted",
    "adjustment_scope:splits",
    "session_semantics:provider_aligned",
)


@dataclass(frozen=True)
class FillSource:
    """A fallback source with an explicitly declared capability set.

    Fill capabilities are *declared*, not derived from the series catalog: the catalog
    is deliberately conservative (``provider_defined`` / ``unknown``) for these feeds.
    """

    identity: SourceIdentity
    capabilities: tuple[str, ...]


YFINANCE_FILL = FillSource(
    YFINANCE_YAHOO_SPLIT,
    (
        *_FILL_BASE_CAPABILITIES,
        "volume_basis:split_adjusted",
        "currency:USD",
        CAP_PRICE_OHLC,
        CAP_CONSOLIDATED_VOLUME,
        CAP_SPLIT_ADJUSTED_VOLUME,
    ),
)

# Price-only: the volume basis of this feed is unknown.
TWELVE_DATA_FILL = FillSource(
    TWELVE_DATA_DAILY_SPLIT,
    (
        *_FILL_BASE_CAPABILITIES,
        "volume_basis:unknown",
        "currency:USD",
        CAP_PRICE_OHLC,
    ),
)

DEFAULT_FILL_SOURCES: tuple[FillSource, ...] = (YFINANCE_FILL, TWELVE_DATA_FILL)


@dataclass(frozen=True)
class ReconcilePolicy:
    """Selection policy.

    ``primary`` has precedence 0, ``secondary_sources`` follow in order (precedence
    1, 2, ...). ``fill_sources`` are consulted only when neither the primary nor a
    compatible priority source has the bar, in trust order.

    ``volume_priority`` is advisory only: selection is whole-bar, so volume always
    comes from the same observation as the price.
    """

    policy_version: str = POLICY_VERSION_RESEARCH_DAILY_V2
    primary: SourceIdentity = MASSIVE_EOD_SPLIT
    secondary_sources: tuple[SourceIdentity, ...] = (ALPACA_SIP_SPLIT,)
    fill_sources: tuple[FillSource, ...] = DEFAULT_FILL_SOURCES
    price_tolerance_pct: Decimal = PRICE_TOLERANCE_PCT
    volume_tolerance_pct: Decimal = VOLUME_TOLERANCE_PCT
    corroboration_quorum: int = CORROBORATION_QUORUM
    volume_priority: tuple[SourceIdentity, ...] = (MASSIVE_EOD_SPLIT, ALPACA_SIP_SPLIT)
    # Wait this many minutes after the close before a session counts as complete.
    completion_wait_minutes: int = 30
    # False: missing sessions are recorded as known gaps instead of backfill jobs.
    refill_gaps: bool = True
    extra_rules: dict[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in ("price_tolerance_pct", "volume_tolerance_pct"):
            value = getattr(self, name)
            if isinstance(value, (bool, float)) or not isinstance(value, Decimal):
                raise TypeError(f"{name} must be Decimal, not {type(value).__name__}")
            if value < 0:
                raise ValueError(f"{name} must be >= 0")
        if self.corroboration_quorum < 1:
            raise ValueError("corroboration_quorum must be >= 1")
        if self.completion_wait_minutes < 0:
            raise ValueError("completion_wait_minutes must be >= 0")
        object.__setattr__(self, "secondary_sources", tuple(self.secondary_sources))
        object.__setattr__(self, "fill_sources", tuple(self.fill_sources))
        if self.primary in self.secondary_sources:
            raise ValueError("primary must not also be a secondary source")

    @property
    def priority_sources(self) -> tuple[SourceIdentity, ...]:
        return (self.primary, *self.secondary_sources)

    def precedence(self, source: SourceIdentity) -> int:
        """0 for the primary, 1.. for secondaries, 99 for everything else."""
        priority = self.priority_sources
        return priority.index(source) if source in priority else 99

    def is_priority_source(self, source: SourceIdentity) -> bool:
        return source in self.priority_sources

    def fill_for(self, source: SourceIdentity) -> FillSource | None:
        for fill in self.fill_sources:
            if fill.identity == source:
                return fill
        return None


DEFAULT_POLICY = ReconcilePolicy()
