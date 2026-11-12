"""Source identity, series semantics and the research-daily-v2 canonical row.

See docs/architecture.md §11.2. The string values of the source identity triples are
part of the compatibility contract and must not change.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import ClassVar

from openbars.core.models import Interval
from openbars.core.ohlcv import validate_ohlcv

POLICY_VERSION_RESEARCH_DAILY_V2 = "research-daily-v2"


@dataclass(frozen=True)
class SourceIdentity:
    """``(provider, feed, adjustment)``; ``adjustment`` is the legacy string ("split"/"raw")."""

    provider: str
    feed: str
    adjustment: str

    def __post_init__(self) -> None:
        for name in ("provider", "feed", "adjustment"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value:
                raise ValueError(f"{name} must be a non-empty string")

    @property
    def key(self) -> str:
        return f"{self.provider}/{self.feed}/{self.adjustment}"

    @classmethod
    def parse(cls, key: str) -> SourceIdentity:
        parts = key.split("/")
        if len(parts) != 3:
            raise ValueError(f"source identity must look like provider/feed/adjustment: {key!r}")
        return cls(*parts)


MASSIVE_EOD_SPLIT = SourceIdentity("massive", "eod", "split")
ALPACA_SIP_SPLIT = SourceIdentity("alpaca", "sip", "split")
ALPACA_IEX_SPLIT = SourceIdentity("alpaca", "iex", "split")
YFINANCE_YAHOO_SPLIT = SourceIdentity("yfinance", "yahoo", "split")
TWELVE_DATA_DAILY_SPLIT = SourceIdentity("twelve_data", "daily", "split")
ALPHA_VANTAGE_DAILY_RAW = SourceIdentity("alpha_vantage", "daily", "raw")

KNOWN_SOURCES: tuple[SourceIdentity, ...] = (
    MASSIVE_EOD_SPLIT,
    ALPACA_SIP_SPLIT,
    ALPACA_IEX_SPLIT,
    YFINANCE_YAHOO_SPLIT,
    TWELVE_DATA_DAILY_SPLIT,
    ALPHA_VANTAGE_DAILY_RAW,
)


class PriceBasis(str, Enum):
    RAW = "raw"
    SPLIT_ADJUSTED = "split_adjusted"
    TOTAL_RETURN_ADJUSTED = "total_return_adjusted"
    PROVIDER_DEFINED = "provider_defined"
    UNKNOWN = "unknown"


class AdjustmentScope(str, Enum):
    NONE = "none"
    SPLITS = "splits"
    DIVIDENDS = "dividends"
    SPLITS_AND_DIVIDENDS = "splits_and_dividends"
    PROVIDER_DEFINED = "provider_defined"
    UNKNOWN = "unknown"


class SessionSemantics(str, Enum):
    REGULAR = "regular"
    EXTENDED = "extended"
    PROVIDER_ALIGNED = "provider_aligned"
    UNKNOWN = "unknown"


class VolumeBasis(str, Enum):
    AS_TRADED = "as_traded"
    SPLIT_ADJUSTED = "split_adjusted"
    PROVIDER_DEFINED = "provider_defined"
    UNKNOWN = "unknown"


_CURRENCY_RE = re.compile(r"^[A-Za-z]{3}$")


@dataclass(frozen=True)
class BarSeriesIdentity:
    """What a source's bars *mean*: adjustment basis, session alignment, volume basis."""

    provider: str
    feed: str
    legacy_adjustment: str
    interval: Interval = Interval.D1
    price_basis: PriceBasis = PriceBasis.UNKNOWN
    adjustment_scope: AdjustmentScope = AdjustmentScope.UNKNOWN
    adjustment_as_of: date | None = None
    session_semantics: SessionSemantics = SessionSemantics.UNKNOWN
    currency: str | None = None
    volume_basis: VolumeBasis = VolumeBasis.UNKNOWN

    def __post_init__(self) -> None:
        if self.currency is not None:
            if not isinstance(self.currency, str) or not _CURRENCY_RE.match(self.currency):
                raise ValueError(f"currency must be a 3-letter code or None: {self.currency!r}")
            object.__setattr__(self, "currency", self.currency.upper())

    @property
    def source(self) -> SourceIdentity:
        return SourceIdentity(self.provider, self.feed, self.legacy_adjustment)


_SPLIT_ADJUSTED = dict(
    price_basis=PriceBasis.SPLIT_ADJUSTED,
    adjustment_scope=AdjustmentScope.SPLITS,
    session_semantics=SessionSemantics.PROVIDER_ALIGNED,
)

# Conservative mapping; anything not listed resolves to all-unknown.
_CATALOG: dict[tuple[SourceIdentity, Interval], dict[str, object]] = {
    (ALPACA_IEX_SPLIT, Interval.D1): dict(**_SPLIT_ADJUSTED, volume_basis=VolumeBasis.SPLIT_ADJUSTED),
    (ALPACA_SIP_SPLIT, Interval.D1): dict(**_SPLIT_ADJUSTED, volume_basis=VolumeBasis.SPLIT_ADJUSTED),
    (MASSIVE_EOD_SPLIT, Interval.D1): dict(**_SPLIT_ADJUSTED, volume_basis=VolumeBasis.SPLIT_ADJUSTED),
    (ALPHA_VANTAGE_DAILY_RAW, Interval.D1): dict(
        price_basis=PriceBasis.RAW,
        adjustment_scope=AdjustmentScope.NONE,
        session_semantics=SessionSemantics.PROVIDER_ALIGNED,
        volume_basis=VolumeBasis.AS_TRADED,
    ),
    # Volume handling is provider-dependent, so it stays unknown.
    (TWELVE_DATA_DAILY_SPLIT, Interval.D1): dict(**_SPLIT_ADJUSTED, volume_basis=VolumeBasis.UNKNOWN),
    # Usable for inspection; not treated as proven-compatible.
    (YFINANCE_YAHOO_SPLIT, Interval.D1): dict(
        price_basis=PriceBasis.PROVIDER_DEFINED,
        adjustment_scope=AdjustmentScope.UNKNOWN,
        session_semantics=SessionSemantics.PROVIDER_ALIGNED,
        volume_basis=VolumeBasis.PROVIDER_DEFINED,
    ),
}


class BarSeriesCatalog:
    """Single place that maps a source identity to its series semantics."""

    @staticmethod
    def resolve(
        source: SourceIdentity,
        *,
        interval: Interval = Interval.D1,
        currency: str | None = "USD",
    ) -> BarSeriesIdentity:
        semantics = _CATALOG.get((source, interval), {})
        return BarSeriesIdentity(
            provider=source.provider,
            feed=source.feed,
            legacy_adjustment=source.adjustment,
            interval=interval,
            currency=currency,
            **semantics,  # type: ignore[arg-type]
        )


# --- research-daily-v2 canonical row -------------------------------------------------

SELECTION_PRIMARY_SOURCE = "primary_source"
SELECTION_PRIMARY_SOURCE_CORROBORATED = "primary_source_corroborated"
SELECTION_COMPATIBLE_GAP_FILL = "compatible_gap_fill"
SELECTION_FILL_PREFIX = "fill_"  # fill_<provider>
SELECTION_SPIKE_REPLACED = "spike_replaced"
SELECTION_REASONS = (
    SELECTION_PRIMARY_SOURCE,
    SELECTION_PRIMARY_SOURCE_CORROBORATED,
    SELECTION_COMPATIBLE_GAP_FILL,
    SELECTION_SPIKE_REPLACED,
)  # plus fill_<provider>

QUALITY_COMPLETE = "complete"
QUALITY_DIVERGENT = "divergent"
QUALITY_VOLUME_MISSING = "volume_missing"
QUALITY_SPIKE_SUSPECT = "spike_suspect"
QUALITY_STATUSES = (
    QUALITY_COMPLETE,
    QUALITY_DIVERGENT,
    QUALITY_VOLUME_MISSING,
    QUALITY_SPIKE_SUSPECT,
)


def is_valid_selection_reason(value: str) -> bool:
    if value in SELECTION_REASONS:
        return True
    return value.startswith(SELECTION_FILL_PREFIX) and len(value) > len(SELECTION_FILL_PREFIX)


@dataclass(frozen=True)
class CanonicalDailyRow:
    """One canonical daily row. Unique key: ``(instrument_id, session_date, policy_version)``."""

    UNIQUE_KEY: ClassVar[tuple[str, ...]] = ("instrument_id", "session_date", "policy_version")

    instrument_id: str
    session_date: date
    policy_version: str
    selected_source_bar_id: str | None
    provider: str
    feed: str
    adjustment: str  # legacy adjustment string of the selected source
    revision: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    selection_reason: str
    quality_status: str
    capabilities: tuple[str, ...]
    source_fingerprint: str
    generated_at: datetime

    def __post_init__(self) -> None:
        for name in ("instrument_id", "policy_version", "provider", "feed", "adjustment"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value:
                raise ValueError(f"{name} must be a non-empty string")
        if type(self.session_date) is not date:
            raise TypeError("session_date must be a date (not datetime)")
        if isinstance(self.revision, bool) or not isinstance(self.revision, int):
            raise TypeError("revision must be int")
        if self.revision < 0:
            raise ValueError(f"revision must be >= 0, got {self.revision}")
        if self.volume is None:
            raise ValueError("volume is required for a canonical row")
        o, h, low, c, v, _ = validate_ohlcv(self.open, self.high, self.low, self.close, self.volume)
        for name, value in (("open", o), ("high", h), ("low", low), ("close", c), ("volume", v)):
            object.__setattr__(self, name, value)
        if not is_valid_selection_reason(self.selection_reason):
            raise ValueError(f"invalid selection_reason: {self.selection_reason!r}")
        if self.quality_status not in QUALITY_STATUSES:
            raise ValueError(f"invalid quality_status: {self.quality_status!r}")
        if isinstance(self.capabilities, str):
            raise TypeError("capabilities must be a sequence of strings, not a string")
        caps = tuple(self.capabilities)
        if not all(isinstance(x, str) for x in caps):
            raise TypeError("capabilities must contain only strings")
        object.__setattr__(self, "capabilities", caps)
        if not isinstance(self.source_fingerprint, str):
            raise TypeError("source_fingerprint must be str")
        if self.generated_at.tzinfo is None or self.generated_at.utcoffset() is None:
            raise ValueError("generated_at must be timezone-aware")

    @property
    def unique_key(self) -> tuple[str, date, str]:
        return (self.instrument_id, self.session_date, self.policy_version)
