"""Provider contract and capability declarations."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from enum import Enum
from typing import Iterable, Protocol, Sequence

from openbars.core.errors import ProviderError
from openbars.core.models import (
    Adjustment,
    AssetClass,
    CorporateAction,
    DateRange,
    Interval,
    Market,
    RawBar,
)
from openbars.quota.policy import QuotaPolicy

API_VERSION = 1


class BatchShape(str, Enum):
    PER_SYMBOL = "per_symbol"
    MULTI_SYMBOL = "multi_symbol"
    WHOLE_MARKET_PER_DAY = "whole_market_per_day"


@dataclass(frozen=True)
class ProviderTerms:
    personal_only: bool
    redistribution_allowed: bool
    url: str | None = None
    notes: str | None = None


@dataclass(frozen=True)
class SourceCapability:
    provider: str
    market: Market
    interval: Interval
    asset_classes: frozenset[AssetClass]
    history_start: date | None  # None = since listing
    rolling_window: timedelta | None = None  # e.g. Massive Basic ~2 years
    latency: timedelta = timedelta(0)  # earliest availability after session close
    batch: BatchShape = BatchShape.PER_SYMBOL
    max_symbols_per_request: int = 1
    max_bars_per_request: int | None = None
    adjusted_modes: frozenset[Adjustment] = frozenset({Adjustment.RAW})
    feed: str | None = None
    terms: ProviderTerms = field(default_factory=lambda: ProviderTerms(True, False))


@dataclass(frozen=True)
class ProviderInstrument:
    symbol: str
    market: Market
    asset_class: AssetClass | None
    name: str | None = None
    exchange: str | None = None
    active: bool = True
    native_id: str | None = None  # provider's own instrument id, when it publishes one
    base_asset: str | None = None  # crypto pairs: base / quote asset (see normalize.crypto_pairs)
    quote_asset: str | None = None


@dataclass(frozen=True)
class FetchRequest:
    market: Market
    interval: Interval
    symbols: tuple[str, ...]
    window: DateRange
    cursor: str | None = None
    feed: str | None = None  # provider feed, e.g. "sip"; None = provider default
    adjustment: str | None = None  # legacy adjustment string ("split" / "raw"); None = provider default


@dataclass(frozen=True)
class RawBatch:
    provider: str
    fetch_id: str
    request: FetchRequest
    bars: tuple[RawBar, ...]
    next_cursor: str | None = None
    error: ProviderError | None = None  # set on a partial batch: bars so far + why it stopped


class Provider(Protocol):
    id: str
    api_version: int

    def capabilities(self) -> Sequence[SourceCapability]: ...

    def quota_policy(self) -> QuotaPolicy: ...

    def list_instruments(self, market: Market) -> Iterable[ProviderInstrument]: ...

    def fetch_bars(self, request: FetchRequest) -> RawBatch: ...

    def fetch_corporate_actions(self, request: FetchRequest) -> Iterable[CorporateAction]: ...
