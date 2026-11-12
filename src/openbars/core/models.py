"""Core data models shared by every layer (see docs/architecture.md §3)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from enum import Enum, Flag, auto
from typing import Mapping


class Market(str, Enum):
    US = "us"
    CRYPTO = "crypto"
    CN = "cn"
    HK = "hk"


class AssetClass(str, Enum):
    EQUITY = "equity"
    ETF = "etf"
    ADR = "adr"
    INDEX = "index"
    FUND = "fund"
    CRYPTO_SPOT = "crypto_spot"


class Interval(str, Enum):
    D1 = "1d"
    H1 = "1h"
    M30 = "30m"
    M15 = "15m"
    M5 = "5m"
    M1 = "1m"

    @property
    def is_intraday(self) -> bool:
        return self is not Interval.D1

    @property
    def duration(self) -> timedelta:
        return {
            "1d": timedelta(days=1),
            "1h": timedelta(hours=1),
            "30m": timedelta(minutes=30),
            "15m": timedelta(minutes=15),
            "5m": timedelta(minutes=5),
            "1m": timedelta(minutes=1),
        }[self.value]


class Session(str, Enum):
    REGULAR = "regular"
    PRE = "pre"
    POST = "post"
    CONTINUOUS = "continuous"  # 24/7 markets (crypto)


class Adjustment(str, Enum):
    """Read-time adjustment. Storage always holds RAW + corporate actions."""

    RAW = "raw"
    SPLIT = "split"
    SPLIT_DIVIDEND = "split_dividend"
    ALL = "all"


class QualityFlag(Flag):
    NONE = 0
    DIVERGENT = auto()  # sources disagree beyond tolerance
    SPIKE_SUSPECT = auto()  # large move without a corporate action
    VOLUME_MISSING = auto()
    SINGLE_SOURCE = auto()
    FILLED_FROM_FALLBACK = auto()
    STALE = auto()
    GAP = auto()
    REVISED = auto()


@dataclass(frozen=True)
class DateRange:
    start: date
    end: date  # inclusive


@dataclass(frozen=True)
class Instrument:
    instrument_id: str  # stable internal id, never reused
    market: Market
    asset_class: AssetClass
    currency: str
    name: str | None = None
    exchange: str | None = None  # MIC
    listed_on: date | None = None
    delisted_on: date | None = None
    external_ids: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class SymbolMapping:
    """A symbol points to an instrument only within [valid_from, valid_to]."""

    instrument_id: str
    symbol: str
    valid_from: date
    valid_to: date | None = None
    provider: str = "*"


@dataclass(frozen=True)
class Bar:
    instrument_id: str
    interval: Interval
    ts_start: datetime  # UTC bar start
    session_date: date  # exchange-local trading day
    session: Session
    open: float
    high: float
    low: float
    close: float
    volume: float | None
    source: str
    feed: str | None = None  # e.g. "alpaca:sip"; volumes never mixed across feeds
    adjustment: Adjustment = Adjustment.RAW
    vwap: float | None = None
    trade_count: int | None = None
    flags: QualityFlag = QualityFlag.NONE


@dataclass(frozen=True)
class RawBar:
    provider: str
    symbol_as_returned: str
    fetch_id: str
    interval: Interval
    ts_start: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float | None
    feed: str | None = None
    extra: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class CanonicalBar:
    bar: Bar
    price_source: str
    volume_source: str | None
    sources_seen: tuple[str, ...]
    reconcile_rule: str
    revision: int = 0


class CorporateActionKind(str, Enum):
    SPLIT = "split"
    DIVIDEND = "dividend"
    SPINOFF = "spinoff"
    SYMBOL_CHANGE = "symbol_change"


@dataclass(frozen=True)
class CorporateAction:
    instrument_id: str
    ex_date: date
    kind: CorporateActionKind
    source: str
    ratio: float | None = None
    amount: float | None = None
