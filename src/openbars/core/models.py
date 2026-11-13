"""Core data models shared by every layer (see docs/architecture.md §3)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import Enum, Flag, auto
from typing import TYPE_CHECKING, Mapping

from openbars.core.ohlcv import validate_ohlcv

if TYPE_CHECKING:  # series imports Interval from here; avoid a runtime cycle
    from openbars.core.series import BarSeriesIdentity


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


class HourlyAlignment(str, Enum):
    """How US hourly bars are cut inside a session (see Discussion #112)."""

    SESSION_OPEN = "session_open"  # 09:30, 10:30, …; last bar may be shorter
    CLOCK_HOUR = "clock_hour"  # stubs to the next NY clock hour, then on the hour


class Adjustment(str, Enum):
    """Price basis of a bar series.

    v0.1 / ``research-daily-v2`` (US): storage holds bars **split-adjusted as delivered** by
    the source (backward-adjusted to the latest split, no dividend adjustment), so the stored
    column is ``SPLIT`` and the default read (``SPLIT``) is the stored value. ``RAW``,
    ``SPLIT_DIVIDEND``, ``ALL`` and ``SPLIT`` at an earlier ``as_of`` are derived at query
    time by ``openbars.normalize.adjust`` from the stored bars + corporate actions.

    ``cn-daily-v1`` (A-share): storage holds **unadjusted** ``RAW`` bars; ``QFQ`` (前复权) and
    ``HFQ`` (后复权) are derived at query time by ``openbars.normalize.cn_adjust`` from the
    stored bars + the adjust-factor table (never persisted: a forward adjustment would rewrite
    history on every new ex-date). A plain default read (``SPLIT``) of a CN series means "the
    stored series", i.e. ``RAW``.

    A later, optional US profile may store ``RAW`` bars and derive every other basis; the
    adjuster handles a stored ``RAW`` series as well (see ``normalize.adjust``).
    """

    RAW = "raw"
    SPLIT = "split"
    SPLIT_DIVIDEND = "split_dividend"
    ALL = "all"
    QFQ = "qfq"  # 前复权: A-share, anchored at the latest visible adjust-factor event
    HFQ = "hfq"  # 后复权: A-share, cumulative backward factor


class QualityFlag(Flag):
    NONE = 0
    DIVERGENT = auto()  # sources disagree beyond tolerance
    SPIKE_SUSPECT = auto()  # large move without a corporate action
    VOLUME_MISSING = auto()
    SINGLE_SOURCE = auto()
    FILLED_FROM_FALLBACK = auto()
    STALE = auto()  # fetched before a later split: the stored price basis may be stale
    GAP = auto()
    REVISED = auto()
    ADJUSTMENT_UNVERIFIED = auto()  # no corporate-action rows: the stored basis could not be verified/derived
    LIMIT_UP = auto()  # A-share: close at the daily upper price limit (cn_quality)
    LIMIT_DOWN = auto()  # A-share: close at the daily lower price limit
    SUSPENDED = auto()  # A-share: the session was a trading suspension (tradestatus 0)


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


def _set(obj: object, name: str, value: object) -> None:
    object.__setattr__(obj, name, value)


def _require_aware(value: datetime | None, name: str) -> None:
    if value is not None and (value.tzinfo is None or value.utcoffset() is None):
        raise ValueError(f"{name} must be timezone-aware")


@dataclass(frozen=True)
class Bar:
    """One OHLCV bar. Prices/volume are ``Decimal`` (never float); see ``core.ohlcv``."""

    instrument_id: str
    interval: Interval
    ts_start: datetime  # UTC bar start
    session_date: date  # exchange-local trading day
    session: Session
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal | None
    source: str
    feed: str | None = None  # e.g. "alpaca:sip"; volumes never mixed across feeds
    adjustment: Adjustment = Adjustment.RAW
    vwap: Decimal | None = None
    trade_count: int | None = None
    flags: QualityFlag = QualityFlag.NONE
    ingested_at: datetime | None = None

    def __post_init__(self) -> None:
        _require_aware(self.ts_start, "ts_start")
        _require_aware(self.ingested_at, "ingested_at")
        o, h, low, c, v, w = validate_ohlcv(
            self.open, self.high, self.low, self.close, self.volume, self.vwap
        )
        for name, value in (
            ("open", o), ("high", h), ("low", low), ("close", c), ("volume", v), ("vwap", w)
        ):
            _set(self, name, value)


@dataclass(frozen=True)
class RawBar:
    """A bar exactly as one source delivered it (revision >= 1)."""

    provider: str
    symbol_as_returned: str
    fetch_id: str
    interval: Interval
    ts_start: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal | None
    feed: str | None = None
    extra: Mapping[str, object] = field(default_factory=dict)
    vwap: Decimal | None = None
    payload_hash: str | None = None
    revision: int = 1
    instrument_id: str | None = None  # set once the symbol is resolved
    session_date: date | None = None
    series: BarSeriesIdentity | None = None  # series semantics (see core.series)

    def __post_init__(self) -> None:
        _require_aware(self.ts_start, "ts_start")
        if isinstance(self.revision, bool) or not isinstance(self.revision, int):
            raise TypeError("revision must be int")
        if self.revision < 1:
            raise ValueError(f"revision must be >= 1, got {self.revision}")
        o, h, low, c, v, w = validate_ohlcv(
            self.open, self.high, self.low, self.close, self.volume, self.vwap
        )
        for name, value in (
            ("open", o), ("high", h), ("low", low), ("close", c), ("volume", v), ("vwap", w)
        ):
            _set(self, name, value)
        if self.series is not None:
            if self.series.provider != self.provider:
                raise ValueError("series.provider does not match provider")
            if self.feed is not None and self.series.feed != self.feed:
                raise ValueError("series.feed does not match feed")
            if self.series.interval != self.interval:
                raise ValueError("series.interval does not match interval")


@dataclass(frozen=True)
class CanonicalBar:
    """A reconciled bar. The wrapped ``Bar`` is already validated on construction."""

    bar: Bar
    price_source: str
    volume_source: str | None
    sources_seen: tuple[str, ...]
    reconcile_rule: str
    revision: int = 0

    def __post_init__(self) -> None:
        if not isinstance(self.bar, Bar):
            raise TypeError("bar must be a Bar")
        if isinstance(self.revision, bool) or not isinstance(self.revision, int):
            raise TypeError("revision must be int")
        if self.revision < 0:
            raise ValueError(f"revision must be >= 0, got {self.revision}")
        _set(self, "sources_seen", tuple(self.sources_seen))


class CorporateActionKind(str, Enum):
    SPLIT = "split"
    DIVIDEND = "dividend"
    SPINOFF = "spinoff"
    SYMBOL_CHANGE = "symbol_change"
    RIGHTS = "rights"  # 供股: recorded only, never applied to prices


def _exact_decimal(value: object, name: str) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, bool) or isinstance(value, float):
        raise TypeError(f"{name} must be Decimal, int or str, not {type(value).__name__}")
    if isinstance(value, Decimal):
        result = value
    elif isinstance(value, (int, str)):
        try:
            result = Decimal(value.strip() if isinstance(value, str) else value)
        except InvalidOperation:
            raise ValueError(f"{name} is not a valid decimal: {value!r}") from None
    else:
        raise TypeError(f"{name} must be Decimal, int or str, not {type(value).__name__}")
    if not result.is_finite():
        raise ValueError(f"{name} must be finite, got {result}")
    return result


@dataclass(frozen=True)
class CorporateAction:
    """One corporate action as one source reported it.

    ``ratio`` is new shares / old shares (4-for-1 -> ``Decimal("4")``, 1-for-4 reverse ->
    ``Decimal("0.25")``); ``amount`` is a cash amount per share. Both are exact ``Decimal``
    (never ``float``). A split must have a positive ratio (``None`` = the source gave none).
    """

    instrument_id: str
    ex_date: date
    kind: CorporateActionKind
    source: str
    ratio: Decimal | None = None
    amount: Decimal | None = None
    currency: str | None = None  # ISO 4217 of ``amount``; None = same currency as the bars

    def __post_init__(self) -> None:
        _set(self, "ratio", _exact_decimal(self.ratio, "ratio"))
        _set(self, "amount", _exact_decimal(self.amount, "amount"))
        if self.kind is CorporateActionKind.SPLIT and self.ratio is not None and self.ratio <= 0:
            raise ValueError(f"split ratio must be positive, got {self.ratio}")
        if self.currency is not None:
            cur = self.currency.strip() if isinstance(self.currency, str) else ""
            if len(cur) != 3 or not cur.isalpha():
                raise ValueError(f"currency must be a 3-letter code or None, got {self.currency!r}")
            _set(self, "currency", cur.upper())
