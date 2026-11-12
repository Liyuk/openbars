"""Storage row types: exact-``Decimal`` views of the Arrow schemas in ``schema.py``.

The current ``openbars.core.models`` bar dataclasses use ``Decimal`` prices. To
avoid a lossy round trip the storage layer works on these row types instead:

* ingesting a model object converts floats with ``Decimal(str(value))`` (shortest repr)
  and rounds to the column scale (8) with round-half-even;
* ``Decimal`` / ``str`` / ``int`` inputs are never altered: more than 8 fractional digits
  (or too many integer digits) is an error, not a silent rounding;
* scanning back yields ``Decimal`` rows (``scan_rows`` / ``scan_table``). The protocol
  methods ``scan`` return ``RawBar`` / ``CanonicalBar`` with ``Decimal`` prices and volumes.

Fields the legacy models do not carry (``market``, ``session_date``, price-basis
metadata, ``policy_version`` ...) are taken from ``RawBar.extra`` / explicit arguments
and documented in architecture.md section 10.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass, field, fields
from datetime import UTC, date, datetime
from decimal import ROUND_HALF_EVEN, Context, Decimal, InvalidOperation
from enum import Enum
from typing import Any, Iterable, Mapping, Sequence, TypeVar

import pyarrow as pa

from openbars.core.models import (
    Adjustment,
    CanonicalBar,
    Bar,
    Interval,
    Market,
    QualityFlag,
    RawBar,
    Session,
)
from openbars.core.series import (
    BarSeriesIdentity, PriceBasis as SeriesPriceBasis, AdjustmentScope as SeriesAdjustmentScope,
    SessionSemantics as SeriesSessionSemantics, VolumeBasis as SeriesVolumeBasis,
)
from openbars.providers.base import RawBatch
from openbars.storage.schema import (
    CANONICAL_BARS_SCHEMA,
    DEFAULT_POLICY_VERSION,
    PRICE_SCALE,
    RAW_BARS_PARQUET_SCHEMA,
    RAW_BARS_SCHEMA,
    UNKNOWN,
    AdjustmentScope,
    PriceBasis,
    SessionSemantics,
    VolumeBasis,
)

_CTX = Context(prec=60)
_CURRENCY = re.compile(r"^[A-Z]{3}$")


class InvalidBarError(ValueError):
    """A bar violates the OHLCV invariants or the column precision."""


# --- scalar coercion ---------------------------------------------------------------------


def to_decimal(value: Any, *, precision: int, scale: int = PRICE_SCALE, name: str = "value") -> Decimal:
    if isinstance(value, bool):
        raise InvalidBarError(f"{name}: bool is not a number")
    exact = True
    if isinstance(value, float):
        if not math.isfinite(value):
            raise InvalidBarError(f"{name}: non-finite float")
        value = Decimal(str(value))  # shortest repr, never the binary expansion
        exact = False
    try:
        dec = value if isinstance(value, Decimal) else Decimal(str(value))
    except InvalidOperation as exc:
        raise InvalidBarError(f"{name}: not a number: {value!r}") from exc
    if not dec.is_finite():
        raise InvalidBarError(f"{name}: non-finite decimal")
    quantum = Decimal(1).scaleb(-scale)
    try:
        q = dec.quantize(quantum, rounding=ROUND_HALF_EVEN, context=_CTX)
    except InvalidOperation as exc:  # pragma: no cover - absurd magnitudes
        raise InvalidBarError(f"{name}: out of range") from exc
    if exact and q != dec:
        raise InvalidBarError(f"{name}: {dec} needs more than {scale} fractional digits")
    if len(q.as_tuple().digits) > precision:
        raise InvalidBarError(f"{name}: {dec} exceeds decimal({precision},{scale})")
    return q


def _price(value: Any, name: str) -> Decimal:
    return to_decimal(value, precision=20, name=name)


def _opt_price(value: Any, name: str) -> Decimal | None:
    return None if value is None else _price(value, name)


def _volume(value: Any, name: str = "volume") -> Decimal | None:
    return None if value is None else to_decimal(value, precision=28, name=name)


def _utc(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime):
        raise InvalidBarError(f"{name}: expected datetime")
    if value.tzinfo is None:
        raise InvalidBarError(f"{name}: naive datetime; storage requires tz-aware UTC")
    return value.astimezone(UTC)


def _date(value: Any, name: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value)
    raise InvalidBarError(f"{name}: expected date, got {type(value).__name__}")


def _opt_date(value: Any, name: str) -> date | None:
    return None if value is None else _date(value, name)


def _enum_value(enum_cls: type[Enum], value: Any, name: str) -> str:
    try:
        return enum_cls(value).value
    except ValueError as exc:
        raise InvalidBarError(f"{name}: {value!r} is not a valid {enum_cls.__name__}") from exc


def _set(obj: Any, name: str, value: Any) -> None:
    object.__setattr__(obj, name, value)


def validate_ohlcv(
    open_: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    volume: Decimal | None,
) -> list[str]:
    """OHLCV invariants (same rules as the upstream internal implementation)."""
    errors: list[str] = []
    if min(open_, high, low, close) <= 0:
        errors.append("prices must be > 0")
    if high < low:
        errors.append("high < low")
    if not low <= open_ <= high:
        errors.append("open outside [low, high]")
    if not low <= close <= high:
        errors.append("close outside [low, high]")
    if volume is not None and volume < 0:
        errors.append("volume < 0")
    return errors


def flags_to_names(flags: QualityFlag) -> tuple[str, ...]:
    return tuple(f.name for f in QualityFlag if f is not QualityFlag.NONE and f in flags)  # type: ignore[misc]


def names_to_flags(names: Iterable[str]) -> QualityFlag:
    out = QualityFlag.NONE
    for name in names:
        out |= QualityFlag[name]
    return out


def quality_status_for(flags: QualityFlag) -> str:
    """Map flags to the upstream ``quality_status`` vocabulary."""
    if QualityFlag.DIVERGENT in flags:
        return "divergent"
    if QualityFlag.SPIKE_SUSPECT in flags:
        return "spike_suspect"
    if QualityFlag.VOLUME_MISSING in flags:
        return "volume_missing"
    return "complete"


# --- raw rows ----------------------------------------------------------------------------

# ``RawBar.extra`` keys that map to a first-class ``RawBarRow`` column, or that ``to_raw_bar``
# synthesises from one; they are never stored in the leftover ``extra`` JSON.
_FIRST_CLASS_EXTRA = frozenset(
    {
        "session_date", "instrument_id", "adjustment", "price_basis", "adjustment_scope",
        "adjustment_as_of", "session_semantics", "currency", "volume_basis", "revision", "vwap",
        "trade_count", "market", "source_artifact_ref", "ingest_run_id", "ingested_at",
        "open_exact", "high_exact", "low_exact", "close_exact", "volume_exact",
    }
)  # fmt: skip
_TAGS = {"$decimal": Decimal, "$date": date.fromisoformat, "$datetime": datetime.fromisoformat}


def leftover_extra(extra: Mapping[str, Any]) -> dict[str, object] | None:
    """The ``RawBar.extra`` entries without a column of their own (``None`` when there are none)."""
    rest = {k: v for k, v in extra.items() if k not in _FIRST_CLASS_EXTRA}
    return rest or None


def _json_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise InvalidBarError("extra: NaN / infinity cannot be stored")
        return value
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise InvalidBarError("extra: NaN / infinity cannot be stored")
        return {"$decimal": str(value)}
    if isinstance(value, datetime):  # before date: datetime is a date
        return {"$datetime": value.isoformat()}
    if isinstance(value, date):
        return {"$date": value.isoformat()}
    if isinstance(value, Mapping):
        return {str(k): _json_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(v) for v in value]
    return str(value)


def encode_extra(extra: Mapping[str, object] | None) -> str | None:
    """``RawBar.extra`` leftovers -> a deterministic JSON object string (``None`` when empty).

    ``Decimal`` / ``date`` / ``datetime`` become ``{"$decimal": "10.15"}`` / ``{"$date": ...}``
    / ``{"$datetime": ...}`` and come back as the same types; ``bool`` / ``int`` / ``str`` /
    ``None`` are native JSON. Keys are sorted so an unchanged row is a no-op re-write.
    """
    if not extra:
        return None
    return json.dumps(_json_value(dict(extra)), sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _revive(obj: dict[str, Any]) -> Any:
    if len(obj) == 1:
        (key, value), = obj.items()
        if key in _TAGS and isinstance(value, str):
            return _TAGS[key](value)
    return obj


def _revive_tree(obj: Any) -> Any:
    """Walk a parsed JSON value and revive ``$decimal`` / ``$date`` / ``$datetime`` tags."""
    if isinstance(obj, list):
        return [_revive_tree(v) for v in obj]
    if isinstance(obj, dict):
        return _revive({k: _revive_tree(v) for k, v in obj.items()})
    return obj


def decode_extra(value: Mapping[str, object] | str | None) -> Mapping[str, object] | None:
    """Inverse of ``encode_extra``; a mapping (e.g. JSONB) is revived too; empty / null is ``None``."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    if isinstance(value, str):
        try:
            value = json.loads(value, object_hook=_revive)
        except ValueError as exc:
            raise InvalidBarError(f"extra is not valid JSON: {exc}") from None
    elif isinstance(value, Mapping):
        value = _revive_tree(dict(value))
    if not isinstance(value, Mapping):
        raise InvalidBarError("extra must be a JSON object")
    return dict(value) or None


@dataclass(frozen=True)
class RawBarRow:
    provider: str
    market: Market
    symbol_as_returned: str
    interval: Interval
    ts_start: datetime
    session_date: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    fetch_id: str
    volume: Decimal | None = None
    feed: str = ""
    adjustment: str = "raw"
    instrument_id: str | None = None
    price_basis: str = PriceBasis.UNKNOWN.value
    adjustment_scope: str = AdjustmentScope.UNKNOWN.value
    adjustment_as_of: date | None = None
    session_semantics: str = SessionSemantics.UNKNOWN.value
    currency: str | None = None
    volume_basis: str = VolumeBasis.UNKNOWN.value
    revision: int = 0
    vwap: Decimal | None = None
    trade_count: int | None = None
    source_artifact_ref: str | None = None
    ingest_run_id: str | None = None
    ingested_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    # ``RawBar.extra`` keys without a column of their own (tradestatus, isST, preclose, ...).
    # Parquet stores a JSON string; Postgres native stores JSONB (a mapping is accepted here
    # and decoded). ``None`` = nothing to keep.
    extra: Mapping[str, object] | str | None = None

    def __post_init__(self) -> None:
        if not self.provider or not self.symbol_as_returned or not self.fetch_id:
            raise InvalidBarError("provider, symbol_as_returned and fetch_id are required")
        _set(self, "extra", decode_extra(self.extra))
        _set(self, "market", Market(self.market))
        _set(self, "interval", Interval(self.interval))
        _set(self, "ts_start", _utc(self.ts_start, "ts_start"))
        _set(self, "session_date", _date(self.session_date, "session_date"))
        for name in ("open", "high", "low", "close"):
            _set(self, name, _price(getattr(self, name), name))
        _set(self, "volume", _volume(self.volume))
        _set(self, "vwap", _opt_price(self.vwap, "vwap"))
        _set(self, "feed", self.feed or "")
        _set(self, "adjustment", str(getattr(self.adjustment, "value", self.adjustment)))
        _set(self, "price_basis", _enum_value(PriceBasis, self.price_basis, "price_basis"))
        _set(self, "adjustment_scope", _enum_value(AdjustmentScope, self.adjustment_scope, "adjustment_scope"))
        _set(self, "session_semantics", _enum_value(SessionSemantics, self.session_semantics, "session_semantics"))
        _set(self, "volume_basis", _enum_value(VolumeBasis, self.volume_basis or UNKNOWN, "volume_basis"))
        _set(self, "adjustment_as_of", _opt_date(self.adjustment_as_of, "adjustment_as_of"))
        if self.currency is not None:
            cur = str(self.currency).upper()
            if not _CURRENCY.fullmatch(cur):
                raise InvalidBarError(f"currency: {self.currency!r} is not a 3-letter code")
            _set(self, "currency", cur)
        _set(self, "revision", int(self.revision))
        _set(self, "ingested_at", _utc(self.ingested_at, "ingested_at"))

    def validation_errors(self) -> list[str]:
        return validate_ohlcv(self.open, self.high, self.low, self.close, self.volume)

    def key(self) -> tuple:
        daily = self.interval is Interval.D1
        return (
            self.market.value,
            self.interval.value,
            self.provider,
            self.feed,
            self.adjustment,
            self.revision,
            self.instrument_id or self.symbol_as_returned,
            self.session_date if daily else None,
            None if daily else self.ts_start,
        )

    def to_raw_bar(self) -> RawBar:
        """In-memory ``RawBar``. Exact ``Decimal`` values; extra fields travel in ``extra``."""
        extra: dict[str, object] = {
            **(self.extra or {}),  # leftovers first: a first-class column below always wins
            "market": self.market.value,
            "instrument_id": self.instrument_id,
            "session_date": self.session_date,
            "adjustment": self.adjustment,
            "price_basis": self.price_basis,
            "adjustment_scope": self.adjustment_scope,
            "adjustment_as_of": self.adjustment_as_of,
            "session_semantics": self.session_semantics,
            "currency": self.currency,
            "volume_basis": self.volume_basis,
            "revision": self.revision,
            "vwap": self.vwap,
            "trade_count": self.trade_count,
            "source_artifact_ref": self.source_artifact_ref,
            "ingest_run_id": self.ingest_run_id,
            "ingested_at": self.ingested_at,  # #154: canonical ``available_at`` of a selected source
            "open_exact": self.open,
            "high_exact": self.high,
            "low_exact": self.low,
            "close_exact": self.close,
            "volume_exact": self.volume,
        }
        series_data = extra.pop("_bar_series", None)
        series = None
        if isinstance(series_data, dict):
            series_data = dict(series_data)
            for name, enum in (("interval", Interval), ("price_basis", SeriesPriceBasis),
                               ("adjustment_scope", SeriesAdjustmentScope),
                               ("session_semantics", SeriesSessionSemantics),
                               ("volume_basis", SeriesVolumeBasis)):
                series_data[name] = enum(series_data[name])
            series = BarSeriesIdentity(**series_data)
        return RawBar(
            provider=self.provider,
            symbol_as_returned=self.symbol_as_returned,
            fetch_id=self.fetch_id,
            interval=self.interval,
            ts_start=self.ts_start,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            volume=self.volume,
            feed=self.feed or None,
            extra=extra,
            session_date=self.session_date,
            instrument_id=self.instrument_id if self.interval is Interval.H1 else None,
            series=series,
        )


def raw_ingested_at(bar: RawBar) -> datetime | None:
    """The stored raw row's ``ingested_at`` (``to_raw_bar`` puts it in ``extra``), else None.

    Bars straight from a provider (not read back from the raw layer) have none.
    """
    value = bar.extra.get("ingested_at")
    if isinstance(value, datetime) and value.tzinfo is not None and value.utcoffset() is not None:
        return value.astimezone(UTC)
    return None


def available_by_utc(value: datetime) -> datetime:
    """Validate a point-in-time ``available_by`` (#154): tz-aware, returned in UTC."""
    if not isinstance(value, datetime):
        raise ValueError("available_by must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("available_by must be timezone-aware")
    return value.astimezone(UTC)


def is_available(row: Any, available_by: datetime) -> bool:
    """``available_at <= available_by``; a row without ``available_at`` is not available."""
    at = getattr(row, "available_at", None)
    return at is not None and at <= available_by


def hourly_adjustment(bar: RawBar) -> str:
    """Resolve the adjustment identity consistently before and after storage."""
    return bar.series.legacy_adjustment if bar.series else str(bar.extra.get("adjustment") or "split")


def raw_rows_from_batch(
    batch: RawBatch,
    *,
    source_artifact_ref: str | None = None,
    ingest_run_id: str | None = None,
    ingested_at: datetime | None = None,
) -> list[RawBarRow]:
    """Map a ``RawBatch`` to storage rows.

    ``market`` comes from ``batch.request``. Everything the legacy ``RawBar`` lacks is read
    from ``RawBar.extra`` (keys: ``instrument_id``, ``session_date``, ``adjustment``,
    ``price_basis``, ``adjustment_scope``, ``adjustment_as_of``, ``session_semantics``,
    ``currency``, ``volume_basis``, ``revision``, ``vwap``, ``trade_count``); every other key
    is kept in ``RawBarRow.extra`` (stored by the Parquet raw layer, see ``leftover_extra``).
    ``session_date`` is ``extra["session_date"]``, else ``RawBar.session_date``
    (A-share bars start at 00:00 Asia/Shanghai, the previous UTC day), else the UTC date of
    ``ts_start`` - normalizers should supply the exchange-local session date.
    """
    now = ingested_at or datetime.now(UTC)
    rows: list[RawBarRow] = []
    for bar in batch.bars:
        if bar.provider != batch.provider:
            raise InvalidBarError(f"bar provider {bar.provider!r} != batch provider {batch.provider!r}")
        extra: Mapping[str, Any] = bar.extra
        ts = _utc(bar.ts_start, "ts_start")
        leftovers = leftover_extra(extra) or {}
        if bar.interval in (Interval.D1, Interval.H1) and bar.series is not None:
            leftovers["_bar_series"] = {
                f.name: getattr(bar.series, f.name) for f in fields(BarSeriesIdentity)
            }
        rows.append(
            RawBarRow(
                provider=bar.provider,
                market=batch.request.market,
                symbol_as_returned=bar.symbol_as_returned,
                interval=bar.interval,
                ts_start=ts,
                session_date=extra.get("session_date") or bar.session_date or ts.date(),
                open=bar.open,
                high=bar.high,
                low=bar.low,
                close=bar.close,
                volume=bar.volume,
                fetch_id=bar.fetch_id or batch.fetch_id,
                feed=bar.feed or "",
                adjustment=(hourly_adjustment(bar) if bar.interval in (Interval.D1, Interval.H1) else extra.get("adjustment") or Adjustment.RAW.value),
                instrument_id=(bar.instrument_id if bar.interval is Interval.H1 else None) or extra.get("instrument_id"),
                price_basis=extra.get("price_basis") or UNKNOWN,
                adjustment_scope=extra.get("adjustment_scope") or UNKNOWN,
                adjustment_as_of=extra.get("adjustment_as_of"),
                session_semantics=extra.get("session_semantics") or UNKNOWN,
                currency=extra.get("currency"),
                volume_basis=extra.get("volume_basis") or UNKNOWN,
                revision=extra.get("revision") or 0,
                vwap=extra.get("vwap"),
                trade_count=extra.get("trade_count"),
                source_artifact_ref=source_artifact_ref,
                ingest_run_id=ingest_run_id,
                ingested_at=now,
                extra=leftovers or None,
            )
        )
    return rows


# --- canonical rows ----------------------------------------------------------------------


@dataclass(frozen=True)
class CanonicalBarRow:
    instrument_id: str
    market: Market
    interval: Interval
    ts_start: datetime
    session_date: date
    session: Session
    provider: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    price_source: str
    reconcile_rule: str
    policy_version: str = DEFAULT_POLICY_VERSION
    feed: str = ""
    adjustment: str = "raw"
    revision: int = 0
    volume: Decimal | None = None
    vwap: Decimal | None = None
    trade_count: int | None = None
    flags: tuple[str, ...] = ()
    selection_reason: str = "unspecified"
    quality_status: str = "complete"
    capabilities: tuple[str, ...] = ()
    source_fingerprint: str = ""
    volume_source: str | None = None
    sources_seen: tuple[str, ...] = ()
    generated_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    # #154: earliest moment the bar may be used for research (selected source's raw
    # ``ingested_at`` / upstream ``received_at``); None = unknown (hidden by ``available_by``).
    available_at: datetime | None = None
    currency: str | None = None  # ISO 4217 of the bar prices; not a schema column (see capabilities)

    def __post_init__(self) -> None:
        if not self.instrument_id or not self.policy_version or not self.provider:
            raise InvalidBarError("instrument_id, policy_version and provider are required")
        _set(self, "market", Market(self.market))
        _set(self, "interval", Interval(self.interval))
        _set(self, "session", Session(self.session))
        _set(self, "ts_start", _utc(self.ts_start, "ts_start"))
        _set(self, "session_date", _date(self.session_date, "session_date"))
        for name in ("open", "high", "low", "close"):
            _set(self, name, _price(getattr(self, name), name))
        _set(self, "volume", _volume(self.volume))
        _set(self, "vwap", _opt_price(self.vwap, "vwap"))
        _set(self, "feed", self.feed or "")
        _set(self, "adjustment", str(getattr(self.adjustment, "value", self.adjustment)))
        _set(self, "revision", int(self.revision))
        for name in ("flags", "capabilities", "sources_seen"):
            _set(self, name, tuple(getattr(self, name)))
        for flag in self.flags:
            if flag not in QualityFlag.__members__ or flag == "NONE":
                raise InvalidBarError(f"flags: unknown quality flag {flag!r}")
        _set(self, "generated_at", _utc(self.generated_at, "generated_at"))
        if self.available_at is not None:
            _set(self, "available_at", _utc(self.available_at, "available_at"))
        if self.currency is None:
            for cap in self.capabilities:
                if isinstance(cap, str) and cap.lower().startswith("currency:"):
                    token = cap.split(":", 1)[1]
                    if len(token) == 3 and token.isalpha():
                        _set(self, "currency", token.upper())
                        break
        if self.currency is not None:
            cur = str(self.currency).upper()
            if len(cur) != 3 or not cur.isalpha():
                raise InvalidBarError(f"currency: {self.currency!r} is not a 3-letter code")
            _set(self, "currency", cur)
        if not self.source_fingerprint:
            _set(self, "source_fingerprint", self._fingerprint())

    def _fingerprint(self) -> str:
        payload = json.dumps(
            [
                self.instrument_id,
                self.interval.value,
                self.session_date.isoformat(),
                self.provider,
                self.feed,
                self.adjustment,
                self.revision,
                *(str(v) for v in (self.open, self.high, self.low, self.close, self.volume)),
            ],
            separators=(",", ":"),
        )
        return hashlib.sha256(payload.encode()).hexdigest()

    def validation_errors(self) -> list[str]:
        return validate_ohlcv(self.open, self.high, self.low, self.close, self.volume)

    def key(self) -> tuple:
        daily = self.interval is Interval.D1
        return (
            self.instrument_id,
            self.interval.value,
            self.policy_version,
            self.session_date if daily else None,
            None if daily else self.ts_start,
        )

    @classmethod
    def from_canonical(
        cls,
        canonical: CanonicalBar,
        *,
        market: Market,
        policy_version: str = DEFAULT_POLICY_VERSION,
        selection_reason: str = "unspecified",
        quality_status: str | None = None,
        capabilities: Sequence[str] = (),
        source_fingerprint: str = "",
        generated_at: datetime | None = None,
    ) -> "CanonicalBarRow":
        bar = canonical.bar
        return cls(
            instrument_id=bar.instrument_id,
            market=market,
            interval=bar.interval,
            ts_start=bar.ts_start,
            session_date=bar.session_date,
            session=bar.session,
            provider=bar.source,
            feed=bar.feed or "",
            adjustment=bar.adjustment.value,
            revision=canonical.revision,
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=bar.volume,
            vwap=bar.vwap,
            trade_count=bar.trade_count,
            flags=flags_to_names(bar.flags),
            selection_reason=selection_reason,
            quality_status=quality_status or quality_status_for(bar.flags),
            capabilities=tuple(capabilities),
            source_fingerprint=source_fingerprint,
            price_source=canonical.price_source,
            volume_source=canonical.volume_source,
            sources_seen=canonical.sources_seen,
            reconcile_rule=canonical.reconcile_rule,
            policy_version=policy_version,
            generated_at=generated_at or datetime.now(UTC),
        )

    def to_canonical_bar(self) -> CanonicalBar:
        """In-memory ``CanonicalBar``. ``policy_version`` and friends are not on the model."""
        bar = Bar(
            instrument_id=self.instrument_id,
            interval=self.interval,
            ts_start=self.ts_start,
            session_date=self.session_date,
            session=self.session,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            volume=self.volume,
            source=self.provider,
            feed=self.feed or None,
            adjustment=Adjustment(self.adjustment),
            vwap=self.vwap,
            trade_count=self.trade_count,
            flags=names_to_flags(self.flags),
        )
        return CanonicalBar(
            bar=bar,
            price_source=self.price_source,
            volume_source=self.volume_source,
            sources_seen=self.sources_seen,
            reconcile_rule=self.reconcile_rule,
            revision=self.revision,
        )


def check_rows(rows: Sequence[Any], what: str) -> None:
    bad = [(r, r.validation_errors()) for r in rows]
    bad = [(r, e) for r, e in bad if e]
    if bad:
        first, errors = bad[0]
        ident = getattr(first, "symbol_as_returned", None) or getattr(first, "instrument_id", "?")
        raise InvalidBarError(
            f"{len(bad)} invalid {what} bar(s); first: {ident} {first.session_date}: {', '.join(errors)}"
        )


# --- Arrow conversion --------------------------------------------------------------------

R = TypeVar("R")


def _cell(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, tuple):
        return list(value)
    return value


def rows_to_table(rows: Sequence[Any], schema: pa.Schema) -> pa.Table:
    arrays = [
        pa.array([_cell(getattr(r, f.name)) for r in rows], type=f.type) for f in schema
    ]
    return pa.Table.from_arrays(arrays, schema=schema)


def table_to_rows(table: pa.Table, row_type: type[R]) -> list[R]:
    return records_to_rows(table.to_pylist(), row_type)


def records_to_rows(records: Iterable[Mapping[str, Any]], row_type: type[R]) -> list[R]:
    """Dict records (Arrow ``to_pylist`` or a DB cursor) -> row dataclasses."""
    names = {f.name for f in fields(row_type)}  # type: ignore[arg-type]
    out: list[R] = []
    for record in records:
        kwargs = {k: v for k, v in record.items() if k in names}
        for k, v in kwargs.items():
            if isinstance(v, list):
                kwargs[k] = tuple(v)
        out.append(row_type(**kwargs))
    return out


def row_values(row: Any, schema: pa.Schema) -> list[Any]:
    """Row -> positional values in schema order (for DB drivers)."""
    return [_cell(getattr(row, f.name)) for f in schema]


def raw_rows_to_table(rows: Sequence[RawBarRow]) -> pa.Table:
    return rows_to_table(rows, RAW_BARS_SCHEMA)


def raw_rows_to_parquet_table(rows: Sequence[RawBarRow]) -> pa.Table:
    """Like ``raw_rows_to_table`` but for the Parquet raw layer: adds the JSON ``extra`` column."""
    arrays = [
        pa.array(
            [encode_extra(r.extra) if f.name == "extra" else _cell(getattr(r, f.name)) for r in rows],
            type=f.type,
        )
        for f in RAW_BARS_PARQUET_SCHEMA
    ]
    return pa.Table.from_arrays(arrays, schema=RAW_BARS_PARQUET_SCHEMA)


def canonical_rows_to_table(rows: Sequence[CanonicalBarRow]) -> pa.Table:
    return rows_to_table(rows, CANONICAL_BARS_SCHEMA)
