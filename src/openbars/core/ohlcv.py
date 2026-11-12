"""OHLCV numeric policy and validation (``research-daily-v2``).

Prices are ``Decimal`` with precision (20, 8); volume is ``Decimal`` with precision
(28, 8). Values never pass through ``float``: ``float`` inputs are rejected, and
``Decimal`` / ``int`` / ``str`` are stored as ``Decimal``. The magnitude limits are
exclusive upper bounds so every accepted value fits the declared storage precision.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation, localcontext
from typing import NamedTuple

PRICE_PRECISION = 20
PRICE_SCALE = 8
VOLUME_PRECISION = 28
VOLUME_SCALE = 8

# Exclusive bounds: Numeric(20, 8) holds < 1e12, Numeric(28, 8) holds < 1e20.
MAX_PRICE = Decimal("1e12")
MAX_VOLUME = Decimal("1e20")

DecimalInput = Decimal | int | str


def to_decimal(value: object, *, name: str, scale: int = PRICE_SCALE) -> Decimal:
    """Convert ``Decimal`` / ``int`` / ``str`` to a finite ``Decimal`` exactly.

    Rejects ``float`` (and ``bool``), non-finite values and values that need more
    than ``scale`` fractional digits (they could not be stored without rounding).
    """
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
    with localcontext() as ctx:
        ctx.prec = 80
        if result != result.quantize(Decimal(1).scaleb(-scale)):
            raise ValueError(f"{name} has more than {scale} fractional digits: {result}")
    return result


def to_price(value: object, *, name: str = "price") -> Decimal:
    """Validate a price: Decimal(20, 8), 0 < price < 1e12."""
    result = to_decimal(value, name=name, scale=PRICE_SCALE)
    if result <= 0:
        raise ValueError(f"{name} must be > 0, got {result}")
    if result >= MAX_PRICE:
        raise ValueError(f"{name} magnitude must be < {MAX_PRICE:.0E}, got {result}")
    return result


def to_volume(value: object, *, name: str = "volume") -> Decimal:
    """Validate a volume: Decimal(28, 8), 0 <= volume < 1e20."""
    result = to_decimal(value, name=name, scale=VOLUME_SCALE)
    if result < 0:
        raise ValueError(f"{name} must be >= 0, got {result}")
    if result >= MAX_VOLUME:
        raise ValueError(f"{name} magnitude must be < {MAX_VOLUME:.0E}, got {result}")
    return result


class OHLCV(NamedTuple):
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal | None
    vwap: Decimal | None


def validate_ohlcv(
    open: object,  # noqa: A002 - mirrors the field names
    high: object,
    low: object,
    close: object,
    volume: object | None,
    vwap: object | None = None,
) -> OHLCV:
    """Validate and normalise one OHLCV record; raise ``ValueError`` on violations.

    Rules: prices > 0; volume >= 0 (``None`` allowed); high >= low;
    low <= open <= high; low <= close <= high. ``float`` inputs raise ``TypeError``
    (a caller bug rather than bad data).
    """
    o = to_price(open, name="open")
    h = to_price(high, name="high")
    low_ = to_price(low, name="low")
    c = to_price(close, name="close")
    if h < low_:
        raise ValueError(f"high ({h}) must be >= low ({low_})")
    if not low_ <= o <= h:
        raise ValueError(f"open ({o}) must be within [low ({low_}), high ({h})]")
    if not low_ <= c <= h:
        raise ValueError(f"close ({c}) must be within [low ({low_}), high ({h})]")
    v = None if volume is None else to_volume(volume)
    w = None if vwap is None else to_price(vwap, name="vwap")
    return OHLCV(o, h, low_, c, v, w)
