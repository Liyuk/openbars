"""Synthetic observation builders shared by the reconcile tests (codes TEST1 / ZZZA)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from openbars.core.series import (
    ALPACA_IEX_SPLIT,
    ALPACA_SIP_SPLIT,
    ALPHA_VANTAGE_DAILY_RAW,
    MASSIVE_EOD_SPLIT,
    TWELVE_DATA_DAILY_SPLIT,
    YFINANCE_YAHOO_SPLIT,
    SourceIdentity,
)
from openbars.reconcile.projection import SourceObservation

DAY = date(2024, 1, 3)
MASSIVE = MASSIVE_EOD_SPLIT
SIP = ALPACA_SIP_SPLIT
IEX = ALPACA_IEX_SPLIT
YAHOO = YFINANCE_YAHOO_SPLIT
TWELVE = TWELVE_DATA_DAILY_SPLIT
ALPHA = ALPHA_VANTAGE_DAILY_RAW

D = Decimal


def _q(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.00000001"))


def obs(
    source: SourceIdentity,
    close: str | Decimal,
    volume: str | int | Decimal = 1_000_000,
    *,
    day: date = DAY,
    instrument_id: str = "TEST1",
    revision: int = 1,
    source_bar_id: str | None = None,
    flat: bool = False,
) -> SourceObservation:
    """OHLC are fixed ratios of ``close`` so price differences scale with the close."""
    c = D(close)
    if flat:
        o = h = low = c
    else:
        o, h, low = _q(c * D("0.99")), _q(c * D("1.02")), _q(c * D("0.98"))
    return SourceObservation(
        instrument_id=instrument_id,
        symbol=instrument_id,
        session_date=day,
        open=o,
        high=h,
        low=low,
        close=_q(c),
        volume=D(volume),
        provider=source.provider,
        feed=source.feed,
        adjustment=source.adjustment,
        revision=revision,
        source_bar_id=source_bar_id,
    )
