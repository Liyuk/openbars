"""Provider ticker type -> security type -> ``AssetClass``.

The table is the public ticker-type vocabulary used by the upstream internal
implementation. ETF is its own asset class; no ticker list is involved.
"""

from __future__ import annotations

from typing import Final

from openbars.core.models import AssetClass

UNKNOWN: Final = "unknown"

TICKER_TYPE_MAP: Final[dict[str, str]] = {
    "CS": "common_stock",
    "ADRC": "adr", "ADRP": "adr", "ADRR": "adr", "ADRW": "adr",
    "ETF": "etf", "ETN": "etf", "ETV": "etf",
    "FUND": "fund",
    "PFD": "preferred",
    "WARRANT": "warrant",
    "RIGHT": "right",
    "UNIT": "unit",
    "SP": "other", "OS": "other", "GDR": "other", "NYRS": "other",
    "BOND": "other", "INDEX": "other", "BASKET": "other", "OTHER": "other",
}

# Security types that have an ``AssetClass``. Everything else (preferred, warrant, right,
# unit, other, unknown) is classified but never written as an instrument.
ASSET_CLASS_FOR_TYPE: Final[dict[str, AssetClass]] = {
    "common_stock": AssetClass.EQUITY,
    "etf": AssetClass.ETF,
    "adr": AssetClass.ADR,
    "fund": AssetClass.FUND,
}

# Default universe: all common stock + ETF from the listing snapshot. Further types are opt-in.
DEFAULT_SECURITY_TYPES: Final = frozenset({"common_stock", "etf"})


def security_type_for(ticker_type: str | None) -> str:
    """The mapped security type, or ``unknown`` for a type the table does not list."""
    return TICKER_TYPE_MAP.get((ticker_type or "").strip().upper(), UNKNOWN)


def asset_class_for(ticker_type: str | None) -> AssetClass | None:
    """``AssetClass`` for a provider ticker type, ``None`` when it has no instrument class."""
    return ASSET_CLASS_FOR_TYPE.get(security_type_for(ticker_type))


def security_type_of_asset_class(asset_class: AssetClass) -> str:
    for type_, klass in ASSET_CLASS_FOR_TYPE.items():
        if klass is asset_class:
            return type_
    return UNKNOWN
