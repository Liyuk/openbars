"""Instrument master: stable ids and time-bounded symbol mappings."""

from __future__ import annotations

from datetime import date
from typing import Iterable, Protocol

from openbars.core.models import Instrument, Market, SymbolMapping


class InstrumentMaster(Protocol):
    def resolve(self, symbol: str, *, on: date, provider: str, market: Market) -> str | None:
        """Return instrument_id the symbol meant on that date, or None if unknown/ambiguous."""
        ...

    def upsert(self, instruments: Iterable[Instrument], mappings: Iterable[SymbolMapping]) -> None: ...
