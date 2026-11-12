"""Trading calendar contract."""

from __future__ import annotations

from datetime import date, datetime
from typing import Iterable, Protocol, Sequence

from openbars.core.models import Market


class TradingCalendar(Protocol):
    market: Market
    timezone: str

    def sessions(self, start: date, end: date) -> Iterable[date]: ...

    def is_session(self, day: date) -> bool: ...

    def session_bounds(self, day: date) -> Sequence[tuple[datetime, datetime]]:
        """UTC (open, close) segments; several when there is a lunch break."""
        ...
