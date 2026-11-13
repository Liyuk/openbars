"""Trading calendar contract."""

from __future__ import annotations

from dataclasses import dataclass
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


@dataclass(frozen=True)
class TradingSession:
    """One trading day with timezone-aware open/close instants."""

    session_date: date
    opens_at: datetime
    closes_at: datetime

    def __post_init__(self) -> None:
        for name in ("opens_at", "closes_at"):
            value = getattr(self, name)
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError(f"{name} must be timezone-aware")
        if self.closes_at <= self.opens_at:
            raise ValueError("closes_at must be after opens_at")
