"""RawStore contract."""

from __future__ import annotations

from typing import Iterable, Protocol

from openbars.core.models import DateRange, Interval, Market, RawBar
from openbars.providers.base import RawBatch


class RawStore(Protocol):
    def append(self, batch: RawBatch) -> None: ...

    def scan(
        self, provider: str, market: Market, interval: Interval, window: DateRange
    ) -> Iterable[RawBar]: ...
