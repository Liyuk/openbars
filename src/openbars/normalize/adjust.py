"""Read-time price adjustment from raw bars + corporate actions."""

from __future__ import annotations

from typing import Iterable, Protocol, Sequence

from openbars.core.models import Adjustment, Bar, CorporateAction


class Adjuster(Protocol):
    def adjust(
        self, bars: Sequence[Bar], actions: Iterable[CorporateAction], mode: Adjustment
    ) -> Sequence[Bar]: ...
