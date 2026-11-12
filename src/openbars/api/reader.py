"""BarReader contract."""

from __future__ import annotations

from datetime import date
from typing import Any, Protocol, Sequence

from openbars.core.models import Adjustment, Interval


class BarReader(Protocol):
    def bars(
        self,
        symbols_or_ids: Sequence[str],
        *,
        interval: Interval = Interval.D1,
        start: date | None = None,
        end: date | None = None,
        adjustment: Adjustment = Adjustment.SPLIT,
        as_of: date | None = None,
    ) -> Any:
        """Return a columnar table (pyarrow.Table when the parquet extra is installed)."""
        ...
