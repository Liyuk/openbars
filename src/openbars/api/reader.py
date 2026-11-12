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
        """Return a columnar table (pyarrow.Table when the parquet extra is installed).

        ``adjustment=SPLIT`` (default) is the stored ``research-daily-v2`` series (split-adjusted as
        delivered, no dividends); other modes, or ``SPLIT`` at an earlier ``as_of``, are derived at
        query time. ``as_of`` is a knowledge date (rows generated later are hidden) and the
        adjustment as-of. A ticker resolves point-in-time per day across ``[start, end]``; an
        ``instrument_id`` is queried as is. The parquet reader also takes ``market`` and
        ``policy_version`` (default auto: ``crypto-daily-v1`` for crypto, else ``research-daily-v2``).
        """
        ...
