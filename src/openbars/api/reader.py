"""BarReader contract."""

from __future__ import annotations

from datetime import date, datetime
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
        available_by: datetime | None = None,
        id_kind: str = "auto",
    ) -> Any:
        """Return a columnar table (pyarrow.Table when the parquet extra is installed).

        ``adjustment=SPLIT`` (default) is the stored ``research-daily-v2`` series (split-adjusted as
        delivered, no dividends); other modes, or ``SPLIT`` at an earlier ``as_of``, are derived at
        query time. A-share series (``cn-daily-v1``) are stored unadjusted: the default read returns
        the stored ``RAW`` series and ``QFQ`` / ``HFQ`` are derived from the stored adjust factors
        (``normalize.cn_adjust``). ``as_of`` is a knowledge date (rows generated later are hidden)
        and the adjustment as-of. A ticker resolves point-in-time per day across ``[start, end]``;
        an ``instrument_id`` is queried as is. The parquet reader also takes ``market`` and
        ``policy_version`` (default auto: ``crypto-daily-v1`` for crypto, ``cn-daily-v1`` for CN,
        else ``research-daily-v2``).

        ``available_by`` (#154, tz-aware datetime, optional) is a point-in-time filter: only the
        stored row of each key whose ``available_at <= available_by`` is returned; a row with a
        null ``available_at`` is treated as not available. An older revision that a newer row
        replaced is never returned. ``None`` (default) = no filter.

        ``id_kind="upstream"`` (#155): ``symbols_or_ids`` are upstream integer ids (``"42"`` or
        ``42``) matched to ``instrument_id`` verbatim: no symbol mapping, no instrument-master
        record needed, identity is the upstream's. Default ``"auto"`` = ids or tickers as above.
        """
        ...
