"""BarReader over ``CanonicalStore.scan_rows``.

Used by the Postgres backend (no DuckDB) so ``openbars serve`` can serve the same
read path as Parquet. Derivation goes through ``adjust_rows``.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from openbars.core.models import Adjustment, DateRange, Interval, Market
from openbars.normalize.adjust import adjust_rows, requires_derivation
from openbars.normalize.cn_adjust import adjust_cn_rows
from openbars.reconcile import policy_version_for_market
from openbars.storage.rows import (
    ID_KIND_AUTO,
    ID_KIND_UPSTREAM,
    available_by_utc,
    check_id_kind,
    is_available,
    upstream_instrument_ids,
)
from openbars.storage.schema import AUTO_POLICY_VERSION

_WIDE_START = date(1970, 1, 1)


class StoreBarReader:
    """Read canonical bars from a ``StorageBackend`` via ``scan_rows`` + ``adjust_rows``."""

    def __init__(self, backend: Any) -> None:
        self.backend = backend

    def bars(
        self,
        symbols_or_ids: list[str] | tuple[str, ...],
        *,
        interval: Interval = Interval.D1,
        start: date | None = None,
        end: date | None = None,
        adjustment: Adjustment | None = Adjustment.SPLIT,
        as_of: date | None = None,
        market: Market | None = None,
        policy_version: str = AUTO_POLICY_VERSION,
        stored_adjustment: Adjustment = Adjustment.SPLIT,
        on_missing_actions: str = "raise",
        available_by: datetime | None = None,
        id_kind: str = ID_KIND_AUTO,
        on_missing_factors: str | None = None,
        factor_source: str | None = None,
    ) -> list[Any]:
        """Return ``CanonicalBarRow`` values for ``symbols_or_ids`` (ids or tickers).

        ``available_by`` (#154): keep only stored rows with ``available_at <= available_by``
        (null = not available), before any derivation - same rule as ``DuckDbBarReader``.
        ``id_kind="upstream"`` (#155): upstream integer ids matched to ``instrument_id`` verbatim.
        A-share ``QFQ`` / ``HFQ`` (#124) derive from the master's ``adjust_factors`` with
        ``normalize.cn_adjust`` (``factor_source``, ``on_missing_factors`` default
        ``on_missing_actions``), as in ``DuckDbBarReader``.
        """
        if available_by is not None:
            available_by = available_by_utc(available_by)
        master = self.backend.instruments
        ids: list[str] = []
        if check_id_kind(id_kind) == ID_KIND_UPSTREAM:
            # #155: upstream integer ids match instrument_id verbatim; no master / mapping lookup
            ids = list(dict.fromkeys(upstream_instrument_ids(symbols_or_ids or [])))
        else:
            on = end or date.today()
            for item in symbols_or_ids or []:
                text = str(item)
                inst = master.get(text) if hasattr(master, "get") else None
                if inst is not None:
                    ids.append(inst.instrument_id)
                    if market is None:
                        market = inst.market
                    continue
                if hasattr(master, "resolve_any"):
                    try:
                        resolved = master.resolve_any(text, on=on, market=market)
                    except Exception:
                        resolved = text
                    ids.append(resolved)
                else:
                    ids.append(text)
        if not ids:
            return []

        window = DateRange(start or _WIDE_START, end or date.today())
        if policy_version in {AUTO_POLICY_VERSION, "auto", None}:
            pv = policy_version_for_market(market, instrument_ids=ids)
        else:
            pv = policy_version

        rows = list(
            self.backend.canonical.scan_rows(
                ids, interval, window, market=market, policy_version=pv
            )
        )
        if available_by is not None:
            rows = [r for r in rows if is_available(r, available_by)]
        if not rows:
            return []

        # cn-daily-v1 stores RAW; a default SPLIT read means the stored series.
        if (pv or "").startswith("cn-") or market is Market.CN:
            mode = None if adjustment is None else Adjustment(adjustment)
            if mode in (None, Adjustment.RAW, Adjustment.SPLIT):
                return rows
            factors = master.adjust_factors(ids) if hasattr(master, "adjust_factors") else []
            return list(adjust_cn_rows(
                rows, factors, mode, as_of=as_of, source=factor_source,
                on_missing_factors=on_missing_factors or on_missing_actions,
            ).rows)
        else:
            stored = Adjustment(stored_adjustment)
            mode = None if adjustment is None else Adjustment(adjustment)
            if mode is None or mode is stored or (
                mode is Adjustment.SPLIT and stored is Adjustment.SPLIT and as_of is None
            ):
                return rows

        actions = master.corporate_actions(ids) if hasattr(master, "corporate_actions") else []
        if not requires_derivation(mode, stored=stored, as_of=as_of, actions=actions):
            return rows
        result = adjust_rows(
            rows,
            actions,
            mode,
            as_of=as_of,
            on_missing_actions=on_missing_actions,
        )
        return list(result.rows)
