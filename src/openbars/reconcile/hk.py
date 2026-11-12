"""``hk-daily-v1``: Hong Kong daily merge/reconcile profile.

yfinance (``yfinance/yahoo/raw``) is the primary source. AKShare
(``akshare/eastmoney/raw``) is a same-market secondary used only for
corroboration: it is not planned by the orchestrator, does not fill gaps, and
values are never averaged. Disagreement beyond tolerance is flagged
``divergent``. Quality-flag rules are the existing ``MemoryReconciler`` rules
(no A-share limit-up / suspension flags).

Storage is unadjusted ``RAW``. ``SPLIT`` / ``SPLIT_DIVIDEND`` / ``ALL`` are
derived at query time by ``openbars.normalize.hk_adjust``.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, replace
from datetime import date, datetime
from decimal import Decimal

from openbars.calendars.hk import HKCalendar
from openbars.core.series import (
    AKSHARE_EASTMONEY_RAW,
    SELECTION_COMPATIBLE_GAP_FILL,
    SELECTION_FILL_PREFIX,
    YFINANCE_YAHOO_RAW,
)
from openbars.reconcile.crypto import POLICY_VERSION_HK_DAILY_V1
from openbars.reconcile.policy import ReconcilePolicy
from openbars.reconcile.projection import SourceObservation
from openbars.reconcile.reconciler import MemoryReconciler, ReconcileResult
from openbars.reconcile.store import CanonicalRowStore

HK_PRICE_TOLERANCE_PCT = Decimal("0.5")  # same equity rule as research-daily-v2
HK_VOLUME_TOLERANCE_PCT = Decimal(5)
HK_CORROBORATION_QUORUM = 2
HK_COMPLETION_WAIT_MINUTES = 30

HK_DAILY_V1 = ReconcilePolicy(
    policy_version=POLICY_VERSION_HK_DAILY_V1,
    primary=YFINANCE_YAHOO_RAW,
    secondary_sources=(AKSHARE_EASTMONEY_RAW,),
    fill_sources=(),
    price_tolerance_pct=HK_PRICE_TOLERANCE_PCT,
    volume_tolerance_pct=HK_VOLUME_TOLERANCE_PCT,
    corroboration_quorum=HK_CORROBORATION_QUORUM,
    volume_priority=(YFINANCE_YAHOO_RAW, AKSHARE_EASTMONEY_RAW),
    completion_wait_minutes=HK_COMPLETION_WAIT_MINUTES,
)


@dataclass(frozen=True)
class HkReconcileResult:
    result: ReconcileResult


def reconcile_hk(
    instrument_id: str,
    start: date,
    end: date,
    observations: Iterable[SourceObservation],
    *,
    generated_at: datetime | None = None,
    store: CanonicalRowStore | None = None,
) -> HkReconcileResult:
    """Reconcile one HK instrument over ``[start, end]`` (XHKG sessions)."""
    reconciler = MemoryReconciler(store, policy=HK_DAILY_V1, calendar=HKCalendar())
    result = reconciler.reconcile_range(
        instrument_id, start, end, observations, generated_at=generated_at
    )
    # AKShare is corroboration only: never fill a day the primary source did not publish.
    kept = tuple(
        row
        for row in result.canonical
        if row.selection_reason != SELECTION_COMPATIBLE_GAP_FILL
        and not row.selection_reason.startswith(SELECTION_FILL_PREFIX)
    )
    if kept != result.canonical:
        result = replace(result, canonical=kept)
    return HkReconcileResult(result)


__all__ = [
    "HK_DAILY_V1",
    "HK_PRICE_TOLERANCE_PCT",
    "HK_VOLUME_TOLERANCE_PCT",
    "POLICY_VERSION_HK_DAILY_V1",
    "HkReconcileResult",
    "reconcile_hk",
]
