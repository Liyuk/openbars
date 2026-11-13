"""Multi-source arbitration into the canonical layer, with quality flags and audit."""

from openbars.reconcile.audit import AuditRecord
from openbars.reconcile.compatibility import SeriesCompatibility, compare_series
from openbars.reconcile.crypto import (
    CRYPTO_DAILY_V1,
    POLICY_VERSION_CN_DAILY_V1,
    POLICY_VERSION_HK_DAILY_V1,
    policy_version_for_market,
    reconcile_crypto,
)
from openbars.reconcile.hk import HK_DAILY_V1, reconcile_hk
from openbars.reconcile.gaps import BackfillJob, GapReport, KnownGap, detect_gaps
from openbars.reconcile.overrides import SelectionOverride
from openbars.reconcile.policy import DEFAULT_POLICY, ReconcilePolicy
from openbars.reconcile.projection import (
    CanonicalSelection,
    SourceObservation,
    is_priority_source,
    project_canonical_bars,
    source_fingerprint,
)
from openbars.reconcile.reconciler import MemoryReconciler, ReconcileResult
from openbars.reconcile.store import InMemoryCanonicalStore

__all__ = [
    "AuditRecord",
    "BackfillJob",
    "CRYPTO_DAILY_V1",
    "HK_DAILY_V1",
    "CanonicalSelection",
    "DEFAULT_POLICY",
    "GapReport",
    "InMemoryCanonicalStore",
    "KnownGap",
    "MemoryReconciler",
    "POLICY_VERSION_CN_DAILY_V1",
    "POLICY_VERSION_HK_DAILY_V1",
    "ReconcilePolicy",
    "ReconcileResult",
    "SelectionOverride",
    "SeriesCompatibility",
    "SourceObservation",
    "compare_series",
    "detect_gaps",
    "is_priority_source",
    "policy_version_for_market",
    "project_canonical_bars",
    "reconcile_crypto",
    "reconcile_hk",
    "source_fingerprint",
]
