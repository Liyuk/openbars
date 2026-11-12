"""Multi-source arbitration into the canonical layer, with quality flags and audit."""

from openbars.reconcile.audit import AuditRecord
from openbars.reconcile.compatibility import SeriesCompatibility, compare_series
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
    "CanonicalSelection",
    "DEFAULT_POLICY",
    "GapReport",
    "InMemoryCanonicalStore",
    "KnownGap",
    "MemoryReconciler",
    "ReconcilePolicy",
    "ReconcileResult",
    "SelectionOverride",
    "SeriesCompatibility",
    "SourceObservation",
    "compare_series",
    "detect_gaps",
    "is_priority_source",
    "project_canonical_bars",
    "source_fingerprint",
]
