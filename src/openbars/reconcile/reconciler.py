"""Reconciler contract."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Protocol, Sequence

from openbars.core.models import Bar, CanonicalBar
from openbars.reconcile.policy import ReconcilePolicy


@dataclass(frozen=True)
class AuditRecord:
    instrument_id: str
    key: str  # e.g. session_date or ts_start
    rule: str
    detail: str


@dataclass(frozen=True)
class ReconcileResult:
    canonical: Sequence[CanonicalBar]
    audit: Sequence[AuditRecord]
    gaps: Sequence[str]


class Reconciler(Protocol):
    def reconcile(
        self, candidates: Mapping[str, Sequence[Bar]], policy: ReconcilePolicy
    ) -> ReconcileResult:
        """candidates: provider id -> normalized bars for one instrument/interval/window."""
        ...
