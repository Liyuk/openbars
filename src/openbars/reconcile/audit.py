"""Audit records: why each canonical bar was selected."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from openbars.core.series import SourceIdentity
from openbars.reconcile.projection import CanonicalSelection


@dataclass(frozen=True)
class AuditRecord:
    instrument_id: str
    session_date: date
    policy_version: str
    rule: str  # selection_reason
    quality_status: str
    price_source: SourceIdentity
    volume_source: SourceIdentity  # always equal to price_source (whole-bar selection)
    sources_seen: tuple[SourceIdentity, ...]
    revision: int  # revision of the canonical row
    revised: bool  # True when this write replaced an earlier canonical row
    source_fingerprint: str


def audit_record(selection: CanonicalSelection, *, revision: int, revised: bool) -> AuditRecord:
    return AuditRecord(
        instrument_id=selection.instrument_id,
        session_date=selection.session_date,
        policy_version=selection.policy_version,
        rule=selection.selection_reason,
        quality_status=selection.quality_status,
        price_source=selection.price_source,
        volume_source=selection.volume_source,
        sources_seen=selection.sources_seen,
        revision=revision,
        revised=revised,
        source_fingerprint=selection.source_fingerprint,
    )
