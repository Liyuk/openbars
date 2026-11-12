"""Reconciler: observations -> canonical rows + audit + backfill jobs + known gaps.

``MemoryReconciler`` is the research-daily-v2 implementation on top of the pure
projection in ``projection.py``; persistence goes through ``CanonicalRowStore``.
"""

from __future__ import annotations

from collections.abc import Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Protocol

from openbars.calendars.xnys import XNYSCalendar, reject_non_session_bars
from openbars.core.models import Bar
from openbars.core.series import CanonicalDailyRow
from openbars.reconcile.audit import AuditRecord, audit_record
from openbars.reconcile.gaps import BackfillJob, KnownGap, detect_gaps
from openbars.reconcile.overrides import OverrideMap
from openbars.reconcile.policy import DEFAULT_POLICY, ReconcilePolicy
from openbars.reconcile.projection import (
    CanonicalSelection,
    SourceObservation,
    project_canonical_bars,
)
from openbars.reconcile.store import CanonicalRowStore, InMemoryCanonicalStore

REJECT_NON_SESSION = "non_session_date"


@dataclass(frozen=True)
class RejectedObservation:
    observation: SourceObservation
    reason: str


@dataclass(frozen=True)
class ReconcileResult:
    canonical: tuple[CanonicalDailyRow, ...]
    audit: tuple[AuditRecord, ...]
    backfill_jobs: tuple[BackfillJob, ...] = ()
    known_gaps: tuple[KnownGap, ...] = ()
    rejected: tuple[RejectedObservation, ...] = field(default=())


class Reconciler(Protocol):
    def reconcile(
        self, candidates: Mapping[str, Sequence[Bar]], policy: ReconcilePolicy
    ) -> ReconcileResult:
        """candidates: provider id -> normalized bars for one instrument/interval/window."""
        ...


def _row_content(row: CanonicalDailyRow) -> tuple[object, ...]:
    return (
        row.selected_source_bar_id, row.provider, row.feed, row.adjustment,
        row.open, row.high, row.low, row.close, row.volume,
        row.selection_reason, row.quality_status, row.capabilities, row.source_fingerprint,
    )  # fmt: skip


class MemoryReconciler:
    """research-daily-v2 reconciler backed by a ``CanonicalRowStore`` (in-memory default)."""

    def __init__(
        self,
        store: CanonicalRowStore | None = None,
        *,
        policy: ReconcilePolicy | None = None,
        calendar: XNYSCalendar | None = None,
    ) -> None:
        self.store: CanonicalRowStore = store if store is not None else InMemoryCanonicalStore()
        self.policy = policy or DEFAULT_POLICY
        self.calendar = calendar or XNYSCalendar()

    def reconcile_range(
        self,
        instrument_id: str,
        start: date,
        end: date,
        observations: Iterable[SourceObservation],
        *,
        generated_at: datetime | None = None,
        unfillable: Collection[date] = (),
        overrides: OverrideMap | None = None,
    ) -> ReconcileResult:
        """Reconcile one instrument over the inclusive window ``[start, end]``.

        Observations of other instruments or outside the window are ignored; bars on
        non-session dates are rejected. Missing sessions become backfill jobs, or known
        gaps when listed in ``unfillable`` (or when the policy has ``refill_gaps`` off).
        """
        generated_at = generated_at or datetime.now(timezone.utc)
        mine = [o for o in observations if o.key == instrument_id and start <= o.session_date <= end]
        filtered = reject_non_session_bars(mine)
        rejected = tuple(
            RejectedObservation(r.bar, REJECT_NON_SESSION) for r in filtered.rejected
        )
        selections = project_canonical_bars(
            filtered.accepted, policy=self.policy, overrides=overrides
        )

        rows: list[CanonicalDailyRow] = []
        audits: list[AuditRecord] = []
        for sel in selections:
            row, record = self._write(sel, generated_at)
            rows.append(row)
            audits.append(record)

        report = detect_gaps(
            instrument_id,
            start,
            end,
            (r.session_date for r in rows),
            calendar=self.calendar,
            unfillable=unfillable,
            refill=self.policy.refill_gaps,
        )
        self.store.add_backfill_jobs(report.backfill_jobs)
        self.store.add_known_gaps(report.known_gaps)
        return ReconcileResult(
            canonical=tuple(rows),
            audit=tuple(audits),
            backfill_jobs=report.backfill_jobs,
            known_gaps=report.known_gaps,
            rejected=rejected,
        )

    def _write(
        self, sel: CanonicalSelection, generated_at: datetime
    ) -> tuple[CanonicalDailyRow, AuditRecord]:
        key = (sel.instrument_id, sel.session_date, sel.policy_version)
        previous = self.store.get(key)
        candidate = sel.to_row(generated_at=generated_at, revision=1)
        if previous is None:
            row, revised = candidate, False
        elif _row_content(previous) == _row_content(candidate):
            # Unchanged: keep the stored row and revision, nothing to persist.
            return previous, audit_record(sel, revision=previous.revision, revised=False)
        else:
            row = sel.to_row(generated_at=generated_at, revision=previous.revision + 1)
            revised = True
        record = audit_record(sel, revision=row.revision, revised=revised)
        self.store.put(row, record)
        return row, record

    # --- thin adapter for the generic ``Reconciler`` protocol ---------------------------

    def reconcile(
        self, candidates: Mapping[str, Sequence[Bar]], policy: ReconcilePolicy | None = None
    ) -> ReconcileResult:
        """Adapter: provider id -> ``Bar`` sequences for a single instrument.

        The window is the span of the supplied bars. Bars without volume are skipped
        (the canonical row requires a volume).
        """
        policy = policy or self.policy
        observations: list[SourceObservation] = []
        for provider, bars in candidates.items():
            for bar in bars:
                feed = (bar.feed or "default").split(":")[-1]
                if bar.volume is None:
                    continue
                observations.append(
                    SourceObservation(
                        instrument_id=bar.instrument_id,
                        symbol=bar.instrument_id,
                        session_date=bar.session_date,
                        open=bar.open, high=bar.high, low=bar.low, close=bar.close,
                        volume=bar.volume,
                        provider=provider, feed=feed, adjustment=bar.adjustment.value,
                    )  # fmt: skip
                )
        if not observations:
            return ReconcileResult((), ())
        ids = sorted({o.key for o in observations})
        if len(ids) != 1:
            raise ValueError("reconcile() expects bars of a single instrument")
        days = [o.session_date for o in observations]
        inner = MemoryReconciler(self.store, policy=policy, calendar=self.calendar)
        result = inner.reconcile_range(ids[0], min(days), max(days), observations)
        return ReconcileResult(
            result.canonical, result.audit, result.backfill_jobs, result.known_gaps,
            result.rejected,
        )  # fmt: skip
