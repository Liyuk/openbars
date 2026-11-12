"""Post-fetch hook: raw bars -> reconciler -> canonical rows.

The raw layer is always written first by the node. A ``CanonicalSink`` then looks at the
sessions the fetched bars touch, gathers what other providers already stored for them, and
lets the existing :class:`~openbars.reconcile.reconciler.MemoryReconciler` pick canonical
rows. Bars whose instrument is not resolved yet are reconciled under their symbol.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Protocol

from openbars.core.models import DateRange, Interval, Market, RawBar
from openbars.core.series import CanonicalDailyRow
from openbars.orchestrator.jobs import Job
from openbars.raw.store import RawStore
from openbars.reconcile.projection import SourceObservation
from openbars.reconcile.reconciler import MemoryReconciler

CanonicalWriter = Callable[[Sequence[CanonicalDailyRow], Market], object]


class CanonicalSink(Protocol):
    def after_fetch(self, job: Job, bars: Sequence[RawBar]) -> None: ...


def _session_date(bar: RawBar) -> date:
    if bar.session_date is not None:
        return bar.session_date
    extra = bar.extra.get("session_date")
    if isinstance(extra, date) and not isinstance(extra, datetime):
        return extra
    return bar.ts_start.astimezone(UTC).date()


def observation_from_raw(bar: RawBar) -> SourceObservation | None:
    """Map a raw daily bar to a reconciler observation (None when it has no volume)."""
    if bar.volume is None:
        return None
    if bar.series is not None:
        feed, adjustment = bar.series.feed, bar.series.legacy_adjustment
    else:
        feed = (bar.feed or "default").split(":")[-1]
        adjustment = str(bar.extra.get("adjustment") or "split")
    symbol = bar.symbol_as_returned
    return SourceObservation(
        instrument_id=bar.instrument_id or None,
        symbol=symbol,
        session_date=_session_date(bar),
        open=bar.open,
        high=bar.high,
        low=bar.low,
        close=bar.close,
        volume=Decimal(bar.volume),
        provider=bar.provider,
        feed=feed,
        adjustment=adjustment,
        series=bar.series,
    )


@dataclass
class ReconcilingSink:
    """Reconcile the sessions touched by a fetch and hand canonical rows to ``writer``.

    ``other_providers`` are scanned in ``raw`` for the same sessions so the reconciler sees
    every source that has the bar; the fetched bars themselves are used as is.
    """

    reconciler: MemoryReconciler
    writer: CanonicalWriter
    raw: RawStore | None = None
    other_providers: Sequence[str] = ()

    def after_fetch(self, job: Job, bars: Sequence[RawBar]) -> None:
        if Interval(job.interval) is not Interval.D1:
            return  # the reconciler is daily-only for now
        mine = [o for o in (observation_from_raw(b) for b in bars) if o is not None]
        if not mine:
            return
        by_key: dict[str, list[SourceObservation]] = defaultdict(list)
        for obs in mine:
            by_key[obs.key].append(obs)
        days = [o.session_date for o in mine]
        window = DateRange(min(days), max(days))
        extra = self._others(job, window)
        rows: list[CanonicalDailyRow] = []
        for key, observations in sorted(by_key.items()):
            observations = observations + extra.get(key, [])
            first = min(o.session_date for o in observations)
            last = max(o.session_date for o in observations)
            first, last = max(first, window.start), min(last, window.end)
            result = self.reconciler.reconcile_range(key, first, last, observations)
            rows.extend(result.canonical)
        if rows:
            self.writer(rows, Market(job.market))

    def _others(self, job: Job, window: DateRange) -> dict[str, list[SourceObservation]]:
        out: dict[str, list[SourceObservation]] = defaultdict(list)
        if self.raw is None:
            return out
        for provider in self.other_providers:
            if provider == job.provider:
                continue
            for bar in self.raw.scan(provider, Market(job.market), Interval.D1, window):
                obs = observation_from_raw(bar)
                if obs is not None:
                    out[obs.key].append(obs)
        return out


def parquet_canonical_writer(canonical_store: object) -> CanonicalWriter:
    """Writer for ``ParquetCanonicalStore``: maps canonical daily rows to storage rows."""

    def write(rows: Sequence[CanonicalDailyRow], market: Market) -> int:
        from openbars.core.models import Session
        from openbars.storage.rows import CanonicalBarRow

        mapped = [
            CanonicalBarRow(
                instrument_id=r.instrument_id,
                market=market,
                interval=Interval.D1,
                ts_start=datetime(r.session_date.year, r.session_date.month, r.session_date.day, tzinfo=UTC),
                session_date=r.session_date,
                session=Session.REGULAR,
                provider=r.provider,
                open=r.open,
                high=r.high,
                low=r.low,
                close=r.close,
                price_source=f"{r.provider}:{r.feed}",
                reconcile_rule=r.selection_reason,
                policy_version=r.policy_version,
                feed=r.feed,
                adjustment=r.adjustment,
                revision=r.revision,
                volume=r.volume,
                selection_reason=r.selection_reason,
                quality_status=r.quality_status,
                capabilities=r.capabilities,
                source_fingerprint=r.source_fingerprint,
                volume_source=f"{r.provider}:{r.feed}",
                sources_seen=(r.provider,),
                generated_at=r.generated_at,
            )
            for r in rows
        ]
        return canonical_store.upsert_rows(mapped)  # type: ignore[attr-defined]

    return write

