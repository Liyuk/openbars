"""Post-fetch hook: raw bars -> reconciler -> canonical rows.

The raw layer is always written first by the node. A ``CanonicalSink`` then looks at the
sessions the fetched bars touch, gathers what other providers already stored for them, and
lets the existing :class:`~openbars.reconcile.reconciler.MemoryReconciler` pick canonical
rows. Bars whose instrument is not resolved yet are reconciled under their symbol.

Crypto jobs (``Market.CRYPTO``) never go through that US reconciler: ``CryptoReconcilingSink``
resolves each bar to its ``crypto:spot:<venue>:<BASE>-<QUOTE>`` instrument and reconciles per
instrument with ``crypto-daily-v1`` (``reconcile_crypto``).

A-share jobs (``Market.CN``) are not reconciled either: ``CnCanonicalSink`` projects the single
source (BaoStock) into ``cn-daily-v1`` canonical rows on the stored ``RAW`` basis. Suspended
sessions (``tradestatus`` 0) are dropped from canonical (a calendar hole; the raw layer keeps the
placeholder), limit-up / limit-down closes get ``LIMIT_UP`` / ``LIMIT_DOWN`` flags
(``normalize.cn_quality``), and the provider's adjust factors, when it has any, are ingested into
the factor table so ``QFQ`` / ``HFQ`` can be derived at read time.

``DispatchingSink`` picks the sink by ``job.market`` so callers keep passing one sink object.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, Protocol

from openbars.core.models import DateRange, Interval, Market, QualityFlag, RawBar, Session
from openbars.core.series import SELECTION_PRIMARY_SOURCE, CanonicalDailyRow
from openbars.normalize.cn_adjust import from_baostock
from openbars.normalize.cn_ingest import ensure_cn_instruments
from openbars.normalize.cn_quality import flags_for_cn_series, is_suspended
from openbars.orchestrator.jobs import Job
from openbars.normalize.instruments import InstrumentMaster
from openbars.providers.base import FetchRequest
from openbars.reconcile.crypto import POLICY_VERSION_CN_DAILY_V1
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
        if Market(job.market) in (Market.CRYPTO, Market.CN, Market.HK):
            return  # XNYS reconciler: other markets stay raw-only unless their own sink handles them
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


@dataclass
class CryptoReconcilingSink:
    """Resolve crypto bars to pair instruments and reconcile each with ``crypto-daily-v1``.

    * ``instrument_id`` missing on a bar: ``master.resolve(symbol, on=session_date,
      provider=bar.provider, market=CRYPTO)``. Unresolved bars stay raw-only.
    * Observations are grouped per ``instrument_id`` (one venue each) and handed to
      ``reconcile_crypto``; cross-venue observations are rejected there, so a Coinbase bar never
      fills or corroborates a Binance instrument.
    * ``other_providers`` are scanned in ``raw`` for the same window (e.g. a second feed of the
      same venue); they are resolved the same way and only used for instruments in this fetch.
    """

    master: InstrumentMaster
    writer: CanonicalWriter
    raw: RawStore | None = None
    other_providers: Sequence[str] = ()

    def after_fetch(self, job: Job, bars: Sequence[RawBar]) -> None:
        from openbars.reconcile.crypto import reconcile_crypto

        if Interval(job.interval) is not Interval.D1 or Market(job.market) is not Market.CRYPTO:
            return
        mine = self._observations(bars)
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
            first = max(min(o.session_date for o in observations), window.start)
            last = min(max(o.session_date for o in observations), window.end)
            result = reconcile_crypto(key, first, last, observations)
            rows.extend(result.result.canonical)
        if rows:
            self.writer(rows, Market.CRYPTO)

    def _observations(self, bars: Sequence[RawBar]) -> list[SourceObservation]:
        out: list[SourceObservation] = []
        cache: dict[tuple[str, str, date], str | None] = {}
        for bar in bars:
            obs = observation_from_raw(bar)
            if obs is None:
                continue
            if obs.instrument_id is None:
                ck = (bar.provider, bar.symbol_as_returned, obs.session_date)
                if ck not in cache:
                    cache[ck] = self.master.resolve(
                        bar.symbol_as_returned, on=obs.session_date,
                        provider=bar.provider, market=Market.CRYPTO,
                    )  # fmt: skip
                if cache[ck] is None:
                    continue  # unresolved: raw only
                obs = replace(obs, instrument_id=cache[ck])
            out.append(obs)
        return out

    def _others(self, job: Job, window: DateRange) -> dict[str, list[SourceObservation]]:
        out: dict[str, list[SourceObservation]] = defaultdict(list)
        if self.raw is None:
            return out
        for provider in self.other_providers:
            if provider == job.provider:
                continue
            bars = list(self.raw.scan(provider, Market.CRYPTO, Interval.D1, window))
            for obs in self._observations(bars):
                out[obs.key].append(obs)
        return out


CnWriter = Callable[[Sequence[Any]], object]  # takes ``CanonicalBarRow`` rows (``upsert_rows``)


@dataclass
class CnCanonicalSink:
    """Raw A-share daily bars -> ``cn-daily-v1`` canonical rows (single source, stored RAW).

    * **Identity**: ``bar.instrument_id``, else ``master`` resolution, else the deterministic
      generation-1 id (``ensure_cn_instruments`` registers it first). A symbol that is not a
      recognised CN spelling stays raw-only.
    * **Suspensions** (``tradestatus`` 0) are dropped: no placeholder bar in canonical (the raw
      row stays; ``cn_quality.missing_sessions`` treats the session as not missing). A trading
      day with volume 0 that is *not* suspended is kept.
    * **Flags**: ``LIMIT_UP`` / ``LIMIT_DOWN`` from ``cn_quality`` (reference = ``extra["preclose"]``,
      else the previous raw close; IPO windows from the instrument's ``listed_on`` when known).
    * Rows are ``RAW``, ``policy_version="cn-daily-v1"``, regular session, ``selection_reason``
      ``primary_source``. They never go through the US XNYS reconciler.
    * **Adjust factors**: with a ``factor_store`` (``upsert_adjust_factors``) and a provider that has
      ``fetch_adjust_factors``, each symbol's factors are fetched and stored after the canonical
      write, so QFQ / HFQ work after a run. The window is the job window, or
      ``[factor_start, job end]`` when ``factor_start`` is given: BaoStock factors are cumulative
      from listing, so a late start silently misses earlier ex-dates. Pass a long start (listing
      date / ``1990-01-01``) for a complete history; it costs one extra request per symbol and job.
      A factor failure never fails the job (the bars are already written); it is recorded in
      ``factor_errors`` as ``(symbol, error code)``. No ``CorporateAction`` rows are invented.
    """

    master: InstrumentMaster
    writer: CnWriter
    providers: Mapping[str, Any] = field(default_factory=dict)
    factor_store: Any = None
    factor_start: date | None = None
    factor_errors: list[tuple[str, str]] = field(default_factory=list)

    def after_fetch(self, job: Job, bars: Sequence[RawBar]) -> None:
        if Interval(job.interval) is not Interval.D1 or Market(job.market) is not Market.CN:
            return
        ids = self._resolve(job, bars)
        by_id: dict[str, list[RawBar]] = defaultdict(list)
        for bar in bars:
            iid = bar.instrument_id or ids.get(bar.symbol_as_returned)
            if iid is not None:
                by_id[iid].append(bar)
        rows = []
        for iid, group in sorted(by_id.items()):
            group.sort(key=lambda b: (_session_date(b), b.ts_start))
            flags = flags_for_cn_series(group, listed_on=self._listed_on(iid))
            rows += [self._row(iid, b, f) for b, f in zip(group, flags, strict=True) if not is_suspended(b)]
        if rows:
            self.writer(rows)
        self._ingest_factors(job, by_id)

    def _resolve(self, job: Job, bars: Sequence[RawBar]) -> dict[str, str]:
        out: dict[str, str] = {}
        first: dict[str, date] = {}
        for bar in bars:
            if not bar.instrument_id:
                day = _session_date(bar)
                first[bar.symbol_as_returned] = min(day, first.get(bar.symbol_as_returned, day))
        for symbol, day in first.items():
            try:
                out.update(ensure_cn_instruments(self.master, [symbol], as_of=day, provider=job.provider))
            except ValueError:
                continue  # not a recognised CN symbol: raw only
        return out

    def _listed_on(self, instrument_id: str) -> date | None:
        get = getattr(self.master, "get", None)
        inst = get(instrument_id) if get is not None else None
        return None if inst is None else inst.listed_on

    @staticmethod
    def _row(instrument_id: str, bar: RawBar, flags: QualityFlag) -> Any:
        from openbars.storage.rows import CanonicalBarRow, flags_to_names, quality_status_for

        source = f"{bar.provider}:{bar.series.feed if bar.series is not None else bar.feed or ''}"
        return CanonicalBarRow(
            instrument_id=instrument_id,
            market=Market.CN,
            interval=Interval.D1,
            ts_start=bar.ts_start,
            session_date=_session_date(bar),
            session=Session.REGULAR,
            provider=bar.provider,
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=bar.volume,
            price_source=source,
            reconcile_rule=SELECTION_PRIMARY_SOURCE,
            policy_version=POLICY_VERSION_CN_DAILY_V1,
            feed=(bar.series.feed if bar.series is not None else bar.feed) or "",
            adjustment="raw",
            flags=flags_to_names(flags),
            selection_reason=SELECTION_PRIMARY_SOURCE,
            quality_status=quality_status_for(flags),  # limit / suspended marks stay "complete"
            volume_source=source,
            sources_seen=(bar.provider,),
        )

    def _ingest_factors(self, job: Job, by_id: Mapping[str, Sequence[RawBar]]) -> None:
        provider = self.providers.get(job.provider)
        fetch = getattr(provider, "fetch_adjust_factors", None)
        if self.factor_store is None or fetch is None:
            return
        window = DateRange(min(self.factor_start or job.window.start, job.window.end), job.window.end)
        for iid, group in sorted(by_id.items()):
            symbol = group[0].symbol_as_returned
            try:
                request = FetchRequest(Market.CN, Interval.D1, (symbol,), window)
                found = [from_baostock(f, iid) for f in fetch(request)]
                self.factor_store.upsert_adjust_factors(found)
            except Exception as exc:  # noqa: BLE001 - bars are written; a factor outage must not fail the job
                self.factor_errors.append((symbol, getattr(exc, "code", type(exc).__name__)))


@dataclass
class DispatchingSink:
    """One sink for every market: ``job.market`` selects the ``us``, ``crypto`` or ``cn`` sink.

    A market without a sink (including HK in this version) is raw-only, never mis-reconciled.
    """

    us: CanonicalSink | None = None
    crypto: CanonicalSink | None = None
    cn: CanonicalSink | None = None

    def after_fetch(self, job: Job, bars: Sequence[RawBar]) -> None:
        market = Market(job.market)
        sink = {Market.US: self.us, Market.CRYPTO: self.crypto, Market.CN: self.cn}.get(market)
        if sink is not None:
            sink.after_fetch(job, bars)


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

