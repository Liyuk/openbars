"""Collector nodes: local (in-process) or remote (HTTP)."""

from __future__ import annotations

import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from openbars.core.errors import (
    OpenBarsError,
    ProviderError,
    QuotaExhausted,
    provider_error_diagnostic,
)
from openbars.core.models import Interval, Market
from openbars.orchestrator.finalize import CanonicalSink
from openbars.orchestrator.jobs import WHOLE_MARKET, Checkpoint, Job
from openbars.providers.adapter import NullQuotaGate
from openbars.providers.base import FetchRequest, Provider, RawBatch
from openbars.quota.gate import QuotaGate
from openbars.raw.store import RawStore
from openbars.storage.state import SqlStateStore

DEFAULT_FEEDS: Mapping[str, str] = {"alpaca": "sip"}
MAX_PAGES = 10_000


@dataclass(frozen=True)
class JobResult:
    job_id: str
    ok: bool
    bars_written: int = 0
    requests_made: int = 0
    error: str | None = None
    error_code: str | None = None
    retryable: bool = True  # only meaningful when ``ok`` is False
    retry_at: datetime | None = None  # provider hint (Retry-After, quota reset)


class CollectorNode(Protocol):
    name: str

    def markets(self) -> set[Market]: ...

    def execute(self, job: Job) -> JobResult: ...


class PostProcessError(OpenBarsError):
    """Raw bars were stored but the post-fetch step (reconcile/canonical) failed."""


class RemoteNode:
    """Placeholder for HTTP collector nodes (CN/HK behind a different network).

    Remote execution is a later issue; the class only reserves the name and the contract.
    """

    def __init__(self, name: str, url: str, *, markets: set[Market] | frozenset[Market] = frozenset()) -> None:
        self.name = name
        self.url = url
        self._markets = set(markets)

    def markets(self) -> set[Market]:
        return set(self._markets)

    def execute(self, job: Job) -> JobResult:
        raise NotImplementedError("remote collector nodes are not implemented yet")


class LocalNode:
    """Runs jobs in-process against the injected providers.

    ``execute`` never raises for provider problems: they come back as a failed
    :class:`JobResult` carrying ``retryable`` / ``retry_at`` so the orchestrator decides
    between retry, backoff and ``dead``.

    Quota: real providers take a token from their own gate on every HTTP call. A provider
    that does not (no gate, or the no-op ``NullQuotaGate``) is gated here with the injected
    ``gate`` once per ``fetch_bars`` call, so a configured gate is never bypassed.
    """

    name = "local"

    def __init__(
        self,
        providers: Mapping[str, Provider],
        *,
        store: SqlStateStore,
        raw: RawStore,
        sink: CanonicalSink | None = None,
        gate: QuotaGate | None = None,
        feeds: Mapping[str, str] | None = None,
        clock: Callable[[], datetime] | None = None,
        redact: Callable[[str], str] | None = None,
        max_pages: int = MAX_PAGES,
        hourly_alignment: str = "session_open",
        include_extended_hours: bool = False,
    ) -> None:
        self.providers = dict(providers)
        self.store = store
        self.raw = raw
        self.sink = sink
        self.gate = gate
        self.feeds = dict(DEFAULT_FEEDS if feeds is None else feeds)
        self._clock = clock or (lambda: datetime.now(UTC))
        self._redact = redact or (lambda text: text)
        self.max_pages = max_pages
        self.hourly_alignment = hourly_alignment
        self.include_extended_hours = include_extended_hours

    def markets(self) -> set[Market]:
        return {cap.market for p in self.providers.values() for cap in p.capabilities()}

    # -- helpers ---------------------------------------------------------------------
    def _node_gate_applies(self, provider: Provider) -> bool:
        if self.gate is None:
            return False
        own = getattr(provider, "_quota_gate", None)
        return own is None or isinstance(own, NullQuotaGate)

    def _fail(self, job: Job, *, requests: int, written: int, code: str, message: str, retryable: bool,
              retry_at: datetime | None = None) -> JobResult:  # fmt: skip
        return JobResult(
            job.job_id, False, written, requests, self._redact(message), code, retryable, retry_at
        )

    # -- execution -------------------------------------------------------------------
    def execute(self, job: Job) -> JobResult:
        provider = self.providers.get(job.provider)
        if provider is None:
            return self._fail(
                job, requests=0, written=0, code="not_configured", retryable=False,
                message=f"provider {job.provider!r} is not enabled/configured",
            )  # fmt: skip
        stats = _Stats()
        try:
            return self._run(job, provider, stats)
        except Exception as exc:  # noqa: BLE001 - a job must never crash the scheduler
            return self._to_result(job, exc, stats.requests, stats.written)

    def _to_result(self, job: Job, exc: BaseException, requests: int, written: int) -> JobResult:
        if isinstance(exc, ProviderError):
            return self._fail(
                job, requests=requests, written=written, code=exc.code,
                message=provider_error_diagnostic(exc), retryable=exc.retryable,
                retry_at=exc.retry_at,
            )  # fmt: skip
        if isinstance(exc, QuotaExhausted):
            return self._fail(
                job, requests=requests, written=written, code="quota_exhausted",
                message=str(exc), retryable=True, retry_at=exc.retry_at,
            )  # fmt: skip
        if isinstance(exc, PostProcessError):
            return self._fail(
                job, requests=requests, written=written, code="post_process_failed",
                message=str(exc), retryable=True,
            )  # fmt: skip
        from openbars.storage.compat_write import AmbiguousInstrumentError

        if isinstance(exc, AmbiguousInstrumentError):
            # Bootstrap hit a reused ticker; retrying will not help. Keep the candidate report.
            return self._fail(
                job, requests=requests, written=written, code="ambiguous_instrument",
                message=exc.format_report(), retryable=False,
            )  # fmt: skip
        # Unknown failure: keep only the type name (messages may carry request details).
        return self._fail(
            job, requests=requests, written=written, code="unexpected",
            message=f"unexpected {type(exc).__name__}", retryable=True,
        )  # fmt: skip

    def _run(self, job: Job, provider: Provider, stats: "_Stats") -> JobResult:
        checkpoint = self.store.load_checkpoint(job.job_id)
        cursor = checkpoint.cursor if checkpoint else None
        symbols = () if tuple(job.instruments) == WHOLE_MARKET else tuple(job.instruments)
        feed = self.feeds.get(job.provider)
        seen: set[str] = set()
        for _ in range(self.max_pages):
            request = FetchRequest(
                market=job.market,
                interval=job.interval,
                symbols=symbols,
                window=job.window,
                cursor=cursor,
                feed=feed,
                hourly_alignment=self.hourly_alignment if job.market is Market.US and job.interval is Interval.H1 else None,
                include_extended_hours=self.include_extended_hours if job.market is Market.US and job.interval is Interval.H1 else False,
            )
            fetch_id = f"{job.job_id}-{uuid.uuid4().hex[:12]}"
            self.store.start_fetch(
                provider=job.provider, fetch_id=fetch_id, market=job.market,
                interval=job.interval, window=job.window, job_id=job.job_id,
            )  # fmt: skip
            try:
                if self._node_gate_applies(provider):
                    self.gate.acquire(job.provider, 1)  # type: ignore[union-attr]
                stats.requests += 1
                batch: RawBatch = provider.fetch_bars(request)
            except (ProviderError, QuotaExhausted) as exc:
                self._finish_error(job, fetch_id, exc)
                raise
            if batch.bars:
                self.raw.append(batch)  # raw first; the store dedups replays
                stats.written += len(batch.bars)
            partial = batch.error is not None or batch.next_cursor is not None
            self.store.finish_fetch(
                job.provider, fetch_id, "partial" if partial else ("ok" if batch.bars else "empty"),
                row_count=len(batch.bars),
                error_code=batch.error.code if batch.error else None,
                error_message=self._redact(str(batch.error))[:500] if batch.error else None,
            )  # fmt: skip
            if batch.bars and self.sink is not None:
                try:
                    self.sink.after_fetch(job, batch.bars)
                except Exception as exc:  # noqa: BLE001
                    raise PostProcessError(f"post-fetch step failed ({type(exc).__name__})") from exc
            now = self._clock()
            if batch.next_cursor is not None:
                self.store.save_checkpoint(Checkpoint(job.job_id, batch.next_cursor, now))
            if batch.error is not None:
                raise batch.error
            if batch.next_cursor is None:
                self.store.save_checkpoint(Checkpoint(job.job_id, None, now))
                return JobResult(job.job_id, True, stats.written, stats.requests)
            if batch.next_cursor == cursor or batch.next_cursor in seen:
                raise ProviderError("invalid_payload", "provider returned a repeating cursor", retryable=False)
            seen.add(batch.next_cursor)
            cursor = batch.next_cursor
            self.store.touch_job(job.job_id, now=now)
        raise ProviderError("invalid_payload", "pagination did not terminate", retryable=False)

    def _finish_error(self, job: Job, fetch_id: str, exc: BaseException) -> None:
        code = exc.code if isinstance(exc, ProviderError) else "quota_exhausted"
        self.store.finish_fetch(
            job.provider, fetch_id, "error", error_code=code,
            error_message=self._redact(str(exc))[:500],
        )  # fmt: skip


@dataclass
class _Stats:
    requests: int = 0
    written: int = 0
