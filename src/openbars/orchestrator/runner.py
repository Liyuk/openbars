"""Orchestrator contract and the local (single-machine) implementation."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Protocol
from zoneinfo import ZoneInfo

from openbars.orchestrator.jobs import Job, JobState
from openbars.orchestrator.lock import SchedulerLock
from openbars.orchestrator.nodes import CollectorNode, JobResult
from openbars.quota.policy import BackoffPolicy, QuotaPolicy
from openbars.quota.retry import RetryState
from openbars.storage.state import SqlStateStore

NEW_YORK = ZoneInfo("America/New_York")
DATA_NOT_READY = "data_not_ready"
# Failures that say nothing about provider health: they never feed the circuit breaker.
NON_CIRCUIT_CODES = frozenset({DATA_NOT_READY, "quota_exhausted", "post_process_failed"})
DEFAULT_LEASE_TTL = timedelta(seconds=60)


@dataclass(frozen=True)
class RunReport:
    done: int
    failed: int
    dead: int
    requests: int
    claimed: int = 0
    waiting: int = 0  # jobs left pending/failed (retry_at in the future or circuit open)
    open_circuits: tuple[str, ...] = ()


class Orchestrator(Protocol):
    def submit(self, jobs: Iterable[Job]) -> int: ...

    def run(self, *, until_idle: bool = True, max_jobs: int | None = None) -> RunReport: ...


def next_data_ready_time(now: datetime) -> datetime:
    """The next 00:15 America/New_York strictly after ``now`` (UTC)."""
    local = now.astimezone(NEW_YORK)
    candidate = local.replace(hour=0, minute=15, second=0, microsecond=0)
    if candidate <= local:
        candidate = (candidate + timedelta(days=1)).replace(hour=0, minute=15)
    return candidate.astimezone(UTC)


class LocalOrchestrator:
    """Claims jobs from the state store and runs them on a local node.

    * one :class:`RetryState` per provider: backoff, attempt budget and a circuit breaker
      (kept in process; after a restart jobs simply stay ``failed`` with their ``retry_at``);
    * a provider whose circuit is open is excluded from claiming, other providers go on;
    * ``run`` holds the scheduler lock (re-entrant, so callers may wrap plan + run in it).
    """

    def __init__(
        self,
        store: SqlStateStore,
        nodes: CollectorNode | Sequence[CollectorNode],
        *,
        policies: Mapping[str, QuotaPolicy] | None = None,
        workers: int = 1,
        lease_ttl: timedelta = DEFAULT_LEASE_TTL,
        lock_path: str | Path | None = None,
        clock: Callable[[], datetime] | None = None,
        sleep: Callable[[float], None] | None = None,
        redact: Callable[[str], str] | None = None,
    ) -> None:
        if workers < 1:
            raise ValueError("workers must be at least 1")
        self.store = store
        self.nodes = [nodes] if hasattr(nodes, "execute") else list(nodes)  # type: ignore[arg-type]
        self.policies = dict(policies or {})
        self.workers = workers
        self.lease_ttl = lease_ttl
        self.lock = SchedulerLock(lock_path) if lock_path is not None else None
        self._clock = clock or (lambda: datetime.now(UTC))
        self._sleep = sleep or time.sleep
        self._redact = redact or (lambda text: text)
        self._retry: dict[str, RetryState] = {}
        self._mutex = threading.Lock()
        self._stop = threading.Event()

    # -- lock ------------------------------------------------------------------------
    def __enter__(self) -> "LocalOrchestrator":
        if self.lock is not None:
            self.lock.acquire()
        return self

    def __exit__(self, *exc: object) -> None:
        if self.lock is not None:
            self.lock.release()

    def stop(self) -> None:
        self._stop.set()

    # -- retry bookkeeping -------------------------------------------------------------
    def _policy(self, provider: str) -> BackoffPolicy:
        quota = self.policies.get(provider)
        return quota.backoff if quota is not None else BackoffPolicy()

    def retry_state(self, provider: str) -> RetryState:
        with self._mutex:
            if provider not in self._retry:
                self._retry[provider] = RetryState(self._policy(provider))
            return self._retry[provider]

    def open_circuits(self, now: datetime | None = None) -> dict[str, datetime]:
        now = now or self._clock()
        with self._mutex:
            return {
                name: state.circuit_open_until
                for name, state in self._retry.items()
                if state.circuit_open_until is not None and not state.allow(now)
            }

    # -- Orchestrator ------------------------------------------------------------------
    def submit(self, jobs: Iterable[Job]) -> int:
        """Insert new jobs as given (normally pending); returns how many were new."""
        return self.store.submit_jobs(jobs)

    def _node_for(self, job: Job) -> CollectorNode | None:
        for node in self.nodes:
            if job.node is not None and node.name != job.node:
                continue
            if job.market in node.markets():
                return node
        return self.nodes[0] if self.nodes and job.node is None else None

    def run(
        self, *, until_idle: bool = True, max_jobs: int | None = None, poll_seconds: float = 1.0
    ) -> RunReport:
        counters = {"done": 0, "failed": 0, "dead": 0, "requests": 0, "claimed": 0}
        with self:
            self._run_loop(counters, until_idle, max_jobs, poll_seconds)
        now = self._clock()
        waiting = self.store.count_jobs()
        return RunReport(
            done=counters["done"],
            failed=counters["failed"],
            dead=counters["dead"],
            requests=counters["requests"],
            claimed=counters["claimed"],
            waiting=waiting[JobState.PENDING] + waiting[JobState.FAILED],
            open_circuits=tuple(sorted(self.open_circuits(now))),
        )

    def _reap(self) -> None:
        self.store.reap_expired_leases(
            now=self._clock(), ttl=self.lease_ttl, max_attempts=self._max_attempts()
        )

    def _max_attempts(self) -> int:
        return max((self._policy(p).max_attempts for p in self.policies), default=BackoffPolicy().max_attempts)

    def _run_loop(
        self, counters: dict[str, int], until_idle: bool, max_jobs: int | None, poll_seconds: float
    ) -> None:
        self._reap()
        in_flight: dict[Future[None], str] = {}
        pool = ThreadPoolExecutor(max_workers=self.workers) if self.workers > 1 else None
        try:
            while not self._stop.is_set():
                # Fill the worker slots.
                claimed_any = False
                while len(in_flight) < self.workers and (
                    max_jobs is None or counters["claimed"] < max_jobs
                ):
                    now = self._clock()
                    job = self.store.claim_job(now=now, exclude_providers=self._blocked(now))
                    if job is None:
                        break
                    claimed_any = True
                    counters["claimed"] += 1
                    if pool is None:
                        self._process(job, counters)
                    else:
                        in_flight[pool.submit(self._process, job, counters)] = job.job_id
                if in_flight:
                    finished, _ = wait(list(in_flight), return_when=FIRST_COMPLETED)
                    for future in finished:
                        in_flight.pop(future)
                        future.result()
                    continue
                if max_jobs is not None and counters["claimed"] >= max_jobs:
                    return
                if claimed_any:
                    continue
                # Nothing claimable right now. Rows still leased/running belong to a lost
                # worker (we hold the scheduler lock): wait out their TTL, then re-queue.
                if self._has_active_leases():
                    self._sleep(min(self.lease_ttl.total_seconds() / 4, 0.25) or 0.01)
                    self._reap()
                    continue
                if until_idle:
                    return
                self._sleep(poll_seconds)
        finally:
            if pool is not None:
                pool.shutdown(wait=True)

    def _has_active_leases(self) -> bool:
        counts = self.store.count_jobs()
        return counts[JobState.LEASED] + counts[JobState.RUNNING] > 0

    def _blocked(self, now: datetime) -> list[str]:
        with self._mutex:
            return [name for name, state in self._retry.items() if not state.allow(now)]

    # -- one job -----------------------------------------------------------------------
    def _process(self, job: Job, counters: dict[str, int]) -> None:
        now = self._clock()
        if not self.store.start_job(job.job_id, now=now):
            return  # lease lost before we started
        node = self._node_for(job)
        if node is None:
            result = JobResult(
                job.job_id, False, error=f"no collector node serves market {job.market.value}",
                error_code="no_node", retryable=False,
            )  # fmt: skip
        else:
            try:
                result = node.execute(job)
            except Exception as exc:  # noqa: BLE001 - nodes should not raise, but never crash the run
                result = JobResult(
                    job.job_id, False, error=f"node error ({type(exc).__name__})",
                    error_code="node_error", retryable=True,
                )  # fmt: skip
        with self._mutex:
            counters["requests"] += result.requests_made
        outcome = self._finish(job, result)
        with self._mutex:
            counters[outcome] += 1

    def _finish(self, job: Job, result: JobResult) -> str:
        now = self._clock()
        state = self.retry_state(job.provider)
        if result.ok:
            with self._mutex:
                state.record_success()
            self.store.finish_job(job.job_id, JobState.DONE, now=now, attempts=job.attempts)
            return "done"

        attempts = job.attempts + 1
        message = self._redact(result.error or result.error_code or "job failed")[:1000]
        policy = self._policy(job.provider)
        hint = result.retry_at
        if result.error_code == DATA_NOT_READY and hint is None:
            hint = next_data_ready_time(now)

        with self._mutex:
            if result.error_code in NON_CIRCUIT_CODES:
                decision = None
            else:
                state.attempts = job.attempts  # per-job attempt budget lives in the job row
                decision = state.record_failure(now, retryable=result.retryable, retry_at=hint)

        if not result.retryable or attempts >= policy.max_attempts:
            self.store.finish_job(
                job.job_id, JobState.DEAD, now=now, attempts=attempts, last_error=message
            )
            return "dead"
        if decision is None:
            retry_at = hint or now + timedelta(seconds=policy.base_seconds)
        else:
            retry_at = decision.retry_at
            if retry_at is None:  # circuit opened: wait for it to cool
                retry_at = state.circuit_open_until
            if hint is not None and retry_at is not None and hint > retry_at:
                retry_at = hint
        self.store.finish_job(
            job.job_id, JobState.FAILED, now=now, attempts=attempts, retry_at=retry_at, last_error=message
        )
        return "failed"
