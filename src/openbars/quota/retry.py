"""Retry / backoff helpers driven by :class:`~openbars.quota.policy.BackoffPolicy`.

Pure bookkeeping (no sleeping, no I/O) so an orchestrator can decide *when* to retry:

* exponential backoff from ``base_seconds``, capped at ``max_seconds``;
* at most ``max_attempts`` failed attempts per operation;
* after ``circuit_break_after`` consecutive failures the provider's circuit opens for
  ``max_seconds``; once that elapses one probe is allowed (a failed probe re-opens it).
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timedelta

from openbars.quota.policy import BackoffPolicy


def compute_backoff(
    policy: BackoffPolicy,
    failures: int,
    *,
    jitter: float = 0.0,
    rng: random.Random | None = None,
) -> float:
    """Delay in seconds after ``failures`` failed attempts (1 = first failure).

    ``min(max_seconds, base_seconds * 2 ** (failures - 1))``. With ``jitter`` in [0, 1] the
    delay is scaled by a factor drawn from ``[1 - jitter, 1]`` (never above the cap);
    ``rng`` is injectable for deterministic tests.
    """
    if failures < 1:
        raise ValueError("failures must be at least 1")
    if not 0 <= jitter <= 1:
        raise ValueError("jitter must be in [0, 1]")
    # Clamp the exponent so huge failure counts cannot overflow a float.
    exponent = min(failures - 1, 64)
    delay = min(policy.max_seconds, policy.base_seconds * (2.0**exponent))
    if jitter:
        delay *= 1.0 - jitter * (rng or random).random()
    return delay


@dataclass(frozen=True)
class RetryDecision:
    retry: bool
    delay_seconds: float  # 0 when not retrying
    retry_at: datetime | None  # earliest next attempt
    circuit_open: bool
    reason: str  # retry | not_retryable | attempts_exhausted | circuit_open


class RetryState:
    """Failure bookkeeping for one provider (consecutive failures) and one operation (attempts)."""

    def __init__(
        self,
        policy: BackoffPolicy,
        *,
        jitter: float = 0.0,
        rng: random.Random | None = None,
    ) -> None:
        self.policy = policy
        self.jitter = jitter
        self.rng = rng
        self.attempts = 0  # failed attempts of the current operation
        self.consecutive_failures = 0
        self.circuit_open_until: datetime | None = None

    def start_operation(self) -> None:
        """Begin a new operation (job): per-operation attempts restart, the circuit does not."""
        self.attempts = 0

    def allow(self, now: datetime) -> bool:
        """False while the circuit is open."""
        return self.circuit_open_until is None or now >= self.circuit_open_until

    def record_success(self) -> None:
        self.attempts = 0
        self.consecutive_failures = 0
        self.circuit_open_until = None

    def record_failure(
        self,
        now: datetime,
        *,
        retryable: bool = True,
        retry_at: datetime | None = None,
    ) -> RetryDecision:
        """Register a failed attempt. ``retry_at`` is a provider hint (e.g. ``Retry-After``)
        and only ever delays the next attempt further."""
        self.attempts += 1
        self.consecutive_failures += 1
        if self.consecutive_failures >= self.policy.circuit_break_after:
            self.circuit_open_until = now + timedelta(seconds=self.policy.max_seconds)
            return RetryDecision(False, 0.0, self.circuit_open_until, True, "circuit_open")
        if not retryable:
            return RetryDecision(False, 0.0, None, False, "not_retryable")
        if self.attempts >= self.policy.max_attempts:
            return RetryDecision(False, 0.0, None, False, "attempts_exhausted")
        delay = compute_backoff(
            self.policy, self.attempts, jitter=self.jitter, rng=self.rng
        )
        at = now + timedelta(seconds=delay)
        if retry_at is not None and retry_at > at:
            at = retry_at
            delay = (at - now).total_seconds()
        return RetryDecision(True, delay, at, False, "retry")
