"""Persistent, multi-window quota gate.

``PersistentQuotaGate`` holds the algorithm and is store-agnostic; ``SqliteQuotaGate`` is the
default, fully tested flavour.

Algorithm for ``acquire(provider, weight)`` inside one exclusive store transaction:

1. ``base = max(now, next_request_at)`` -- fixed-cadence spacing, as a paced worker would.
2. ``t`` starts at ``max(base, penalized_until)`` and moves forward until ``weight`` fits
   *every* window after the safety margin (see :func:`plan_reservation`).
3. ``t == base``: grant at ``t`` -- record the usage, set
   ``next_request_at = t + min_interval * weight`` and sleep until ``t``.
   ``t > base`` (a window is exhausted or a penalty is active): nothing is recorded; the
   caller raises ``QuotaExhausted(retry_at=t)`` or, with no timeout, sleeps until ``t`` and
   re-runs the reservation (a penalty may have changed meanwhile).

``min_interval`` comes from the rolling window with the shortest ``per`` (so 5/min is
paced at 12 s instead of burst-then-block). Longer windows are enforced by counting only,
so a daily budget may still be spent at the per-minute cadence.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import UTC, date, datetime, time as dtime, timedelta
from pathlib import Path
from typing import Callable, Mapping, Sequence

from openbars.core.errors import ConfigError, QuotaExhausted
from openbars.quota.defaults import DEFAULT_POLICIES
from openbars.quota.gate import Lease, QuotaUsage
from openbars.quota.policy import QuotaPolicy, RateLimit
from openbars.quota.store import (
    PROVIDER_WIDE_SCOPE,
    GateState,
    QuotaStore,
    SqliteQuotaStore,
    UsageRow,
)

Clock = Callable[[], datetime]
Sleep = Callable[[float], None]


def _utcnow() -> datetime:
    return datetime.now(UTC)


# --- window arithmetic --------------------------------------------------------------


def _calendar_bounds(limit: RateLimit, at: datetime) -> tuple[datetime, datetime]:
    at = at.astimezone(UTC)
    if limit.per == timedelta(days=1):
        start = datetime.combine(at.date(), dtime.min, tzinfo=UTC)
        return start, start + timedelta(days=1)
    start = datetime(at.year, at.month, 1, tzinfo=UTC)
    following = date(at.year + (at.month == 12), at.month % 12 + 1, 1)
    return start, datetime.combine(following, dtime.min, tzinfo=UTC)


def window_start(limit: RateLimit, at: datetime) -> datetime:
    """Earliest instant that can still count toward ``limit`` at ``at`` (inclusive for
    calendar windows, exclusive for rolling ones)."""
    if limit.kind == "rolling":
        return at - limit.per
    return _calendar_bounds(limit, at)[0]


def window_used(limit: RateLimit, rows: Sequence[UsageRow], at: datetime) -> int:
    """Weight already counted in the window that contains ``at``."""
    if limit.kind == "rolling":
        floor = at - limit.per
        return sum(r.weight for r in rows if r.reserved_at > floor)
    start, end = _calendar_bounds(limit, at)
    return sum(r.weight for r in rows if start <= r.reserved_at < end)


def retention_start(policy: QuotaPolicy, at: datetime) -> datetime:
    """Usage older than this can never count toward any window at or after ``at``."""
    return min(window_start(limit, at) for limit in policy.limits)


def _fit_time(
    limit: RateLimit, cap: int, rows: Sequence[UsageRow], weight: int, t: datetime
) -> datetime:
    """Earliest instant >= ``t`` at which ``weight`` fits in ``limit`` (cap = effective count)."""
    if limit.kind == "utc_calendar":
        start, end = _calendar_bounds(limit, t)
        used = sum(r.weight for r in rows if start <= r.reserved_at < end)
        return t if used + weight <= cap else end  # next UTC boundary
    floor = t - limit.per
    live = sorted((r for r in rows if r.reserved_at > floor), key=lambda r: r.reserved_at)
    excess = sum(r.weight for r in live) + weight - cap
    if excess <= 0:
        return t
    freed = 0
    for row in live:  # the window frees up once the oldest reservations age out
        freed += row.weight
        if freed >= excess:
            return row.reserved_at + limit.per
    return t  # unreachable: weight <= cap


@dataclass(frozen=True)
class Granted:
    slot: datetime


@dataclass(frozen=True)
class Blocked:
    retry_at: datetime


def plan_reservation(
    policy: QuotaPolicy,
    now: datetime,
    state: GateState,
    rows: Sequence[UsageRow],
    weight: int,
) -> Granted | Blocked:
    """Pure decision step shared by every store."""
    caps = [(limit, policy.effective_limit(limit)) for limit in policy.limits]
    for limit, cap in caps:
        if weight > cap:
            raise ValueError(
                f"weight {weight} can never fit window {limit.name!r} of {policy.provider} "
                f"(effective limit {cap})"
            )
    base = max(now, state.next_request_at) if state.next_request_at else now
    t = base
    if state.penalized_until is not None and state.penalized_until > t:
        t = state.penalized_until
    while True:  # each pass either settles or moves t strictly forward
        moved = False
        for limit, cap in caps:
            later = _fit_time(limit, cap, rows, weight, t)
            if later > t:
                t, moved = later, True
        if not moved:
            break
    return Granted(t) if t == base else Blocked(t)


# --- the gate -----------------------------------------------------------------------


class PersistentQuotaGate:
    """Store-backed :class:`~openbars.quota.gate.QuotaGate`."""

    def __init__(
        self,
        store: QuotaStore,
        policies: Mapping[str, QuotaPolicy] | None = None,
        *,
        clock: Clock = _utcnow,
        sleep: Sleep = time.sleep,
    ) -> None:
        self.store = store
        self.policies = dict(DEFAULT_POLICIES if policies is None else policies)
        self.clock = clock
        self.sleep = sleep

    def _now(self) -> datetime:
        now = self.clock()
        if now.tzinfo is None:
            raise ValueError("clock must return timezone-aware datetimes")
        return now.astimezone(UTC)

    def _policy(self, provider: str) -> QuotaPolicy:
        try:
            return self.policies[provider]
        except KeyError:
            raise ConfigError(f"no quota policy for provider {provider!r}") from None

    def acquire(
        self,
        provider: str,
        weight: int = 1,
        *,
        scope: str = "default",
        timeout: float | None = None,
    ) -> Lease:
        if weight < 1:
            raise ValueError("weight must be at least 1")
        if scope == PROVIDER_WIDE_SCOPE:
            raise ValueError(f"scope {PROVIDER_WIDE_SCOPE!r} is reserved")
        policy = self._policy(provider)
        interval = policy.min_interval()
        deadline: datetime | None = None
        while True:
            now = self._now()
            if deadline is None and timeout is not None:
                deadline = now + timedelta(seconds=timeout)
            retry_at: datetime | None = None
            slot: datetime | None = None
            with self.store.transaction(provider, scope) as tx:
                state, rows = tx.load(retention_start(policy, now))
                decision = plan_reservation(policy, now, state, rows, weight)
                if isinstance(decision, Blocked):
                    retry_at = decision.retry_at
                elif deadline is not None and decision.slot > deadline:
                    retry_at = decision.slot  # spacing wait longer than the caller allows
                else:
                    slot = decision.slot
                    tx.record(
                        slot,
                        weight,
                        slot + interval * weight,
                        retention_start(policy, slot),
                    )
            if slot is not None:
                self._sleep_until(slot)
                return Lease(provider, weight, slot)
            assert retry_at is not None
            if deadline is not None and retry_at > deadline:
                raise QuotaExhausted(provider, retry_at)
            self._sleep_until(retry_at)

    def _sleep_until(self, moment: datetime) -> None:
        delay = (moment - self._now()).total_seconds()
        if delay > 0:
            self.sleep(delay)

    def penalize(self, provider: str, until: datetime, scope: str | None = None) -> None:
        """No grant before ``until``; only ever extends an existing penalty.

        ``scope=None`` (default) penalizes every scope of the provider.
        """
        self._policy(provider)
        if until.tzinfo is None:
            raise ValueError("until must be timezone-aware")
        self.store.penalize(provider, until.astimezone(UTC), scope)

    def snapshot(self) -> Mapping[str, QuotaUsage]:
        """Usage per provider (``"provider"`` for the default scope, else ``"provider:scope"``)."""
        now = self._now()
        result: dict[str, QuotaUsage] = {}
        for provider, policy in self.policies.items():
            scopes = ["default", *(s for s in self.store.scopes(provider) if s != "default")]
            for scope in scopes:
                state, rows = self.store.read(provider, scope, retention_start(policy, now))
                used: dict[str, int] = {}
                remaining: dict[str, int] = {}
                for limit in policy.limits:
                    count = window_used(limit, rows, now)
                    used[limit.name] = count
                    remaining[limit.name] = max(0, policy.effective_limit(limit) - count)
                penalty = state.penalized_until
                key = provider if scope == "default" else f"{provider}:{scope}"
                result[key] = QuotaUsage(
                    provider,
                    used,
                    remaining,
                    penalty if penalty is not None and penalty > now else None,
                )
        return result

    def close(self) -> None:
        self.store.close()


class SqliteQuotaGate(PersistentQuotaGate):
    """Quota gate persisted in a SQLite file (WAL, ``BEGIN IMMEDIATE`` per reservation)."""

    def __init__(
        self,
        path: str | Path,
        policies: Mapping[str, QuotaPolicy] | None = None,
        *,
        clock: Clock = _utcnow,
        sleep: Sleep = time.sleep,
    ) -> None:
        super().__init__(SqliteQuotaStore(path), policies, clock=clock, sleep=sleep)
