"""Fixed-cadence request pacers (ported from the upstream internal implementation)."""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from typing import Callable

from openbars.quota.store import PROVIDER_WIDE_SCOPE, QuotaStore


class RequestPacer:
    """Serializes one worker's requests to a fixed calls-per-minute cadence (in-process)."""

    def __init__(
        self,
        calls_per_minute: int,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if calls_per_minute < 1:
            raise ValueError("calls_per_minute must be positive")
        self.interval = 60.0 / calls_per_minute
        self.monotonic = monotonic
        self.sleep = sleep
        self.next_at: float | None = None

    def wait(self) -> None:
        now = self.monotonic()
        if self.next_at is not None and now < self.next_at:
            self.sleep(self.next_at - now)
            now = self.monotonic()
        self.next_at = now + self.interval


class PersistentRequestPacer:
    """Reserve fixed-cadence provider slots durably before making HTTP requests.

    ``reserve_slot(provider, interval)`` returns the instant this request may go out; the
    pacer sleeps until then. Because the slot lives in a shared store, spacing survives
    recreating the pacer (a worker restart) and is shared between processes.
    """

    def __init__(
        self,
        provider: str,
        calls_per_minute: int,
        *,
        reserve_slot: Callable[[str, timedelta], datetime],
        utcnow: Callable[[], datetime] = lambda: datetime.now(UTC),
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if calls_per_minute < 1:
            raise ValueError("calls_per_minute must be positive")
        self.provider = provider
        self.interval = timedelta(seconds=60.0 / calls_per_minute)
        self.reserve_slot = reserve_slot
        self.utcnow = utcnow
        self.sleep = sleep

    def wait(self) -> None:
        slot = self.reserve_slot(self.provider, self.interval)
        if slot.tzinfo is None:
            slot = slot.replace(tzinfo=UTC)
        delay = (slot - self.utcnow().astimezone(UTC)).total_seconds()
        if delay > 0:
            self.sleep(delay)


def store_slot_reserver(
    store: QuotaStore,
    *,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    scope: str = "pacer",
) -> Callable[[str, timedelta], datetime]:
    """A ``reserve_slot`` callable backed by a quota store.

    Uses its own gate row (``scope``, default ``"pacer"``) so that pacing does not
    double-count with :class:`~openbars.quota.persistent.PersistentQuotaGate` reservations.
    Honors a provider-wide penalty.
    """
    if scope == PROVIDER_WIDE_SCOPE:
        raise ValueError(f"scope {PROVIDER_WIDE_SCOPE!r} is reserved")

    def reserve(provider: str, interval: timedelta) -> datetime:
        if interval <= timedelta(0):
            raise ValueError("request interval must be positive")
        now = clock()
        if now.tzinfo is None:
            raise ValueError("request reservation time must be timezone-aware")
        now = now.astimezone(UTC)
        with store.transaction(provider, scope) as tx:
            state, _ = tx.load(now)
            slot = now
            for floor in (state.next_request_at, state.penalized_until):
                if floor is not None and floor > slot:
                    slot = floor
            tx.record(slot, 0, slot + interval, now)
        return slot

    return reserve
