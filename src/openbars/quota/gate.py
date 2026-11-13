"""QuotaGate contract."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Mapping, Protocol


@dataclass(frozen=True)
class Lease:
    provider: str
    weight: int
    granted_at: datetime


@dataclass(frozen=True)
class QuotaUsage:
    provider: str
    used: Mapping[str, int]  # window label -> used
    remaining: Mapping[str, int]  # window label -> effective limit minus used (>= 0)
    penalized_until: datetime | None = None


class QuotaGate(Protocol):
    def acquire(
        self, provider: str, weight: int = 1, *, scope: str = "default", timeout: float | None = None
    ) -> Lease:
        """Reserve ``weight`` units. Returns once the grant instant has arrived.

        ``timeout=None`` waits as long as needed; otherwise raises
        :class:`openbars.core.errors.QuotaExhausted` (with ``retry_at``) when the grant
        would come later than ``timeout`` seconds from now.
        """
        ...

    def penalize(self, provider: str, until: datetime) -> None:
        """No grant for ``provider`` before ``until`` (429 / Retry-After)."""
        ...

    def snapshot(self) -> Mapping[str, QuotaUsage]: ...
