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
    remaining: Mapping[str, int]
    penalized_until: datetime | None = None


class QuotaGate(Protocol):
    def acquire(
        self, provider: str, weight: int = 1, *, scope: str = "default", timeout: float | None = None
    ) -> Lease: ...

    def penalize(self, provider: str, until: datetime) -> None: ...

    def snapshot(self) -> Mapping[str, QuotaUsage]: ...
