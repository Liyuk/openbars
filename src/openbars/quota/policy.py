"""Quota policies declared by providers; user config may only tighten them."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta


@dataclass(frozen=True)
class RateLimit:
    count: int
    per: timedelta


@dataclass(frozen=True)
class BackoffPolicy:
    base_seconds: float = 2.0
    max_seconds: float = 600.0
    max_attempts: int = 5
    circuit_break_after: int = 10  # consecutive failures


@dataclass(frozen=True)
class QuotaPolicy:
    provider: str
    limits: tuple[RateLimit, ...]
    key_scope: str = "per_key"  # per_key | per_ip (per node)
    safety_margin: float = 0.9
    backoff: BackoffPolicy = field(default_factory=BackoffPolicy)
