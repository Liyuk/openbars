"""Quota policies declared by providers; user config may only tighten them."""

from __future__ import annotations

import math
from dataclasses import dataclass, field, replace
from datetime import date, timedelta
from typing import Literal, Mapping

WindowKind = Literal["rolling", "utc_calendar"]

_MINUTE = timedelta(minutes=1)


@dataclass(frozen=True)
class RateLimit:
    """At most ``count`` requests (weight units) per window.

    ``rolling``: any ``per``-long span ending now.
    ``utc_calendar``: the window is aligned to UTC boundaries -- ``per == 1 day`` is the
    current UTC day, and a ~30-day ``per`` is the current UTC calendar month.
    """

    count: int
    per: timedelta
    kind: WindowKind = "rolling"
    label: str | None = None

    def __post_init__(self) -> None:
        if self.count < 1:
            raise ValueError("RateLimit.count must be positive")
        if self.per <= timedelta(0):
            raise ValueError("RateLimit.per must be positive")
        if self.kind not in ("rolling", "utc_calendar"):
            raise ValueError(f"unknown RateLimit.kind: {self.kind!r}")
        if self.kind == "utc_calendar" and not (
            self.per == timedelta(days=1) or timedelta(days=28) <= self.per <= timedelta(days=31)
        ):
            raise ValueError("utc_calendar windows must be one day or about one month")

    @property
    def name(self) -> str:
        """Stable window label used in snapshots."""
        if self.label:
            return self.label
        seconds = self.per.total_seconds()
        for unit, size in (("d", 86400), ("h", 3600), ("m", 60), ("s", 1)):
            if seconds % size == 0:
                span = f"{int(seconds // size)}{unit}"
                break
        else:
            span = f"{seconds:g}s"
        return f"{self.kind}:{span}"


@dataclass(frozen=True)
class BackoffPolicy:
    base_seconds: float = 2.0
    max_seconds: float = 600.0
    max_attempts: int = 5
    circuit_break_after: int = 10  # consecutive failures


@dataclass(frozen=True)
class QuotaPolicy:
    """Request ceilings for one provider.

    The effective ceiling of a window is ``max(1, floor(count * safety_margin))``
    (see :meth:`effective_limit`); the gate never grants beyond it.
    """

    provider: str
    limits: tuple[RateLimit, ...]
    key_scope: str = "per_key"  # per_key | per_ip (per node)
    safety_margin: float = 0.9
    backoff: BackoffPolicy = field(default_factory=BackoffPolicy)
    # Optional hints for planners.
    max_batch_symbols: int | None = None
    max_bars_per_request: int | None = None
    history_start: date | None = None  # None = since listing
    official_docs_url: str | None = None

    def __post_init__(self) -> None:
        if not self.limits:
            raise ValueError("QuotaPolicy needs at least one RateLimit")
        if not 0 < self.safety_margin <= 1:
            raise ValueError("safety_margin must be in (0, 1]")

    def effective_limit(self, limit: RateLimit) -> int:
        """``max(1, floor(count * safety_margin))``."""
        # round() first so 1.0 * 4 or 0.9 * 10 never lose a unit to float error.
        scaled = limit.count * self.safety_margin
        return max(1, math.floor(round(scaled, 9)))

    def spacing_limit(self) -> RateLimit | None:
        """The rolling window that sets the minimum spacing (the shortest ``per``)."""
        rolling = [lim for lim in self.limits if lim.kind == "rolling"]
        return min(rolling, key=lambda lim: lim.per) if rolling else None

    def min_interval(self) -> timedelta:
        """Spacing between consecutive weight-1 grants (0 when no rolling window)."""
        limit = self.spacing_limit()
        if limit is None:
            return timedelta(0)
        return limit.per / self.effective_limit(limit)


def tighten(policy: QuotaPolicy, overrides: Mapping[str, float]) -> QuotaPolicy:
    """Apply user overrides, never raising anything above the policy default.

    Recognised keys (anything else raises ``ValueError``):

    * ``safety_margin`` -- lowers the margin (a higher value is ignored).
    * ``per_minute`` -- caps the count of every rolling window of at most one minute.
    * ``per_day`` -- caps the count of every window of one day (rolling 24h or UTC day).
    * a window label (``RateLimit.name``) -- caps that window's count.

    A cap is applied as ``min(default, override)``; values below 1 are rejected.
    """
    limits = list(policy.limits)
    margin = policy.safety_margin
    for key, value in overrides.items():
        if key == "safety_margin":
            if not 0 < value <= 1:
                raise ValueError("safety_margin override must be in (0, 1]")
            margin = min(margin, float(value))
            continue
        if value < 1:
            raise ValueError(f"quota override {key!r} must be at least 1")
        cap = int(value)
        matched = False
        for index, limit in enumerate(limits):
            if key == "per_minute":
                hit = limit.kind == "rolling" and limit.per <= _MINUTE
            elif key == "per_day":
                hit = limit.per == timedelta(days=1)
            else:
                hit = limit.name == key
            if hit:
                matched = True
                limits[index] = replace(limit, count=min(limit.count, cap))
        if not matched:
            raise ValueError(f"quota override {key!r} matches no window of {policy.provider}")
    return replace(policy, limits=tuple(limits), safety_margin=margin)
