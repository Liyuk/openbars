"""Reconcile policy: priorities per market x interval x field, tolerances."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping

from openbars.core.models import Interval, Market


@dataclass(frozen=True)
class Tolerances:
    price_rel: float = 0.005
    volume_rel: float = 0.10
    spike_rel: float = 0.5


@dataclass(frozen=True)
class ReconcilePolicy:
    market: Market
    interval: Interval
    price_priority: tuple[str, ...]
    volume_priority: tuple[str, ...]
    tolerances: Tolerances = field(default_factory=Tolerances)
    extra_rules: Mapping[str, object] = field(default_factory=dict)
