"""Typed configuration objects."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Mapping

from openbars.core.models import Adjustment, Interval, Market


@dataclass(frozen=True)
class Selection:
    """What the user wants: markets x intervals over a history range."""

    markets: frozenset[Market]
    intervals: frozenset[Interval] = frozenset({Interval.D1})
    history_start: date | None = None  # None = provider max
    history_end: date | None = None


@dataclass(frozen=True)
class ProviderConfig:
    name: str
    enabled: bool = True
    role: str = "primary"  # primary | fallback | audit
    options: Mapping[str, object] = field(default_factory=dict)  # secrets resolved from env
    quota_overrides: Mapping[str, float] = field(default_factory=dict)


@dataclass(frozen=True)
class NodeConfig:
    name: str
    kind: str = "local"  # local | remote
    url: str | None = None
    markets: frozenset[Market] = frozenset()


@dataclass(frozen=True)
class Settings:
    selection: Selection
    providers: Mapping[str, ProviderConfig]
    storage_backend: str = "parquet_duckdb"
    storage_path: str = "./data"
    default_adjustment: Adjustment = Adjustment.SPLIT
    nodes: tuple[NodeConfig, ...] = (NodeConfig(name="local"),)
