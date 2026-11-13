"""Typed configuration objects."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Mapping

from openbars.core.models import Adjustment, HourlyAlignment, Interval, Market


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
    # ``quota: {override: true}`` -- allow raising (not just tightening) the listed limits.
    quota_override: bool = False
    # option -> names of the ``${ENV_VAR}`` it referenced in config.yaml (for error messages).
    env_refs: Mapping[str, tuple[str, ...]] = field(default_factory=dict)


@dataclass(frozen=True)
class NodeConfig:
    name: str
    kind: str = "local"  # local | remote
    url: str | None = None
    markets: frozenset[Market] = frozenset()


RETAINABLE_LAYERS = ("raw",)  # canonical / ref are never pruned by retention (#127)


@dataclass(frozen=True)
class RetentionRule:
    """Keep the most recent ``keep_years`` calendar years (UTC year of ``now``) of a layer.

    ``provider`` / ``market`` / ``interval`` narrow the rule; ``None`` matches all.
    """

    layer: str
    keep_years: int
    provider: str | None = None
    market: str | None = None
    interval: str | None = None

    def matches(self, dims: Mapping[str, str]) -> bool:
        return all(
            want is None or dims.get(dim) == want
            for dim, want in (("provider", self.provider), ("market", self.market), ("interval", self.interval))
        )

    def describe(self) -> str:
        scope = ", ".join(f"{k}={v}" for k, v in (("provider", self.provider), ("market", self.market), ("interval", self.interval)) if v)
        return f"{self.layer} keep {self.keep_years} year(s)" + (f" [{scope}]" if scope else "")


@dataclass(frozen=True)
class Settings:
    selection: Selection
    providers: Mapping[str, ProviderConfig]
    storage_backend: str = "parquet_duckdb"
    storage_path: str = "./data"
    postgres_dsn: str | None = None
    postgres_mode: str = "native"
    postgres_write: bool = False
    postgres_schema: str | None = None
    postgres_quota: str = "sqlite"  # sqlite | postgres
    default_adjustment: Adjustment = Adjustment.SPLIT
    us_hourly_alignment: HourlyAlignment = HourlyAlignment.SESSION_OPEN
    include_extended_hours: bool = False
    nodes: tuple[NodeConfig, ...] = (NodeConfig(name="local"),)
    # ``storage.retention`` rules (#127); empty = retention off (nothing is ever pruned).
    retention: tuple[RetentionRule, ...] = ()
    # Explicit symbols per market (InstrumentMaster-based universes come later).
    universe: Mapping[Market, tuple[str, ...]] = field(default_factory=dict)
    reconcile: Mapping[str, object] = field(default_factory=dict)  # raw ``reconcile:`` section
    schedule: Mapping[str, object] = field(default_factory=dict)  # raw ``schedule:`` section
    # Path of the dotenv file that was actually loaded, if any, and how it was chosen.
    env_file: str | None = None
    env_file_source: str | None = None  # --env-file | OPENBARS_ENV_FILE | config.env_file | ./.env

    def secret_values(self, min_length: int = 8) -> tuple[str, ...]:
        """Resolved credential values (for output scrubbing only; never log them)."""
        values = {
            v
            for p in self.providers.values()
            for k, v in p.options.items()
            if isinstance(v, str) and len(v) >= min_length and is_secret_key(k)
        }
        if isinstance(self.postgres_dsn, str) and len(self.postgres_dsn) >= min_length:
            values.add(self.postgres_dsn)
        return tuple(sorted(values))


_SECRET_KEY_PARTS = ("key", "secret", "token", "password", "dsn")


def is_secret_key(name: str) -> bool:
    lowered = name.lower()
    return any(part in lowered for part in _SECRET_KEY_PARTS)
