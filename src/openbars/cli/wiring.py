"""Compose settings into runtime objects (providers, quota gate, policies).

Nothing here talks to the network: real providers only open connections on ``fetch_*``.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from pathlib import Path

from openbars.config.schema import ProviderConfig, Settings
from openbars.core.errors import ConfigError
from openbars.providers.base import Provider
from openbars.providers.registry import discover
from openbars.quota.defaults import DEFAULT_POLICIES
from openbars.quota.gate import QuotaGate
from openbars.quota.policy import QuotaPolicy, tighten

# (settings, quota gate or None while only planning) -> enabled providers by id.
ProviderFactory = Callable[[Settings, "QuotaGate | None"], Mapping[str, Provider]]

# provider id -> (constructor keyword, config option holding it) for required credentials.
_CREDENTIALS: dict[str, dict[str, str]] = {
    "alpaca": {"api_key": "key_id", "api_secret": "secret_key"},
    "massive": {"api_key": "api_key"},
    "yfinance": {},
}


def data_dir(settings: Settings, override: str | Path | None = None) -> Path:
    return Path(override if override else settings.storage_path)


def build_providers(settings: Settings, gate: QuotaGate | None = None) -> dict[str, Provider]:
    """Enabled providers with credentials. Providers without a key are left out."""
    classes = discover()
    out: dict[str, Provider] = {}
    for name, cfg in settings.providers.items():
        if not cfg.enabled or name not in _CREDENTIALS or name not in classes:
            continue
        kwargs = {
            param: str(cfg.options.get(option) or "").strip() or None
            for param, option in _CREDENTIALS[name].items()
        }
        if any(v is None for v in kwargs.values()):
            continue  # missing credentials: treated as disabled (see ``skipped_providers``)
        out[name] = classes[name](**kwargs, quota_gate=gate)  # type: ignore[operator]
    return out


def skipped_providers(settings: Settings, active: Mapping[str, object]) -> list[str]:
    """Human-readable reasons for enabled providers that are not active."""
    notes = []
    for name, cfg in settings.providers.items():
        if cfg.enabled and name not in active:
            reason = "missing credentials" if name in _CREDENTIALS else "unsupported provider"
            notes.append(f"provider {name} enabled but skipped: {reason}")
    return notes


def effective_policies(settings: Settings, providers: Mapping[str, Provider]) -> dict[str, QuotaPolicy]:
    """Provider-declared (else default) quota policy, tightened by user overrides."""
    out: dict[str, QuotaPolicy] = {}
    for name, provider in providers.items():
        policy = provider.quota_policy() if hasattr(provider, "quota_policy") else DEFAULT_POLICIES.get(name)
        if policy is None:
            continue
        cfg: ProviderConfig | None = settings.providers.get(name)
        if cfg is not None and cfg.quota_overrides:
            try:
                policy = tighten(policy, cfg.quota_overrides)
            except ValueError as exc:
                raise ConfigError(f"providers.{name}.quota: {exc}") from None
        out[name] = policy
    return out
