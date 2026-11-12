"""Load ``config.yaml``: parse, expand ``${ENV}`` references, map into typed settings.

``${NAME}`` expands to the environment value (empty when unset, so a disabled provider's
unset key is harmless); ``${NAME:-default}`` supplies a default. Resolved values are never
logged or echoed in error messages -- errors name the offending key, not its value.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from datetime import date, datetime
from pathlib import Path
from typing import Any

import yaml

from openbars.config.schema import NodeConfig, ProviderConfig, Selection, Settings
from openbars.core.errors import ConfigError
from openbars.core.models import Adjustment, Interval, Market

_ENV_REF = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")
_RESERVED_PROVIDER_KEYS = {"enabled", "role", "quota"}


def expand_env(value: Any, env: Mapping[str, str]) -> Any:
    """Recursively expand ``${VAR}`` / ``${VAR:-default}`` in every string of ``value``."""
    if isinstance(value, str):
        return _ENV_REF.sub(
            lambda m: env.get(m.group(1)) or (m.group(2) if m.group(2) is not None else ""), value
        )
    if isinstance(value, list):
        return [expand_env(v, env) for v in value]
    if isinstance(value, dict):
        return {k: expand_env(v, env) for k, v in value.items()}
    return value


def _enum(enum_cls: type, raw: Any, what: str) -> Any:
    try:
        return enum_cls(str(raw).strip().lower())
    except ValueError:
        valid = ", ".join(m.value for m in enum_cls)
        raise ConfigError(f"invalid {what} {raw!r}; expected one of: {valid}") from None


def _as_list(raw: Any, what: str) -> list[Any]:
    if raw is None:
        return []
    if isinstance(raw, (list, tuple)):
        return list(raw)
    if isinstance(raw, str):
        return [raw]
    raise ConfigError(f"{what} must be a list")


def _parse_date(raw: Any, what: str) -> date | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw.date()
    if isinstance(raw, date):
        return raw
    text = str(raw).strip().lower()
    if text in ("", "max", "null", "none"):
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        raise ConfigError(f"{what} must be an ISO date (YYYY-MM-DD) or 'max'") from None


def _section(data: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = data.get(key)
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ConfigError(f"'{key}' must be a mapping")
    return value


def parse_settings(data: Any) -> Settings:
    if not isinstance(data, Mapping):
        raise ConfigError("config root must be a mapping")

    markets = frozenset(_enum(Market, m, "market") for m in _as_list(data.get("markets", ["us"]), "markets"))
    if not markets:
        raise ConfigError("'markets' must list at least one market")
    intervals = frozenset(
        _enum(Interval, i, "interval") for i in _as_list(data.get("intervals", ["1d"]), "intervals")
    )
    if not intervals:
        raise ConfigError("'intervals' must list at least one interval")
    history = _section(data, "history")
    start = _parse_date(history.get("start"), "history.start")
    end = _parse_date(history.get("end"), "history.end")
    if start and end and end < start:
        raise ConfigError("history.end is before history.start")
    selection = Selection(markets, intervals, start, end)

    providers: dict[str, ProviderConfig] = {}
    for name, body in _section(data, "providers").items():
        body = body or {}
        if not isinstance(body, Mapping):
            raise ConfigError(f"providers.{name} must be a mapping")
        quota = body.get("quota") or {}
        if not isinstance(quota, Mapping):
            raise ConfigError(f"providers.{name}.quota must be a mapping")
        try:
            overrides = {str(k): float(v) for k, v in quota.items()}
        except (TypeError, ValueError):
            raise ConfigError(f"providers.{name}.quota values must be numbers") from None
        providers[str(name)] = ProviderConfig(
            name=str(name),
            enabled=bool(body.get("enabled", True)),
            role=str(body.get("role", "primary")),
            options={k: v for k, v in body.items() if k not in _RESERVED_PROVIDER_KEYS},
            quota_overrides=overrides,
        )

    storage = _section(data, "storage")
    conventions = _section(data, "conventions")
    nodes = []
    for entry in _as_list(data.get("nodes"), "nodes"):
        if not isinstance(entry, Mapping) or "name" not in entry:
            raise ConfigError("each node needs a name")
        nodes.append(
            NodeConfig(
                name=str(entry["name"]),
                kind=str(entry.get("kind", "local")),
                url=entry.get("url") or None,
                markets=frozenset(_enum(Market, m, "node market") for m in _as_list(entry.get("markets"), "node markets")),
            )
        )

    universe: dict[Market, tuple[str, ...]] = {}
    for market_key, body in _section(data, "universe").items():
        market = _enum(Market, market_key, "universe market")
        symbols = _as_list((body or {}).get("symbols") if isinstance(body, Mapping) else None, "universe symbols")
        cleaned = tuple(sorted({str(s).strip() for s in symbols if str(s).strip()}))
        if cleaned:
            universe[market] = cleaned

    kwargs: dict[str, Any] = {}
    if nodes:
        kwargs["nodes"] = tuple(nodes)
    if "adjustment" in conventions:
        kwargs["default_adjustment"] = _enum(Adjustment, conventions["adjustment"], "conventions.adjustment")
    return Settings(
        selection=selection,
        providers=providers,
        storage_backend=str(storage.get("backend", "parquet_duckdb")),
        storage_path=str(storage.get("path", "./data")),
        universe=universe,
        reconcile=dict(_section(data, "reconcile")),
        schedule=dict(_section(data, "schedule")),
        **kwargs,
    )


def load_settings(path: str | Path, *, env: Mapping[str, str] | None = None) -> Settings:
    """Parse ``config.yaml``, expand ``${ENV_VAR}`` references and validate.

    Raises :class:`~openbars.core.errors.ConfigError` for a missing/unreadable file, bad
    YAML or an invalid market/interval/adjustment.
    """
    file = Path(path)
    if not file.is_file():
        raise ConfigError(f"config file not found: {file}")
    try:
        data = yaml.safe_load(file.read_text(encoding="utf-8"))
    except yaml.YAMLError:
        raise ConfigError(f"config file {file} is not valid YAML") from None
    return parse_settings(expand_env(data, os.environ if env is None else env))
