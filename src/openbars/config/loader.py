"""Load ``config.yaml``: parse, expand ``${ENV}`` references, map into typed settings.

Resolution order for secrets and other ``${VAR}`` values (highest wins):

1. The process environment (or the ``env`` mapping passed to :func:`load_settings`).
2. A dotenv file, chosen by the first of: the ``env_file`` argument (CLI
   ``--env-file``), the ``OPENBARS_ENV_FILE`` environment variable, or the
   ``env_file`` key in ``config.yaml``.
   If none of those is set, ``.env`` next to ``config.yaml`` is read when it exists
   (no error when it does not).
3. A ``${VAR:-default}`` default in the YAML.

Literal (non-placeholder) values in ``config.yaml`` are used as written and are
not overwritten by the environment. ``ALPACA_API_KEY`` / ``ALPACA_API_SECRET``
are accepted as aliases for ``ALPACA_API_KEY_ID`` / ``ALPACA_API_SECRET_KEY``
when the canonical name is unset.

``${NAME}`` expands to the environment value (empty when unset, so a disabled
provider's unset key is harmless); ``${NAME:-default}`` supplies a default.
Resolved values are never logged or echoed in error messages -- errors name the
offending key or file path, not its value.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import replace
from datetime import date, datetime
from pathlib import Path
from typing import Any

import yaml

from openbars.config.schema import RETAINABLE_LAYERS, NodeConfig, ProviderConfig, RetentionRule, Selection, Settings
from openbars.core.errors import ConfigError
from openbars.core.models import Adjustment, HourlyAlignment, Interval, Market

_ENV_REF = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")
_RESERVED_PROVIDER_KEYS = {"enabled", "role", "quota"}
_ENV_LINE = re.compile(
    r"^(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$"
)

ENV_FILE_VAR = "OPENBARS_ENV_FILE"
DEFAULT_ENV_FILE = ".env"
DEFAULT_ENV_SOURCE = "./.env"

# Canonical name -> accepted aliases (used only when the canonical name is unset).
ENV_ALIASES: dict[str, tuple[str, ...]] = {
    "ALPACA_API_KEY_ID": ("ALPACA_API_KEY",),
    "ALPACA_API_SECRET_KEY": ("ALPACA_API_SECRET",),
}


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


def _strip_env_value(raw: str) -> str:
    text = raw.strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {'"', "'"}:
        return text[1:-1]
    # Unquoted: drop an inline comment that starts at a preceding space.
    if " #" in text:
        text = text[: text.index(" #")]
    return text.rstrip()


def parse_env_file(path: str | Path) -> dict[str, str]:
    """Parse a dotenv file into a name->value mapping. Values never appear in errors."""
    file = Path(path)
    if not file.is_file():
        raise ConfigError(f"env file not found: {file}")
    try:
        text = file.read_text(encoding="utf-8-sig")
    except OSError:
        raise ConfigError(f"env file not readable: {file}") from None
    out: dict[str, str] = {}
    for lineno, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        match = _ENV_LINE.match(line)
        if match is None:
            raise ConfigError(f"env file {file} line {lineno} is not NAME=VALUE")
        out[match.group(1)] = _strip_env_value(match.group(2))
    return out


def apply_env_aliases(env: Mapping[str, str]) -> dict[str, str]:
    """Copy alias values onto unset (or empty) canonical names."""
    out = dict(env)
    for canonical, aliases in ENV_ALIASES.items():
        if out.get(canonical):
            continue
        for alias in aliases:
            if out.get(alias):
                out[canonical] = out[alias]
                break
    return out


def merge_env(process_env: Mapping[str, str], file_env: Mapping[str, str] | None = None) -> dict[str, str]:
    """Process environment wins; then the dotenv file; then aliases."""
    merged = dict(file_env or {})
    merged.update(process_env)
    return apply_env_aliases(merged)


def resolve_env_file(
    data: Mapping[str, Any],
    process_env: Mapping[str, str],
    *,
    env_file: str | Path | None = None,
    config_dir: Path | None = None,
) -> tuple[Path | None, str | None]:
    """Pick the dotenv path: argument, ``OPENBARS_ENV_FILE``, YAML ``env_file``, else ``<config dir>/.env``.

    The default ``.env`` is only returned when it exists; its absence is not an error.
    """
    if env_file:
        return Path(env_file).expanduser(), "--env-file"
    if process_env.get(ENV_FILE_VAR):
        return Path(process_env[ENV_FILE_VAR]).expanduser(), ENV_FILE_VAR
    raw = data.get("env_file") if isinstance(data, Mapping) else None
    if raw is None or raw == "":
        default = (config_dir if config_dir is not None else Path(".")) / DEFAULT_ENV_FILE
        return (default, DEFAULT_ENV_SOURCE) if default.is_file() else (None, None)
    if not isinstance(raw, str):
        raise ConfigError("'env_file' must be a string path")
    path = Path(expand_env(raw, process_env)).expanduser()
    if not path.is_absolute() and config_dir is not None:
        path = config_dir / path
    return path, "config.env_file"


def _enum(enum_cls: type, raw: Any, what: str) -> Any:
    try:
        return enum_cls(str(raw).strip().lower())
    except ValueError:
        valid = ", ".join(m.value for m in enum_cls)
        raise ConfigError(f"invalid {what} {raw!r}; expected one of: {valid}") from None


def _as_bool(raw: Any, what: str) -> bool:
    if isinstance(raw, bool):
        return raw
    if raw is None:
        return False
    text = str(raw).strip().lower()
    if text in ("1", "true", "yes", "on"):
        return True
    if text in ("0", "false", "no", "off", ""):
        return False
    raise ConfigError(f"{what} must be a boolean")


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
        quota = dict(quota)
        allow_widen = quota.pop("override", False)
        if not isinstance(allow_widen, bool):
            raise ConfigError(f"providers.{name}.quota.override must be true or false")
        try:
            if any(isinstance(v, bool) for v in quota.values()):
                raise TypeError
            overrides = {str(k): float(v) for k, v in quota.items()}
        except (TypeError, ValueError):
            raise ConfigError(f"providers.{name}.quota values must be numbers") from None
        providers[str(name)] = ProviderConfig(
            name=str(name),
            enabled=bool(body.get("enabled", True)),
            role=str(body.get("role", "primary")),
            options={k: v for k, v in body.items() if k not in _RESERVED_PROVIDER_KEYS},
            quota_overrides=overrides,
            quota_override=allow_widen,
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
        for symbol in symbols:
            if not isinstance(symbol, str):
                raise ConfigError(f"universe {market.value} symbol {symbol!r} must be a string; quote it in YAML")
        cleaned = tuple(sorted({s.strip() for s in symbols if s.strip()}))
        if cleaned:
            universe[market] = cleaned

    kwargs: dict[str, Any] = {}
    if nodes:
        kwargs["nodes"] = tuple(nodes)
    if "adjustment" in conventions:
        kwargs["default_adjustment"] = _enum(Adjustment, conventions["adjustment"], "conventions.adjustment")
    if "us_hourly_alignment" in conventions:
        kwargs["us_hourly_alignment"] = _enum(
            HourlyAlignment, conventions["us_hourly_alignment"], "conventions.us_hourly_alignment"
        )
    if "include_extended_hours" in conventions:
        kwargs["include_extended_hours"] = _as_bool(
            conventions["include_extended_hours"], "conventions.include_extended_hours"
        )
    pg = storage.get("postgres") if isinstance(storage.get("postgres"), Mapping) else {}
    dsn = pg.get("dsn")
    dsn_text = str(dsn).strip() if dsn is not None and str(dsn).strip() else None
    mode = str(pg.get("mode", "native") or "native")
    if mode not in ("native", "upstream_compat"):
        raise ConfigError(f"storage.postgres.mode must be native or upstream_compat, not {mode!r}")
    schema_name = pg.get("schema")
    schema_text = str(schema_name).strip() if schema_name is not None and str(schema_name).strip() else None
    quota = str(pg.get("quota", "sqlite") or "sqlite").strip().lower()
    if quota not in ("sqlite", "postgres"):
        raise ConfigError(f"storage.postgres.quota must be sqlite or postgres, not {quota!r}")
    return Settings(
        selection=selection,
        providers=providers,
        storage_backend=str(storage.get("backend", "parquet_duckdb")),
        storage_path=str(storage.get("path", "./data")),
        postgres_dsn=dsn_text,
        postgres_mode=mode,
        postgres_write=_as_bool(pg.get("write", False), "storage.postgres.write"),
        postgres_schema=schema_text,
        postgres_quota=quota,
        universe=universe,
        retention=_retention_rules(storage.get("retention")),
        reconcile=dict(_section(data, "reconcile")),
        schedule=dict(_section(data, "schedule")),
        **kwargs,
    )


_RETENTION_KEYS = frozenset({"layer", "keep_years", "provider", "market", "interval"})


def _retention_rules(value: Any) -> tuple[RetentionRule, ...]:
    """``storage.retention``: a list of rules; absent / empty = retention off (#127)."""
    rules = []
    for index, entry in enumerate(_as_list(value, "storage.retention")):
        where = f"storage.retention[{index}]"
        if not isinstance(entry, Mapping):
            raise ConfigError(f"{where} must be a mapping with layer and keep_years")
        unknown = sorted(set(map(str, entry)) - _RETENTION_KEYS)
        if unknown:
            raise ConfigError(f"{where}: unknown keys {unknown}")
        layer = str(entry.get("layer", "")).strip()
        if layer not in RETAINABLE_LAYERS:
            raise ConfigError(
                f"{where}.layer must be one of {list(RETAINABLE_LAYERS)}; canonical and ref are always kept"
            )
        years = entry.get("keep_years")
        if isinstance(years, bool) or not isinstance(years, int) or years < 1:
            raise ConfigError(f"{where}.keep_years must be a whole number >= 1")
        market = entry.get("market")
        interval = entry.get("interval")
        rules.append(
            RetentionRule(
                layer=layer,
                keep_years=years,
                provider=str(entry["provider"]) if entry.get("provider") is not None else None,
                market=_enum(Market, market, f"{where}.market").value if market is not None else None,
                interval=_enum(Interval, interval, f"{where}.interval").value if interval is not None else None,
            )
        )
    return tuple(rules)


def _referenced_values(data: Any, env: Mapping[str, str], min_length: int = 4) -> list[str]:
    """Values substituted for ``${NAME}`` references in the raw YAML (to scrub from errors)."""
    names: set[str] = set()

    def walk(node: Any) -> None:
        if isinstance(node, str):
            names.update(m.group(1) for m in _ENV_REF.finditer(node))
        elif isinstance(node, list):
            for item in node:
                walk(item)
        elif isinstance(node, dict):
            for item in node.values():
                walk(item)

    walk(data)
    return sorted({env[n] for n in names if len(env.get(n, "")) >= min_length}, key=len, reverse=True)


def _scrub(text: str, values: list[str]) -> str:
    for value in values:
        text = text.replace(value, "***")
    return text


def _provider_env_refs(data: Mapping[str, Any]) -> dict[str, dict[str, tuple[str, ...]]]:
    """provider -> option -> env var names referenced by that option in the raw YAML."""
    out: dict[str, dict[str, tuple[str, ...]]] = {}
    providers = data.get("providers")
    if not isinstance(providers, Mapping):
        return out
    for name, body in providers.items():
        if not isinstance(body, Mapping):
            continue
        refs = {
            str(k): tuple(m.group(1) for m in _ENV_REF.finditer(v))
            for k, v in body.items()
            if isinstance(v, str) and _ENV_REF.search(v)
        }
        if refs:
            out[str(name)] = refs
    return out


def load_settings(
    path: str | Path,
    *,
    env: Mapping[str, str] | None = None,
    env_file: str | Path | None = None,
    default_storage_path: str = "./data",
) -> Settings:
    """Parse ``config.yaml``, optionally load a dotenv file, expand ``${ENV_VAR}`` and validate.

    ``default_storage_path`` is used only when the YAML omits ``storage.path``.
    Raises :class:`~openbars.core.errors.ConfigError` for a missing/unreadable file, bad
    YAML or an invalid market/interval/adjustment. Secret values never appear in the error.
    """
    file = Path(path)
    if not file.is_file():
        raise ConfigError(f"config file not found: {file}")
    try:
        data = yaml.safe_load(file.read_text(encoding="utf-8"))
    except yaml.YAMLError:
        raise ConfigError(f"config file {file} is not valid YAML") from None
    if data is None:
        data = {}
    if not isinstance(data, Mapping):
        raise ConfigError("config root must be a mapping")

    process_env = os.environ if env is None else env
    chosen, source = resolve_env_file(data, process_env, env_file=env_file, config_dir=file.parent)
    file_env = parse_env_file(chosen) if chosen is not None else None
    merged = merge_env(process_env, file_env)
    try:
        expanded = expand_env(data, merged)
        if "path" not in _section(expanded, "storage"):
            expanded.setdefault("storage", {})["path"] = default_storage_path
        settings = parse_settings(expanded)
    except ConfigError as exc:
        raise ConfigError(_scrub(str(exc), _referenced_values(data, merged))) from None
    refs = _provider_env_refs(data)
    providers = {
        name: replace(cfg, env_refs=refs[name]) if name in refs else cfg
        for name, cfg in settings.providers.items()
    }
    return replace(
        settings,
        providers=providers,
        env_file=str(chosen) if chosen is not None else None,
        env_file_source=source,
    )
