"""Render effective settings for humans. Secret values are never printed."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from openbars.config.schema import Settings, is_secret_key

PRIORITY_HELP = (
    "priority (highest wins): process environment; then env file "
    "(--env-file, else OPENBARS_ENV_FILE, else config.yaml env_file, else ./.env next to config.yaml "
    "when it exists); then ${VAR:-default} in config.yaml. Literal YAML values are used as written. "
    "ALPACA_API_KEY / ALPACA_API_SECRET alias ALPACA_API_KEY_ID / ALPACA_API_SECRET_KEY "
    "when the canonical name is unset."
)


def _flag(value: object) -> str:
    if value is None:
        return "missing"
    text = str(value).strip()
    return "set" if text else "missing"


def _safe_option(name: str, value: object, secrets: tuple[str, ...]) -> str:
    if is_secret_key(name):
        return _flag(value)
    text = "" if value is None else str(value)
    if text and text in secrets:
        return "set"
    return text if text else "missing"


def format_settings(settings: Settings, *, config_path: str | Path | None = None) -> str:
    """Plain-text effective settings. Every secret is shown as ``set`` or ``missing``."""
    secrets = settings.secret_values()
    lines: list[str] = ["# effective settings (secrets redacted)"]
    if config_path is not None:
        lines.append(f"config: {config_path}")
    if settings.env_file:
        source = settings.env_file_source or "env_file"
        lines.append(f"env_file: {settings.env_file}  ({source})")
    else:
        lines.append("env_file: (none)")

    sel = settings.selection
    markets = ",".join(sorted(m.value for m in sel.markets)) or "(none)"
    intervals = ",".join(sorted(i.value for i in sel.intervals)) or "(none)"
    start = sel.history_start.isoformat() if sel.history_start else "max"
    end = sel.history_end.isoformat() if sel.history_end else "max"
    lines.append(f"markets: {markets}")
    lines.append(f"intervals: {intervals}")
    lines.append(f"history: {start} .. {end}")
    lines.append(f"storage: {settings.storage_backend}  {settings.storage_path}")
    if settings.storage_backend == "postgres":
        lines.append(f"  postgres.mode: {settings.postgres_mode}")
        lines.append(f"  postgres.write: {str(settings.postgres_write).lower()}")
        lines.append(f"  postgres.dsn: {'set' if settings.postgres_dsn else 'missing'}")
        lines.append(f"  postgres.quota: {settings.postgres_quota}")
        if settings.postgres_schema:
            lines.append(f"  postgres.schema: {settings.postgres_schema}")
    lines.append(f"adjustment: {settings.default_adjustment.value}")

    lines.append("providers:")
    if not settings.providers:
        lines.append("  (none)")
    for name in sorted(settings.providers):
        cfg = settings.providers[name]
        lines.append(f"  {name}:")
        lines.append(f"    enabled: {str(cfg.enabled).lower()}")
        lines.append(f"    role: {cfg.role}")
        for key in sorted(cfg.options):
            lines.append(f"    {key}: {_safe_option(str(key), cfg.options[key], secrets)}")
        if cfg.quota_overrides:
            quota = ", ".join(f"{k}={v:g}" for k, v in sorted(cfg.quota_overrides.items()))
            suffix = "  (override: true, may exceed defaults)" if cfg.quota_override else ""
            lines.append(f"    quota: {quota}{suffix}")

    lines.append("universe:")
    if not settings.universe:
        lines.append("  (none)")
    else:
        for market in sorted(settings.universe, key=lambda m: m.value):
            symbols = settings.universe[market]
            lines.append(f"  {market.value}: {len(symbols)} symbols")

    if settings.reconcile:
        lines.append(f"reconcile: {_mapping_preview(settings.reconcile, secrets)}")
    if settings.schedule:
        lines.append(f"schedule: {_mapping_preview(settings.schedule, secrets)}")
    lines.append(f"# {PRIORITY_HELP}")
    return "\n".join(lines) + "\n"


def _mapping_preview(raw: Mapping[str, object], secrets: tuple[str, ...]) -> str:
    parts = []
    for key, value in raw.items():
        if is_secret_key(str(key)):
            parts.append(f"{key}={_flag(value)}")
        elif isinstance(value, Mapping):
            inner = ", ".join(
                f"{k}={_safe_option(str(k), v, secrets)}" for k, v in value.items()
            )
            parts.append(f"{key}={{{inner}}}")
        else:
            parts.append(f"{key}={_safe_option(str(key), value, secrets)}")
    return ", ".join(parts)
