"""Compose settings into runtime objects (providers, quota gate, policies).

Nothing here talks to the network: real providers only open connections on ``fetch_*``.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import date
from pathlib import Path

from openbars.config.schema import ProviderConfig, Settings
from openbars.core.errors import ConfigError
from openbars.core.models import Market
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
    "binance": {},  # public market data: no key
    "coinbase": {},
    "baostock": {},  # anonymous login: no key (optional extra openbars[baostock])
}


# Providers whose constructor takes ``base_url`` (read from ``providers.<name>.base_url``).
_BASE_URL_PROVIDERS = frozenset({"binance", "coinbase"})

# Markets each known provider serves (used to decide whether missing credentials matter).
_PROVIDER_MARKETS: dict[str, frozenset[Market]] = {
    "alpaca": frozenset({Market.US}),
    "massive": frozenset({Market.US}),
    "yfinance": frozenset({Market.US, Market.HK}),
    "binance": frozenset({Market.CRYPTO}),
    "coinbase": frozenset({Market.CRYPTO}),
    "baostock": frozenset({Market.CN}),
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
        if missing_credentials(settings, name):
            continue  # treated as disabled (see ``skipped_providers`` / ``check_ready``)
        kwargs = {
            param: str(cfg.options.get(option) or "").strip()
            for param, option in _CREDENTIALS[name].items()
        }
        base_url = str(cfg.options.get("base_url") or "").strip()
        if base_url and name in _BASE_URL_PROVIDERS:
            kwargs["base_url"] = base_url
        out[name] = classes[name](**kwargs, quota_gate=gate)  # type: ignore[operator]
    return out


def make_sink(backend: object, providers: Mapping[str, Provider]):  # noqa: ANN201
    """The post-fetch sink for ``run``: US (XNYS, ``research-daily-v2``), crypto (``crypto-daily-v1``),
    A-share (``cn-daily-v1``) and Hong Kong (``hk-daily-v1``: RAW, yfinance primary, AKShare corroboration).

    ``backend`` is a ``ParquetDuckDbBackend`` (``raw``, ``canonical``, ``instruments``). Drop-in
    for the sink ``run`` builds inline: ``LocalNode(..., sink=make_sink(backend, providers))``.
    """
    from openbars.orchestrator.finalize import (
        CnCanonicalSink,
        CryptoReconcilingSink,
        DispatchingSink,
        HkCanonicalSink,
        ReconcilingSink,
        parquet_canonical_writer,
    )
    from openbars.reconcile.reconciler import MemoryReconciler

    writer = parquet_canonical_writer(backend.canonical)  # type: ignore[attr-defined]
    raw = backend.raw  # type: ignore[attr-defined]
    others = tuple(providers)
    return DispatchingSink(
        us=ReconcilingSink(MemoryReconciler(), writer, raw=raw, other_providers=others),
        crypto=CryptoReconcilingSink(
            backend.instruments, writer, raw=raw, other_providers=others  # type: ignore[attr-defined]
        ),
        cn=CnCanonicalSink(
            backend.instruments,  # type: ignore[attr-defined]
            backend.canonical.upsert_rows,  # type: ignore[attr-defined]
            providers=providers,
            factor_store=backend.instruments,  # type: ignore[attr-defined]
        ),
        hk=HkCanonicalSink(
            backend.instruments,  # type: ignore[attr-defined]
            writer,
            raw=raw,
            other_providers=others,
        ),
    )


def ingest_crypto_listings(
    backend: object,
    providers: Mapping[str, Provider],
    *,
    as_of: date,
    notes: list[str],
    markets: frozenset[Market] | set[Market] | None = None,
) -> str | None:
    """List crypto pairs and upsert them into ``backend.instruments`` (write paths only).

    Runs only when ``markets`` contains ``Market.CRYPTO``; calls ``list_instruments(CRYPTO)`` on
    enabled providers that declare a CRYPTO capability, never on any other provider. A listing
    failure or an unusable row is noted / counted and never raised. Returns a one-line summary
    (``None`` when nothing applies).
    """
    from openbars.normalize.crypto_pairs import ingest_crypto_pairs, listing_from_provider_instrument

    if markets is not None and Market.CRYPTO not in markets:
        return None
    listings = []
    skipped = 0
    listed_from = []
    for name, provider in providers.items():
        if not any(cap.market is Market.CRYPTO for cap in provider.capabilities()):
            continue
        try:
            rows = list(provider.list_instruments(Market.CRYPTO))
        except Exception as exc:  # noqa: BLE001 - a venue outage must not fail the run
            notes.append(f"crypto pair listing failed for {name}: {type(exc).__name__}")
            continue
        listed_from.append(name)
        for row in rows:
            try:
                listings.append(listing_from_provider_instrument(row, provider=name, venue=name))
            except ValueError:
                skipped += 1
    if not listed_from:
        return None
    summary = ingest_crypto_pairs(backend.instruments, listings, as_of=as_of)  # type: ignore[attr-defined]
    return (
        f"crypto pairs ({', '.join(listed_from)}): {summary.created} created, "
        f"{summary.reused} reused, {summary.unresolved + skipped} unresolved"
    )


def missing_credentials(settings: Settings, name: str) -> list[str]:
    """Required credential options of ``name`` that are empty, with the env var names they read.

    Only names are returned -- never values. Example: ``["api_key (MASSIVE_API_KEY)"]``.
    """
    cfg = settings.providers.get(name)
    if cfg is None:
        return []
    out = []
    for option in _CREDENTIALS.get(name, {}).values():
        if not str(cfg.options.get(option) or "").strip():
            refs = cfg.env_refs.get(option, ())
            out.append(f"{option} ({' / '.join(refs)})" if refs else option)
    return out


def skipped_providers(settings: Settings, active: Mapping[str, object]) -> list[str]:
    """Human-readable reasons for enabled providers that are not active."""
    notes = []
    for name, cfg in settings.providers.items():
        if cfg.enabled and name not in active:
            missing = missing_credentials(settings, name)
            if missing:
                reason = f"missing credentials: {', '.join(missing)}"
            elif name in _CREDENTIALS:
                reason = "provider not available"
            else:
                reason = "unsupported provider"
            notes.append(f"provider {name} enabled but skipped: {reason}")
    return notes


def check_ready(settings: Settings, providers: Mapping[str, Provider], *, strict: bool) -> None:
    """Fail early, with a clear message, when the selection cannot be served.

    * ``strict`` (``run``): an enabled ``primary`` provider for a selected market that lacks
      credentials is an error (names the variables, never the values).
    * every command: no enabled, built provider for any selected market, or a selected
      interval that no such provider advertises, is an error.
    Raises :class:`ConfigError`.
    """
    sel = settings.selection
    markets = ",".join(sorted(m.value for m in sel.markets))
    if strict:
        lines = []
        for name, cfg in settings.providers.items():
            if not (cfg.enabled and cfg.role == "primary" and name not in providers):
                continue
            if name in _PROVIDER_MARKETS and not (_PROVIDER_MARKETS[name] & sel.markets):
                continue
            missing = missing_credentials(settings, name)
            if missing:
                lines.append(f"  provider {name} (role primary): {', '.join(missing)}")
        if lines:
            raise ConfigError(
                "missing credentials for enabled primary provider(s):\n"
                + "\n".join(lines)
                + "\nSet the variable(s) in the environment or in .env / --env-file "
                "(`openbars config` shows what is set), or set `enabled: false` for that provider."
            )

    caps = [
        cap
        for provider in providers.values()
        for cap in provider.capabilities()
        if cap.market in sel.markets
    ]
    if not caps:
        notes = skipped_providers(settings, providers)
        detail = "".join(f"\n  {n}" for n in notes) or "\n  no provider is enabled in config.yaml"
        raise ConfigError(f"no usable provider for the selected market(s) {markets}:{detail}")
    offered = {cap.interval for cap in caps}
    for interval in sorted(sel.intervals, key=lambda i: i.duration, reverse=True):
        if interval not in offered:
            have = ", ".join(i.value for i in sorted(offered, key=lambda i: i.duration, reverse=True))
            raise ConfigError(
                f"interval {interval.value} is not implemented by any enabled provider "
                f"for market(s) {markets} (available: {have}); remove it from `intervals`"
            )


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
                policy = tighten(policy, cfg.quota_overrides, override=cfg.quota_override)
            except ValueError as exc:
                raise ConfigError(f"providers.{name}.quota: {exc}") from None
        out[name] = policy
    return out
