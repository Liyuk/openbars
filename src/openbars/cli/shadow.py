"""``openbars shadow``: read-only compare of local canonical bars vs a research-daily-v2 API.

The backend is only contacted with GET. Credentials and bearer tokens are never printed,
written into the report, or placed on the query string. Provider fetches, when requested,
always pass an explicit symbol list (never a whole-market grouped call) and go through the
injected QuotaGate.
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen

from openbars.cli.wiring import (
    ProviderFactory,
    build_providers,
    data_dir,
    effective_policies,
    skipped_providers,
)
from openbars.config.schema import Settings
from openbars.core.errors import ConfigError, ProviderError
from openbars.core.models import DateRange, Interval, Market
from openbars.providers.base import FetchRequest
from openbars.reconcile.shadow import (
    PRICE_TOLERANCE_PCT,
    VOLUME_TOLERANCE_PCT,
    ShadowBar,
    ShadowReport,
    compare_shadow,
    format_report,
)

DEFAULT_BACKEND_URL = "http://127.0.0.1:8000"
DEFAULT_POLICY = "research-daily-v2"
# Optional bearer, read from the environment. Never logged.
TOKEN_ENV_VARS = ("OPENBARS_BACKEND_TOKEN", "RESEARCH_RUNNER_TOKEN")
SERIES_PATH = "/api/v1/market/research-series/daily"


def read_symbols_file(path: str | Path) -> list[str]:
    """Read tickers from a text file (one per line, ``#`` comments). No default universe."""
    file = Path(path)
    if not file.is_file():
        raise ConfigError(f"symbols file not found: {file}")
    out: list[str] = []
    seen: set[str] = set()
    for line in file.read_text(encoding="utf-8").splitlines():
        text = line.split("#", 1)[0].strip()
        if not text:
            continue
        symbol = text.upper()
        if symbol not in seen:
            seen.add(symbol)
            out.append(symbol)
    if not out:
        raise ConfigError(f"symbols file is empty: {file}")
    return out


def backend_token(env: Mapping[str, str] | None = None) -> str | None:
    source = os.environ if env is None else env
    for name in TOKEN_ENV_VARS:
        value = str(source.get(name) or "").strip()
        if value:
            return value
    return None


def _scrub(text: str, secrets: Sequence[str]) -> str:
    out = text
    for secret in secrets:
        if secret:
            out = out.replace(secret, "***")
    return out


def fetch_backend_series(
    base_url: str,
    symbol: str,
    start: date,
    end: date,
    *,
    policy_version: str = DEFAULT_POLICY,
    token: str | None = None,
    timeout: float = 30,
    opener: Callable[..., Any] | None = None,
) -> tuple[list[ShadowBar], str | None]:
    """GET one symbol's research-daily-v2 series. Returns (bars, error_code_or_none)."""
    query = urlencode(
        {
            "symbol": symbol,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "policy_version": policy_version,
        }
    )
    root = base_url.rstrip("/")
    url = f"{root}{SERIES_PATH}?{query}"
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    request = Request(url, headers=headers, method="GET")
    open_url = opener or urlopen
    try:
        with open_url(request, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        body = exc.read().decode("utf-8", "replace") if exc.fp else ""
        code = None
        try:
            code = json.loads(body).get("error", {}).get("code") or json.loads(body).get("code")
        except json.JSONDecodeError:
            code = None
        return [], code or f"http_{exc.code}"
    except URLError as exc:
        return [], "backend_unreachable"
    except json.JSONDecodeError:
        return [], "invalid_payload"
    if not isinstance(payload, dict):
        return [], "invalid_payload"
    items = payload.get("items") or payload.get("bars") or []
    bars: list[ShadowBar] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        source = item.get("source") if isinstance(item.get("source"), dict) else {}
        merged = {
            **item,
            "provider": item.get("provider") or source.get("provider"),
            "feed": item.get("feed") or source.get("feed"),
            "adjustment": item.get("adjustment") or source.get("adjustment"),
        }
        try:
            bars.append(ShadowBar.from_mapping(merged))
        except (ValueError, KeyError, ArithmeticError):
            continue
    return bars, None


def load_local_series(backend: Any, symbols: Sequence[str], window: DateRange) -> dict[str, list[ShadowBar]]:
    """Read canonical SPLIT bars for ``symbols`` from an openbars store."""
    out: dict[str, list[ShadowBar]] = {s: [] for s in symbols}
    reader = getattr(backend, "reader", None)
    if reader is None:
        return out
    table = reader.bars(
        list(symbols),
        interval=Interval.D1,
        start=window.start,
        end=window.end,
        market=Market.US,
    )
    rows = table.to_pylist() if hasattr(table, "to_pylist") else list(table)
    alias: dict[str, str] = {}
    # Prefer matching by the requested symbol; fall back to instrument_id equality.
    wanted = {s.upper(): s for s in symbols}
    for row in rows:
        iid = str(row.get("instrument_id") or "")
        key = wanted.get(iid.upper()) or wanted.get(str(row.get("symbol") or "").upper())
        if key is None:
            continue
        try:
            out[key].append(
                ShadowBar.from_mapping(
                    {
                        "date": row.get("session_date") or row.get("date"),
                        "open": row["open"],
                        "high": row["high"],
                        "low": row["low"],
                        "close": row["close"],
                        "volume": row.get("volume"),
                        "adjustment": row.get("adjustment"),
                        "provider": row.get("provider") or row.get("price_source"),
                        "feed": row.get("feed"),
                    }
                )
            )
        except (ValueError, KeyError, ArithmeticError):
            continue
        alias[iid] = key
    return out


def fetch_local(
    settings: Settings,
    symbols: Sequence[str],
    window: DateRange,
    *,
    root: Path,
    factory: ProviderFactory,
    clock: Callable[[], datetime],
    secrets: Sequence[str] = (),
) -> dict[str, int]:
    """Fetch explicit symbols through QuotaGate and reconcile into ``root``. No whole-market calls."""
    from openbars.orchestrator.finalize import ReconcilingSink, parquet_canonical_writer
    from openbars.orchestrator.jobs import Job, JobKind
    from openbars.quota.persistent import SqliteQuotaGate
    from openbars.reconcile.reconciler import MemoryReconciler
    from openbars.storage.parquet_duckdb import open_backend

    if not symbols:
        raise ConfigError("shadow fetch requires an explicit symbol list")
    probe = factory(settings, None)
    root.mkdir(parents=True, exist_ok=True)
    store = open_backend(root)
    policies = effective_policies(settings, probe)
    gate = SqliteQuotaGate(root / "quota.db", policies)
    providers = factory(settings, gate)
    if not providers:
        notes = skipped_providers(settings, providers)
        raise ConfigError("no usable provider for shadow fetch: " + ("; ".join(notes) or "none enabled"))
    sink = ReconcilingSink(
        MemoryReconciler(),
        parquet_canonical_writer(store.canonical),
        raw=store.raw,
        other_providers=tuple(providers),
    )
    request = FetchRequest(
        market=Market.US,
        interval=Interval.D1,
        symbols=tuple(symbols),
        window=window,
    )
    acquired: dict[str, int] = {}
    for name, provider in providers.items():
        # Guard: never call a whole-market grouped endpoint.
        if not request.symbols:
            raise ConfigError("refusing a whole-market fetch; pass an explicit symbol list")
        try:
            batch = provider.fetch_bars(request)
        except ProviderError as exc:
            raise ConfigError(_scrub(f"provider {name} failed: {exc}", secrets)) from None
        acquired[name] = acquired.get(name, 0) + 1
        if batch.bars:
            store.raw.append(batch)
            job = Job(
                job_id=f"shadow-{name}",
                kind=JobKind.VERIFY,
                provider=name,
                market=Market.US,
                interval=Interval.D1,
                instruments=tuple(symbols),
                window=window,
            )
            sink.after_fetch(job, batch.bars)
        if batch.error is not None and not batch.bars:
            raise ConfigError(_scrub(f"provider {name} failed: {batch.error}", secrets))
    _ = clock  # reserved for future readiness checks; fetch uses the provider clock
    return acquired


def write_report(report: ShadowReport, output: Path, secrets: Sequence[str]) -> Path:
    output.mkdir(parents=True, exist_ok=True)
    json_path = output / "shadow-report.json"
    text_path = output / "shadow-report.txt"
    payload = json.dumps(report.to_dict(), indent=2, sort_keys=True)
    payload = _scrub(payload, secrets)
    json_path.write_text(payload + "\n", encoding="utf-8")
    text_path.write_text(_scrub("\n".join(format_report(report)) + "\n", secrets), encoding="utf-8")
    return json_path


def run_shadow(
    *,
    settings: Settings,
    symbols: Sequence[str],
    window: DateRange,
    output: Path,
    backend_url: str = DEFAULT_BACKEND_URL,
    policy_version: str = DEFAULT_POLICY,
    fetch: bool = True,
    data: str | Path | None = None,
    env: Mapping[str, str] | None = None,
    factory: ProviderFactory | None = None,
    clock: Callable[[], datetime] | None = None,
    opener: Callable[..., Any] | None = None,
    now: Callable[[], datetime] | None = None,
) -> tuple[int, ShadowReport, list[str]]:
    """Run a shadow compare. Returns (exit_code, report, lines). Exit 0 even when agreement < 100%."""
    from datetime import UTC

    from openbars.storage.parquet_duckdb import open_backend

    secrets = settings.secret_values()
    token = backend_token(env)
    if token:
        secrets = tuple({*secrets, token})
    root = data_dir(settings, data)
    notes: list[str] = []
    if fetch:
        counts = fetch_local(
            settings, symbols, window, root=root,
            factory=factory or build_providers,
            clock=clock or now or (lambda: datetime.now(UTC)),
            secrets=secrets,
        )
        notes.append("fetch acquires (logical calls, one per provider): " + ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))
        notes.append("fetches used an explicit symbol list (no whole-market grouped request)")
    if not root.is_dir():
        raise ConfigError(f"no data directory at {root}; rerun with --fetch or pass --data")

    store = open_backend(root)
    local = load_local_series(store, symbols, window)
    series: dict[str, tuple[list[ShadowBar], list[ShadowBar]]] = {}
    for symbol in symbols:
        bars, err = fetch_backend_series(
            backend_url, symbol, window.start, window.end,
            policy_version=policy_version, token=token, opener=opener,
        )
        if err:
            notes.append(f"{symbol}: backend returned {err}")
        series[symbol] = (local.get(symbol, []), bars)
        if not local.get(symbol) and not err:
            notes.append(f"{symbol}: no local canonical bars in the window")
    report = compare_shadow(
        series,
        window_start=window.start,
        window_end=window.end,
        policy_version=policy_version,
        price_tolerance_pct=PRICE_TOLERANCE_PCT,
        volume_tolerance_pct=VOLUME_TOLERANCE_PCT,
        notes=notes,
    )
    write_report(report, output, secrets)
    host = urlsplit(backend_url).hostname or "backend"
    lines = format_report(report)
    lines.append(f"backend host: {host} (read-only GET)")
    lines.append(f"wrote {_scrub(str(output), secrets)}")
    return 0, report, [_scrub(line, secrets) for line in lines]
