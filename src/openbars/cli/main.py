"""`openbars` CLI: ``init``, ``plan`` (dry run), ``run``, ``status``, ``verify``, ``config``, ``shadow``.

``plan`` never writes jobs unless ``--submit`` is given; ``run`` plans, submits and executes
until the queue is idle. Everything printed passes through a scrubber that masks configured
credential values. ``verify`` is a read-only report over the stored bars.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable, Mapping
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from openbars import __version__
from openbars.cli.wiring import (
    ProviderFactory,
    build_providers,
    check_ready,
    data_dir,
    effective_policies,
    ingest_crypto_listings,
    make_sink,
    skipped_providers,
)
from openbars.config.display import format_settings
from openbars.config.loader import load_settings
from openbars.config.schema import Settings
from openbars.core.errors import ConfigError
from openbars.orchestrator.jobs import JobState
from openbars.orchestrator.lock import SchedulerLock, SchedulerLocked, scheduler_lock_path
from openbars.orchestrator.planner import Budget, DefaultPlanner, Plan, RawStoreCoverage
from openbars.orchestrator.runner import DEFAULT_LEASE_TTL, LocalOrchestrator
from openbars.storage.state import SqliteStateStore

COMMANDS = ("init", "plan", "run", "backfill", "verify", "status", "config", "shadow")
EXIT_OK, EXIT_ERROR, EXIT_USAGE, EXIT_BUDGET = 0, 1, 2, 3
LIST_LIMIT = 50
DEFAULT_CONFIG = "./config.yaml"


class _Out:
    """stdout/stderr writer that masks credential values."""

    def __init__(self, secrets: tuple[str, ...] = ()) -> None:
        self.secrets = secrets

    def _scrub(self, text: str) -> str:
        for secret in self.secrets:
            text = text.replace(secret, "***")
        return text

    def line(self, text: str = "") -> None:
        print(self._scrub(text))

    def err(self, text: str) -> None:
        print(self._scrub(text), file=sys.stderr)


def _fmt_duration(delta: timedelta) -> str:
    seconds = int(delta.total_seconds())
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m"
    return f"{seconds / 3600:.1f}h"


def _iso_date(text: str) -> date:
    try:
        return date.fromisoformat(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not a YYYY-MM-DD date: {text!r}") from None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="openbars", description="openbars (pre-alpha)")
    parser.add_argument("--version", action="version", version=f"openbars {__version__}")
    parser.add_argument("--config", default=DEFAULT_CONFIG, help=f"config file (default {DEFAULT_CONFIG})")
    parser.add_argument("--data", default=None, help="storage directory (overrides storage.path)")
    parser.add_argument(
        "--env-file",
        default=None,
        help="dotenv file with provider keys (overrides OPENBARS_ENV_FILE and config env_file)",
    )
    sub = parser.add_subparsers(dest="command")

    def common(p: argparse.ArgumentParser) -> None:
        # Accept the global options after the subcommand too, without overriding them.
        p.add_argument("--config", default=argparse.SUPPRESS, help=argparse.SUPPRESS)
        p.add_argument("--data", default=argparse.SUPPRESS, help=argparse.SUPPRESS)
        p.add_argument("--env-file", default=argparse.SUPPRESS, help=argparse.SUPPRESS)

    init = sub.add_parser("init", help="write config.yaml and .env from the examples (never overwrites)")
    init.add_argument("directory", nargs="?", default=".", help="target directory (default .)")

    verify = sub.add_parser("verify", help="coverage and cross-source agreement report over stored bars")
    common(verify)
    verify.add_argument("--start", type=_iso_date, default=None, help="first session (default: history.start or oldest stored)")
    verify.add_argument("--end", type=_iso_date, default=None, help="last session (default: latest ready session)")
    verify.add_argument("--recent", type=int, default=20, help="also report the last N sessions (0 = off, default 20)")
    verify.add_argument("--json", default=None, metavar="PATH", help="also write the report as JSON under the data dir")

    plan = sub.add_parser("plan", help="show the jobs and request budget (dry run; --submit to queue them)")
    common(plan)
    plan.add_argument("--submit", action="store_true", help="write the planned jobs to the state db")
    plan.add_argument("--accept-budget", action="store_true", help="accept a large non-daily request budget")

    for name, help_text in (
        ("run", "plan, submit and execute jobs until idle"),
        ("backfill", "alias of run"),
    ):
        run = sub.add_parser(name, help=help_text)
        common(run)
        run.add_argument("--max-jobs", type=int, default=None, help="stop after claiming N jobs")
        run.add_argument("--workers", type=int, default=1, help="worker threads (default 1)")
        run.add_argument("--accept-budget", action="store_true", help="accept a large non-daily request budget")
        run.add_argument("--lease-ttl", type=float, default=DEFAULT_LEASE_TTL.total_seconds(), help=argparse.SUPPRESS)

    status = sub.add_parser("status", help="job counts by state, dead jobs, checkpoints")
    common(status)

    show = sub.add_parser("config", help="print the effective settings; secret values are redacted")
    common(show)

    shadow = sub.add_parser(
        "shadow",
        help="read-only compare of local canonical bars to a research-daily-v2 HTTP API",
    )
    common(shadow)
    shadow.add_argument("--symbols-file", required=True, help="text file of symbols, one per line (required; no default universe)")
    shadow.add_argument("--start", type=_iso_date, required=True, help="first session (inclusive)")
    shadow.add_argument("--end", type=_iso_date, required=True, help="last session (inclusive)")
    shadow.add_argument("--output", required=True, help="directory for the redacted report (json + txt); not a repo path")
    shadow.add_argument("--backend-url", default="http://127.0.0.1:8000", help="read-only backend base URL")
    shadow.add_argument("--policy-version", default="research-daily-v2", help="backend policy version")
    shadow.add_argument("--fetch", action="store_true", help="fetch the listed symbols through QuotaGate before comparing")
    return parser


def main(
    argv: list[str] | None = None,
    *,
    provider_factory: ProviderFactory | None = None,
    clock: Callable[[], datetime] | None = None,
    env: Mapping[str, str] | None = None,
) -> int:
    """CLI entry point. ``provider_factory``/``clock``/``env`` are injection points for tests."""
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help()
        return EXIT_OK
    if args.command == "init":
        from openbars.cli.init import init_project

        if args.config != DEFAULT_CONFIG:
            print("error: init takes a directory argument; --config is not used", file=sys.stderr)
            return EXIT_USAGE
        return init_project(args.directory, _Out(), exit_usage=EXIT_USAGE)

    out = _Out()
    try:
        settings = load_settings(args.config, env=env, env_file=getattr(args, "env_file", None))
        out = _Out(settings.secret_values())
        factory = provider_factory or build_providers
        now = clock or (lambda: datetime.now(UTC))
        if args.command == "config":
            return _show_config(args, settings, out)
        if args.command == "shadow":
            return _shadow(args, settings, out, factory, now)
        if args.command == "status":
            return _status(args, settings, out)
        if args.command == "verify":
            return _verify(args, settings, out, now)
        if args.command == "plan":
            return _plan(args, settings, out, factory, now)
        return _run(args, settings, out, factory, now)
    except ConfigError as exc:
        out.err(f"error: {exc}")
        return EXIT_USAGE
    except SchedulerLocked as exc:
        out.err(f"error: {exc}")
        return EXIT_ERROR
    except ImportError as exc:
        out.err(f"error: missing optional dependency ({exc.name}); install openbars[parquet]")
        return EXIT_USAGE


# --- shared -------------------------------------------------------------------------------


def _open_backend(path: Path) -> Any:
    from openbars.storage.parquet_duckdb import open_backend

    return open_backend(path)


def _crypto_master(settings: Settings, backend: Any) -> Any:
    """The pair master for planning: only when crypto is selected and it already holds pairs
    (otherwise the planner keeps its symbol heuristic). Read-only."""
    from openbars.core.models import Market

    if Market.CRYPTO not in settings.selection.markets:
        return None
    master = backend.instruments
    return master if any(m.instrument_id.startswith("crypto:") for m in master.mappings()) else None


def _ingest_pairs(settings: Settings, providers: Mapping[str, Any], backend: Any, now: datetime,
                  notes: list[str], out: _Out) -> None:  # fmt: skip
    """Write paths only (``run``, ``plan --submit``): list + ingest crypto pairs before planning."""
    summary = ingest_crypto_listings(
        backend, providers, as_of=now.date(), notes=notes, markets=settings.selection.markets
    )
    if summary:
        out.line(summary)


def _make_plan(settings: Settings, providers: Mapping[str, Any], backend: Any, now: datetime,
               master: Any = None) -> Plan:  # fmt: skip
    planner = DefaultPlanner(policies=effective_policies(settings, providers), instruments=master)
    return planner.plan(
        settings.selection,
        RawStoreCoverage(backend.raw),
        now=now,
        universe=settings.universe,
        providers=providers,
    )


def _print_plan(out: _Out, plan: Plan, state: SqliteStateStore | None, extra_notes: list[str]) -> int:
    new = len(plan) if state is None else sum(1 for job in plan.jobs if state.load_job(job.job_id) is None)
    out.line(f"plan: {len(plan)} jobs ({new} new, {len(plan) - new} already queued)")
    for (provider, kind), count in plan.counts().items():
        out.line(f"  {provider:<10} {kind:<9} {count}")
    out.line("budget (estimated requests, lower-bound duration from quota):")
    if not plan.budget.providers:
        out.line("  none")
    for name, b in plan.budget.providers.items():
        out.line(f"  {name:<10} {b.requests} requests in {b.jobs} jobs, >= {_fmt_duration(b.min_duration)}")
    if plan.budget.providers:
        out.line(f"  total      {plan.budget.total_requests} requests, >= {_fmt_duration(plan.budget.min_duration)}")
    for note in [*extra_notes, *(n.message for n in plan.notes)]:
        out.line(f"note: {note}")
    return new


def _budget_blocked(budget: Budget, accepted: bool, out: _Out) -> bool:
    if budget.requires_acceptance() and not accepted:
        out.err(
            "error: non-daily intervals need a large request budget "
            f"(>= {_fmt_duration(budget.min_duration)}); re-run with --accept-budget to proceed"
        )
        return True
    return False


# --- plan ---------------------------------------------------------------------------------


def _plan(args: argparse.Namespace, settings: Settings, out: _Out, factory: ProviderFactory,
          clock: Callable[[], datetime]) -> int:  # fmt: skip
    root = data_dir(settings, args.data)
    providers = factory(settings, None)
    notes = skipped_providers(settings, providers)
    check_ready(settings, providers, strict=False)
    backend = _open_backend(root)
    db = root / "state.db"
    if not args.submit:  # dry run: read-only apart from creating empty storage folders
        state = SqliteStateStore(db) if db.exists() else None
        plan = _make_plan(settings, providers, backend, clock(), _crypto_master(settings, backend))
        _print_plan(out, plan, state, notes)
        out.line("dry run: nothing written (use --submit to queue these jobs)")
        return EXIT_OK
    state = SqliteStateStore(db)
    with SchedulerLock(scheduler_lock_path(db)):  # singleton: do not plan under a running scheduler
        now = clock()
        _ingest_pairs(settings, providers, backend, now, notes, out)
        plan = _make_plan(settings, providers, backend, now, _crypto_master(settings, backend))
        _print_plan(out, plan, state, notes)
        if _budget_blocked(plan.budget, args.accept_budget, out):
            return EXIT_BUDGET
        out.line(f"submitted {state.submit_jobs(plan.jobs)} new jobs")
    return EXIT_OK


# --- run ----------------------------------------------------------------------------------


def _run(args: argparse.Namespace, settings: Settings, out: _Out, factory: ProviderFactory,
         clock: Callable[[], datetime]) -> int:  # fmt: skip
    from openbars.orchestrator.nodes import DEFAULT_FEEDS, LocalNode
    from openbars.quota.persistent import SqliteQuotaGate

    root = data_dir(settings, args.data)
    # The gate needs the providers' policies; build once without it to read them.
    probe = factory(settings, None)
    check_ready(settings, probe, strict=True)  # before anything is written
    root.mkdir(parents=True, exist_ok=True)
    backend = _open_backend(root)
    state = SqliteStateStore(root / "state.db")
    policies = effective_policies(settings, probe)
    gate = SqliteQuotaGate(root / "quota.db", policies)
    providers = factory(settings, gate)
    notes = skipped_providers(settings, providers)

    node = LocalNode(
        providers,
        store=state,
        raw=backend.raw,
        sink=make_sink(backend, providers),
        gate=gate,
        feeds={**DEFAULT_FEEDS, **{n: str(c.options["feed"]) for n, c in settings.providers.items() if c.options.get("feed")}},
        clock=clock,
        redact=out._scrub,
    )
    orch = LocalOrchestrator(
        state,
        node,
        policies=policies,
        workers=max(1, args.workers),
        lease_ttl=timedelta(seconds=args.lease_ttl),
        lock_path=scheduler_lock_path(root / "state.db"),
        clock=clock,
        redact=out._scrub,
    )
    with orch:
        now = clock()
        _ingest_pairs(settings, providers, backend, now, notes, out)
        plan = _make_plan(settings, providers, backend, now, _crypto_master(settings, backend))
        _print_plan(out, plan, state, notes)
        if _budget_blocked(plan.budget, args.accept_budget, out):
            return EXIT_BUDGET
        added = orch.submit(plan.jobs)
        out.line(f"submitted {added} new jobs")
        report = orch.run(until_idle=True, max_jobs=args.max_jobs)
    out.line(
        f"run: claimed {report.claimed}, done {report.done}, failed {report.failed}, "
        f"dead {report.dead}, requests {report.requests}; waiting {report.waiting}"
    )
    if report.open_circuits:
        out.line(f"circuit open: {', '.join(report.open_circuits)}")
    return EXIT_OK


# --- status -------------------------------------------------------------------------------


def _status(args: argparse.Namespace, settings: Settings, out: _Out) -> int:
    db = data_dir(settings, args.data) / "state.db"
    if not db.exists():
        out.line(f"no state database at {db} (nothing planned yet)")
        return EXIT_OK
    state = SqliteStateStore(db)
    counts = state.count_jobs()
    total = sum(counts.values())
    out.line(f"jobs: {total} total")
    out.line("  " + "  ".join(f"{s.value}={counts[s]}" for s in JobState))
    jobs = state.list_jobs()
    by_provider: dict[str, dict[str, int]] = {}
    for job in jobs:
        by_provider.setdefault(job.provider, {}).setdefault(job.state.value, 0)
        by_provider[job.provider][job.state.value] += 1
    for provider in sorted(by_provider):
        detail = " ".join(f"{k}={v}" for k, v in sorted(by_provider[provider].items()))
        out.line(f"  {provider}: {detail}")

    def show(title: str, selected: list[Any]) -> None:
        if not selected:
            return
        out.line(f"{title} ({len(selected)}):")
        for job in selected[:LIST_LIMIT]:
            extra = f" retry_at={job.retry_at.isoformat()}" if job.state is JobState.FAILED and job.retry_at else ""
            out.line(
                f"  {job.job_id} {job.provider} {job.market.value}/{job.interval.value} "
                f"{job.window.start}..{job.window.end} attempts={job.attempts}{extra} "
                f"error={job.last_error or '-'}"
            )
        if len(selected) > LIST_LIMIT:
            out.line(f"  ... {len(selected) - LIST_LIMIT} more")

    show("dead jobs", [j for j in jobs if j.state is JobState.DEAD])
    show("failed jobs waiting for retry", [j for j in jobs if j.state is JobState.FAILED])
    in_progress = [c for c in state.list_checkpoints() if c.cursor is not None]
    out.line(f"checkpoints: {len(in_progress)} in progress")
    for cp in in_progress[:LIST_LIMIT]:
        out.line(f"  {cp.job_id} cursor={cp.cursor}")
    out.line("circuits: tracked in the running process only (none persisted)")
    return EXIT_OK


# --- config --------------------------------------------------------------------------------


def _show_config(args: argparse.Namespace, settings: Settings, out: _Out) -> int:
    out.line(format_settings(settings, config_path=args.config).rstrip())
    return EXIT_OK


def _shadow(args: argparse.Namespace, settings: Settings, out: _Out, factory: ProviderFactory,
            clock) -> int:  # fmt: skip
    from openbars.cli.shadow import read_symbols_file, run_shadow
    from openbars.core.models import DateRange

    if args.end < args.start:
        out.err("error: --end is before --start")
        return EXIT_USAGE
    try:
        symbols = read_symbols_file(args.symbols_file)
        code, _report, lines = run_shadow(
            settings=settings,
            symbols=symbols,
            window=DateRange(args.start, args.end),
            output=Path(args.output),
            backend_url=args.backend_url,
            policy_version=args.policy_version,
            fetch=bool(args.fetch),
            data=args.data,
            factory=factory,
            clock=clock,
        )
    except ConfigError as exc:
        out.err(f"error: {exc}")
        return EXIT_USAGE
    for line in lines:
        out.line(line)
    return code




# --- verify -------------------------------------------------------------------------------



def _verify(args: argparse.Namespace, settings: Settings, out: _Out, clock: Callable[[], datetime]) -> int:
    import json

    from openbars.calendars import XNYSCalendar
    from openbars.core.models import DateRange, Market
    from openbars.orchestrator.planner import latest_ready_session
    from openbars.orchestrator.verify import (
        Tolerances,
        build_aliases,
        format_report,
        load_canonical,
        load_raw_bars,
        stored_session_range,
        verify,
    )

    if Market.US not in settings.selection.markets:
        out.err("error: verify supports the us market only (XNYS calendar); add 'us' to markets")
        return EXIT_USAGE
    if args.recent < 0:
        out.err("error: --recent must be >= 0")
        return EXIT_USAGE
    root = data_dir(settings, args.data)
    if not root.is_dir():
        out.err(f"error: no data directory at {root}; run `openbars run` first")
        return EXIT_ERROR
    json_path = None
    if args.json:
        json_path = (root / args.json).resolve()  # relative paths live under the data dir
        if not json_path.is_relative_to(root.resolve()):
            out.err("error: --json must point inside the data directory")
            return EXIT_USAGE

    tolerances = Tolerances.from_settings(settings)
    backend = _open_backend(root)
    ready = latest_ready_session(clock())
    sel = settings.selection
    bounds = [d for d in (sel.history_end, ready) if d is not None]
    end = args.end or (min(bounds) if bounds else None)
    start = args.start or sel.history_start
    probe = DateRange(start or date(1970, 1, 1), end or clock().date())
    stored = stored_session_range(backend, Market.US, probe)
    if stored is None:
        out.err(f"error: no stored bars in {root} for the requested window; run `openbars run` first")
        return EXIT_ERROR
    window = DateRange(start or stored.start, end or stored.end)
    if window.end < window.start:
        out.err(f"error: empty window {window.start}..{window.end}")
        return EXIT_USAGE

    raw = load_raw_bars(backend, Market.US, window)
    canonical = load_canonical(backend, Market.US, window)
    notes = []
    symbols = settings.universe.get(Market.US, ())
    if symbols:
        universe = list(symbols)
    else:
        master = backend.instruments.instruments()
        universe = [i.instrument_id for i in master if i.market is Market.US]
        if not universe:
            universe = sorted({b.key for b in raw} | {c.key for c in canonical})
            notes.append("no universe configured or registered: using every symbol found in the store")
    aliases = build_aliases(
        backend.instruments, [*universe, *(b.key for b in raw), *(c.key for c in canonical)],
        on=window.end, market=Market.US,
    )  # fmt: skip
    report = verify(
        raw, canonical, calendar=XNYSCalendar(), window=window, universe=universe,
        tolerances=tolerances, aliases=aliases, recent_sessions=args.recent or None,
    )  # fmt: skip
    for line in [*format_report(report), *(f"note: {n}" for n in notes)]:
        out.line(line)
    if json_path is not None:
        json_path.parent.mkdir(parents=True, exist_ok=True)
        payload = report.to_dict()
        payload["notes"] = [*payload["notes"], *notes]
        json_path.write_text(out._scrub(json.dumps(payload, indent=2, sort_keys=True)) + "\n", encoding="utf-8")
        out.line(f"wrote {json_path}")
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
