"""``openbars jobs retry`` (#151): explicit retry of dead / failed jobs.

``plan --submit`` and ``run`` never reset an existing job row, so a job that ended ``dead``
(permanent error or out of attempts) stays dead until the user asks for a retry here. The
reset rows go back to ``pending`` with a fresh attempt budget; the next ``run`` executes them.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable
from datetime import datetime
from typing import Any

from openbars.config.schema import Settings
from openbars.core.models import Interval, Market
from openbars.orchestrator.jobs import JobState

EXIT_OK, EXIT_ERROR, EXIT_USAGE = 0, 1, 2
LIST_LIMIT = 50


def register(sub: Any, common: Callable[[argparse.ArgumentParser], None]) -> None:
    jobs = sub.add_parser("jobs", help="job maintenance: `jobs retry` re-queues dead or failed jobs")
    common(jobs)
    actions = jobs.add_subparsers(dest="jobs_action")
    retry = actions.add_parser("retry", help="put dead (default) or failed jobs back to pending")
    common(retry)
    retry.add_argument("--dead", action="store_true", help="retry dead jobs (default when neither flag is given)")
    retry.add_argument("--failed", action="store_true", help="retry failed jobs now instead of at their retry_at")
    retry.add_argument("--provider", default=None, help="only this provider")
    retry.add_argument("--market", default=None, choices=[m.value for m in Market], help="only this market")
    retry.add_argument("--interval", default=None, choices=[i.value for i in Interval], help="only this interval")
    retry.add_argument("--error", default=None, metavar="TEXT", help="only jobs whose last error contains TEXT (case-insensitive)")
    retry.add_argument("--dry-run", action="store_true", help="list the matching jobs without changing them")


def run_jobs(args: argparse.Namespace, settings: Settings, out: Any, clock: Callable[[], datetime]) -> int:
    if getattr(args, "jobs_action", None) != "retry":
        out.err("error: usage: openbars jobs retry [--dead] [--failed] [--provider P] [--market M] [--interval I] [--error TEXT] [--dry-run]")
        return EXIT_USAGE
    from openbars.cli.main import _job_state, _open_backend
    from openbars.cli.wiring import data_dir

    root = data_dir(settings, args.data)
    db = root / "state.db"
    if settings.storage_backend == "postgres":
        # a real retry writes the PG job table: honour the configured write flag (like
        # ``plan --submit``); ``--dry-run`` stays read-only
        state = _job_state(_open_backend(settings, root, write=False if args.dry_run else None), db)
    elif not db.exists():
        out.err(f"error: no state database at {db} (nothing planned yet)")
        return EXIT_ERROR
    else:
        from openbars.storage.state import SqliteStateStore

        state = SqliteStateStore(db)
    states = [s for s, on in ((JobState.DEAD, args.dead), (JobState.FAILED, args.failed)) if on] or [JobState.DEAD]
    matched = state.retry_jobs(
        states=states,
        provider=args.provider,
        market=Market(args.market) if args.market else None,
        interval=Interval(args.interval) if args.interval else None,
        error_contains=args.error,
        now=clock(),
        dry_run=args.dry_run,
    )
    label = "/".join(s.value for s in states)
    for job in matched[:LIST_LIMIT]:
        out.line(
            f"  {job.job_id} {job.provider} {Market(job.market).value}/{Interval(job.interval).value} "
            f"{job.window.start}..{job.window.end} {job.state.value} attempts={job.attempts} error={job.last_error or ''}"
        )
    if len(matched) > LIST_LIMIT:
        out.line(f"  ... and {len(matched) - LIST_LIMIT} more")
    if args.dry_run:
        out.line(f"dry run: {len(matched)} {label} jobs would be reset to pending")
    else:
        out.line(f"retry: {len(matched)} {label} jobs reset to pending; `openbars run` executes them")
    return EXIT_OK
