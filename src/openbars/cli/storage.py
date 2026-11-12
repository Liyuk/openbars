"""Storage maintenance subcommands (#127): ``usage``, ``compact``, ``retention``, ``backup``.

Parquet backend only. ``compact`` and ``retention`` are read-only plans unless ``--apply``
is given; retention is off unless ``storage.retention`` rules are configured, and only the
raw layer can be pruned. ``backup`` copies the data directory, never credentials.
Reports print paths relative to the data directory.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

from openbars.config.schema import Settings
from openbars.core.models import Interval, Market

EXIT_OK, EXIT_ERROR, EXIT_USAGE = 0, 1, 2
COMMANDS = ("usage", "compact", "retention", "backup")


def register(sub: Any, common: Callable[[argparse.ArgumentParser], None]) -> None:
    usage = sub.add_parser("usage", help="disk usage by layer / market / interval / year (read-only)")
    common(usage)
    usage.add_argument("--json", action="store_true", help="print the report as JSON")

    compact = sub.add_parser("compact", help="merge Parquet parts per partition (plan only unless --apply)")
    common(compact)
    compact.add_argument("--layer", choices=("raw", "canonical", "all"), default="all", help="layer to compact (default all)")
    compact.add_argument("--min-parts", type=int, default=2, help="only partitions with at least N parts (default 2)")
    compact.add_argument("--apply", action="store_true", help="actually compact (default: report only)")

    retention = sub.add_parser(
        "retention", help="list raw partitions older than storage.retention rules (delete only with --apply)"
    )
    common(retention)
    retention.add_argument("--apply", action="store_true", help="delete the listed partitions")

    backup = sub.add_parser("backup", help="copy the data directory to a new directory (no credentials)")
    common(backup)
    backup.add_argument("destination", help="new or empty directory outside the data directory")
    backup.add_argument("--market", action="append", default=[], choices=[m.value for m in Market],
                        help="only raw/canonical partitions of this market (repeatable)")  # fmt: skip
    backup.add_argument("--interval", action="append", default=[], choices=[i.value for i in Interval],
                        help="only raw/canonical partitions of this interval (repeatable)")  # fmt: skip


def run_storage(args: argparse.Namespace, settings: Settings, out: Any, clock: Callable[[], datetime]) -> int:
    from openbars.cli.wiring import data_dir

    if settings.storage_backend != "parquet_duckdb":
        out.err(f"error: `{args.command}` works on the parquet_duckdb backend only (configured: {settings.storage_backend})")
        return EXIT_USAGE
    root = data_dir(settings, args.data)
    handler = {"usage": _usage, "compact": _compact, "retention": _retention, "backup": _backup}[args.command]
    return handler(args, settings, out, root, clock)


def _no_data(out: Any, root: Path) -> int:
    out.err(f"error: no data directory at {root}")
    return EXIT_ERROR


# --- usage --------------------------------------------------------------------------------


def _usage(args: argparse.Namespace, settings: Settings, out: Any, root: Path, clock: Callable[[], datetime]) -> int:
    from openbars.storage.maintenance import human_bytes, summarize, usage

    if not root.is_dir():
        return _no_data(out, root)
    entries = usage(root)
    by_interval = summarize(entries, ("layer", "interval"))
    total_files = sum(e.files for e in entries)
    total_bytes = sum(e.bytes for e in entries)
    if args.json:
        out.line(json.dumps({
            "by_layer_interval": by_interval,
            "partitions": [e.as_dict() for e in entries],
            "total": {"files": total_files, "bytes": total_bytes},
        }, indent=2))  # fmt: skip
        return EXIT_OK
    out.line("storage usage (paths relative to the data directory):")
    out.line("by layer / interval:")
    for row in by_interval:
        out.line(f"  {row['layer']:<16} {row.get('interval', '-'):<4} {row['files']:>7} files  {human_bytes(row['bytes']):>10}")
    out.line("by partition (layer / provider / market / interval / year):")
    for e in entries:
        dims = " ".join(f"{k}={v}" for k, v in (("provider", e.provider), ("market", e.market), ("interval", e.interval), ("year", e.year)) if v)
        out.line(f"  {e.layer:<16} {dims or '-'}: {e.files} files, {human_bytes(e.bytes)}")
    out.line(f"total: {total_files} files, {human_bytes(total_bytes)}")
    return EXIT_OK


# --- compact ------------------------------------------------------------------------------


def _compact(args: argparse.Namespace, settings: Settings, out: Any, root: Path, clock: Callable[[], datetime]) -> int:
    from openbars.storage.maintenance import compact_plan, human_bytes

    if args.min_parts < 2:
        out.err("error: --min-parts must be >= 2")
        return EXIT_USAGE
    if not root.is_dir():
        return _no_data(out, root)
    layer = None if args.layer == "all" else args.layer
    plan = compact_plan(root, layer, min_parts=args.min_parts)
    out.line(f"compact plan (layer={args.layer}, min_parts={args.min_parts}):")
    for c in plan:
        out.line(
            f"  {c.partition}: {c.parts} parts -> 1, {c.rows_before} rows -> {c.rows_after} visible, "
            f"{human_bytes(c.bytes_before)} -> ~{human_bytes(c.estimated_bytes_after)}"
        )
    before = sum(c.bytes_before for c in plan)
    after = sum(c.estimated_bytes_after for c in plan)
    out.line(
        f"summary: {len(plan)} partitions, {sum(c.parts for c in plan)} parts, "
        f"~{human_bytes(max(before - after, 0))} reclaimable (estimate)"
    )
    if not args.apply:
        out.line("dry run: nothing changed (use --apply to compact)")
        return EXIT_OK
    from openbars.storage.parquet_duckdb import ParquetDuckDbBackend

    backend = ParquetDuckDbBackend(root)
    results = backend.compact(layer, min_parts=args.min_parts)
    for r in results:
        out.line(f"  compacted {r.partition}: {r.parts_before} parts, {r.rows_before} -> {r.rows_after} rows")
    out.line(f"compacted {len(results)} partitions")
    return EXIT_OK


# --- retention ----------------------------------------------------------------------------


def _retention(args: argparse.Namespace, settings: Settings, out: Any, root: Path, clock: Callable[[], datetime]) -> int:
    from openbars.storage.maintenance import apply_retention, human_bytes, retention_candidates

    if not settings.retention:
        out.line("retention: off (no storage.retention rules configured); nothing to delete")
        return EXIT_OK
    if not root.is_dir():
        return _no_data(out, root)
    now = clock()
    out.line("retention rules (canonical and ref are always kept):")
    for rule in settings.retention:
        out.line(f"  {rule.describe()}: partitions before year {now.year - rule.keep_years + 1}")
    candidates = retention_candidates(root, settings.retention, now=now)
    out.line(f"candidates ({len(candidates)}):")
    for c in candidates:
        out.line(f"  {c.partition}: {c.files} files, {human_bytes(c.bytes)}")
    out.line(f"total: {sum(c.files for c in candidates)} files, {human_bytes(sum(c.bytes for c in candidates))}")
    start = settings.selection.history_start
    oldest_kept = min(now.year - rule.keep_years + 1 for rule in settings.retention)
    if candidates and (start is None or start.year < oldest_kept):
        out.line(
            "note: history.start reaches before the retained window; later `plan` / `run` may fetch "
            "the pruned sessions again -- move history.start forward to avoid that"
        )
    if not args.apply:
        out.line("dry run: nothing deleted (use --apply to delete the candidates above)")
        return EXIT_OK
    from openbars.core.atomic import ProcessLock
    from openbars.storage.parquet_duckdb import LOCK_FILE

    removed = apply_retention(root, candidates, ProcessLock(root / LOCK_FILE))
    out.line(f"deleted {removed} partitions")
    return EXIT_OK


# --- backup -------------------------------------------------------------------------------


def _backup(args: argparse.Namespace, settings: Settings, out: Any, root: Path, clock: Callable[[], datetime]) -> int:
    from openbars.core.atomic import ProcessLock
    from openbars.storage.maintenance import backup, human_bytes
    from openbars.storage.parquet_duckdb import LOCK_FILE

    if not root.is_dir():
        return _no_data(out, root)
    report = backup(
        root,
        Path(args.destination).expanduser(),
        lock=ProcessLock(root / LOCK_FILE),
        markets=args.market,
        intervals=args.interval,
        now=clock(),
    )
    out.line(f"backup: {report.files} files, {human_bytes(report.bytes)} -> {args.destination}")
    if report.skipped:
        out.line(f"  skipped {len(report.skipped)} lock / temp / hidden / catalog files")
    out.line("  wrote backup-manifest.json; no config or credential files are copied")
    return EXIT_OK
