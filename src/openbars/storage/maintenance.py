"""Storage maintenance for the Parquet backend: usage, compaction plan, retention, backup (#127).

Everything here works on the data directory layout described in ``parquet_duckdb`` and
docs/architecture.md section 10. Reports use paths *relative* to the data directory only.

* ``usage`` -- file count and bytes per layer / market / interval / year (raw also by provider).
* ``compact_plan`` -- read-only: which partitions ``compact`` would merge, and an estimate of
  what it frees. ``ParquetDuckDbBackend.compact`` does the work; semantics are unchanged.
* ``retention_candidates`` / ``apply_retention`` -- off unless rules are configured; only the
  ``raw`` layer may be pruned (canonical and ref are always kept); candidates are whole
  ``year=`` partitions, listed before anything is deleted.
* ``backup`` -- copies the data directory (optionally narrowed by market / interval) to a
  new directory, holding the writer lock; skips the lock file, temp files, dotfiles and the
  rebuildable DuckDB catalog. The data directory holds no credentials (keys live in the
  config / dotenv file, which is never copied).
"""

from __future__ import annotations

import json
import shutil
import sqlite3
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from openbars.config.schema import RETAINABLE_LAYERS, RetentionRule
from openbars.storage.errors import StorageError
from openbars.storage.parquet_duckdb import (
    CATALOG_FILE,
    LAYERS,
    LOCK_FILE,
    PART_GLOB,
    _dedup_sql,
    _mem_connection,
    _PartitionedTable,
)

_SQLITE_FILES = ("state.db", "quota.db")
_SQLITE_SIDECARS = ("-wal", "-shm", "-journal")


def _dims(parts: Iterable[str]) -> dict[str, str]:
    return dict(seg.split("=", 1) for seg in parts if "=" in seg)


# --- usage --------------------------------------------------------------------------------


@dataclass(frozen=True)
class UsageEntry:
    layer: str
    provider: str | None
    market: str | None
    interval: str | None
    year: str | None
    files: int
    bytes: int

    def as_dict(self) -> dict[str, Any]:
        return {k: v for k, v in self.__dict__.items() if v is not None}


def _usage_key(root: Path, path: Path) -> tuple[str, str | None, str | None, str | None, str | None]:
    rel = path.relative_to(root).parts
    top = rel[0] if len(rel) > 1 else "(top)"
    if top in ("raw", "canonical", "raw-archive"):
        dims = _dims(rel[1:-1])
        layer = top
        if path.name.startswith(".") and path.name.endswith(".tmp"):
            layer = f"{top} (tmp)"
        return (layer, dims.get("provider"), dims.get("market"), dims.get("interval"), dims.get("year"))
    return (top, None, None, None, None)


def usage(root: str | Path) -> list[UsageEntry]:
    """File count and bytes grouped by layer and partition dims. Read-only; never creates files."""
    base = Path(root)
    if not base.is_dir():
        return []
    totals: dict[tuple[Any, ...], list[int]] = defaultdict(lambda: [0, 0])
    for path in base.rglob("*"):
        if not path.is_file() or path.is_symlink():
            continue
        key = _usage_key(base, path)
        totals[key][0] += 1
        totals[key][1] += path.stat().st_size
    order = lambda k: tuple("" if v is None else v for v in k)  # noqa: E731
    return [UsageEntry(*key, files, size) for key, (files, size) in sorted(totals.items(), key=lambda kv: order(kv[0]))]


def summarize(entries: Sequence[UsageEntry], by: Sequence[str]) -> list[dict[str, Any]]:
    """Roll ``usage`` entries up to the given dims (e.g. ``("layer", "interval")``)."""
    acc: dict[tuple[Any, ...], list[int]] = defaultdict(lambda: [0, 0])
    for entry in entries:
        key = tuple(getattr(entry, dim) for dim in by)
        acc[key][0] += entry.files
        acc[key][1] += entry.bytes
    rows = []
    for key in sorted(acc, key=lambda k: tuple("" if v is None else v for v in k)):
        row = {dim: value for dim, value in zip(by, key) if value is not None}
        row.update(files=acc[key][0], bytes=acc[key][1])
        rows.append(row)
    return rows


def human_bytes(size: int) -> str:
    value = float(size)
    for unit in ("B", "KiB", "MiB", "GiB"):
        if value < 1024 or unit == "GiB":
            return f"{value:.0f} {unit}" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} TiB"  # pragma: no cover


# --- compaction plan ----------------------------------------------------------------------


@dataclass(frozen=True)
class CompactionCandidate:
    layer: str
    partition: str  # relative to the data directory
    parts: int
    rows_before: int  # including superseded duplicates
    rows_after: int  # retained rows: visible rows for raw, all distinct canonical history rows
    bytes_before: int

    @property
    def estimated_bytes_after(self) -> int:
        """Rough: proportional to surviving rows (Parquet compression varies)."""
        if not self.rows_before:
            return 0
        return int(self.bytes_before * self.rows_after / self.rows_before)


class _ReadOnlyBackend:
    """Just enough of the backend for ``_PartitionedTable`` scans, without creating folders."""

    def __init__(self, root: Path) -> None:
        from openbars.storage.parquet_duckdb import check_parquet_versions

        self.root = root
        self.check_versions = check_parquet_versions


def _layers(layer: str | None) -> list[str]:
    names = list(LAYERS) if layer in (None, "all") else [layer]
    for name in names:
        if name not in LAYERS:
            raise ValueError(f"unknown layer {name!r} (raw, canonical or all)")
    return names


def compact_plan(root: str | Path, layer: str | None = None, *, min_parts: int = 2) -> list[CompactionCandidate]:
    """Partitions ``compact(layer, min_parts=...)`` would merge and rows each layer retains."""
    base = Path(root)
    backend = _ReadOnlyBackend(base)
    out: list[CompactionCandidate] = []
    for name in _layers(layer):
        table = _PartitionedTable(backend, LAYERS[name])  # type: ignore[arg-type]
        by_dir: dict[Path, list[Path]] = defaultdict(list)
        for part in table.parts():
            by_dir[part.parent].append(part)
        for directory, files in sorted(by_dir.items()):
            if len(files) < max(min_parts, 2):
                continue
            backend.check_versions(files)
            rows_before = sum(pq.ParquetFile(p).metadata.num_rows for p in files)
            con = _mem_connection()
            try:
                if name == "canonical":
                    has_rank = table._has_write_rank(files)
                    rank_expr = (
                        "coalesce(__write_rank, try_cast(regexp_extract(filename, 'part-([0-9]+)', 1) AS BIGINT))"
                        if has_rank else "try_cast(regexp_extract(filename, 'part-([0-9]+)', 1) AS BIGINT)"
                    )
                    exclude = "filename, __write_rank" if has_rank else "filename"
                    retained = (
                        f"SELECT DISTINCT * EXCLUDE ({exclude}), {rank_expr} AS __write_rank "
                        "FROM read_parquet(?, filename=true, union_by_name=true)"
                    )
                    sql = f"SELECT count(*) FROM ({retained})"
                else:
                    sql = f"SELECT count(*) FROM ({_dedup_sql(LAYERS[name], 'read_parquet(?, filename=true, union_by_name=true)')})"
                row = con.execute(sql, [[str(p) for p in files]]).fetchone()
            finally:
                con.close()
            out.append(
                CompactionCandidate(
                    name,
                    directory.relative_to(base).as_posix(),
                    len(files),
                    rows_before,
                    int(row[0]) if row else 0,
                    sum(p.stat().st_size for p in files),
                )
            )
    return out


# --- retention ----------------------------------------------------------------------------


@dataclass(frozen=True)
class RetentionCandidate:
    rule: RetentionRule
    partition: str  # relative ``.../year=YYYY`` directory
    files: int
    bytes: int


def retention_candidates(
    root: str | Path, rules: Sequence[RetentionRule], *, now: datetime
) -> list[RetentionCandidate]:
    """Whole ``year=`` partitions older than each rule's window. Read-only."""
    base = Path(root)
    seen: set[Path] = set()
    out: list[RetentionCandidate] = []
    for rule in rules:
        if rule.layer not in RETAINABLE_LAYERS:
            raise StorageError(f"retention may only prune {', '.join(RETAINABLE_LAYERS)}; {rule.layer!r} is always kept")
        first_kept = now.year - rule.keep_years + 1
        layer_dir = base / LAYERS[rule.layer].base
        if not layer_dir.is_dir():
            continue
        for year_dir in sorted(layer_dir.rglob("year=*")):
            if not year_dir.is_dir() or year_dir.is_symlink() or year_dir in seen:
                continue
            dims = _dims(year_dir.relative_to(layer_dir).parts)
            try:
                year = int(dims.get("year", ""))
            except ValueError:
                continue
            if year >= first_kept or not rule.matches(dims):
                continue
            files = [p for p in year_dir.rglob("*") if p.is_file()]
            seen.add(year_dir)
            out.append(
                RetentionCandidate(
                    rule, year_dir.relative_to(base).as_posix(), len(files), sum(p.stat().st_size for p in files)
                )
            )
    return out


def apply_retention(root: str | Path, candidates: Sequence[RetentionCandidate], lock: Any) -> int:
    """Delete the listed partitions under the writer lock; returns the count removed."""
    base = Path(root).resolve()
    removed = 0
    with lock:
        for candidate in candidates:
            target = (base / candidate.partition).resolve()
            allowed = [base / LAYERS[name].base for name in RETAINABLE_LAYERS]
            if not any(target.is_relative_to(a.resolve()) and target != a.resolve() for a in allowed):
                raise StorageError(f"refusing to delete outside the raw layer: {candidate.partition}")
            if target.is_dir():
                shutil.rmtree(target)
                removed += 1
    return removed


# --- backup -------------------------------------------------------------------------------


@dataclass
class BackupReport:
    destination: Path
    files: int = 0
    bytes: int = 0
    skipped: list[str] = field(default_factory=list)


def _skip(rel: Path) -> bool:
    name = rel.name
    if rel.as_posix() in (LOCK_FILE, CATALOG_FILE, f"{CATALOG_FILE}.wal"):
        return True
    if any(seg.startswith(".") for seg in rel.parts):  # .lock, .env, .part-*.tmp, ...
        return True
    if name.endswith(".tmp"):
        return True
    return any(name == f"{db}{suffix}" for db in _SQLITE_FILES for suffix in _SQLITE_SIDECARS)


def _wanted(rel: Path, markets: frozenset[str], intervals: frozenset[str]) -> bool:
    if rel.parts[0] not in ("raw", "canonical", "raw-archive"):
        return True  # ref/, state.db, audit/ ... are small and market-wide
    dims = _dims(rel.parts[1:-1])
    if markets and dims.get("market") not in markets:
        return False
    if intervals and dims.get("interval") not in intervals:
        return False
    return True


def backup(
    root: str | Path,
    destination: str | Path,
    *,
    lock: Any,
    markets: Iterable[str] = (),
    intervals: Iterable[str] = (),
    now: datetime,
) -> BackupReport:
    """Copy the data directory to ``destination`` (must not exist or be empty).

    Holds the writer lock so no part is half-written; SQLite files are copied with the
    online backup API. Writes ``backup-manifest.json`` (relative paths and sizes only).
    """
    src = Path(root).resolve()
    dst = Path(destination).resolve()
    if not src.is_dir():
        raise StorageError(f"no data directory at {root}")
    if dst == src or dst.is_relative_to(src):
        raise StorageError("backup destination must be outside the data directory")
    if src.is_relative_to(dst):
        raise StorageError("backup destination must not contain the data directory")
    if dst.exists() and (not dst.is_dir() or any(dst.iterdir())):
        raise StorageError("backup destination exists and is not an empty directory")
    want_markets, want_intervals = frozenset(markets), frozenset(intervals)
    report = BackupReport(dst)
    manifest: list[dict[str, Any]] = []
    with lock:
        dst.mkdir(parents=True, exist_ok=True)
        for path in sorted(src.rglob("*")):
            if not path.is_file() or path.is_symlink():
                continue
            rel = path.relative_to(src)
            if _skip(rel):
                report.skipped.append(rel.as_posix())
                continue
            if not _wanted(rel, want_markets, want_intervals):
                continue
            target = dst / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            if rel.as_posix() in _SQLITE_FILES:
                _sqlite_copy(path, target)
            else:
                shutil.copy2(path, target)
            size = target.stat().st_size
            report.files += 1
            report.bytes += size
            manifest.append({"path": rel.as_posix(), "bytes": size})
    from openbars import __version__
    from openbars.storage.schema import SCHEMA_VERSION

    (dst / "backup-manifest.json").write_text(
        json.dumps(
            {
                "openbars_version": __version__,
                "schema_version": SCHEMA_VERSION,
                "created_at": now.isoformat(),
                "markets": sorted(want_markets),
                "intervals": sorted(want_intervals),
                "files": manifest,
            },
            indent=2,
        )
        + "\n"
    )
    return report


def _sqlite_copy(src: Path, dst: Path) -> None:
    source = sqlite3.connect(f"file:{src}?mode=ro", uri=True)
    try:
        target = sqlite3.connect(dst)
        try:
            source.backup(target)
        finally:
            target.close()
    finally:
        source.close()
