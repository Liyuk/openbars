"""Crash-safe file helpers shared by the storage and raw-archive layers.

* ``atomic_write``: write to a temp file in the *same directory*, fsync, then
  ``os.replace`` (or ``os.link`` for create-exclusive). A kill at any point leaves
  either the old file or the new file, never a half-written one; leftovers are
  hidden ``.<name>.<token>.tmp`` files that readers never match.
* ``cleanup_stale_tmp``: remove temp files left behind by a killed writer.
* ``ProcessLock``: re-entrant writer lock (thread lock + ``flock`` when available).
"""

from __future__ import annotations

import os
import threading
import time
import uuid
from pathlib import Path
from typing import BinaryIO, Callable

try:  # pragma: no cover - platform dependent
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None  # type: ignore[assignment]

TMP_SUFFIX = ".tmp"


def fsync_dir(directory: Path) -> None:
    """Persist a rename/link in ``directory`` (best effort, no-op where unsupported)."""
    try:
        fd = os.open(directory, os.O_RDONLY)
    except OSError:  # pragma: no cover - e.g. Windows
        return
    try:
        os.fsync(fd)
    except OSError:  # pragma: no cover
        pass
    finally:
        os.close(fd)


def tmp_path_for(path: Path) -> Path:
    return path.with_name(f".{path.name}.{uuid.uuid4().hex}{TMP_SUFFIX}")


def atomic_write(
    path: Path,
    writer: Callable[[BinaryIO], None],
    *,
    exclusive: bool = False,
) -> None:
    """Atomically materialise ``path`` with the bytes produced by ``writer``.

    ``exclusive=True`` uses ``os.link`` so an existing target is never replaced and
    ``FileExistsError`` is raised instead (the caller decides what a duplicate means).
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = tmp_path_for(path)
    try:
        with open(tmp, "wb") as handle:
            writer(handle)
            handle.flush()
            os.fsync(handle.fileno())
        if exclusive:
            try:
                os.link(tmp, path)
            except (NotImplementedError, PermissionError):  # pragma: no cover - FS without hardlinks
                if path.exists():
                    raise FileExistsError(path) from None
                os.replace(tmp, path)
        else:
            os.replace(tmp, path)
        fsync_dir(path.parent)
    finally:
        tmp.unlink(missing_ok=True)


def cleanup_stale_tmp(root: Path, *, max_age_seconds: float = 3600.0) -> list[Path]:
    """Delete ``.*.tmp`` files older than ``max_age_seconds`` under ``root``."""
    removed: list[Path] = []
    if not root.exists():
        return removed
    cutoff = time.time() - max_age_seconds
    for candidate in root.rglob(f".*{TMP_SUFFIX}"):
        try:
            if candidate.is_file() and candidate.stat().st_mtime <= cutoff:
                candidate.unlink()
                removed.append(candidate)
        except OSError:  # pragma: no cover - raced with another cleaner
            continue
    return removed


class ProcessLock:
    """Re-entrant exclusive lock: ``threading.RLock`` plus ``flock`` across processes."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._rlock = threading.RLock()
        self._depth = 0
        self._fd: int | None = None

    def __enter__(self) -> "ProcessLock":
        self._rlock.acquire()
        try:
            if self._depth == 0 and fcntl is not None:
                self._path.parent.mkdir(parents=True, exist_ok=True)
                self._fd = os.open(self._path, os.O_RDWR | os.O_CREAT, 0o644)
                fcntl.flock(self._fd, fcntl.LOCK_EX)
            self._depth += 1
        except BaseException:
            self._rlock.release()
            raise
        return self

    def __exit__(self, *exc: object) -> None:
        try:
            self._depth -= 1
            if self._depth == 0 and self._fd is not None:
                try:
                    fcntl.flock(self._fd, fcntl.LOCK_UN)  # type: ignore[union-attr]
                finally:
                    os.close(self._fd)
                    self._fd = None
        finally:
            self._rlock.release()
