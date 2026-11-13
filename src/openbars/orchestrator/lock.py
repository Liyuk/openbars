"""Scheduler singleton: an exclusive, non-blocking ``flock`` on a file beside ``state.db``.

Replaces the upstream advisory lock on a database server. The OS drops the lock when the
holding process dies (including ``kill -9``), so a crashed scheduler never leaves a stale
lock behind. The lock is re-entrant per instance so a CLI command can hold it across
``plan`` + ``run`` while ``run`` takes it again.
"""

from __future__ import annotations

import os
import threading
from pathlib import Path
from types import TracebackType

from openbars.core.errors import OpenBarsError

try:  # POSIX only; on other platforms the lock degrades to in-process exclusion
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None  # type: ignore[assignment]

LOCK_FILENAME = "scheduler.lock"


class SchedulerLocked(OpenBarsError):
    """Another scheduler already holds the lock."""


def scheduler_lock_path(state_db: str | Path) -> Path:
    return Path(state_db).with_name(LOCK_FILENAME)


class SchedulerLock:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._mutex = threading.Lock()
        self._depth = 0
        self._fd: int | None = None

    @property
    def held(self) -> bool:
        return self._depth > 0

    def acquire(self) -> None:
        with self._mutex:
            if self._depth == 0:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o644)
                if fcntl is not None:
                    try:
                        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except OSError:
                        os.close(fd)
                        raise SchedulerLocked(
                            "another openbars scheduler is already running (lock held)"
                        ) from None
                self._fd = fd
            self._depth += 1

    def release(self) -> None:
        with self._mutex:
            if self._depth == 0:
                return
            self._depth -= 1
            if self._depth == 0 and self._fd is not None:
                try:
                    if fcntl is not None:
                        fcntl.flock(self._fd, fcntl.LOCK_UN)
                finally:
                    os.close(self._fd)
                    self._fd = None

    def __enter__(self) -> "SchedulerLock":
        self.acquire()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.release()
