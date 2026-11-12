"""Collector nodes: local (in-process) or remote (HTTP)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from openbars.core.models import Market
from openbars.orchestrator.jobs import Job


@dataclass(frozen=True)
class JobResult:
    job_id: str
    ok: bool
    bars_written: int = 0
    requests_made: int = 0
    error: str | None = None


class CollectorNode(Protocol):
    name: str

    def markets(self) -> set[Market]: ...

    def execute(self, job: Job) -> JobResult: ...
