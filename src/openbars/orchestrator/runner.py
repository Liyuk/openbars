"""Orchestrator contract."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Protocol

from openbars.orchestrator.jobs import Job


@dataclass(frozen=True)
class RunReport:
    done: int
    failed: int
    dead: int
    requests: int


class Orchestrator(Protocol):
    def submit(self, jobs: Iterable[Job]) -> None: ...

    def run(self, *, until_idle: bool = True) -> RunReport: ...
