"""Planner: expected grid minus coverage = jobs."""

from __future__ import annotations

from typing import Iterable, Protocol

from openbars.config.schema import Selection
from openbars.orchestrator.jobs import Job


class CoverageIndex(Protocol):
    def missing(self, selection: Selection) -> Iterable[object]: ...


class Planner(Protocol):
    def plan(self, selection: Selection, coverage: CoverageIndex) -> Iterable[Job]: ...
