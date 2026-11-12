"""Single-source normalization contract."""

from __future__ import annotations

from typing import Iterable, Protocol

from openbars.core.models import Bar, RawBar


class Normalizer(Protocol):
    def normalize(self, raw: Iterable[RawBar]) -> Iterable[Bar]: ...
