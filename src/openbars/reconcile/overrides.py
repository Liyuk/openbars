"""Selection overrides: re-apply deterministic data-fix results on top of a projection."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Mapping

ACTION_REPLACE_BAR = "replace_bar"
ACTION_MARK_SUSPECT = "mark_suspect"
OVERRIDE_ACTIONS = (ACTION_REPLACE_BAR, ACTION_MARK_SUSPECT)


@dataclass(frozen=True)
class SelectionOverride:
    """One override for ``(instrument_id, session_date)``.

    ``replace_bar`` needs ``provider``/``feed``/``adjustment`` of the observation that
    should become the selected bar (its latest revision is used).
    """

    action: str
    provider: str | None = None
    feed: str | None = None
    adjustment: str | None = None

    def __post_init__(self) -> None:
        if self.action not in OVERRIDE_ACTIONS:
            raise ValueError(f"unknown override action: {self.action!r}")
        if self.action == ACTION_REPLACE_BAR and not (self.provider and self.feed and self.adjustment):
            raise ValueError("replace_bar needs provider, feed and adjustment")


OverrideMap = Mapping[tuple[str, date], SelectionOverride]
