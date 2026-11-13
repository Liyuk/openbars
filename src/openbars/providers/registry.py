"""Provider discovery via the `openbars.providers` entry point group."""

from __future__ import annotations

from importlib.metadata import entry_points

ENTRY_POINT_GROUP = "openbars.providers"


def discover() -> dict[str, object]:
    """Return {name: provider_class} for every installed provider plugin."""
    return {ep.name: ep.load() for ep in entry_points(group=ENTRY_POINT_GROUP)}
