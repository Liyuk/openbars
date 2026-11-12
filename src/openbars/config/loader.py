"""Config loading (not implemented yet)."""

from __future__ import annotations

from pathlib import Path

from openbars.config.schema import Settings


def load_settings(path: str | Path) -> Settings:
    """Parse config.yaml, expand ${ENV_VAR} references and validate. TODO(v0.1)."""
    raise NotImplementedError
