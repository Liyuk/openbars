"""``openbars init [directory]``: write ``config.yaml`` and ``.env`` from the shipped examples.

Templates are located, in order:

1. ``openbars/_templates/`` inside the installed package (the wheel force-includes
   ``config.example.yaml`` and ``.env.example`` there, see ``pyproject.toml``);
2. the repository root, three levels above this module (``src/openbars/cli/init.py``),
   which is what a git checkout / editable install uses.

``init`` never overwrites: if *either* target already exists nothing is written.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Protocol

CONFIG_TEMPLATE = "config.example.yaml"
ENV_TEMPLATE = ".env.example"
CONFIG_TARGET = "config.yaml"
ENV_TARGET = ".env"


class _Writer(Protocol):
    def line(self, text: str = "") -> None: ...

    def err(self, text: str) -> None: ...


def template_dirs() -> list[Path]:
    package = Path(__file__).resolve().parent.parent
    return [package / "_templates", package.parent.parent]


def find_template(name: str) -> Path | None:
    for directory in template_dirs():
        candidate = directory / name
        if candidate.is_file():
            return candidate
    return None


def init_project(directory: str | os.PathLike[str], out: _Writer, *, exit_usage: int = 2) -> int:
    """Create ``<directory>/config.yaml`` and ``<directory>/.env``. Returns an exit code."""
    target = Path(directory).expanduser()
    config_src, env_src = find_template(CONFIG_TEMPLATE), find_template(ENV_TEMPLATE)
    if config_src is None or env_src is None:
        missing = [n for n, p in ((CONFIG_TEMPLATE, config_src), (ENV_TEMPLATE, env_src)) if p is None]
        out.err(f"error: cannot find template file(s): {', '.join(missing)}")
        return 1
    if target.exists() and not target.is_dir():
        out.err(f"error: {target} exists and is not a directory")
        return exit_usage
    config_dst, env_dst = target / CONFIG_TARGET, target / ENV_TARGET
    existing = [p for p in (config_dst, env_dst) if os.path.lexists(p)]
    if existing:
        names = ", ".join(str(p) for p in existing)
        out.err(f"error: {names} already exists; init never overwrites. Nothing was written.")
        return exit_usage

    target.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    try:
        for src, dst, mode in ((config_src, config_dst, 0o644), (env_src, env_dst, 0o600)):
            with open(dst, "xb") as handle:  # exclusive: never clobbers a file created meanwhile
                written.append(dst)
                handle.write(src.read_bytes())
            os.chmod(dst, mode)
    except FileExistsError as exc:
        for path in written:  # roll back a half-init
            if path != Path(exc.filename):
                path.unlink(missing_ok=True)
        out.err(f"error: {exc.filename} already exists; init never overwrites.")
        return exit_usage

    out.line(f"created {config_dst}")
    out.line(f"created {env_dst}  (git-ignored; keep it private)")
    out.line("next:")
    prefix = "" if target == Path(".") else f"cd {target} && "
    out.line("  1. edit .env and fill in your own keys (or point OPENBARS_ENV_FILE / --env-file at a dotenv you have)")
    out.line("     .env next to config.yaml is loaded automatically")
    out.line(f"  2. {prefix}openbars config   # effective settings, secrets shown as set/missing")
    out.line("  3. openbars plan              # dry run: jobs and request budget")
    out.line("  4. openbars run               # fetch; safe to re-run")
    return 0
