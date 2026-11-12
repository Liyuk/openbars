"""#157: the wheel must not add a file twice (non-editable installs failed on web/static)."""

from __future__ import annotations

import shutil
import subprocess
import tomllib
import zipfile
from collections import Counter
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
REQUIRED = (
    "openbars/web/static/index.html",
    "openbars/_templates/config.example.yaml",
    "openbars/_templates/.env.example",
)


def _wheel_config() -> dict:
    return tomllib.loads((ROOT / "pyproject.toml").read_text())["tool"]["hatch"]["build"]["targets"]["wheel"]


def test_force_include_does_not_repeat_packaged_sources():
    config = _wheel_config()
    packages = [Path(p) for p in config["packages"]]
    for source in config.get("force-include", {}):
        src = Path(source)
        assert not any(src == p or p in src.parents for p in packages), f"{source} is already in packages"


def test_wheel_builds_without_duplicate_entries(tmp_path):
    if shutil.which("uv") is None:
        pytest.skip("uv not on PATH")
    proc = subprocess.run(
        ["uv", "build", "--wheel", "--offline", "-o", str(tmp_path), str(ROOT)],
        capture_output=True, text=True, timeout=300, check=False,
    )  # fmt: skip
    output = proc.stdout + proc.stderr
    assert "second file is being added" not in output
    if proc.returncode != 0:
        pytest.skip("wheel build unavailable offline in this environment")
    (wheel,) = tmp_path.glob("openbars-*.whl")
    names = zipfile.ZipFile(wheel).namelist()
    duplicates = [name for name, count in Counter(names).items() if count > 1]
    assert duplicates == []
    assert set(REQUIRED) <= set(names)
