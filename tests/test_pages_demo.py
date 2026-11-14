"""Static GitHub Pages demo: synthetic TEST* sample only, no backend or network."""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.usefixtures("no_network")

ROOT = Path(__file__).resolve().parent.parent
SAMPLE = ROOT / "demo" / "sample"
BUILDER = ROOT / "scripts" / "build_pages_demo.py"
DEMO_URL = "https://liyuk.github.io/openbars/"
SELF_HOSTED = "https://github.com/Liyuk/openbars#local-service"
TEST_SYMBOL = re.compile(r"^TEST[A-Z0-9]*$")


def _sample_files() -> list[Path]:
    return sorted(SAMPLE.glob("*.json"))


def _run_builder(*args: str) -> None:
    subprocess.run(
        [sys.executable, str(BUILDER), *args],
        check=True,
        capture_output=True,
        text=True,
        cwd=ROOT,
    )


def test_sample_is_checked_in_and_only_uses_test_symbols():
    manifest = json.loads((SAMPLE / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["responses"], "the sample manifest must list the exported requests"
    for name in sorted(set(manifest["responses"].values())):
        assert (SAMPLE / name).is_file(), name
        json.loads((SAMPLE / name).read_text(encoding="utf-8"))

    # Every instrument id / symbol in the sample is a synthetic TEST* demo name.
    text = "".join(path.read_text(encoding="utf-8") for path in _sample_files())
    for symbol in ("TEST1", "TEST2", "TESTA", "TESTH", "TESTBTC"):
        assert symbol in text
    for value in set(re.findall(r'"(?:instrument_id|symbol)": "([^"]+)"', text)):
        assert TEST_SYMBOL.match(value.split(":")[-1]), value
    assert "example" in text  # fictional provider / quota snapshot
    for private in (str(ROOT), str(Path.home())):
        assert private not in text


def test_sample_matches_the_acceptance_data():
    daily = json.loads((SAMPLE / "bars-interval-1d-source-canonical-symbol-test1.json").read_text())
    assert [bar["session_date"] for bar in daily["bars"]] == [
        "2026-11-06",
        "2026-11-09",
        "2026-11-10",
        "2026-11-11",
        "2026-11-12",
    ]
    assert [bar["close"] for bar in daily["bars"]] == [
        "101.20000000",
        "102.00000000",
        "101.50000000",
        "103.85000000",
        "103.10000000",
    ]
    assert [bar["flags"] for bar in daily["bars"]] == [
        [],
        ["divergent"],
        [],
        ["spike_suspect"],
        ["volume_missing"],
    ]
    assert daily["bars"][-1]["volume"] == "0.00000000"
    assert [bar["freshness"] for bar in daily["bars"]] == [
        "final",
        "final",
        "final",
        "final",
        "provisional",
    ]
    assert daily["missing_sessions"] == []

    spaced = json.loads((SAMPLE / "bars-interval-1d-source-canonical-symbol-test2.json").read_text())
    assert [bar["session_date"] for bar in spaced["bars"]] == [
        "2026-11-06",
        "2026-11-09",
        "2026-11-11",
        "2026-11-12",
    ]
    assert spaced["missing_sessions"] == ["2026-11-10"]

    hourly = json.loads((SAMPLE / "bars-interval-1h-source-canonical-symbol-test1.json").read_text())
    assert hourly["bars"] == []
    assert hourly["note"] == "No canonical hourly bars are available."

    quota = json.loads((SAMPLE / "quota.json").read_text())
    assert quota["available"] is True
    assert [provider["provider"] for provider in quota["providers"]] == ["example"]
    assert quota["providers"][0]["used"] == {"day": 12, "minute": 3}
    assert quota["providers"][0]["remaining"] == {"day": 88, "minute": 57}


def test_static_site_builds_with_banner_and_pages(tmp_path):
    out = tmp_path / "site"
    _run_builder("--out", str(out))

    for relative in (
        "index.html",
        "chart/index.html",
        "flags/index.html",
        "assets/app.js",
        "assets/static-api.js",
        "assets/style.css",
        "assets/vendor/lightweight-charts.standalone.production.js",
        "sample/demo-data.js",
        "sample/manifest.json",
    ):
        assert (out / relative).is_file(), relative

    for relative in ("index.html", "chart/index.html", "flags/index.html"):
        html = (out / relative).read_text(encoding="utf-8")
        assert "Demo with synthetic data" in html
        assert "合成数据演示" in (out / "assets/app.js").read_text(encoding="utf-8")
        assert SELF_HOSTED in html
        assert "window.OPENBARS_STATIC = true" in html
        assert 'src="assets/app.js"' in html
        # GitHub Pages serves this from /openbars/, never from the site root.
        assert 'src="/assets/' not in html and 'href="/assets/' not in html
    assert '<base href="../">' in (out / "chart/index.html").read_text(encoding="utf-8")
    assert '<base href="../">' in (out / "flags/index.html").read_text(encoding="utf-8")
    assert "<base " not in (out / "index.html").read_text(encoding="utf-8")

    app = (out / "assets" / "app.js").read_text(encoding="utf-8")
    assert "OPENBARS_STATIC_API" in app
    assert "pageHref" in app
    assert 'STATIC ? "TEST1" : ""' in app


def test_checked_in_sample_is_reproducible(tmp_path):
    pytest.importorskip("fastapi")
    regenerated = tmp_path / "sample"
    _run_builder("--write-sample", "--sample-dir", str(regenerated))
    committed = sorted(path.name for path in _sample_files())
    assert committed == sorted(path.name for path in regenerated.glob("*.json"))
    for name in committed:
        expected = json.loads((SAMPLE / name).read_text(encoding="utf-8"))
        actual = json.loads((regenerated / name).read_text(encoding="utf-8"))
        assert actual == expected, f"{name} drifted; rerun scripts/build_pages_demo.py --write-sample"


def test_readmes_link_to_the_demo():
    for name in ("README.md", "README.zh-CN.md"):
        text = (ROOT / name).read_text(encoding="utf-8")
        assert DEMO_URL in text, name
        assert "synthetic" in text.lower() or "合成" in text, name


def test_static_api_shim_executes_in_node():
    node = shutil.which("node")
    if node is None:
        pytest.skip("node is not installed")
    subprocess.run(
        [node, "--test", str(ROOT / "tests" / "js" / "static_api.test.js")],
        check=True,
        capture_output=True,
        text=True,
        cwd=ROOT,
    )
