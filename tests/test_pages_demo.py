"""Static GitHub Pages demo: synthetic TEST* sample only, no backend or network."""

from __future__ import annotations

import json
import re
import shutil
import struct
import subprocess
import sys
from pathlib import Path
from xml.etree import ElementTree

import pytest

pytestmark = pytest.mark.usefixtures("no_network")

ROOT = Path(__file__).resolve().parent.parent
SAMPLE = ROOT / "demo" / "sample"
STATIC_APP = ROOT / "src" / "openbars" / "web" / "static" / "assets" / "app.js"
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
        "data-sources/index.html",
        "assets/app.js",
        "assets/static-api.js",
        "assets/style.css",
        "assets/demo.css",
        "assets/demo-i18n.js",
        "assets/logo.svg",
        "assets/apple-touch-icon.png",
        "assets/social-preview.png",
        "assets/vendor/lightweight-charts.standalone.production.js",
        "sample/demo-data.js",
        "sample/manifest.json",
    ):
        assert (out / relative).is_file(), relative

    for relative in ("index.html", "chart/index.html", "flags/index.html", "data-sources/index.html"):
        prefix = "../" if "/" in relative else ""
        html = (out / relative).read_text(encoding="utf-8")
        assert "Demo with synthetic data" in html
        assert "合成数据演示" in (out / "assets/app.js").read_text(encoding="utf-8")
        assert SELF_HOSTED in html
        assert "window.OPENBARS_STATIC = true" in html
        assert f'src="{prefix}assets/app.js"' in html
        assert f'src="{prefix}assets/demo-i18n.js"' in html
        assert f'href="{prefix}assets/demo.css"' in html
        # The GitHub mark links to the repository; no target=_blank, so no opener leak.
        assert 'href="https://github.com/Liyuk/openbars"' in html
        assert "target=" not in html
        # Logo in the header and as the favicon, with a skip link and a labelled main landmark.
        assert f'<link rel="icon" type="image/svg+xml" href="{prefix}assets/logo.svg">' in html
        assert f'src="{prefix}assets/logo.svg"' in html
        assert 'class="dm-skip" href="#main"' in html and '<main id="main"' in html
        assert '<nav class="tabs" aria-label="Pages">' in html
        assert 'id="lang-toggle"' in html and 'aria-label="Switch language"' in html
        assert "social-preview.png" in html
        # GitHub Pages serves this from /openbars/, never from the site root; no <base> tricks.
        assert 'src="/assets/' not in html and 'href="/assets/' not in html
        assert "<base " not in html
        # The shell's design notes are not shipped to browsers.
        assert "THESIS" not in html and "<!--" not in html
        assert "OWN-WORLD" not in html and "DESIGN.md" not in html

    app = (out / "assets" / "app.js").read_text(encoding="utf-8")
    assert "OPENBARS_STATIC_API" in app
    assert "pageHref" in app
    assert 'STATIC ? "TEST1" : ""' in app
    assert "OPENBARS_EXTRA_I18N" in app


def test_demo_has_a_synthetic_only_data_sources_page(tmp_path):
    out = tmp_path / "site"
    _run_builder("--out", str(out))
    html = (out / "data-sources" / "index.html").read_text(encoding="utf-8")
    assert 'id="p-data-sources"' in html
    assert 'data-p="data-sources"' in html
    assert "DATA_SOURCES.md" in html and "DATA_SOURCES.zh-CN.md" in html
    assert "reads no provider at all" in html
    assert "Alpha Vantage and Tiingo are not implemented" in html
    # Every source on the page is one the code supports, and the adapter-only ones say so.
    content = _content()
    for source in content["sources"]:
        assert source["name"].split(" (")[0] in html, source["key"]
    assert html.count("Adapter only, not wired into the CLI") >= 3
    # The page never loads a provider or any external script/style.
    for external in ("<script src=\"http", "<link rel=\"stylesheet\" href=\"http", "fetch("):
        assert external not in html
    # The static data shim answers from memory only.
    shim = (ROOT / "demo" / "static" / "static-api.js").read_text(encoding="utf-8")
    assert "fetch(" not in shim and "XMLHttpRequest" not in shim


def _content() -> dict:
    return json.loads((ROOT / "demo" / "static" / "content.json").read_text(encoding="utf-8"))


def test_demo_copy_is_bilingual_and_every_key_resolves(tmp_path):
    content = _content()
    en, zh = content["strings"]["en"], content["strings"]["zh"]
    assert set(en) == set(zh)
    assert all(en[key].strip() and zh[key].strip() for key in en)

    app = (STATIC_APP).read_text(encoding="utf-8")
    known = {"en": set(re.findall(r'"([\w.]+)":', app)), "zh": set(re.findall(r'"([\w.]+)":', app))}
    out = tmp_path / "site"
    _run_builder("--out", str(out))
    for relative in ("index.html", "chart/index.html", "flags/index.html", "data-sources/index.html"):
        html = (out / relative).read_text(encoding="utf-8")
        for attr in ("data-i18n", "data-i18n-title"):
            for key in re.findall(rf'{attr}="([^"]+)"', html):
                assert key in en or key in known["en"], (relative, key)


def test_data_source_summary_matches_code_and_docs():
    """The demo page, DATA_SOURCES*.md, pyproject and the CLI wiring all list the same providers."""
    pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    group = pyproject.split('[project.entry-points."openbars.providers"]', 1)[1].split("[build-system]", 1)[0]
    registered = set(re.findall(r"^(\w+) = ", group, flags=re.M))
    sources = _content()["sources"]
    assert {s["provider"] for s in sources} == registered

    from openbars.cli.wiring import _CREDENTIALS

    wired = {s["provider"] for s in sources if s["status"] == "cli"}
    adapter_only = {s["provider"] for s in sources if s["status"] == "adapter"}
    assert wired == set(_CREDENTIALS)
    assert adapter_only == registered - set(_CREDENTIALS)
    assert adapter_only == {"akshare", "futu", "longport"}

    for name in ("DATA_SOURCES.md", "DATA_SOURCES.zh-CN.md"):
        text = (ROOT / name).read_text(encoding="utf-8")
        for provider in registered:
            assert f"`{provider}`" in text, (name, provider)
        # Alpha Vantage / Tiingo are named only to say they are not implemented.
        for name_ in ("Alpha Vantage", "Tiingo"):
            assert name_ in text
        assert not re.search(r"\|\s*\[?(?:Alpha Vantage|Tiingo)", text), name
        assert "freshness=provisional" in text and "final" in text
        assert "TUSHARE_TOKEN" in text and "MASSIVE_API_KEY" in text and "ALPACA_API_KEY_ID" in text
    english = (ROOT / "DATA_SOURCES.md").read_text(encoding="utf-8")
    assert "not wired into the CLI" in english.replace("**", "")
    assert "Alpha Vantage and Tiingo" in english


def test_logo_and_social_preview():
    logo_dir = ROOT / "docs" / "images" / "logo"
    logo = logo_dir / "logo.svg"
    root = ElementTree.fromstring(logo.read_text(encoding="utf-8"))
    assert root.tag.endswith("svg") and root.get("viewBox") == "0 0 128 128"
    assert not list(logo_dir.glob("candidate-*.svg"))
    assert not (logo_dir / "candidates-preview.png").exists()
    for light_or_dark in ("light", "dark"):
        png = logo_dir / "previews" / f"logo-{light_or_dark}.png"
        assert _png_size(png) == (512, 512), png.name
    assert (logo_dir / "README.md").is_file()

    assert _png_size(ROOT / "docs" / "images" / "social-preview.png") == (1280, 640)
    assert _png_size(ROOT / "demo" / "static" / "apple-touch-icon.png") == (180, 180)

    for name in ("README.md", "README.zh-CN.md"):
        head = (ROOT / name).read_text(encoding="utf-8").split("# OpenBars", 1)[0]
        assert "docs/images/logo/logo.svg" in head, name


def _png_size(path: Path) -> tuple[int, int]:
    data = path.read_bytes()
    assert data[:8] == b"\x89PNG\r\n\x1a\n", path.name
    return struct.unpack(">II", data[16:24])


def test_architecture_overview_docs_cover_the_requested_topics():
    pairs = {
        "docs/ARCHITECTURE.md": ("Positioning", "Same-day mode", "Storage layout", "Scheduling", "Adding a data source"),
        "docs/ARCHITECTURE.zh-CN.md": ("定位", "当日模式", "存储布局", "调度", "新增数据源"),
    }
    for name, headings in pairs.items():
        text = (ROOT / name).read_text(encoding="utf-8")
        assert "```mermaid" in text, name
        for heading in headings:
            assert f"## {heading}" in text, (name, heading)
        for token in ("openbars_bars", "cron", "systemd", "LaunchAgent", "provisional", "final", "raw/", "canonical/"):
            assert token in text, (name, token)
        # the overview defers to the internals file, which keeps the section numbers
        assert "architecture-internals.md" in text
    assert (ROOT / "docs" / "architecture-internals.md").is_file()
    assert "ARCHITECTURE.md" in (ROOT / "docs" / "architecture-internals.md").read_text(encoding="utf-8")


def test_new_docs_leak_no_private_paths_or_secrets():
    names = ["DATA_SOURCES.md", "DATA_SOURCES.zh-CN.md", "docs/ARCHITECTURE.md", "docs/ARCHITECTURE.zh-CN.md"]
    names += ["docs/images/logo/README.md", "demo/static/content.json", "demo/static/demo.css"]
    for name in names:
        text = (ROOT / name).read_text(encoding="utf-8")
        assert not re.search(r"(?<![\w.])/(?:Users|home)/[A-Za-z0-9._-]+", text), name
        assert not re.search(r"(?i)(api[_-]?key|token|secret)\s*[:=]\s*[\"']?[A-Za-z0-9/+_-]{16,}", text), name


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


def test_readmes_link_the_new_docs():
    expected = {
        "README.md": ("DATA_SOURCES.md", "docs/ARCHITECTURE.md"),
        "README.zh-CN.md": ("DATA_SOURCES.zh-CN.md", "docs/ARCHITECTURE.zh-CN.md"),
    }
    for name, links in expected.items():
        text = (ROOT / name).read_text(encoding="utf-8")
        for link in links:
            assert f"]({link})" in text, (name, link)
            assert (ROOT / link).is_file(), link


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
