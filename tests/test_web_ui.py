"""Local read-only UI pages (dashboard, chart, flags). No real market data."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient

from openbars.api.http import create_app, default_static_dir
from openbars.core.models import QualityFlag

pytestmark = pytest.mark.usefixtures("no_network")


@pytest.fixture
def client():
    return TestClient(create_app())


def test_dashboard_is_bilingual(client):
    response = client.get("/")
    assert response.status_code == 200
    text = response.text
    # The brand is prose `OpenBars`; identifiers such as `openbars.lang` stay lowercase.
    assert "OpenBars" in text
    assert "read-only" in text.lower()
    assert 'href="/chart"' in text
    assert "/assets/app.js" in text

    # The Chinese half of the UI ships in the JS bundle, not in the shell markup.
    js = client.get("/assets/app.js").text
    assert "只读" in js
    assert "观测记录" in js
    assert "Observation register" in js
    assert "openbars.lang" in js


def test_chart_and_flags_share_the_shell(client):
    for path in ("/chart", "/flags"):
        response = client.get(path)
        assert response.status_code == 200
        assert "OpenBars" in response.text
        assert "lightweight-charts.standalone.production.js" in response.text

    # The bars are also exposed as a keyboard-readable table, not only as a canvas.
    text = client.get("/chart").text
    assert 'id="bar-table-daily"' in text
    assert 'id="bar-table-hourly"' in text
    assert "<summary>" in text
    assert 'data-i18n="chart.bars_daily_caption"' in text
    assert 'role="img"' in text
    assert 'aria-label="Pages"' in text


def test_vendor_chart_library_is_pinned_and_licensed(client):
    version = client.get("/assets/vendor/VERSION")
    assert version.status_code == 200
    body = version.text
    assert "4.2.3" in body
    assert "lightweight-charts" in body

    script = client.get("/assets/vendor/lightweight-charts.standalone.production.js")
    assert script.status_code == 200
    assert "Lightweight Charts" in script.text or "lightweight-charts" in script.text
    assert "v4.2.3" in script.text

    license_text = client.get("/assets/vendor/LICENSE")
    assert license_text.status_code == 200
    assert "Apache License" in license_text.text
    static = default_static_dir()
    assert (static / "assets" / "vendor" / "LICENSE").is_file()
    assert (static / "assets" / "vendor" / "VERSION").read_text().strip().startswith("lightweight-charts 4.2.3")


def test_frontend_switches_source_and_annotates_gaps(client):
    js = client.get("/assets/app.js")
    assert js.status_code == 200
    text = js.text
    assert "source=canonical" in text or 'params.set("source"' in text
    assert "available_sources" in text
    assert "missing_sessions" in text
    assert "missing_hours" in text
    assert "missing_fields" in text
    # Lightweight Charts v4 marker API: one setMarkers() call per draw.
    assert "setMarkers" in text
    assert "addMarker(" not in text
    assert "canonical" in text
    # Gaps and quality markers share the remark ink from the stylesheet.
    assert 'getPropertyValue("--remark")' in text
    assert "--remark:" in client.get("/assets/style.css").text
    # Both languages are bundled in the page script.
    assert "观测记录" in text
    assert "Observation register" in text
    assert "缺失交易日" in text
    assert "Missing sessions" in text


def test_pages_reject_writes(client):
    for path in ("/", "/chart", "/flags"):
        assert client.post(path).status_code == 405

def test_dashboard_hides_zero_flag_counts(client):
    js = client.get("/assets/app.js").text
    # Only flags with a positive count are drawn on the dashboard summary.
    assert "counts[k] > 0" in js
    assert "if (!nonZero.length)" in js
    # A market with no flags shows a plain 0 instead of an empty cell.
    assert '<span class="zero">0</span>' in js


def test_every_quality_flag_has_en_and_zh_labels(client):
    """New QualityFlag members must ship with both language labels."""
    js = client.get("/assets/app.js").text
    names = [flag.name.lower() for flag in QualityFlag if flag is not QualityFlag.NONE]
    assert names, "expected at least one quality flag"
    missing = []
    for name in names:
        key = f"flag.{name}"
        values = re.findall(rf'"{re.escape(key)}"\s*:\s*"([^"]+)"', js)
        if len(values) != 2:
            missing.append(f"{key} (found {len(values)} labels: {values})")
            continue
        if any(not value.strip() or value.strip() == key for value in values):
            missing.append(f"{key} has an empty or fallback label: {values}")
    assert not missing, "quality flags missing bilingual labels: " + "; ".join(missing)
    # Both languages actually differ for at least the newly completed keys.
    assert "single source" in js and "单一来源" in js
    assert "limit up" in js and "涨停" in js
    # English must be readable prose, not the raw enum keys (single words may match).
    for key in names:
        if "_" not in key:
            continue
        raw = f'"flag.{key}": "{key}"'
        assert raw not in js, f"English label for flag.{key} is still the enum key"


def test_interval_radios_carry_values(client):
    html = client.get("/chart").text
    for group in ("iv1", "iv2", "iv3"):
        assert f'name="{group}" value="1d" checked' in html
        assert f'name="{group}" value="1h"' in html
    # A radio without a value reports "on", which would break the interval switching.
    assert 'name="iv2" checked' not in html.replace('name="iv2" value="1d" checked', "")

    js = client.get("/assets/app.js").text
    assert 'params.set("interval", interval)' in js
    assert 'r.value === interval' in js
    # The masthead reports the address the page was actually served from.
    assert "location.host" in js
    assert 'id="host-addr"' in html


def test_hourly_controls_and_new_york_labels(client):
    js = client.get("/assets/app.js").text
    assert 'params.set("interval", interval)' in js
    assert "America/New_York" in js
    assert "missing_hours" in js
    assert "hourly-bars-table" in client.get("/chart").text
    for key in ("interval.label", "chart.hours", "chart.no_hourly", "chart.bars_hourly_table"):
        assert len(re.findall(rf'"{re.escape(key)}":', js)) == 2


def test_chart_source_and_interval_changes_reset_dates(client):
    js = client.get("/assets/app.js").text
    assert 'chartSource.addEventListener("change", resetChartWindow)' in js
    assert 'r.addEventListener("change", resetChartWindow)' in js
    reset = js.split("function resetChartWindow()", 1)[1].split("function loadChartData(", 1)[0]
    assert 'document.getElementById("chart-start").value = ""' in reset
    assert 'document.getElementById("chart-end").value = ""' in reset
    assert "loadChartData()" in reset
    assert "chartStart.value = body.start" in js
    assert "chartEnd.value = body.end" in js


def test_chart_interactions_execute_in_node():
    import shutil
    import subprocess

    node = shutil.which("node")
    if node is None:
        pytest.skip("node is not installed")
    script = Path(__file__).parent / "js" / "chart_interactions.test.js"
    subprocess.run([node, "--test", str(script)], check=True, capture_output=True, text=True)
