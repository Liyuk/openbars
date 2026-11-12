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
    assert "openbars" in text
    assert "read-only" in text.lower()
    assert "只读" in text
    assert 'href="/chart"' in text
    assert "/assets/app.js" in text


def test_chart_and_flags_share_the_shell(client):
    for path in ("/chart", "/flags"):
        response = client.get(path)
        assert response.status_code == 200
        assert "openbars" in response.text
        assert "lightweight-charts.standalone.production.js" in response.text


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
    assert "missing_fields" in text
    assert "setMarkers" in text
    assert "canonical" in text
    # Both languages are bundled in the page script.
    assert "覆盖看板" in text
    assert "Coverage dashboard" in text
    assert "缺失交易日" in text
    assert "Missing sessions" in text


def test_pages_reject_writes(client):
    for path in ("/", "/chart", "/flags"):
        assert client.post(path).status_code == 405

def test_dashboard_hides_zero_flag_counts(client):
    js = client.get("/assets/app.js").text
    # Only flags with a positive count are drawn on the dashboard summary.
    assert "counts[k] > 0" in js
    assert "dash.flags_empty" in js


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


def test_hourly_controls_and_new_york_labels(client):
    js = client.get("/assets/app.js").text
    assert 'value="1h"' in js
    assert 'params.set("interval", interval)' in js
    assert 'America/New_York' in js
    assert 'missing_hours' in js
    assert 'chart.hourly_bars' in js
    for key in ("interval.label", "chart.hours", "chart.no_hourly", "chart.hourly_bars"):
        assert len(re.findall(rf'"{re.escape(key)}"\s*:', js)) == 2
