"""Local read-only UI pages (dashboard, chart, flags). No real market data."""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient

from openbars.api.http import create_app, default_static_dir

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
