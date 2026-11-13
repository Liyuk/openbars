"""Read-only HTTP skeleton for the local UI (fastapi extra)."""

from __future__ import annotations

import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient

from openbars import __version__
from openbars.api.http import create_app

pytestmark = pytest.mark.usefixtures("no_network")


def test_health_get():
    client = TestClient(create_app())
    response = client.get("/api/v1/health")
    assert response.status_code == 200
    assert response.json() == {"ok": True, "version": __version__}


@pytest.mark.parametrize("method", ["post", "put", "patch", "delete"])
def test_health_rejects_writes(method):
    client = TestClient(create_app())
    response = getattr(client, method)("/api/v1/health")
    assert response.status_code == 405


def test_docs_are_disabled():
    client = TestClient(create_app())
    assert client.get("/docs").status_code == 404
    assert client.get("/redoc").status_code == 404
    assert client.get("/openapi.json").status_code == 404


def test_index_page_is_served():
    client = TestClient(create_app())
    response = client.get("/")
    assert response.status_code == 200
    text = response.text
    # The brand is written OpenBars; only identifiers stay lowercase `openbars`.
    assert "OpenBars" in text
    assert "read-only" in text.lower() or "只读" in text
    assert "/assets/app.js" in text
