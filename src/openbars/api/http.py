"""Optional HTTP read API (fastapi extra).

Routes under ``/api/v1`` exist only for the local ``openbars serve`` UI.
They are not a stable public API and must stay read-only.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from openbars import __version__


def default_static_dir() -> Path:
    return Path(__file__).resolve().parent.parent / "web" / "static"


def create_app(
    *,
    settings: Any | None = None,
    backend: Any | None = None,
    data_dir: Path | str | None = None,
    static_dir: Path | str | None = None,
) -> Any:
    """Build the read-only FastAPI app. Requires the ``http`` extra."""
    try:
        from fastapi import FastAPI
        from fastapi.responses import FileResponse
        from fastapi.staticfiles import StaticFiles
    except ImportError as exc:
        raise ImportError(
            "missing optional dependency (fastapi); install with: uv sync --extra http"
        ) from exc

    app = FastAPI(
        title="openbars",
        version=__version__,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.state.settings = settings
    app.state.backend = backend
    app.state.data_dir = Path(data_dir) if data_dir is not None else None

    @app.get("/api/v1/health")
    def health() -> dict[str, Any]:
        return {"ok": True, "version": __version__}

    root = Path(static_dir) if static_dir is not None else default_static_dir()
    if root.is_dir():
        index = root / "index.html"

        @app.get("/")
        def index_page() -> Any:
            if index.is_file():
                return FileResponse(index)
            return {"ok": True, "version": __version__}

        assets = root / "assets"
        if assets.is_dir():
            app.mount("/assets", StaticFiles(directory=assets), name="assets")

    return app
