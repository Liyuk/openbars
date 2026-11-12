"""Optional HTTP read API (fastapi extra).

Routes under ``/api/v1`` exist only for the local ``openbars serve`` UI.
They are not a stable public API and must stay read-only.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from openbars import __version__
from openbars.api.queries import (
    DEFAULT_FLAG_LIMIT,
    DEFAULT_INSTRUMENT_LIMIT,
    MAX_FLAG_LIMIT,
    MAX_INSTRUMENT_LIMIT,
    QueryError,
    list_bars,
    list_flags,
    list_instruments,
    list_markets,
    parse_adjustment,
    parse_date,
    parse_interval,
    parse_market,
    quota_snapshot,
)


def default_static_dir() -> Path:
    return Path(__file__).resolve().parent.parent / "web" / "static"


def _http_error(status: int, detail: str) -> Any:
    from fastapi import HTTPException

    return HTTPException(status_code=status, detail=detail)


def create_app(
    *,
    settings: Any | None = None,
    backend: Any | None = None,
    data_dir: Path | str | None = None,
    static_dir: Path | str | None = None,
    quota_gate: Any | None = None,
) -> Any:
    """Build the read-only FastAPI app. Requires the ``http`` extra."""
    try:
        from fastapi import FastAPI
        from fastapi.responses import FileResponse, JSONResponse
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
    app.state.quota_gate = quota_gate

    # Close over these so route signatures do not need FastAPI's Request type
    # (imported only inside this function; string annotations would otherwise
    # treat ``request`` as a required query parameter).
    store = backend
    gate = quota_gate

    def _store() -> Any:
        if store is None:
            raise _http_error(503, "no storage backend")
        return store

    @app.exception_handler(QueryError)
    def _query_error(_request: Any, exc: QueryError) -> JSONResponse:
        return JSONResponse({"detail": str(exc)}, status_code=400)

    @app.get("/api/v1/health")
    def health() -> dict[str, Any]:
        return {"ok": True, "version": __version__}

    @app.get("/api/v1/markets")
    def markets() -> dict[str, Any]:
        return {"markets": list_markets(_store())}

    @app.get("/api/v1/instruments")
    def instruments(
        market: str | None = None,
        q: str | None = None,
        limit: int = DEFAULT_INSTRUMENT_LIMIT,
        offset: int = 0,
    ) -> dict[str, Any]:
        if limit < 1 or limit > MAX_INSTRUMENT_LIMIT:
            raise _http_error(400, f"limit must be between 1 and {MAX_INSTRUMENT_LIMIT}")
        if offset < 0:
            raise _http_error(400, "offset must be >= 0")
        return list_instruments(
            _store(),
            market=parse_market(market),
            q=q,
            limit=limit,
            offset=offset,
        )

    @app.get("/api/v1/bars")
    def bars(
        id: str | None = None,
        symbol: str | None = None,
        market: str | None = None,
        interval: str | None = None,
        start: str | None = None,
        end: str | None = None,
        adjustment: str | None = None,
        as_of: str | None = None,
        source: str | None = None,
    ) -> dict[str, Any]:
        return list_bars(
            _store(),
            instrument_id=id,
            symbol=symbol,
            market=parse_market(market),
            interval=parse_interval(interval),
            start=parse_date(start, "start"),
            end=parse_date(end, "end"),
            adjustment=parse_adjustment(adjustment),
            as_of=parse_date(as_of, "as_of"),
            source=source,
        )

    @app.get("/api/v1/flags")
    def flags(
        market: str | None = None,
        flag: str | None = None,
        start: str | None = None,
        end: str | None = None,
        limit: int = DEFAULT_FLAG_LIMIT,
        offset: int = 0,
    ) -> dict[str, Any]:
        if limit < 1 or limit > MAX_FLAG_LIMIT:
            raise _http_error(400, f"limit must be between 1 and {MAX_FLAG_LIMIT}")
        if offset < 0:
            raise _http_error(400, "offset must be >= 0")
        return list_flags(
            _store(),
            market=parse_market(market),
            flag=flag,
            start=parse_date(start, "start"),
            end=parse_date(end, "end"),
            limit=limit,
            offset=offset,
        )

    @app.get("/api/v1/quota")
    def quota() -> dict[str, Any]:
        return quota_snapshot(gate)

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
