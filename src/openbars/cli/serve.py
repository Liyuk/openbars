"""``openbars serve``: local read-only UI and the self-hosted ``/api/v1`` read API.

The default bind is loopback only (``127.0.0.1:8787``). A non-loopback ``--host``
is refused unless ``--allow-non-loopback`` is passed explicitly; the service has
no built-in authentication, so exposing it is the caller's responsibility. The
JSON routes are the public read API documented in ``docs/api.md``; the bundled UI
is one client of them. This module must not import FastAPI at import time so
``openbars serve --help`` works without the ``http`` extra.
"""

from __future__ import annotations

import ipaddress
from argparse import ArgumentParser
from collections.abc import Callable
from typing import Any

from openbars.config.schema import Settings

EXIT_OK, EXIT_USAGE, EXIT_ERROR = 0, 2, 1
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8787
ALLOW_NON_LOOPBACK_FLAG = "--allow-non-loopback"
_LOOPBACK_NAMES = frozenset({"localhost", "localhost.localdomain"})

# Install hint kept in one place so the CLI and tests stay aligned.
HTTP_EXTRA_HINT = "uv sync --extra http"


def is_loopback_host(host: str) -> bool:
    """Return True if ``host`` is a loopback address or the name ``localhost``."""
    text = host.strip()
    if text.startswith("[") and text.endswith("]"):
        text = text[1:-1]
    if not text:
        return False
    if text.lower() in _LOOPBACK_NAMES:
        return True
    try:
        ip = ipaddress.ip_address(text)
    except ValueError:
        return False
    if ip.is_loopback:
        return True
    mapped = getattr(ip, "ipv4_mapped", None)
    return mapped is not None and mapped.is_loopback


def check_bind_host(host: str, *, allow_non_loopback: bool = False) -> str | None:
    """Return an error message if ``host`` is non-loopback and not opted in, else None."""
    if is_loopback_host(host) or allow_non_loopback:
        return None
    return (
        f"--host {host!r} is not a loopback address; "
        f"pass {ALLOW_NON_LOOPBACK_FLAG} to bind it (no built-in authentication)"
    )


def register(sub: Any, common: Callable[[ArgumentParser], None]) -> None:
    """Add the ``serve`` subcommand. Safe to call without the ``http`` extra."""
    serve = sub.add_parser(
        "serve",
        help="read-only local UI plus the self-hosted /api/v1 API (loopback by default)",
        description=(
            "Start the read-only local UI and the self-hosted read API under /api/v1 "
            "(endpoints and stability rules: docs/api.md). OpenBars provides no hosted "
            "service: whoever downloads it deploys and owns it, including private "
            f"deployments. Binds loopback ({DEFAULT_HOST}:{DEFAULT_PORT}) by default; a "
            f"non-loopback --host requires {ALLOW_NON_LOOPBACK_FLAG}. The server has no "
            "built-in authentication, so exposing it beyond this machine (firewall, "
            "authenticating reverse proxy) is the caller's responsibility."
        ),
    )
    common(serve)
    serve.add_argument(
        "--host",
        default=DEFAULT_HOST,
        help=(
            f"bind address (default {DEFAULT_HOST}); a non-loopback host requires "
            f"{ALLOW_NON_LOOPBACK_FLAG}"
        ),
    )
    serve.add_argument(
        "--port",
        type=int,
        default=DEFAULT_PORT,
        help=f"port (default {DEFAULT_PORT})",
    )
    serve.add_argument(
        ALLOW_NON_LOOPBACK_FLAG,
        action="store_true",
        help=(
            "allow binding a non-loopback --host; the service has no built-in "
            "authentication, so add a firewall or an authenticating reverse proxy"
        ),
    )


def run_serve(
    args: Any,
    settings: Settings,
    out: Any,
    *,
    server: Callable[..., None] | None = None,
) -> int:
    """Validate the bind address, build the app, and start the server.

    ``server`` is an injection point for tests (replaces ``uvicorn.run``).
    Storage is opened read-only; this command never plans, fetches, or writes.
    """
    host = getattr(args, "host", DEFAULT_HOST)
    allow_non_loopback = bool(getattr(args, "allow_non_loopback", False))
    message = check_bind_host(host, allow_non_loopback=allow_non_loopback)
    if message:
        out.err(f"error: {message}")
        return EXIT_USAGE
    port = int(getattr(args, "port", DEFAULT_PORT))
    if port < 1 or port > 65535:
        out.err(f"error: invalid --port {port}")
        return EXIT_USAGE

    try:
        from openbars.api.http import create_app
    except ImportError as exc:
        name = exc.name or "fastapi"
        out.err(f"error: missing optional dependency ({name}); install with: {HTTP_EXTRA_HINT}")
        return EXIT_USAGE

    from openbars.cli.wiring import data_dir
    from openbars.core.errors import ConfigError
    from openbars.storage.errors import StorageError
    from openbars.storage.factory import open_configured_backend

    root = data_dir(settings, getattr(args, "data", None))
    try:
        backend = open_configured_backend(settings, data_dir=root, write=False)
    except (ConfigError, StorageError) as exc:
        out.err(f"error: {exc}")
        return EXIT_ERROR if isinstance(exc, StorageError) else EXIT_USAGE

    from openbars.api.queries import open_readonly_quota_gate

    gate = open_readonly_quota_gate(settings, root)
    app = create_app(settings=settings, backend=backend, data_dir=root, quota_gate=gate)
    out.line("openbars serve: read-only local UI and self-hosted API (/api/v1, see docs/api.md)")
    if is_loopback_host(host):
        out.line(f"listening on http://{host}:{port}  (loopback only; no fetch, no writes)")
    else:
        out.line(f"listening on http://{host}:{port}  (non-loopback; no fetch, no writes)")
        out.err(
            "WARNING: openbars serve is bound to a non-loopback address "
            f"({host}:{port}); it has NO built-in authentication and NO built-in TLS. "
            "Anyone who can reach this address can read your stored bars, instruments and "
            "quota usage. Add a firewall or an authenticating reverse proxy; "
            f"{ALLOW_NON_LOOPBACK_FLAG} only lifts the loopback restriction."
        )
    run = server
    if run is None:
        try:
            import uvicorn
        except ImportError as exc:
            name = exc.name or "uvicorn"
            out.err(f"error: missing optional dependency ({name}); install with: {HTTP_EXTRA_HINT}")
            return EXIT_USAGE
        run = uvicorn.run
    run(app, host=host, port=port, log_level="info")
    return EXIT_OK
