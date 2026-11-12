"""Open the storage backend described by ``Settings``."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from openbars.config.schema import Settings
from openbars.core.errors import ConfigError


def open_configured_backend(
    settings: Settings,
    *,
    data_dir: str | Path | None = None,
    write: bool | None = None,
) -> Any:
    """Return the backend selected by ``settings.storage_backend``.

    The Postgres DSN is taken from already-resolved settings (config / env /
    env file) and is never logged here. ``write`` defaults to
    ``settings.postgres_write``. ``upstream_compat`` jobs live in SQLite under
    ``{data_dir}/state.db``.
    """
    root = Path(data_dir if data_dir is not None else settings.storage_path)
    name = (settings.storage_backend or "parquet_duckdb").strip().lower()
    if name in {"parquet_duckdb", "parquet"}:
        from openbars.storage.parquet_duckdb import open_backend

        return open_backend(root)
    if name == "postgres":
        dsn = settings.postgres_dsn
        if not dsn:
            raise ConfigError(
                "storage.postgres.dsn is required when storage.backend is postgres "
                "(set OPENBARS_PG_DSN or storage.postgres.dsn at runtime)"
            )
        from openbars.storage.postgres import PostgresBackend

        write_flag = settings.postgres_write if write is None else write
        return PostgresBackend(
            dsn,
            mode=settings.postgres_mode,
            write=write_flag,
            schema=settings.postgres_schema,
            state_path=root / "state.db",
        )
    raise ConfigError(f"unknown storage.backend {settings.storage_backend!r}")
