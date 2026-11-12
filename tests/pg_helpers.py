"""Helpers for ``postgres_live`` tests: a throwaway schema per test, dropped afterwards."""

from __future__ import annotations

import uuid
from contextlib import contextmanager


@contextmanager
def throwaway_schema(dsn: str):
    """Create ``openbars_test_<random>`` and drop it (CASCADE) on exit; yields its name."""
    import psycopg

    schema = f"openbars_test_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    try:
        yield schema
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(f'DROP SCHEMA "{schema}" CASCADE')
