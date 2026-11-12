from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

import pytest


class FakeClock:
    """A controllable UTC clock whose ``sleep`` advances time and records the delays."""

    def __init__(self, start: datetime | None = None) -> None:
        self.now = start or datetime(2026, 10, 15, 12, 0, tzinfo=UTC)
        self.sleeps: list[float] = []

    def __call__(self) -> datetime:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += timedelta(seconds=seconds)

    def advance(self, **kwargs: float) -> None:
        self.now += timedelta(**kwargs)


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "postgres_live: needs a reachable Postgres (set OPENBARS_TEST_PG_DSN); skipped otherwise",
    )


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def pg_dsn():
    dsn = os.environ.get("OPENBARS_TEST_PG_DSN")
    if not dsn:
        pytest.skip("OPENBARS_TEST_PG_DSN not set")
    pytest.importorskip("psycopg")
    return dsn
