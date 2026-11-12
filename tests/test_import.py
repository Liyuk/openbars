import importlib

import pytest

MODULES = [
    "openbars",
    "openbars.core.models",
    "openbars.core.errors",
    "openbars.core.calendar",
    "openbars.core.ohlcv",
    "openbars.core.series",
    "openbars.core.schema",
    "openbars.calendars",
    "openbars.calendars.xnys",
    "openbars.calendars.crypto",
    "openbars.config.schema",
    "openbars.config.loader",
    "openbars.config.display",
    "openbars.cli.main",
    "openbars.orchestrator.jobs",
    "openbars.orchestrator.planner",
    "openbars.orchestrator.nodes",
    "openbars.orchestrator.runner",
    "openbars.orchestrator.lock",
    "openbars.orchestrator.finalize",
    "openbars.orchestrator.verify",
    "openbars.cli.wiring",
    "openbars.quota.policy",
    "openbars.quota.gate",
    "openbars.quota.defaults",
    "openbars.quota.pacer",
    "openbars.quota.persistent",
    "openbars.quota.postgres",
    "openbars.quota.retry",
    "openbars.quota.store",
    "openbars.providers.base",
    "openbars.providers.registry",
    "openbars.providers.http",
    "openbars.providers.adapter",
    "openbars.providers.us",
    "openbars.providers.us.alpaca",
    "openbars.providers.us.alpaca_time",
    "openbars.providers.us.massive",
    "openbars.providers.us.massive_window",
    "openbars.providers.us.yfinance",
    "openbars.providers.crypto",
    "openbars.providers.crypto.binance",
    "openbars.raw.store",
    "openbars.normalize.instruments",
    "openbars.normalize.normalizer",
    "openbars.normalize.adjust",
    "openbars.reconcile.policy",
    "openbars.reconcile.reconciler",
    "openbars.reconcile.compatibility",
    "openbars.reconcile.projection",
    "openbars.reconcile.overrides",
    "openbars.reconcile.rules",
    "openbars.reconcile.gaps",
    "openbars.reconcile.audit",
    "openbars.reconcile.store",
    "openbars.storage.base",
    "openbars.storage.parquet_duckdb",
    "openbars.storage.postgres",
    "openbars.storage.schema",
    "openbars.storage.rows",
    "openbars.storage.state",
    "openbars.storage.state_schema",
    "openbars.storage.errors",
    "openbars.raw.archive",
    "openbars.core.atomic",
    "openbars.api.reader",
    "openbars.api.http",
]


@pytest.mark.parametrize("name", MODULES)
def test_module_imports(name):
    importlib.import_module(name)


def test_version_and_cli():
    import openbars
    from openbars.cli.main import main

    assert openbars.__version__
    assert main([]) == 0


def test_interval_semantics():
    from openbars.core.models import Interval

    assert not Interval.D1.is_intraday
    assert Interval.H1.is_intraday
