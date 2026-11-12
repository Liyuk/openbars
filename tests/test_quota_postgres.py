"""Postgres store without a live database: a recording fake connection checks the SQL shape."""

from datetime import UTC, datetime

import pytest

from openbars.quota.postgres import PostgresQuotaStore

NOW = datetime(2026, 10, 15, 12, tzinfo=UTC)


class FakeCursor:
    def __init__(self, log):
        self.log = log

    def execute(self, sql, params=()):
        self.log.append((" ".join(sql.split()), params))

    def fetchall(self):
        return []


class FakeConnection:
    def __init__(self, log):
        self.log = log

    def cursor(self):
        return FakeCursor(self.log)

    def commit(self):
        self.log.append(("COMMIT", ()))

    def rollback(self):
        self.log.append(("ROLLBACK", ()))

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.log.append(("COMMIT" if exc[0] is None else "ROLLBACK", ()))


def make(mode):
    log = []
    return PostgresQuotaStore(lambda: FakeConnection(log), mode=mode), log


def sql_of(log):
    return [entry[0] for entry in log]


def test_native_reservation_takes_a_row_lock_and_writes_scope_and_weight():
    store, log = make("native")
    with store.transaction("p", "k") as tx:
        tx.load(NOW)
        tx.record(NOW, 3, NOW, NOW)
    statements = sql_of(log)
    lock = next(s for s in statements if "FOR UPDATE" in s)
    assert "FROM quota_gate" in lock and "scope = %s" in lock
    assert any(s.startswith("INSERT INTO quota_usage") and "weight" in s for s in statements)
    assert statements[-1] == "COMMIT"
    assert not any("provider_request" in s for s in statements)


def test_upstream_compat_uses_upstream_tables_unit_rows_and_never_migrates():
    store, log = make("upstream_compat")
    with store.transaction("p", "default") as tx:
        tx.load(NOW)
        tx.record(NOW, 3, NOW, NOW)
    statements = sql_of(log)
    assert any("FROM provider_request_gate" in s and "FOR UPDATE" in s for s in statements)
    assert sum(s.startswith("INSERT INTO provider_request_usage") for s in statements) == 3
    assert not any(s.startswith(("CREATE", "ALTER", "DROP")) for s in statements)
    assert not any("quota_gate" in s or "quota_usage" in s for s in statements)
    with pytest.raises(RuntimeError, match="never migrates"):
        store.ensure_schema()
    with pytest.raises(ValueError, match="scope"):
        store.transaction("p", "other").__enter__()


def test_upstream_compat_penalty_moves_the_gate_forward():
    store, log = make("upstream_compat")
    store.penalize("p", NOW)
    assert "GREATEST(provider_request_gate.next_request_at" in sql_of(log)[0]


def test_native_penalty_keeps_the_longest_and_ensure_schema_creates_tables():
    store, log = make("native")
    store.penalize("p", NOW)
    assert "GREATEST(quota_gate.penalized_until" in sql_of(log)[0]
    log.clear()
    store.ensure_schema()
    assert any("CREATE TABLE IF NOT EXISTS quota_gate" in s for s in sql_of(log))


def test_failed_block_rolls_back():
    store, log = make("native")
    with pytest.raises(RuntimeError):
        with store.transaction("p", "default"):
            raise RuntimeError("boom")
    assert sql_of(log)[-1] == "ROLLBACK"


def test_unknown_mode_is_rejected():
    with pytest.raises(ValueError):
        PostgresQuotaStore(lambda: None, mode="other")  # type: ignore[arg-type]
