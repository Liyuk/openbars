import sqlite3
from datetime import UTC, datetime, timedelta

import pytest

from openbars.core.errors import ConfigError, QuotaExhausted
from openbars.quota.persistent import SqliteQuotaGate, plan_reservation
from openbars.quota.policy import QuotaPolicy, RateLimit
from openbars.quota.store import GateState, UsageRow

MINUTE = timedelta(minutes=1)
DAY = timedelta(days=1)


def policy(name="p", *limits, margin=1.0):
    return QuotaPolicy(name, limits, safety_margin=margin)


def make_gate(tmp_path, clock, *policies):
    return SqliteQuotaGate(
        tmp_path / "state.db", {p.provider: p for p in policies}, clock=clock, sleep=clock.sleep
    )


def test_fixed_cadence_paces_instead_of_bursting(tmp_path, clock):
    gate = make_gate(tmp_path, clock, policy("p", RateLimit(5, MINUTE)))
    start = clock.now

    leases = [gate.acquire("p") for _ in range(3)]

    assert clock.sleeps == [12.0, 12.0]
    assert [lease.granted_at for lease in leases] == [start + timedelta(seconds=12 * i) for i in range(3)]


def test_multi_window_weights_consume_every_window(tmp_path, clock):
    day = RateLimit(25, DAY, label="day")
    gate = make_gate(tmp_path, clock, policy("p", RateLimit(5, MINUTE, label="min"), day))

    lease = gate.acquire("p", weight=3)
    usage = gate.snapshot()["p"]

    assert (lease.weight, lease.granted_at) == (3, clock.now)
    assert usage.used == {"min": 3, "day": 3}
    assert usage.remaining == {"min": 2, "day": 22}


def test_weight_larger_than_a_window_is_a_caller_error(tmp_path, clock):
    gate = make_gate(tmp_path, clock, policy("p", RateLimit(5, MINUTE)))
    with pytest.raises(ValueError, match="can never fit"):
        gate.acquire("p", weight=6)
    with pytest.raises(ValueError):
        gate.acquire("p", weight=0)
    with pytest.raises(ValueError):
        gate.acquire("p", scope="*")


def test_rolling_minute_window_blocks_until_oldest_ages_out(tmp_path, clock):
    # No spacing (huge shortest window is not rolling-shorter), so exhaustion is visible.
    gate = make_gate(
        tmp_path,
        clock,
        policy("p", RateLimit(1000, timedelta(seconds=1), label="fast"), RateLimit(3, MINUTE, label="min")),
    )
    start = clock.now
    gate.acquire("p", weight=2)
    gate.acquire("p", weight=1)

    with pytest.raises(QuotaExhausted) as raised:
        gate.acquire("p", timeout=0)
    assert raised.value.retry_at == start + MINUTE  # the weight-2 reservation frees two units

    gate.acquire("p", timeout=None)  # waits instead of raising
    assert clock.now >= start + MINUTE


def test_safety_margin_is_honored(tmp_path, clock):
    gate = make_gate(
        tmp_path,
        clock,
        QuotaPolicy("p", (RateLimit(10, DAY, kind="utc_calendar", label="day"),), safety_margin=0.5),
    )
    for _ in range(5):
        gate.acquire("p")
    with pytest.raises(QuotaExhausted):
        gate.acquire("p", timeout=0)
    assert gate.snapshot()["p"].remaining == {"day": 0}


def test_utc_day_quota_is_shared_across_instances_and_resets_at_midnight(tmp_path, clock):
    clock.now = datetime(2026, 10, 15, 23, 59, tzinfo=UTC)
    pol = policy("p", RateLimit(60, MINUTE, label="min"), RateLimit(2, DAY, kind="utc_calendar", label="day"))
    first, second = make_gate(tmp_path, clock, pol), make_gate(tmp_path, clock, pol)

    first.acquire("p")
    second.acquire("p")
    with pytest.raises(QuotaExhausted) as raised:
        first.acquire("p", timeout=0)
    assert raised.value.retry_at == datetime(2026, 10, 16, tzinfo=UTC)
    assert raised.value.provider == "p"

    clock.now = datetime(2026, 10, 16, tzinfo=UTC)
    assert second.acquire("p", timeout=0).granted_at == clock.now
    assert second.snapshot()["p"].used["day"] == 1


def test_rolling_24h_quota_returns_the_earliest_eligible_retry_time(tmp_path, clock):
    pol = policy("p", RateLimit(60, MINUTE, label="min"), RateLimit(2, DAY, label="24h"))
    gate = make_gate(tmp_path, clock, pol)
    noon = clock.now
    gate.acquire("p")
    gate.acquire("p")

    with pytest.raises(QuotaExhausted) as raised:
        gate.acquire("p", timeout=0)
    assert raised.value.retry_at == noon + DAY

    clock.now = noon + DAY
    assert gate.acquire("p", timeout=0).granted_at == noon + DAY


def test_timeout_bounds_spacing_waits_and_a_refusal_reserves_nothing(tmp_path, clock):
    gate = make_gate(tmp_path, clock, policy("p", RateLimit(5, MINUTE, label="min")))
    start = clock.now
    gate.acquire("p")

    with pytest.raises(QuotaExhausted) as raised:
        gate.acquire("p", timeout=5)
    assert raised.value.retry_at == start + timedelta(seconds=12)
    assert gate.snapshot()["p"].used == {"min": 1}

    assert gate.acquire("p", timeout=12).granted_at == start + timedelta(seconds=12)
    with pytest.raises(QuotaExhausted):
        gate.acquire("p", timeout=0)


def test_reopening_the_file_keeps_usage_spacing_and_penalty(tmp_path, clock):
    pol = policy("p", RateLimit(5, MINUTE, label="min"), RateLimit(25, DAY, label="day"))
    first = make_gate(tmp_path, clock, pol)
    start = clock.now
    first.acquire("p", weight=2)
    first.penalize("p", start + timedelta(minutes=10))
    first.close()  # the process "dies"

    second = make_gate(tmp_path, clock, pol)
    usage = second.snapshot()["p"]
    assert usage.used == {"min": 2, "day": 2}
    assert usage.penalized_until == start + timedelta(minutes=10)
    with pytest.raises(QuotaExhausted) as raised:
        second.acquire("p", timeout=0)
    assert raised.value.retry_at == start + timedelta(minutes=10)

    # Without the penalty the persisted next_request_at still paces the new process.
    other = make_gate(tmp_path, clock, pol)
    clock.now = start + timedelta(minutes=11)
    other.store.penalize("p", start)  # no-op: only extends
    assert other.acquire("p", timeout=0).granted_at == clock.now


def test_next_request_at_survives_restart(tmp_path, clock):
    pol = policy("p", RateLimit(5, MINUTE))
    start = clock.now
    make_gate(tmp_path, clock, pol).acquire("p", weight=2)  # next slot = start + 24s
    reopened = make_gate(tmp_path, clock, pol)
    with pytest.raises(QuotaExhausted) as raised:
        reopened.acquire("p", timeout=0)
    assert raised.value.retry_at == start + timedelta(seconds=24)


def test_penalize_blocks_grants_until_the_instant(tmp_path, clock):
    gate = make_gate(tmp_path, clock, policy("p", RateLimit(60, MINUTE)), policy("q", RateLimit(60, MINUTE)))
    until = clock.now + timedelta(seconds=300)
    gate.penalize("p", until)

    with pytest.raises(QuotaExhausted) as raised:
        gate.acquire("p", timeout=0)
    assert raised.value.retry_at == until
    with pytest.raises(QuotaExhausted):
        gate.acquire("p", scope="other-key", timeout=0)  # provider-wide
    assert gate.acquire("q", timeout=0)  # other providers unaffected

    assert gate.snapshot()["p"].penalized_until == until
    assert gate.acquire("p").granted_at == until  # waited for the penalty to lapse
    assert clock.sleeps[-1] == 300.0
    assert gate.snapshot()["p"].penalized_until is None


def test_penalty_only_extends_and_can_target_one_scope(tmp_path, clock):
    gate = make_gate(tmp_path, clock, policy("p", RateLimit(60, MINUTE)))
    long_until = clock.now + timedelta(minutes=10)
    gate.penalize("p", long_until)
    gate.penalize("p", clock.now + timedelta(minutes=1))
    with pytest.raises(QuotaExhausted) as raised:
        gate.acquire("p", timeout=0)
    assert raised.value.retry_at == long_until

    scoped = make_gate(tmp_path, clock, policy("s", RateLimit(60, MINUTE)))
    scoped.penalize("s", clock.now + timedelta(minutes=1), scope="key-a")
    with pytest.raises(QuotaExhausted):
        scoped.acquire("s", scope="key-a", timeout=0)
    assert scoped.acquire("s", scope="key-b", timeout=0)


def test_scopes_have_independent_cadence_and_snapshot_keys(tmp_path, clock):
    gate = make_gate(tmp_path, clock, policy("p", RateLimit(5, MINUTE, label="min")))
    gate.acquire("p")
    assert gate.acquire("p", scope="other", timeout=0).granted_at == clock.now
    snap = gate.snapshot()
    assert set(snap) == {"p", "p:other"}
    assert snap["p:other"].provider == "p"


def test_unknown_provider_is_a_config_error(tmp_path, clock):
    gate = make_gate(tmp_path, clock, policy("p", RateLimit(5, MINUTE)))
    with pytest.raises(ConfigError):
        gate.acquire("nope")
    with pytest.raises(ConfigError):
        gate.penalize("nope", clock.now)


def test_default_policies_are_used_when_none_given(tmp_path, clock):
    gate = SqliteQuotaGate(tmp_path / "s.db", clock=clock, sleep=clock.sleep)
    assert "massive" in gate.snapshot()
    gate.acquire("massive")
    assert gate.snapshot()["massive"].used == {"per_minute": 1}
    assert gate.snapshot()["massive"].remaining == {"per_minute": 3}
    assert set(gate.snapshot()["twelve_data"].used) == {"per_minute", "per_utc_day"}


def test_sqlite_file_uses_wal_and_private_table_names(tmp_path, clock):
    gate = make_gate(tmp_path, clock, policy("p", RateLimit(5, MINUTE)))
    gate.acquire("p")
    conn = sqlite3.connect(tmp_path / "state.db")
    assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"quota_gate", "quota_usage"} <= tables
    assert not tables & {"provider_request_gate", "provider_request_usage"}
    conn.close()


def test_usage_older_than_every_window_is_pruned(tmp_path, clock):
    gate = make_gate(tmp_path, clock, policy("p", RateLimit(5, MINUTE)))
    gate.acquire("p")
    clock.advance(hours=1)
    gate.acquire("p")
    count = gate.store._conn.execute("SELECT COUNT(*) FROM quota_usage").fetchone()[0]
    assert count == 1


def test_plan_reservation_month_window_rolls_over_the_year():
    pol = policy("p", RateLimit(2, timedelta(days=30), kind="utc_calendar", label="month"))
    now = datetime(2026, 12, 20, tzinfo=UTC)
    rows = [UsageRow(datetime(2026, 12, 1, tzinfo=UTC), 2)]
    decision = plan_reservation(pol, now, GateState(), rows, 1)
    assert decision.retry_at == datetime(2027, 1, 1, tzinfo=UTC)
    # Usage from the previous month does not count.
    old = [UsageRow(datetime(2026, 11, 30, 23, tzinfo=UTC), 2)]
    assert plan_reservation(pol, now, GateState(), old, 1).slot == now
