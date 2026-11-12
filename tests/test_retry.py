import random
from datetime import UTC, datetime, timedelta

import pytest

from openbars.quota.policy import BackoffPolicy
from openbars.quota.retry import RetryState, compute_backoff

NOW = datetime(2026, 10, 15, 12, tzinfo=UTC)
POLICY = BackoffPolicy(base_seconds=2.0, max_seconds=30.0, max_attempts=4, circuit_break_after=6)


def test_backoff_is_exponential_and_capped():
    assert [compute_backoff(POLICY, n) for n in range(1, 7)] == [2, 4, 8, 16, 30, 30]
    assert compute_backoff(POLICY, 10_000) == 30  # no float overflow
    with pytest.raises(ValueError):
        compute_backoff(POLICY, 0)


def test_jitter_scales_down_with_injected_rng_and_never_exceeds_the_cap():
    class Fixed(random.Random):
        def random(self):
            return 0.5

    assert compute_backoff(POLICY, 3, jitter=0.5, rng=Fixed()) == 6.0  # 8 * (1 - 0.25)
    seeded = random.Random(7)
    for failures in range(1, 12):
        delay = compute_backoff(POLICY, failures, jitter=1.0, rng=seeded)
        assert 0 <= delay <= 30
    with pytest.raises(ValueError):
        compute_backoff(POLICY, 1, jitter=2)


def test_retry_state_walks_the_backoff_then_gives_up_after_max_attempts():
    state = RetryState(POLICY)
    decisions = [state.record_failure(NOW) for _ in range(4)]

    assert [d.delay_seconds for d in decisions[:3]] == [2, 4, 8]
    assert all(d.retry and d.reason == "retry" for d in decisions[:3])
    assert decisions[0].retry_at == NOW + timedelta(seconds=2)
    assert (decisions[3].retry, decisions[3].reason) == (False, "attempts_exhausted")


def test_non_retryable_and_retry_at_hint():
    state = RetryState(POLICY)
    assert state.record_failure(NOW, retryable=False).reason == "not_retryable"

    state = RetryState(POLICY)
    hint = NOW + timedelta(seconds=90)
    late = state.record_failure(NOW, retry_at=hint)
    assert (late.retry_at, late.delay_seconds) == (hint, 90.0)
    early = state.record_failure(NOW, retry_at=NOW + timedelta(seconds=1))
    assert early.delay_seconds == 4  # a hint never shortens the backoff


def test_success_resets_and_new_operation_keeps_the_circuit_count():
    state = RetryState(POLICY)
    state.record_failure(NOW)
    state.record_failure(NOW)
    state.start_operation()
    assert (state.attempts, state.consecutive_failures) == (0, 2)
    assert state.record_failure(NOW).delay_seconds == 2
    state.record_success()
    assert (state.attempts, state.consecutive_failures, state.circuit_open_until) == (0, 0, None)


def test_circuit_opens_after_consecutive_failures_and_half_opens_after_cooldown():
    state = RetryState(POLICY)
    for _ in range(5):
        state.start_operation()
        state.record_failure(NOW)
    assert state.allow(NOW)

    tripped = state.record_failure(NOW)
    assert (tripped.circuit_open, tripped.retry, tripped.reason) == (True, False, "circuit_open")
    assert tripped.retry_at == NOW + timedelta(seconds=30)
    assert not state.allow(NOW + timedelta(seconds=29))
    assert state.allow(NOW + timedelta(seconds=30))  # one probe allowed

    again = state.record_failure(NOW + timedelta(seconds=30))  # failed probe re-opens at once
    assert again.circuit_open
    assert not state.allow(NOW + timedelta(seconds=31))
