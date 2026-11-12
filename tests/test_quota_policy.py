from datetime import date, timedelta

import pytest

from openbars.quota.defaults import (
    ALPACA,
    ALPHA_VANTAGE,
    DEFAULT_POLICIES,
    MASSIVE,
    TWELVE_DATA,
    YFINANCE,
)
from openbars.quota.policy import BackoffPolicy, QuotaPolicy, RateLimit, tighten

MINUTE = timedelta(minutes=1)


def counts(policy):
    return {lim.name: (lim.count, policy.effective_limit(lim)) for lim in policy.limits}


def test_default_ceilings_match_the_upstream_numbers():
    assert ALPACA.limits[0].count == 200 and ALPACA.limits[0].per == MINUTE
    assert ALPACA.history_start == date(2016, 1, 1) and ALPACA.max_batch_symbols == 200
    assert MASSIVE.limits[0].count == 4 and MASSIVE.effective_limit(MASSIVE.limits[0]) == 4
    assert MASSIVE.history_start is None and MASSIVE.max_batch_symbols is None
    assert YFINANCE.limits[0].count == 30
    minute, day = TWELVE_DATA.limits
    assert (minute.count, minute.kind) == (8, "rolling")
    assert (day.count, day.per, day.kind) == (800, timedelta(days=1), "utc_calendar")
    assert TWELVE_DATA.max_bars_per_request == 5000
    assert TWELVE_DATA.history_start == date(2016, 1, 1)
    minute, rolling_day = ALPHA_VANTAGE.limits
    assert (minute.count, rolling_day.count, rolling_day.kind) == (5, 25, "rolling")
    assert set(DEFAULT_POLICIES) == {"alpaca", "massive", "yfinance", "twelve_data", "alpha_vantage"}


def test_defaults_carry_official_urls_except_where_none_is_published():
    for policy in DEFAULT_POLICIES.values():
        assert policy.official_docs_url is None or policy.official_docs_url.startswith("https://")
    assert ALPACA.official_docs_url and TWELVE_DATA.official_docs_url


def test_effective_limit_is_floor_of_margin_with_floor_of_one():
    policy = QuotaPolicy("p", (RateLimit(10, MINUTE), RateLimit(1, MINUTE, label="one")))
    first, one = policy.limits
    assert policy.effective_limit(first) == 9  # 10 * 0.9 never loses a unit to float error
    assert policy.effective_limit(one) == 1
    assert QuotaPolicy("p", (RateLimit(5, MINUTE),)).effective_limit(RateLimit(5, MINUTE)) == 4


def test_min_interval_uses_the_shortest_rolling_window():
    assert ALPHA_VANTAGE.min_interval() == MINUTE / 4
    assert MASSIVE.min_interval() == timedelta(seconds=15)
    calendar_only = QuotaPolicy("p", (RateLimit(5, timedelta(days=1), kind="utc_calendar"),))
    assert calendar_only.min_interval() == timedelta(0)


def test_rate_limit_validation_and_labels():
    with pytest.raises(ValueError):
        RateLimit(0, MINUTE)
    with pytest.raises(ValueError):
        RateLimit(1, timedelta(hours=5), kind="utc_calendar")
    assert RateLimit(1, timedelta(days=1)).name == "rolling:1d"
    assert RateLimit(1, timedelta(seconds=90)).name == "rolling:90s"
    assert RateLimit(1, MINUTE, label="x").name == "x"
    with pytest.raises(ValueError):
        QuotaPolicy("p", (RateLimit(1, MINUTE),), safety_margin=1.5)


def test_policy_is_frozen():
    with pytest.raises(AttributeError):
        MASSIVE.safety_margin = 0.5  # type: ignore[misc]
    assert BackoffPolicy().max_attempts == 5


def test_tighten_only_lowers_limits():
    tighter = tighten(TWELVE_DATA, {"per_minute": 3, "per_day": 100, "safety_margin": 0.5})
    assert counts(tighter) == {"per_minute": (3, 1), "per_utc_day": (100, 50)}
    # Raising is ignored: the result never exceeds the default.
    same = tighten(TWELVE_DATA, {"per_minute": 99, "per_day": 10_000, "safety_margin": 1.0})
    assert same.limits == TWELVE_DATA.limits and same.safety_margin == TWELVE_DATA.safety_margin
    assert TWELVE_DATA.limits[0].count == 8  # original untouched


def test_tighten_by_label_and_errors():
    assert tighten(ALPHA_VANTAGE, {"per_24h": 10}).limits[1].count == 10
    with pytest.raises(ValueError):
        tighten(ALPHA_VANTAGE, {"nope": 1})
    with pytest.raises(ValueError):
        tighten(ALPHA_VANTAGE, {"per_minute": 0})
    with pytest.raises(ValueError):
        tighten(ALPHA_VANTAGE, {"safety_margin": 0})
