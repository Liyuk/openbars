"""Default per-provider quota policies.

The numeric ceilings match the upstream internal implementation so that a later backend
switch stays compatible. They are *defaults*: user configuration may only tighten them
(see :func:`openbars.quota.policy.tighten`). Only the public free tier of each provider is
described; check the linked official pages for current limits.
"""

from __future__ import annotations

from datetime import date, timedelta

from openbars.quota.policy import QuotaPolicy, RateLimit

_MINUTE = timedelta(minutes=1)
_DAY = timedelta(days=1)

ALPACA = QuotaPolicy(
    provider="alpaca",
    # Market-data API: 200 requests per minute.
    limits=(RateLimit(200, _MINUTE, label="per_minute"),),
    # The multi-symbol bars endpoint paginates at 10,000 bars; 200 symbols keeps the
    # request URL short while cutting the number of year-window jobs fourfold.
    max_batch_symbols=200,
    # SIP history on the free plan starts here; nothing earlier can be requested.
    history_start=date(2016, 1, 1),
    official_docs_url="https://docs.alpaca.markets/docs/about-market-data-api",
)

MASSIVE = QuotaPolicy(
    provider="massive",
    # The free tier allows 5 calls a minute. Default to 4 -- one below -- so a burst at
    # a window boundary is never counted in two adjacent rolling windows. The margin is
    # therefore 1.0: the effective limit stays 4.
    limits=(RateLimit(4, _MINUTE, label="per_minute"),),
    safety_margin=1.0,
    # One grouped-daily call already returns the whole market for a session.
    max_batch_symbols=None,
    history_start=None,  # since listing
    official_docs_url="https://massive.com/pricing",
)

YFINANCE = QuotaPolicy(
    provider="yfinance",
    # Yahoo publishes no quota. 30 paced requests a minute (~1,800 an hour) stays below
    # the rate at which Yahoo typically starts answering HTTP 429.
    limits=(RateLimit(30, _MINUTE, label="per_minute"),),
    # No official quota page exists; this is the library's own reference.
    official_docs_url="https://ranaroussi.github.io/yfinance/",
)

TWELVE_DATA = QuotaPolicy(
    provider="twelve_data",
    # Free tier: 8 requests a minute and 800 a day (the day resets at 00:00 UTC).
    limits=(
        RateLimit(8, _MINUTE, label="per_minute"),
        RateLimit(800, _DAY, kind="utc_calendar", label="per_utc_day"),
    ),
    # One request returns up to 5,000 bars for a symbol, i.e. the whole 2016+ history.
    max_batch_symbols=1,
    max_bars_per_request=5000,
    # Kept at 2016 so windows that already ran are not requested again; another
    # source supplies earlier history.
    history_start=date(2016, 1, 1),
    official_docs_url="https://twelvedata.com/pricing",
)

ALPHA_VANTAGE = QuotaPolicy(
    provider="alpha_vantage",
    # Free key: 5 requests a minute and 25 a day (counted here as a rolling 24h).
    limits=(
        RateLimit(5, _MINUTE, label="per_minute"),
        RateLimit(25, _DAY, label="per_24h"),
    ),
    max_batch_symbols=1,
    official_docs_url="https://www.alphavantage.co/support/#api-key",
)

DEFAULT_POLICIES: dict[str, QuotaPolicy] = {
    p.provider: p for p in (ALPACA, MASSIVE, YFINANCE, TWELVE_DATA, ALPHA_VANTAGE)
}
