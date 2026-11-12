"""Live BaoStock smoke. Skipped unless ``OPENBARS_BAOSTOCK_SMOKE=1``.

One symbol (``sh.600000``), two session days, ONE attempt, short socket timeout, no retry loop.
Needs ``openbars[baostock]``. Nothing is ingested; at most a raw archive in pytest ``tmp_path``.
If BaoStock is unreachable (common from overseas) the test skips with the error type.
"""

from __future__ import annotations

import os
from datetime import date

import pytest

from openbars.core.errors import ProviderError, QuotaExhausted
from openbars.core.models import DateRange, Interval, Market
from openbars.providers.base import FetchRequest
from openbars.providers.cn.baostock import BaoStockProvider

pytestmark = pytest.mark.skipif(
    os.environ.get("OPENBARS_BAOSTOCK_SMOKE") != "1", reason="set OPENBARS_BAOSTOCK_SMOKE=1 to hit BaoStock"
)

# 2024-01-02 and 2024-01-03 were both SSE trading days.
WINDOW = DateRange(date(2024, 1, 2), date(2024, 1, 3))
DAYS = {date(2024, 1, 2), date(2024, 1, 3)}


def test_one_attempt_two_days(tmp_path):
    provider = BaoStockProvider(timeout=10.0)
    request = FetchRequest(Market.CN, Interval.D1, ("sh.600000",), WINDOW)
    try:
        batch = provider.fetch_bars(request)  # exactly one attempt
    except (ProviderError, QuotaExhausted) as exc:
        pytest.skip(f"baostock unreachable or rejected: {type(exc).__name__}: {exc}")
    assert {b.session_date for b in batch.bars} == DAYS
    for bar in batch.bars:
        assert bar.symbol_as_returned == "sh.600000" and bar.high >= bar.low
        assert bar.extra["adjustflag"] == "3" and "tradestatus" in bar.extra
    from openbars.storage.parquet_duckdb import open_backend

    backend = open_backend(tmp_path / "data")  # raw only, in tmp_path
    assert backend.raw.append(batch) == len(batch.bars)
