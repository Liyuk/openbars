"""Live public-endpoint smoke for the crypto providers. Skipped unless ``OPENBARS_CRYPTO_SMOKE=1``.

One request per venue (Binance ``BTCUSDT`` via ``data-api.binance.vision``, Coinbase ``BTC-USD`` via
``api.exchange.coinbase.com``), two fixed UTC days. No keys, never ``api.binance.com``. Raw rows go
only to pytest ``tmp_path``; no pair ingest, nothing under the repo ``data/``.
"""

from __future__ import annotations

import os
from datetime import date

import pytest

from openbars.core.errors import ProviderError
from openbars.core.models import DateRange, Interval, Market
from openbars.providers.base import FetchRequest
from openbars.providers.crypto.binance import DEFAULT_BASE_URL as BINANCE_URL
from openbars.providers.crypto.binance import BinanceProvider
from openbars.providers.crypto.coinbase import DEFAULT_BASE_URL as COINBASE_URL
from openbars.providers.crypto.coinbase import CoinbaseProvider
from openbars.storage.parquet_duckdb import open_backend

pytestmark = pytest.mark.skipif(
    os.environ.get("OPENBARS_CRYPTO_SMOKE") != "1", reason="set OPENBARS_CRYPTO_SMOKE=1 to hit public crypto APIs"
)

WINDOW = DateRange(date(2024, 1, 2), date(2024, 1, 3))  # two closed UTC days
DAYS = {date(2024, 1, 2), date(2024, 1, 3)}


def test_public_urls_are_the_expected_hosts():
    assert BINANCE_URL == "https://data-api.binance.vision"
    assert COINBASE_URL == "https://api.exchange.coinbase.com"


@pytest.mark.parametrize(
    ("make", "symbol", "base_url"),
    [
        (BinanceProvider, "BTCUSDT", "https://data-api.binance.vision"),
        (CoinbaseProvider, "BTC-USD", "https://api.exchange.coinbase.com"),
    ],
)
def test_one_public_daily_request(tmp_path, make, symbol, base_url):
    provider = make(base_url=base_url)
    request = FetchRequest(Market.CRYPTO, Interval.D1, (symbol,), WINDOW)
    try:
        batch = provider.fetch_bars(request)
    except ProviderError as exc:
        pytest.skip(f"{provider.id} public API unreachable: {type(exc).__name__}")
    if batch.error is not None and not batch.bars:
        pytest.skip(f"{provider.id} public API returned an error: {type(batch.error).__name__}")
    assert {b.session_date for b in batch.bars} == DAYS
    assert all(b.symbol_as_returned == symbol and b.high >= b.low for b in batch.bars)
    backend = open_backend(tmp_path / "data")  # raw only, in tmp_path
    assert backend.raw.append(batch) == len(batch.bars)
