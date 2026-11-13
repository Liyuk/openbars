"""Nasdaq Trader symbol-directory files -> ``ListingRecord``.

The two public pipe-delimited files (``nasdaqlisted`` and ``otherlisted``) carry symbol, name,
exchange and an ETF flag, but no instrument-level id. The rows are therefore symbol-only
evidence: ``InstrumentDirectory`` uses them to confirm existing open aliases, never to mint or
reassign an instrument. The ETF flag is the only type information (other rows stay unclassified).

The HTTP source takes a configurable ``User-Agent`` (default ``openbars/<version>``); pass an
``httpx.Client`` (e.g. one with a mock transport) to control transport.
"""

from __future__ import annotations

import csv
import io
import time
from collections.abc import Callable
from dataclasses import dataclass

import httpx

from openbars import __version__
from openbars.core.errors import ProviderError
from openbars.core.models import AssetClass, Market
from openbars.normalize.directory import ListingRecord
from openbars.providers.http import provider_error_from_response

__all__ = ["NasdaqTraderListingSource", "default_user_agent", "parse_nasdaq_listed", "parse_other_listed"]

PROVIDER_ID = "nasdaq_trader"
NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
OTHER_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
# ``otherlisted`` exchange column -> MIC
OTHER_EXCHANGE_CODES = {"N": "XNYS", "A": "XASE", "P": "ARCX", "Z": "BATS", "V": "IEXG"}


def default_user_agent() -> str:
    return f"openbars/{__version__}"


def _parse(payload: str, *, symbol_key: str, exchange_key: str | None) -> list[ListingRecord]:
    rows = csv.DictReader(io.StringIO(payload), delimiter="|")
    if not rows.fieldnames:
        raise ValueError("symbol directory has no header")
    required = {symbol_key, "Security Name", "Test Issue"} | ({exchange_key} if exchange_key else set())
    if not required.issubset(rows.fieldnames):
        raise ValueError("symbol directory is missing required columns")
    out = []
    for row in rows:
        symbol = (row.get(symbol_key) or "").strip().upper()
        name = (row.get("Security Name") or "").strip()
        # the trailing "File Creation Time" footer has no name; test issues are not securities
        if not symbol or not name or (row.get("Test Issue") or "").strip().upper() == "Y":
            continue
        if exchange_key is None:
            exchange = "XNAS"
        else:
            exchange = OTHER_EXCHANGE_CODES.get((row.get(exchange_key) or "").strip().upper(), "UNKNOWN")
        is_etf = (row.get("ETF") or "").strip().upper() == "Y"
        out.append(
            ListingRecord(
                provider=PROVIDER_ID,
                symbol=symbol,
                exchange=exchange,
                asset_class=AssetClass.ETF if is_etf else None,
                name=name,
                market=Market.US,
            )
        )
    out.sort(key=lambda r: (r.symbol, r.exchange or ""))
    return out


def parse_nasdaq_listed(payload: str) -> list[ListingRecord]:
    """Rows of ``nasdaqlisted.txt`` (all on XNAS)."""
    return _parse(payload, symbol_key="Symbol", exchange_key=None)


def parse_other_listed(payload: str) -> list[ListingRecord]:
    """Rows of ``otherlisted.txt`` (exchange from the ``Exchange`` column)."""
    return _parse(payload, symbol_key="ACT Symbol", exchange_key="Exchange")


@dataclass
class NasdaqTraderListingSource:
    client: httpx.Client | None = None
    user_agent: str | None = None
    wait: Callable[[float], None] = time.sleep
    minimum_request_interval_seconds: float = 0.5
    nasdaq_url: str = NASDAQ_LISTED_URL
    other_url: str = OTHER_LISTED_URL

    def load(self) -> list[ListingRecord]:
        """Download and parse both files (the only network call in this module)."""
        client = self.client or httpx.Client(timeout=30.0)
        headers = {"Accept": "text/plain", "User-Agent": self.user_agent or default_user_agent()}
        records: list[ListingRecord] = []
        try:
            for index, (url, parser) in enumerate(
                ((self.nasdaq_url, parse_nasdaq_listed), (self.other_url, parse_other_listed))
            ):
                if index and self.minimum_request_interval_seconds > 0:
                    self.wait(self.minimum_request_interval_seconds)
                try:
                    response = client.get(url, headers=headers)
                except httpx.HTTPError as exc:
                    raise ProviderError(
                        "transport_error", "listing directory request failed", retryable=True
                    ) from exc
                if response.is_error:
                    raise provider_error_from_response(response)
                try:
                    records.extend(parser(response.text))
                except (csv.Error, ValueError) as exc:
                    raise ProviderError(
                        "invalid_payload", "listing directory is malformed", retryable=False
                    ) from exc
        finally:
            if self.client is None:
                client.close()
        if not records:
            raise ProviderError("empty_result", "listing directories returned no records", retryable=True)
        return records
