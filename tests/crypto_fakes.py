"""Offline crypto providers for planner / e2e tests: fake prices, synthetic TEST pairs only."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from fakes import FakeProvider

from openbars.core.models import AssetClass, Interval, Market, RawBar
from openbars.core.series import BINANCE_SPOT_RAW, COINBASE_EXCHANGE_RAW, BarSeriesCatalog
from openbars.providers.base import BatchShape, FetchRequest, RawBatch, SourceCapability

SERIES = {
    "binance": BarSeriesCatalog.resolve(BINANCE_SPOT_RAW, currency=None),
    "coinbase": BarSeriesCatalog.resolve(COINBASE_EXCHANGE_RAW, currency=None),
}
FEEDS = {"binance": "spot", "coinbase": "exchange"}


class CryptoFake(FakeProvider):
    """A PER_SYMBOL crypto provider (1 symbol per request) that serves every UTC day.

    Rejects symbols of the other venue's dialect so a mis-routed job fails loudly.
    """

    def __init__(self, name: str, *, base_price: int = 100, **kw) -> None:
        kw.setdefault("max_symbols", 1)
        super().__init__(name, batch=BatchShape.PER_SYMBOL, feed=FEEDS[name], **kw)
        self.base_price = base_price

    def capabilities(self):
        return (
            SourceCapability(
                provider=self.id,
                market=Market.CRYPTO,
                interval=Interval.D1,
                asset_classes=frozenset({AssetClass.CRYPTO_SPOT}),
                history_start=None,
                batch=BatchShape.PER_SYMBOL,
                max_symbols_per_request=1,
                max_bars_per_request=300,
            ),
        )

    def bar(self, symbol: str, day: date, fetch_id: str) -> RawBar:
        base = Decimal(self.base_price + day.day)
        return RawBar(
            provider=self.id,
            symbol_as_returned=symbol,
            fetch_id=fetch_id,
            interval=Interval.D1,
            ts_start=datetime(day.year, day.month, day.day, tzinfo=UTC),
            open=base,
            high=base + 2,
            low=base - 2,
            close=base + 1,
            volume=Decimal(1000 + day.day),
            feed=FEEDS[self.id],
            extra={"session_date": day},
            session_date=day,
            series=SERIES[self.id],
        )

    def fetch_bars(self, request: FetchRequest) -> RawBatch:
        with self._lock:
            self.requests.append(request)
            call_no = len(self.requests)
        symbol = request.symbols[0]
        dash = "-" in symbol
        if dash != (self.id == "coinbase"):
            raise AssertionError(f"{symbol!r} routed to {self.id}")
        fetch_id = f"{self.id}-{call_no}"
        days = (request.window.end - request.window.start).days + 1
        bars = tuple(
            self.bar(symbol, request.window.start + timedelta(days=i), fetch_id) for i in range(days)
        )
        return RawBatch(self.id, fetch_id, request, bars)
