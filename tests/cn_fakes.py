"""Offline A-share provider for planner / e2e tests: synthetic prices on real XSHG sessions."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from fakes import FakeProvider

from openbars.calendars import cn as cn_calendar
from openbars.core.models import AssetClass, Interval, Market, RawBar
from openbars.core.series import BAOSTOCK_ASHARE_RAW, BarSeriesCatalog
from openbars.providers.base import BatchShape, FetchRequest, RawBatch, SourceCapability
from openbars.providers.cn.baostock import BaoStockAdjustFactor, to_baostock_code

SERIES = BarSeriesCatalog.resolve(BAOSTOCK_ASHARE_RAW, currency="CNY")

# One week of Shanghai sessions (2026-03-02 Mon .. 03-06 Fri, no holiday):
#   Mon / Tue normal; Wed suspended (placeholder at the unchanged close, volume 0);
#   Thu limit up (10.15 * 1.1 = 11.165 -> 11.17 half-up); Fri normal.
WEEK = [date(2026, 3, d) for d in (2, 3, 4, 5, 6)]
# day -> (open, high, low, close, volume, preclose, tradestatus)
PRICES: dict[date, tuple[str, str, str, str, str, str, int]] = {
    date(2026, 3, 2): ("10.00", "10.30", "9.90", "10.05", "1000", "10.00", 1),
    date(2026, 3, 3): ("10.05", "10.20", "10.00", "10.15", "1100", "10.05", 1),
    date(2026, 3, 4): ("10.15", "10.15", "10.15", "10.15", "0", "10.15", 0),
    date(2026, 3, 5): ("11.00", "11.17", "10.90", "11.17", "5000", "10.15", 1),
    date(2026, 3, 6): ("11.10", "11.30", "10.95", "11.00", "2000", "11.17", 1),
}


class CnFake(FakeProvider):
    """A PER_SYMBOL CN provider: one symbol per request, bars on XSHG sessions only.

    Like BaoStock it returns the native spelling (``sh.600000``) as ``symbol_as_returned``, whatever
    spelling was requested. ``factors`` (``code -> [BaoStockAdjustFactor]``) feed
    ``fetch_adjust_factors``.
    """

    def __init__(self, name: str = "baostock", *, factors=None, prices=None, **kw) -> None:
        kw.setdefault("max_symbols", 1)
        super().__init__(name, batch=BatchShape.PER_SYMBOL, feed="ashare", **kw)
        self.factors = dict(factors or {})
        self.prices = dict(PRICES if prices is None else prices)
        self.factor_requests: list[FetchRequest] = []

    def capabilities(self):
        return (
            SourceCapability(
                provider=self.id,
                market=Market.CN,
                interval=Interval.D1,
                asset_classes=frozenset({AssetClass.EQUITY}),
                history_start=None,
                batch=BatchShape.PER_SYMBOL,
                max_symbols_per_request=1,
                max_bars_per_request=10_000,
            ),
        )

    def bar(self, symbol: str, day: date, fetch_id: str) -> RawBar | None:
        if day not in self.prices:
            return None
        o, h, low, c, v, pre, status = self.prices[day]
        return RawBar(
            provider=self.id,
            symbol_as_returned=symbol,
            fetch_id=fetch_id,
            interval=Interval.D1,
            ts_start=datetime(day.year, day.month, day.day, tzinfo=cn_calendar.CN_TZ),
            open=Decimal(o),
            high=Decimal(h),
            low=Decimal(low),
            close=Decimal(c),
            volume=Decimal(v),
            feed="ashare",
            extra={"code": symbol, "tradestatus": status, "isST": 0, "preclose": pre},
            session_date=day,
            series=SERIES,
        )

    def fetch_bars(self, request: FetchRequest) -> RawBatch:
        with self._lock:
            self.requests.append(request)
            call_no = len(self.requests)
        (symbol,) = request.symbols
        code = to_baostock_code(symbol)
        fetch_id = f"{self.id}-{call_no}"
        bars = tuple(
            b
            for day in cn_calendar.sessions(request.window.start, request.window.end)
            if (b := self.bar(code, day, fetch_id)) is not None
        )
        return RawBatch(self.id, fetch_id, request, bars)

    def fetch_adjust_factors(self, request: FetchRequest) -> list[BaoStockAdjustFactor]:
        self.factor_requests.append(request)
        native = to_baostock_code(request.symbols[0])
        return [
            f
            for f in self.factors.get(native, [])
            if request.window.start <= f.divid_operate_date <= request.window.end
        ]
