"""Offline fakes for planner / orchestrator tests: provider, raw stores. Test symbols only."""

from __future__ import annotations

import sqlite3
import threading
import time
from collections.abc import Callable, Iterable, Sequence
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from openbars.core.errors import ProviderError
from openbars.core.models import (
    AssetClass,
    CorporateAction,
    DateRange,
    Interval,
    Market,
    RawBar,
)
from openbars.providers.base import (
    BatchShape,
    FetchRequest,
    ProviderInstrument,
    RawBatch,
    SourceCapability,
)
from openbars.quota.policy import BackoffPolicy, QuotaPolicy, RateLimit

DEFAULT_SYMBOLS = ("TEST1", "ZZZA")


def weekdays_between(start: date, end: date) -> list[date]:
    out, day = [], start
    while day <= end:
        if day.weekday() < 5:
            out.append(day)
        day += timedelta(days=1)
    return out


class FakeProvider:
    """In-memory provider. ``behavior(request, call_no)`` may raise to inject failures."""

    api_version = 1

    def __init__(
        self,
        name: str = "fake",
        *,
        batch: BatchShape = BatchShape.MULTI_SYMBOL,
        interval: Interval = Interval.D1,
        history_start: date | None = None,
        rolling_window: timedelta | None = None,
        max_symbols: int = 200,
        whole_market_symbols: Sequence[str] = DEFAULT_SYMBOLS,
        behavior: Callable[[FetchRequest, int], None] | None = None,
        on_fetch: Callable[[FetchRequest], None] | None = None,
        page_size: int | None = None,
        feed: str = "fake",
        delay: float = 0.0,
        limit_per_minute: int = 1000,
        backoff: BackoffPolicy | None = None,
    ) -> None:
        self.id = name
        self.batch = batch
        self.interval = interval
        self.history_start = history_start
        self.rolling_window = rolling_window
        self.max_symbols = max_symbols
        self.whole_market_symbols = tuple(whole_market_symbols)
        self.behavior = behavior
        self.on_fetch = on_fetch
        self.page_size = page_size
        self.feed = feed
        self.delay = delay
        self.limit = limit_per_minute
        self.backoff = backoff or BackoffPolicy(base_seconds=1.0, max_seconds=60.0)
        self.requests: list[FetchRequest] = []
        self._lock = threading.Lock()

    # -- Provider protocol ------------------------------------------------------------
    def capabilities(self) -> Sequence[SourceCapability]:
        return (
            SourceCapability(
                provider=self.id,
                market=Market.US,
                interval=self.interval,
                asset_classes=frozenset({AssetClass.EQUITY}),
                history_start=self.history_start,
                rolling_window=self.rolling_window,
                batch=self.batch,
                max_symbols_per_request=self.max_symbols,
                max_bars_per_request=10_000,
            ),
        )

    def quota_policy(self) -> QuotaPolicy:
        return QuotaPolicy(
            provider=self.id,
            limits=(RateLimit(self.limit, timedelta(minutes=1)),),
            backoff=self.backoff,
        )

    def list_instruments(self, market: Market) -> Iterable[ProviderInstrument]:
        return ()

    def fetch_corporate_actions(self, request: FetchRequest) -> Iterable[CorporateAction]:
        return ()

    # -- data ---------------------------------------------------------------------------
    def bar(self, symbol: str, day: date, fetch_id: str) -> RawBar | None:
        base = Decimal(10 + sum(map(ord, symbol)) % 50) + Decimal(day.toordinal() % 7) / 4
        return RawBar(
            provider=self.id,
            symbol_as_returned=symbol,
            fetch_id=fetch_id,
            interval=Interval.D1,
            ts_start=datetime(day.year, day.month, day.day, tzinfo=UTC),
            open=base,
            high=base + 1,
            low=base - 1,
            close=base + Decimal("0.5"),
            volume=Decimal(1000 + day.day),
            feed=self.feed,
            extra={"session_date": day, "adjustment": "split"},
            session_date=day,
        )

    def fetch_bars(self, request: FetchRequest) -> RawBatch:
        with self._lock:
            self.requests.append(request)
            call_no = len(self.requests)
        if self.delay:
            time.sleep(self.delay)
        if self.behavior is not None:
            self.behavior(request, call_no)
        symbols = request.symbols or self.whole_market_symbols
        fetch_id = f"{self.id}-{call_no}"
        bars = [
            bar
            for day in weekdays_between(request.window.start, request.window.end)
            for sym in symbols
            if (bar := self.bar(sym, day, fetch_id)) is not None  # subclasses may skip a cell
        ]
        next_cursor = None
        if self.page_size:
            offset = int(request.cursor or 0)
            page = bars[offset : offset + self.page_size]
            if offset + self.page_size < len(bars):
                next_cursor = str(offset + self.page_size)
            bars = page
        if self.on_fetch is not None:
            self.on_fetch(request)
        return RawBatch(self.id, fetch_id, request, tuple(bars), next_cursor=next_cursor)


def always_fail(error: ProviderError) -> Callable[[FetchRequest, int], None]:
    def behavior(request: FetchRequest, call_no: int) -> None:
        raise error

    return behavior


def fail_first(n: int, error: ProviderError) -> Callable[[FetchRequest, int], None]:
    def behavior(request: FetchRequest, call_no: int) -> None:
        if call_no <= n:
            raise error

    return behavior


class MemoryRawStore:
    """RawStore fake that dedups on (provider, symbol, ts_start, feed)."""

    def __init__(self) -> None:
        self.rows: dict[tuple, RawBar] = {}
        self.appends = 0
        self._lock = threading.Lock()

    def append(self, batch: RawBatch) -> None:
        with self._lock:
            self.appends += 1
            for bar in batch.bars:
                self.rows[(bar.provider, bar.symbol_as_returned, bar.ts_start, bar.feed)] = bar

    def scan(self, provider: str, market: Market, interval: Interval, window: DateRange) -> Iterable[RawBar]:
        for bar in list(self.rows.values()):
            day = bar.session_date or bar.ts_start.date()
            if bar.provider == provider and bar.interval == interval and window.start <= day <= window.end:
                yield bar


class SqliteSideStore:
    """Cross-process raw store + execution log for the kill -9 test (sqlite, dedup by key)."""

    def __init__(self, path: str | Path) -> None:
        self.path = str(path)
        with self._con() as con:
            con.execute("PRAGMA journal_mode=WAL")
            con.execute(
                "CREATE TABLE IF NOT EXISTS raw (provider TEXT, symbol TEXT, ts TEXT, "
                "PRIMARY KEY (provider, symbol, ts))"
            )
            con.execute("CREATE TABLE IF NOT EXISTS exec_log (key TEXT)")

    def _con(self) -> sqlite3.Connection:
        return sqlite3.connect(self.path, timeout=30)

    def append(self, batch: RawBatch) -> None:
        with self._con() as con:
            con.executemany(
                "INSERT OR IGNORE INTO raw VALUES (?,?,?)",
                [(b.provider, b.symbol_as_returned, b.ts_start.isoformat()) for b in batch.bars],
            )

    def scan(self, provider, market, interval, window):  # noqa: ANN001
        return iter(())

    def log_exec(self, key: str) -> None:
        with self._con() as con:
            con.execute("INSERT INTO exec_log VALUES (?)", (key,))

    def raw_count(self) -> int:
        with self._con() as con:
            return con.execute("SELECT COUNT(*) FROM raw").fetchone()[0]

    def exec_counts(self) -> dict[str, int]:
        with self._con() as con:
            return dict(con.execute("SELECT key, COUNT(*) FROM exec_log GROUP BY key").fetchall())


def exec_key(request: FetchRequest) -> str:
    return f"{','.join(request.symbols)}|{request.window.start.isoformat()}"
