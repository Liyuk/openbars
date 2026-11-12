"""Synthetic bars for storage tests. Test symbols only; no real market data."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from openbars.core.models import (
    Adjustment,
    Bar,
    CanonicalBar,
    DateRange,
    Interval,
    Market,
    QualityFlag,
    RawBar,
    Session,
)
from openbars.providers.base import FetchRequest, RawBatch


def weekdays(start: date, count: int) -> list[date]:
    out, d = [], start
    while len(out) < count:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def utc(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, tzinfo=UTC)


def raw_batch(
    symbols,
    days,
    *,
    provider="prov_a",
    fetch_id="fetch-1",
    market=Market.US,
    base=11.25,
    volume=1000000.5,
    feed="feed1",
):
    bars = []
    for si, sym in enumerate(symbols):
        for di, day in enumerate(days):
            px = base + si + (di % 5) * 0.25
            bars.append(
                RawBar(
                    provider=provider,
                    symbol_as_returned=sym,
                    fetch_id=fetch_id,
                    interval=Interval.D1,
                    ts_start=utc(day),
                    open=px,
                    high=px + 1.0,
                    low=px - 1.0,
                    close=px + 0.5,
                    volume=volume + di,
                    feed=feed,
                    extra={"session_date": day, "currency": "USD"},
                )
            )
    req = FetchRequest(market, Interval.D1, tuple(symbols), DateRange(days[0], days[-1]))
    return RawBatch(provider, fetch_id, req, tuple(bars))


def canonical_bars(instrument_ids, days, *, source="prov_a", base=11.25, flags=QualityFlag.NONE):
    out = []
    for si, iid in enumerate(instrument_ids):
        for di, day in enumerate(days):
            px = base + si + (di % 5) * 0.25
            bar = Bar(
                instrument_id=iid,
                interval=Interval.D1,
                ts_start=utc(day),
                session_date=day,
                session=Session.REGULAR,
                open=px,
                high=px + 1.0,
                low=px - 1.0,
                close=px + 0.5,
                volume=1000000.5 + di,
                source=source,
                feed="feed1",
                adjustment=Adjustment.SPLIT,
                flags=flags,
            )
            out.append(CanonicalBar(bar, source, source, (source, "prov_b"), "primary_wins", revision=1))
    return out
