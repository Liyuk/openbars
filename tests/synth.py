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
    base=Decimal("11.25"),
    volume=Decimal("1000000.5"),
    feed="feed1",
):
    bars = []
    for si, sym in enumerate(symbols):
        for di, day in enumerate(days):
            px = Decimal(base) + si + (di % 5) * Decimal("0.25")
            bars.append(
                RawBar(
                    provider=provider,
                    symbol_as_returned=sym,
                    fetch_id=fetch_id,
                    interval=Interval.D1,
                    ts_start=utc(day),
                    open=px,
                    high=px + Decimal("1"),
                    low=px - Decimal("1"),
                    close=px + Decimal("0.5"),
                    volume=volume + di,
                    feed=feed,
                    extra={"session_date": day, "currency": "USD"},
                )
            )
    req = FetchRequest(market, Interval.D1, tuple(symbols), DateRange(days[0], days[-1]))
    return RawBatch(provider, fetch_id, req, tuple(bars))


def canonical_bars(instrument_ids, days, *, source="prov_a", base=Decimal("11.25"), flags=QualityFlag.NONE):
    out = []
    for si, iid in enumerate(instrument_ids):
        for di, day in enumerate(days):
            px = Decimal(base) + si + (di % 5) * Decimal("0.25")
            bar = Bar(
                instrument_id=iid,
                interval=Interval.D1,
                ts_start=utc(day),
                session_date=day,
                session=Session.REGULAR,
                open=px,
                high=px + Decimal("1"),
                low=px - Decimal("1"),
                close=px + Decimal("0.5"),
                volume=Decimal("1000000.5") + di,
                source=source,
                feed="feed1",
                adjustment=Adjustment.SPLIT,
                flags=flags,
            )
            out.append(CanonicalBar(bar, source, source, (source, "prov_b"), "primary_wins", revision=1))
    return out


def hourly_starts(day: date, count: int = 7, *, first_hour: int = 14, minute: int = 30) -> list[datetime]:
    """UTC ``ts_start`` values one hour apart (default 14:30 .. 20:30 UTC = 09:30..15:30 NY in winter)."""
    return [datetime(day.year, day.month, day.day, first_hour + i, minute, tzinfo=UTC) for i in range(count)]


def raw_batch_h1(
    symbols,
    days,
    *,
    count=7,
    provider="prov_a",
    fetch_id="fetch-1",
    market=Market.US,
    base=Decimal("11.25"),
    volume=Decimal("1000.5"),
    feed="feed1",
):
    bars = []
    for si, sym in enumerate(symbols):
        for day in days:
            for hi, ts in enumerate(hourly_starts(day, count)):
                px = Decimal(base) + si + hi * Decimal("0.25")
                bars.append(
                    RawBar(
                        provider=provider,
                        symbol_as_returned=sym,
                        fetch_id=fetch_id,
                        interval=Interval.H1,
                        ts_start=ts,
                        open=px,
                        high=px + Decimal("1"),
                        low=px - Decimal("1"),
                        close=px + Decimal("0.5"),
                        volume=volume + hi,
                        feed=feed,
                        extra={"session_date": day, "currency": "USD"},
                    )
                )
    req = FetchRequest(market, Interval.H1, tuple(symbols), DateRange(days[0], days[-1]))
    return RawBatch(provider, fetch_id, req, tuple(bars))


def canonical_bars_h1(instrument_ids, days, *, count=7, source="prov_a", base=Decimal("11.25")):
    out = []
    for si, iid in enumerate(instrument_ids):
        for day in days:
            for hi, ts in enumerate(hourly_starts(day, count)):
                px = Decimal(base) + si + hi * Decimal("0.25")
                bar = Bar(
                    instrument_id=iid,
                    interval=Interval.H1,
                    ts_start=ts,
                    session_date=day,
                    session=Session.REGULAR,
                    open=px,
                    high=px + Decimal("1"),
                    low=px - Decimal("1"),
                    close=px + Decimal("0.5"),
                    volume=Decimal("1000.5") + hi,
                    source=source,
                    feed="feed1",
                    adjustment=Adjustment.SPLIT,
                    flags=QualityFlag.NONE,
                )
                out.append(CanonicalBar(bar, source, source, (source, "prov_b"), "primary_wins", revision=1))
    return out
