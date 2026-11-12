"""Shared rules for US 1h provider adapters (issue #115, Discussion #112).

Every US hourly adapter lands bars on the same XNYS grid (``calendars.xnys.hourly_slots``):

* Sources that only serve clock-aligned buckets (Massive aggregates, Alpaca bars) are asked
  for **30-minute** bars and summed up into the configured grid. Every slot edge of both
  ``session_open`` and ``clock_hour`` (and of extended hours / early closes) sits on a
  half hour, so a 30-minute bucket never straddles two slots.
* Sources that already deliver hourly bars on the ``session_open`` grid (yfinance, Twelve
  Data) are kept only when the bar start is a grid slot start; everything else is skipped.

Free-tier history windows are enforced here: a request is clipped to the window, and a
request that lies wholly outside it is rejected instead of being sent.
Bars carry ``extra["session"]`` (``regular`` / ``pre`` / ``post``) and ``extra["ts_end"]``.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

from openbars.calendars import xnys
from openbars.core.errors import ProviderError
from openbars.core.models import DateRange, HourlyAlignment, Interval, RawBar, Session
from openbars.core.series import BarSeriesIdentity, SessionSemantics
from openbars.providers.base import FetchRequest

__all__ = [
    "ALPACA_HOURLY_HISTORY_START",
    "HourlyGrid",
    "HourlyOptions",
    "MASSIVE_HOURLY_WINDOW_DAYS",
    "SubBar",
    "TWELVE_DATA_HOURLY_HISTORY_START",
    "YFINANCE_HOURLY_WINDOW_DAYS",
    "aggregate_to_grid",
    "clip_hourly_window",
    "hourly_options",
    "hourly_series",
    "place_on_grid",
    "require_session_open",
    "to_raw_bar",
]

NEW_YORK = ZoneInfo("America/New_York")
_UTC = timezone.utc
# Massive Basic: about two years of aggregates.
MASSIVE_HOURLY_WINDOW_DAYS = 730
# Yahoo serves 1h bars only for the last 730 days; one day of margin so the edge is never hit.
YFINANCE_HOURLY_WINDOW_DAYS = 729
# Alpaca free plan: SIP history starts in 2016 (same as daily).
ALPACA_HOURLY_HISTORY_START = date(2016, 1, 1)
# Twelve Data: kept at the daily default so free credits are not spent on older windows.
TWELVE_DATA_HOURLY_HISTORY_START = date(2016, 1, 1)


@dataclass(frozen=True)
class HourlyOptions:
    alignment: HourlyAlignment = HourlyAlignment.SESSION_OPEN
    include_extended_hours: bool = False


def hourly_options(request: FetchRequest) -> HourlyOptions:
    raw = request.hourly_alignment
    try:
        alignment = (
            HourlyAlignment.SESSION_OPEN
            if raw is None
            else raw if isinstance(raw, HourlyAlignment) else HourlyAlignment(str(raw).strip())
        )
    except ValueError:
        raise ProviderError(
            "invalid_request", f"unsupported hourly alignment {raw!r}", retryable=False
        ) from None
    return HourlyOptions(alignment, bool(request.include_extended_hours))


def hourly_series(series: BarSeriesIdentity, options: HourlyOptions) -> BarSeriesIdentity:
    """Regular-only by default; ``EXTENDED`` once pre/post bars may be present."""
    semantics = SessionSemantics.EXTENDED if options.include_extended_hours else SessionSemantics.REGULAR
    return replace(series, session_semantics=semantics)


def new_york_today(now: datetime) -> date:
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    return now.astimezone(NEW_YORK).date()


def clip_hourly_window(
    window: DateRange,
    *,
    provider: str,
    now: datetime,
    rolling_days: int | None = None,
    history_start: date | None = None,
) -> DateRange:
    """Clip ``window`` to ``[max(history_start, today - rolling_days), today]`` (New York dates).

    Raises ``outside_free_window`` when nothing is left: such a request must not be sent.
    """
    if window.end < window.start:
        raise ProviderError("invalid_request", "window end is before start", retryable=False)
    today = new_york_today(now)
    lower = window.start
    if rolling_days is not None:
        lower = max(lower, today - timedelta(days=rolling_days))
    if history_start is not None:
        lower = max(lower, history_start)
    upper = min(window.end, today)
    if upper < lower:
        raise ProviderError(
            "outside_free_window",
            f"{provider} 1h window {window.start.isoformat()}..{window.end.isoformat()} is outside "
            "the free-tier history window",
            retryable=False,
        )
    return DateRange(lower, upper)


class HourlyGrid:
    """XNYS hourly slots for every session of a window, with lookups by bar time."""

    def __init__(self, window: DateRange, options: HourlyOptions) -> None:
        self.window = window
        self.options = options
        slots: list[xnys.HourlyBarSlot] = []
        for day in xnys.sessions(window.start, window.end):
            slots.extend(
                xnys.hourly_slots(
                    day,
                    alignment=options.alignment,
                    include_extended_hours=options.include_extended_hours,
                )
            )
        self.slots = tuple(sorted(slots, key=lambda s: s.ts_start))
        self._by_start = {s.ts_start: s for s in self.slots}

    def __bool__(self) -> bool:
        return bool(self.slots)

    @property
    def start(self) -> datetime:
        return self.slots[0].ts_start

    @property
    def end(self) -> datetime:
        return self.slots[-1].ts_end

    def slot_starting_at(self, ts: datetime) -> xnys.HourlyBarSlot | None:
        return self._by_start.get(ts.astimezone(_UTC))

    def slot_containing(self, ts: datetime, length: timedelta) -> xnys.HourlyBarSlot | None:
        """The slot that wholly contains ``[ts, ts + length)``; ``None`` if it falls outside."""
        ts = ts.astimezone(_UTC)
        # Slot edges sit on half hours and slots are at most an hour long, so the slot start
        # is the bar start floored to a half hour, or 30 / 60 minutes before that.
        floored = ts.replace(minute=ts.minute - ts.minute % 30, second=0, microsecond=0)
        for back in (0, 30, 60):
            slot = self._by_start.get(floored - timedelta(minutes=back))
            if slot is not None and slot.ts_start <= ts and ts + length <= slot.ts_end:
                return slot
        return None


@dataclass
class SubBar:
    """One provider bar before it is placed on the grid (already parsed to ``Decimal``)."""

    ts_start: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal | None
    vwap: Decimal | None = None
    trade_count: int | None = None
    payload: dict = field(default_factory=dict)


@dataclass(frozen=True)
class GridBar:
    slot: xnys.HourlyBarSlot
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal | None
    vwap: Decimal | None
    trade_count: int | None
    parts: int
    payloads: tuple[dict, ...]

    def extra(self) -> dict[str, object]:
        out: dict[str, object] = {
            "session": self.slot.session.value,
            "ts_end": self.slot.ts_end.isoformat(),
        }
        if self.trade_count is not None:
            out["trade_count"] = self.trade_count
        if self.parts > 1:
            out["source_bars"] = self.parts
        return out


_VWAP_QUANTUM = Decimal(1).scaleb(-8)


def aggregate_to_grid(
    bars: Iterable[SubBar],
    grid: HourlyGrid,
    *,
    sub_length: timedelta,
    now: datetime,
    skipped: dict[str, int],
) -> list[GridBar]:
    """Sum sub-bars (sorted or not) into grid slots: first open, max high, min low, last close.

    Volume is summed (``None`` if any part has none); VWAP is the volume-weighted mean when
    every part has one; trade counts are summed. Sub-bars outside the grid are skipped
    (``off_grid``); slots that have not ended by ``now`` are dropped (``incomplete_slot``).
    """

    def skip(reason: str) -> None:
        skipped[reason] = skipped.get(reason, 0) + 1

    groups: dict[datetime, list[SubBar]] = {}
    slots: dict[datetime, xnys.HourlyBarSlot] = {}
    seen: set[datetime] = set()
    for bar in sorted(bars, key=lambda b: b.ts_start):
        if bar.ts_start in seen:
            skip("duplicate")
            continue
        seen.add(bar.ts_start)
        slot = grid.slot_containing(bar.ts_start, sub_length)
        if slot is None:
            skip("off_grid")
            continue
        groups.setdefault(slot.ts_start, []).append(bar)
        slots[slot.ts_start] = slot
    out: list[GridBar] = []
    for key in sorted(groups):
        slot, parts = slots[key], groups[key]
        if slot.ts_end > now:
            skip("incomplete_slot")
            continue
        volumes = [p.volume for p in parts]
        volume = None if any(v is None for v in volumes) else sum(volumes, Decimal(0))
        vwap = None
        if volume is not None and volume > 0 and all(p.vwap is not None for p in parts):
            weighted = sum((p.vwap * p.volume for p in parts), Decimal(0))  # type: ignore[operator]
            vwap = (weighted / volume).quantize(_VWAP_QUANTUM)
        counts = [p.trade_count for p in parts]
        trade_count = None if any(c is None for c in counts) else sum(counts)  # type: ignore[arg-type]
        out.append(
            GridBar(
                slot=slot,
                open=parts[0].open,
                high=max(p.high for p in parts),
                low=min(p.low for p in parts),
                close=parts[-1].close,
                volume=volume,
                vwap=vwap,
                trade_count=trade_count,
                parts=len(parts),
                payloads=tuple(p.payload for p in parts),
            )
        )
    return out


def place_on_grid(
    bars: Iterable[SubBar], grid: HourlyGrid, *, now: datetime, skipped: dict[str, int]
) -> list[GridBar]:
    """Keep provider hourly bars whose start is a grid slot start (no aggregation)."""

    def skip(reason: str) -> None:
        skipped[reason] = skipped.get(reason, 0) + 1

    out: dict[datetime, GridBar] = {}
    for bar in bars:
        slot = grid.slot_starting_at(bar.ts_start)
        if slot is None:
            skip("off_grid")
            continue
        if slot.ts_end > now:
            skip("incomplete_slot")
            continue
        if slot.ts_start in out:
            skip("duplicate")
            continue
        out[slot.ts_start] = GridBar(
            slot=slot,
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=bar.volume,
            vwap=bar.vwap,
            trade_count=bar.trade_count,
            parts=1,
            payloads=(bar.payload,),
        )
    return [out[k] for k in sorted(out)]


def require_session_open(provider: str, options: HourlyOptions) -> None:
    """For sources whose 1h bars start at 09:30 New York and cannot be re-cut."""
    if options.alignment is not HourlyAlignment.SESSION_OPEN:
        raise ProviderError(
            "unsupported_request",
            f"{provider} 1h bars are session_open aligned; clock_hour is not available",
            retryable=False,
        )


def is_regular(bar: GridBar) -> bool:
    return bar.slot.session is Session.REGULAR


def to_raw_bar(
    bar: GridBar,
    *,
    provider: str,
    symbol: str,
    fetch_id: str,
    feed: str,
    series: BarSeriesIdentity,
    instrument_id: str | None = None,
) -> RawBar:
    """A grid bar as a ``RawBar`` (``interval=1h``, ``ts_start`` = slot start)."""
    from openbars.providers.http import digest_payload

    try:
        return RawBar(
            provider=provider,
            symbol_as_returned=symbol,
            fetch_id=fetch_id,
            interval=Interval.H1,
            ts_start=bar.slot.ts_start,
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=bar.volume,
            feed=feed,
            extra=bar.extra(),
            vwap=bar.vwap,
            payload_hash=digest_payload({"T": symbol, "parts": list(bar.payloads)}),
            instrument_id=instrument_id,
            session_date=bar.slot.session_date,
            series=series,
        )
    except (ValueError, TypeError):
        raise ProviderError(
            "invalid_payload", "hourly bar failed OHLCV validation", retryable=False
        ) from None


def sub_bar_from_json(obj: dict, ts: datetime) -> SubBar:
    """Parse an ``{o, h, l, c, v, vw, n}`` aggregate (Massive / Alpaca shape) into a ``SubBar``."""
    from openbars.providers.http import decimal_value, integer_value

    vwap = None
    if obj.get("vw") is not None:
        raw_vwap = decimal_value(obj["vw"], "vw")
        if raw_vwap > 0:
            vwap = raw_vwap
    trade_count = integer_value(obj["n"], "n") if obj.get("n") is not None else None
    return SubBar(
        ts_start=ts,
        open=decimal_value(obj.get("o"), "o"),
        high=decimal_value(obj.get("h"), "h"),
        low=decimal_value(obj.get("l"), "l"),
        close=decimal_value(obj.get("c"), "c"),
        volume=decimal_value(obj.get("v"), "v"),
        vwap=vwap,
        trade_count=trade_count,
        payload=dict(obj),
    )
