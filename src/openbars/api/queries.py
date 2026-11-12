"""Read-only helpers for the local ``openbars serve`` UI.

These functions talk to an already-opened ``StorageBackend``. They never plan,
fetch, or write. Routes under ``/api/v1`` are for this UI only and are not a
stable public API.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from openbars.core.models import Adjustment, DateRange, HourlyAlignment, Interval, Market, QualityFlag
from openbars.reconcile import policy_version_for_market
from openbars.storage.schema import AUTO_POLICY_VERSION

MAX_BARS = 8000
DEFAULT_INSTRUMENT_LIMIT = 50
MAX_INSTRUMENT_LIMIT = 200
DEFAULT_FLAG_LIMIT = 100
MAX_FLAG_LIMIT = 500
WIDE_START = date(1970, 1, 1)
WIDE_END = date(2100, 12, 31)
SOURCE_CANONICAL = "canonical"
KNOWN_FLAGS = tuple(f.name.lower() for f in QualityFlag if f is not QualityFlag.NONE)

# Keys that must never appear in a quota snapshot returned to the UI.
_SECRET_KEY_PARTS = ("key", "secret", "token", "password", "passwd", "dsn", "credential")


class QueryError(ValueError):
    """Bad request parameters for the read API."""


def calendar_for(market: Market) -> Any:
    """Return the trading calendar for ``market``."""
    from openbars.calendars import calendar_for_market

    try:
        return calendar_for_market(market)
    except ValueError as exc:
        raise QueryError(str(exc)) from exc


def parse_market(value: str | None) -> Market | None:
    if value is None or not str(value).strip():
        return None
    text = str(value).strip().lower()
    try:
        return Market(text)
    except ValueError as exc:
        raise QueryError(f"unknown market {value!r}") from exc


def parse_interval(value: str | None) -> Interval:
    if value is None or not str(value).strip():
        return Interval.D1
    text = str(value).strip()
    for item in (Interval.D1, Interval.H1):
        if item.value == text or item.name.lower() == text.lower():
            return item
    raise QueryError(f"unknown interval {value!r}; expected 1d or 1h")


def parse_adjustment(value: str | None) -> Adjustment:
    if value is None or not str(value).strip():
        return Adjustment.SPLIT
    text = str(value).strip().lower()
    for item in Adjustment:
        if item.value == text or item.name.lower() == text:
            return item
    raise QueryError(f"unknown adjustment {value!r}")


def parse_date(value: str | date | None, name: str) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    text = str(value).strip()
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise QueryError(f"invalid {name} {value!r}") from exc


def parse_flag(value: str | None) -> str | None:
    if value is None or not str(value).strip():
        return None
    name = str(value).strip().lower()
    if name not in KNOWN_FLAGS:
        raise QueryError(f"unknown flag {value!r}")
    return name


def dec_str(value: Any) -> str | None:
    """Render a number as a decimal string (no binary float)."""
    if value is None:
        return None
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return format(Decimal(value), "f")
    text = str(value).strip()
    if not text:
        return None
    return format(Decimal(text), "f")


def _iso_date(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _flag_names(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, QualityFlag):
        return [f.name.lower() for f in QualityFlag if f and f in value and f is not QualityFlag.NONE]
    names: list[str] = []
    if isinstance(value, str):
        items = [value]
    else:
        try:
            items = list(value)
        except TypeError:
            items = [value]
    for item in items:
        if item is None:
            continue
        if isinstance(item, QualityFlag):
            name = item.name.lower()
        else:
            name = str(item).strip().lower()
        if name and name != "none" and name not in names:
            names.append(name)
    return names


def _wide_window(start: date | None = None, end: date | None = None) -> DateRange:
    return DateRange(start or WIDE_START, end or WIDE_END)


def _policy_for(market: Market | None, instrument_ids: list[str] | None = None) -> str:
    return policy_version_for_market(market, instrument_ids=instrument_ids or [])


def _scan_canonical(
    backend: Any,
    instrument_ids: list[str] | None,
    *,
    interval: Interval = Interval.D1,
    start: date | None = None,
    end: date | None = None,
    market: Market | None = None,
    policy_version: str | None = AUTO_POLICY_VERSION,
) -> list[Any]:
    window = _wide_window(start, end)
    pv: str | None
    if policy_version in {AUTO_POLICY_VERSION, "auto"}:
        # Hourly and daily coverage use the same market policy selection.
        pv = _policy_for(market, instrument_ids)
    else:
        pv = policy_version
    return list(
        backend.canonical.scan_rows(
            instrument_ids, interval, window, market=market, policy_version=pv
        )
    )


def _from_canonical_row(row: Any) -> dict[str, Any]:
    session = getattr(row, "session_date", None)
    return {
        "instrument_id": getattr(row, "instrument_id", None),
        "session_date": _iso_date(session),
        "ts_start": getattr(row, "ts_start", None).isoformat() if getattr(row, "ts_start", None) else None,
        "session": getattr(row, "session", None),
        "open": dec_str(getattr(row, "open", None)),
        "high": dec_str(getattr(row, "high", None)),
        "low": dec_str(getattr(row, "low", None)),
        "close": dec_str(getattr(row, "close", None)),
        "volume": dec_str(getattr(row, "volume", None)),
        "flags": _flag_names(getattr(row, "flags", None)),
        "provider": getattr(row, "provider", None) or getattr(row, "price_source", None),
        "adjustment": getattr(getattr(row, "adjustment", None), "value", getattr(row, "adjustment", None)),
    }


def _from_arrow_table(table: Any) -> list[dict[str, Any]]:
    records = table.to_pylist()
    out: list[dict[str, Any]] = []
    for rec in records:
        flags = rec.get("flags")
        out.append(
            {
                "instrument_id": rec.get("instrument_id"),
                "session_date": _iso_date(rec.get("session_date")),
                "ts_start": rec.get("ts_start").isoformat() if rec.get("ts_start") else None,
                "open": dec_str(rec.get("open")),
                "high": dec_str(rec.get("high")),
                "low": dec_str(rec.get("low")),
                "close": dec_str(rec.get("close")),
                "volume": dec_str(rec.get("volume")),
                "flags": _flag_names(flags),
                "provider": rec.get("provider") or rec.get("price_source"),
                "adjustment": rec.get("adjustment"),
            }
        )
    return out


def load_canonical(
    backend: Any,
    instrument_ids: list[str],
    *,
    interval: Interval = Interval.D1,
    start: date | None = None,
    end: date | None = None,
    adjustment: Adjustment = Adjustment.SPLIT,
    as_of: date | None = None,
    market: Market | None = None,
) -> list[dict[str, Any]]:
    """Load canonical bars via ``BarReader`` when present, else ``scan_rows``."""
    if interval == Interval.H1:
        if as_of is not None:
            raise QueryError("as_of is not supported for canonical hourly bars")
        rows = [_from_canonical_row(row) for row in _scan_canonical(
            backend, instrument_ids, interval=interval, start=start, end=end, market=market
        )]
        if any(row["adjustment"] != adjustment.value for row in rows):
            raise QueryError("requested adjustment is not available for canonical hourly bars")
        return rows
    reader = getattr(backend, "reader", None)
    if reader is not None and hasattr(reader, "bars"):
        result = reader.bars(
            instrument_ids,
            interval=interval,
            start=start,
            end=end,
            adjustment=adjustment,
            as_of=as_of,
            market=market,
        )
        if hasattr(result, "to_pylist"):
            return _from_arrow_table(result)
        return [_from_canonical_row(row) for row in result]
    rows = _scan_canonical(
        backend,
        instrument_ids,
        interval=interval,
        start=start,
        end=end,
        market=market,
        policy_version=AUTO_POLICY_VERSION,
    )
    return [_from_canonical_row(row) for row in rows]


def _mappings(backend: Any) -> list[Any]:
    master = backend.instruments
    if not hasattr(master, "mappings"):
        return []
    return list(master.mappings())


def _symbol_for(instrument_id: str, mappings: list[Any], on: date | None = None) -> str | None:
    chosen = None
    for mapping in mappings:
        if mapping.instrument_id != instrument_id:
            continue
        if on is not None:
            if mapping.valid_from > on:
                continue
            if mapping.valid_to is not None and mapping.valid_to < on:
                continue
        if chosen is None or mapping.valid_from > chosen.valid_from:
            chosen = mapping
    return chosen.symbol if chosen is not None else None


def resolve_instrument(
    backend: Any,
    *,
    instrument_id: str | None = None,
    symbol: str | None = None,
    market: Market | None = None,
    on: date | None = None,
) -> tuple[str, Market | None]:
    """Resolve ``id`` or ``symbol`` to ``(instrument_id, market)``."""
    master = backend.instruments
    on = on or date.today()
    text_id = (instrument_id or "").strip()
    if text_id:
        inst = master.get(text_id) if hasattr(master, "get") else None
        if inst is not None:
            return inst.instrument_id, inst.market
        return text_id, market
    text_symbol = (symbol or "").strip()
    if not text_symbol:
        raise QueryError("id or symbol is required")
    inst = master.get(text_symbol) if hasattr(master, "get") else None
    if inst is not None:
        return inst.instrument_id, inst.market
    if hasattr(master, "resolve_any"):
        try:
            resolved = master.resolve_any(text_symbol, on=on, market=market)
        except Exception as exc:
            raise QueryError(f"unknown symbol {text_symbol!r}") from exc
        inst = master.get(resolved) if hasattr(master, "get") else None
        return resolved, inst.market if inst is not None else market
    raise QueryError(f"unknown symbol {text_symbol!r}")


def last_session_for(backend: Any, instrument_id: str, market: Market | None) -> date | None:
    rows = _scan_canonical(backend, [instrument_id], market=market)
    if not rows:
        return None
    days = [r.session_date for r in rows if getattr(r, "session_date", None) is not None]
    return max(days) if days else None


def list_markets(backend: Any, *, interval: Interval = Interval.D1) -> list[dict[str, Any]]:
    """Coverage summary for each market that has stored canonical bars."""
    out: list[dict[str, Any]] = []
    for market in Market:
        rows = _scan_canonical(backend, None, market=market, interval=interval)
        if not rows:
            continue
        instruments: set[str] = set()
        sessions: list[date] = []
        flags = {name: 0 for name in KNOWN_FLAGS}
        for row in rows:
            iid = getattr(row, "instrument_id", None)
            if iid:
                instruments.add(iid)
            session = getattr(row, "session_date", None)
            if session is not None:
                sessions.append(session)
            for name in _flag_names(getattr(row, "flags", None)):
                if name in flags:
                    flags[name] += 1
        out.append(
            {
                "market": market.value,
                "instruments": len(instruments),
                "bars": len(rows),
                "first_session": min(sessions).isoformat() if sessions else None,
                "last_session": max(sessions).isoformat() if sessions else None,
                "flags": flags,
            }
        )
    return out


def list_instruments(
    backend: Any,
    *,
    market: Market | None = None,
    q: str | None = None,
    limit: int = DEFAULT_INSTRUMENT_LIMIT,
    offset: int = 0,
) -> dict[str, Any]:
    if limit < 1 or limit > MAX_INSTRUMENT_LIMIT:
        raise QueryError(f"limit must be between 1 and {MAX_INSTRUMENT_LIMIT}")
    if offset < 0:
        raise QueryError("offset must be >= 0")
    master = backend.instruments
    insts = list(master.instruments()) if hasattr(master, "instruments") else []
    if market is not None:
        insts = [i for i in insts if i.market is market]
    mappings = _mappings(backend)
    by_id: dict[str, list[Any]] = {}
    for mapping in mappings:
        by_id.setdefault(mapping.instrument_id, []).append(mapping)
    needle = (q or "").strip().lower()
    items: list[dict[str, Any]] = []
    today = date.today()
    for inst in insts:
        maps = by_id.get(inst.instrument_id, [])
        symbols = sorted({m.symbol for m in maps})
        hay = " ".join([inst.instrument_id, inst.name or "", *symbols]).lower()
        if needle and needle not in hay:
            continue
        last = last_session_for(backend, inst.instrument_id, inst.market)
        items.append(
            {
                "instrument_id": inst.instrument_id,
                "symbol": _symbol_for(inst.instrument_id, maps, on=today) or (symbols[0] if symbols else None),
                "symbols": symbols,
                "name": inst.name,
                "market": inst.market.value,
                "listed_on": _iso_date(inst.listed_on),
                "delisted_on": _iso_date(inst.delisted_on),
                "last_session": _iso_date(last),
            }
        )
    items.sort(key=lambda row: (row["market"] or "", row["instrument_id"] or ""))
    total = len(items)
    return {"items": items[offset : offset + limit], "total": total, "limit": limit, "offset": offset}


def available_sources(
    backend: Any,
    instrument_id: str,
    *,
    market: Market,
    interval: Interval,
    start: date | None,
    end: date | None,
) -> list[str]:
    """Return ``canonical`` plus raw provider names that have rows in the window."""
    names: list[str] = []
    if _scan_canonical(
        backend, [instrument_id], interval=interval, start=start, end=end, market=market
    ):
        names.append(SOURCE_CANONICAL)
    raw_rows = _raw_rows(
        backend, instrument_id, None, market=market, interval=interval, start=start, end=end
    )
    providers = sorted({r.provider for r in raw_rows if getattr(r, "provider", None)})
    for name in providers:
        if name not in names:
            names.append(name)
    return names


def _raw_rows(
    backend: Any,
    instrument_id: str,
    provider: str | None,
    *,
    market: Market,
    interval: Interval,
    start: date | None,
    end: date | None,
) -> list[Any]:
    window = _wide_window(start, end)
    try:
        rows = list(backend.raw.scan_rows(
            provider, market, interval, window, instrument_ids=[instrument_id]
        ))
    except TypeError:
        rows = []
    symbols = [m.symbol for m in _mappings(backend) if m.instrument_id == instrument_id]
    if interval != Interval.H1:
        # Daily and other intervals keep the pre-#147 rule: rows written before
        # instrument_id was filled are used only when no mapped row exists.
        if rows or not symbols:
            return rows
        return list(backend.raw.scan_rows(provider, market, interval, window, symbols=symbols))
    if symbols:
        # Normalize only the instrument portion of the storage key: legacy rows
        # reached through these mappings represent the requested instrument.
        mapped_keys = {row.key()[:6] + row.key()[7:] for row in rows}
        legacy = backend.raw.scan_rows(provider, market, interval, window, symbols=symbols)
        rows.extend(
            row for row in legacy
            if not getattr(row, "instrument_id", None)
            and row.key()[:6] + row.key()[7:] not in mapped_keys
        )
    return rows


def _from_raw_row(row: Any, instrument_id: str) -> dict[str, Any]:
    missing: list[str] = []
    volume = getattr(row, "volume", None)
    if volume is None:
        missing.append("volume")
    for field in ("open", "high", "low", "close"):
        if getattr(row, field, None) is None:
            missing.append(field)
    extra = getattr(row, "extra", None) or {}
    return {
        "session": extra.get("session"),
        "ts_end": extra.get("ts_end"),
        "instrument_id": getattr(row, "instrument_id", None) or instrument_id,
        "session_date": _iso_date(getattr(row, "session_date", None)),
        "ts_start": getattr(row, "ts_start", None).isoformat() if getattr(row, "ts_start", None) else None,
        "open": dec_str(getattr(row, "open", None)),
        "high": dec_str(getattr(row, "high", None)),
        "low": dec_str(getattr(row, "low", None)),
        "close": dec_str(getattr(row, "close", None)),
        "volume": dec_str(volume),
        "flags": [],
        "provider": getattr(row, "provider", None),
        "adjustment": getattr(row, "adjustment", None) or "raw",
        "missing_fields": missing,
    }


def _session_dates(calendar: Any, start: date, end: date) -> list[date]:
    range_fn = getattr(calendar, "sessions", None) or getattr(calendar, "range", None)
    if range_fn is None:
        raise QueryError("calendar has no sessions method")
    days = list(range_fn(start, end))
    out: list[date] = []
    for item in days:
        if isinstance(item, datetime):
            out.append(item.date())
        else:
            out.append(item)
    return out


def _count_sessions(calendar: Any, start: date, end: date) -> int:
    return len(_session_dates(calendar, start, end))


def list_bars(
    backend: Any,
    *,
    instrument_id: str | None = None,
    symbol: str | None = None,
    market: Market | None = None,
    interval: Interval = Interval.D1,
    start: date | None = None,
    end: date | None = None,
    adjustment: Adjustment = Adjustment.SPLIT,
    as_of: date | None = None,
    source: str | None = None,
) -> dict[str, Any]:
    """Canonical (default) or raw-provider bars, plus ``available_sources`` and gaps."""
    source_name = (source or SOURCE_CANONICAL).strip()
    if not source_name:
        raise QueryError("source must be 'canonical' or a provider name")
    iid, resolved_market = resolve_instrument(
        backend, instrument_id=instrument_id, symbol=symbol, market=market, on=end
    )
    inst = backend.instruments.get(iid) if hasattr(backend.instruments, "get") else None
    market = resolved_market or market or (inst.market if inst is not None else None)
    if market is None:
        # Infer from the id prefix when the instrument row is missing.
        prefix = iid.split(":", 1)[0] if ":" in iid else ""
        try:
            market = Market(prefix)
        except ValueError as exc:
            raise QueryError("market is required when the instrument is unknown") from exc

    default_raw_hourly = interval == Interval.H1 and source_name != SOURCE_CANONICAL and start is None and end is None
    # Bound the default window to stored data so we do not emit decades of gaps.
    if start is None or end is None:
        if source_name == SOURCE_CANONICAL:
            stored = _scan_canonical(backend, [iid], interval=interval, market=market)
        else:
            stored = _raw_rows(backend, iid, source_name, market=market, interval=interval, start=None, end=None)
        if not stored and interval == Interval.H1:
            start = start or end or date.today()
            end = end or start
        if stored:
            days = [r.session_date for r in stored if getattr(r, "session_date", None) is not None]
            if start is None and days:
                start = min(days)
            if end is None and days:
                end = max(days)
    start = start or WIDE_START
    end = end or date.today()
    if start > end:
        raise QueryError("start must be on or before end")

    calendar = calendar_for(market)
    n_sessions = _count_sessions(calendar, start, end)
    if default_raw_hourly and n_sessions * 7 > MAX_BARS:
        # A long history cannot fit in one hourly response. Show its latest day.
        start = end
        n_sessions = _count_sessions(calendar, start, end)
    if interval == Interval.H1 and n_sessions * 7 > MAX_BARS:
        raise QueryError(f"window has too many hourly slots (max {MAX_BARS}); pass a smaller start/end")
    if n_sessions > MAX_BARS:
        raise QueryError(
            f"window has {n_sessions} sessions (max {MAX_BARS}); pass a smaller start/end"
        )

    # Hourly sources may have disjoint historical coverage, including when the
    # default canonical window is empty. Limit the scan to this instrument.
    sources = available_sources(
        backend, iid, market=market, interval=interval,
        start=None if interval == Interval.H1 else start,
        end=None if interval == Interval.H1 else end,
    )
    session_list = _session_dates(calendar, start, end)
    present: set[date] = set()
    bars: list[dict[str, Any]] = []

    if source_name == SOURCE_CANONICAL:
        records = load_canonical(
            backend,
            [iid],
            interval=interval,
            start=start,
            end=end,
            adjustment=adjustment,
            as_of=as_of,
            market=market,
        )
        for rec in records:
            day = date.fromisoformat(rec["session_date"]) if rec.get("session_date") else None
            if day is not None:
                present.add(day)
            rec.setdefault("missing_fields", [])
            bars.append(rec)
    else:
        rows = _raw_rows(
            backend, iid, source_name, market=market, interval=interval, start=start, end=end
        )
        for row in rows:
            rec = _from_raw_row(row, iid)
            day = date.fromisoformat(rec["session_date"]) if rec.get("session_date") else None
            if day is not None:
                present.add(day)
            bars.append(rec)

    bars.sort(key=lambda rec: rec.get("ts_start") or rec.get("session_date") or "")
    missing_hours = []
    ambiguous = False
    if interval == Interval.H1 and hasattr(calendar, "hourly_slots"):
        present_ts = {datetime.fromisoformat(b["ts_start"]).timestamp() for b in bars if b.get("ts_start")}
        extended = any(b.get("session") in {"pre", "post"} for b in bars)
        # Compare end times too: the opening start is shared by both grids.
        observed = []
        for bar in bars:
            if not bar.get("ts_start") or bar.get("session") in {"pre", "post"}:
                continue
            start_ts = datetime.fromisoformat(bar["ts_start"]).timestamp()
            try:
                end_ts = datetime.fromisoformat(bar["ts_end"]).timestamp() if bar.get("ts_end") else None
            except (TypeError, ValueError):
                end_ts = None
            observed.append((start_ts, end_ts))
        grids = {}
        scores = {}
        for alignment in (HourlyAlignment.SESSION_OPEN, HourlyAlignment.CLOCK_HOUR):
            slots = [slot for day in session_list for slot in calendar.hourly_slots(
                day, alignment=alignment, include_extended_hours=extended
            )]
            grids[alignment] = slots
            regular = {(slot.ts_start.timestamp(), slot.ts_end.timestamp()) for slot in slots
                       if slot.session.value == "regular"}
            starts = {slot_start for slot_start, _ in regular}
            scores[alignment] = sum(
                (start_ts in starts if end_ts is None else (start_ts, end_ts) in regular)
                for start_ts, end_ts in observed
            )
        alignment = max(scores, key=scores.get)
        grid_shapes = [{(slot.ts_start.timestamp(), slot.ts_end.timestamp(), slot.session.value)
                        for slot in slots} for slots in grids.values()]
        ambiguous = len(set(scores.values())) == 1 and grid_shapes[0] != grid_shapes[1]
        if len(grids[alignment]) > MAX_BARS:
            raise QueryError(f"window has too many hourly slots (max {MAX_BARS}); pass a smaller start/end")
        if not ambiguous:
            for slot in grids[alignment]:
                if slot.ts_start.timestamp() not in present_ts:
                    missing_hours.append({"ts_start": slot.ts_start.isoformat(), "ts_end": slot.ts_end.isoformat(), "session": slot.session.value})
    missing = [d.isoformat() for d in session_list if d not in present]
    symbol_name = _symbol_for(iid, _mappings(backend), on=end)
    if symbol_name is None and inst is not None:
        symbol_name = symbol
    return {
        "instrument_id": iid,
        "symbol": symbol_name,
        "market": market.value,
        "interval": interval.value,
        "adjustment": adjustment.value if source_name == SOURCE_CANONICAL else "raw",
        "source": source_name,
        "available_sources": sources,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "bars": bars,
        "missing_sessions": missing if interval == Interval.D1 else [],
        "missing_hours": missing_hours,
        "note": ("No canonical hourly bars are available." if interval == Interval.H1 and source_name == SOURCE_CANONICAL and not bars
                 else "Hourly alignment is unknown; missing hours are unavailable." if ambiguous else None),
    }


def list_flags(
    backend: Any,
    *,
    market: Market | None = None,
    flag: str | None = None,
    interval: Interval = Interval.D1,
    start: date | None = None,
    end: date | None = None,
    limit: int = DEFAULT_FLAG_LIMIT,
    offset: int = 0,
) -> dict[str, Any]:
    if limit < 1 or limit > MAX_FLAG_LIMIT:
        raise QueryError(f"limit must be between 1 and {MAX_FLAG_LIMIT}")
    if offset < 0:
        raise QueryError("offset must be >= 0")
    flag_name = parse_flag(flag)
    markets = [market] if market is not None else list(Market)
    mappings = _mappings(backend)
    items: list[dict[str, Any]] = []
    counts = {name: 0 for name in KNOWN_FLAGS}
    for mkt in markets:
        rows = _scan_canonical(backend, None, interval=interval, start=start, end=end, market=mkt)
        for row in rows:
            names = _flag_names(getattr(row, "flags", None))
            if not names:
                continue
            for name in names:
                if name in counts:
                    counts[name] += 1
            if flag_name is not None and flag_name not in names:
                continue
            iid = getattr(row, "instrument_id", None)
            items.append(
                {
                    "instrument_id": iid,
                    "symbol": _symbol_for(iid, mappings, on=getattr(row, "session_date", None)),
                    "market": mkt.value,
                    "session_date": _iso_date(getattr(row, "session_date", None)),
                    "ts_start": row.ts_start.isoformat() if getattr(row, "ts_start", None) else None,
                    "session": getattr(row, "session", None),
                    "flags": names,
                }
            )
    items.sort(key=lambda rec: (rec.get("session_date") or "", rec.get("instrument_id") or ""))
    total = len(items)
    return {
        "counts": counts,
        "items": items[offset : offset + limit],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


def _drop_secrets(value: Any) -> Any:
    if isinstance(value, dict):
        out = {}
        for key, item in value.items():
            name = str(key).lower()
            if any(part in name for part in _SECRET_KEY_PARTS):
                continue
            out[key] = _drop_secrets(item)
        return out
    if isinstance(value, list):
        return [_drop_secrets(item) for item in value]
    return value


def _usage_entry(key: str, usage: Any) -> dict[str, Any]:
    if hasattr(usage, "provider"):
        entry = {
            "id": key,
            "provider": usage.provider,
            "used": dict(getattr(usage, "used", {}) or {}),
            "remaining": dict(getattr(usage, "remaining", {}) or {}),
            "penalized_until": (
                usage.penalized_until.isoformat() if getattr(usage, "penalized_until", None) else None
            ),
        }
        return _drop_secrets(entry)
    if isinstance(usage, dict):
        entry = dict(usage)
        entry.setdefault("id", key)
        entry.setdefault("provider", key)
        return _drop_secrets(entry)
    return _drop_secrets({"id": key, "provider": key, "used": {}, "remaining": {}})


def quota_snapshot(gate: Any | None) -> dict[str, Any]:
    """Return a sanitized usage snapshot, or an empty one when no gate is open."""
    if gate is None:
        return {"available": False, "providers": []}
    raw = gate.snapshot() if hasattr(gate, "snapshot") else {}
    if isinstance(raw, dict):
        items = [_usage_entry(str(key), usage) for key, usage in raw.items()]
    elif isinstance(raw, list):
        items = [_usage_entry(str(i), usage) for i, usage in enumerate(raw)]
    else:
        items = []
    return {"available": True, "providers": items}


def open_readonly_quota_gate(settings: Any, data_dir: Path | str) -> Any | None:
    """Open an existing quota store for snapshots only. Never creates ``quota.db``."""
    root = Path(data_dir)
    kind = str(getattr(settings, "postgres_quota", "sqlite") or "sqlite").strip().lower()
    if kind == "postgres":
        try:
            from openbars.quota.postgres import open_configured_quota_gate

            return open_configured_quota_gate(settings, data_dir=root, policies=None)
        except Exception:
            return None
    path = root / "quota.db"
    if not path.is_file():
        return None
    try:
        from openbars.quota.persistent import SqliteQuotaGate

        return SqliteQuotaGate(path, policies=None)
    except Exception:
        return None
