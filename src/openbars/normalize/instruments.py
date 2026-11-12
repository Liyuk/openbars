"""Instrument master: stable ids, time-bounded symbol mappings, point-in-time resolution.

Known failure mode this module exists for: a ticker is recycled by a different company, and a
ticker-keyed history silently attaches the new company's bars to the previous company's
instrument. Bars therefore carry an ``instrument_id`` that is never reused, and a symbol only
means an instrument inside a ``SymbolMapping`` window.

Resolution rules (``resolve_symbol``):

1. Only mappings of the requested market count; ``valid_to`` is inclusive.
2. A provider-specific mapping beats a generic (``"*"``) one. More than one distinct instrument
   at the winning precedence is ambiguity -> unresolved.
3. If the date is covered by no window, the symbol may still resolve through the *unique-history
   fallback*: the symbol has been bound to exactly one instrument, ever, and the date is not
   after that instrument's last closed window (so a first-observed date that is later than the
   true listing still resolves, but a recycled ticker after delisting does not). If the symbol
   was ever bound to two or more instruments there is never a fallback.
4. Anything else is unresolved with a reason code; the caller queues it, it is never guessed.

Stable ids (``new_instrument_id`` / ``figi_instrument_id``): ``<market>:eq:<26-char ULID-shaped
id>``. With a share-class FIGI the id is the first 128 bits of SHA-256 over
``share_class_figi:<FIGI>`` in Crockford base32, so every process derives the same id for the
same security; otherwise it is a random ULID. Ids are looked up by external id before being
minted and never deleted, so an id is never handed to a second security.
"""

from __future__ import annotations

import dataclasses
import hashlib
import secrets
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from typing import Callable, Iterable, Mapping, Protocol, Sequence, runtime_checkable

from openbars.core.models import Instrument, Market, RawBar, SymbolMapping

__all__ = [
    "AssignedBars",
    "InstrumentMaster",
    "InstrumentStore",
    "MemoryInstrumentMaster",
    "SymbolResolution",
    "UnresolvedRecord",
    "assign_instrument",
    "assign_instruments",
    "figi_instrument_id",
    "new_instrument_id",
    "resolve_symbol",
]

_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

# Reason codes (bars and listing rows share one vocabulary).
NO_MAPPING = "no_mapping"
AMBIGUOUS_MAPPING = "ambiguous_mapping"
SYMBOL_REUSED_UNCOVERED = "symbol_reused_uncovered_date"
OUTSIDE_MAPPING_WINDOW = "outside_mapping_window"


class InstrumentMaster(Protocol):
    def resolve(self, symbol: str, *, on: date, provider: str, market: Market) -> str | None:
        """Return instrument_id the symbol meant on that date, or None if unknown/ambiguous."""
        ...

    def upsert(self, instruments: Iterable[Instrument], mappings: Iterable[SymbolMapping]) -> None: ...


@dataclass(frozen=True)
class UnresolvedRecord:
    """A bar or listing row that could not be mapped to exactly one instrument."""

    kind: str  # "bar" | "listing"
    reason_code: str
    symbol: str
    provider: str
    session_date: date | None = None
    market: Market = Market.US
    detail: str | None = None

    @property
    def key(self) -> tuple:
        """Bars queue once per (provider, symbol, day); a listing row once per (provider,
        symbol), so a daily snapshot that keeps failing does not grow the queue."""
        day = self.session_date if self.kind == "bar" else None
        return (self.kind, self.provider, Market(self.market).value, self.symbol, day)


@runtime_checkable
class InstrumentStore(InstrumentMaster, Protocol):
    """What the directory needs from a persistence backend (memory and Parquet implement it)."""

    def instruments(self) -> list[Instrument]: ...

    def mappings(self) -> list[SymbolMapping]: ...

    def remove_mappings(self, mappings: Iterable[SymbolMapping]) -> None: ...

    def record_unresolved(self, records: Iterable[UnresolvedRecord]) -> None: ...

    def unresolved(self, kind: str | None = None) -> list[UnresolvedRecord]: ...

    def unresolved_count(self, kind: str | None = None) -> int: ...

    def discard_unresolved(self, records: Iterable[UnresolvedRecord]) -> None: ...

    def observe(self, observations: Iterable[tuple[str, str, str, date]]) -> None:
        """Record ``(instrument_id, symbol, provider, on)`` sightings (keeps the latest date)."""
        ...

    def observations(self) -> list[tuple[str, str, str, date]]: ...

    def set_upstream_id(self, instrument_id: str, upstream_id: int) -> None: ...

    def upstream_ids(self) -> dict[str, int]: ...


# --- stable ids -------------------------------------------------------------------------------


def _crockford128(value: int) -> str:
    chars = []
    for _ in range(26):
        chars.append(_CROCKFORD[value & 31])
        value >>= 5
    return "".join(reversed(chars))


def _namespace(market: Market) -> str:
    return f"{Market(market).value}:eq"


def new_instrument_id(
    market: Market = Market.US,
    *,
    now_ms: Callable[[], int] | None = None,
    randbits: Callable[[int], int] | None = None,
) -> str:
    """A fresh ULID-shaped id: 48-bit millisecond time + 80 random bits."""
    ms = (now_ms or (lambda: int(time.time() * 1000)))() & ((1 << 48) - 1)
    rnd = (randbits or secrets.randbits)(80)
    return f"{_namespace(market)}:{_crockford128((ms << 80) | rnd)}"


def figi_instrument_id(share_class_figi: str, market: Market = Market.US) -> str:
    """Deterministic id derived from a share-class FIGI (see module docstring)."""
    digest = hashlib.sha256(f"share_class_figi:{share_class_figi.strip().upper()}".encode()).digest()
    return f"{_namespace(market)}:{_crockford128(int.from_bytes(digest[:16], 'big'))}"


# --- point-in-time resolution -----------------------------------------------------------------


@dataclass(frozen=True)
class SymbolResolution:
    instrument_id: str | None
    reason_code: str | None = None  # set when unresolved
    via: str | None = None  # "window" | "unique_history" when resolved


def resolve_symbol(
    mappings: Sequence[SymbolMapping],
    market_of: Callable[[str], Market | None],
    *,
    on: date,
    provider: str,
    market: Market,
    unique_history_fallback: bool = True,
) -> SymbolResolution:
    """Resolve one symbol's mappings at ``on`` (rules in the module docstring)."""
    market = Market(market)
    candidates = [m for m in mappings if market_of(m.instrument_id) is market]
    applicable = [m for m in candidates if m.provider in (provider, "*")]
    if not applicable:
        return SymbolResolution(None, NO_MAPPING)
    specific: set[str] = set()
    generic: set[str] = set()
    for m in applicable:
        if m.valid_from > on or (m.valid_to is not None and on > m.valid_to):
            continue
        (specific if m.provider == provider else generic).add(m.instrument_id)
    for ids in (specific, generic):
        if ids:
            if len(ids) == 1:
                return SymbolResolution(next(iter(ids)), via="window")
            return SymbolResolution(None, AMBIGUOUS_MAPPING)
    ever = {m.instrument_id for m in applicable}
    if len(ever) > 1:
        return SymbolResolution(None, SYMBOL_REUSED_UNCOVERED)
    if unique_history_fallback:
        closed_before = all(m.valid_to is not None and m.valid_to < on for m in applicable)
        if not closed_before:
            return SymbolResolution(next(iter(ever)), via="unique_history")
    return SymbolResolution(None, OUTSIDE_MAPPING_WINDOW)


class _ResolveMixin:
    """Shared ``resolve`` over a ``(instruments, mappings-by-symbol)`` view."""

    def _resolution_view(self) -> tuple[Mapping[str, Instrument], Mapping[str, Sequence[SymbolMapping]]]:
        raise NotImplementedError

    def resolve_detailed(
        self, symbol: str, *, on: date, provider: str, market: Market, unique_history_fallback: bool = True
    ) -> SymbolResolution:
        instruments, by_symbol = self._resolution_view()
        return resolve_symbol(
            by_symbol.get(symbol, ()),
            lambda iid: instruments[iid].market if iid in instruments else None,
            on=on,
            provider=provider,
            market=market,
            unique_history_fallback=unique_history_fallback,
        )

    def resolve(self, symbol: str, *, on: date, provider: str, market: Market) -> str | None:
        """Instrument a symbol meant on ``on`` (``valid_to`` inclusive), else ``None``."""
        return self.resolve_detailed(symbol, on=on, provider=provider, market=market).instrument_id


# --- in-memory store --------------------------------------------------------------------------


def _mapping_key(m: SymbolMapping) -> tuple:
    return (m.instrument_id, m.provider, m.symbol, m.valid_from)


class MemoryInstrumentMaster(_ResolveMixin):
    """Non-persistent ``InstrumentStore`` (tests, dry runs, in-process caches)."""

    def __init__(self) -> None:
        self._instruments: dict[str, Instrument] = {}
        self._mappings: dict[tuple, SymbolMapping] = {}
        self._unresolved: dict[tuple, UnresolvedRecord] = {}
        self._observed: dict[tuple[str, str, str], date] = {}
        self._upstream: dict[str, int] = {}

    def _resolution_view(self):
        by_symbol: dict[str, list[SymbolMapping]] = defaultdict(list)
        for m in self._mappings.values():
            by_symbol[m.symbol].append(m)
        return self._instruments, by_symbol

    def upsert(self, instruments: Iterable[Instrument], mappings: Iterable[SymbolMapping]) -> None:
        for i in instruments:
            self._instruments[i.instrument_id] = i
        for m in mappings:
            self._mappings[_mapping_key(m)] = m

    def remove_mappings(self, mappings: Iterable[SymbolMapping]) -> None:
        for m in mappings:
            self._mappings.pop(_mapping_key(m), None)

    def get(self, instrument_id: str) -> Instrument | None:
        return self._instruments.get(instrument_id)

    def instruments(self) -> list[Instrument]:
        return list(self._instruments.values())

    def mappings(self) -> list[SymbolMapping]:
        return list(self._mappings.values())

    def record_unresolved(self, records: Iterable[UnresolvedRecord]) -> None:
        for r in records:
            self._unresolved[r.key] = r

    def unresolved(self, kind: str | None = None) -> list[UnresolvedRecord]:
        return [r for r in self._unresolved.values() if kind is None or r.kind == kind]

    def unresolved_count(self, kind: str | None = None) -> int:
        return len(self.unresolved(kind))

    def discard_unresolved(self, records: Iterable[UnresolvedRecord]) -> None:
        for r in records:
            self._unresolved.pop(r.key, None)

    def observe(self, observations: Iterable[tuple[str, str, str, date]]) -> None:
        for instrument_id, symbol, provider, on in observations:
            k = (instrument_id, symbol, provider)
            if k not in self._observed or self._observed[k] < on:
                self._observed[k] = on

    def observations(self) -> list[tuple[str, str, str, date]]:
        return [(*k, v) for k, v in self._observed.items()]

    def set_upstream_id(self, instrument_id: str, upstream_id: int) -> None:
        _check_upstream(self._upstream, instrument_id, upstream_id)
        self._upstream[instrument_id] = int(upstream_id)

    def upstream_ids(self) -> dict[str, int]:
        return dict(self._upstream)


def _check_upstream(existing: Mapping[str, int], instrument_id: str, upstream_id: int) -> None:
    """An upstream integer id links to one instrument and an instrument to one upstream id."""
    if existing.get(instrument_id, upstream_id) != upstream_id:
        raise ValueError(f"{instrument_id} is already linked to a different upstream id")
    owner = next((k for k, v in existing.items() if v == upstream_id and k != instrument_id), None)
    if owner is not None:
        raise ValueError(f"upstream id {upstream_id} is already linked to {owner}")


# --- raw bar assignment -----------------------------------------------------------------------


@dataclass(frozen=True)
class AssignedBars:
    resolved: tuple[RawBar, ...]
    unresolved: tuple[RawBar, ...]  # instrument_id is None; never write these as canonical
    records: tuple[UnresolvedRecord, ...]


def _assign(
    raw: RawBar, master: InstrumentMaster, market: Market, on: date | None
) -> tuple[RawBar, UnresolvedRecord | None]:
    if raw.instrument_id:
        return raw, None
    day = on or raw.session_date or raw.ts_start.date()
    detailed = getattr(master, "resolve_detailed", None)
    if detailed is not None:
        res = detailed(raw.symbol_as_returned, on=day, provider=raw.provider, market=market)
        found, reason = res.instrument_id, res.reason_code
    else:
        found, reason = master.resolve(raw.symbol_as_returned, on=day, provider=raw.provider, market=market), None
    if found is None:
        return raw, UnresolvedRecord(
            "bar", reason or "unresolved_symbol", raw.symbol_as_returned, raw.provider, day, Market(market)
        )
    return dataclasses.replace(raw, instrument_id=found, session_date=raw.session_date or day), None


def assign_instrument(
    raw: RawBar,
    master: InstrumentMaster,
    *,
    market: Market = Market.US,
    session_date: date | None = None,
    record: bool = True,
) -> RawBar:
    """Fill ``instrument_id`` from the symbol as returned and the bar's session date.

    An unresolvable or ambiguous symbol leaves ``instrument_id`` as ``None`` and (with
    ``record=True`` and a master that has a queue) is added to the unresolved queue.
    Bars with ``instrument_id is None`` must not be written as canonical.
    """
    bar, miss = _assign(raw, master, market, session_date)
    if miss is not None and record:
        sink = getattr(master, "record_unresolved", None)
        if sink is not None:
            sink([miss])
    return bar


def assign_instruments(
    bars: Iterable[RawBar], master: InstrumentMaster, *, market: Market = Market.US, record: bool = True
) -> AssignedBars:
    """Batch ``assign_instrument``: split into resolved bars and queued unresolved ones."""
    resolved: list[RawBar] = []
    missed: list[RawBar] = []
    records: list[UnresolvedRecord] = []
    for raw in bars:
        bar, miss = _assign(raw, master, market, None)
        if miss is None:
            resolved.append(bar)
        else:
            missed.append(bar)
            records.append(miss)
    sink = getattr(master, "record_unresolved", None)
    if record and records and sink is not None:
        sink(records)
    return AssignedBars(tuple(resolved), tuple(missed), tuple(records))
