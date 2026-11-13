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
from datetime import date, timedelta
from typing import Callable, Iterable, Mapping, Protocol, Sequence, runtime_checkable

from openbars.core.models import CorporateAction, CorporateActionKind, Instrument, Market, RawBar, SymbolMapping

__all__ = [
    "AssignedActions",
    "AssignedBars",
    "CORPORATE_ACTION_KIND",
    "InstrumentMaster",
    "IngestSummary",
    "InstrumentStore",
    "MemoryInstrumentMaster",
    "SymbolResolution",
    "UnresolvedRecord",
    "assign_corporate_actions",
    "assign_instrument",
    "assign_instruments",
    "ingest_corporate_actions",
    "figi_instrument_id",
    "new_instrument_id",
    "resolve_symbol",
    "resolve_symbol_windows",
]

_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

# Reason codes (bars and listing rows share one vocabulary).
NO_MAPPING = "no_mapping"
AMBIGUOUS_MAPPING = "ambiguous_mapping"
SYMBOL_REUSED_UNCOVERED = "symbol_reused_uncovered_date"
OUTSIDE_MAPPING_WINDOW = "outside_mapping_window"

CORPORATE_ACTION_KIND = "corporate_action"  # ``UnresolvedRecord.kind`` of a ticker-keyed action


class InstrumentMaster(Protocol):
    def resolve(self, symbol: str, *, on: date, provider: str, market: Market) -> str | None:
        """Return instrument_id the symbol meant on that date, or None if unknown/ambiguous."""
        ...

    def upsert(self, instruments: Iterable[Instrument], mappings: Iterable[SymbolMapping]) -> None: ...


@dataclass(frozen=True)
class UnresolvedRecord:
    """A bar, listing row or corporate action that could not be mapped to exactly one instrument."""

    kind: str  # "bar" | "listing" | "corporate_action"
    reason_code: str
    symbol: str
    provider: str
    session_date: date | None = None
    market: Market = Market.US
    detail: str | None = None

    @property
    def key(self) -> tuple:
        """Bars queue once per (provider, symbol, day); a listing row once per (provider,
        symbol), so a daily snapshot that keeps failing does not grow the queue; a corporate
        action once per (provider, symbol, ex-date, action kind) -- ``session_date`` is the
        ex-date and ``detail`` the action kind (``split`` / ``dividend`` ...)."""
        is_action = self.kind == CORPORATE_ACTION_KIND
        day = self.session_date if self.kind == "bar" or is_action else None
        return (
            self.kind, self.provider, Market(self.market).value, self.symbol, day,
            (self.detail or "") if is_action else None,
        )


@runtime_checkable
class InstrumentStore(InstrumentMaster, Protocol):
    """What the directory needs from a persistence backend (memory and Parquet implement it)."""

    def instruments(self) -> list[Instrument]: ...

    def mappings(self, provider: str | None = None) -> list[SymbolMapping]:
        """All mappings, or those visible to ``provider`` (exact match or ``"*"``)."""
        ...

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


def resolve_symbol_windows(
    mappings: Sequence[SymbolMapping],
    market_of: Callable[[str], Market | None],
    *,
    start: date | None,
    end: date | None,
    provider: str,
    market: Market,
) -> list[tuple[str, date | None, date | None]]:
    """Which instrument a symbol meant on each day of ``[start, end]`` (``None`` = unbounded).

    Returns ``(instrument_id, from, to)`` segments, inclusive, clipped to the query window
    (a ``None`` bound stays ``None`` only where the request itself was unbounded). Days on
    which ``resolve_symbol`` would be unresolved (gap between holders, ambiguity, after a
    delisting with reuse ...) belong to no segment. The result is exactly
    ``resolve_symbol(..., on=d)`` evaluated for every day, computed per interval between the
    dates where the answer can change (window edges), so the unique-history fallback and the
    reuse rules are the same as for a single-day ``resolve``.
    """
    lo, hi = start or date.min, end or date.max
    if lo > hi:
        return []
    cuts = {lo}
    for m in mappings:
        for d in (m.valid_from, None if m.valid_to is None else m.valid_to + timedelta(days=1)):
            if d is not None and lo < d <= hi:
                cuts.add(d)
    points = sorted(cuts)
    out: list[tuple[str, date, date]] = []
    for i, first in enumerate(points):
        last = points[i + 1] - timedelta(days=1) if i + 1 < len(points) else hi
        found = resolve_symbol(mappings, market_of, on=first, provider=provider, market=market).instrument_id
        if found is None:
            continue
        if out and out[-1][0] == found and out[-1][2] + timedelta(days=1) == first:
            out[-1] = (found, out[-1][1], last)
        else:
            out.append((found, first, last))
    return [
        (iid, None if start is None and a == date.min else a, None if end is None and b == date.max else b)
        for iid, a, b in out
    ]


class _ResolveMixin:
    """Shared ``resolve`` over a ``(instruments, mappings-by-symbol)`` view."""

    def _resolution_view(
        self, symbol: str | None = None
    ) -> tuple[Mapping[str, Instrument], Mapping[str, Sequence[SymbolMapping]]]:
        """``(instruments by id, mappings by symbol)``; a backend that queries lazily may
        load only ``symbol``'s mappings (and the instruments they point to) when it is given."""
        raise NotImplementedError

    def resolve_detailed(
        self, symbol: str, *, on: date, provider: str, market: Market, unique_history_fallback: bool = True
    ) -> SymbolResolution:
        instruments, by_symbol = self._resolution_view(symbol)
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

    def resolve_windows(
        self,
        symbol: str,
        *,
        start: date | None,
        end: date | None,
        provider: str = "*",
        market: Market,
    ) -> list[tuple[str, date | None, date | None]]:
        """Point-in-time ``resolve`` across a window: ``(instrument_id, from, to)`` segments."""
        instruments, by_symbol = self._resolution_view(symbol)
        return resolve_symbol_windows(
            by_symbol.get(symbol, ()),
            lambda iid: instruments[iid].market if iid in instruments else None,
            start=start,
            end=end,
            provider=provider,
            market=market,
        )


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
        self._actions: dict[tuple, CorporateAction] = {}

    def _resolution_view(self, symbol: str | None = None):
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

    def upsert_corporate_actions(self, actions: Iterable[CorporateAction]) -> None:
        for a in actions:
            self._actions[(a.instrument_id, a.ex_date, CorporateActionKind(a.kind).value, a.source)] = a

    def corporate_actions(self, instrument_ids: Sequence[str] | None = None) -> list[CorporateAction]:
        wanted = None if instrument_ids is None else set(instrument_ids)
        return [a for k, a in sorted(self._actions.items()) if wanted is None or a.instrument_id in wanted]

    def get(self, instrument_id: str) -> Instrument | None:
        return self._instruments.get(instrument_id)

    def instruments(self) -> list[Instrument]:
        return list(self._instruments.values())

    def mappings(self, provider: str | None = None) -> list[SymbolMapping]:
        return select_mappings(self._mappings.values(), provider)

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


def select_mappings(mappings: Iterable[SymbolMapping], provider: str | None = None) -> list[SymbolMapping]:
    """If ``provider`` is set, keep rows whose provider is that name or ``"*"``."""
    rows = list(mappings)
    if provider is None:
        return rows
    return [m for m in rows if m.provider in (provider, "*")]


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


# --- corporate-action assignment (ingest boundary) -----------------------------------------


@dataclass(frozen=True)
class AssignedActions:
    """Corporate actions split into those with a stable ``instrument_id`` and those without."""

    resolved: tuple[CorporateAction, ...]  # instrument_id is a known stable id
    unresolved: tuple[CorporateAction, ...]  # instrument_id still the provider's ticker; never write
    records: tuple[UnresolvedRecord, ...]  # one per unresolved action (queue rows)
    mapped: tuple[UnresolvedRecord, ...] = ()  # queue rows the ticker-mapped actions would have had

    @property
    def reasons(self) -> dict[str, int]:
        out: dict[str, int] = defaultdict(int)
        for r in self.records:
            out[r.reason_code] += 1
        return dict(sorted(out.items()))


@dataclass(frozen=True)
class IngestSummary:
    resolved: int  # written to the corporate-action table
    unresolved: int  # queued, not written
    already_mapped: int  # resolved actions whose id was already a known instrument
    mapped_from_ticker: int  # resolved actions mapped through ``resolve`` (ticker + ex_date)
    reasons: dict[str, int]  # unresolved reason code -> count
    cleared: int = 0  # previously queued actions that resolved this time and left the queue


def _assign_action(
    action: CorporateAction, master: InstrumentMaster, market: Market
) -> tuple[CorporateAction, UnresolvedRecord | None, UnresolvedRecord | None]:
    """``(action, queue row if unresolved, queue row it would have had if mapped from a ticker)``."""
    known = getattr(master, "get", None)
    if known is not None and known(action.instrument_id) is not None:
        return action, None, None
    detailed = getattr(master, "resolve_detailed", None)
    kwargs = dict(on=action.ex_date, provider=action.source, market=market)
    if detailed is not None:
        res = detailed(action.instrument_id, **kwargs)
        found, reason = res.instrument_id, res.reason_code
    else:
        found, reason = master.resolve(action.instrument_id, **kwargs), None
    record = UnresolvedRecord(
        CORPORATE_ACTION_KIND, reason or "unresolved_symbol", action.instrument_id, action.source,
        action.ex_date, Market(market), CorporateActionKind(action.kind).value,
    )
    if found is None:
        return action, record, None
    return dataclasses.replace(action, instrument_id=found), None, record


def assign_corporate_actions(
    actions: Iterable[CorporateAction],
    master: InstrumentMaster,
    *,
    market: Market = Market.US,
    record: bool = True,
) -> AssignedActions:
    """Map provider-returned corporate actions to stable instrument ids (the ingest step).

    Providers return the ticker in ``CorporateAction.instrument_id`` (as returned). Here an id
    that ``master.get`` already knows is kept as is; anything else is a ticker resolved on the
    action's ``ex_date`` for ``provider=action.source`` with the same rules as bars (a
    provider-specific mapping beats ``"*"``, ambiguity is unresolved, the unique-history
    fallback applies only where it would for that date). Unresolved actions keep the ticker,
    are never meant to be written, and (``record=True``, master with a queue) are queued as
    ``kind="corporate_action"`` rows, one per (provider, symbol, ex-date, action kind).
    """
    resolved: list[CorporateAction] = []
    missed: list[CorporateAction] = []
    records: list[UnresolvedRecord] = []
    mapped: list[UnresolvedRecord] = []
    for a in actions:
        out, miss, was_ticker = _assign_action(a, master, market)
        if miss is None:
            resolved.append(out)
            if was_ticker is not None:
                mapped.append(was_ticker)
        else:
            missed.append(a)
            records.append(miss)
    sink = getattr(master, "record_unresolved", None)
    if record and records and sink is not None:
        sink(records)
    return AssignedActions(tuple(resolved), tuple(missed), tuple(records), tuple(mapped))


def ingest_corporate_actions(
    actions: Iterable[CorporateAction], master: InstrumentMaster, *, market: Market = Market.US
) -> IngestSummary:
    """The call ingest paths use: map, upsert the resolved actions, queue the rest.

    ``master`` needs ``upsert_corporate_actions`` (every master in this package has it).
    Re-ingesting an unresolved event does not grow the queue (the queue key includes the
    ex-date and action kind); an event that resolves later is written and removed from the
    queue. Returns counts and reason codes; nothing unresolved reaches the action table.
    """
    assigned = assign_corporate_actions(actions, master, market=market)
    if assigned.resolved:
        master.upsert_corporate_actions(assigned.resolved)  # type: ignore[attr-defined]
    cleared = 0
    queue, drop = getattr(master, "unresolved", None), getattr(master, "discard_unresolved", None)
    if queue is not None and drop is not None and assigned.mapped:
        queued = {r.key for r in queue(CORPORATE_ACTION_KIND)}
        done = [r for r in assigned.mapped if r.key in queued]
        if done:
            drop(done)
            cleared = len(done)
    return IngestSummary(
        resolved=len(assigned.resolved),
        unresolved=len(assigned.unresolved),
        already_mapped=len(assigned.resolved) - len(assigned.mapped),
        mapped_from_ticker=len(assigned.mapped),
        reasons=assigned.reasons,
        cleared=cleared,
    )
