"""Crypto trading-pair master data: one instrument per (venue, base, quote).

Identity
--------
A pair's id is derived, never minted::

    crypto:spot:<venue>:<BASE>-<QUOTE>        e.g. crypto:spot:binance:TEST-USDT

* ``venue`` is a lowercase ``[a-z0-9]+`` slug (``BINANCE`` / ``Binance`` -> ``binance``).
* ``base`` / ``quote`` are uppercase ASCII asset codes ``[A-Z0-9]+`` (no ``-``, no ``:``).
* The same (venue, base, quote) always yields the same id, so re-listing a pair reuses its
  instrument. A different venue, base or quote is a *different* id: the same base/quote on two
  venues are two instruments (their prices are not interchangeable), and TEST-USDT / TEST-USDC
  on one venue are two instruments. An existing id is never "fixed" or rewritten.
* Not ULID / FIGI based; ``new_instrument_id`` and ``_namespace`` (``<market>:eq``) are US-only.

Stored record
-------------
``Instrument`` keeps its schema (no new columns). A pair is stored as:

* ``market=CRYPTO``, ``asset_class=CRYPTO_SPOT``
* ``exchange`` = venue slug upper-cased (``BINANCE``, ``COINBASE``)
* ``currency`` = the quote asset (``USDT`` is not an ISO currency; ``Instrument.currency`` has no
  length restriction)
* ``external_ids``: ``base``, ``quote``, ``venue`` (always present, stable key names) and
  ``provider_symbol`` (venue-native symbol of the last provider that listed it; informational)

Symbol mappings
---------------
Only provider-specific ``SymbolMapping(provider, symbol)`` rows are written (``binance`` +
``TESTUSDT``, ``coinbase`` + ``TEST-USDT``). A generic ``"*"`` mapping of ``BASE-QUOTE`` is never
written: two venues share that display form and it would be ambiguous or merge them.

Ingest is batch-tolerant: invalid rows and symbol conflicts go to ``unresolved`` (and the
master's queue when it has one); they never raise and never merge identities.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field, replace
from datetime import date, timedelta
from typing import Sequence

from openbars.core.models import AssetClass, Instrument, Market, SymbolMapping
from openbars.normalize.instruments import InstrumentMaster, UnresolvedRecord
from openbars.providers.base import ProviderInstrument

__all__ = [
    "CryptoPair",
    "CryptoPairIngestSummary",
    "CryptoPairListing",
    "EXTERNAL_ID_BASE",
    "EXTERNAL_ID_PROVIDER_SYMBOL",
    "EXTERNAL_ID_QUOTE",
    "EXTERNAL_ID_VENUE",
    "INVALID_PAIR",
    "SYMBOL_CONFLICT",
    "build_crypto_instrument",
    "crypto_instrument_id",
    "ingest_crypto_pairs",
    "listing_from_provider_instrument",
    "pair_assets",
    "pair_venue",
    "parse_crypto_instrument_id",
]

ID_PREFIX = "crypto:spot:"

# ``Instrument.external_ids`` keys (stable).
EXTERNAL_ID_BASE = "base"
EXTERNAL_ID_QUOTE = "quote"
EXTERNAL_ID_VENUE = "venue"
EXTERNAL_ID_PROVIDER_SYMBOL = "provider_symbol"

# Unresolved reason codes.
INVALID_PAIR = "invalid_crypto_pair"
SYMBOL_CONFLICT = "symbol_conflict"

_VENUE_RE = re.compile(r"[a-z0-9]+")
_ASSET_RE = re.compile(r"[A-Z0-9]+")
_ID_RE = re.compile(r"crypto:spot:([a-z0-9]+):([A-Z0-9]+)-([A-Z0-9]+)")
_DAY = timedelta(days=1)


def _venue(value: str) -> str:
    slug = str(value).strip().lower()
    if not slug.isascii() or not _VENUE_RE.fullmatch(slug):
        raise ValueError(f"invalid venue {value!r}: expected a [a-z0-9]+ slug")
    return slug


def _asset(value: str, what: str) -> str:
    code = str(value).strip()
    if not code.isascii() or not _ASSET_RE.fullmatch(code.upper()):
        raise ValueError(f"invalid {what} asset code {value!r}: expected letters/digits only")
    return code.upper()


@dataclass(frozen=True)
class CryptoPair:
    """Identity of one spot pair on one venue; inputs are normalized and validated."""

    venue: str
    base: str
    quote: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "venue", _venue(self.venue))
        object.__setattr__(self, "base", _asset(self.base, "base"))
        object.__setattr__(self, "quote", _asset(self.quote, "quote"))

    @property
    def instrument_id(self) -> str:
        return f"{ID_PREFIX}{self.venue}:{self.base}-{self.quote}"

    @property
    def display(self) -> str:
        return f"{self.base}-{self.quote}"

    @property
    def exchange(self) -> str:
        return self.venue.upper()

    @classmethod
    def parse(cls, instrument_id: str) -> "CryptoPair":
        """Read (venue, base, quote) back from an id."""
        m = _ID_RE.fullmatch(instrument_id)
        if m is None:
            raise ValueError(f"not a crypto pair instrument id: {instrument_id!r}")
        return cls(*m.groups())

    @classmethod
    def from_instrument(cls, instrument: Instrument) -> "CryptoPair":
        """Read the pair from ``external_ids`` (``base`` / ``quote`` / ``venue``); ids that
        parse must agree with them. Falls back to the id when the keys are absent."""
        ext = instrument.external_ids
        keys = (EXTERNAL_ID_VENUE, EXTERNAL_ID_BASE, EXTERNAL_ID_QUOTE)
        if all(ext.get(k) for k in keys):
            pair = cls(*(ext[k] for k in keys))
            if _ID_RE.fullmatch(instrument.instrument_id) and pair.instrument_id != instrument.instrument_id:
                raise ValueError(f"{instrument.instrument_id!r} disagrees with its external_ids")
            return pair
        if any(ext.get(k) for k in keys):
            raise ValueError(f"{instrument.instrument_id!r} has partial pair external_ids")
        return cls.parse(instrument.instrument_id)


@dataclass(frozen=True)
class CryptoPairListing:
    """One venue/provider listing row. Not validated on construction; ingest queues bad rows."""

    venue: str
    base: str
    quote: str
    provider: str  # e.g. "binance"
    symbol: str  # venue-native, e.g. "TESTUSDT" or "TEST-USDT"
    name: str | None = None
    active: bool = True
    listed_on: date | None = None
    delisted_on: date | None = None

    @property
    def pair(self) -> CryptoPair:
        return CryptoPair(self.venue, self.base, self.quote)


@dataclass(frozen=True)
class CryptoPairIngestSummary:
    created: int  # new instruments
    reused: int  # listings whose instrument already existed (master or earlier in the batch)
    unresolved: int  # rows not applied
    delisted: int  # instruments newly marked delisted / mappings closed
    reasons: dict[str, int] = field(default_factory=dict)  # unresolved reason code -> count


def crypto_instrument_id(venue: str, base: str, quote: str) -> str:
    """``crypto:spot:<venue>:<BASE>-<QUOTE>``; raises ``ValueError`` on an illegal part."""
    return CryptoPair(venue, base, quote).instrument_id


def parse_crypto_instrument_id(instrument_id: str) -> CryptoPair:
    return CryptoPair.parse(instrument_id)


def listing_from_provider_instrument(
    row: ProviderInstrument, *, provider: str, venue: str | None = None
) -> CryptoPairListing:
    """Turn a provider's ``ProviderInstrument`` into a ``CryptoPairListing``.

    ``base_asset`` / ``quote_asset`` are required (``ValueError`` otherwise). ``venue`` defaults to
    ``row.exchange``; one of the two must be given. Codes are not validated here: ingest queues
    invalid rows. Not called by the orchestrator.
    """
    base = (row.base_asset or "").strip()
    quote = (row.quote_asset or "").strip()
    if not base or not quote:
        raise ValueError(f"provider instrument {row.symbol!r} has no base_asset/quote_asset")
    resolved = (venue or row.exchange or "").strip()
    if not resolved:
        raise ValueError(f"provider instrument {row.symbol!r} has no venue (pass venue=)")
    return CryptoPairListing(
        venue=resolved,
        base=base,
        quote=quote,
        provider=provider,
        symbol=row.symbol,
        name=row.name,
        active=row.active,
    )


def pair_assets(instrument: Instrument) -> tuple[str, str]:
    """``(base, quote)`` of a stored pair instrument."""
    pair = CryptoPair.from_instrument(instrument)
    return pair.base, pair.quote


def pair_venue(instrument: Instrument) -> str:
    """Venue slug (``binance``) of a stored pair instrument."""
    return CryptoPair.from_instrument(instrument).venue


def build_crypto_instrument(
    pair: CryptoPair,
    *,
    name: str | None = None,
    listed_on: date | None = None,
    delisted_on: date | None = None,
    provider_symbol: str | None = None,
) -> Instrument:
    ext = {
        EXTERNAL_ID_BASE: pair.base,
        EXTERNAL_ID_QUOTE: pair.quote,
        EXTERNAL_ID_VENUE: pair.venue,
    }
    if provider_symbol:
        ext[EXTERNAL_ID_PROVIDER_SYMBOL] = provider_symbol
    return Instrument(
        instrument_id=pair.instrument_id,
        market=Market.CRYPTO,
        asset_class=AssetClass.CRYPTO_SPOT,
        currency=pair.quote,
        name=name,
        exchange=pair.exchange,
        listed_on=listed_on,
        delisted_on=delisted_on,
        external_ids=ext,
    )


# --- ingest -----------------------------------------------------------------------------------


def _earlier(a: date | None, b: date | None) -> date | None:
    return a if b is None else b if a is None else min(a, b)


class _Ingest:
    def __init__(self, master: InstrumentMaster, as_of: date) -> None:
        get = getattr(master, "get", None)
        mappings = getattr(master, "mappings", None)
        if get is None or mappings is None:
            raise TypeError("ingest_crypto_pairs needs a master with get() and mappings()")
        self.master, self.get, self.as_of = master, get, as_of
        self.instruments: dict[str, Instrument] = {}
        self.dirty_instruments: dict[str, Instrument] = {}
        # crypto-pair mappings by (provider, symbol), keyed for upsert
        self.by_symbol: dict[tuple[str, str], dict[tuple, SymbolMapping]] = {}
        self.dirty_maps: dict[tuple, SymbolMapping] = {}
        for m in mappings():
            if m.instrument_id.startswith(ID_PREFIX):
                self._index(m)
        self.claims: dict[tuple[str, str], str] = {}
        self.created = self.reused = self.delisted = 0
        self.unresolved: list[UnresolvedRecord] = []

    @staticmethod
    def _key(m: SymbolMapping) -> tuple:
        return (m.instrument_id, m.provider, m.symbol, m.valid_from)

    def _instrument(self, iid: str) -> Instrument | None:
        if iid not in self.instruments:
            found = self.get(iid)
            if found is None:
                return None
            self.instruments[iid] = found
        return self.instruments[iid]

    def _put_instrument(self, inst: Instrument) -> None:
        self.instruments[inst.instrument_id] = inst
        self.dirty_instruments[inst.instrument_id] = inst

    def _index(self, m: SymbolMapping, old: SymbolMapping | None = None) -> None:
        bucket = self.by_symbol.setdefault((m.provider, m.symbol), {})
        if old is not None:
            bucket.pop(self._key(old), None)
        bucket[self._key(m)] = m

    def _put_map(self, m: SymbolMapping, old: SymbolMapping | None = None) -> None:
        self._index(m, old)
        self.dirty_maps[self._key(m)] = m

    def _reject(self, row: CryptoPairListing, reason: str, detail: str) -> None:
        self.unresolved.append(
            UnresolvedRecord(
                "listing", reason, str(row.symbol), str(row.provider), self.as_of, Market.CRYPTO, detail
            )
        )

    def process(self, row: CryptoPairListing) -> None:
        try:
            pair = row.pair
            provider = str(row.provider).strip().lower()
            symbol = str(row.symbol).strip()
            if not provider or not symbol:
                raise ValueError("provider and symbol are required")
            if row.listed_on and row.delisted_on and row.delisted_on < row.listed_on:
                raise ValueError("delisted_on is before listed_on")
        except ValueError as exc:
            self._reject(row, INVALID_PAIR, str(exc))
            return
        iid = pair.instrument_id
        owner = self.claims.setdefault((provider, symbol), iid)
        if owner != iid:
            self._reject(row, SYMBOL_CONFLICT, f"{provider}:{symbol} already claimed by {owner}")
            return
        other_open = next(
            (
                m for m in self.by_symbol.get((provider, symbol), {}).values()
                if m.valid_to is None and m.instrument_id != iid
            ),
            None,
        )
        if other_open is not None:
            self._reject(row, SYMBOL_CONFLICT, f"{provider}:{symbol} is open on {other_open.instrument_id}")
            return
        self._apply(row, pair, provider, symbol)

    def _apply(self, row: CryptoPairListing, pair: CryptoPair, provider: str, symbol: str) -> None:
        iid = pair.instrument_id
        existing = self._instrument(iid)
        if existing is None:
            self.created += 1
        else:
            self.reused += 1
        delisting = (not row.active) or row.delisted_on is not None
        prev_delisted = existing.delisted_on if existing else None
        delisted_on = (row.delisted_on or prev_delisted or self.as_of) if delisting else None
        base = existing or build_crypto_instrument(pair)
        ext = {**base.external_ids, EXTERNAL_ID_PROVIDER_SYMBOL: symbol}
        inst = replace(
            base,
            name=row.name or base.name,
            listed_on=_earlier(base.listed_on, row.listed_on),
            delisted_on=delisted_on,
            external_ids=ext,
        )
        if existing is None or inst != existing:
            self._put_instrument(inst)
        if delisting and prev_delisted != delisted_on:
            self.delisted += 1
        self._mapping(inst, provider, symbol, delisted_on)

    def _mapping(self, inst: Instrument, provider: str, symbol: str, delisted_on: date | None) -> None:
        iid = inst.instrument_id
        same = list(self.by_symbol.get((provider, symbol), {}).values())
        open_own = next((m for m in same if m.instrument_id == iid and m.valid_to is None), None)
        if delisted_on is not None:
            if open_own is not None:
                self._put_map(replace(open_own, valid_to=max(delisted_on, open_own.valid_from)), open_own)
                return
            if any(m.instrument_id == iid for m in same):
                return  # already closed
            start = min(inst.listed_on or delisted_on, delisted_on)
            floor = self._floor(same)
            start = max(start, floor) if floor else start
            self._put_map(SymbolMapping(iid, symbol, start, max(delisted_on, start), provider))
            return
        if open_own is not None:
            return
        relisted = any(m.instrument_id == iid for m in same)  # only closed windows exist
        start = self.as_of if relisted else inst.listed_on or self.as_of
        floor = self._floor(same)
        if floor is not None:
            start = max(start, floor)
        self._put_map(SymbolMapping(iid, symbol, start, None, provider))

    @staticmethod
    def _floor(same: Sequence[SymbolMapping]) -> date | None:
        """First day after every closed window of this (provider, symbol): windows never overlap."""
        closed = [m.valid_to for m in same if m.valid_to is not None]
        return max(closed) + _DAY if closed else None


def ingest_crypto_pairs(
    master: InstrumentMaster,
    listings: Sequence[CryptoPairListing],
    *,
    as_of: date | None = None,
) -> CryptoPairIngestSummary:
    """Upsert pair instruments and provider-specific symbol mappings from listing rows.

    * Lookup is by the deterministic id: an existing pair is reused (never a second id); the name
      is refreshed, the earlier ``listed_on`` wins, and an active row without ``delisted_on``
      re-opens a delisted pair (new mapping window from ``as_of``, after the closed one).
    * Only ``(provider, symbol)`` mappings are written, never a generic ``"*"`` one.
    * Delisting (``active=False`` or ``delisted_on``): sets ``delisted_on`` (row date, else the
      existing one, else ``as_of``) and closes that provider symbol's open window (inclusive).
    * Unresolved (counted, queued when the master has a queue, never raised): illegal venue / base /
      quote / dates, and a ``(provider, symbol)`` claimed by a different pair earlier in the
      batch or open on a different pair in the master. Identities are never merged.
    * ``as_of`` (default today) opens windows when ``listed_on`` is unknown.

    Other instruments in the master (e.g. US) are untouched.
    """
    run = _Ingest(master, as_of or date.today())
    for row in listings:
        run.process(row)
    if run.dirty_instruments or run.dirty_maps:
        master.upsert(run.dirty_instruments.values(), run.dirty_maps.values())
    sink = getattr(master, "record_unresolved", None)
    if run.unresolved and sink is not None:
        sink(run.unresolved)
    reasons: dict[str, int] = {}
    for r in run.unresolved:
        reasons[r.reason_code] = reasons.get(r.reason_code, 0) + 1
    return CryptoPairIngestSummary(
        created=run.created,
        reused=run.reused,
        unresolved=len(run.unresolved),
        delisted=run.delisted,
        reasons=dict(sorted(reasons.items())),
    )
