"""Instrument directory: ingest listing snapshots into stable instruments + symbol windows.

Evidence model (ported from the upstream internal implementation, see ``identity.py``): only a
share-class FIGI or a provider-native instrument id identifies a security. A ticker or CIK alone
never creates, renames, reuses or disputes an instrument; symbol-only rows can only *confirm* an
unambiguous open alias (and close it on a reported delisting).

What one ``ingest`` does per record:

* new stable identity -> new ``Instrument`` and an open ``SymbolMapping`` starting at the listing
  date (or, when the provider has none, the snapshot date: first observed);
* **ticker reuse** -- the symbol has an open alias on a *different* instrument -> that alias is
  closed (day before the new listing date, else the last date it was observed) and the new
  instrument gets the symbol. If neither date can be established the row is *unresolved*, never
  guessed;
* **rename** -- same stable identity, new symbol -> the old symbol's window is closed and the new
  symbol opens on the SAME instrument, so history stays continuous;
* **delisting** -> ``Instrument.delisted_on`` set and open aliases closed at the reported date.
  A delisting reported for a symbol the instrument no longer holds is a ticker change and only
  records the old symbol's closed window;
* conflicts (two instruments claim the same identity, issuer or asset class differs, overlapping
  windows) -> *unresolved* queue with a reason code; nothing is written for that row.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
from typing import Callable, Iterable, Mapping

from openbars.core.models import AssetClass, Instrument, Market, SymbolMapping
from openbars.normalize.identity import IdentityEvidence, IdentityResolver, IdentityStatus
from openbars.normalize.instruments import (
    InstrumentStore,
    UnresolvedRecord,
    figi_instrument_id,
    new_instrument_id,
)
from openbars.providers.base import ProviderInstrument
from openbars.normalize.security_types import (
    DEFAULT_SECURITY_TYPES,
    asset_class_for,
    security_type_of_asset_class,
)

__all__ = ["IngestPolicy", "IngestSummary", "InstrumentDirectory", "ListingRecord", "UNKNOWN_LISTING_START"]

# Start of a closed window whose provider gave neither a listing date nor an instrument start.
UNKNOWN_LISTING_START = date(1900, 1, 1)
GENERIC = "*"
_DAY = timedelta(days=1)


@dataclass(frozen=True)
class ListingRecord:
    """One row of a provider's listing snapshot (any provider can feed this)."""

    provider: str
    symbol: str
    exchange: str | None = None  # MIC where known
    ticker_type: str | None = None  # provider type code, e.g. "CS" / "ETF"
    asset_class: AssetClass | None = None  # overrides ``ticker_type`` when given
    name: str | None = None
    share_class_figi: str | None = None
    composite_figi: str | None = None
    cik: str | None = None
    isin: str | None = None
    provider_instrument_id: str | None = None
    list_date: date | None = None
    delisted_on: date | None = None
    active: bool = True
    market: Market = Market.US
    currency: str = "USD"

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", self.symbol.strip().upper())
        if not self.symbol or not self.provider:
            raise ValueError("provider and symbol are required")

    @classmethod
    def from_provider_instrument(cls, item: "ProviderInstrument", provider: str) -> "ListingRecord":
        """Adapt a ``Provider.list_instruments`` row (a provider without a listing date is
        first-observed evidence only)."""
        return cls(
            provider=provider,
            symbol=item.symbol,
            exchange=item.exchange,
            asset_class=item.asset_class,
            name=item.name,
            provider_instrument_id=item.native_id,
            active=item.active,
            market=item.market,
        )


@dataclass(frozen=True)
class IngestPolicy:
    """Which security types become instruments.

    Default universe: all common stock + ETF in the snapshot. ``extra_security_types`` (from
    configuration, default empty) adds further types such as ``"adr"`` or ``"fund"``.
    """

    extra_security_types: frozenset[str] = frozenset()

    @property
    def security_types(self) -> frozenset[str]:
        return DEFAULT_SECURITY_TYPES | self.extra_security_types


@dataclass
class IngestSummary:
    counts: Counter = field(default_factory=Counter)
    status: Counter = field(default_factory=Counter)  # IdentityStatus value -> rows
    unresolved: list[UnresolvedRecord] = field(default_factory=list)

    @property
    def unresolved_count(self) -> int:
        return len(self.unresolved)


def _aware_now() -> datetime:
    return datetime.now(timezone.utc)


def _overlaps(a_from: date, a_to: date | None, b_from: date, b_to: date | None) -> bool:
    return (a_to is None or b_from <= a_to) and (b_to is None or a_from <= b_to)


class _Work:
    """Mutable working copy of the master for one ingest (flushed once at the end)."""

    def __init__(self, store: InstrumentStore) -> None:
        self.instruments: dict[str, Instrument] = {i.instrument_id: i for i in store.instruments()}
        self.original_instruments = dict(self.instruments)
        self.mappings: dict[tuple, SymbolMapping] = {}
        self.by_symbol: dict[str, set[tuple]] = defaultdict(set)
        self.by_instrument: dict[str, set[tuple]] = defaultdict(set)
        for m in store.mappings():
            self.put(m)
        self.original_mappings = dict(self.mappings)
        self.by_figi: dict[str, str] = {}
        self.by_native: dict[tuple[str, str], str] = {}
        for i in self.instruments.values():
            self.index(i)
        self.last_obs: dict[tuple[str, str], date] = {}
        self.providers: dict[str, set[str]] = defaultdict(set)
        for iid, symbol, provider, on in store.observations():
            self.note_observation(iid, symbol, provider, on)
        self.new_observations: list[tuple[str, str, str, date]] = []

    @staticmethod
    def key(m: SymbolMapping) -> tuple:
        return (m.instrument_id, m.provider, m.symbol, m.valid_from)

    def put(self, m: SymbolMapping) -> None:
        self.mappings[self.key(m)] = m
        self.by_symbol[m.symbol].add(self.key(m))
        self.by_instrument[m.instrument_id].add(self.key(m))

    def drop(self, m: SymbolMapping) -> None:
        self.mappings.pop(self.key(m), None)
        self.by_symbol[m.symbol].discard(self.key(m))
        self.by_instrument[m.instrument_id].discard(self.key(m))

    def replace(self, old: SymbolMapping, **changes) -> SymbolMapping:
        new = replace(old, **changes)
        self.drop(old)
        self.put(new)
        return new

    def index(self, inst: Instrument) -> None:
        figi = inst.external_ids.get("figi")
        if figi:
            self.by_figi[figi] = inst.instrument_id
        for k, v in inst.external_ids.items():
            if k.startswith("native:"):
                self.by_native[(k[len("native:"):], v)] = inst.instrument_id

    def set_instrument(self, inst: Instrument) -> None:
        self.instruments[inst.instrument_id] = inst
        self.index(inst)

    def windows(self, market: Market, symbol: str) -> list[SymbolMapping]:
        out = []
        for k in self.by_symbol.get(symbol, ()):
            m = self.mappings[k]
            inst = self.instruments.get(m.instrument_id)
            if m.provider == GENERIC and inst is not None and inst.market is market:
                out.append(m)
        return sorted(out, key=lambda m: (m.valid_from, m.instrument_id))

    def own_windows(self, instrument_id: str) -> list[SymbolMapping]:
        return [
            m for k in self.by_instrument.get(instrument_id, ())
            if (m := self.mappings[k]).provider == GENERIC
        ]

    def note_observation(self, iid: str, symbol: str, provider: str, on: date) -> None:
        k = (iid, symbol)
        if k not in self.last_obs or self.last_obs[k] < on:
            self.last_obs[k] = on
        self.providers[iid].add(provider)


class InstrumentDirectory:
    """Builds and maintains the instrument master from listing snapshots."""

    def __init__(
        self,
        store: InstrumentStore,
        *,
        policy: IngestPolicy | None = None,
        id_factory: Callable[[Market], str] | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.store = store
        self.policy = policy or IngestPolicy()
        self._new_id = id_factory or new_instrument_id
        self._clock = clock or _aware_now

    # -- unresolved queue / upstream compat ---------------------------------------------------

    def unresolved(self, kind: str | None = None) -> list[UnresolvedRecord]:
        return self.store.unresolved(kind)

    def unresolved_count(self, kind: str | None = None) -> int:
        return self.store.unresolved_count(kind)

    def link_upstream_id(self, instrument_id: str, upstream_id: int) -> None:
        """Record that ``instrument_id`` corresponds to an upstream integer instrument id."""
        if not any(i.instrument_id == instrument_id for i in self.store.instruments()):
            raise KeyError(instrument_id)
        self.store.set_upstream_id(instrument_id, upstream_id)

    def upstream_id(self, instrument_id: str) -> int | None:
        return self.store.upstream_ids().get(instrument_id)

    def instrument_for_upstream_id(self, upstream_id: int) -> str | None:
        return next((k for k, v in self.store.upstream_ids().items() if v == upstream_id), None)

    # -- ingest ---------------------------------------------------------------------------------

    def ingest(
        self,
        records: Iterable[ListingRecord],
        *,
        as_of: date,
        observed_at: datetime | None = None,
    ) -> IngestSummary:
        """Apply one listing snapshot taken on ``as_of``."""
        observed_at = observed_at or self._clock()
        run = _Run(self, as_of, observed_at)
        for rec in sorted(records, key=lambda r: (r.market.value, r.symbol, r.list_date or date.max, r.provider)):
            run.process(rec)
        return run.flush()


@dataclass
class _Plan:
    inst_id: str | None
    conflicts: tuple[str, ...] = ()
    reason: str | None = None


class _Run:
    def __init__(self, directory: InstrumentDirectory, as_of: date, observed_at: datetime) -> None:
        self.d = directory
        self.as_of = as_of
        self.observed_at = observed_at
        self.w = _Work(directory.store)
        self.summary = IngestSummary()
        self.resolved_rows: set[tuple[str, str, str]] = set()  # (provider, market, symbol)
        self.dirty: set[str] = set()

    # -- helpers -------------------------------------------------------------------------------

    def _queue(self, rec: ListingRecord, reason: str, detail: str | None = None) -> None:
        self.summary.counts["unresolved"] += 1
        self.summary.unresolved.append(
            UnresolvedRecord("listing", reason, rec.symbol, rec.provider, self.as_of, rec.market, detail)
        )

    def _done(self, rec: ListingRecord) -> None:
        self.resolved_rows.add((rec.provider, rec.market.value, rec.symbol))

    def _classify(self, rec: ListingRecord) -> tuple[AssetClass | None, bool]:
        """(asset_class, skip). A row with no type information is unclassified (None, no skip)."""
        if rec.asset_class is None and not rec.ticker_type:
            return None, False
        ac = rec.asset_class or asset_class_for(rec.ticker_type)
        if ac is None or security_type_of_asset_class(ac) not in self.d.policy.security_types:
            return None, True
        return ac, False

    def _match(self, rec: ListingRecord, ac: AssetClass | None) -> _Plan:
        w = self.w
        ids = {
            i
            for i in (
                w.by_figi.get(rec.share_class_figi) if rec.share_class_figi else None,
                w.by_native.get((rec.provider, rec.provider_instrument_id)) if rec.provider_instrument_id else None,
            )
            if i
        }
        if len(ids) > 1:
            return _Plan(None, tuple(sorted(ids)), "conflicting_stable_identity")
        if not ids:
            return _Plan(None)
        iid = next(iter(ids))
        inst = w.instruments[iid]
        figi = inst.external_ids.get("figi")
        if rec.share_class_figi and figi and figi != rec.share_class_figi:
            return _Plan(None, (iid,), "conflicting_stable_identity")
        cik = inst.external_ids.get("cik")
        if rec.cik and cik and cik != rec.cik:
            return _Plan(None, (iid,), "issuer_mismatch")
        if inst.market is not rec.market or (ac is not None and inst.asset_class is not ac):
            return _Plan(None, (iid,), "asset_class_mismatch")
        return _Plan(iid)

    def _last_obs(self, iid: str, symbol: str) -> date | None:
        return self.w.last_obs.get((iid, symbol))

    def _observe(self, iid: str, symbol: str, provider: str) -> None:
        self.w.note_observation(iid, symbol, provider, self.as_of)
        self.w.new_observations.append((iid, symbol, provider, self.as_of))

    # -- per record ----------------------------------------------------------------------------

    def process(self, rec: ListingRecord) -> None:
        ac, skip = self._classify(rec)
        if skip:
            self.summary.counts["skipped_type"] += 1
            return
        plan = self._match(rec, ac)
        corroborating = ()
        if plan.inst_id:
            corroborating = tuple(sorted(self.w.providers[plan.inst_id] - {rec.provider}))
        evidence = IdentityEvidence(
            provider=rec.provider,
            symbol=rec.symbol,
            exchange=rec.exchange or "UNKNOWN",
            cik=rec.cik,
            provider_instrument_id=rec.provider_instrument_id,
            share_class_figi=rec.share_class_figi,
            composite_figi=rec.composite_figi,
            effective_from=rec.list_date or self.as_of,
            observed_at=self.observed_at,
            corroborating_providers=corroborating,
            conflicting_instrument_keys=plan.conflicts,
        )
        resolution = IdentityResolver.resolve(evidence, as_known_at=self.observed_at)
        self.summary.status[resolution.status.value] += 1
        if resolution.status is IdentityStatus.DISPUTED:
            self._queue(rec, plan.reason or resolution.reason_code, ",".join(plan.conflicts))
        elif resolution.status is IdentityStatus.UNRESOLVED:
            self._symbol_only(rec, resolution.reason_code)
        else:
            self._stable(rec, ac, plan.inst_id)

    def _symbol_only(self, rec: ListingRecord, reason: str) -> None:
        """No stable key: may only confirm / close the one unambiguous open alias."""
        w = self.w
        open_ = [m for m in w.windows(rec.market, rec.symbol) if m.valid_to is None]
        if not open_:
            return self._queue(rec, reason)
        if len(open_) > 1:
            return self._queue(rec, "ambiguous_open_alias")
        alias = open_[0]
        inst = w.instruments[alias.instrument_id]
        cik = inst.external_ids.get("cik")
        if rec.cik and cik and rec.cik != cik:
            return self._queue(rec, "issuer_mismatch", alias.instrument_id)
        if rec.delisted_on is not None:
            if rec.delisted_on < alias.valid_from:
                return self._queue(rec, "delisting_before_open_alias", alias.instrument_id)
            self._delist(inst.instrument_id, rec.delisted_on)
            self._done(rec)
        elif rec.active:
            self._observe(inst.instrument_id, rec.symbol, rec.provider)
            self.summary.counts["confirmed"] += 1
            self._done(rec)

    def _stable(self, rec: ListingRecord, ac: AssetClass | None, inst_id: str | None) -> None:
        w = self.w
        to = rec.delisted_on
        if not rec.active and to is None:
            if inst_id is None:
                self.summary.counts["skipped_inactive"] += 1
            else:
                self._close_inactive(rec, inst_id)
                self._done(rec)
            return
        if inst_id is None and ac is None:
            # unclassified rows can only refer to instruments we already know
            return self._queue(rec, "unclassified_security_type")

        if inst_id is None:
            new_id = (
                figi_instrument_id(rec.share_class_figi, rec.market)
                if rec.share_class_figi
                else self.d._new_id(rec.market)
            )
            if new_id in w.instruments:  # minted ids are never reused
                return self._queue(rec, "instrument_id_collision", new_id)
            from_ = rec.list_date or (UNKNOWN_LISTING_START if to is not None else self.as_of)
            reason = self._bind(new_id, rec, from_, to, new_instrument=True)
            if reason:
                return self._queue(rec, reason)
            assert ac is not None
            inst = Instrument(
                new_id, rec.market, ac, rec.currency, name=rec.name, exchange=rec.exchange,
                listed_on=rec.list_date, delisted_on=to, external_ids=self._external_ids(rec, {}),
            )
            w.set_instrument(inst)
            self.dirty.add(new_id)
            self.summary.counts["created"] += 1
            if to is None:
                self._observe(new_id, rec.symbol, rec.provider)
            self._done(rec)
            return

        inst = w.instruments[inst_id]
        own = [m for m in w.own_windows(inst_id) if m.symbol == rec.symbol]
        open_own = [m for m in own if m.valid_to is None]
        if to is None:
            if open_own:
                self._extend_start(rec, open_own[0])
            else:
                from_ = rec.list_date or self.as_of
                reason = self._bind(inst_id, rec, from_, None, new_instrument=False)
                if reason:
                    return self._queue(rec, reason)
                if own:
                    self.summary.counts["relisted"] += 1
            self._observe(inst_id, rec.symbol, rec.provider)
        else:
            other_open = [m for m in w.own_windows(inst_id) if m.valid_to is None and m.symbol != rec.symbol]
            if not open_own and other_open:
                # the security lives on under another symbol: only the old symbol's window ends
                if not any(_overlaps(m.valid_from, m.valid_to, rec.list_date or UNKNOWN_LISTING_START, to) for m in own):
                    start = rec.list_date or inst.listed_on or UNKNOWN_LISTING_START
                    reason = self._bind(inst_id, rec, start, to, new_instrument=False)
                    if reason:
                        return self._queue(rec, reason)
                self.summary.counts["ticker_changed"] += 1
            else:
                if not own:
                    start = rec.list_date or inst.listed_on or UNKNOWN_LISTING_START
                    reason = self._bind(inst_id, rec, start, to, new_instrument=False)
                    if reason:
                        return self._queue(rec, reason)
                self._delist(inst_id, to)
        self._refresh(rec, inst_id)
        self._done(rec)

    def _external_ids(self, rec: ListingRecord, existing: Mapping[str, str]) -> dict[str, str]:
        ids = dict(existing)
        for key, value in (
            ("figi", rec.share_class_figi),
            ("composite_figi", rec.composite_figi),
            ("cik", rec.cik),
            ("isin", rec.isin),
            (f"native:{rec.provider}", rec.provider_instrument_id),
        ):
            if value and key not in ids:
                ids[key] = value
            elif value and key.startswith("native:"):
                ids[key] = value  # a provider re-keying its native id; FIGI match already agreed
        return ids

    def _refresh(self, rec: ListingRecord, iid: str) -> None:
        inst = self.w.instruments[iid]
        listed_on = inst.listed_on
        if rec.list_date and (listed_on is None or rec.list_date < listed_on):
            listed_on = rec.list_date
        new = replace(
            inst,
            name=inst.name or rec.name,
            exchange=inst.exchange or rec.exchange,
            listed_on=listed_on,
            external_ids=self._external_ids(rec, inst.external_ids),
        )
        if new != inst:
            self.w.set_instrument(new)
            self.dirty.add(iid)

    # -- window mutations ----------------------------------------------------------------------

    def _extend_start(self, rec: ListingRecord, alias: SymbolMapping) -> None:
        """A provider listing date beats a first-observed start, unless another instrument held it."""
        if rec.list_date is None or rec.list_date >= alias.valid_from:
            return
        for m in self.w.windows(rec.market, rec.symbol):
            if m.instrument_id != alias.instrument_id and _overlaps(
                m.valid_from, m.valid_to, rec.list_date, alias.valid_from - _DAY
            ):
                return
        self.w.replace(alias, valid_from=rec.list_date)

    def _bind(
        self, iid: str, rec: ListingRecord, from_: date, to: date | None, *, new_instrument: bool
    ) -> str | None:
        """Open (or add a closed) window of ``rec.symbol`` for ``iid``; reason code on refusal."""
        w = self.w
        symbol = rec.symbol
        others = [m for m in w.windows(rec.market, symbol) if m.instrument_id != iid]
        open_others = [m for m in others if m.valid_to is None]
        closing: tuple[SymbolMapping, date] | None = None
        if open_others:
            if to is not None and all(to < m.valid_from for m in open_others):
                pass  # older, already finished history
            elif to is not None:
                return "symbol_window_overlap"
            elif len(open_others) > 1:
                return "ambiguous_open_alias"
            else:
                old = open_others[0]
                last = self._last_obs(old.instrument_id, symbol)
                if rec.list_date is not None:
                    if rec.list_date <= old.valid_from or (last is not None and last >= rec.list_date):
                        return "symbol_has_open_alias"
                    close_at = rec.list_date - _DAY
                else:
                    if last is None or last >= from_ or last < old.valid_from:
                        return "symbol_has_open_alias"
                    close_at = last
                closing = (old, close_at)
        for m in others:
            m_to = closing[1] if closing and m == closing[0] else m.valid_to
            if _overlaps(m.valid_from, m_to, from_, to):
                return "symbol_window_overlap"
        for m in w.own_windows(iid):
            if m.symbol == symbol and _overlaps(m.valid_from, m.valid_to, from_, to):
                return None  # already known
        if closing is not None:
            w.replace(closing[0], valid_to=closing[1])
            self.summary.counts["reused"] += 1
        if to is None and not new_instrument:
            self._close_siblings(iid, symbol, rec, from_)
        w.put(SymbolMapping(iid, symbol, from_, to, GENERIC))
        return None

    def _close_siblings(self, iid: str, symbol: str, rec: ListingRecord, from_: date) -> None:
        """Rename: the same instrument's other open symbols end where the new one begins."""
        for m in [m for m in self.w.own_windows(iid) if m.valid_to is None and m.symbol != symbol]:
            last = self._last_obs(iid, m.symbol)
            if rec.list_date is not None:
                close_at = rec.list_date - _DAY
            elif last is None:
                close_at = from_ - _DAY
            elif last < from_:
                close_at = last
            else:
                continue  # still observed alongside the new symbol; leave it open
            self.w.replace(m, valid_to=max(close_at, m.valid_from))
            self.summary.counts["renamed"] += 1

    def _close_inactive(self, rec: ListingRecord, iid: str) -> None:
        for m in [m for m in self.w.own_windows(iid) if m.valid_to is None and m.symbol == rec.symbol]:
            last = self._last_obs(iid, m.symbol)
            if last is not None and last >= m.valid_from:
                self.w.replace(m, valid_to=last)
                self.summary.counts["closed_inactive"] += 1

    def _delist(self, iid: str, on: date) -> None:
        for m in [m for m in self.w.own_windows(iid) if m.valid_to is None]:
            self.w.replace(m, valid_to=max(on, m.valid_from))
        inst = self.w.instruments[iid]
        if inst.delisted_on != on:
            self.w.set_instrument(replace(inst, delisted_on=on))
            self.dirty.add(iid)
        self.summary.counts["delisted"] += 1

    # -- persistence -----------------------------------------------------------------------------

    def flush(self) -> IngestSummary:
        w, store = self.w, self.d.store
        removed = [m for k, m in w.original_mappings.items() if k not in w.mappings]
        added = [m for k, m in w.mappings.items() if w.original_mappings.get(k) != m]
        changed = [
            i for k, i in w.instruments.items()
            if k in self.dirty and w.original_instruments.get(k) != i
        ]
        if removed:
            store.remove_mappings(removed)
        store.upsert(changed, added)
        store.observe(w.new_observations)
        pending = [
            r for r in store.unresolved("listing")
            if (r.provider, Market(r.market).value, r.symbol) in self.resolved_rows
        ]
        store.discard_unresolved(pending)
        store.record_unresolved(self.summary.unresolved)
        return self.summary
