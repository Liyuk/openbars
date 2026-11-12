"""``openbars verify``: coverage and cross-source agreement report over stored bars.

The core (:func:`verify`) is pure: it takes iterables of :class:`VerifyBar` (raw bars) and
:class:`VerifyCanonical` (canonical rows), a trading calendar, a window, a universe and
tolerances. :func:`load_raw_bars` / :func:`load_canonical` adapt a storage backend.

What is scored:

* **coverage**: expected sessions x instruments vs cells that hold a canonical (and/or raw)
  bar, plus per-source summaries (provider, feed, adjustment, bars, symbols, oldest, newest);
* **price / volume agreement**: for every instrument-session with two or more raw sources,
  each pair of sources is first run through
  :func:`openbars.reconcile.compatibility.compare_series`. Incomparable pairs (for example
  IEX vs SIP, or a source with provider-defined bases) count as ``not_comparable`` and are
  never scored as disagreements, so volumes of different feeds are never mixed;
* **quality-flag agreement**: only when canonical rows of several policy versions exist for
  the same cell; otherwise ``n/a`` (the status distribution and the canonical flag counts, for
  example A-share ``LIMIT_UP`` / ``LIMIT_DOWN``, are still reported).

Every market is verified on its own calendar (the caller passes it in) and its own
``policy_version``. Stored bars are always RAW: A-share QFQ / HFQ and Hong Kong split-only reads
are derived at query time and are never scored here. An A-share suspension keeps its raw bar
(``tradestatus`` 0) but has no canonical bar; that expected hole is a *suspended cell*, not an
ordinary missing cell.

A report is not a pass/fail gate: agreement below 100% is a finding, not an error.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, InvalidOperation
from enum import Enum
from itertools import combinations
from typing import Any

from openbars.core.errors import ConfigError
from openbars.core.models import DateRange, Interval, Market
from openbars.core.calendar import TradingCalendar
from openbars.core.series import (
    POLICY_VERSION_RESEARCH_DAILY_V2,
    AdjustmentScope,
    BarSeriesCatalog,
    BarSeriesIdentity,
    PriceBasis,
    SessionSemantics,
    SourceIdentity,
    VolumeBasis,
)
from openbars.reconcile.compatibility import compare_series
from openbars.reconcile.policy import PRICE_TOLERANCE_PCT, VOLUME_TOLERANCE_PCT
from openbars.storage.rows import decode_extra

DEFAULT_RECENT_SESSIONS = 20
_HUNDRED = Decimal(100)
_UNKNOWN = "unknown"


class Agreement(str, Enum):
    MATCH = "match"
    WITHIN_TOLERANCE = "within_tolerance"
    DIVERGENT = "divergent"


# --- inputs ----------------------------------------------------------------------------------


@dataclass(frozen=True)
class Tolerances:
    """Relative tolerances in **percent** (``Decimal("0.5")`` == 0.5%), as in ``ReconcilePolicy``."""

    price_pct: Decimal = PRICE_TOLERANCE_PCT
    volume_pct: Decimal = VOLUME_TOLERANCE_PCT

    def __post_init__(self) -> None:
        for name in ("price_pct", "volume_pct"):
            value = getattr(self, name)
            if isinstance(value, (bool, float)) or not isinstance(value, Decimal):
                raise TypeError(f"{name} must be Decimal, not {type(value).__name__}")
            if value < 0:
                raise ValueError(f"{name} must be >= 0")

    @classmethod
    def from_settings(cls, settings: Any) -> Tolerances:
        """Read ``reconcile.tolerances`` (``price_rel`` / ``volume_rel`` as fractions).

        Missing keys keep the research-daily-v2 defaults.
        """
        section = getattr(settings, "reconcile", None) or {}
        raw = section.get("tolerances") if isinstance(section, Mapping) else None
        if raw is None:
            return cls()
        if not isinstance(raw, Mapping):
            raise ConfigError("reconcile.tolerances must be a mapping")
        values: dict[str, Decimal] = {}
        for key, target in (("price_rel", "price_pct"), ("volume_rel", "volume_pct")):
            if raw.get(key) is None:
                continue
            try:
                fraction = Decimal(str(raw[key]))
            except InvalidOperation:
                raise ConfigError(f"reconcile.tolerances.{key} is not a number: {raw[key]!r}") from None
            if not fraction.is_finite() or fraction < 0:
                raise ConfigError(f"reconcile.tolerances.{key} must be a non-negative number")
            values[target] = fraction * _HUNDRED
        return cls(**values)


@dataclass(frozen=True)
class VerifyBar:
    """A stored raw bar, reduced to what verification needs."""

    key: str  # instrument id, else the symbol as returned
    session_date: date
    provider: str
    feed: str
    adjustment: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal | None
    series: BarSeriesIdentity | None = None  # None: resolved via BarSeriesCatalog
    revision: int = 0
    suspended: bool = False  # A-share raw row of a trading suspension (tradestatus 0)

    @property
    def source(self) -> SourceIdentity:
        return SourceIdentity(self.provider, self.feed or "default", self.adjustment or "raw")

    def resolved_series(self) -> BarSeriesIdentity:
        return self.series if self.series is not None else BarSeriesCatalog.resolve(self.source)


@dataclass(frozen=True)
class VerifyCanonical:
    """A stored canonical row, reduced to what verification needs."""

    key: str
    session_date: date
    policy_version: str = POLICY_VERSION_RESEARCH_DAILY_V2
    provider: str = ""
    feed: str = ""
    quality_status: str | None = None
    flags: tuple[str, ...] = ()


# --- outputs ---------------------------------------------------------------------------------


@dataclass
class AgreementStats:
    match: int = 0
    within_tolerance: int = 0
    divergent: int = 0
    not_comparable: int = 0  # series gate said no: never scored
    missing: int = 0  # comparable series but one side has no value (e.g. volume)

    @property
    def compared(self) -> int:
        return self.match + self.within_tolerance + self.divergent

    @property
    def agreeing(self) -> int:
        return self.match + self.within_tolerance

    @property
    def rate(self) -> float | None:
        """Share of compared pairs that match or are within tolerance; ``None`` = n/a."""
        return self.agreeing / self.compared if self.compared else None

    def add(self, outcome: Agreement) -> None:
        if outcome is Agreement.MATCH:
            self.match += 1
        elif outcome is Agreement.WITHIN_TOLERANCE:
            self.within_tolerance += 1
        else:
            self.divergent += 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "match": self.match,
            "within_tolerance": self.within_tolerance,
            "divergent": self.divergent,
            "not_comparable": self.not_comparable,
            "missing": self.missing,
            "compared": self.compared,
            "rate": self.rate,
        }


@dataclass(frozen=True)
class SourceSummary:
    """Port of the upstream coverage summary: one row per source identity."""

    provider: str
    feed: str
    adjustment: str
    bars: int
    symbols: int
    oldest: date | None
    newest: date | None


@dataclass
class Coverage:
    sessions: int = 0
    universe: int = 0
    expected_cells: int = 0
    canonical_cells: int = 0
    raw_cells: int = 0  # cells with a bar from any raw source
    suspended_cells: int = 0  # expected canonical holes: raw shows a suspension, canonical dropped it
    canonical_available: bool = False
    by_source: dict[str, int] = field(default_factory=dict)  # "provider/feed/adjustment" -> cells
    canonical_by_provider: dict[str, int] = field(default_factory=dict)

    def _rate(self, cells: int) -> float | None:
        return cells / self.expected_cells if self.expected_cells else None

    @property
    def canonical_rate(self) -> float | None:
        return self._rate(self.canonical_cells)

    @property
    def raw_rate(self) -> float | None:
        return self._rate(self.raw_cells)

    @property
    def missing_cells(self) -> int:
        """Expected cells without a canonical bar (raw bar when no canonical rows exist).

        Suspended cells (A-share halts: raw kept, canonical dropped) are expected holes and are
        not counted as missing.
        """
        if not self.canonical_available:
            return self.expected_cells - self.raw_cells
        return self.expected_cells - self.canonical_cells - self.suspended_cells

    def to_dict(self) -> dict[str, Any]:
        return {
            "sessions": self.sessions,
            "universe": self.universe,
            "expected_cells": self.expected_cells,
            "canonical_cells": self.canonical_cells,
            "canonical_rate": self.canonical_rate,
            "raw_cells": self.raw_cells,
            "raw_rate": self.raw_rate,
            "missing_cells": self.missing_cells,
            "suspended_cells": self.suspended_cells,
            "by_source": {k: {"cells": v, "rate": self._rate(v)} for k, v in sorted(self.by_source.items())},
            "canonical_by_provider": dict(sorted(self.canonical_by_provider.items())),
        }


@dataclass
class QualityAgreement:
    """Quality status agreement between policy versions; ``rate`` is ``None`` when n/a."""

    distribution: dict[str, int] = field(default_factory=dict)  # status -> canonical rows
    flags: dict[str, int] = field(default_factory=dict)  # quality flag -> canonical rows carrying it
    cells_compared: int = 0  # cells with rows from >= 2 policy versions
    cells_agreeing: int = 0

    @property
    def rate(self) -> float | None:
        return self.cells_agreeing / self.cells_compared if self.cells_compared else None

    def to_dict(self) -> dict[str, Any]:
        return {
            "distribution": dict(sorted(self.distribution.items())),
            "flags": dict(sorted(self.flags.items())),
            "cells_compared": self.cells_compared,
            "cells_agreeing": self.cells_agreeing,
            "rate": self.rate,
        }


@dataclass(frozen=True)
class PairSummary:
    left: str  # source key "provider/feed/adjustment"
    right: str
    price: AgreementStats
    volume: AgreementStats


@dataclass
class PeriodMetrics:
    label: str
    window: DateRange | None
    coverage: Coverage
    price: AgreementStats  # all of open/high/low/close within tolerance
    close: AgreementStats  # close only
    volume: AgreementStats
    quality: QualityAgreement
    pairs: tuple[PairSummary, ...] = ()
    single_source_cells: int = 0  # cells with exactly one raw source: nothing to compare
    multi_source_cells: int = 0
    not_comparable_pairs: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "label": self.label,
            "start": self.window.start.isoformat() if self.window else None,
            "end": self.window.end.isoformat() if self.window else None,
            "coverage": self.coverage.to_dict(),
            "price": self.price.to_dict(),
            "close": self.close.to_dict(),
            "volume": self.volume.to_dict(),
            "quality": self.quality.to_dict(),
            "counts": {
                "missing": self.coverage.missing_cells,
                "suspended": self.coverage.suspended_cells,
                "not_comparable": self.not_comparable_pairs,
                "divergent_price": self.price.divergent,
                "divergent_volume": self.volume.divergent,
                "single_source_cells": self.single_source_cells,
                "multi_source_cells": self.multi_source_cells,
            },
            "pairs": [
                {"left": p.left, "right": p.right, "price": p.price.to_dict(), "volume": p.volume.to_dict()}
                for p in self.pairs
            ],
        }


@dataclass(frozen=True)
class VerifyReport:
    window: DateRange
    universe_size: int
    expected_sessions: int
    tolerances: Tolerances
    policy_version: str
    sources: tuple[SourceSummary, ...]
    periods: tuple[PeriodMetrics, ...]  # periods[0] is the whole window
    notes: tuple[str, ...] = ()

    @property
    def overall(self) -> PeriodMetrics:
        return self.periods[0]

    @property
    def coverage(self) -> Coverage:
        return self.overall.coverage

    @property
    def price(self) -> AgreementStats:
        return self.overall.price

    @property
    def close(self) -> AgreementStats:
        return self.overall.close

    @property
    def volume(self) -> AgreementStats:
        return self.overall.volume

    @property
    def quality(self) -> QualityAgreement:
        return self.overall.quality

    def to_dict(self) -> dict[str, Any]:
        return {
            "window": {"start": self.window.start.isoformat(), "end": self.window.end.isoformat()},
            "universe_size": self.universe_size,
            "expected_sessions": self.expected_sessions,
            "policy_version": self.policy_version,
            "tolerances": {"price_pct": str(self.tolerances.price_pct), "volume_pct": str(self.tolerances.volume_pct)},
            "sources": [
                {
                    "provider": s.provider,
                    "feed": s.feed,
                    "adjustment": s.adjustment,
                    "bars": s.bars,
                    "symbols": s.symbols,
                    "oldest": s.oldest.isoformat() if s.oldest else None,
                    "newest": s.newest.isoformat() if s.newest else None,
                }
                for s in self.sources
            ],
            "periods": [p.to_dict() for p in self.periods],
            "notes": list(self.notes),
        }


# --- core --------------------------------------------------------------------------------------


def diff_pct(a: Decimal, b: Decimal) -> Decimal:
    """Relative difference in percent against the larger magnitude (same rule as the reconciler)."""
    if a == 0 and b == 0:
        return Decimal(0)
    return abs(a - b) / max(abs(a), abs(b)) * _HUNDRED


def classify(diff: Decimal, tolerance_pct: Decimal) -> Agreement:
    if diff == 0:
        return Agreement.MATCH
    return Agreement.WITHIN_TOLERANCE if diff <= tolerance_pct else Agreement.DIVERGENT


def _source_key(bar: VerifyBar) -> str:
    return bar.source.key


def verify(
    raw: Iterable[VerifyBar],
    canonical: Iterable[VerifyCanonical],
    *,
    calendar: TradingCalendar,
    window: DateRange,
    universe: Iterable[str],
    tolerances: Tolerances | None = None,
    aliases: Mapping[str, str] | None = None,
    recent_sessions: int | None = DEFAULT_RECENT_SESSIONS,
    policy_version: str = POLICY_VERSION_RESEARCH_DAILY_V2,
) -> VerifyReport:
    """Build a :class:`VerifyReport`.

    ``universe`` lists instrument ids or symbols; ``aliases`` maps any id/symbol found in the
    data (or the universe) to the entity it stands for, so a symbol-keyed raw bar and an
    instrument-keyed canonical row meet in the same cell. Bars outside the universe, the
    window or the calendar's sessions are ignored.
    """
    tolerances = tolerances or Tolerances()
    alias = dict(aliases or {})

    def entity(key: str) -> str:
        return alias.get(key, key)

    entities = frozenset(entity(u) for u in universe)
    sessions = list(calendar.sessions(window.start, window.end))
    session_set = frozenset(sessions)

    # cells: day -> entity -> source key -> latest-revision bar
    raw_by_day: dict[date, dict[str, dict[str, VerifyBar]]] = defaultdict(lambda: defaultdict(dict))
    summaries: dict[str, list[Any]] = {}  # source key -> [source, bars, symbols, oldest, newest]
    for bar in raw:
        e = entity(bar.key)
        if e not in entities or bar.session_date not in session_set:
            continue
        slot = raw_by_day[bar.session_date][e]
        sk = _source_key(bar)
        previous = slot.get(sk)
        if previous is None:
            info = summaries.setdefault(sk, [bar.source, 0, set(), bar.session_date, bar.session_date])
            info[1] += 1
            info[2].add(e)
            info[3], info[4] = min(info[3], bar.session_date), max(info[4], bar.session_date)
        if previous is None or bar.revision >= previous.revision:
            slot[sk] = bar

    suspended: set[tuple[date, str]] = set()
    for day, by_entity in raw_by_day.items():
        for e, by_source in by_entity.items():
            if any(b.suspended for b in by_source.values()):
                suspended.add((day, e))

    canon_status: dict[date, dict[str, dict[str, str | None]]] = defaultdict(lambda: defaultdict(dict))
    canon_flags: dict[date, dict[str, tuple[str, ...]]] = defaultdict(dict)  # of ``policy_version``
    canon_provider: dict[date, dict[str, str]] = defaultdict(dict)  # selected source of ``policy_version``
    canonical_rows = 0
    for row in canonical:
        e = entity(row.key)
        if e not in entities or row.session_date not in session_set:
            continue
        canonical_rows += 1
        canon_status[row.session_date][e][row.policy_version] = row.quality_status
        if row.policy_version == policy_version:
            canon_flags[row.session_date][e] = tuple(row.flags)
            canon_provider[row.session_date][e] = f"{row.provider}/{row.feed}" if row.provider else "unknown"

    ctx = _Context(tolerances, len(entities), canonical_rows > 0)
    notes: list[str] = []
    if not entities:
        notes.append("empty universe: nothing to verify")
    elif not canonical_rows:
        notes.append("no canonical rows in the window: coverage and missing counts use raw bars")

    ctx.suspended = suspended
    ctx.canon_flags = canon_flags
    periods = [ctx.metrics("window", sessions, raw_by_day, canon_status, canon_provider, policy_version)]
    if recent_sessions and 0 < recent_sessions < len(sessions):
        periods.append(
            ctx.metrics(
                f"last {recent_sessions} sessions", sessions[-recent_sessions:],
                raw_by_day, canon_status, canon_provider, policy_version,
            )  # fmt: skip
        )

    held = periods[0].coverage.suspended_cells
    if held:
        notes.append(
            f"{held} suspended cell(s): raw bar with tradestatus 0, canonical dropped the halt day; "
            "an expected hole, not counted as missing"
        )
    sources = tuple(
        SourceSummary(info[0].provider, info[0].feed, info[0].adjustment, info[1], len(info[2]), info[3], info[4])
        for _, info in sorted(summaries.items())
    )
    return VerifyReport(
        window=window,
        universe_size=len(entities),
        expected_sessions=len(sessions),
        tolerances=tolerances,
        policy_version=policy_version,
        sources=sources,
        periods=tuple(periods),
        notes=tuple(notes),
    )


class _Context:
    def __init__(self, tolerances: Tolerances, universe: int, canonical_available: bool) -> None:
        self.tol = tolerances
        self.universe = universe
        self.canonical_available = canonical_available
        self.suspended: set[tuple[date, str]] = set()
        self.canon_flags: Mapping[date, Mapping[str, tuple[str, ...]]] = {}
        self._gate: dict[tuple[BarSeriesIdentity, BarSeriesIdentity], bool] = {}

    def comparable(self, a: VerifyBar, b: VerifyBar) -> bool:
        sa, sb = a.resolved_series(), b.resolved_series()
        if (sa, sb) not in self._gate:
            self._gate[(sa, sb)] = compare_series("x", sa, "x", sb).comparable
        return self._gate[(sa, sb)]

    def metrics(
        self,
        label: str,
        sessions: Sequence[date],
        raw_by_day: Mapping[date, Mapping[str, Mapping[str, VerifyBar]]],
        canon_status: Mapping[date, Mapping[str, Mapping[str, str | None]]],
        canon_provider: Mapping[date, Mapping[str, str]],
        policy_version: str,
    ) -> PeriodMetrics:
        cov = Coverage(
            sessions=len(sessions),
            universe=self.universe,
            expected_cells=len(sessions) * self.universe,
            canonical_available=self.canonical_available,
        )
        price, close, volume = AgreementStats(), AgreementStats(), AgreementStats()
        pairs: dict[tuple[str, str], tuple[AgreementStats, AgreementStats]] = {}
        quality = QualityAgreement()
        by_source: Counter[str] = Counter()
        provider_counts: Counter[str] = Counter()
        statuses: Counter[str] = Counter()
        flag_counts: Counter[str] = Counter()
        single = multi = not_comparable = 0

        for day in sessions:
            for e, sources in raw_by_day.get(day, {}).items():
                cov.raw_cells += 1
                if (day, e) in self.suspended and policy_version not in canon_status.get(day, {}).get(e, {}):
                    cov.suspended_cells += 1
                by_source.update(sources.keys())
                if len(sources) < 2:
                    single += 1
                    continue
                multi += 1
                for (ka, a), (kb, b) in combinations(sorted(sources.items()), 2):
                    p_stats, v_stats = pairs.setdefault((ka, kb), (AgreementStats(), AgreementStats()))
                    if not self.comparable(a, b):
                        for stats in (price, close, volume, p_stats, v_stats):
                            stats.not_comparable += 1
                        not_comparable += 1
                        continue
                    self._score_prices(a, b, price, close, p_stats)
                    self._score_volume(a, b, volume, v_stats)
            for e, versions in canon_status.get(day, {}).items():
                if policy_version in versions:
                    cov.canonical_cells += 1
                    provider_counts[canon_provider[day][e]] += 1
                    statuses[versions[policy_version] or _UNKNOWN] += 1
                    flag_counts.update(self.canon_flags.get(day, {}).get(e, ()))
                known = [v for v in versions.values() if v is not None]
                if len(versions) >= 2 and len(known) == len(versions):  # unknown statuses are not guessed
                    quality.cells_compared += 1
                    quality.cells_agreeing += len(set(known)) == 1

        cov.by_source = dict(by_source)
        cov.canonical_by_provider = dict(provider_counts)
        quality.distribution = dict(statuses)
        quality.flags = dict(flag_counts)
        return PeriodMetrics(
            label=label,
            window=DateRange(sessions[0], sessions[-1]) if sessions else None,
            coverage=cov,
            price=price,
            close=close,
            volume=volume,
            quality=quality,
            pairs=tuple(PairSummary(ka, kb, p, v) for (ka, kb), (p, v) in sorted(pairs.items())),
            single_source_cells=single,
            multi_source_cells=multi,
            not_comparable_pairs=not_comparable,
        )

    def _score_prices(
        self, a: VerifyBar, b: VerifyBar, price: AgreementStats, close: AgreementStats, pair: AgreementStats
    ) -> None:
        close_diff = diff_pct(a.close, b.close)
        ohlc_diff = max(
            diff_pct(a.open, b.open), diff_pct(a.high, b.high), diff_pct(a.low, b.low), close_diff
        )
        close.add(classify(close_diff, self.tol.price_pct))
        outcome = classify(ohlc_diff, self.tol.price_pct)
        price.add(outcome)
        pair.add(outcome)

    def _score_volume(self, a: VerifyBar, b: VerifyBar, volume: AgreementStats, pair: AgreementStats) -> None:
        if a.volume is None or b.volume is None:
            volume.missing += 1
            pair.missing += 1
            return
        outcome = classify(diff_pct(a.volume, b.volume), self.tol.volume_pct)
        volume.add(outcome)
        pair.add(outcome)


# --- storage adapters --------------------------------------------------------------------------


def _series_from_record(record: Mapping[str, Any]) -> BarSeriesIdentity | None:
    """Series stored with the raw row; ``None`` when the bases were never recorded."""
    bases = (
        record.get("price_basis"), record.get("adjustment_scope"),
        record.get("session_semantics"), record.get("volume_basis"),
    )  # fmt: skip
    if all(b in (None, "", _UNKNOWN) for b in bases):
        return None
    return BarSeriesIdentity(
        provider=record["provider"],
        feed=record.get("feed") or "default",
        legacy_adjustment=record.get("adjustment") or "raw",
        interval=Interval(record["interval"]),
        price_basis=PriceBasis(record.get("price_basis") or _UNKNOWN),
        adjustment_scope=AdjustmentScope(record.get("adjustment_scope") or _UNKNOWN),
        adjustment_as_of=record.get("adjustment_as_of"),
        session_semantics=SessionSemantics(record.get("session_semantics") or _UNKNOWN),
        currency=record.get("currency"),
        volume_basis=VolumeBasis(record.get("volume_basis") or _UNKNOWN),
    )


def _is_suspended(extra: Any, volume: Decimal | None) -> bool:
    """CN rule (``cn_quality.suspended_dates``): ``tradestatus`` decides; without it, volume 0."""
    status = (decode_extra(extra) or {}).get("tradestatus")
    if status is None:
        return volume is not None and volume == 0
    return str(status).strip() == "0"


def load_raw_bars(
    backend: Any,
    market: Market,
    window: DateRange,
    *,
    interval: Interval = Interval.D1,
    providers: Sequence[str] = (),
) -> list[VerifyBar]:
    """All raw bars of the window. Parquet backends are scanned across providers at once;
    other ``RawStore`` implementations need ``providers``."""
    store = backend.raw
    out: list[VerifyBar] = []
    cn = Market(market) is Market.CN  # only A-shares have trading suspensions
    if hasattr(store, "scan_table"):
        for r in store.scan_table(None, market, interval, window).to_pylist():
            out.append(
                VerifyBar(
                    key=r.get("instrument_id") or r["symbol_as_returned"],
                    session_date=r["session_date"],
                    provider=r["provider"], feed=r.get("feed") or "", adjustment=r.get("adjustment") or "raw",
                    open=r["open"], high=r["high"], low=r["low"], close=r["close"], volume=r.get("volume"),
                    series=_series_from_record(r), revision=int(r.get("revision") or 0),
                    suspended=cn and _is_suspended(r.get("extra"), r.get("volume")),
                )  # fmt: skip
            )
        return out
    for provider in providers:
        for bar in store.scan(provider, market, interval, window):
            day = bar.session_date or bar.ts_start.date()
            out.append(
                VerifyBar(
                    key=bar.instrument_id or bar.symbol_as_returned, session_date=day,
                    provider=bar.provider, feed=bar.feed or "",
                    adjustment=str(bar.extra.get("adjustment") or "raw"),
                    open=bar.open, high=bar.high, low=bar.low, close=bar.close, volume=bar.volume,
                    series=bar.series, revision=bar.revision,
                    suspended=cn and _is_suspended(bar.extra, bar.volume),
                )  # fmt: skip
            )
    return out


def load_canonical(
    backend: Any,
    market: Market,
    window: DateRange,
    *,
    interval: Interval = Interval.D1,
    instrument_ids: Sequence[str] | None = None,
) -> list[VerifyCanonical]:
    """Canonical rows of every policy version (Parquet), else the default-policy ``scan``."""
    store = backend.canonical
    if hasattr(store, "scan_table"):
        table = store.scan_table(None, interval, window, market=market, policy_version=None)
        return [
            VerifyCanonical(
                key=r["instrument_id"], session_date=r["session_date"], policy_version=r["policy_version"],
                provider=r.get("provider") or "", feed=r.get("feed") or "", quality_status=r.get("quality_status"),
                flags=tuple(r.get("flags") or ()),
            )  # fmt: skip
            for r in table.to_pylist()
        ]
    return [
        VerifyCanonical(
            key=cb.bar.instrument_id, session_date=cb.bar.session_date,
            provider=cb.bar.source, feed=cb.bar.feed or "",
        )  # fmt: skip
        for cb in store.scan(list(instrument_ids or ()), interval, window)
    ]


def stored_session_range(backend: Any, market: Market, window: DateRange) -> DateRange | None:
    """Oldest/newest ``session_date`` stored (raw or canonical) inside ``window``."""
    days: list[date] = []
    raw_store, canon_store = backend.raw, backend.canonical
    if hasattr(raw_store, "scan_table"):
        t = raw_store.scan_table(None, market, Interval.D1, window)
        days += [d for d in t.column("session_date").to_pylist() if d is not None]
    if hasattr(canon_store, "scan_table"):
        t = canon_store.scan_table(None, Interval.D1, window, market=market, policy_version=None)
        days += [d for d in t.column("session_date").to_pylist() if d is not None]
    return DateRange(min(days), max(days)) if days else None


def build_aliases(master: Any, keys: Iterable[str], *, on: date, market: Market) -> dict[str, str]:
    """Map symbols/ids to instrument ids via the instrument master (point in time ``on``)."""
    resolve = getattr(master, "resolve_any", None)
    if resolve is None:
        return {}
    out: dict[str, str] = {}
    for key in set(keys):
        try:
            found = resolve(key, on=on, market=market)
        except Exception:  # noqa: BLE001 - an unresolvable key just stays itself
            continue
        if found and found != key:
            out[key] = found
    return out


MARKET_NOTES: dict[Market, str] = {
    Market.CN: (
        "A-share bars are stored RAW (unadjusted); QFQ / HFQ are derived at query time and are not "
        "what verify scores. Suspended sessions keep a raw bar but have no canonical bar; "
        "LIMIT_UP / LIMIT_DOWN / SUSPENDED flags leave quality_status complete"
    ),
    Market.HK: (
        "Hong Kong bars are stored RAW; split-only adjustment is derived at query time and is not "
        "what verify scores"
    ),
}


def market_notes(market: Market | str) -> tuple[str, ...]:
    """Fixed per-market remarks for the report (empty for markets without any)."""
    note = MARKET_NOTES.get(Market(market))
    return (note,) if note else ()


# --- rendering ---------------------------------------------------------------------------------


def _pct(rate: float | None) -> str:
    return "n/a" if rate is None else f"{rate * 100:.2f}%"


def _stats_line(name: str, s: AgreementStats) -> str:
    return (
        f"  {name:<8} {_pct(s.rate):>8}  compared {s.compared} (match {s.match}, within tol {s.within_tolerance}, "
        f"divergent {s.divergent}); not comparable {s.not_comparable}, missing {s.missing}"
    )


def format_report(report: VerifyReport) -> list[str]:
    """Human-readable lines for the CLI."""
    w = report.window
    lines = [
        f"verify: {w.start}..{w.end}  universe {report.universe_size}  expected sessions {report.expected_sessions}",
        f"tolerances: price {report.tolerances.price_pct}%  volume {report.tolerances.volume_pct}%  "
        f"policy {report.policy_version}",
    ]
    lines += [f"note: {n}" for n in report.notes]
    lines.append("sources (provider/feed/adjustment):")
    if not report.sources:
        lines.append("  none")
    for s in report.sources:
        lines.append(
            f"  {s.provider}/{s.feed}/{s.adjustment}: {s.bars} bars, {s.symbols} symbols, "
            f"{s.oldest}..{s.newest}"
        )
    for period in report.periods:
        cov, q = period.coverage, period.quality
        span = f" {period.window.start}..{period.window.end}" if period.window else ""
        lines.append(f"[{period.label}]{span}  {cov.sessions} sessions x {cov.universe} instruments = {cov.expected_cells} cells")
        lines.append(
            f"  coverage canonical {_pct(cov.canonical_rate)} ({cov.canonical_cells}), "
            f"raw {_pct(cov.raw_rate)} ({cov.raw_cells}), missing {cov.missing_cells}"
            + (f", suspended {cov.suspended_cells} (expected)" if cov.suspended_cells else "")
        )
        for source, cells in sorted(cov.by_source.items()):
            rate = cells / cov.expected_cells if cov.expected_cells else None
            lines.append(f"    {source}: {_pct(rate)} ({cells})")
        lines.append(_stats_line("price", period.price))
        lines.append(_stats_line("close", period.close))
        lines.append(_stats_line("volume", period.volume))
        for pair in period.pairs:
            lines.append(
                f"    {pair.left} vs {pair.right}: price {_pct(pair.price.rate)} "
                f"volume {_pct(pair.volume.rate)} (not comparable {pair.price.not_comparable})"
            )
        status = ", ".join(f"{k}={v}" for k, v in sorted(q.distribution.items())) or "none"
        lines.append(f"  quality  {_pct(q.rate):>8}  canonical status: {status}")
        flags = ", ".join(f"{k}={v}" for k, v in sorted(q.flags.items())) or "none"
        lines.append(f"  flags    canonical rows with a quality flag: {flags}")
        lines.append(
            f"  counts   missing {cov.missing_cells}, not comparable {period.not_comparable_pairs}, "
            f"divergent price {period.price.divergent}, divergent volume {period.volume.divergent}, "
            f"single-source cells {period.single_source_cells}"
            + (f", suspended {cov.suspended_cells}" if cov.suspended_cells else "")
        )
    return lines
