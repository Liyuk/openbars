"""Planner: expected grid minus coverage = jobs, routed to the cheapest enabled source.

Routing per market x interval (only providers that declare the market/interval take part):

1. whole-market grouped providers (Massive): one job per XNYS session, ``instruments=("*",)``,
   only sessions ``massive_grouped_sessions`` would request (in the rolling window, ready,
   not covered);
2. multi-symbol history providers (Alpaca): symbol batches x bounded windows for what (1)
   does not cover, never before the provider's ``history_start``; the latest session is its
   own daily-increment job;
3. the fallback (yfinance): only what ``should_request_yfinance`` asks for -- pre-2016
   sessions and sessions no primary source covers or will cover.

Crypto (``Market.CRYPTO``) uses the 24/7 UTC calendar: every UTC date is a session and the
latest ready session is the last UTC day closed at least 30 minutes ago. Whole-market and
fallback routing are US-only; every crypto provider that declares ``crypto``/``1d`` takes part,
but a symbol is only sent to a provider whose symbol dialect it matches (see
``crypto_symbol_for_provider``), so Coinbase product ids never reach Binance or vice versa.

Priorities (lower runs first): latest-session daily jobs 0-10, other primary backfill
20-40, fallback 60-80.
"""

from __future__ import annotations

import math
import re
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Protocol
from zoneinfo import ZoneInfo

from openbars.calendars import crypto as crypto_calendar
from openbars.calendars import xnys
from openbars.config.schema import Selection
from openbars.core.models import DateRange, Interval, Market
from openbars.normalize.instruments import InstrumentMaster
from openbars.orchestrator.jobs import WHOLE_MARKET, Job, JobKind, make_job_id
from openbars.providers.base import BatchShape, Provider, SourceCapability
from openbars.providers.us.massive_window import ROLLING_WINDOW_DAYS, massive_grouped_sessions
from openbars.providers.us.yfinance import should_request_yfinance
from openbars.quota.policy import QuotaPolicy
from openbars.raw.store import RawStore
from openbars.reconcile.crypto import CRYPTO_COMPLETION_WAIT_MINUTES

NEW_YORK = ZoneInfo("America/New_York")
FALLBACK_PROVIDERS = frozenset({"yfinance"})
MAX_WINDOW_DAYS = 366  # bounded history window per multi-symbol job
BUDGET_ACCEPT_THRESHOLD = timedelta(hours=1)
REGULAR_SESSION_MINUTES = 390

PRIORITY_DAILY_WHOLE_MARKET = 0
PRIORITY_DAILY_MULTI = 5
PRIORITY_BACKFILL_WHOLE_MARKET = 20
PRIORITY_BACKFILL_MULTI = 40
PRIORITY_FALLBACK_GAP = 60
PRIORITY_FALLBACK_HISTORY = 80


# --- coverage ---------------------------------------------------------------------------


class CoverageIndex(Protocol):
    def covered(
        self,
        provider: str,
        market: Market,
        interval: Interval,
        window: DateRange,
        *,
        symbol: str | None = None,
    ) -> set[date]:
        """Sessions of ``window`` already stored for ``provider``.

        ``symbol=None`` asks for whole-market coverage (any bar of that session counts);
        otherwise only that symbol's sessions (plus whole-market ones) count.
        """
        ...


class EmptyCoverage:
    def covered(self, provider, market, interval, window, *, symbol=None) -> set[date]:  # noqa: ANN001
        return set()


class StaticCoverage:
    """Explicit covered keys: ``(provider, session)`` or ``(provider, session, symbol)``.

    A two-part key covers the session for every symbol (a whole-market fetch).
    """

    def __init__(self, keys: Iterable[tuple]) -> None:
        self._whole: dict[str, set[date]] = defaultdict(set)
        self._by_symbol: dict[tuple[str, str], set[date]] = defaultdict(set)
        for key in keys:
            if len(key) == 2:
                self._whole[key[0]].add(key[1])
            else:
                self._by_symbol[(key[0], key[2])].add(key[1])

    def covered(self, provider, market, interval, window, *, symbol=None) -> set[date]:  # noqa: ANN001
        days = set(self._whole.get(provider, ()))
        if symbol is not None:
            days |= self._by_symbol.get((provider, symbol), set())
        return {d for d in days if window.start <= d <= window.end}


class RawStoreCoverage:
    """Coverage derived from ``RawStore.scan`` (one scan per provider/market/interval/window)."""

    def __init__(self, raw: RawStore) -> None:
        self._raw = raw
        self._cache: dict[tuple, tuple[set[date], dict[str, set[date]]]] = {}

    def _load(
        self, provider: str, market: Market, interval: Interval, window: DateRange
    ) -> tuple[set[date], dict[str, set[date]]]:
        key = (provider, market, interval, window)
        if key not in self._cache:
            whole: set[date] = set()
            per_symbol: dict[str, set[date]] = defaultdict(set)
            for bar in self._raw.scan(provider, market, interval, window):
                day = bar.session_date or bar.ts_start.date()
                whole.add(day)
                per_symbol[bar.symbol_as_returned].add(day)
                if bar.instrument_id:
                    per_symbol[bar.instrument_id].add(day)
            self._cache[key] = (whole, per_symbol)
        return self._cache[key]

    def covered(self, provider, market, interval, window, *, symbol=None) -> set[date]:  # noqa: ANN001
        whole, per_symbol = self._load(provider, market, interval, window)
        return set(whole) if symbol is None else set(per_symbol.get(symbol, ()))


# --- plan / budget ------------------------------------------------------------------------


@dataclass(frozen=True)
class PlanNote:
    code: str  # no_provider | no_universe | no_symbols | no_calendar | nothing_to_do
    message: str


@dataclass(frozen=True)
class ProviderBudget:
    provider: str
    jobs: int
    requests: int
    min_duration: timedelta  # lower bound from the quota policy


@dataclass(frozen=True)
class Budget:
    providers: Mapping[str, ProviderBudget] = field(default_factory=dict)
    non_daily: bool = False  # some requested interval is not 1d

    @property
    def total_requests(self) -> int:
        return sum(p.requests for p in self.providers.values())

    @property
    def min_duration(self) -> timedelta:
        """Providers run side by side, so the slowest one bounds the whole plan."""
        return max((p.min_duration for p in self.providers.values()), default=timedelta(0))

    def requires_acceptance(self, threshold: timedelta = BUDGET_ACCEPT_THRESHOLD) -> bool:
        return self.non_daily and self.min_duration > threshold


@dataclass(frozen=True)
class Plan:
    jobs: tuple[Job, ...]
    budget: Budget
    notes: tuple[PlanNote, ...] = ()

    def __iter__(self):
        return iter(self.jobs)

    def __len__(self) -> int:
        return len(self.jobs)

    def counts(self) -> dict[tuple[str, str], int]:
        """Jobs by ``(provider, kind)``."""
        out: dict[tuple[str, str], int] = defaultdict(int)
        for job in self.jobs:
            out[(job.provider, job.kind.value)] += 1
        return dict(sorted(out.items()))


class Planner(Protocol):
    def plan(
        self,
        selection: Selection,
        coverage: CoverageIndex,
        *,
        now: datetime,
        universe: Mapping[Market, Sequence[str]] | Sequence[str] | None = None,
        providers: Mapping[str, Provider] | None = None,
    ) -> Plan: ...


# --- helpers --------------------------------------------------------------------------------


def split_windows(sessions: Sequence[date], max_days: int = MAX_WINDOW_DAYS) -> list[DateRange]:
    """Group sorted sessions into windows spanning fewer than ``max_days`` calendar days."""
    out: list[DateRange] = []
    start = prev = None
    for day in sessions:
        if start is None:
            start = day
        elif (day - start).days >= max_days:
            out.append(DateRange(start, prev))
            start = day
        prev = day
    if start is not None:
        out.append(DateRange(start, prev))
    return out


def _chunks(items: Sequence[str], size: int) -> list[tuple[str, ...]]:
    return [tuple(items[i : i + size]) for i in range(0, len(items), size)]


def _normalize_universe(
    universe: Mapping[Market, Sequence[str]] | Sequence[str] | None,
) -> dict[Market, list[str]]:
    if universe is None:
        return {}
    if not isinstance(universe, Mapping):
        universe = {Market.US: universe}
    return {
        Market(m): sorted({s.strip() for s in syms if s and s.strip()}) for m, syms in universe.items()
    }


def latest_ready_session(now: datetime, market: Market = Market.US) -> date | None:
    """Latest ready session of ``market``.

    US: latest XNYS session strictly before the New York date of ``now``. Crypto: latest UTC
    day that closed at least ``CRYPTO_COMPLETION_WAIT_MINUTES`` (30) before ``now``.
    Other markets have no calendar yet: ``None``.
    """
    market = Market(market)
    if market is Market.CRYPTO:
        return crypto_calendar.latest_completed_session(now, CRYPTO_COMPLETION_WAIT_MINUTES)
    if market is not Market.US:
        return None
    today = now.astimezone(NEW_YORK).date()
    found = xnys.sessions(today - timedelta(days=14), today - timedelta(days=1))
    return found[-1] if found else None


_latest_ready_session = latest_ready_session


def _has_calendar(market: Market) -> bool:
    return market in (Market.US, Market.CRYPTO)


def _sessions(market: Market, start: date, end: date) -> list[date]:
    """Sessions of the market's own calendar (XNYS for US, every UTC day for crypto)."""
    if Market(market) is Market.CRYPTO:
        return crypto_calendar.sessions(start, end)
    return xnys.sessions(start, end)


def _today(now: datetime, market: Market) -> date:
    """Calendar date of ``now`` in the market's clock (UTC for crypto, New York for US)."""
    return now.astimezone(UTC if market is Market.CRYPTO else NEW_YORK).date()


_BINANCE_STYLE = re.compile(r"[A-Z0-9]+")


def crypto_symbol_for_provider(provider: str, symbol: str) -> bool:
    """Heuristic (no instrument master): does ``symbol`` look like ``provider``'s dialect?

    ``coinbase`` product ids are ``BASE-QUOTE`` (contain ``-``); every other crypto provider
    (Binance) uses plain ``[A-Z0-9]+`` such as ``TESTUSDT``. A symbol matching neither is
    skipped everywhere: a skipped symbol is better than a failed job.
    """
    if provider == "coinbase":
        return "-" in symbol and symbol.replace("-", "").isalnum() and symbol == symbol.upper()
    return bool(_BINANCE_STYLE.fullmatch(symbol))


def _bars_per_session(interval: Interval) -> int:
    if not interval.is_intraday:
        return 1
    return max(1, math.ceil(REGULAR_SESSION_MINUTES / (interval.duration.total_seconds() / 60)))


def estimate_requests(job: Job, cap: SourceCapability | None) -> int:
    """Estimated HTTP requests for one job (lower bound; pagination may add more)."""
    if tuple(job.instruments) == WHOLE_MARKET:
        return 1
    if cap is None or not cap.max_bars_per_request:
        return 1
    sessions = len(_sessions(Market(job.market), job.window.start, job.window.end)) or 1
    bars = len(job.instruments) * sessions * _bars_per_session(Interval(job.interval))
    return max(1, math.ceil(bars / cap.max_bars_per_request))


def min_duration(policy: QuotaPolicy, requests: int) -> timedelta:
    """Lower bound: the slowest quota window needs ``requests / limit`` of its span."""
    seconds = 0.0
    for limit in policy.limits:
        seconds = max(seconds, requests / policy.effective_limit(limit) * limit.per.total_seconds())
    return timedelta(seconds=seconds)


# --- the planner ----------------------------------------------------------------------------


class DefaultPlanner:
    def __init__(
        self,
        *,
        universe: Mapping[Market, Sequence[str]] | Sequence[str] | None = None,
        providers: Mapping[str, Provider] | None = None,
        policies: Mapping[str, QuotaPolicy] | None = None,
        max_window_days: int = MAX_WINDOW_DAYS,
        instruments: InstrumentMaster | None = None,
    ) -> None:
        # ``instruments`` (optional): a crypto pair master. When given, a crypto symbol is
        # planned for a provider only if ``resolve(symbol, provider=...)`` hits; otherwise the
        # ``crypto_symbol_for_provider`` heuristic decides.
        self.instruments = instruments
        self.universe = universe
        self.providers = providers
        self.policies = policies
        self.max_window_days = max_window_days

    def plan(
        self,
        selection: Selection,
        coverage: CoverageIndex | None = None,
        *,
        now: datetime,
        universe: Mapping[Market, Sequence[str]] | Sequence[str] | None = None,
        providers: Mapping[str, Provider] | None = None,
    ) -> Plan:
        if now.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        coverage = coverage if coverage is not None else EmptyCoverage()
        providers = providers if providers is not None else (self.providers or {})
        symbols_by_market = _normalize_universe(universe if universe is not None else self.universe)
        jobs: dict[str, Job] = {}
        caps_by_job: dict[str, SourceCapability] = {}
        notes: list[PlanNote] = []

        for market in sorted(selection.markets, key=lambda m: m.value):
            for interval in sorted(selection.intervals, key=lambda i: i.value):
                self._plan_one(
                    selection, coverage, now, market, interval, providers,
                    symbols_by_market.get(market, []), jobs, caps_by_job, notes,
                )  # fmt: skip

        ordered = tuple(sorted(jobs.values(), key=lambda j: (j.priority, j.provider, j.job_id)))
        budget = self._budget(ordered, caps_by_job, providers, selection)
        return Plan(ordered, budget, tuple(notes))

    # -- per market x interval -----------------------------------------------------------
    def _plan_one(
        self,
        selection: Selection,
        coverage: CoverageIndex,
        now: datetime,
        market: Market,
        interval: Interval,
        providers: Mapping[str, Provider],
        symbols: list[str],
        jobs: dict[str, Job],
        caps_by_job: dict[str, SourceCapability],
        notes: list[PlanNote],
    ) -> None:
        label = f"{market.value}/{interval.value}"
        capable: dict[str, SourceCapability] = {}
        for name in sorted(providers):
            for cap in providers[name].capabilities():
                if cap.market == market and cap.interval == interval:
                    capable.setdefault(name, cap)
        if not capable:
            notes.append(PlanNote("no_provider", f"{label}: no enabled provider declares this market/interval; skipped"))
            return
        if not _has_calendar(market):
            notes.append(PlanNote("no_calendar", f"{label}: no trading calendar available yet; skipped"))
            return

        is_us = market is Market.US
        latest = latest_ready_session(now, market)
        today = _today(now, market)
        if latest is None:
            return
        end = min(selection.history_end or latest, latest)
        start = selection.history_start or self._default_start(capable, today)
        if end < start:
            notes.append(PlanNote("nothing_to_do", f"{label}: history window is empty"))
            return
        window = DateRange(start, end)
        all_sessions = _sessions(market, start, end)

        def emit(
            kind: JobKind, provider: str, instruments: tuple[str, ...], win: DateRange, priority: int
        ) -> None:
            job_id = make_job_id(kind, provider, market, interval, instruments, win)
            if job_id not in jobs:
                jobs[job_id] = Job(job_id, kind, provider, market, interval, instruments, win, priority)
                caps_by_job[job_id] = capable[provider]

        # 1. whole-market grouped providers -------------------------------------------------
        whole_done: set[date] = set()  # covered or planned for every symbol
        whole = [n for n, c in capable.items() if c.batch is BatchShape.WHOLE_MARKET_PER_DAY]
        if whole and is_us:
            name = whole[0]
            have = coverage.covered(name, market, interval, window)
            planned = massive_grouped_sessions(
                window, as_of=today, now=now, covered=have, sessions=all_sessions
            )
            for day in sorted(planned, reverse=True):
                daily = day == latest
                emit(
                    JobKind.DAILY if daily else JobKind.BACKFILL, name, WHOLE_MARKET,
                    DateRange(day, day),
                    PRIORITY_DAILY_WHOLE_MARKET if daily else PRIORITY_BACKFILL_WHOLE_MARKET,
                )  # fmt: skip
            whole_done = set(have) | set(planned)

        # 2/3. symbol based providers need a universe ----------------------------------------
        multi = [
            n for n, c in capable.items()
            if c.batch is not BatchShape.WHOLE_MARKET_PER_DAY
            and not (is_us and n in FALLBACK_PROVIDERS)
        ]  # fmt: skip
        fallback = [n for n in capable if is_us and n in FALLBACK_PROVIDERS]
        if (multi or fallback) and not symbols:
            notes.append(
                PlanNote(
                    "no_universe",
                    f"{label}: no symbols configured (universe.{market.value}.symbols); "
                    f"symbol-based providers skipped: {', '.join(sorted(multi + fallback))}",
                )
            )
            return

        primary_covered: dict[str, set[date]] = {s: set(whole_done) for s in symbols}
        for name in multi:
            cap = capable[name]
            floor = cap.history_start or start
            eligible = [d for d in all_sessions if d >= floor and d not in whole_done]
            if not eligible:
                continue
            span = DateRange(eligible[0], eligible[-1])
            groups: dict[tuple[date, date, JobKind], list[str]] = defaultdict(list)
            for sym in self._symbols_for(name, symbols, market, end, label, notes):
                have = coverage.covered(name, market, interval, span, symbol=sym)
                missing = [d for d in eligible if d not in have]
                primary_covered[sym] |= have | set(missing)  # covered now or by this plan
                for win, kind in self._windows(missing, latest):
                    groups[(win.start, win.end, kind)].append(sym)
            size = self._batch_size(name, cap)
            for (w_start, w_end, kind), syms in sorted(groups.items(), key=lambda kv: kv[0], reverse=True):
                for chunk in _chunks(syms, size):
                    emit(
                        kind, name, chunk, DateRange(w_start, w_end),
                        PRIORITY_DAILY_MULTI if kind is JobKind.DAILY else PRIORITY_BACKFILL_MULTI,
                    )  # fmt: skip

        for name in fallback:
            cap = capable[name]
            groups_fb: dict[tuple[date, date], list[str]] = defaultdict(list)
            for sym in symbols:
                have = coverage.covered(name, market, interval, window, symbol=sym)
                candidates = [d for d in all_sessions if d not in have]
                for rng in should_request_yfinance(
                    window, primary_covered=primary_covered[sym], sessions=candidates
                ):
                    groups_fb[(rng.start, rng.end)].append(sym)
            size = self._batch_size(name, cap)
            for (w_start, w_end), syms in sorted(groups_fb.items(), reverse=True):
                pre_history = w_start < date(2016, 1, 1)
                for chunk in _chunks(syms, size):
                    emit(
                        JobKind.BACKFILL, name, chunk, DateRange(w_start, w_end),
                        PRIORITY_FALLBACK_HISTORY if pre_history else PRIORITY_FALLBACK_GAP,
                    )  # fmt: skip

    def _symbols_for(
        self, name: str, symbols: list[str], market: Market, on: date, label: str, notes: list[PlanNote]
    ) -> list[str]:
        """Symbols ``name`` can serve. US: all. Crypto: only its own symbol dialect."""
        if market is not Market.CRYPTO:
            return symbols
        if self.instruments is not None:
            ok = [
                s for s in symbols
                if self.instruments.resolve(s, on=on, provider=name, market=Market.CRYPTO) is not None
            ]  # fmt: skip
        else:
            ok = [s for s in symbols if crypto_symbol_for_provider(name, s)]
        skipped = [s for s in symbols if s not in ok]
        if skipped and not ok:
            notes.append(
                PlanNote("no_symbols", f"{label}: no configured symbol fits {name}; provider skipped")
            )
        # Skipping a symbol for one provider is routine (the other venue's symbol): no note.
        return ok

    @staticmethod
    def _default_start(capable: Mapping[str, SourceCapability], today: date) -> date:
        starts = []
        for cap in capable.values():
            if cap.rolling_window is not None:
                starts.append(today - cap.rolling_window)
            elif cap.history_start is not None:
                starts.append(cap.history_start)
        return min(starts) if starts else today - timedelta(days=ROLLING_WINDOW_DAYS)

    def _windows(self, missing: list[date], latest: date) -> list[tuple[DateRange, JobKind]]:
        out: list[tuple[DateRange, JobKind]] = []
        rest = list(missing)
        if rest and rest[-1] == latest:
            rest.pop()
            out.append((DateRange(latest, latest), JobKind.DAILY))
        out.extend((w, JobKind.BACKFILL) for w in split_windows(rest, self.max_window_days))
        return out

    def _batch_size(self, name: str, cap: SourceCapability) -> int:
        sizes = [cap.max_symbols_per_request]
        policy = self._policy(name, None)
        if policy is not None and policy.max_batch_symbols:
            sizes.append(policy.max_batch_symbols)
        return max(1, min(s for s in sizes if s))

    def _policy(self, name: str, provider: Provider | None) -> QuotaPolicy | None:
        if self.policies and name in self.policies:
            return self.policies[name]
        return provider.quota_policy() if provider is not None else None

    # -- budget -----------------------------------------------------------------------------
    def _budget(
        self,
        jobs: Sequence[Job],
        caps: Mapping[str, SourceCapability],
        providers: Mapping[str, Provider],
        selection: Selection,
    ) -> Budget:
        job_counts: dict[str, int] = defaultdict(int)
        requests: dict[str, int] = defaultdict(int)
        for job in jobs:
            job_counts[job.provider] += 1
            requests[job.provider] += estimate_requests(job, caps.get(job.job_id))
        out: dict[str, ProviderBudget] = {}
        for name in sorted(job_counts):
            policy = self._policy(name, providers.get(name))
            duration = min_duration(policy, requests[name]) if policy is not None else timedelta(0)
            out[name] = ProviderBudget(name, job_counts[name], requests[name], duration)
        return Budget(out, non_daily=any(i.is_intraday for i in selection.intervals))
