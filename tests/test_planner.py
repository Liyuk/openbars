from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from fakes import FakeProvider, MemoryRawStore

from openbars.config.schema import Selection
from openbars.core.models import DateRange, Interval, Market
from openbars.orchestrator.jobs import WHOLE_MARKET, JobKind, JobState, make_job_id
from openbars.orchestrator.planner import (
    DefaultPlanner,
    EmptyCoverage,
    RawStoreCoverage,
    StaticCoverage,
    split_windows,
)
from openbars.providers.base import BatchShape
from openbars.storage.state import SqliteStateStore

pytestmark = pytest.mark.usefixtures("no_network")

# Tuesday 2026-03-10 11:00 America/New_York; the latest ready session is Monday 2026-03-09.
NOW = datetime(2026, 3, 10, 15, 0, tzinfo=UTC)
LATEST = date(2026, 3, 9)


def massive(**kw) -> FakeProvider:
    return FakeProvider(
        "massive", batch=BatchShape.WHOLE_MARKET_PER_DAY, rolling_window=timedelta(days=730), max_symbols=10**6, **kw
    )


def alpaca(**kw) -> FakeProvider:
    return FakeProvider("alpaca", history_start=date(2016, 1, 1), max_symbols=200, **kw)


def yfinance(**kw) -> FakeProvider:
    return FakeProvider("yfinance", max_symbols=50, **kw)


def sel(start: date, end: date | None = None, intervals=frozenset({Interval.D1})) -> Selection:
    return Selection(frozenset({Market.US}), intervals, start, end)


def plan(selection, providers, coverage=None, universe=("TEST1", "ZZZA"), now=NOW):
    return DefaultPlanner().plan(
        selection, coverage or EmptyCoverage(), now=now, universe=universe,
        providers={p.id: p for p in providers},
    )  # fmt: skip


def by(p, provider):
    return [j for j in p.jobs if j.provider == provider]


def test_massive_one_job_per_session_not_per_symbol():
    p = plan(sel(date(2026, 3, 2), date(2026, 3, 31)), [massive()], universe=["TEST1", "ZZZA", "TEST2"])
    jobs = by(p, "massive")
    assert sorted(j.window.start for j in jobs) == [date(2026, 3, d) for d in (2, 3, 4, 5, 6, 9)]
    assert all(j.window.start == j.window.end for j in jobs)
    assert all(j.instruments == WHOLE_MARKET for j in jobs)
    daily = [j for j in jobs if j.kind is JobKind.DAILY]
    assert [j.window.start for j in daily] == [LATEST]  # end clipped to the last ready session
    assert daily[0].priority < min(j.priority for j in jobs if j.kind is JobKind.BACKFILL)
    assert p.budget.providers["massive"].requests == 6


def test_weekend_and_holiday_sessions_skipped():
    # Jan 17-18 weekend, Jan 19 market holiday.
    p = plan(sel(date(2026, 1, 15), date(2026, 1, 21)), [massive()])
    assert sorted(j.window.start for j in p.jobs) == [date(2026, 1, d) for d in (15, 16, 20, 21)]


def test_dates_outside_massive_rolling_window_not_planned():
    p = plan(sel(date(2020, 1, 2), date(2026, 3, 9)), [massive()])
    floor = NOW.date() - timedelta(days=730)
    assert p.jobs and min(j.window.start for j in p.jobs) >= floor
    old = plan(sel(date(2020, 1, 2), date(2020, 1, 10)), [massive()])
    assert len(old) == 0


def test_massive_session_not_ready_is_not_planned():
    # 15:00 UTC on the session day itself: still before 00:00 ET of D+1.
    p = plan(sel(date(2026, 3, 9), date(2026, 3, 10)), [massive()], now=datetime(2026, 3, 10, 3, 30, tzinfo=UTC))
    assert len(p) == 0  # 2026-03-09 23:30 ET: Monday is not ready yet


def test_alpaca_jobs_are_symbol_batched():
    symbols = [f"S{i:03d}" for i in range(450)]
    p = plan(sel(date(2026, 3, 2), date(2026, 3, 9)), [alpaca()], universe=symbols)
    jobs = by(p, "alpaca")
    assert sorted(len(j.instruments) for j in jobs) == [50, 50, 200, 200, 200, 200]
    daily = [j for j in jobs if j.kind is JobKind.DAILY]
    backfill = [j for j in jobs if j.kind is JobKind.BACKFILL]
    assert len(daily) == 3 and all(j.window == DateRange(LATEST, LATEST) for j in daily)
    assert len(backfill) == 3 and all(j.window == DateRange(date(2026, 3, 2), date(2026, 3, 6)) for j in backfill)
    assert {s for j in jobs for s in j.instruments} == set(symbols)


def test_alpaca_history_windows_bounded_and_not_before_2016():
    p = plan(sel(date(2015, 6, 1), date(2018, 12, 31)), [alpaca()])
    jobs = by(p, "alpaca")
    assert min(j.window.start for j in jobs) >= date(2016, 1, 1)
    assert all((j.window.end - j.window.start).days < 366 for j in jobs)
    assert len(jobs) == 3  # 2016, 2017, 2018 (symbols fit in one batch)


def test_symbol_providers_need_a_universe_but_massive_still_plans():
    p = plan(sel(date(2026, 3, 2), date(2026, 3, 9)), [massive(), alpaca(), yfinance()], universe=None)
    assert {j.provider for j in p.jobs} == {"massive"}
    assert any(n.code == "no_universe" for n in p.notes)


def test_massive_takes_precedence_over_alpaca():
    p = plan(sel(date(2026, 3, 2), date(2026, 3, 9)), [massive(), alpaca()])
    assert {j.provider for j in p.jobs} == {"massive"}


def test_alpaca_covers_sessions_massive_cannot():
    p = plan(sel(date(2023, 1, 3), date(2023, 1, 6)), [massive(), alpaca()])
    assert {j.provider for j in p.jobs} == {"alpaca"}


def test_yfinance_only_pre_2016_with_primaries_enabled():
    syms = [f"S{i:03d}" for i in range(120)]
    p = plan(sel(date(2015, 12, 28), date(2016, 1, 6)), [massive(), alpaca(), yfinance()], universe=syms)
    yf = by(p, "yfinance")
    assert sorted(len(j.instruments) for j in yf) == [20, 50, 50]
    assert {j.window for j in yf} == {DateRange(date(2015, 12, 28), date(2015, 12, 31))}
    assert min(j.priority for j in yf) >= 60
    assert all(j.window.start >= date(2016, 1, 4) for j in by(p, "alpaca"))  # Jan 1 is a holiday


def test_yfinance_fills_gaps_no_primary_can_cover():
    # Alpaca disabled; Massive's window cannot reach 2020.
    p = plan(sel(date(2020, 1, 2), date(2020, 1, 10)), [massive(), yfinance()])
    yf = by(p, "yfinance")
    assert len(yf) == 1 and yf[0].window == DateRange(date(2020, 1, 2), date(2020, 1, 10))
    assert yf[0].instruments == ("TEST1", "ZZZA") and yf[0].kind is JobKind.BACKFILL
    # ... but not for sessions the planned primary covers.
    recent = plan(sel(date(2026, 3, 2), date(2026, 3, 9)), [massive(), yfinance()])
    assert {j.provider for j in recent.jobs} == {"massive"}


def test_no_capable_provider_is_a_note_not_a_crash():
    p = plan(sel(date(2026, 3, 2), date(2026, 3, 9), intervals=frozenset({Interval.H1})), [massive(), alpaca()])
    assert len(p) == 0 and p.notes[0].code == "no_provider"
    crypto = DefaultPlanner().plan(
        Selection(frozenset({Market.CRYPTO}), history_start=date(2026, 3, 2)), EmptyCoverage(), now=NOW,
        providers={"massive": massive()},
    )  # fmt: skip
    assert len(crypto) == 0 and crypto.notes


def test_planning_twice_yields_identical_job_ids_and_submit_is_idempotent(tmp_path):
    args = (sel(date(2026, 3, 2), date(2026, 3, 9)), [massive(), alpaca()])
    a, b = plan(*args), plan(*args)
    assert [j.job_id for j in a] == [j.job_id for j in b]
    store = SqliteStateStore(tmp_path / "state.db")
    assert store.submit_jobs(a.jobs) == len(a)
    assert store.submit_jobs(b.jobs) == 0
    assert len(store.list_jobs()) == len(a)


def test_submit_does_not_reset_done_dead_or_failed_jobs(tmp_path):
    from dataclasses import replace

    store = SqliteStateStore(tmp_path / "state.db")
    jobs = plan(sel(date(2026, 3, 2), date(2026, 3, 4)), [massive()]).jobs
    store.submit_jobs(jobs)
    for job, state in zip(jobs, (JobState.DONE, JobState.DEAD, JobState.FAILED)):
        store.save_job(replace(job, state=state, attempts=2, last_error="x"))
    assert store.submit_jobs(jobs) == 0
    states = {j.job_id: (j.state, j.attempts) for j in store.list_jobs()}
    assert states[jobs[0].job_id] == (JobState.DONE, 2)
    assert states[jobs[1].job_id] == (JobState.DEAD, 2)
    assert states[jobs[2].job_id] == (JobState.FAILED, 2)


def test_job_id_is_stable_and_field_sensitive():
    base = (JobKind.DAILY, "massive", Market.US, Interval.D1, WHOLE_MARKET, DateRange(LATEST, LATEST))
    assert make_job_id(*base) == make_job_id(*base)
    assert len(make_job_id(*base)) >= 16
    assert make_job_id(JobKind.BACKFILL, *base[1:]) != make_job_id(*base)
    assert make_job_id(*base[:1], "alpaca", *base[2:]) != make_job_id(*base)
    assert make_job_id(*base[:4], ("A",), base[5]) != make_job_id(*base[:4], ("B",), base[5])


def test_static_coverage_drops_covered_sessions():
    covered = StaticCoverage([("massive", date(2026, 3, 2)), ("massive", date(2026, 3, 9))])
    p = plan(sel(date(2026, 3, 2), date(2026, 3, 9)), [massive()], coverage=covered)
    assert sorted(j.window.start for j in p.jobs) == [date(2026, 3, d) for d in (3, 4, 5, 6)]
    assert all(j.kind is JobKind.BACKFILL for j in p.jobs)  # the latest session is covered


def test_static_coverage_per_symbol_for_alpaca():
    days = [date(2026, 1, d) for d in (5, 6, 7, 8, 9)]
    covered = StaticCoverage([("alpaca", d, "TEST1") for d in days])
    p = plan(sel(date(2026, 1, 5), date(2026, 1, 9)), [alpaca()], coverage=covered)
    assert [j.instruments for j in p.jobs] == [("ZZZA",)]


def test_raw_store_coverage_drops_what_was_fetched():
    raw = MemoryRawStore()
    fake = massive()
    from openbars.providers.base import FetchRequest, RawBatch

    req = FetchRequest(Market.US, Interval.D1, (), DateRange(date(2026, 3, 3), date(2026, 3, 4)))
    raw.append(RawBatch("massive", "f1", req, tuple(fake.fetch_bars(req).bars)))
    p = plan(sel(date(2026, 3, 2), date(2026, 3, 5)), [massive()], coverage=RawStoreCoverage(raw))
    assert sorted(j.window.start for j in p.jobs) == [date(2026, 3, 2), date(2026, 3, 5)]


def test_budget_estimates_requests_and_duration():
    p = plan(sel(date(2026, 3, 2), date(2026, 3, 9)), [massive(limit_per_minute=4)])
    b = p.budget.providers["massive"]
    assert (b.jobs, b.requests) == (6, 6)
    assert b.min_duration >= timedelta(minutes=1)  # 6 requests at an effective 3/min
    assert not p.budget.requires_acceptance()  # daily only


def test_non_daily_budget_requires_acceptance_when_large():
    h1 = FakeProvider("hourly", interval=Interval.H1, max_symbols=200, limit_per_minute=2)
    p = plan(
        sel(date(2026, 1, 5), date(2026, 3, 9), intervals=frozenset({Interval.H1})),
        [h1], universe=[f"S{i:03d}" for i in range(200)],
    )  # fmt: skip
    assert p.budget.non_daily and p.budget.min_duration > timedelta(0)
    assert p.budget.requires_acceptance(threshold=timedelta(seconds=1))
    assert not p.budget.requires_acceptance(threshold=timedelta(days=365))


def test_split_windows():
    days = [date(2024, 1, 1) + timedelta(days=i) for i in range(0, 800, 40)]
    windows = split_windows(days, 366)
    assert windows[0].start == days[0] and windows[-1].end == days[-1]
    assert all((w.end - w.start).days < 366 for w in windows)


# --- crypto ---------------------------------------------------------------------------------

CRYPTO_NOW = datetime(2026, 3, 10, 0, 29, tzinfo=UTC)  # 29 min after UTC midnight: Mar 9 not ready
CRYPTO_READY = datetime(2026, 3, 10, 0, 30, tzinfo=UTC)  # 30 min wait elapsed: Mar 9 is ready


def crypto_sel(start: date, end: date | None = None) -> Selection:
    return Selection(frozenset({Market.CRYPTO}), frozenset({Interval.D1}), start, end)


def crypto_plan(selection, providers, now=CRYPTO_READY, universe=("TESTUSDT", "TEST-USDT"), **kw):
    from crypto_fakes import CryptoFake  # noqa: F401

    return DefaultPlanner(**kw).plan(
        selection, EmptyCoverage(), now=now, universe={Market.CRYPTO: universe},
        providers={p.id: p for p in providers},
    )  # fmt: skip


def test_latest_ready_session_crypto_is_utc_with_wait():
    from openbars.orchestrator.planner import latest_ready_session

    assert latest_ready_session(CRYPTO_NOW, Market.CRYPTO) == date(2026, 3, 8)
    assert latest_ready_session(CRYPTO_READY, Market.CRYPTO) == date(2026, 3, 9)
    # US path is unchanged (New York based, XNYS sessions only)
    assert latest_ready_session(NOW) == LATEST == latest_ready_session(NOW, Market.US)


def test_crypto_routes_each_symbol_to_its_own_venue_and_includes_weekends():
    from crypto_fakes import CryptoFake

    # Sat 2026-03-07 and Sun 2026-03-08 are sessions.
    p = crypto_plan(
        crypto_sel(date(2026, 3, 6), date(2026, 3, 31)), [CryptoFake("binance"), CryptoFake("coinbase")]
    )
    assert not [n for n in p.notes if n.code in {"no_calendar", "no_provider"}]
    seen = {(j.provider, j.instruments) for j in p.jobs}
    assert seen == {("binance", ("TESTUSDT",)), ("coinbase", ("TEST-USDT",))}
    for name in ("binance", "coinbase"):
        jobs = by(p, name)
        days = {d for j in jobs for d in (j.window.start + timedelta(days=i) for i in range((j.window.end - j.window.start).days + 1))}
        assert days == {date(2026, 3, 6) + timedelta(days=i) for i in range(4)}  # Fri..Mon, ready only
        assert {j.kind for j in jobs} == {JobKind.DAILY, JobKind.BACKFILL}
        assert all(j.market is Market.CRYPTO for j in jobs)
        daily = [j for j in jobs if j.kind is JobKind.DAILY]
        assert [j.window for j in daily] == [DateRange(date(2026, 3, 9), date(2026, 3, 9))]


def test_crypto_session_not_ready_is_not_planned():
    from crypto_fakes import CryptoFake

    p = crypto_plan(
        crypto_sel(date(2026, 3, 8), date(2026, 3, 9)), [CryptoFake("binance")], now=CRYPTO_NOW
    )
    assert [(j.window.start, j.window.end) for j in p.jobs] == [(date(2026, 3, 8), date(2026, 3, 8))]


def test_crypto_budget_counts_weekend_sessions():
    from crypto_fakes import CryptoFake

    p = crypto_plan(crypto_sel(date(2026, 3, 1), date(2026, 3, 9)), [CryptoFake("binance")], universe=["TESTUSDT"])
    assert p.budget.providers["binance"].jobs == 2  # latest daily + one backfill window


def test_crypto_never_uses_yfinance_or_us_providers():
    from crypto_fakes import CryptoFake

    p = crypto_plan(crypto_sel(date(2026, 3, 8), date(2026, 3, 9)), [CryptoFake("binance"), yfinance(), massive()])
    assert {j.provider for j in p.jobs} == {"binance"}
    assert all(j.market is Market.CRYPTO for j in p.jobs)


def test_crypto_symbol_with_no_matching_provider_is_a_note_not_a_job():
    from crypto_fakes import CryptoFake

    p = crypto_plan(crypto_sel(date(2026, 3, 8), date(2026, 3, 9)), [CryptoFake("binance")], universe=["TEST-USDT"])
    assert p.jobs == ()
    assert any(n.code == "no_symbols" for n in p.notes)


def test_crypto_instrument_master_resolver_decides_when_given():
    from crypto_fakes import CryptoFake

    from openbars.normalize.crypto_pairs import CryptoPairListing, ingest_crypto_pairs
    from openbars.normalize.instruments import MemoryInstrumentMaster

    master = MemoryInstrumentMaster()
    ingest_crypto_pairs(
        master,
        [CryptoPairListing("binance", "ZZZ", "USDT", "binance", "ZZZUSDT"),
         CryptoPairListing("coinbase", "ZZZ", "USDT", "coinbase", "ZZZ-USDT")],
        as_of=date(2026, 1, 1),
    )  # fmt: skip
    p = crypto_plan(
        crypto_sel(date(2026, 3, 8), date(2026, 3, 9)), [CryptoFake("binance"), CryptoFake("coinbase")],
        universe=["ZZZUSDT", "ZZZ-USDT", "TESTUSDT"], instruments=master,
    )  # fmt: skip
    assert {(j.provider, j.instruments) for j in p.jobs} == {
        ("binance", ("ZZZUSDT",)), ("coinbase", ("ZZZ-USDT",))
    }  # TESTUSDT is unknown to the master: not planned


class _HkFake(FakeProvider):
    def capabilities(self):
        from dataclasses import replace

        return tuple(replace(c, market=Market.HK) for c in super().capabilities())


def _hk_plan(provider, universe=("00700",), now=None):
    return DefaultPlanner().plan(
        Selection(frozenset({Market.HK}), frozenset({Interval.D1}), date(2026, 3, 2), None),
        EmptyCoverage(), now=now or NOW, universe={Market.HK: list(universe)},
        providers={provider.id: provider},
    )  # fmt: skip


def test_other_markets_are_skipped_without_jobs():
    p = _hk_plan(
        FakeProvider("fake"), universe=("00700",)
    )  # declares US only -> no_provider
    assert p.jobs == () and any(n.code == "no_provider" for n in p.notes)


# --- Hong Kong (Market.HK): XHKG calendar, Asia/Hong_Kong clock, yfinance only -----------------------

HONG_KONG = ZoneInfo("Asia/Hong_Kong")
HK_NOW = datetime(
    2026, 3, 6, 17, 0, tzinfo=HONG_KONG
)  # Friday, 1h after the 16:00 close


def hk_sel(start: date, end: date | None = None) -> Selection:
    return Selection(frozenset({Market.HK}), frozenset({Interval.D1}), start, end)


def hk_plan(selection, providers, coverage=None, universe=("00700",), now=HK_NOW):
    return DefaultPlanner().plan(
        selection, coverage or EmptyCoverage(), now=now, universe={Market.HK: list(universe)},
        providers={p.id: p for p in providers},
    )  # fmt: skip


def test_hk_plans_yfinance_jobs_without_no_calendar_note():
    p = hk_plan(hk_sel(date(2026, 3, 2), date(2026, 3, 6)), [_HkFake("yfinance")])
    assert not [n for n in p.notes if n.code == "no_calendar"]
    assert p.jobs and {j.provider for j in p.jobs} == {"yfinance"}
    assert all(j.market is Market.HK and j.instruments == ("00700",) for j in p.jobs)
    covered = sorted(d for j in p.jobs for d in (j.window.start, j.window.end))
    assert covered[0] == date(2026, 3, 2) and covered[-1] == date(2026, 3, 6)
    daily = [j for j in p.jobs if j.kind is JobKind.DAILY]
    assert [j.window for j in daily] == [DateRange(date(2026, 3, 6), date(2026, 3, 6))]


def test_hk_weekend_and_holiday_windows_contain_only_xhkg_sessions():
    # Around Lunar New Year 2026, mid-week closures (e.g. 2026-02-17) are not sessions
    p = hk_plan(
        hk_sel(date(2026, 2, 12), date(2026, 3, 2)),
        [_HkFake("yfinance")],
        now=datetime(2026, 3, 3, 9, 0, tzinfo=HONG_KONG),
    )
    from openbars.calendars import hk

    expected = hk.sessions(date(2026, 2, 12), date(2026, 3, 2))
    assert date(2026, 2, 17) not in expected
    for j in p.jobs:
        assert hk.is_session(j.window.start) and hk.is_session(j.window.end)
    assert min(j.window.start for j in p.jobs) == expected[0]
    assert max(j.window.end for j in p.jobs) == expected[-1] == date(2026, 3, 2)


def test_hk_covered_sessions_are_not_planned_again_whatever_the_spelling():
    cov = StaticCoverage(
        [
            ("yfinance", d, spelling)
            for d in (date(2026, 3, 2), date(2026, 3, 3), date(2026, 3, 4))
            for spelling in ("0700.HK", "00700")
        ]
    )
    p = hk_plan(
        hk_sel(date(2026, 3, 2), date(2026, 3, 6)),
        [_HkFake("yfinance")],
        coverage=cov,
        universe=["0700.HK"],
    )
    assert [j.window for j in p.jobs if j.kind is JobKind.BACKFILL] == [
        DateRange(date(2026, 3, 5), date(2026, 3, 5))
    ]
    assert [j.instruments for j in p.jobs] == [("0700.HK",)] * len(p.jobs)


def test_hk_duplicate_spellings_plan_once_and_invalid_symbols_are_skipped():
    p = hk_plan(
        hk_sel(date(2026, 3, 5), date(2026, 3, 6)),
        [_HkFake("yfinance")],
        universe=["00700", "0700.HK", "hk:eq:xhkg:00700", "NOT_A_TICKER"],
    )
    assert all(len(j.instruments) == 1 for j in p.jobs)
    planned = {j.instruments[0] for j in p.jobs}
    assert planned <= {"00700", "0700.HK", "hk:eq:xhkg:00700"}
    assert len(planned) == 1
    assert any(n.code == "no_symbols" and "NOT_A_TICKER" in n.message for n in p.notes)


def test_hk_latest_ready_session_waits_thirty_minutes_after_the_actual_close():
    from openbars.orchestrator.planner import latest_ready_session

    fri = date(2026, 3, 6)
    assert latest_ready_session(
        datetime(2026, 3, 6, 16, 29, tzinfo=HONG_KONG), Market.HK
    ) == date(2026, 3, 5)
    assert (
        latest_ready_session(datetime(2026, 3, 6, 16, 30, tzinfo=HONG_KONG), Market.HK)
        == fri
    )
    # 08:30 UTC == 16:30 Hong Kong (HKT, no DST)
    assert (
        latest_ready_session(datetime(2026, 3, 6, 8, 30, tzinfo=UTC), Market.HK) == fri
    )
    assert (
        latest_ready_session(datetime(2026, 3, 8, 12, 0, tzinfo=HONG_KONG), Market.HK)
        == fri
    )  # weekend
    # Christmas Eve 2025 is a half day (close 12:00 HKT)
    eve = date(2025, 12, 24)
    assert (
        latest_ready_session(
            datetime(2025, 12, 24, 12, 29, tzinfo=HONG_KONG), Market.HK
        )
        != eve
    )
    assert (
        latest_ready_session(
            datetime(2025, 12, 24, 12, 30, tzinfo=HONG_KONG), Market.HK
        )
        == eve
    )


def test_hk_job_is_not_planned_before_the_close_and_dates_use_the_hong_kong_day():
    early = datetime(
        2026, 3, 6, 16, 10, tzinfo=HONG_KONG
    )  # Friday, still inside the wait
    p = hk_plan(
        hk_sel(date(2026, 3, 2), date(2026, 3, 6)), [_HkFake("yfinance")], now=early
    )
    assert max(j.window.end for j in p.jobs) == date(2026, 3, 5)
    # 2026-03-06 23:00 UTC is already Saturday 07:00 in Hong Kong (NY and UTC are still Friday)
    sat = hk_plan(
        hk_sel(date(2026, 3, 2), date(2026, 3, 6)),
        [_HkFake("yfinance")],
        now=datetime(2026, 3, 6, 23, 0, tzinfo=UTC),
    )
    assert max(j.window.end for j in sat.jobs) == date(2026, 3, 6)


def test_wiring_lists_yfinance_for_hk():
    from openbars.cli.wiring import _PROVIDER_MARKETS

    assert Market.HK in _PROVIDER_MARKETS["yfinance"]
    assert "akshare" not in _PROVIDER_MARKETS


def test_hk_planner_uses_only_yfinance_even_if_akshare_is_enabled():
    yf = _HkFake("yfinance")
    ak = _HkFake("akshare")
    p = hk_plan(hk_sel(date(2026, 3, 5), date(2026, 3, 6)), [yf, ak])
    assert {j.provider for j in p.jobs} == {"yfinance"}
    only_ak = hk_plan(hk_sel(date(2026, 3, 5), date(2026, 3, 6)), [ak])
    assert only_ak.jobs == () and any(n.code == "no_provider" for n in only_ak.notes)


# --- A-share (Market.CN): XSHG calendar, Asia/Shanghai clock -----------------------------------------

SHANGHAI = ZoneInfo("Asia/Shanghai")
CN_NOW = datetime(2026, 3, 6, 16, 0, tzinfo=SHANGHAI)  # Friday, 1h after the 15:00 close


def cn_sel(start: date, end: date | None = None) -> Selection:
    return Selection(frozenset({Market.CN}), frozenset({Interval.D1}), start, end)


def cn_plan(selection, providers, coverage=None, universe=("sh.600000",), now=CN_NOW):
    return DefaultPlanner().plan(
        selection, coverage or EmptyCoverage(), now=now, universe={Market.CN: list(universe)},
        providers={p.id: p for p in providers},
    )  # fmt: skip


def test_cn_plans_jobs_with_no_calendar_note():
    from cn_fakes import CnFake

    p = cn_plan(cn_sel(date(2026, 3, 2), date(2026, 3, 6)), [CnFake()])
    assert not [n for n in p.notes if n.code == "no_calendar"]
    assert p.jobs and {j.provider for j in p.jobs} == {"baostock"}
    assert all(j.market is Market.CN and j.instruments == ("sh.600000",) for j in p.jobs)
    covered = sorted(d for j in p.jobs for d in (j.window.start, j.window.end))
    assert covered[0] == date(2026, 3, 2) and covered[-1] == date(2026, 3, 6)
    daily = [j for j in p.jobs if j.kind is JobKind.DAILY]
    assert [j.window for j in daily] == [DateRange(date(2026, 3, 6), date(2026, 3, 6))]


def test_cn_weekend_and_holiday_windows_contain_only_xshg_sessions():
    from cn_fakes import CnFake

    # Spring Festival 2026: the exchange is closed 2/16-2/23 (weekdays 2/16-2/20 included)
    p = cn_plan(cn_sel(date(2026, 2, 12), date(2026, 3, 2)), [CnFake()], now=datetime(2026, 3, 3, 9, 0, tzinfo=SHANGHAI))
    from openbars.calendars import cn

    expected = cn.sessions(date(2026, 2, 12), date(2026, 3, 2))
    assert date(2026, 2, 17) not in expected
    # windows start and end on sessions, never on a weekend or holiday
    for j in p.jobs:
        assert cn.is_session(j.window.start) and cn.is_session(j.window.end)
    assert min(j.window.start for j in p.jobs) == expected[0]
    assert max(j.window.end for j in p.jobs) == expected[-1] == date(2026, 3, 2)


def test_cn_covered_sessions_are_not_planned_again_whatever_the_spelling():
    from cn_fakes import CnFake

    cov = StaticCoverage([("baostock", d, "sh.600000") for d in (date(2026, 3, 2), date(2026, 3, 3), date(2026, 3, 4))])
    p = cn_plan(cn_sel(date(2026, 3, 2), date(2026, 3, 6)), [CnFake()], coverage=cov, universe=["600000.SH"])
    assert [j.window for j in p.jobs if j.kind is JobKind.BACKFILL] == [DateRange(date(2026, 3, 5), date(2026, 3, 5))]
    assert [j.instruments for j in p.jobs] == [("600000.SH",)] * len(p.jobs)  # spelled as configured


def test_cn_latest_ready_session_waits_thirty_minutes_after_the_close():
    from openbars.orchestrator.planner import latest_ready_session

    fri = date(2026, 3, 6)
    assert latest_ready_session(datetime(2026, 3, 6, 15, 29, tzinfo=SHANGHAI), Market.CN) == date(2026, 3, 5)
    assert latest_ready_session(datetime(2026, 3, 6, 15, 30, tzinfo=SHANGHAI), Market.CN) == fri
    # an aware UTC timestamp is read on the Shanghai clock: 07:30 UTC == 15:30 Shanghai
    assert latest_ready_session(datetime(2026, 3, 6, 7, 30, tzinfo=UTC), Market.CN) == fri
    assert latest_ready_session(datetime(2026, 3, 8, 12, 0, tzinfo=SHANGHAI), Market.CN) == fri  # weekend


def test_cn_job_is_not_planned_before_the_close_and_dates_use_the_shanghai_day():
    from cn_fakes import CnFake

    early = datetime(2026, 3, 6, 15, 10, tzinfo=SHANGHAI)  # Friday, still inside the wait
    p = cn_plan(cn_sel(date(2026, 3, 2), date(2026, 3, 6)), [CnFake()], now=early)
    assert max(j.window.end for j in p.jobs) == date(2026, 3, 5)
    # 2026-03-06 23:00 UTC is already Saturday 07:00 in Shanghai (NY and UTC are still Friday)
    sat = cn_plan(cn_sel(date(2026, 3, 2), date(2026, 3, 6)), [CnFake()], now=datetime(2026, 3, 6, 23, 0, tzinfo=UTC))
    assert max(j.window.end for j in sat.jobs) == date(2026, 3, 6)


def test_cn_never_uses_us_providers_or_yfinance():
    from cn_fakes import CnFake

    p = cn_plan(cn_sel(date(2026, 3, 5), date(2026, 3, 6)), [CnFake(), yfinance(), massive(), alpaca()])
    assert {j.provider for j in p.jobs} == {"baostock"}
    assert all(j.instruments != WHOLE_MARKET for j in p.jobs)


def test_cn_bare_codes_and_unknown_spellings_are_skipped_with_a_note():
    from cn_fakes import CnFake

    p = cn_plan(cn_sel(date(2026, 3, 5), date(2026, 3, 6)), [CnFake()], universe=["600000", "sh.600000", "hk.00700", "000001.SZ"])
    assert {j.instruments for j in p.jobs} == {("000001.SZ",), ("sh.600000",)}
    assert any(n.code == "no_symbols" and "600000" in n.message and "hk.00700" in n.message for n in p.notes)
    none = cn_plan(cn_sel(date(2026, 3, 5), date(2026, 3, 6)), [CnFake()], universe=["600000"])
    assert none.jobs == () and [n.code for n in none.notes].count("no_symbols") == 2


def test_cn_two_spellings_of_one_stock_plan_once():
    from cn_fakes import CnFake

    p = cn_plan(cn_sel(date(2026, 3, 5), date(2026, 3, 6)), [CnFake()], universe=["sh.600000", "600000.SH", "SH600000"])
    assert {s for j in p.jobs for s in j.instruments} == {"600000.SH"}  # sorted first, one job set


def test_cn_without_a_universe_is_a_note():
    from cn_fakes import CnFake

    p = cn_plan(cn_sel(date(2026, 3, 5), date(2026, 3, 6)), [CnFake()], universe=[])
    assert p.jobs == () and any(n.code == "no_universe" for n in p.notes)
