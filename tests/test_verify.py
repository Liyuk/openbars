"""Pure-function tests for ``openbars.orchestrator.verify`` (synthetic bars, test symbols only)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from types import SimpleNamespace

import pytest

from openbars.calendars import CNCalendar, CryptoCalendar, HKCalendar, XNYSCalendar
from openbars.core.errors import ConfigError
from openbars.core.models import DateRange
from openbars.core.series import BarSeriesCatalog, SourceIdentity
from openbars.orchestrator.verify import (
    Agreement,
    Tolerances,
    VerifyBar,
    VerifyCanonical,
    build_aliases,
    classify,
    diff_pct,
    format_report,
    verify,
)

WINDOW = DateRange(date(2026, 3, 2), date(2026, 3, 6))  # five sessions
DAYS = [date(2026, 3, d) for d in range(2, 7)]
CAL = XNYSCalendar()


def bar(key, day, provider="massive", feed="eod", close="10.5", volume="1000", adjustment="split", **kw):
    c = Decimal(close)
    return VerifyBar(
        key, day, provider, feed, adjustment, c - Decimal("0.5"), c + 1, c - 1, c,
        None if volume is None else Decimal(volume), **kw,
    )  # fmt: skip


def run(raw, canonical=(), universe=("TESTE00",), **kw):
    return verify(raw, canonical, calendar=CAL, window=WINDOW, universe=universe, **kw)


def test_classify_and_diff():
    tol = Decimal("0.5")
    assert diff_pct(Decimal(0), Decimal(0)) == 0
    assert classify(Decimal(0), tol) is Agreement.MATCH
    assert classify(Decimal("0.5"), tol) is Agreement.WITHIN_TOLERANCE
    assert classify(Decimal("0.51"), tol) is Agreement.DIVERGENT
    assert diff_pct(Decimal(100), Decimal(101)) == pytest.approx(Decimal(100) / 101, abs=Decimal("1e-9"))


def test_tolerances_from_settings():
    assert Tolerances.from_settings(SimpleNamespace(reconcile={})) == Tolerances(Decimal("0.5"), Decimal(5))
    t = Tolerances.from_settings(SimpleNamespace(reconcile={"tolerances": {"price_rel": 0.01, "volume_rel": 0.1}}))
    assert (t.price_pct, t.volume_pct) == (Decimal(1), Decimal(10))
    with pytest.raises(ConfigError):
        Tolerances.from_settings(SimpleNamespace(reconcile={"tolerances": {"price_rel": "abc"}}))
    with pytest.raises(TypeError):
        Tolerances(0.5, Decimal(5))  # type: ignore[arg-type]


def test_coverage_and_source_summary():
    raw = [bar("TESTE00", d) for d in DAYS[:4]] + [bar("TESTE01", DAYS[0])] + [bar("TESTE99", DAYS[0])]
    canon = [VerifyCanonical("TESTE00", d, provider="massive", feed="eod", quality_status="complete") for d in DAYS[:3]]
    report = run(raw, canon, universe=["TESTE00", "TESTE01"])
    assert (report.universe_size, report.expected_sessions) == (2, 5)
    cov = report.coverage
    assert cov.expected_cells == 10 and cov.canonical_cells == 3 and cov.raw_cells == 5
    assert cov.missing_cells == 7 and cov.canonical_rate == 0.3
    (src,) = report.sources  # TESTE99 is outside the universe
    assert (src.provider, src.feed, src.adjustment, src.bars, src.symbols) == ("massive", "eod", "split", 5, 2)
    assert (src.oldest, src.newest) == (DAYS[0], DAYS[3])
    assert report.quality.distribution == {"complete": 3} and report.quality.rate is None
    assert report.price.rate is None and report.price.compared == 0  # single source: nothing to compare


def test_no_canonical_rows_falls_back_to_raw_for_missing():
    report = run([bar("TESTE00", d) for d in DAYS[:2]])
    assert report.coverage.canonical_available is False and report.coverage.missing_cells == 3
    assert any("no canonical rows" in n for n in report.notes)


def test_price_and_volume_agreement_with_tolerances():
    raw = []
    for day, (close_b, vol_b) in zip(DAYS, [("10.5", "1000"), ("10.52", "1030"), ("10.8", "1000"), ("10.5", "2000"), ("10.5", None)]):
        raw += [bar("TESTE00", day), bar("TESTE00", day, provider="alpaca", feed="sip", close=close_b, volume=vol_b)]
    report = run(raw)
    # close 10.52 vs 10.5 (also shifts open/high/low by 0.02): within 0.5%; 10.8 is divergent
    assert (report.price.match, report.price.within_tolerance, report.price.divergent) == (3, 1, 1)
    assert report.price.compared == 5 and report.price.divergent == 1
    assert report.close.divergent == 1 and report.close.within_tolerance == 1 and report.close.match == 3
    # volume: match, within (3%), match, divergent (2x), missing (None)
    assert (report.volume.match, report.volume.within_tolerance, report.volume.divergent) == (2, 1, 1)
    assert report.volume.missing == 1 and report.volume.compared == 4
    assert report.volume.rate == pytest.approx(0.75)
    assert report.overall.multi_source_cells == 5 and report.overall.single_source_cells == 0
    wide = run(raw, tolerances=Tolerances(Decimal(5), Decimal(100)))
    assert wide.price.divergent == 0 and wide.volume.divergent == 0


def test_iex_vs_sip_is_not_scored():
    raw = []
    for day in DAYS:
        raw += [
            bar("TESTE00", day, provider="alpaca", feed="sip", volume="1000000"),
            bar("TESTE00", day, provider="alpaca", feed="iex", volume="20000", close="10.5"),  # ~2% of SIP volume
        ]
    report = run(raw)
    assert report.volume.compared == 0 and report.volume.divergent == 0
    assert report.volume.not_comparable == 5 and report.price.not_comparable == 5
    assert report.volume.rate is None and report.overall.not_comparable_pairs == 5


def test_provider_defined_source_is_not_comparable():
    raw = [bar("TESTE00", DAYS[0]), bar("TESTE00", DAYS[0], provider="yfinance", feed="yahoo", close="99")]
    report = run(raw)
    assert report.price.not_comparable == 1 and report.price.divergent == 0


def test_explicit_series_identity_wins_over_catalog():
    sip = BarSeriesCatalog.resolve(SourceIdentity("alpaca", "sip", "split"))
    unknown = BarSeriesCatalog.resolve(SourceIdentity("other", "x", "split"))  # all-unknown bases
    raw = [
        bar("TESTE00", DAYS[0], provider="alpaca", feed="sip", series=sip),
        bar("TESTE00", DAYS[0], provider="other", feed="x", series=unknown),
    ]
    assert run(raw).price.not_comparable == 1


def test_latest_revision_wins_and_alias_joins_symbol_and_id():
    raw = [
        bar("TESTE00", DAYS[0], close="10.5", revision=1),
        bar("TESTE00", DAYS[0], close="11.5", revision=2),
        bar("TESTE00", DAYS[0], provider="alpaca", feed="sip", close="11.5"),
    ]
    canon = [VerifyCanonical("id-0", DAYS[0], provider="massive", feed="eod", quality_status="complete")]
    report = run(raw, canon, universe=["TESTE00"], aliases={"TESTE00": "id-0"})
    assert report.price.match == 1 and report.price.compared == 1
    assert report.coverage.canonical_cells == 1 and report.sources[0].bars == 1


def test_quality_agreement_needs_two_policy_versions():
    canon = []
    for day, (a, b) in zip(DAYS[:3], [("complete", "complete"), ("divergent", "complete"), ("complete", None)]):
        canon.append(VerifyCanonical("TESTE00", day, policy_version="research-daily-v2", quality_status=a))
        canon.append(VerifyCanonical("TESTE00", day, policy_version="candidate", quality_status=b))
    report = run([], canon)
    q = report.quality
    assert (q.cells_compared, q.cells_agreeing, q.rate) == (2, 1, 0.5)  # the unknown status is not guessed
    assert q.distribution == {"complete": 2, "divergent": 1}  # default policy only
    assert report.coverage.canonical_cells == 3


def test_recent_period_breakdown():
    raw = [bar("TESTE00", d) for d in DAYS] + [bar("TESTE00", d, provider="alpaca", feed="sip") for d in DAYS[-2:]]
    report = run(raw, recent_sessions=2)
    assert [p.label for p in report.periods] == ["window", "last 2 sessions"]
    recent = report.periods[1]
    assert recent.coverage.expected_cells == 2 and recent.price.compared == 2
    assert report.price.compared == 2 and report.periods[0].coverage.expected_cells == 5
    assert len(run(raw, recent_sessions=5).periods) == 1 and len(run(raw, recent_sessions=None).periods) == 1


def test_empty_universe_and_render():
    report = run([bar("TESTE00", DAYS[0])], universe=[])
    assert report.universe_size == 0 and report.coverage.canonical_rate is None
    assert any("empty universe" in n for n in report.notes)
    lines = format_report(report)
    assert any("universe 0" in line for line in lines)
    data = report.to_dict()
    assert data["periods"][0]["coverage"]["canonical_rate"] is None


def test_report_to_dict_is_json_serializable():
    import json

    raw = [bar("TESTE00", DAYS[0]), bar("TESTE00", DAYS[0], provider="alpaca", feed="sip", close="11")]
    text = json.dumps(run(raw).to_dict())
    assert "alpaca/sip/split" in text and '"price_pct": "0.5"' in text


def test_build_aliases_uses_master_resolution():
    master = SimpleNamespace(resolve_any=lambda k, on, market: {"TESTE00": "id-0"}.get(k, k))
    assert build_aliases(master, ["TESTE00", "id-0", "TESTE01"], on=date(2026, 3, 6), market="us") == {"TESTE00": "id-0"}
    assert build_aliases(object(), ["TESTE00"], on=date(2026, 3, 6), market="us") == {}


# --- per-market calendars and conventions ------------------------------------------------------

SAT, SUN = date(2026, 3, 7), date(2026, 3, 8)


def test_crypto_every_utc_day_is_a_session_including_weekends():
    window = DateRange(date(2026, 3, 5), date(2026, 3, 9))  # Thu .. Mon
    days = [date(2026, 3, d) for d in range(5, 10)]
    raw = [bar("crypto:spot:binance:TEST-USDT", d, provider="binance", feed="spot", adjustment="raw") for d in days]
    report = verify(
        raw, [], calendar=CryptoCalendar(), window=window, universe=["crypto:spot:binance:TEST-USDT"],
        policy_version="crypto-daily-v1",
    )  # fmt: skip
    assert report.expected_sessions == 5 and report.coverage.expected_cells == 5
    assert report.coverage.raw_cells == 5 and report.coverage.missing_cells == 0  # Sat / Sun count
    assert report.policy_version == "crypto-daily-v1"
    # the same window on XNYS has no weekend sessions
    assert run([], universe=["x"]).expected_sessions == 5
    assert verify([], [], calendar=CAL, window=window, universe=["x"]).expected_sessions == 3


def test_cn_week_uses_xshg_sessions_and_skips_the_weekend():
    window = DateRange(date(2026, 3, 2), date(2026, 3, 8))
    report = verify([], [], calendar=CNCalendar(), window=window, universe=["cn:eq:xshg:600000"])
    assert report.expected_sessions == 5
    sessions = CNCalendar().sessions(window.start, window.end)
    assert sessions == DAYS and SAT not in sessions and SUN not in sessions


def test_hk_uses_xhkg_not_xnys():
    window = DateRange(date(2026, 1, 19), date(2026, 1, 19))  # US holiday (MLK), HK trades
    assert verify([], [], calendar=XNYSCalendar(), window=window, universe=["x"]).expected_sessions == 0
    assert verify([], [], calendar=HKCalendar(), window=window, universe=["x"]).expected_sessions == 1
    lunar = DateRange(date(2026, 2, 16), date(2026, 2, 20))  # HK closed Feb 17-19, US open
    assert verify([], [], calendar=HKCalendar(), window=lunar, universe=["x"]).expected_sessions == 2
    assert verify([], [], calendar=XNYSCalendar(), window=lunar, universe=["x"]).expected_sessions == 4


def test_cn_suspended_day_is_an_expected_hole_and_limit_up_is_counted():
    iid, cn_window = "cn:eq:xshg:600000", DateRange(date(2026, 3, 2), date(2026, 3, 6))
    mon, tue, wed, thu, fri = [date(2026, 3, d) for d in range(2, 7)]

    def cn_bar(day, **kw):
        return bar(iid, day, provider="baostock", feed="ashare", adjustment="raw", **kw)

    raw = [cn_bar(mon), cn_bar(tue), cn_bar(wed, volume="0", suspended=True), cn_bar(thu), cn_bar(fri)]
    canon = [
        VerifyCanonical(iid, d, "cn-daily-v1", "baostock", "ashare", "complete", flags)
        for d, flags in ((mon, ()), (tue, ()), (thu, ("LIMIT_UP",)), (fri, ("LIMIT_DOWN",)))
    ]  # no canonical bar on the halted Wednesday
    report = verify(
        raw, canon, calendar=CNCalendar(), window=cn_window, universe=[iid], policy_version="cn-daily-v1"
    )
    cov = report.coverage
    assert (cov.expected_cells, cov.raw_cells, cov.canonical_cells) == (5, 5, 4)
    assert cov.suspended_cells == 1 and cov.missing_cells == 0  # the halt is not an ordinary missing cell
    assert report.quality.distribution == {"complete": 4}  # limit flags never change the status
    assert report.quality.flags == {"LIMIT_UP": 1, "LIMIT_DOWN": 1}
    assert any("suspended" in n for n in report.notes)
    data = report.to_dict()
    assert data["periods"][0]["coverage"]["suspended_cells"] == 1
    assert data["periods"][0]["quality"]["flags"] == {"LIMIT_DOWN": 1, "LIMIT_UP": 1}
    text = "\n".join(format_report(report))
    assert "suspended 1" in text and "LIMIT_UP=1" in text and "LIMIT_DOWN=1" in text

    # a canonical hole with no suspension evidence in raw is still missing
    unexplained = verify(
        [cn_bar(d) for d in (mon, tue, wed, thu, fri)], canon, calendar=CNCalendar(), window=cn_window,
        universe=[iid], policy_version="cn-daily-v1",
    )  # fmt: skip
    assert unexplained.coverage.suspended_cells == 0 and unexplained.coverage.missing_cells == 1
