"""hk-daily-v1 reconcile profile on synthetic hk:eq:xhkg:00000 data (not a live listing)."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

from reconcile_helpers import obs

from openbars.core.series import (
    AKSHARE_EASTMONEY_RAW,
    KNOWN_SOURCES,
    YFINANCE_YAHOO_RAW,
    YFINANCE_YAHOO_SPLIT,
    BarSeriesCatalog,
)
from openbars.reconcile.hk import (
    HK_DAILY_V1,
    HK_PRICE_TOLERANCE_PCT,
    HK_VOLUME_TOLERANCE_PCT,
    reconcile_hk,
)

IID = "hk:eq:xhkg:00000"
MON, TUE, WED = date(2026, 3, 2), date(2026, 3, 3), date(2026, 3, 4)
GEN = datetime(2026, 3, 10, tzinfo=UTC)


def bar(source, close="100", volume=1000, day=MON):
    return obs(source, close, volume, day=day, instrument_id=IID)


def test_hk_policy_and_catalog():
    p = HK_DAILY_V1
    assert p.policy_version == "hk-daily-v1"
    assert p.primary == YFINANCE_YAHOO_RAW
    assert p.secondary_sources == (AKSHARE_EASTMONEY_RAW,)
    assert p.fill_sources == ()
    assert p.price_tolerance_pct == Decimal("0.5") == HK_PRICE_TOLERANCE_PCT
    assert p.volume_tolerance_pct == Decimal(5) == HK_VOLUME_TOLERANCE_PCT
    assert p.corroboration_quorum == 2
    assert p.completion_wait_minutes == 30
    assert YFINANCE_YAHOO_RAW not in KNOWN_SOURCES
    assert AKSHARE_EASTMONEY_RAW not in KNOWN_SOURCES
    assert YFINANCE_YAHOO_SPLIT in KNOWN_SOURCES
    assert BarSeriesCatalog.resolve(YFINANCE_YAHOO_RAW).currency == "HKD"
    assert BarSeriesCatalog.resolve(AKSHARE_EASTMONEY_RAW).currency == "HKD"
    assert (
        BarSeriesCatalog.resolve(YFINANCE_YAHOO_RAW, currency="USD").currency == "USD"
    )


def test_primary_row_carries_hkd_and_is_not_averaged():
    yf = bar(YFINANCE_YAHOO_RAW, "100")
    ak = bar(AKSHARE_EASTMONEY_RAW, "100.2")  # within 0.5%
    result = reconcile_hk(IID, MON, MON, [yf, ak], generated_at=GEN).result
    (row,) = result.canonical
    assert row.policy_version == "hk-daily-v1"
    assert row.provider == "yfinance"
    assert row.adjustment == "raw"
    assert row.close == Decimal(100)
    assert row.currency == "HKD"
    assert "currency:HKD" in row.capabilities
    assert row.quality_status == "complete"


def test_disagreement_is_flagged_divergent_not_averaged():
    yf = bar(YFINANCE_YAHOO_RAW, "100")
    ak = bar(AKSHARE_EASTMONEY_RAW, "120")  # well beyond 0.5%
    result = reconcile_hk(IID, MON, MON, [yf, ak], generated_at=GEN).result
    (row,) = result.canonical
    assert row.close == Decimal(100)  # primary wins; never averaged
    assert row.quality_status == "divergent"


def test_akshare_does_not_fill_a_primary_gap():
    yf_mon = bar(YFINANCE_YAHOO_RAW, "100", day=MON)
    ak_mon = bar(AKSHARE_EASTMONEY_RAW, "100", day=MON)
    ak_tue = bar(AKSHARE_EASTMONEY_RAW, "101", day=TUE)
    yf_wed = bar(YFINANCE_YAHOO_RAW, "102", day=WED)
    result = reconcile_hk(
        IID, MON, WED, [yf_mon, ak_mon, ak_tue, yf_wed], generated_at=GEN
    ).result
    days = [r.session_date for r in result.canonical]
    assert TUE not in days
    assert days == [MON, WED]
    assert {r.provider for r in result.canonical} == {"yfinance"}


def test_single_primary_source_is_kept():
    yf = bar(YFINANCE_YAHOO_RAW, "100")
    result = reconcile_hk(IID, MON, MON, [yf], generated_at=GEN).result
    (row,) = result.canonical
    assert row.close == Decimal(100)
    assert row.provider == "yfinance"
    assert row.quality_status == "complete"
    assert row.selection_reason == "primary_source"
    assert row.currency == "HKD"
