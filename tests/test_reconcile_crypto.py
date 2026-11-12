"""crypto-daily-v1 reconcile profile on synthetic TEST/USDT and TEST/USDC data."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from reconcile_helpers import obs

from openbars.calendars.crypto import CryptoCalendar
from openbars.core.series import (
    BINANCE_ARCHIVE_RAW,
    BINANCE_SPOT_RAW,
    COINBASE_EXCHANGE_RAW,
    KNOWN_SOURCES,
)
from openbars.reconcile import DEFAULT_POLICY, project_canonical_bars
from openbars.reconcile.crypto import (
    CRYPTO_DAILY_V1,
    POLICY_VERSION_CRYPTO_DAILY_V1,
    instrument_venue,
    policy_for_instrument,
    reconcile_crypto,
    reject_cross_venue,
    source_venue,
)

BIN = "crypto:spot:binance:TEST-USDT"
CB = "crypto:spot:coinbase:TEST-USDT"
BIN_USDC = "crypto:spot:binance:TEST-USDC"
SAT = date(2024, 1, 6)
SUN = date(2024, 1, 7)
GEN = datetime(2024, 2, 1, tzinfo=timezone.utc)


def bar(source, iid, close="100", volume=1000, day=SAT):
    return obs(source, close, volume, day=day, instrument_id=iid)


def test_policy_binance():
    p = policy_for_instrument(BIN)
    assert p.policy_version == POLICY_VERSION_CRYPTO_DAILY_V1 == "crypto-daily-v1"
    assert p.primary == BINANCE_SPOT_RAW
    assert p.secondary_sources == (BINANCE_ARCHIVE_RAW,)
    assert p.fill_sources == ()
    assert p.price_tolerance_pct == Decimal("1")
    assert p.volume_tolerance_pct == Decimal("10")
    assert p.corroboration_quorum == 2
    assert p.completion_wait_minutes == 30
    assert CRYPTO_DAILY_V1 == p


def test_policy_coinbase():
    p = policy_for_instrument(CB)
    assert p.primary == COINBASE_EXCHANGE_RAW
    assert p.secondary_sources == ()
    assert p.policy_version == "crypto-daily-v1"
    assert (p.price_tolerance_pct, p.volume_tolerance_pct) == (Decimal("1"), Decimal("10"))


@pytest.mark.parametrize("bad", ["TEST1", "crypto:spot:kraken:TEST-USDT", "crypto:spot:binance:TEST", ""])
def test_policy_rejects_unknown(bad):
    with pytest.raises(ValueError):
        policy_for_instrument(bad)


def test_default_policy_and_known_sources_untouched():
    assert DEFAULT_POLICY.policy_version == "research-daily-v2"
    assert DEFAULT_POLICY.price_tolerance_pct == Decimal("0.5")
    assert BINANCE_ARCHIVE_RAW not in KNOWN_SOURCES
    assert BINANCE_SPOT_RAW not in KNOWN_SOURCES


def test_venue_helpers():
    assert instrument_venue(BIN) == "binance"
    assert instrument_venue(CB) == "coinbase"
    assert instrument_venue("TEST1") is None
    assert instrument_venue(None) is None
    assert source_venue(BINANCE_SPOT_RAW) == "binance"
    assert source_venue(BINANCE_ARCHIVE_RAW) == "binance"
    assert source_venue(COINBASE_EXCHANGE_RAW) == "coinbase"
    from reconcile_helpers import MASSIVE

    assert source_venue(MASSIVE) is None


def test_reject_cross_venue_wrong_source_on_instrument():
    good = bar(BINANCE_SPOT_RAW, BIN, "100")
    wrong = bar(COINBASE_EXCHANGE_RAW, BIN, "150")
    kept, rejected = reject_cross_venue([good, wrong], BIN)
    assert kept == [good]
    assert [(r.observation, r.reason) for r in rejected] == [(wrong, "cross_venue")]


def test_reject_other_instruments():
    cb = bar(COINBASE_EXCHANGE_RAW, CB)
    usdc = bar(BINANCE_SPOT_RAW, BIN_USDC)
    mine = bar(BINANCE_SPOT_RAW, BIN)
    kept, rejected = reject_cross_venue([cb, usdc, mine], BIN)
    assert kept == [mine]
    assert {r.reason for r in rejected} == {"cross_venue", "other_instrument"}
    by_obs = {id(r.observation): r.reason for r in rejected}
    assert by_obs[id(cb)] == "cross_venue"
    assert by_obs[id(usdc)] == "other_instrument"


def test_two_venues_reconciled_separately_never_averaged():
    b = bar(BINANCE_SPOT_RAW, BIN, "100", 1000)
    c = bar(COINBASE_EXCHANGE_RAW, CB, "150", 2000)
    mixed = [b, c]

    rb = reconcile_crypto(BIN, SAT, SAT, mixed, generated_at=GEN)
    rc = reconcile_crypto(CB, SAT, SAT, mixed, generated_at=GEN)

    (row_b,) = rb.result.canonical
    (row_c,) = rc.result.canonical
    assert (row_b.provider, row_b.close, row_b.quality_status) == ("binance", Decimal("100"), "complete")
    assert (row_c.provider, row_c.close, row_c.quality_status) == ("coinbase", Decimal("150"), "complete")
    assert row_b.policy_version == row_c.policy_version == "crypto-daily-v1"
    assert [r.reason for r in rb.cross_venue_rejected] == ["cross_venue"]
    assert [r.reason for r in rc.cross_venue_rejected] == ["cross_venue"]
    assert rb.cross_venue_rejected[0].observation is c
    # 100 vs 150 is far beyond tolerance, yet nothing was compared: complete, not divergent.


def test_cross_venue_bar_is_never_a_fill_or_corroboration():
    cb_on_bin = bar(COINBASE_EXCHANGE_RAW, BIN, "100")
    r = reconcile_crypto(BIN, SAT, SAT, [cb_on_bin], generated_at=GEN)
    assert r.result.canonical == ()
    assert len(r.cross_venue_rejected) == 1
    assert [j.session_date for j in r.result.backfill_jobs] == [SAT]

    # Corroboration: a wrongly attached Coinbase bar does not rescue a diverging same-venue pair.
    p = bar(BINANCE_SPOT_RAW, BIN, "100")
    s = bar(BINANCE_ARCHIVE_RAW, BIN, "103")
    cb1 = bar(COINBASE_EXCHANGE_RAW, BIN, "100")
    cb2 = bar(COINBASE_EXCHANGE_RAW, BIN, "100")
    out = reconcile_crypto(BIN, SAT, SAT, [p, s, cb1, cb2], generated_at=GEN)
    assert out.result.canonical[0].quality_status == "divergent"
    assert len(out.cross_venue_rejected) == 2


def test_same_venue_price_within_tolerance_is_complete():
    p = bar(BINANCE_SPOT_RAW, BIN, "100")
    s = bar(BINANCE_ARCHIVE_RAW, BIN, "100.5")
    (row,) = reconcile_crypto(BIN, SAT, SAT, [p, s], generated_at=GEN).result.canonical
    assert row.quality_status == "complete"
    assert row.close == Decimal("100")


def test_same_venue_price_over_tolerance_is_divergent_primary_kept():
    p = bar(BINANCE_SPOT_RAW, BIN, "100")
    s = bar(BINANCE_ARCHIVE_RAW, BIN, "102")
    (row,) = reconcile_crypto(BIN, SAT, SAT, [p, s], generated_at=GEN).result.canonical
    assert row.quality_status == "divergent"
    assert row.provider == "binance" and row.feed == "spot"
    assert row.close == Decimal("100")  # primary's, not 101
    assert row.selection_reason == "primary_source"
    assert "consolidated_volume" not in row.capabilities


def test_volume_tolerance():
    p = bar(BINANCE_SPOT_RAW, BIN, "100", 1000)
    ok = bar(BINANCE_ARCHIVE_RAW, BIN, "100", 1090)
    bad = bar(BINANCE_ARCHIVE_RAW, BIN, "100", 1300)
    (r_ok,) = reconcile_crypto(BIN, SAT, SAT, [p, ok], generated_at=GEN).result.canonical
    (r_bad,) = reconcile_crypto(BIN, SAT, SAT, [p, bad], generated_at=GEN).result.canonical
    assert r_ok.quality_status == "complete"
    assert r_bad.quality_status == "divergent"
    assert r_bad.volume == Decimal("1000")


def test_secondary_only_is_same_venue_gap_fill():
    s = bar(BINANCE_ARCHIVE_RAW, BIN, "100")
    (row,) = reconcile_crypto(BIN, SAT, SAT, [s], generated_at=GEN).result.canonical
    assert row.selection_reason == "compatible_gap_fill"
    assert row.provider == "binance" and row.feed == "archive"


def test_weekend_utc_dates_are_sessions():
    cal = CryptoCalendar()
    assert cal.is_session(SAT) and cal.is_session(SUN)
    bars = [bar(BINANCE_SPOT_RAW, BIN, "100", day=SAT), bar(BINANCE_SPOT_RAW, BIN, "101", day=SUN)]
    r = reconcile_crypto(BIN, SAT, SUN, bars, generated_at=GEN)
    assert [row.session_date for row in r.result.canonical] == [SAT, SUN]
    assert r.result.rejected == ()
    assert r.result.backfill_jobs == ()


def test_gaps_use_crypto_calendar():
    r = reconcile_crypto(BIN, SAT, date(2024, 1, 8), [bar(BINANCE_SPOT_RAW, BIN, day=SAT)], generated_at=GEN)
    assert [j.session_date for j in r.result.backfill_jobs] == [SUN, date(2024, 1, 8)]


def test_projection_policy_with_two_instruments_separate():
    # project_canonical_bars already groups by instrument: same base/quote, different venues.
    out = project_canonical_bars(
        [bar(BINANCE_SPOT_RAW, BIN, "100"), bar(BINANCE_SPOT_RAW, BIN_USDC, "200")],
        policy=policy_for_instrument(BIN),
    )
    assert {s.instrument_id: s.selected.close for s in out} == {
        BIN: Decimal("100"),
        BIN_USDC: Decimal("200"),
    }
