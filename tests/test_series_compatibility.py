from __future__ import annotations

from dataclasses import replace
from datetime import date

import pytest

from openbars.core.models import Interval
from openbars.core.series import (
    ALPACA_IEX_SPLIT,
    ALPACA_SIP_SPLIT,
    ALPHA_VANTAGE_DAILY_RAW,
    MASSIVE_EOD_SPLIT,
    TWELVE_DATA_DAILY_SPLIT,
    YFINANCE_YAHOO_SPLIT,
    BarSeriesCatalog,
    PriceBasis,
    SessionSemantics,
    VolumeBasis,
)
from openbars.reconcile.compatibility import compare_series

MASSIVE = BarSeriesCatalog.resolve(MASSIVE_EOD_SPLIT)
SIP = BarSeriesCatalog.resolve(ALPACA_SIP_SPLIT)
IEX = BarSeriesCatalog.resolve(ALPACA_IEX_SPLIT)


def cmp(left, right, li="TEST1", ri="TEST1"):
    return compare_series(li, left, ri, right)


def test_massive_vs_sip_comparable():
    result = cmp(MASSIVE, SIP)
    assert result.comparable is True
    assert result.reason_code is None


@pytest.mark.parametrize("other", [SIP, MASSIVE])
def test_iex_vs_non_iex_is_coverage_mismatch(other):
    for left, right in ((IEX, other), (other, IEX)):
        result = cmp(left, right)
        assert (result.comparable, result.reason_code) == (False, "coverage_mismatch")


def test_iex_vs_iex_not_a_coverage_mismatch():
    assert cmp(IEX, IEX).comparable is True


def test_instrument_mismatch_wins_first():
    result = cmp(MASSIVE, SIP, "TEST1", "ZZZA")
    assert result.reason_code == "identity_mismatch"


def test_interval_mismatch():
    hourly = replace(SIP, interval=Interval.H1)
    assert cmp(MASSIVE, hourly).reason_code == "interval_mismatch"


def test_session_unknown_or_different():
    unknown = replace(SIP, session_semantics=SessionSemantics.UNKNOWN)
    assert cmp(unknown, unknown).reason_code == "session_mismatch"
    regular = replace(SIP, session_semantics=SessionSemantics.REGULAR)
    assert cmp(MASSIVE, regular).reason_code == "session_mismatch"


def test_currency_mismatch():
    assert cmp(MASSIVE, replace(SIP, currency="EUR")).reason_code == "currency_mismatch"


def test_unknown_or_provider_defined_price_basis_not_comparable():
    unknown = replace(SIP, price_basis=PriceBasis.UNKNOWN)
    assert cmp(unknown, unknown).reason_code == "price_basis_mismatch"
    yahoo = BarSeriesCatalog.resolve(YFINANCE_YAHOO_SPLIT)
    assert cmp(yahoo, yahoo).reason_code == "price_basis_mismatch"
    assert cmp(MASSIVE, BarSeriesCatalog.resolve(ALPHA_VANTAGE_DAILY_RAW)).reason_code == (
        "price_basis_mismatch"
    )


def test_adjustment_cutoff_mismatch():
    dated = replace(SIP, adjustment_as_of=date(2024, 1, 2))
    assert cmp(MASSIVE, dated).reason_code == "adjustment_cutoff_mismatch"
    other_scope = replace(SIP, adjustment_scope=SIP.adjustment_scope.__class__.NONE)
    assert cmp(MASSIVE, other_scope).reason_code == "adjustment_cutoff_mismatch"


def test_unknown_volume_basis_not_comparable():
    twelve = BarSeriesCatalog.resolve(TWELVE_DATA_DAILY_SPLIT)
    assert cmp(twelve, twelve).reason_code == "volume_basis_mismatch"
    assert cmp(MASSIVE, replace(SIP, volume_basis=VolumeBasis.AS_TRADED)).reason_code == (
        "volume_basis_mismatch"
    )
