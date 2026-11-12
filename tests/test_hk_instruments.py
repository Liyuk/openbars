"""HK instrument id rules: 5-digit codes, XHKG venue, ADR cross-reference keys."""

from __future__ import annotations

import pytest

from openbars.core.models import Market
from openbars.normalize.hk_instruments import (
    HK_ADR_KEY,
    US_HK_ORDINARY_KEY,
    HKInstrumentId,
    hk_adr_external_ids,
    hk_instrument_id,
    normalize_hk_symbol,
    parse_hk_instrument_id,
    us_hk_ordinary_external_ids,
)


def test_build_first_listing_pads_and_keeps_zeros():
    assert hk_instrument_id("xhkg", "00700") == "hk:eq:xhkg:00700"
    assert hk_instrument_id("xhkg", "700") == "hk:eq:xhkg:00700"
    assert hk_instrument_id("xhkg", "09988", 1) == "hk:eq:xhkg:09988"
    assert hk_instrument_id("XHKG", "5") == "hk:eq:xhkg:00005"


def test_build_generation_suffix():
    assert hk_instrument_id("xhkg", "00700", 2) == "hk:eq:xhkg:00700:g2"
    assert hk_instrument_id("xhkg", "00005", generation=10) == "hk:eq:xhkg:00005:g10"


@pytest.mark.parametrize(
    "args",
    [
        ("xnys", "00700"),
        ("", "00700"),
        ("xhkg", ""),
        ("xhkg", "123456"),
        ("xhkg", "700a"),
        ("xhkg", "００７００"),
        ("xhkg", "00700", 0),
        ("xhkg", "00700", -1),
        ("xhkg", "00700", True),
        ("xhkg", "00700", 1.5),
    ],
)
def test_build_rejects_invalid(args):
    with pytest.raises(ValueError):
        hk_instrument_id(*args)


def test_parse_roundtrip():
    assert parse_hk_instrument_id("hk:eq:xhkg:00700") == HKInstrumentId(
        "xhkg", "00700", 1
    )
    assert parse_hk_instrument_id("hk:eq:xhkg:00700:g2") == ("xhkg", "00700", 2)
    for code, gen in [("00700", 1), ("00005", 3), ("09988", 12)]:
        assert parse_hk_instrument_id(hk_instrument_id("xhkg", code, gen)) == (
            "xhkg",
            code,
            gen,
        )


@pytest.mark.parametrize(
    "bad",
    [
        "00700",
        "hk:eq:00700",
        "hk:eq:xhkg:0700",
        "hk:eq:xhkg:00700:g1",
        "hk:eq:xhkg:00700:g0",
        "hk:eq:xhkg:00700:g02",
        "hk:eq:xhkg:00700:g",
        "hk:eq:xhkg:00700:2",
        "hk:eq:xhkg:00700:g2:x",
        "hk:eq:XHKG:00700",
        "hk:eq:xnys:00700",
        "us:eq:xhkg:00700",
        "hk:cs:xhkg:00700",
        "hk:eq:xhkg:00700\n",
        "",
    ],
)
def test_parse_rejects_invalid(bad):
    with pytest.raises(ValueError):
        parse_hk_instrument_id(bad)


def test_parse_rejects_non_string():
    with pytest.raises(ValueError):
        parse_hk_instrument_id(None)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "raw,code",
    [
        ("00700", "00700"),
        ("700", "00700"),
        ("0700.HK", "00700"),
        ("00700.hk", "00700"),
        ("00700.HKG", "00700"),
        ("00700.HKEX", "00700"),
        ("00700.XHKG", "00700"),
        ("HK.00700", "00700"),
        ("hk.700", "00700"),
        ("HK00700", "00700"),
        ("xhkg:00700", "00700"),
        ("XHKG:5", "00005"),
        ("09988", "09988"),
        ("hk:eq:xhkg:00700", "00700"),
        (" 700 ", "00700"),
    ],
)
def test_normalize_accepts_common_spellings(raw, code):
    assert normalize_hk_symbol(raw) == ("xhkg", code)


@pytest.mark.parametrize(
    "bad",
    ["700.US", "AAPL", "hk:eq:xnys:00700", "00700.SH", "123456", "", "00700.HK.HK"],
)
def test_normalize_rejects(bad):
    with pytest.raises(ValueError):
        normalize_hk_symbol(bad)


def test_normalize_then_build():
    venue, code = normalize_hk_symbol("0700.HK")
    assert hk_instrument_id(venue, code) == "hk:eq:xhkg:00700"


def test_adr_cross_reference_keys():
    hk_id = "hk:eq:xhkg:00700"
    us_id = "us:eq:01ARZ3NDEKTSV4RRFFQ69G5FAV"
    assert hk_adr_external_ids(us_id) == {HK_ADR_KEY: us_id}
    assert us_hk_ordinary_external_ids(hk_id) == {US_HK_ORDINARY_KEY: hk_id}
    with pytest.raises(ValueError):
        us_hk_ordinary_external_ids("00700")
    with pytest.raises(ValueError):
        hk_adr_external_ids("")


def test_market_of_uses_hk_prefix(tmp_path):
    from openbars.storage.parquet_duckdb import ParquetDuckDbBackend

    backend = ParquetDuckDbBackend(tmp_path)
    assert backend.market_of(hk_instrument_id("xhkg", "00700")) is Market.HK
    assert backend.market_of(hk_instrument_id("xhkg", "00005", 2)) is Market.HK
    assert backend.market_of("us:eq:01ARZ3NDEKTSV4RRFFQ69G5FAV") is Market.US
