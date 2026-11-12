"""A-share instrument id rules (#56): build / parse / normalize, no I/O."""

from __future__ import annotations

import pytest

from openbars.core.models import Market
from openbars.normalize.cn_instruments import (
    CNInstrumentId,
    cn_instrument_id,
    normalize_cn_symbol,
    parse_cn_instrument_id,
)


def test_build_first_listing():
    assert cn_instrument_id("xshg", "600000") == "cn:eq:xshg:600000"
    assert cn_instrument_id("xshe", "000001") == "cn:eq:xshe:000001"
    assert cn_instrument_id("xshg", "688001") == "cn:eq:xshg:688001"
    assert cn_instrument_id("xshe", "300750", 1) == "cn:eq:xshe:300750"


def test_build_generation_suffix():
    assert cn_instrument_id("xshg", "600000", 2) == "cn:eq:xshg:600000:g2"
    assert cn_instrument_id("xshe", "000001", generation=10) == "cn:eq:xshe:000001:g10"


@pytest.mark.parametrize(
    "args",
    [
        ("xbse", "830799"),  # Beijing reserved
        ("XSHG", "600000"),  # venue must be lowercase slug
        ("nyse", "600000"),
        ("", "600000"),
        ("xshg", "60000"),
        ("xshg", "6000000"),
        ("xshg", "60000a"),
        ("xshg", "６０００００"),  # non-ASCII digits
        ("xshg", " 600000"),
        ("xshg", "600000", 0),
        ("xshg", "600000", -1),
        ("xshg", "600000", True),
        ("xshg", "600000", 1.5),
    ],
)
def test_build_rejects_invalid(args):
    with pytest.raises(ValueError):
        cn_instrument_id(*args)


def test_parse_roundtrip():
    assert parse_cn_instrument_id("cn:eq:xshg:600000") == CNInstrumentId("xshg", "600000", 1)
    assert parse_cn_instrument_id("cn:eq:xshe:000001:g2") == ("xshe", "000001", 2)
    for venue, code, gen in [("xshg", "600000", 1), ("xshe", "000001", 3), ("xshg", "688001", 12)]:
        assert parse_cn_instrument_id(cn_instrument_id(venue, code, gen)) == (venue, code, gen)


@pytest.mark.parametrize(
    "bad",
    [
        "600000",
        "cn:eq:600000",
        "cn:eq:xshg:60000",
        "cn:eq:xshg:600000:g1",
        "cn:eq:xshg:600000:g0",
        "cn:eq:xshg:600000:g02",
        "cn:eq:xshg:600000:g",
        "cn:eq:xshg:600000:2",
        "cn:eq:xshg:600000:g2:x",
        "cn:eq:xbse:830799",
        "cn:eq:XSHG:600000",
        "cn:eq:xnys:600000",
        "us:eq:xshg:600000",
        "cn:cs:xshg:600000",
        "cn:eq:xshg:600000\n",
        "",
    ],
)
def test_parse_rejects_invalid(bad):
    with pytest.raises(ValueError):
        parse_cn_instrument_id(bad)


def test_parse_rejects_non_string():
    with pytest.raises(ValueError):
        parse_cn_instrument_id(None)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "raw",
    ["600000.SH", "600000.SS", "600000.sh", "sh.600000", "SH.600000", "SH600000", "sh600000",
     " 600000.SH ", "600000.SSE", "600000.XSHG"],
)
def test_normalize_shanghai(raw):
    assert normalize_cn_symbol(raw) == ("xshg", "600000")


@pytest.mark.parametrize(
    "raw",
    ["000001.SZ", "000001.SZSE", "sz.000001", "SZ.000001", "SZ000001", "sz000001", "000001.XSHE"],
)
def test_normalize_shenzhen(raw):
    assert normalize_cn_symbol(raw) == ("xshe", "000001")


def test_normalize_keeps_leading_zeros_and_star_chinext():
    assert normalize_cn_symbol("688001.SH") == ("xshg", "688001")
    assert normalize_cn_symbol("300750.SZ") == ("xshe", "300750")
    assert normalize_cn_symbol("002594.SZ") == ("xshe", "002594")


@pytest.mark.parametrize(
    "bad",
    ["600000", "000001", "60000.SH", "6000001.SH", "600000.BJ", "bj430047", "430047.BJ",
     "600000.HK", "SH.60000", "AAPL", "", "600000.SH.SH", "cn:eq:xshg:600000"],
)
def test_normalize_rejects(bad):
    with pytest.raises(ValueError):
        normalize_cn_symbol(bad)


def test_normalize_bare_code_message_mentions_exchange():
    with pytest.raises(ValueError, match="exchange"):
        normalize_cn_symbol("600000")


def test_normalize_then_build():
    venue, code = normalize_cn_symbol("sz.000001")
    assert cn_instrument_id(venue, code) == "cn:eq:xshe:000001"


def test_market_of_uses_cn_prefix(tmp_path):
    from openbars.storage.parquet_duckdb import ParquetDuckDbBackend

    backend = ParquetDuckDbBackend(tmp_path)
    assert backend.market_of(cn_instrument_id("xshg", "600000")) is Market.CN
    assert backend.market_of(cn_instrument_id("xshe", "000001", 2)) is Market.CN
    assert backend.market_of("us:eq:01ARZ3NDEKTSV4RRFFQ69G5FAV") is Market.US
