"""``ensure_cn_instruments``: first-time A-share instrument registration (#62)."""

from __future__ import annotations

from datetime import date

import pytest

from openbars.core.models import AssetClass, Market
from openbars.normalize.cn_ingest import ensure_cn_instruments
from openbars.normalize.instruments import MemoryInstrumentMaster

AS_OF = date(2026, 3, 2)


def test_registers_generation_one_ids_for_every_spelling():
    master = MemoryInstrumentMaster()
    ids = ensure_cn_instruments(master, ["sh.600000", "000001.SZ", "SZ300750"], as_of=AS_OF)
    assert ids == {
        "sh.600000": "cn:eq:xshg:600000", "000001.SZ": "cn:eq:xshe:000001", "SZ300750": "cn:eq:xshe:300750",
    }  # fmt: skip
    inst = master.get("cn:eq:xshg:600000")
    assert (inst.market, inst.asset_class, inst.currency, inst.exchange) == (
        Market.CN, AssetClass.EQUITY, "CNY", "XSHG",
    )  # fmt: skip
    assert master.get("cn:eq:xshe:000001").exchange == "XSHE"
    for spelling, provider in (("sh.600000", "baostock"), ("sh.600000", "*"), ("600000.SH", "*")):
        assert master.resolve(spelling, on=AS_OF, provider=provider, market=Market.CN) == "cn:eq:xshg:600000"


def test_existing_instrument_is_reused_never_reissued():
    master = MemoryInstrumentMaster()
    first = ensure_cn_instruments(master, ["sh.600000"], as_of=AS_OF, listed_on=date(1999, 11, 10))
    again = ensure_cn_instruments(master, ["600000.SH", "sh.600000"], as_of=date(2026, 3, 9))
    assert again == {"600000.SH": first["sh.600000"], "sh.600000": first["sh.600000"]}
    assert master.get("cn:eq:xshg:600000").listed_on == date(1999, 11, 10)  # not overwritten


def test_listed_on_opens_the_mapping_window():
    master = MemoryInstrumentMaster()
    ensure_cn_instruments(master, ["sh.688001"], as_of=AS_OF, listed_on=date(2019, 7, 22))
    assert master.resolve("sh.688001", on=date(2019, 7, 22), provider="baostock", market=Market.CN) == "cn:eq:xshg:688001"


def test_bad_spelling_raises_before_anything_is_written():
    master = MemoryInstrumentMaster()
    with pytest.raises(ValueError):
        ensure_cn_instruments(master, ["sh.600000", "600000"], as_of=AS_OF)  # a bare code has no exchange
    assert master.instruments() == []
