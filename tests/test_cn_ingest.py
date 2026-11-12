"""``ensure_cn_instruments``: first-time A-share instrument registration (#62)."""

from __future__ import annotations

from datetime import date

import pytest

from openbars.core.models import AssetClass, Market
from decimal import Decimal as D

from openbars.core.models import DateRange
from openbars.normalize.cn_adjust import AdjustFactor
from openbars.normalize.cn_ingest import (
    adjust_factor_fetch_window,
    ensure_cn_instruments,
    ingest_job_adjust_factors,
)
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


JOB = DateRange(date(2026, 3, 2), date(2026, 3, 6))


def _factor(day: date) -> AdjustFactor:
    return AdjustFactor("cn:eq:xshg:600000", day, "baostock", "pct_return", D("0.8"), D("1.25"), D("1.25"))


def test_factor_window_without_stored_factors_is_full_history():
    assert adjust_factor_fetch_window([], JOB) == DateRange(date(1990, 1, 1), JOB.end)
    assert adjust_factor_fetch_window([], JOB, listed_on=date(2001, 8, 27)) == DateRange(date(2001, 8, 27), JOB.end)


def test_factor_window_with_stored_factors_starts_at_the_last_effective_from():
    stored = [_factor(date(2020, 6, 1)), _factor(date(2025, 7, 1)), _factor(date(2010, 1, 4))]
    assert adjust_factor_fetch_window(stored, JOB, listed_on=date(2001, 8, 27)) == DateRange(date(2025, 7, 1), JOB.end)


def test_factor_start_overrides_everything_and_start_is_clamped_to_the_end():
    stored = [_factor(date(2025, 7, 1))]
    assert adjust_factor_fetch_window(stored, JOB, factor_start=date(2000, 1, 1)) == DateRange(date(2000, 1, 1), JOB.end)
    assert adjust_factor_fetch_window([], JOB, factor_start=date(2000, 1, 1), listed_on=date(2010, 1, 1)).start == date(2000, 1, 1)
    # a backfill job older than the last stored event: the start is clamped to the job end
    old = DateRange(date(2020, 1, 2), date(2020, 1, 10))
    assert adjust_factor_fetch_window(stored, old) == DateRange(old.end, old.end)


class _Store:
    def __init__(self, rows=()):
        self.rows = list(rows)

    def adjust_factors(self, ids=None, source=None):
        return [f for f in self.rows if (source is None or f.source == source)]

    def upsert_adjust_factors(self, factors):
        self.rows += list(factors)


def _bars():
    from datetime import UTC, datetime

    from openbars.core.models import Interval, RawBar

    bar = RawBar(
        provider="baostock", symbol_as_returned="sh.600000", fetch_id="f", interval=Interval.D1,
        ts_start=datetime(2026, 3, 2, tzinfo=UTC), open=D(1), high=D(1), low=D(1), close=D(1), volume=D(1),
    )  # fmt: skip
    return {"cn:eq:xshg:600000": [bar]}


def test_empty_full_history_writes_one_seed_and_incremental_empty_adds_none():
    requests = []

    def fetch(request):
        requests.append(request.window)
        return []

    store = _Store()
    errors = ingest_job_adjust_factors(fetch, store, JOB, _bars(), listed_on=lambda _i: date(2001, 8, 27))
    assert errors == [] and requests == [DateRange(date(2001, 8, 27), JOB.end)]
    (seed,) = store.rows
    assert (seed.effective_from, seed.source, seed.method, seed.symbol) == (
        JOB.end, "baostock", "pct_return", "sh.600000",
    )  # fmt: skip
    assert {seed.fore_adjust_factor, seed.back_adjust_factor, seed.adjust_factor} == {D(1)}
    ingest_job_adjust_factors(fetch, store, JOB, _bars(), listed_on=lambda _i: date(2001, 8, 27))
    assert len(store.rows) == 1  # incremental, nothing found: no second seed
    assert requests[-1] == DateRange(JOB.end, JOB.end)  # watermark is the job end, not listing


def test_late_factor_start_with_empty_result_is_not_attested_and_errors_are_recorded():
    store = _Store()
    ingest_job_adjust_factors(lambda r: [], store, JOB, _bars(), factor_start=date(2020, 1, 1))
    assert store.rows == []  # the window does not reach back to 1990: an empty answer proves nothing

    def boom(request):
        raise RuntimeError("down")

    errors = ingest_job_adjust_factors(boom, store, JOB, _bars())
    assert errors == [("sh.600000", "RuntimeError")] and store.rows == []
