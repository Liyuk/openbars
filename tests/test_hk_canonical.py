"""Hong Kong canonical sink, reader, and corporate-action storage. 00000 is not a live listing."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal as D

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from openbars.core.models import (
    Adjustment,
    AssetClass,
    CorporateAction,
    CorporateActionKind,
    DateRange,
    Interval,
    Market,
    RawBar,
)
from openbars.core.series import (
    AKSHARE_EASTMONEY_RAW,
    YFINANCE_YAHOO_RAW,
    BarSeriesCatalog,
)
from openbars.normalize.hk_adjust import hk_cash_dividend, hk_rights, hk_split
from openbars.normalize.hk_ingest import HK_CURRENCY, ensure_hk_instruments
from openbars.orchestrator.finalize import (
    DispatchingSink,
    HkCanonicalSink,
    parquet_canonical_writer,
)
from openbars.orchestrator.jobs import Job, JobKind
from openbars.storage.parquet_duckdb import open_backend
from openbars.storage.schema import (
    CORPORATE_ACTIONS_FIELDS,
    SCHEMA_VERSION,
    _schema,
)

IID = "hk:eq:xhkg:00000"
MON, TUE, WED = date(2026, 3, 2), date(2026, 3, 3), date(2026, 3, 4)
GEN = datetime(2026, 3, 10, 8, tzinfo=UTC)


@pytest.fixture
def backend(tmp_path):
    return open_backend(tmp_path / "data")


def _series(source, currency="HKD"):
    return BarSeriesCatalog.resolve(source, currency=currency)


def raw_bar(
    day,
    close,
    *,
    source=YFINANCE_YAHOO_RAW,
    symbol="00000",
    volume="1000",
    instrument_id=IID,
):
    c = D(close)
    ts = datetime(day.year, day.month, day.day, tzinfo=UTC)
    return RawBar(
        provider=source.provider,
        symbol_as_returned=symbol,
        fetch_id="f",
        interval=Interval.D1,
        ts_start=ts,
        open=c,
        high=c + 1,
        low=c - 1,
        close=c,
        volume=D(volume),
        feed=source.feed,
        instrument_id=instrument_id,
        session_date=day,
        series=_series(source),
    )


def job(provider="yfinance", start=MON, end=WED):
    return Job(
        job_id="hk-1",
        kind=JobKind.DAILY,
        provider=provider,
        market=Market.HK,
        interval=Interval.D1,
        instruments=("00000",),
        window=DateRange(start, end),
    )


def test_ensure_hk_instruments_sets_hkd(backend):
    got = ensure_hk_instruments(
        backend.instruments, ["00000"], as_of=MON, provider="yfinance"
    )
    assert got["00000"] == IID
    inst = backend.instruments.get(IID)
    assert inst is not None
    assert inst.market is Market.HK
    assert inst.asset_class is AssetClass.EQUITY
    assert inst.currency == HK_CURRENCY == "HKD"
    assert inst.exchange == "XHKG"
    # already registered: reused
    again = ensure_hk_instruments(backend.instruments, ["0700.HK"], as_of=MON)
    # 0700.HK is 00700, a different code; just confirm 00000 still there
    assert backend.instruments.get(IID).currency == "HKD"
    assert again["0700.HK"].startswith("hk:eq:xhkg:")


def test_sink_writes_raw_hkd_canonical_rows(backend):
    ensure_hk_instruments(backend.instruments, ["00000"], as_of=MON)
    sink = HkCanonicalSink(
        backend.instruments,
        parquet_canonical_writer(backend.canonical),
        raw=backend.raw,
    )
    bars = [
        raw_bar(d, close) for d, close in ((MON, "100"), (TUE, "101"), (WED, "102"))
    ]
    sink.after_fetch(job(), bars)
    rows = backend.canonical.scan_rows(
        [IID], Interval.D1, DateRange(MON, WED), policy_version="hk-daily-v1"
    )
    assert [r.session_date for r in rows] == [MON, TUE, WED]
    assert {r.policy_version for r in rows} == {"hk-daily-v1"}
    assert {r.adjustment for r in rows} == {"raw"}
    assert {r.provider for r in rows} == {"yfinance"}
    assert {r.currency for r in rows} == {"HKD"}
    assert all("currency:HKD" in r.capabilities for r in rows)
    assert [r.close for r in rows] == [D(100), D(101), D(102)]


def test_akshare_on_disk_corroborates_but_does_not_fill(backend):
    ensure_hk_instruments(backend.instruments, ["00000"], as_of=MON)
    # AKShare already on disk for Mon-Wed; yfinance fetch is only Mon and Wed.
    from openbars.providers.base import FetchRequest, RawBatch

    ak_bars = [
        raw_bar(d, close, source=AKSHARE_EASTMONEY_RAW, symbol="00000")
        for d, close in (
            (MON, "100"),
            (TUE, "999"),
            (WED, "102"),
        )
    ]
    backend.raw.append(
        RawBatch(
            provider="akshare",
            fetch_id="ak-1",
            request=FetchRequest(
                market=Market.HK,
                interval=Interval.D1,
                symbols=("00000",),
                window=DateRange(MON, WED),
            ),
            bars=tuple(ak_bars),
        )
    )
    sink = HkCanonicalSink(
        backend.instruments,
        parquet_canonical_writer(backend.canonical),
        raw=backend.raw,
        other_providers=("akshare",),
    )
    yf = [raw_bar(MON, "100"), raw_bar(WED, "102")]
    sink.after_fetch(job(), yf)
    rows = backend.canonical.scan_rows(
        [IID], Interval.D1, DateRange(MON, WED), policy_version="hk-daily-v1"
    )
    days = [r.session_date for r in rows]
    assert TUE not in days
    assert days == [MON, WED]
    assert {r.provider for r in rows} == {"yfinance"}
    assert {r.currency for r in rows} == {"HKD"}


def test_reader_default_split_derives_from_stored_raw(backend):
    ensure_hk_instruments(backend.instruments, ["00000"], as_of=MON)
    sink = HkCanonicalSink(
        backend.instruments, parquet_canonical_writer(backend.canonical)
    )
    # 2-for-1 on Wednesday: stored RAW pre-ex is 250, post is 130
    sink.after_fetch(job(end=WED), [raw_bar(MON, "250"), raw_bar(WED, "130")])
    backend.instruments.upsert_corporate_actions([hk_split(IID, WED, 2, 1, "yfinance")])
    stored = backend.reader.bars([IID], market=Market.HK, adjustment=Adjustment.RAW)
    assert stored.column("close").to_pylist() == [D(250), D(130)]
    assert set(stored.column("adjustment").to_pylist()) == {"raw"}
    derived = backend.reader.bars([IID], market=Market.HK)  # default SPLIT
    assert derived.column("close").to_pylist() == [D(125), D(130)]
    assert set(derived.column("adjustment").to_pylist()) == {"split"}
    with pytest.raises(Exception, match="only derived for A-share"):
        backend.reader.bars([IID], market=Market.HK, adjustment=Adjustment.QFQ)


def test_usd_dividend_is_not_applied_to_hkd_canonical_rows(backend):
    ensure_hk_instruments(backend.instruments, ["00000"], as_of=MON)
    sink = HkCanonicalSink(
        backend.instruments, parquet_canonical_writer(backend.canonical)
    )
    sink.after_fetch(
        job(), [raw_bar(MON, "100"), raw_bar(TUE, "100"), raw_bar(WED, "100")]
    )
    backend.instruments.upsert_corporate_actions(
        [hk_cash_dividend(IID, WED, "25", "yfinance", currency="USD")]
    )
    out = backend.reader.bars(
        [IID], market=Market.HK, adjustment=Adjustment.SPLIT_DIVIDEND
    )
    assert out.column("close").to_pylist() == [D(100), D(100), D(100)]
    backend.instruments.upsert_corporate_actions(
        [hk_cash_dividend(IID, WED, "25", "yfinance", currency="HKD")]
    )
    applied = backend.reader.bars(
        [IID], market=Market.HK, adjustment=Adjustment.SPLIT_DIVIDEND
    )
    # factor 100/(100+25)=0.8 on sessions before the ex-date
    assert applied.column("close").to_pylist() == [D(80), D(80), D(100)]


def test_rights_are_stored_and_ignored_by_the_reader(backend):
    ensure_hk_instruments(backend.instruments, ["00000"], as_of=MON)
    sink = HkCanonicalSink(
        backend.instruments, parquet_canonical_writer(backend.canonical)
    )
    sink.after_fetch(job(end=WED), [raw_bar(MON, "100"), raw_bar(WED, "100")])
    act = hk_rights(IID, WED, "yfinance", ratio="1.1", amount="10", currency="HKD")
    backend.instruments.upsert_corporate_actions([act])
    got = backend.instruments.corporate_actions([IID])
    assert len(got) == 1
    assert got[0].kind is CorporateActionKind.RIGHTS
    assert got[0].currency == "HKD"
    out = backend.reader.bars([IID], market=Market.HK, adjustment=Adjustment.ALL)
    assert out.column("close").to_pylist() == [D(100), D(100)]


def test_old_corporate_actions_parquet_without_currency_is_readable(backend):
    # Older files omit the additive currency column; readers treat it as None.
    legacy_fields = [f for f in CORPORATE_ACTIONS_FIELDS if f.name != "currency"]
    legacy = _schema("corporate_actions", legacy_fields)
    records = [
        {
            "instrument_id": IID,
            "ex_date": WED,
            "kind": "dividend",
            "source": "yfinance",
            "ratio": None,
            "amount": D("0.25"),
        }
    ]
    path = backend.ref_dir / "corporate_actions.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(records, schema=legacy)
    pq.write_table(table, path)
    assert "currency" not in pq.read_schema(path).names
    got = backend.instruments.corporate_actions([IID])
    assert len(got) == 1 and got[0].amount == D("0.25") and got[0].currency is None
    # a later upsert rewrites the file with the new column and keeps the old row
    backend.instruments.upsert_corporate_actions(
        [
            CorporateAction(
                IID,
                MON,
                CorporateActionKind.SPLIT,
                "yfinance",
                ratio=D(2),
                currency=None,
            )
        ]
    )
    again = {a.ex_date: a for a in backend.instruments.corporate_actions([IID])}
    assert again[WED].currency is None and again[MON].ratio == D(2)
    assert SCHEMA_VERSION == 1
    assert "currency" in pq.read_schema(path).names


def test_dispatching_sink_routes_hk(backend):
    ensure_hk_instruments(backend.instruments, ["00000"], as_of=MON)
    called = []

    class _Spy:
        def after_fetch(self, job, bars):
            called.append((job.market, len(bars)))

    DispatchingSink(hk=_Spy()).after_fetch(job(), [raw_bar(MON, "100")])
    assert called == [(Market.HK, 1)]
    DispatchingSink().after_fetch(job(), [raw_bar(MON, "100")])  # no hk sink: raw-only
    assert called == [(Market.HK, 1)]
