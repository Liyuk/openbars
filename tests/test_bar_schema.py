"""Schema-only tests: OHLCV policy, source identity/catalog, canonical row, Arrow schema."""

from __future__ import annotations

import json
from dataclasses import FrozenInstanceError, fields
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from openbars.calendars import is_non_session_date
from openbars.core.models import (
    Adjustment,
    Bar,
    CanonicalBar,
    Interval,
    QualityFlag,
    RawBar,
    Session,
)
from openbars.core.ohlcv import validate_ohlcv
from openbars.core.schema import (
    CANONICAL_DAILY_FIELDS,
    CANONICAL_DAILY_UNIQUE_KEY,
    RAW_DAILY_BAR_FIELDS,
    SCHEMA_VERSION,
)
from openbars.core.series import (
    ALPACA_IEX_SPLIT,
    ALPHA_VANTAGE_DAILY_RAW,
    KNOWN_SOURCES,
    AdjustmentScope,
    BarSeriesCatalog,
    BarSeriesIdentity,
    CanonicalDailyRow,
    PriceBasis,
    SessionSemantics,
    SourceIdentity,
    VolumeBasis,
)

GOLDEN = Path(__file__).parent / "golden" / "research_daily_v2"
UTC = timezone.utc
TS = datetime(2024, 1, 3, 14, 30, tzinfo=UTC)


def _cases(name: str):
    return json.loads((GOLDEN / "ohlcv_cases.json").read_text())[name]


def _args(case: dict) -> tuple:
    return tuple(case[k] for k in ("open", "high", "low", "close", "volume"))


# --- OHLCV validation ---------------------------------------------------------------


@pytest.mark.parametrize("case", _cases("valid"))
def test_valid_ohlcv_accepted(case):
    out = validate_ohlcv(*_args(case))
    assert all(isinstance(v, Decimal) for v in out[:4])
    assert out.volume is None or isinstance(out.volume, Decimal)


@pytest.mark.parametrize("case", _cases("invalid"), ids=lambda c: c["why"])
def test_invalid_ohlcv_rejected(case):
    with pytest.raises(ValueError):
        validate_ohlcv(*_args(case))


def test_decimal_strings_stay_exact():
    out = validate_ohlcv("10.50000000", "10.50000000", "10.1", "10.50000000", "1234567.12345678")
    assert str(out.open) == "10.50000000"
    assert out.low == Decimal("10.1")
    assert str(out.volume) == "1234567.12345678"
    # 0.1 + 0.2 style values stay exact; nothing is rounded through float
    assert validate_ohlcv("0.30000000", "0.3", "0.1", "0.2", 1).open == Decimal("0.3")


@pytest.mark.parametrize("field_idx", range(5))
def test_float_inputs_rejected(field_idx):
    args = ["10", "11", "9", "10", "1"]
    args[field_idx] = 10.5 if field_idx < 4 else 1.0
    with pytest.raises(TypeError, match="float"):
        validate_ohlcv(*args)
    with pytest.raises(TypeError):
        validate_ohlcv("10", "11", "9", "10", "1", vwap=10.0)
    with pytest.raises(TypeError):
        validate_ohlcv(True, "11", "9", "10", "1")


def test_vwap_validated():
    assert validate_ohlcv("10", "11", "9", "10", "1", vwap="10.25").vwap == Decimal("10.25")
    with pytest.raises(ValueError):
        validate_ohlcv("10", "11", "9", "10", "1", vwap="0")


def _bar(**kw):
    base = dict(
        instrument_id="TEST1", interval=Interval.D1, ts_start=TS, session_date=date(2024, 1, 3),
        session=Session.REGULAR, open="10", high="11", low="9", close=Decimal("10.5"), volume=100,
        source="massive",
    )  # fmt: skip
    base.update(kw)
    return base


def test_bar_normalises_to_decimal_and_rejects_bad_data():
    bar = Bar(**_bar())
    assert bar.open == Decimal("10") and isinstance(bar.open, Decimal)
    assert bar.volume == Decimal(100) and isinstance(bar.volume, Decimal)
    assert bar.flags is QualityFlag.NONE and bar.adjustment is Adjustment.RAW
    assert bar.ingested_at is None
    with pytest.raises(FrozenInstanceError):
        bar.open = Decimal(1)  # type: ignore[misc]
    with pytest.raises(TypeError):
        Bar(**_bar(open=10.0))
    with pytest.raises(ValueError):
        Bar(**_bar(high="8"))
    with pytest.raises(ValueError):
        Bar(**_bar(ts_start=datetime(2024, 1, 3, 14, 30)))
    assert Bar(**_bar(volume=None)).volume is None


def _raw(**kw):
    base = dict(
        provider="massive", symbol_as_returned="TEST1", fetch_id="f1", interval=Interval.D1,
        ts_start=TS, open="10", high="11", low="9", close="10", volume="5",
    )  # fmt: skip
    base.update(kw)
    return base


def test_raw_bar_fields_and_revision():
    raw = RawBar(**_raw())
    assert raw.revision == 1 and raw.payload_hash is None and raw.series is None
    with pytest.raises(ValueError):
        RawBar(**_raw(revision=0))
    with pytest.raises(TypeError):
        RawBar(**_raw(close=10.0))
    series = BarSeriesCatalog.resolve(SourceIdentity("massive", "eod", "split"))
    ok = RawBar(**_raw(series=series, feed="eod", payload_hash="abc"))
    assert ok.series is series
    with pytest.raises(ValueError):
        RawBar(**_raw(series=series, feed="sip"))
    with pytest.raises(ValueError):
        RawBar(**_raw(provider="alpaca", series=series))


def test_canonical_bar_wraps_validated_bar():
    cb = CanonicalBar(Bar(**_bar()), "massive", "massive", ["massive"], "primary_source")  # type: ignore[arg-type]
    assert cb.sources_seen == ("massive",)
    with pytest.raises(TypeError):
        CanonicalBar(object(), "a", None, (), "r")  # type: ignore[arg-type]


# --- source identity + catalog ------------------------------------------------------


def test_source_identity_strings_are_exact():
    assert [s.key for s in KNOWN_SOURCES] == [
        "massive/eod/split",
        "alpaca/sip/split",
        "alpaca/iex/split",
        "yfinance/yahoo/split",
        "twelve_data/daily/split",
        "alpha_vantage/daily/raw",
    ]
    assert SourceIdentity.parse("alpaca/iex/split") == ALPACA_IEX_SPLIT
    assert tuple(f.name for f in fields(SourceIdentity)) == ("provider", "feed", "adjustment")
    with pytest.raises(ValueError):
        SourceIdentity.parse("alpaca/iex")


def test_enum_values_are_exact():
    assert [e.value for e in PriceBasis] == [
        "raw", "split_adjusted", "total_return_adjusted", "provider_defined", "unknown",
    ]  # fmt: skip
    assert [e.value for e in AdjustmentScope] == [
        "none", "splits", "dividends", "splits_and_dividends", "provider_defined", "unknown",
    ]  # fmt: skip
    assert [e.value for e in SessionSemantics] == [
        "regular",
        "extended",
        "provider_aligned",
        "utc_day",
        "unknown",
    ]
    assert [e.value for e in VolumeBasis] == ["as_traded", "split_adjusted", "provider_defined", "unknown"]
    assert Interval.D1.value == "1d"


@pytest.mark.parametrize("case", json.loads((GOLDEN / "series_catalog.json").read_text()), ids=lambda c: c["source"])
def test_catalog_resolves_known_sources(case):
    ident = BarSeriesCatalog.resolve(SourceIdentity.parse(case["source"]))
    assert ident.source.key == case["source"]
    assert ident.interval is Interval.D1
    assert ident.price_basis.value == case["price_basis"]
    assert ident.adjustment_scope.value == case["adjustment_scope"]
    assert ident.session_semantics.value == case["session_semantics"]
    assert ident.volume_basis.value == case["volume_basis"]
    assert ident.currency == "USD"
    assert ident.adjustment_as_of is None


def test_catalog_covers_every_known_source():
    cases = {c["source"] for c in json.loads((GOLDEN / "series_catalog.json").read_text())}
    assert cases == {s.key for s in KNOWN_SOURCES}


def test_catalog_unknown_is_all_unknown():
    ident = BarSeriesCatalog.resolve(SourceIdentity("zzz_provider", "feed", "split"), currency="eur")
    assert ident.price_basis is PriceBasis.UNKNOWN
    assert ident.adjustment_scope is AdjustmentScope.UNKNOWN
    assert ident.session_semantics is SessionSemantics.UNKNOWN
    assert ident.volume_basis is VolumeBasis.UNKNOWN
    assert ident.currency == "EUR"
    assert BarSeriesCatalog.resolve(SourceIdentity("zzz", "f", "raw"), currency=None).currency is None


def test_catalog_known_source_at_other_interval_is_unknown():
    ident = BarSeriesCatalog.resolve(ALPHA_VANTAGE_DAILY_RAW, interval=Interval.H1)
    assert ident.interval is Interval.H1
    assert ident.price_basis is PriceBasis.UNKNOWN


def test_series_identity_validates_currency():
    with pytest.raises(ValueError):
        BarSeriesIdentity("p", "f", "split", currency="US")
    with pytest.raises(ValueError):
        BarSeriesIdentity("p", "f", "split", currency="U$D")
    assert tuple(f.name for f in fields(BarSeriesIdentity)) == (
        "provider", "feed", "legacy_adjustment", "interval", "price_basis", "adjustment_scope",
        "adjustment_as_of", "session_semantics", "currency", "volume_basis",
    )  # fmt: skip


# --- canonical row ------------------------------------------------------------------

CANONICAL_FIELD_NAMES = (
    "instrument_id", "session_date", "policy_version", "selected_source_bar_id", "provider",
    "feed", "adjustment", "revision", "open", "high", "low", "close", "volume",
    "selection_reason", "quality_status", "capabilities", "source_fingerprint", "generated_at",
    "currency", "available_at",
)  # fmt: skip


def _golden_row() -> dict:
    data = json.loads((GOLDEN / "canonical_row_zzza.json").read_text())
    data["session_date"] = date.fromisoformat(data["session_date"])
    data["generated_at"] = datetime.fromisoformat(data["generated_at"])
    return data


def test_canonical_row_fields_exact():
    assert tuple(f.name for f in fields(CanonicalDailyRow)) == CANONICAL_FIELD_NAMES
    assert CanonicalDailyRow.UNIQUE_KEY == ("instrument_id", "session_date", "policy_version")


def test_golden_canonical_row():
    row = CanonicalDailyRow(**_golden_row())
    assert row.unique_key == ("ZZZA", date(2024, 1, 3), "research-daily-v2")
    assert date(2024, 1, 3).weekday() == 2  # Wednesday
    assert not is_non_session_date(row.session_date)
    assert (row.provider, row.feed, row.adjustment) == ("massive", "eod", "split")
    assert row.selection_reason == "primary_source" and row.quality_status == "complete"
    assert row.close == Decimal("101.00000000") and str(row.volume) == "123456.00000000"
    assert row.capabilities[0] == "price_basis:split_adjusted"
    # the golden capabilities agree with the catalog for this source
    series = BarSeriesCatalog.resolve(SourceIdentity("massive", "eod", "split"))
    assert f"price_basis:{series.price_basis.value}" in row.capabilities
    assert f"volume_basis:{series.volume_basis.value}" in row.capabilities


@pytest.mark.parametrize(
    "override,exc",
    [
        ({"selection_reason": "bogus"}, ValueError),
        ({"selection_reason": "fill_"}, ValueError),
        ({"quality_status": "ok"}, ValueError),
        ({"instrument_id": ""}, ValueError),
        ({"revision": -1}, ValueError),
        ({"high": "99"}, ValueError),
        ({"close": 101.0}, TypeError),
        ({"capabilities": "abc"}, TypeError),
        ({"generated_at": datetime(2024, 1, 3, 23, 0)}, ValueError),
        ({"session_date": datetime(2024, 1, 3)}, TypeError),
    ],
)
def test_canonical_row_rejects_bad_values(override, exc):
    with pytest.raises(exc):
        CanonicalDailyRow(**{**_golden_row(), **override})


@pytest.mark.parametrize("reason", ["primary_source", "primary_source_corroborated", "compatible_gap_fill",
                                    "fill_yfinance", "fill_twelve_data", "spike_replaced"])
def test_selection_reasons_accepted(reason):
    assert CanonicalDailyRow(**{**_golden_row(), "selection_reason": reason}).selection_reason == reason


@pytest.mark.parametrize("status", ["complete", "divergent", "volume_missing", "spike_suspect"])
def test_quality_statuses_accepted(status):
    assert CanonicalDailyRow(**{**_golden_row(), "quality_status": status}).quality_status == status


def test_capabilities_list_is_stored_as_tuple():
    row = CanonicalDailyRow(**{**_golden_row(), "capabilities": ["a", "b"]})
    assert row.capabilities == ("a", "b")


# --- Arrow schema -------------------------------------------------------------------


def test_field_declarations_match_models():
    assert tuple(f.name for f in CANONICAL_DAILY_FIELDS) == CANONICAL_FIELD_NAMES
    assert CANONICAL_DAILY_UNIQUE_KEY == CanonicalDailyRow.UNIQUE_KEY
    raw_names = {f.name for f in RAW_DAILY_BAR_FIELDS}
    assert {
        "provider", "feed", "adjustment", "price_basis", "adjustment_scope", "adjustment_as_of",
        "session_semantics", "currency", "volume_basis", "revision", "open", "high", "low",
        "close", "volume", "vwap", "payload_hash", "fetch_id", "symbol_as_returned",
        "instrument_id", "session_date",
    } <= raw_names  # fmt: skip
    assert SCHEMA_VERSION == 1


def test_arrow_schemas():
    pa = pytest.importorskip("pyarrow")
    from openbars.core.schema import canonical_daily_schema, raw_daily_bar_schema

    canon = canonical_daily_schema()
    assert canon.names == list(CANONICAL_FIELD_NAMES)
    for name in ("open", "high", "low", "close"):
        assert canon.field(name).type == pa.decimal128(20, 8)
    assert canon.field("volume").type == pa.decimal128(28, 8)
    assert canon.field("session_date").type == pa.date32()
    assert canon.field("generated_at").type == pa.timestamp("us", tz="UTC")
    assert canon.field("revision").type == pa.int64()
    assert canon.field("capabilities").type == pa.list_(pa.string())
    assert canon.field("selected_source_bar_id").nullable
    assert not canon.field("instrument_id").nullable
    assert canon.metadata[b"openbars.schema_version"] == b"1"

    raw = raw_daily_bar_schema()
    assert raw.field("vwap").type == pa.decimal128(20, 8) and raw.field("vwap").nullable
    assert raw.field("volume").type == pa.decimal128(28, 8)
    assert raw.field("adjustment_as_of").type == pa.date32()
    assert raw.field("revision").type == pa.int64()
    assert raw.field("ts_start").type == pa.timestamp("us", tz="UTC")
    assert not raw.field("provider").nullable

    # a golden row round-trips through Arrow without float
    row = CanonicalDailyRow(**_golden_row())
    table = pa.Table.from_pylist(
        [{f.name: getattr(row, f.name) for f in fields(CanonicalDailyRow)}], schema=canon
    )
    assert table.to_pylist()[0]["close"] == row.close
    assert table.to_pylist()[0]["capabilities"] == list(row.capabilities)


def test_session_semantics_enums_stay_in_sync():
    from openbars.core.series import SessionSemantics as CoreSessionSemantics
    from openbars.storage.schema import SessionSemantics as StorageSessionSemantics

    assert [e.value for e in CoreSessionSemantics] == [e.value for e in StorageSessionSemantics]
