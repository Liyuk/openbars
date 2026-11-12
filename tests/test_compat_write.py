"""Unit tests for upstream_compat write helpers; live round-trip tests need OPENBARS_TEST_PG_DSN."""

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from openbars.core.models import AssetClass, DateRange, Instrument, Interval, Market, SymbolMapping
from openbars.storage.compat_write import (
    RESEARCH_SERIES_READ_SQL,
    advisory_lock_key,
    parse_int_id,
    parse_optional_int_ref,
    quality_status_of,
    security_type_for,
)
from openbars.storage.errors import StorageError
from openbars.storage.postgres import PostgresBackend
from openbars.storage.rows import CanonicalBarRow, RawBarRow
from openbars.storage.schema import DEFAULT_POLICY_VERSION

# Frozen compatibility-contract fixture: do not recompute in a way that hides a prefix typo.
_LOCK_FIXTURE = 4840611360074829998


def test_advisory_lock_key_matches_published_fixture():
    assert advisory_lock_key("research-daily-v2", date(2024, 1, 2)) == _LOCK_FIXTURE


def test_parse_int_id_and_refs():
    assert parse_int_id("42") == 42
    assert parse_int_id("-7") == -7
    assert parse_int_id("ZZZA") is None
    assert parse_int_id("42a") is None
    assert parse_int_id("") is None
    assert parse_optional_int_ref("source_artifact:12") == 12
    assert parse_optional_int_ref("12") == 12
    assert parse_optional_int_ref(12) == 12
    assert parse_optional_int_ref("source_artifact:abc") is None
    assert parse_optional_int_ref("note:12") is None


def test_security_type_and_quality_helpers():
    assert security_type_for(AssetClass.EQUITY) == "common_stock"
    assert security_type_for(AssetClass.ETF) == "etf"
    assert security_type_for(None) == "common_stock"

    class _Row:
        def __init__(self, **kw):
            self.__dict__.update(kw)

    assert quality_status_of(_Row(quality_status="divergent", flags=())) == "divergent"
    assert quality_status_of(_Row(quality_status="", flags=())) == "complete"


def _raw(**kw) -> RawBarRow:
    base = dict(
        provider="prov_a",
        market=Market.US,
        symbol_as_returned="ZZZA",
        interval=Interval.D1,
        ts_start=datetime(2024, 1, 2, tzinfo=UTC),
        session_date=date(2024, 1, 2),
        open=Decimal("11.25"),
        high=Decimal("12"),
        low=Decimal("11"),
        close=Decimal("11.5"),
        volume=Decimal("1000000.5"),
        fetch_id="fetch-1",
        feed="f",
        adjustment="split",
        instrument_id="ZZZA",
        revision=1,
    )
    base.update(kw)
    return RawBarRow(**base)


def _canon(**kw) -> CanonicalBarRow:
    base = dict(
        instrument_id="ZZZA",
        market=Market.US,
        interval=Interval.D1,
        ts_start=datetime(2024, 1, 2, tzinfo=UTC),
        session_date=date(2024, 1, 2),
        session="regular",
        provider="prov_a",
        feed="f",
        adjustment="split",
        revision=1,
        open=Decimal("11.25"),
        high=Decimal("12"),
        low=Decimal("11"),
        close=Decimal("11.5"),
        volume=Decimal("1000000.5"),
        price_source="prov_a",
        reconcile_rule="research-daily-v2",
        policy_version="research-daily-v2",
        selection_reason="primary_source",
        quality_status="complete",
        capabilities=("price_ohlc",),
        source_fingerprint="a" * 64,
        generated_at=datetime(2024, 1, 2, 12, tzinfo=UTC),
    )
    base.update(kw)
    return CanonicalBarRow(**base)


@pytest.mark.postgres_live
def test_live_upstream_compat_writes_and_roundtrips(pg_dsn, tmp_path):
    import psycopg

    ddl = (Path(__file__).parent / "compat" / "upstream_schema.sql").read_text()
    schema = f"openbars_compat_w_{datetime.now(UTC):%H%M%S%f}"
    with psycopg.connect(pg_dsn, autocommit=True) as admin:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    try:
        with psycopg.connect(pg_dsn, autocommit=True, options=f"-c search_path={schema}") as conn:
            conn.execute(ddl)

        backend = PostgresBackend(
            pg_dsn, mode="upstream_compat", write=True, schema=schema, state_path=tmp_path / "state.db"
        )
        window = DateRange(date(2024, 1, 1), date(2024, 1, 31))

        assert backend.raw.append_rows([_raw()]) == 1
        assert backend.canonical.upsert_rows([_canon()]) == 1
        # Idempotent: second write does not duplicate unique keys.
        assert backend.raw.append_rows([_raw(close=Decimal("11.75"))]) == 1
        assert backend.canonical.upsert_rows([_canon(close=Decimal("11.75"))]) == 1

        (raw,) = backend.raw.scan_rows("prov_a", Market.US, Interval.D1, window)
        assert raw.symbol_as_returned == "ZZZA"
        assert raw.close == Decimal("11.75")
        assert raw.instrument_id.isdigit()
        iid = raw.instrument_id
        (canon,) = backend.canonical.scan_rows([iid], Interval.D1, window)
        assert canon.close == Decimal("11.75")
        assert canon.quality_status == "complete"
        assert canon.capabilities == ("price_ohlc",)
        assert canon.selection_reason == "primary_source"

        from psycopg.rows import dict_row

        with psycopg.connect(pg_dsn, row_factory=dict_row, options=f"-c search_path={schema}") as conn:
            wm = conn.execute("SELECT version FROM research_series_watermark WHERE policy_version = %s", ["research-daily-v2"]).fetchone()
            assert wm["version"] >= 1
            dwm = conn.execute(
                "SELECT version FROM research_series_date_watermark WHERE policy_version = %s AND session_date = %s",
                ["research-daily-v2", date(2024, 1, 2)],
            ).fetchone()
            assert dwm is not None and dwm["version"] >= 1
            bars = conn.execute(
                RESEARCH_SERIES_READ_SQL.replace("?", "%s"),
                [[int(iid)], date(2024, 1, 1), date(2024, 1, 31), "research-daily-v2"],
            ).fetchall()
            assert len(bars) == 1
            assert bars[0]["symbol"] == "ZZZA"
            assert bars[0]["close"] == Decimal("11.75")
            # selected_source_bar_id should have been filled from the raw row
            assert bars[0]["selected_source_bar_id"] is not None
            # unique keys held
            n_raw = conn.execute("SELECT count(*) AS n FROM daily_bar").fetchone()["n"]
            n_canon = conn.execute("SELECT count(*) AS n FROM research_daily_bar").fetchone()["n"]
            assert n_raw == 1 and n_canon == 1

        # Numeric string instrument id is reused as the bigint.
        backend.raw.append_rows([_raw(instrument_id="42", symbol_as_returned="ZZZC", session_date=date(2024, 1, 3), ts_start=datetime(2024, 1, 3, tzinfo=UTC), fetch_id="fetch-42")])
        backend.canonical.upsert_rows([_canon(instrument_id="42", session_date=date(2024, 1, 3), ts_start=datetime(2024, 1, 3, tzinfo=UTC))])
        (raw42,) = backend.raw.scan_rows("prov_a", Market.US, Interval.D1, window, instrument_ids=["42"])
        assert raw42.instrument_id == "42"
        assert raw42.symbol_as_returned == "ZZZC"
        assert backend.instruments.resolve("ZZZC", on=date(2024, 1, 3), provider="x", market=Market.US) == "42"

        # Missing volume is refused.
        with pytest.raises(StorageError, match="volume"):
            backend.raw.append_rows([_raw(volume=None, fetch_id="fetch-novol")], validate=False)
    finally:
        with psycopg.connect(pg_dsn, autocommit=True) as admin:
            admin.execute(f'DROP SCHEMA "{schema}" CASCADE')


@pytest.mark.postgres_live
def test_live_upstream_compat_ticker_reuse_is_ambiguous(pg_dsn, tmp_path):
    import psycopg

    ddl = (Path(__file__).parent / "compat" / "upstream_schema.sql").read_text()
    schema = f"openbars_compat_r_{datetime.now(UTC):%H%M%S%f}"
    with psycopg.connect(pg_dsn, autocommit=True) as admin:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    try:
        with psycopg.connect(pg_dsn, autocommit=True, options=f"-c search_path={schema}") as conn:
            conn.execute(ddl)
            conn.execute("INSERT INTO instrument (id, asset_class, security_type) VALUES (10, 'equity', 'common_stock')")
            conn.execute("INSERT INTO instrument (id, asset_class, security_type) VALUES (11, 'equity', 'common_stock')")
            conn.execute(
                "INSERT INTO symbol_alias (instrument_id, symbol, exchange, valid_from, valid_from_basis, verification_status) "
                "VALUES (10, 'ZZZB', 'TESTX', '2010-01-01', 'listing', 'verified'), "
                "(11, 'ZZZB', 'TESTX', '2020-01-01', 'listing', 'verified')"
            )
            # Close the first alias so both do not overlap? The test wants SAME-day coverage
            # overlap: leave both open (valid_to NULL) so 2024-01-02 matches two instruments.
        backend = PostgresBackend(
            pg_dsn, mode="upstream_compat", write=True, schema=schema, state_path=tmp_path / "state.db"
        )
        with pytest.raises(StorageError, match="more than one instrument"):
            backend.raw.append_rows([_raw(instrument_id="ZZZB", symbol_as_returned="ZZZB")])
    finally:
        with psycopg.connect(pg_dsn, autocommit=True) as admin:
            admin.execute(f'DROP SCHEMA "{schema}" CASCADE')
