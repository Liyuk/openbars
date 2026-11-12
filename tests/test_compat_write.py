"""Unit tests for upstream_compat write helpers; live round-trip tests need OPENBARS_TEST_PG_DSN."""

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from openbars.core.models import (
    AssetClass,
    CorporateAction,
    CorporateActionKind,
    DateRange,
    Instrument,
    Interval,
    Market,
    SymbolMapping,
)
from openbars.storage.compat_write import (
    RESEARCH_SERIES_READ_SQL,
    AmbiguousInstrumentError,
    action_details,
    action_event_key,
    action_type_for,
    actions_from_revision,
    kind_from_action_type,
    ratio_amount_from_details,
    advisory_lock_key,
    parse_int_id,
    parse_optional_int_ref,
    quality_status_of,
    security_type_for,
    split_fraction,
)
from openbars.normalize.instruments import UnresolvedRecord, select_mappings
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



def test_action_helpers_and_split_fraction():
    assert split_fraction(Decimal("4")) == (4, 1)
    assert split_fraction(Decimal("0.25")) == (1, 4)
    assert split_fraction(Decimal("1.5")) == (3, 2)
    assert action_type_for(CorporateActionKind.SPLIT) == "split"
    assert action_type_for(CorporateActionKind.DIVIDEND, amount=Decimal("0.2")) == "cash_dividend"
    assert action_type_for(CorporateActionKind.DIVIDEND, ratio=Decimal("1.1")) == "stock_dividend"
    assert action_type_for(CorporateActionKind.SPINOFF) == "spin_off"
    details = action_details(CorporateActionKind.SPLIT, ratio=Decimal("4"))
    assert details == {"new_shares_numerator": 4, "old_shares_denominator": 1}
    assert action_event_key("split", 42, date(2024, 1, 2)) == "openbars:split:42:2024-01-02"


def test_revision_maps_back_to_corporate_actions():
    assert kind_from_action_type("split") is CorporateActionKind.SPLIT
    assert kind_from_action_type("cash_dividend") is CorporateActionKind.DIVIDEND
    assert kind_from_action_type("stock_dividend") is CorporateActionKind.DIVIDEND
    assert kind_from_action_type("spin_off") is CorporateActionKind.SPINOFF
    assert kind_from_action_type("nope") is None
    ratio, amount = ratio_amount_from_details("split", {"new_shares_numerator": 4, "old_shares_denominator": 1})
    assert ratio == Decimal("4") and amount is None
    ratio, amount = ratio_amount_from_details("cash_dividend", {"cash_amount": "0.20"})
    assert ratio is None and amount == Decimal("0.20")
    actions = actions_from_revision(
        action_type="split",
        ex_date=date(2024, 3, 1),
        instrument_ids=[42],
        details={"new_shares_numerator": 4, "old_shares_denominator": 1},
        source="prov_a",
    )
    assert len(actions) == 1
    assert actions[0].instrument_id == "42" and actions[0].kind is CorporateActionKind.SPLIT
    assert actions[0].ratio == Decimal("4") and actions[0].source == "prov_a"


def test_select_mappings_keeps_wildcard_and_exact_provider():
    generic = SymbolMapping("1", "ZZZA", date(2020, 1, 1), provider="*")
    specific = SymbolMapping("1", "ZZZA", date(2020, 1, 1), provider="prov_a")
    other = SymbolMapping("2", "ZZZB", date(2020, 1, 1), provider="prov_b")
    assert select_mappings([generic, specific, other]) == [generic, specific, other]
    assert select_mappings([generic, specific, other], "prov_a") == [generic, specific]
    assert select_mappings([generic, specific, other], "prov_b") == [generic, other]


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
        with pytest.raises(AmbiguousInstrumentError) as exc:
            backend.raw.append_rows([_raw(instrument_id="ZZZB", symbol_as_returned="ZZZB")])
        err = exc.value
        assert err.symbol == "ZZZB" and err.on == date(2024, 1, 2)
        assert [c.instrument_id for c in err.candidates] == [10, 11]
        report = err.format_report()
        assert "instrument_id=10" in report and "instrument_id=11" in report
        assert "2010-01-01..open" in report and "2020-01-01..open" in report
        assert "ZZZB" in report
    finally:
        with psycopg.connect(pg_dsn, autocommit=True) as admin:
            admin.execute(f'DROP SCHEMA "{schema}" CASCADE')


@pytest.mark.postgres_live
def test_live_compat_batch_skips_ambiguous_rows(pg_dsn, tmp_path, caplog):
    import logging

    import psycopg
    from psycopg.rows import dict_row

    ddl = (Path(__file__).parent / "compat" / "upstream_schema.sql").read_text()
    # Live upstream has no default on instrument.created_at.
    ddl = ddl.replace("created_at TIMESTAMPTZ NOT NULL DEFAULT now()", "created_at TIMESTAMPTZ NOT NULL", 1)
    schema = f"openbars_compat_s_{datetime.now(UTC):%H%M%S%f}"
    with psycopg.connect(pg_dsn, autocommit=True) as admin:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    try:
        with psycopg.connect(pg_dsn, autocommit=True, options=f"-c search_path={schema}") as conn:
            conn.execute(ddl)
            conn.execute(
                "INSERT INTO instrument (id, asset_class, security_type, created_at) "
                "VALUES (10, 'equity', 'common_stock', now()), (11, 'equity', 'common_stock', now())"
            )
            conn.execute(
                "INSERT INTO symbol_alias (instrument_id, symbol, exchange, valid_from, valid_from_basis, verification_status) "
                "VALUES (10, 'ZZZB', 'TESTX', '2010-01-01', 'listing', 'verified'), "
                "(11, 'ZZZB', 'TESTX', '2020-01-01', 'listing', 'verified')"
            )
        backend = PostgresBackend(
            pg_dsn, mode="upstream_compat", write=True, schema=schema, state_path=tmp_path / "state.db"
        )
        with caplog.at_level(logging.WARNING, logger="openbars.storage.compat"):
            assert backend.raw.append_rows([
                _raw(instrument_id="ZZZA", symbol_as_returned="ZZZA"),
                _raw(instrument_id="ZZZB", symbol_as_returned="ZZZB", fetch_id="fetch-b"),
            ]) == 1
            assert backend.canonical.upsert_rows([_canon(instrument_id="ZZZA"), _canon(instrument_id="ZZZB")]) == 1
        warned = [r.getMessage() for r in caplog.records if r.name == "openbars.storage.compat"]
        assert len(warned) == 2
        assert all("ZZZB" in m and "2024-01-02" in m for m in warned)
        with psycopg.connect(pg_dsn, row_factory=dict_row, options=f"-c search_path={schema}") as conn:
            for table in ("daily_bar", "research_daily_bar"):
                ids = [r["instrument_id"] for r in conn.execute(f"SELECT instrument_id FROM {table}").fetchall()]
                assert len(ids) == 1 and ids[0] not in (10, 11)
            aliases = conn.execute("SELECT count(*) AS n FROM symbol_alias WHERE symbol = 'ZZZB'").fetchone()["n"]
            assert aliases == 2
            created = conn.execute("SELECT created_at FROM instrument WHERE id NOT IN (10, 11)").fetchall()
            assert len(created) == 1 and created[0]["created_at"] is not None
        # Canonical-only batch where every row is ambiguous still raises.
        with pytest.raises(AmbiguousInstrumentError):
            backend.canonical.upsert_rows([_canon(instrument_id="ZZZB", session_date=date(2024, 1, 3), ts_start=datetime(2024, 1, 3, tzinfo=UTC))])
    finally:
        with psycopg.connect(pg_dsn, autocommit=True) as admin:
            admin.execute(f'DROP SCHEMA "{schema}" CASCADE')


@pytest.mark.postgres_live
def test_live_canonical_only_fills_selected_source_bar_id(pg_dsn, tmp_path):
    import psycopg
    from psycopg.rows import dict_row

    ddl = (Path(__file__).parent / "compat" / "upstream_schema.sql").read_text()
    schema = f"openbars_compat_c_{datetime.now(UTC):%H%M%S%f}"
    with psycopg.connect(pg_dsn, autocommit=True) as admin:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    try:
        with psycopg.connect(pg_dsn, autocommit=True, options=f"-c search_path={schema}") as conn:
            conn.execute(ddl)
        backend = PostgresBackend(
            pg_dsn, mode="upstream_compat", write=True, schema=schema, state_path=tmp_path / "state.db"
        )
        assert backend.canonical.upsert_rows([_canon()]) == 1
        with psycopg.connect(pg_dsn, row_factory=dict_row, options=f"-c search_path={schema}") as conn:
            research = conn.execute("SELECT * FROM research_daily_bar").fetchone()
            raw = conn.execute("SELECT * FROM daily_bar").fetchone()
            assert research["selected_source_bar_id"] is not None
            assert raw is not None
            assert research["selected_source_bar_id"] == raw["id"]
            assert raw["close"] == Decimal("11.5")
            assert raw["provider"] == "prov_a"
        # A later raw upsert with the same identity keeps the same id and updates values.
        assert backend.raw.append_rows([_raw(close=Decimal("11.80"))]) == 1
        with psycopg.connect(pg_dsn, row_factory=dict_row, options=f"-c search_path={schema}") as conn:
            rows = conn.execute("SELECT id, close FROM daily_bar").fetchall()
            assert len(rows) == 1
            assert rows[0]["id"] == research["selected_source_bar_id"]
            assert rows[0]["close"] == Decimal("11.80")
    finally:
        with psycopg.connect(pg_dsn, autocommit=True) as admin:
            admin.execute(f'DROP SCHEMA "{schema}" CASCADE')


@pytest.mark.postgres_live
def test_live_upstream_compat_writes_corporate_actions(pg_dsn, tmp_path):
    import psycopg
    from psycopg.rows import dict_row

    ddl = (Path(__file__).parent / "compat" / "upstream_schema.sql").read_text()
    schema = f"openbars_compat_a_{datetime.now(UTC):%H%M%S%f}"
    with psycopg.connect(pg_dsn, autocommit=True) as admin:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    try:
        with psycopg.connect(pg_dsn, autocommit=True, options=f"-c search_path={schema}") as conn:
            conn.execute(ddl)
        backend = PostgresBackend(
            pg_dsn, mode="upstream_compat", write=True, schema=schema, state_path=tmp_path / "state.db"
        )
        action = CorporateAction(
            instrument_id="42",
            ex_date=date(2024, 3, 1),
            kind=CorporateActionKind.SPLIT,
            source="prov_a",
            ratio=Decimal("4"),
        )
        backend.instruments.upsert_corporate_actions([action])
        backend.instruments.upsert_corporate_actions([action])  # idempotent
        with psycopg.connect(pg_dsn, row_factory=dict_row, options=f"-c search_path={schema}") as conn:
            events = conn.execute("SELECT * FROM corporate_action_event").fetchall()
            revs = conn.execute("SELECT * FROM corporate_action_revision ORDER BY revision").fetchall()
            obs = conn.execute("SELECT * FROM corporate_action_observation").fetchall()
            links = conn.execute("SELECT * FROM corporate_action_observation_link").fetchall()
            inst = conn.execute("SELECT id FROM instrument WHERE id = 42").fetchone()
            assert inst is not None
            assert len(events) == 1 and events[0]["event_key"] == "openbars:split:42:2024-03-01"
            assert len(revs) == 1 and revs[0]["action_type"] == "split" and revs[0]["revision"] == 1
            details = revs[0]["details"]
            if isinstance(details, str):
                import json
                details = json.loads(details)
            assert details["new_shares_numerator"] == 4 and details["old_shares_denominator"] == 1
            assert list(revs[0]["instrument_ids"] if not isinstance(revs[0]["instrument_ids"], str) else __import__("json").loads(revs[0]["instrument_ids"])) == [42]
            assert len(obs) == 1 and obs[0]["provider"] == "prov_a"
            assert len(links) == 1
        read_back = backend.instruments.corporate_actions(["42"])
        assert len(read_back) == 1
        assert read_back[0].instrument_id == "42"
        assert read_back[0].kind is CorporateActionKind.SPLIT
        assert read_back[0].ex_date == date(2024, 3, 1)
        assert read_back[0].ratio == Decimal("4")
        assert read_back[0].amount is None
        assert read_back[0].source == "prov_a"
        dividend = CorporateAction(
            instrument_id="42",
            ex_date=date(2024, 6, 1),
            kind=CorporateActionKind.DIVIDEND,
            source="prov_a",
            amount=Decimal("0.25"),
        )
        backend.instruments.upsert_corporate_actions([dividend])
        kinds = {(a.kind, a.ex_date, a.amount or a.ratio) for a in backend.instruments.corporate_actions(["42"])}
        assert (CorporateActionKind.SPLIT, date(2024, 3, 1), Decimal("4")) in kinds
        assert (CorporateActionKind.DIVIDEND, date(2024, 6, 1), Decimal("0.25")) in kinds
        assert backend.instruments.corporate_actions(["99"]) == []
    finally:
        with psycopg.connect(pg_dsn, autocommit=True) as admin:
            admin.execute(f'DROP SCHEMA "{schema}" CASCADE')


@pytest.mark.postgres_live
def test_live_compat_mappings_lists_symbol_aliases(pg_dsn, tmp_path):
    import psycopg

    ddl = (Path(__file__).parent / "compat" / "upstream_schema.sql").read_text()
    schema = f"openbars_compat_m_{datetime.now(UTC):%H%M%S%f}"
    with psycopg.connect(pg_dsn, autocommit=True) as admin:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    try:
        with psycopg.connect(pg_dsn, autocommit=True, options=f"-c search_path={schema}") as conn:
            conn.execute(ddl)
            conn.execute("INSERT INTO instrument (id, asset_class, security_type) VALUES (10, 'equity', 'common_stock')")
            conn.execute(
                "INSERT INTO symbol_alias (instrument_id, symbol, exchange, valid_from, valid_from_basis, verification_status) "
                "VALUES (10, 'ZZZA', 'TESTX', '2020-01-01', 'listing', 'verified')"
            )
        backend = PostgresBackend(
            pg_dsn, mode="upstream_compat", write=False, schema=schema, state_path=tmp_path / "state.db"
        )
        maps = backend.instruments.mappings()
        assert len(maps) == 1
        assert maps[0].instrument_id == "10"
        assert maps[0].symbol == "ZZZA"
        assert maps[0].valid_from == date(2020, 1, 1) and maps[0].valid_to is None
        assert maps[0].provider == "*"
        # aliases are provider-agnostic, so a provider filter still returns them
        assert backend.instruments.mappings(provider="prov_a") == maps
        insts = backend.instruments.instruments()
        assert [i.instrument_id for i in insts] == ["10"]
        assert insts[0].market is Market.US and insts[0].exchange == "TESTX"
        assert insts[0].listed_on == date(2020, 1, 1) and insts[0].delisted_on is None
        assert backend.instruments.get("10") is not None
        assert backend.instruments.upstream_ids()["10"] == 10
        assert not any(m.instrument_id.startswith("crypto:") for m in maps)
    finally:
        with psycopg.connect(pg_dsn, autocommit=True) as admin:
            admin.execute(f'DROP SCHEMA "{schema}" CASCADE')


@pytest.mark.postgres_live
def test_live_compat_instrument_store_side_tables(pg_dsn, tmp_path):
    import psycopg

    ddl = (Path(__file__).parent / "compat" / "upstream_schema.sql").read_text()
    schema = f"openbars_compat_s_{datetime.now(UTC):%H%M%S%f}"
    with psycopg.connect(pg_dsn, autocommit=True) as admin:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    try:
        with psycopg.connect(pg_dsn, autocommit=True, options=f"-c search_path={schema}") as conn:
            conn.execute(ddl)
            conn.execute("INSERT INTO instrument (id, asset_class, security_type) VALUES (10, 'equity', 'common_stock')")
            conn.execute(
                "INSERT INTO symbol_alias (instrument_id, symbol, exchange, valid_from, valid_from_basis, verification_status) "
                "VALUES (10, 'ZZZA', 'TESTX', '2020-01-01', 'listing', 'verified')"
            )
        master = PostgresBackend(
            pg_dsn, mode="upstream_compat", write=True, schema=schema, state_path=tmp_path / "state.db"
        ).instruments
        rec = UnresolvedRecord(
            "corporate_action", "unknown_action_type", "10", "prov_a", date(2024, 1, 2), Market.US, "mystery"
        )
        master.record_unresolved([rec])
        assert master.unresolved_count() == 1
        assert master.unresolved("corporate_action")[0].detail == "mystery"
        master.discard_unresolved(master.unresolved())
        assert master.unresolved_count() == 0
        master.observe([("10", "ZZZA", "prov_a", date(2024, 2, 1))])
        master.observe([("10", "ZZZA", "prov_a", date(2024, 3, 1))])
        assert master.observations() == [("10", "ZZZA", "prov_a", date(2024, 3, 1))]
        master.set_upstream_id("local-x", 99)
        assert master.upstream_ids()["10"] == 10 and master.upstream_ids()["local-x"] == 99
        master.remove_mappings(master.mappings())
        assert master.mappings() == []
    finally:
        with psycopg.connect(pg_dsn, autocommit=True) as admin:
            admin.execute(f'DROP SCHEMA "{schema}" CASCADE')


@pytest.mark.postgres_live
def test_live_unknown_corporate_action_type_is_recorded_and_warned(pg_dsn, tmp_path):
    import json
    import warnings

    import psycopg

    ddl = (Path(__file__).parent / "compat" / "upstream_schema.sql").read_text()
    schema = f"openbars_compat_u_{datetime.now(UTC):%H%M%S%f}"
    with psycopg.connect(pg_dsn, autocommit=True) as admin:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    try:
        with psycopg.connect(pg_dsn, autocommit=True, options=f"-c search_path={schema}") as conn:
            conn.execute(ddl)
            ev = conn.execute(
                "INSERT INTO corporate_action_event (event_key) VALUES ('upstream:mystery:10:2024-07-01') RETURNING id"
            ).fetchone()[0]
            conn.execute(
                "INSERT INTO corporate_action_revision ("
                "event_id, revision, action_type, verification_status, valid_from, known_at, "
                "ex_date, instrument_ids, observation_ids, details, actor) "
                "VALUES (%s, 1, 'mystery_event', 'source_reported', %s, now(), %s, %s, %s, %s, 'upstream')",
                (ev, date(2024, 7, 1), date(2024, 7, 1), json.dumps([10]), json.dumps([]), json.dumps({})),
            )
        backend = PostgresBackend(
            pg_dsn, mode="upstream_compat", write=False, schema=schema, state_path=tmp_path / "state.db"
        )
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            actions = backend.instruments.corporate_actions()
        assert actions == []
        assert any("mystery_event" in str(w.message) and "unknown corporate action type" in str(w.message) for w in caught)
        queued = backend.instruments.unresolved("corporate_action")
        assert len(queued) == 1
        assert queued[0].reason_code == "unknown_action_type"
        assert queued[0].detail == "mystery_event"
        assert queued[0].symbol == "10"
    finally:
        with psycopg.connect(pg_dsn, autocommit=True) as admin:
            admin.execute(f'DROP SCHEMA "{schema}" CASCADE')
