"""Postgres backend: introspection checker and DDL are tested offline with a fake catalog;
the live tests (marker ``postgres_live``) need OPENBARS_TEST_PG_DSN and psycopg."""

import re
import sqlite3
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest
from synth import canonical_bars, raw_batch, weekdays

from openbars.core.models import (
    AssetClass,
    DateRange,
    Instrument,
    Interval,
    Market,
    SymbolMapping,
)
from openbars.storage.errors import (
    ReadOnlyError,
    SchemaMismatchError,
    StorageError,
    UpstreamSchemaError,
)
from openbars.storage.postgres import (
    NATIVE_REQUIRED,
    UPSTREAM_REQUIRED,
    ColumnInfo,
    PgMode,
    PostgresBackend,
    check_native,
    check_schema,
    check_upstream_compat,
    native_ddl,
)
from openbars.storage.schema import REF_IDENTITY_SCHEMAS, STATE_TABLES, TABLE_SCHEMAS

ALL_SCHEMAS = {**TABLE_SCHEMAS, **REF_IDENTITY_SCHEMAS}

_PG_TYPE = {
    "int": "integer",
    "text": "character varying",
    "date": "date",
    "timestamptz": "timestamp with time zone",
    "json": "jsonb",
    "bool": "boolean",
    "array": "ARRAY",
}


def fake_catalog(required):
    catalog = {}
    for table, cols in required.items():
        catalog[table] = {}
        for name, spec in cols.items():
            if spec.family == "numeric":
                catalog[table][name] = ColumnInfo("numeric", spec.precision, spec.scale)
            else:
                catalog[table][name] = ColumnInfo(_PG_TYPE[spec.family])
    return catalog


# --- upstream_compat introspection (offline) ---


def test_upstream_catalog_passes():
    check_upstream_compat(fake_catalog(UPSTREAM_REQUIRED))
    # extra tables / extra columns are fine: upstream owns its schema
    cat = fake_catalog(UPSTREAM_REQUIRED)
    cat["unrelated_table"] = {"x": ColumnInfo("text")}
    cat["daily_bar"]["extra_col"] = ColumnInfo("text")
    check_upstream_compat(cat)


def test_upstream_missing_table_column_and_type_are_structured():
    cat = fake_catalog(UPSTREAM_REQUIRED)
    del cat["source_artifact"]
    del cat["daily_bar"]["vwap"]
    cat["daily_bar"]["open"] = ColumnInfo("numeric", 18, 6)  # wrong precision
    cat["research_daily_bar"]["session_date"] = ColumnInfo("text")  # wrong family
    cat["research_daily_bar"]["capabilities"] = ColumnInfo("text")  # json expected
    with pytest.raises(UpstreamSchemaError) as exc:
        check_upstream_compat(cat)
    problems = {(p.table, p.column, p.kind): p for p in exc.value.problems}
    assert ("source_artifact", None, "missing_table") in problems
    assert ("daily_bar", "vwap", "missing_column") in problems
    mismatch = problems[("daily_bar", "open", "type_mismatch")]
    assert (mismatch.expected, mismatch.actual) == ("numeric(20,8)", "numeric(18,6)")
    assert problems[("research_daily_bar", "session_date", "type_mismatch")].actual == "text"
    assert ("research_daily_bar", "capabilities", "type_mismatch") in problems
    assert len(exc.value.problems) == 5
    assert "daily_bar.vwap" in str(exc.value)


def test_integer_widths_are_all_accepted():
    cat = fake_catalog(UPSTREAM_REQUIRED)
    cat["daily_bar"]["id"] = ColumnInfo("bigint")
    cat["daily_bar"]["revision"] = ColumnInfo("smallint")
    check_upstream_compat(cat)


def test_empty_catalog_reports_every_table():
    problems = check_schema({}, UPSTREAM_REQUIRED)
    assert {p.table for p in problems} == set(UPSTREAM_REQUIRED)
    assert all(p.kind == "missing_table" for p in problems)


# --- compat constructor never runs DDL ---


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.rows[0] if self.rows else None


class FakeConn:
    def __init__(self, catalog, log):
        self.catalog, self.log = catalog, log

    def execute(self, sql, params=()):
        self.log.append(sql)
        if "information_schema.columns" in sql:
            return FakeCursor(
                [
                    {
                        "table_name": t, "column_name": c, "data_type": i.data_type,
                        "numeric_precision": i.numeric_precision, "numeric_scale": i.numeric_scale,
                    }
                    for t, cols in self.catalog.items() for c, i in cols.items()
                ]
            )
        return FakeCursor([])

    def cursor(self):  # pragma: no cover
        raise AssertionError("unexpected write")

    def commit(self):
        self.log.append("COMMIT")

    def rollback(self):
        self.log.append("ROLLBACK")

    def close(self):
        pass


def test_compat_backend_introspects_without_ddl(tmp_path):
    log = []
    backend = PostgresBackend(
        mode="upstream_compat", connect=lambda: FakeConn(fake_catalog(UPSTREAM_REQUIRED), log),
        state_path=tmp_path / "state.db",
    )
    assert backend.mode is PgMode.UPSTREAM_COMPAT
    assert not any(s.upper().startswith(("CREATE", "ALTER", "DROP", "INSERT")) for s in log)
    with pytest.raises(ReadOnlyError):
        backend.canonical.upsert([])
    with pytest.raises(ReadOnlyError):
        backend.raw.append(None)
    assert backend.state.load_checkpoint("x") is None  # local sqlite state


def test_compat_backend_raises_structured_error(tmp_path):
    cat = fake_catalog(UPSTREAM_REQUIRED)
    del cat["research_daily_bar"]["quality_status"]
    with pytest.raises(UpstreamSchemaError) as exc:
        PostgresBackend(mode="upstream_compat", connect=lambda: FakeConn(cat, []), state_path=tmp_path / "s.db")
    assert exc.value.problems[0].column == "quality_status"


def test_compat_write_constructs_but_readonly_store_rejects(tmp_path):
    factory = lambda: FakeConn(fake_catalog(UPSTREAM_REQUIRED), [])  # noqa: E731
    writable = PostgresBackend(
        mode="upstream_compat", write=True, connect=factory, state_path=tmp_path / "s.db"
    )
    assert writable.write is True
    readonly = PostgresBackend(
        mode="upstream_compat", write=False, connect=factory, state_path=tmp_path / "r.db"
    )
    with pytest.raises(ReadOnlyError):
        readonly.raw.append_rows([])
    with pytest.raises(ReadOnlyError):
        readonly.canonical.upsert_rows([])
    with pytest.raises(ReadOnlyError):
        readonly.instruments.upsert([], [])


def test_compat_needs_state_path(tmp_path):
    factory = lambda: FakeConn(fake_catalog(UPSTREAM_REQUIRED), [])  # noqa: E731
    with pytest.raises(StorageError, match="state_path"):
        PostgresBackend(mode="upstream_compat", connect=factory)
    with pytest.raises(StorageError, match="state_path"):
        PostgresBackend(mode="upstream_compat", write=True, connect=factory)


def test_native_readonly_verifies_but_never_creates():
    log = []
    with pytest.raises(SchemaMismatchError) as exc:
        PostgresBackend(mode="native", connect=lambda: FakeConn({}, log))
    assert any(p.kind == "missing_table" for p in exc.value.problems)
    assert not any(s.upper().startswith("CREATE") for s in log)


def test_native_readonly_backend_rejects_writes():
    backend = PostgresBackend(mode="native", connect=lambda: FakeConn(fake_catalog(NATIVE_REQUIRED), []))
    with pytest.raises(ReadOnlyError):
        backend.raw.append_rows([])
    with pytest.raises(ReadOnlyError):
        backend.canonical.upsert_rows([])
    with pytest.raises(ReadOnlyError):
        backend.instruments.upsert([], [])
    with pytest.raises(ReadOnlyError):
        backend.state.save_checkpoint(None)
    with pytest.raises(ReadOnlyError):
        backend.state.start_fetch(provider="p")


# --- native DDL ↔ Parquet schema 1:1 ---


def test_native_ddl_matches_parquet_schemas():
    ddl = {m.group(1): m.group(2) for s in native_ddl() if (m := re.match(r'CREATE TABLE IF NOT EXISTS "(\w+)" \(\n(.*)\n\)$', s, re.S))}
    assert set(REF_IDENTITY_SCHEMAS) == {"unresolved", "symbol_observations", "upstream_ids"}
    for name, schema in ALL_SCHEMAS.items():
        cols = re.findall(r'^\s*"(\w+)" ', ddl[name], re.M)
        assert cols == [f.name for f in schema], name
    assert '"open" NUMERIC(20,8) NOT NULL' in ddl["raw_bars"]
    assert '"volume" NUMERIC(28,8),' in ddl["raw_bars"]
    assert '"flags" TEXT[] NOT NULL' in ddl["canonical_bars"]
    assert '"external_ids" JSONB NOT NULL' in ddl["instruments"]
    assert '"instrument_id" TEXT,' in ddl["raw_bars"]  # nullable
    for t in STATE_TABLES:
        assert re.findall(r'^\s*"(\w+)" ', ddl[t.name], re.M) == [c.name for c in t.columns]
    assert not any("quota_" in s for s in native_ddl())
    idx = [s for s in native_ddl() if "UNIQUE INDEX" in s]
    partial = [s for s in idx if "WHERE" in s]
    assert len(idx) == 6 and len(partial) == 4  # raw + canonical daily/intraday, plus two identity indexes
    # identity tables: keys mirror the Parquet merge keys / UnresolvedRecord.key
    assert 'PRIMARY KEY ("instrument_id", "symbol", "provider")' in ddl["symbol_observations"]
    assert 'PRIMARY KEY ("instrument_id")' in ddl["upstream_ids"]
    assert any('"ux_upstream_ids_upstream_id" ON "upstream_ids" ("upstream_id")' in s for s in idx)
    (unresolved_idx,) = [s for s in idx if '"ux_unresolved_key"' in s]
    assert "kind, provider, market, symbol" in unresolved_idx and "corporate_action" in unresolved_idx
    assert "PRIMARY KEY" not in ddl["unresolved"]  # nullable key parts: expression index instead


def test_state_columns_match_between_sqlite_and_postgres_ddl(tmp_path):
    from openbars.storage.state import SqliteStateStore

    store = SqliteStateStore(tmp_path / "s.db")
    con = sqlite3.connect(store.path)
    for t in STATE_TABLES:
        sqlite_cols = [r[1] for r in con.execute(f"PRAGMA table_info({t.name})")]
        assert sqlite_cols == [c.name for c in t.columns]


def test_native_requirements_cover_every_arrow_column():
    for name, schema in ALL_SCHEMAS.items():
        assert set(NATIVE_REQUIRED[name]) == {f.name for f in schema}
    check_native(fake_catalog(NATIVE_REQUIRED))


def test_compat_row_mapping_helpers():
    from openbars.storage.postgres import compat_canonical_row, compat_raw_row

    d = date(2024, 1, 2)
    rec = dict(
        id=7, instrument_id=42, session_date=d, policy_version="research-daily-v2", provider="prov_a",
        feed="f", adjustment="split", revision=1, open=Decimal("11.25"), high=Decimal("12"),
        low=Decimal("11"), close=Decimal("11.5"), volume=Decimal("1000000.5"),
        selection_reason="primary_source", quality_status="divergent", capabilities=["price_ohlc"],
        source_fingerprint="a" * 64, generated_at=datetime(2024, 1, 3, tzinfo=UTC),
    )
    row = compat_canonical_row(rec)
    assert row.instrument_id == "42" and row.flags == ("DIVERGENT",) and row.open == Decimal("11.25")
    raw = compat_raw_row(
        dict(rec, symbol="ZZZA", price_basis="split_adjusted", adjustment_scope="splits",
             adjustment_as_of=None, session_semantics="regular", currency="USD",
             volume_basis="as_traded", vwap=None, source_artifact_id=3, ingest_run_id=None)
    )
    assert raw.symbol_as_returned == "ZZZA" and raw.fetch_id == "compat:7"
    assert raw.source_artifact_ref == "source_artifact:3" and raw.price_basis == "split_adjusted"


# --- live (opt-in) ---

DAYS = weekdays(date(2024, 1, 2), 5)
WINDOW = DateRange(DAYS[0], DAYS[-1])


@pytest.mark.postgres_live
def test_live_native_roundtrip(pg_dsn):
    import psycopg

    schema = f"openbars_test_{datetime.now(UTC):%H%M%S%f}"
    with psycopg.connect(pg_dsn, autocommit=True) as admin:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    try:
        rw = PostgresBackend(pg_dsn, mode="native", write=True, schema=schema)
        batch = raw_batch(["TEST1", "TEST2"], DAYS)
        assert rw.raw.append(batch) == 10
        assert rw.raw.append(batch) == 10  # upsert: same rows, no duplicates
        rows = rw.raw.scan_rows("prov_a", Market.US, Interval.D1, WINDOW)
        assert len(rows) == 10 and rows[0].open == Decimal("11.25") and rows[0].volume == Decimal("1000000.5")
        parquet_like = batch.bars[0]
        assert rows[0].symbol_as_returned == parquet_like.symbol_as_returned

        assert rw.canonical.upsert(canonical_bars(["us:eq:T1"], DAYS)) == 5
        assert rw.canonical.upsert(canonical_bars(["us:eq:T1"], DAYS, base=40.0)) == 5
        crow = rw.canonical.scan_rows(["us:eq:T1"], Interval.D1, WINDOW)
        assert len(crow) == 5 and crow[0].open == Decimal("40") and crow[0].sources_seen == ("prov_a", "prov_b")

        rw.instruments.upsert(
            [Instrument("us:eq:A", Market.US, AssetClass.EQUITY, "USD", external_ids={"figi": "TESTFIGI1"})],
            [SymbolMapping("us:eq:A", "ZZZA", date(2015, 1, 1), date(2019, 12, 31))],
        )
        assert rw.instruments.resolve("ZZZA", on=date(2019, 12, 31), provider="p", market=Market.US) == "us:eq:A"
        assert rw.instruments.resolve("ZZZA", on=date(2020, 1, 1), provider="p", market=Market.US) is None
        assert rw.instruments.get("us:eq:A").external_ids == {"figi": "TESTFIGI1"}

        from openbars.orchestrator.jobs import Checkpoint

        rw.state.save_checkpoint(Checkpoint("j1", "c1", datetime(2024, 1, 2, tzinfo=UTC)))
        assert rw.state.load_checkpoint("j1").cursor == "c1"
        rw.state.start_fetch(provider="prov_a", fetch_id="f1", market=Market.US, interval=Interval.D1, window=WINDOW)
        rw.state.finish_fetch("prov_a", "f1", "ok", row_count=10)
        assert rw.state.get_fetch("prov_a", "f1").row_count == 10

        ro = PostgresBackend(pg_dsn, mode="native", write=False, schema=schema)
        assert len(ro.raw.scan_rows("prov_a", Market.US, Interval.D1, WINDOW)) == 10
        with pytest.raises(ReadOnlyError):
            ro.raw.append(batch)
    finally:
        with psycopg.connect(pg_dsn, autocommit=True) as admin:
            admin.execute(f'DROP SCHEMA "{schema}" CASCADE')


@pytest.mark.postgres_live
def test_live_native_identity_tables_ingest_and_resolve(pg_dsn):
    from pg_helpers import throwaway_schema

    from openbars.core.models import CorporateAction, CorporateActionKind
    from openbars.normalize.instruments import (
        CORPORATE_ACTION_KIND,
        UnresolvedRecord,
        ingest_corporate_actions,
    )

    a, b = "us:eq:ZZA1", "us:eq:ZZB1"
    with throwaway_schema(pg_dsn) as schema:
        rw = PostgresBackend(pg_dsn, mode="native", write=True, schema=schema)
        PostgresBackend(pg_dsn, mode="native", write=True, schema=schema)  # DDL is idempotent
        m = rw.instruments
        m.upsert(
            [Instrument(a, Market.US, AssetClass.EQUITY, "USD"), Instrument(b, Market.US, AssetClass.EQUITY, "USD")],
            [
                SymbolMapping(a, "ZZZA", date(2020, 1, 1), date(2020, 1, 10)),
                SymbolMapping(b, "ZZZA", date(2020, 1, 15)),
                SymbolMapping(a, "ZZZA", date(2019, 1, 1), date(2019, 12, 31), provider="prov_special"),
            ],
        )
        kw = dict(provider="prov_x", market=Market.US)
        assert m.resolve("ZZZA", on=date(2020, 1, 10), **kw) == a  # valid_to inclusive
        assert m.resolve("ZZZA", on=date(2020, 1, 13), **kw) is None  # gap between holders: reused, no fallback
        assert m.resolve_detailed("ZZZA", on=date(2020, 1, 13), **kw).reason_code == "symbol_reused_uncovered_date"
        assert m.resolve("ZZZA", on=date(2020, 1, 20), **kw) == b
        assert m.resolve("ZZZA", on=date(2019, 6, 1), provider="prov_special", market=Market.US) == a
        assert m.resolve("NOPE", on=date(2020, 1, 1), **kw) is None
        # unique-history fallback now works on Postgres too (before: window-only SQL)
        m.upsert([Instrument("us:eq:U1", Market.US, AssetClass.EQUITY, "USD")],
                 [SymbolMapping("us:eq:U1", "TEST1", date(2020, 1, 1), date(2020, 6, 30))])
        assert m.resolve("TEST1", on=date(2019, 12, 1), **kw) == "us:eq:U1"
        assert m.resolve("TEST1", on=date(2020, 7, 1), **kw) is None
        assert m.resolve_windows("TEST1", start=None, end=None, market=Market.US) == [("us:eq:U1", None, date(2020, 6, 30))]
        gap_windows = m.resolve_windows("ZZZA", start=date(2020, 1, 6), end=date(2020, 1, 20), market=Market.US)
        assert gap_windows == [(a, date(2020, 1, 6), date(2020, 1, 10)), (b, date(2020, 1, 15), date(2020, 1, 20))]
        assert {i.instrument_id for i in m.instruments()} == {a, b, "us:eq:U1"} and len(m.mappings()) == 4
        assert m.resolve_any("ZZZA", on=date(2020, 1, 20)) == b and m.resolve_any(a, on=date(2020, 1, 20)) == a

        # ingest: ticker -> stable id by ex-date, gap unresolved and not written
        third = Decimal("1") / Decimal("3")
        summary = ingest_corporate_actions(
            [
                CorporateAction("ZZZA", date(2020, 1, 8), CorporateActionKind.SPLIT, "prov_x", ratio=Decimal("4")),
                CorporateAction("ZZZA", date(2020, 1, 20), CorporateActionKind.DIVIDEND, "prov_x", amount=Decimal("0.25"), currency="USD"),
                CorporateAction("ZZZA", date(2020, 1, 21), CorporateActionKind.SPLIT, "prov_x", ratio=third),
                CorporateAction("ZZZA", date(2020, 1, 13), CorporateActionKind.SPLIT, "prov_x", ratio=Decimal("2")),
                CorporateAction(a, date(2020, 1, 13), CorporateActionKind.SPLIT, "prov_x", ratio=Decimal("5")),  # id kept
            ],
            m,
        )
        assert (summary.resolved, summary.unresolved, summary.already_mapped) == (4, 1, 1)
        got = m.corporate_actions()
        assert {(x.instrument_id, x.ex_date) for x in got} == {
            (a, date(2020, 1, 8)), (a, date(2020, 1, 13)), (b, date(2020, 1, 20)), (b, date(2020, 1, 21))
        }
        assert all(isinstance(x.ratio, Decimal) or x.ratio is None for x in got)
        assert not any(isinstance(v, float) for x in got for v in (x.ratio, x.amount))
        assert {x.ratio for x in got if x.ex_date == date(2020, 1, 21)} == {Decimal("0.33333333")}  # as in Parquet
        assert [x.amount for x in m.corporate_actions([b]) if x.amount] == [Decimal("0.25")]
        assert [x.currency for x in m.corporate_actions([b]) if x.amount] == ["USD"]
        for _ in range(2):  # re-ingest: the unresolved row does not multiply
            ingest_corporate_actions(
                [CorporateAction("ZZZA", date(2020, 1, 13), CorporateActionKind.SPLIT, "prov_x", ratio=Decimal("2"))], m
            )
        (q,) = m.unresolved(CORPORATE_ACTION_KIND)
        assert (q.symbol, q.session_date, q.detail) == ("ZZZA", date(2020, 1, 13), "split")
        assert m.unresolved_count() == 1
        # keys: listings dedupe per provider+symbol (no date), bars per day, actions per kind
        m.record_unresolved([
            UnresolvedRecord("listing", "no_mapping", "ZZZA", "p", date(2020, 1, 1)),
            UnresolvedRecord("listing", "no_mapping", "ZZZA", "p", date(2020, 1, 2)),
            UnresolvedRecord("bar", "no_mapping", "ZZZA", "p", date(2020, 1, 1)),
            UnresolvedRecord("bar", "no_mapping", "ZZZA", "p", date(2020, 1, 2)),
            UnresolvedRecord("corporate_action", "no_mapping", "ZZZA", "prov_x", date(2020, 1, 13), detail="dividend"),
        ])
        assert (m.unresolved_count("listing"), m.unresolved_count("bar"), m.unresolved_count(CORPORATE_ACTION_KIND)) == (1, 2, 2)
        m.discard_unresolved([UnresolvedRecord("listing", "x", "ZZZA", "p", date(2030, 1, 1))])  # date is not part of the key
        m.discard_unresolved([UnresolvedRecord("bar", "x", "ZZZA", "p", date(2020, 1, 1))])
        assert (m.unresolved_count("listing"), m.unresolved_count("bar")) == (0, 1)

        # sightings keep the latest date; upstream ids are 1:1
        m.observe([(a, "ZZZA", "p", date(2020, 1, 5)), (a, "ZZZA", "p", date(2020, 1, 3)), (b, "ZZZA", "p", date(2020, 2, 1))])
        assert sorted(m.observations()) == [(a, "ZZZA", "p", date(2020, 1, 5)), (b, "ZZZA", "p", date(2020, 2, 1))]
        m.set_upstream_id(a, 101)
        m.set_upstream_id(a, 101)
        assert m.upstream_ids() == {a: 101}
        with pytest.raises(ValueError):
            m.set_upstream_id(a, 102)
        with pytest.raises(ValueError):
            m.set_upstream_id(b, 101)
        assert m.upstream_ids() == {a: 101}

        # remove_mappings: the holder's window disappears and resolution follows
        m.remove_mappings([SymbolMapping(a, "ZZZA", date(2020, 1, 1), date(2020, 1, 10))])
        assert m.resolve("ZZZA", on=date(2020, 1, 20), **kw) == b
        # b is now the symbol's only generic holder, so the unique-history fallback covers the past
        assert m.resolve_detailed("ZZZA", on=date(2020, 1, 8), **kw).via == "unique_history"

        ro = PostgresBackend(pg_dsn, mode="native", write=False, schema=schema).instruments
        assert ro.unresolved_count() == 3 and ro.upstream_ids() == {a: 101} and len(ro.observations()) == 2
        for call in (
            lambda: ro.record_unresolved([UnresolvedRecord("bar", "x", "S", "p", date(2020, 1, 1))]),
            lambda: ro.discard_unresolved([]),
            lambda: ro.observe([]),
            lambda: ro.set_upstream_id(a, 1),
            lambda: ro.remove_mappings([]),
        ):
            with pytest.raises(ReadOnlyError):
                call()


@pytest.mark.postgres_live
def test_live_quota_native_store_reserve_commit_and_penalize(pg_dsn):
    from pg_helpers import throwaway_schema

    from openbars.quota.postgres import PostgresQuotaStore

    now = datetime(2026, 10, 15, 12, tzinfo=UTC)
    with throwaway_schema(pg_dsn) as schema:
        store = PostgresQuotaStore.from_dsn(pg_dsn, mode="native", schema=schema)
        store.ensure_schema()
        store.ensure_schema()  # idempotent
        with store.transaction("prov_a", "default") as tx:
            gate, usage = tx.load(now - timedelta(hours=1))
            assert gate.next_request_at is None and gate.penalized_until is None and list(usage) == []
            tx.record(now, 3, now + timedelta(seconds=2), now - timedelta(hours=1))
        gate, usage = store.read("prov_a", "default", now - timedelta(hours=1))
        assert gate.next_request_at == now + timedelta(seconds=2)
        assert [(u.reserved_at, u.weight) for u in usage] == [(now, 3)]
        with pytest.raises(RuntimeError):  # a rolled-back block leaves nothing behind
            with store.transaction("prov_a", "default") as tx:
                tx.record(now + timedelta(seconds=5), 1, now + timedelta(seconds=9), now - timedelta(hours=1))
                raise RuntimeError("boom")
        _, usage = store.read("prov_a", "default", now - timedelta(hours=1))
        assert [u.weight for u in usage] == [3]
        until = now + timedelta(minutes=5)
        store.penalize("prov_a", until)
        store.penalize("prov_a", now + timedelta(minutes=1))  # shorter penalty never shortens
        gate, _ = store.read("prov_a", "default", now)
        assert gate.penalized_until == until
        assert list(store.scopes("prov_a")) == ["default"]
        # the quota tables live next to, not inside, the storage native schema
        with pytest.raises(StorageError):
            PostgresBackend(pg_dsn, mode="native", write=False, schema=schema)  # storage tables were never created


@pytest.mark.postgres_live
def test_live_upstream_compat_reads_and_introspects(pg_dsn, tmp_path):
    from pathlib import Path

    import psycopg

    ddl = (Path(__file__).parent / "compat" / "upstream_schema.sql").read_text()
    schema = f"openbars_compat_{datetime.now(UTC):%H%M%S%f}"
    with psycopg.connect(pg_dsn, autocommit=True) as admin:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    try:
        with psycopg.connect(pg_dsn, autocommit=True, options=f"-c search_path={schema}") as conn:
            conn.execute(ddl)
            conn.execute("INSERT INTO instrument (id, asset_class, security_type) VALUES (42, 'equity', 'common_stock')")
            conn.execute(
                "INSERT INTO symbol_alias (instrument_id, symbol, exchange, valid_from, valid_from_basis, verification_status) "
                "VALUES (42, 'ZZZA', 'TESTX', '2015-01-01', 'listing', 'verified')"
            )
            conn.execute(
                "INSERT INTO daily_bar (instrument_id, session_date, provider, feed, adjustment, revision, "
                "open, high, low, close, volume) VALUES (42, '2024-01-02', 'prov_a', 'f', 'split', 1, "
                "11.25, 12, 11, 11.5, 1000000.5)"
            )
            conn.execute(
                "INSERT INTO research_daily_bar (instrument_id, session_date, policy_version, provider, feed, "
                "adjustment, revision, open, high, low, close, volume, selection_reason, quality_status, "
                "capabilities, source_fingerprint) VALUES (42, '2024-01-02', 'research-daily-v2', 'prov_a', 'f', "
                "'split', 1, 11.25, 12, 11, 11.5, 1000000.5, 'primary_source', 'complete', '[\"price_ohlc\"]', "
                f"'{'a' * 64}')"
            )
        backend = PostgresBackend(
            pg_dsn, mode="upstream_compat", schema=schema, state_path=tmp_path / "state.db"
        )
        window = DateRange(date(2024, 1, 1), date(2024, 1, 31))
        (raw,) = backend.raw.scan_rows("prov_a", Market.US, Interval.D1, window)
        assert raw.symbol_as_returned == "ZZZA" and raw.open == Decimal("11.25") and raw.instrument_id == "42"
        (canon,) = backend.canonical.scan_rows(["42"], Interval.D1, window)
        assert canon.close == Decimal("11.5") and canon.capabilities == ("price_ohlc",)
        assert backend.canonical.scan_rows(["43"], Interval.D1, window) == []
        assert backend.instruments.resolve("ZZZA", on=date(2024, 1, 2), provider="x", market=Market.US) == "42"

        with psycopg.connect(pg_dsn, autocommit=True, options=f"-c search_path={schema}") as conn:
            conn.execute("ALTER TABLE daily_bar DROP COLUMN vwap")
            conn.execute("ALTER TABLE research_daily_bar ALTER COLUMN open TYPE NUMERIC(10,2)")
        with pytest.raises(UpstreamSchemaError) as exc:
            PostgresBackend(pg_dsn, mode="upstream_compat", schema=schema, state_path=tmp_path / "s2.db")
        kinds = {(p.table, p.column, p.kind) for p in exc.value.problems}
        assert kinds == {
            ("daily_bar", "vwap", "missing_column"),
            ("research_daily_bar", "open", "type_mismatch"),
        }
    finally:
        with psycopg.connect(pg_dsn, autocommit=True) as admin:
            admin.execute(f'DROP SCHEMA "{schema}" CASCADE')
