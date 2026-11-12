"""Reading canonical rows by upstream integer id skips openbars' symbol mapping (#155)."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from openbars.api.queries import QueryError, list_bars, load_canonical
from openbars.core.models import AssetClass, DateRange, Instrument, Interval, Market, Session, SymbolMapping
from openbars.storage.bar_reader import StoreBarReader
from openbars.storage.parquet_duckdb import open_backend
from openbars.storage.rows import CanonicalBarRow, upstream_instrument_ids

D1, D2 = date(2025, 1, 2), date(2025, 1, 3)
WINDOW = DateRange(D1, D2)
AT = datetime(2025, 1, 4, tzinfo=UTC)


def row(iid: str, day: date, close: str, interval: Interval = Interval.D1, hour: int = 0) -> CanonicalBarRow:
    return CanonicalBarRow(
        instrument_id=iid, market=Market.US, interval=interval,
        ts_start=datetime(day.year, day.month, day.day, hour, tzinfo=UTC), session_date=day,
        session=Session.REGULAR, provider="massive", open=Decimal("10"), high=Decimal("40"),
        low=Decimal("5"), close=Decimal(close), volume=Decimal("100"), price_source="massive:eod",
        reconcile_rule="primary_source", feed="eod", adjustment="split", available_at=AT,
    )


def seed(tmp_path, *, mappings: bool):
    backend = open_backend(tmp_path)
    # an equity export: integer ids as strings, no openbars instrument / mapping rows at all
    backend.canonical.upsert_rows([row("17", D1, "11"), row("17", D2, "12"),
                                   row("42", D1, "21"), row("42", D2, "22")])
    if mappings:
        # openbars' own view of a reused ticker ABC, plus a mapping whose *symbol* is "42"
        # (HK codes are digit strings) pointing somewhere else entirely
        old, new = "us:eq:OLDCO", "us:eq:NEWCO"
        backend.instruments.upsert(
            [Instrument(old, Market.US, AssetClass.EQUITY, "USD"), Instrument(new, Market.US, AssetClass.EQUITY, "USD")],
            [SymbolMapping(old, "ABC", date(2020, 1, 1), D1, "*"), SymbolMapping(new, "ABC", D2, None, "*"),
             SymbolMapping(old, "42", date(2020, 1, 1), None, "*")],
        )
        backend.canonical.upsert_rows([row(old, D1, "31"), row(new, D2, "32")])
    return backend


def ids_and_closes(table):
    return sorted((r["instrument_id"], r["session_date"], format(r["close"].normalize(), "f"))
                  for r in table.to_pylist())


def test_upstream_ids_read_back_without_any_mapping(tmp_path):
    backend = seed(tmp_path, mappings=False)
    assert backend.instruments.mappings() == [] and backend.instruments.instruments() == []
    got = ids_and_closes(backend.reader.bars(["42"], id_kind="upstream"))
    assert got == [("42", D1, "21"), ("42", D2, "22")]
    assert ids_and_closes(backend.reader.bars([42], id_kind="upstream", start=D2)) == [("42", D2, "22")]
    rows = load_canonical(backend, ["42"], id_kind="upstream", market=Market.US)
    assert [(r["instrument_id"], r["close"]) for r in rows] == [("42", "21.00000000"), ("42", "22.00000000")]


def test_reused_ticker_upstream_ids_stay_separate_and_ignore_openbars_mappings(tmp_path):
    backend = seed(tmp_path, mappings=True)
    reader = backend.reader
    assert ids_and_closes(reader.bars(["17"], id_kind="upstream")) == [("17", D1, "11"), ("17", D2, "12")]
    assert ids_and_closes(reader.bars(["42"], id_kind="upstream")) == [("42", D1, "21"), ("42", D2, "22")]
    both = ids_and_closes(reader.bars(["17", "42", "42"], id_kind="upstream"))
    assert both == [("17", D1, "11"), ("17", D2, "12"), ("42", D1, "21"), ("42", D2, "22")]
    # the default (auto) reading of "42" is a ticker here: openbars' mapping wins, as before
    assert ids_and_closes(reader.bars(["42"])) == [("us:eq:OLDCO", D1, "31")]
    # ticker reads are unchanged: ABC is OLDCO through D1, NEWCO from D2
    assert ids_and_closes(reader.bars(["ABC"])) == [("us:eq:NEWCO", D2, "32"), ("us:eq:OLDCO", D1, "31")]


def test_upstream_ids_combine_with_available_by_and_hourly(tmp_path):
    backend = seed(tmp_path, mappings=True)
    assert backend.reader.bars(["42"], id_kind="upstream", available_by=datetime(2025, 1, 3, tzinfo=UTC)).num_rows == 0
    assert backend.reader.bars(["42"], id_kind="upstream", available_by=AT).num_rows == 2
    backend.canonical.upsert_rows([row("42", D1, "21", Interval.H1, 15)])
    from openbars.core.models import Adjustment

    rows = load_canonical(backend, ["42"], interval=Interval.H1, start=D1, end=D1, market=Market.US,
                          adjustment=Adjustment.SPLIT, id_kind="upstream")
    assert [(r["instrument_id"], r["ts_start"]) for r in rows] == [("42", "2025-01-02T15:00:00+00:00")]


@pytest.mark.parametrize("bad", ["abc", "42a", "-1", "", " ", "4.2", True, None, "us:eq:X", "４２"])
def test_upstream_ids_must_be_digit_strings(bad):
    with pytest.raises(ValueError):
        upstream_instrument_ids([bad])


def test_bad_id_kind_and_bad_ids_are_errors(tmp_path):
    backend = seed(tmp_path, mappings=False)
    with pytest.raises(ValueError):
        backend.reader.bars(["42"], id_kind="numeric")
    with pytest.raises(ValueError):
        backend.reader.bars(["ABC"], id_kind="upstream")
    with pytest.raises(QueryError):
        load_canonical(backend, ["ABC"], id_kind="upstream")
    with pytest.raises(QueryError):
        load_canonical(backend, ["42"], id_kind="nope")


class _Master:
    """Would rewrite every text if asked (a mapping hit); upstream reads must never ask."""

    def get(self, instrument_id):
        raise AssertionError("instrument master consulted for an upstream id")

    def resolve_any(self, text, *, on=None, market=None):
        raise AssertionError("symbol mapping consulted for an upstream id")

    def corporate_actions(self, ids):
        return []


class _Canonical:
    def __init__(self, rows):
        self.rows = rows

    def scan_rows(self, ids, interval, window, *, market=None, policy_version=None):
        return [r for r in self.rows if r.instrument_id in ids and r.interval is interval]


class _Backend:
    def __init__(self, rows):
        self.instruments = _Master()
        self.canonical = _Canonical(rows)


def test_store_bar_reader_upstream_ids_skip_the_master():
    rows = [row("17", D1, "11"), row("42", D1, "21")]
    reader = StoreBarReader(_Backend(rows))
    got = reader.bars(["42"], id_kind="upstream", market=Market.US, start=D1, end=D2)
    assert [(r.instrument_id, r.close) for r in got] == [("42", Decimal("21"))]
    with pytest.raises(AssertionError):
        reader.bars(["42"], market=Market.US, start=D1, end=D2)  # auto still asks the master


def test_list_bars_upstream_id_needs_no_instrument_and_returns_no_symbol(tmp_path):
    backend = seed(tmp_path, mappings=True)
    out = list_bars(backend, instrument_id="42", market=Market.US, start=D1, end=D2, id_kind="upstream")
    assert out["instrument_id"] == "42" and out["symbol"] is None
    assert [b["close"] for b in out["bars"]] == ["21.00000000", "22.00000000"]
    assert out["available_sources"] == ["canonical"] and out["missing_sessions"] == []
    with pytest.raises(QueryError):
        list_bars(backend, instrument_id="42", start=D1, end=D2, id_kind="upstream")  # market required
    with pytest.raises(QueryError):
        list_bars(backend, symbol="ABC", market=Market.US, id_kind="upstream")
    with pytest.raises(QueryError):
        list_bars(backend, instrument_id="ABC", market=Market.US, id_kind="upstream")


def test_http_bars_upstream_id(tmp_path):
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    from openbars.api.http import create_app

    backend = seed(tmp_path, mappings=True)
    client = TestClient(create_app(backend=backend, data_dir=backend.root))
    res = client.get("/api/v1/bars", params={"id": "42", "market": "us", "id_kind": "upstream",
                                              "start": "2025-01-02", "end": "2025-01-03"})
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["instrument_id"] == "42" and body["symbol"] is None
    assert [b["instrument_id"] for b in body["bars"]] == ["42", "42"]
    for params in ({"id": "42", "id_kind": "upstream"}, {"id": "x1", "market": "us", "id_kind": "upstream"},
                   {"id": "42", "market": "us", "id_kind": "bogus"}):
        assert client.get("/api/v1/bars", params=params).status_code == 400
