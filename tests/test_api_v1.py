"""Read-only /api/v1 routes for the local UI. Synthetic fixtures only."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal as D
from pathlib import Path

import pytest
from synth import raw_batch, utc, weekdays

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient

from openbars.api.http import create_app
from openbars.api.queries import MAX_BARS, open_readonly_quota_gate, quota_snapshot
from openbars.core.models import AssetClass, DateRange, Instrument, Interval, Market, SymbolMapping
from openbars.quota.gate import QuotaUsage
from openbars.quota.persistent import SqliteQuotaGate
from openbars.storage.parquet_duckdb import open_backend
from openbars.storage.rows import CanonicalBarRow, raw_rows_from_batch
from openbars.storage.schema import DEFAULT_POLICY_VERSION

pytestmark = pytest.mark.usefixtures("no_network")

T1 = "us:eq:xnys:TEST1"
SYM = "TEST1"
# Weekdays in early January 2020: Thu 2, Fri 3, Mon 6, Tue 7, Wed 8 (XNYS open).
DAYS = weekdays(date(2020, 1, 2), 5)
START, END = DAYS[0], DAYS[-1]
MISSING_RAW = DAYS[2]  # 2020-01-06 is present in canonical, absent from prov_a raw


def _canon(iid, day, close, *, flags=(), volume="1000000"):
    c = D(close)
    return CanonicalBarRow(
        instrument_id=iid,
        market=Market.US,
        interval=Interval.D1,
        ts_start=utc(day),
        session_date=day,
        session="regular",
        provider="prov_a",
        open=c - 1,
        high=c + 1,
        low=c - 1,
        close=c,
        volume=D(volume),
        price_source="prov_a",
        reconcile_rule="primary_wins",
        policy_version=DEFAULT_POLICY_VERSION,
        adjustment="split",
        flags=flags,
        generated_at=datetime(day.year, day.month, day.day, 21, tzinfo=UTC),
    )


def seed_store(root: Path):
    backend = open_backend(root)
    backend.instruments.upsert(
        [Instrument(T1, Market.US, AssetClass.EQUITY, "USD", name="Test One")],
        [SymbolMapping(T1, SYM, date(2019, 1, 1))],
    )
    rows = [
        _canon(T1, DAYS[0], "10.00"),
        _canon(T1, DAYS[1], "10.25", flags=("DIVERGENT",)),
        _canon(T1, DAYS[2], "10.50"),
        _canon(T1, DAYS[3], "10.75", flags=("SPIKE_SUSPECT",)),
        _canon(T1, DAYS[4], "11.00", flags=("VOLUME_MISSING",), volume="0"),
    ]
    backend.canonical.upsert_rows(rows)

    # prov_a: all days except MISSING_RAW; one bar has no volume.
    present = [d for d in DAYS if d != MISSING_RAW]
    batch = raw_batch([SYM], present, provider="prov_a", fetch_id="fetch-a")
    raw_rows = []
    for row in raw_rows_from_batch(batch):
        row = replace(row, instrument_id=T1, market=Market.US)
        if row.session_date == DAYS[4]:
            row = replace(row, volume=None)
        raw_rows.append(row)
    backend.raw.append_rows(raw_rows)

    # prov_b: only the first two days, so later sessions are gaps.
    batch_b = raw_batch([SYM], DAYS[:2], provider="prov_b", fetch_id="fetch-b", base=D("9.00"))
    raw_b = [replace(r, instrument_id=T1, market=Market.US) for r in raw_rows_from_batch(batch_b)]
    backend.raw.append_rows(raw_b)
    return backend


@pytest.fixture
def backend(tmp_path):
    return seed_store(tmp_path / "data")


@pytest.fixture
def client(backend):
    return TestClient(create_app(backend=backend, data_dir=backend.root))


def test_markets_coverage(client):
    response = client.get("/api/v1/markets")
    assert response.status_code == 200
    markets = {row["market"]: row for row in response.json()["markets"]}
    assert "us" in markets
    row = markets["us"]
    assert row["instruments"] == 1
    assert row["bars"] == 5
    assert row["first_session"] == START.isoformat()
    assert row["last_session"] == END.isoformat()
    assert row["flags"]["divergent"] == 1
    assert row["flags"]["spike_suspect"] == 1
    assert row["flags"]["volume_missing"] == 1


def test_instruments_search_and_paging(client):
    response = client.get("/api/v1/instruments", params={"market": "us", "q": "test"})
    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    item = body["items"][0]
    assert item["instrument_id"] == T1
    assert item["symbol"] == SYM
    assert item["name"] == "Test One"
    assert item["last_session"] == END.isoformat()

    empty = client.get("/api/v1/instruments", params={"q": "no-such-ticker"})
    assert empty.status_code == 200
    assert empty.json()["total"] == 0

    page = client.get("/api/v1/instruments", params={"limit": 1, "offset": 1})
    assert page.status_code == 200
    assert page.json()["items"] == []
    assert page.json()["total"] == 1


def test_instruments_bad_limit(client):
    assert client.get("/api/v1/instruments", params={"limit": 0}).status_code == 400
    assert client.get("/api/v1/instruments", params={"offset": -1}).status_code == 400
    assert client.get("/api/v1/instruments", params={"market": "nope"}).status_code == 400


def test_bars_canonical_default(client):
    response = client.get(
        "/api/v1/bars",
        params={"id": T1, "start": START.isoformat(), "end": END.isoformat()},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["source"] == "canonical"
    assert body["instrument_id"] == T1
    assert body["symbol"] == SYM
    assert "canonical" in body["available_sources"]
    assert "prov_a" in body["available_sources"]
    assert "prov_b" in body["available_sources"]
    assert [b["session_date"] for b in body["bars"]] == [d.isoformat() for d in DAYS]
    assert body["missing_sessions"] == []
    first = body["bars"][0]
    assert first["open"] == "9.00000000" or first["open"].startswith("9.")
    assert "." in first["close"]  # decimal string, not a JSON number
    assert isinstance(first["close"], str)
    flagged = next(b for b in body["bars"] if b["session_date"] == DAYS[1].isoformat())
    assert "divergent" in flagged["flags"]


def test_bars_by_symbol(client):
    response = client.get(
        "/api/v1/bars",
        params={"symbol": SYM, "start": START.isoformat(), "end": END.isoformat()},
    )
    assert response.status_code == 200
    assert response.json()["instrument_id"] == T1


def test_bars_raw_source_reports_gaps_and_missing_fields(client):
    response = client.get(
        "/api/v1/bars",
        params={
            "id": T1,
            "source": "prov_a",
            "start": START.isoformat(),
            "end": END.isoformat(),
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["source"] == "prov_a"
    assert body["missing_sessions"] == [MISSING_RAW.isoformat()]
    assert [b["session_date"] for b in body["bars"]] == [
        d.isoformat() for d in DAYS if d != MISSING_RAW
    ]
    last = body["bars"][-1]
    assert last["session_date"] == DAYS[4].isoformat()
    assert last["volume"] is None
    assert "volume" in last["missing_fields"]


def test_bars_raw_other_provider_has_more_gaps(client):
    response = client.get(
        "/api/v1/bars",
        params={
            "id": T1,
            "source": "prov_b",
            "start": START.isoformat(),
            "end": END.isoformat(),
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert len(body["bars"]) == 2
    assert body["missing_sessions"] == [d.isoformat() for d in DAYS[2:]]


def test_bars_unknown_provider_is_all_gaps(client):
    response = client.get(
        "/api/v1/bars",
        params={
            "id": T1,
            "source": "missing_feed",
            "start": START.isoformat(),
            "end": END.isoformat(),
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["bars"] == []
    assert body["missing_sessions"] == [d.isoformat() for d in DAYS]


def test_bars_requires_id_or_symbol(client):
    response = client.get("/api/v1/bars", params={"start": START.isoformat(), "end": END.isoformat()})
    assert response.status_code == 400


def test_bars_rejects_oversized_window(client):
    response = client.get(
        "/api/v1/bars",
        params={"id": T1, "start": "1970-01-01", "end": "2099-12-31"},
    )
    assert response.status_code == 400
    assert str(MAX_BARS) in response.json()["detail"]


def test_bars_rejects_bad_params(client):
    assert client.get("/api/v1/bars", params={"id": T1, "start": "not-a-date"}).status_code == 400
    assert client.get("/api/v1/bars", params={"id": T1, "interval": "1w"}).status_code == 400
    assert client.get("/api/v1/bars", params={"id": T1, "adjustment": "bogus"}).status_code == 400
    assert client.get("/api/v1/bars", params={"id": T1, "source": "  "}).status_code == 400


def test_bars_write_methods_rejected(client):
    for method in ("post", "put", "patch", "delete"):
        assert getattr(client, method)("/api/v1/bars").status_code == 405


def test_flags_counts_and_filter(client):
    response = client.get("/api/v1/flags", params={"market": "us"})
    assert response.status_code == 200
    body = response.json()
    assert body["counts"]["divergent"] == 1
    assert body["counts"]["spike_suspect"] == 1
    assert body["counts"]["volume_missing"] == 1
    assert body["total"] == 3
    names = {item["session_date"]: item["flags"] for item in body["items"]}
    assert "divergent" in names[DAYS[1].isoformat()]

    only = client.get("/api/v1/flags", params={"flag": "divergent"})
    assert only.status_code == 200
    assert only.json()["total"] == 1
    assert only.json()["items"][0]["instrument_id"] == T1

    assert client.get("/api/v1/flags", params={"flag": "not-a-flag"}).status_code == 400


def test_quota_empty_when_no_gate(client):
    response = client.get("/api/v1/quota")
    assert response.status_code == 200
    assert response.json() == {"available": False, "providers": []}


def test_quota_snapshot_redacts_secrets(backend):
    class FakeGate:
        def snapshot(self):
            return {
                "yfinance": QuotaUsage("yfinance", {"day": 3}, {"day": 97}, None),
                "demo": {
                    "provider": "demo",
                    "used": {"day": 1},
                    "remaining": {"day": 9},
                    "api_key": "should-not-appear",
                    "secret_token": "should-not-appear",
                    "password": "should-not-appear",
                },
            }

    client = TestClient(create_app(backend=backend, quota_gate=FakeGate()))
    body = client.get("/api/v1/quota").json()
    assert body["available"] is True
    text = str(body)
    assert "should-not-appear" not in text
    assert "api_key" not in text
    assert "secret_token" not in text
    assert "password" not in text
    providers = {row["provider"]: row for row in body["providers"]}
    assert providers["yfinance"]["used"]["day"] == 3
    assert providers["demo"]["used"]["day"] == 1


def test_quota_from_existing_sqlite(tmp_path, backend):
    path = tmp_path / "quota.db"
    gate = SqliteQuotaGate(path)
    gate.acquire("yfinance", 2, timeout=0)
    gate.close()
    opened = open_readonly_quota_gate(type("S", (), {"postgres_quota": "sqlite"})(), tmp_path)
    assert opened is not None
    snap = quota_snapshot(opened)
    assert snap["available"] is True
    yf = next(row for row in snap["providers"] if row["provider"] == "yfinance")
    assert yf["used"]
    opened.close()

    client = TestClient(create_app(backend=backend, data_dir=tmp_path, quota_gate=opened if False else SqliteQuotaGate(path)))
    body = client.get("/api/v1/quota").json()
    assert body["available"] is True
    assert any(row["provider"] == "yfinance" for row in body["providers"])


def test_open_readonly_quota_gate_does_not_create_file(tmp_path):
    settings = type("S", (), {"postgres_quota": "sqlite"})()
    assert open_readonly_quota_gate(settings, tmp_path) is None
    assert not (tmp_path / "quota.db").exists()


def test_hourly_raw_and_interval_filters(client, backend):
    day = DAYS[0]
    original = list(backend.raw.scan_rows('prov_a', Market.US, Interval.D1, DateRange(day, day)))[0]
    hourly = replace(original, interval=Interval.H1, ts_start=datetime(2020, 1, 2, 14, 30, tzinfo=UTC), extra={'session': 'regular', 'ts_end': '2020-01-02T15:30:00+00:00'})
    backend.raw.append_rows([hourly])
    params = {'id': T1, 'interval': '1h', 'source': 'prov_a', 'start': str(day), 'end': str(day)}
    body = client.get('/api/v1/bars', params=params).json()
    assert len(body['bars']) == 1
    assert body['bars'][0]['session'] == 'regular'
    assert body['bars'][0]['ts_end'] == '2020-01-02T15:30:00+00:00'
    assert len(body['missing_hours']) == 6
    empty = client.get('/api/v1/bars', params={**params, 'source': 'canonical'}).json()
    assert empty['bars'] == [] and empty['note']
    assert client.get('/api/v1/markets', params={'interval': '1h'}).json()['markets'] == []
    assert client.get('/api/v1/flags', params={'interval': '1h'}).json()['total'] == 0
    for route in ('markets', 'flags', 'bars'):
        assert client.get('/api/v1/' + route, params={'interval': 'bad', 'id': T1}).status_code == 400


def test_hourly_canonical_flags_and_coverage(client, backend):
    row = replace(_canon(T1, DAYS[0], "10", flags=("DIVERGENT",)),
                  interval=Interval.H1, ts_start=datetime(2020, 1, 2, 14, 30, tzinfo=UTC))
    backend.canonical.upsert_rows([row])
    params = {"interval": "1h", "id": T1, "start": str(DAYS[0]), "end": str(DAYS[0])}
    bars = client.get("/api/v1/bars", params=params).json()
    assert len(bars["bars"]) == 1
    assert bars["bars"][0]["flags"] == ["divergent"]
    assert bars["bars"][0]["session"] == "regular"
    assert client.get("/api/v1/markets", params=params).json()["markets"][0]["bars"] == 1
    flags = client.get("/api/v1/flags", params=params).json()
    assert flags["total"] == 1 and flags["items"][0]["ts_start"]


def test_hourly_clock_grid_and_extended_sessions(client, backend):
    day = DAYS[0]
    original = list(backend.raw.scan_rows('prov_a', Market.US, Interval.D1, DateRange(day, day)))[0]
    rows = [replace(original, interval=Interval.H1, ts_start=datetime(2020, 1, 2, hour, minute, tzinfo=UTC), extra={'session': session})
            for hour, minute, session in [(9, 0, 'pre'), (14, 30, 'regular'), (15, 0, 'regular')]]
    backend.raw.append_rows(rows)
    body = client.get('/api/v1/bars', params={'id': T1, 'interval': '1h', 'source': 'prov_a', 'start': str(day), 'end': str(day)}).json()
    missing = body['missing_hours']
    assert any(x['session'] == 'pre' for x in missing)
    assert any(x['session'] == 'post' for x in missing)
    assert any(x['ts_start'] == '2020-01-02T16:00:00+00:00' for x in missing)
    assert not any(x['ts_start'] == '2020-01-02T15:30:00+00:00' for x in missing)


def test_hourly_canonical_policy_and_adjustment(client, backend):
    row = replace(_canon(T1, DAYS[0], '10', flags=('DIVERGENT',)), interval=Interval.H1,
                  ts_start=datetime(2020, 1, 2, 14, 30, tzinfo=UTC))
    backend.canonical.upsert_rows([row, replace(row, policy_version='obsolete', flags=('SPIKE_SUSPECT',))])
    params = {'id': T1, 'interval': '1h', 'start': str(DAYS[0]), 'end': str(DAYS[0])}
    assert len(client.get('/api/v1/bars', params=params).json()['bars']) == 1
    assert client.get('/api/v1/markets', params=params).json()['markets'][0]['bars'] == 1
    assert client.get('/api/v1/flags', params=params).json()['total'] == 1
    assert client.get('/api/v1/bars', params={**params, 'adjustment': 'raw'}).status_code == 400
    assert client.get('/api/v1/bars', params={**params, 'as_of': str(DAYS[0])}).status_code == 400


def test_hourly_canonical_stored_adjustment_is_not_mislabeled(client, backend):
    row = replace(_canon(T1, DAYS[0], '10'), interval=Interval.H1, adjustment='raw',
                  ts_start=datetime(2020, 1, 2, 14, 30, tzinfo=UTC))
    backend.canonical.upsert_rows([row])
    params = {'id': T1, 'interval': '1h', 'start': str(DAYS[0]), 'end': str(DAYS[0])}
    assert client.get('/api/v1/bars', params=params).status_code == 400
    body = client.get('/api/v1/bars', params={**params, 'adjustment': 'raw'}).json()
    assert body['adjustment'] == body['bars'][0]['adjustment'] == 'raw'
