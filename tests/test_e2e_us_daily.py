"""End-to-end US daily path with synthetic providers: plan -> run -> daily increment -> verify.

No network, no real tickers: 100 fake symbols (90 equities TESTE00-TESTE89, 10 ETFs
TESTF00-TESTF09), three fake providers, a short XNYS window, Parquet + SQLite under tmp_path.

The planner routes every session to the cheapest source (here the whole-market provider), so
cross-source overlap -- what ``verify`` compares -- comes from explicit audit jobs for the
second and third provider, submitted into the same job queue.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest
from fakes import FakeProvider

from openbars.cli.main import main
from openbars.core.models import (
    AssetClass,
    DateRange,
    Instrument,
    Interval,
    Market,
    RawBar,
    SymbolMapping,
)
from openbars.orchestrator.jobs import Job, JobKind, JobState, make_job_id
from openbars.providers.base import BatchShape
from openbars.storage.parquet_duckdb import open_backend
from openbars.storage.state import SqliteStateStore

pytestmark = pytest.mark.usefixtures("no_network")

EQUITIES = [f"TESTE{i:02d}" for i in range(90)]
ETFS = [f"TESTF{i:02d}" for i in range(10)]
SYMBOLS = EQUITIES + ETFS

NOW1 = datetime(2026, 3, 10, 15, 0, tzinfo=UTC)  # latest ready session: Mon 2026-03-09
NOW2 = datetime(2026, 3, 11, 15, 0, tzinfo=UTC)  # next day: Tue 2026-03-10 becomes ready
SESSIONS1 = [date(2026, 3, d) for d in (2, 3, 4, 5, 6, 9)]
KEY_ID = "test-key-" + "id-xx"
SECRET_KEY = "test-secret-" + "key-xx"
MASSIVE_KEY = "test-massive-" + "key-xx"

# Intentional differences between the whole-market source and the batched source.
PRICE_WITHIN = {"TESTE05": Decimal("1.002"), "TESTE06": Decimal("1.002")}  # 0.2% < 0.5%
PRICE_DIVERGENT = {"TESTE10": Decimal("1.02"), "TESTE11": Decimal("1.02")}  # 2% > 0.5%
VOLUME_WITHIN = {"TESTF03": Decimal("1.03")}  # 3% < 5%
VOLUME_DIVERGENT = {"TESTF04": Decimal("1.20")}  # 17% > 5%
MASSIVE_GAPS = {
    ("TESTE20", date(2026, 3, 4)),
    ("TESTE21", date(2026, 3, 4)),
    ("TESTF05", date(2026, 3, 5)),
}  # the whole-market source misses these cells; the batched source has them
YF_SYMBOLS = ["TESTE30", "TESTE31", "TESTE32", "TESTE33", "TESTE34"]
YEAR = DateRange(date(2026, 1, 1), date(2026, 12, 31))


class SyntheticProvider(FakeProvider):
    """Deterministic Decimal OHLCV per (symbol, day); optional per-symbol skews and gaps."""

    def __init__(self, name, *, price_skew=None, volume_skew=None, skip=frozenset(), **kw):
        super().__init__(name, **kw)
        self.price_skew = price_skew or {}
        self.volume_skew = volume_skew or {}
        self.skip = set(skip)

    def bar(self, symbol: str, day: date, fetch_id: str) -> RawBar | None:
        if (symbol, day) in self.skip:
            return None
        index = int(symbol[5:]) + (100 if symbol.startswith("TESTF") else 0)
        base = Decimal(20 + index) + Decimal(day.day % 5) / 4
        k = self.price_skew.get(symbol, Decimal(1))
        q = Decimal("0.0001")
        volume = Decimal(1_000_000 + index * 1000 + day.day * 10) * self.volume_skew.get(symbol, Decimal(1))
        return RawBar(
            provider=self.id,
            symbol_as_returned=symbol,
            fetch_id=fetch_id,
            interval=Interval.D1,
            ts_start=datetime(day.year, day.month, day.day, tzinfo=UTC),
            open=(base * k).quantize(q),
            high=((base + 1) * k).quantize(q),
            low=((base - 1) * k).quantize(q),
            close=((base + Decimal("0.5")) * k).quantize(q),
            volume=volume.quantize(Decimal(1)),
            feed=self.feed,
            extra={"session_date": day, "adjustment": "split"},
            session_date=day,
        )


def make_providers():
    massive = SyntheticProvider(
        "massive", batch=BatchShape.WHOLE_MARKET_PER_DAY, rolling_window=timedelta(days=730),
        max_symbols=10**6, whole_market_symbols=SYMBOLS, feed="eod", skip=MASSIVE_GAPS,
    )  # fmt: skip
    alpaca = SyntheticProvider(
        "alpaca", batch=BatchShape.MULTI_SYMBOL, max_symbols=50, feed="sip",
        price_skew={**PRICE_WITHIN, **PRICE_DIVERGENT}, volume_skew={**VOLUME_WITHIN, **VOLUME_DIVERGENT},
    )  # fmt: skip
    yahoo = SyntheticProvider("yfinance", batch=BatchShape.MULTI_SYMBOL, max_symbols=50, feed="yahoo")
    return {p.id: p for p in (massive, alpaca, yahoo)}


class World:
    def __init__(self, tmp_path) -> None:
        self.providers = make_providers()
        self.data = tmp_path / "data"
        self.config = tmp_path / "config.yaml"
        self.config.write_text(
            f"""
version: 1
markets: [us]
intervals: [1d]
history: {{start: 2026-03-02, end: null}}
universe:
  us:
    symbols: [{", ".join(SYMBOLS)}]
storage: {{backend: parquet_duckdb, path: {self.data}}}
providers:
  massive: {{enabled: true, api_key: "${{TEST_MASSIVE_KEY}}"}}
  alpaca: {{enabled: true, key_id: "${{TEST_KEY_ID}}", secret_key: "${{TEST_SECRET_KEY}}"}}
  yfinance: {{enabled: true}}
"""
        )
        self.env = {"TEST_MASSIVE_KEY": MASSIVE_KEY, "TEST_KEY_ID": KEY_ID, "TEST_SECRET_KEY": SECRET_KEY}
        self.now = NOW1

    def cli(self, *argv: str) -> int:
        return main(
            ["--config", str(self.config), *argv],
            provider_factory=lambda settings, gate: self.providers,
            clock=lambda: self.now,
            env=self.env,
        )

    def requests(self) -> dict[str, int]:
        return {n: len(p.requests) for n, p in self.providers.items()}

    def raw_rows(self) -> list[dict]:
        return open_backend(self.data).raw.scan_table(None, Market.US, Interval.D1, YEAR).to_pylist()

    def canonical_rows(self) -> list[dict]:
        table = open_backend(self.data).canonical.scan_table(None, Interval.D1, YEAR, policy_version=None)
        return table.to_pylist()

    def parquet_files(self) -> list[str]:
        return sorted(str(p) for p in self.data.rglob("*.parquet"))


@pytest.fixture
def world(tmp_path) -> World:
    return World(tmp_path)


def register_instruments(data) -> None:
    instruments, mappings = [], []
    for symbol in SYMBOLS:
        iid = f"us:test:{symbol}"
        kind = AssetClass.ETF if symbol.startswith("TESTF") else AssetClass.EQUITY
        instruments.append(
            Instrument(iid, Market.US, kind, "USD", name=f"Synthetic {symbol}", exchange="XNYS", listed_on=date(2020, 1, 1))
        )
        mappings.append(SymbolMapping(iid, symbol, date(2020, 1, 1)))
    open_backend(data).instruments.upsert(instruments, mappings)


def audit_jobs(provider: str, symbols: list[str], window: DateRange, size: int = 50) -> list[Job]:
    jobs = []
    for i in range(0, len(symbols), size):
        chunk = tuple(symbols[i : i + size])
        job_id = make_job_id(JobKind.VERIFY, provider, Market.US, Interval.D1, chunk, window)
        jobs.append(Job(job_id, JobKind.VERIFY, provider, Market.US, Interval.D1, chunk, window, priority=50))
    return jobs


def raw_keys(rows: list[dict]) -> list[tuple]:
    return [(r["provider"], r["feed"], r["adjustment"], r["symbol_as_returned"], r["session_date"]) for r in rows]


def test_us_daily_end_to_end(world, capsys):
    register_instruments(world.data)
    assert len(SYMBOLS) == 100 and len(set(SYMBOLS)) == 100

    # --- plan: a dry run writes nothing; the whole-market source takes every session --------
    assert world.cli("plan") == 0
    out = capsys.readouterr().out
    assert "plan: 6 jobs (6 new" in out and "massive" in out and "dry run" in out
    assert not (world.data / "state.db").exists() or SqliteStateStore(world.data / "state.db").list_jobs() == []

    # --- plan --submit + audit samples of the other providers, then run to idle -------------
    assert world.cli("plan", "--submit") == 0
    state = SqliteStateStore(world.data / "state.db")
    window = DateRange(SESSIONS1[0], SESSIONS1[-1])
    extra = audit_jobs("alpaca", SYMBOLS, window) + audit_jobs("yfinance", YF_SYMBOLS, window)
    assert state.submit_jobs(extra) == 3
    capsys.readouterr()
    assert world.cli("run") == 0
    out = capsys.readouterr().out
    assert "done 9" in out and "dead 0" in out and "failed 0" in out
    assert state.count_jobs()[JobState.DONE] == 9
    assert world.requests() == {"massive": 6, "alpaca": 2, "yfinance": 1}

    # raw: one row per (source, symbol, session); canonical: one row per (symbol, session)
    raw = world.raw_rows()
    keys = raw_keys(raw)
    assert len(keys) == len(set(keys))
    by_provider = {p: sum(1 for r in raw if r["provider"] == p) for p in ("massive", "alpaca", "yfinance")}
    assert by_provider == {"massive": 600 - len(MASSIVE_GAPS), "alpaca": 600, "yfinance": 30}
    canon = world.canonical_rows()
    ckeys = [(r["instrument_id"], r["session_date"], r["policy_version"]) for r in canon]
    assert len(ckeys) == len(set(ckeys)) == 600  # no missing expected session in the fake window
    assert {r["session_date"] for r in canon} == set(SESSIONS1)

    # --- re-running a finished backfill is a no-op ------------------------------------------
    files, requests = world.parquet_files(), world.requests()
    assert world.cli("run") == 0
    out = capsys.readouterr().out
    assert "plan: 0 jobs" in out and "claimed 0" in out
    assert world.requests() == requests and world.parquet_files() == files

    # --- daily increment: the next session becomes ready ------------------------------------
    world.now = NOW2
    assert world.cli("plan") == 0
    out = capsys.readouterr().out
    assert "plan: 1 jobs (1 new" in out and "daily" in out
    assert world.cli("run") == 0
    assert "done 1" in capsys.readouterr().out
    assert world.requests() == {**requests, "massive": requests["massive"] + 1}
    keys = raw_keys(world.raw_rows())
    assert len(keys) == len(set(keys)) == 600 - len(MASSIVE_GAPS) + 600 + 30 + 100
    canon = world.canonical_rows()
    assert len({(r["instrument_id"], r["session_date"]) for r in canon}) == len(canon) == 700
    files, requests = world.parquet_files(), world.requests()
    assert world.cli("run") == 0
    assert "plan: 0 jobs" in capsys.readouterr().out
    assert world.requests() == requests and world.parquet_files() == files

    # --- status -----------------------------------------------------------------------------
    assert world.cli("status") == 0
    assert "done=10" in capsys.readouterr().out

    # --- verify -----------------------------------------------------------------------------
    assert world.cli("verify", "--recent", "3", "--json", "reports/verify.json") == 0
    text = capsys.readouterr().out
    for secret in (KEY_ID, SECRET_KEY, MASSIVE_KEY):
        assert secret not in text
    assert "universe 100" in text and "expected sessions 7" in text and "n/a" in text
    report = json.loads((world.data / "reports" / "verify.json").read_text())
    assert report["universe_size"] == 100 and report["expected_sessions"] == 7
    assert report["window"] == {"start": "2026-03-02", "end": "2026-03-10"}

    whole, recent = report["periods"]
    cov = whole["coverage"]
    assert cov["expected_cells"] == 700 and cov["canonical_cells"] == 700 and cov["canonical_rate"] == 1.0
    assert cov["missing_cells"] == 0
    assert cov["by_source"]["massive/eod/split"]["cells"] == 700 - len(MASSIVE_GAPS)  # raw gaps stay visible
    assert cov["by_source"]["alpaca/sip/split"]["cells"] == 600
    assert cov["by_source"]["yfinance/yahoo/split"]["cells"] == 30
    assert 0 < cov["by_source"]["massive/eod/split"]["rate"] < 1

    sources = {(s["provider"], s["feed"], s["adjustment"]): s for s in report["sources"]}
    assert sources[("massive", "eod", "split")] == {
        "provider": "massive", "feed": "eod", "adjustment": "split", "bars": 697,
        "symbols": 100, "oldest": "2026-03-02", "newest": "2026-03-10",
    }  # fmt: skip
    assert sources[("alpaca", "sip", "split")]["bars"] == 600
    assert sources[("yfinance", "yahoo", "split")]["symbols"] == 5

    # massive vs alpaca: 597 comparable pairs; 12 within tolerance, 12 divergent (2 symbols x 6)
    price = whole["price"]
    assert price["divergent"] == 12 and price["within_tolerance"] == 12
    assert price["compared"] == 597 and price["match"] == 597 - 24
    assert price["rate"] == pytest.approx(585 / 597) and 0 < price["rate"] < 1
    assert whole["close"]["divergent"] == 12
    # volume: TESTF04 diverges (6 sessions), TESTF03 is within tolerance
    volume = whole["volume"]
    assert volume["divergent"] == 6 and volume["within_tolerance"] == 6
    assert 0 < volume["rate"] < 1
    # yfinance is never scored against the others (provider-defined price basis)
    assert price["not_comparable"] == 5 * 6 * 2 == whole["counts"]["not_comparable"]
    assert whole["counts"]["single_source_cells"] == 103 and whole["counts"]["multi_source_cells"] == 597
    pair = next(p for p in whole["pairs"] if p["left"] == "alpaca/sip/split" and p["right"] == "massive/eod/split")
    assert pair["price"]["compared"] == 597 and pair["price"]["not_comparable"] == 0
    # one canonical projection only -> quality agreement is n/a; the distribution is still shown
    assert whole["quality"]["rate"] is None and sum(whole["quality"]["distribution"].values()) == 700
    assert "divergent" in whole["quality"]["distribution"]

    # last 3 sessions (Mar 6, 9, 10): audit samples cover Mar 6 and 9 only
    assert recent["label"] == "last 3 sessions" and recent["coverage"]["expected_cells"] == 300
    assert recent["price"]["divergent"] == 4 and recent["counts"]["missing"] == 0


def test_verify_before_any_run_is_a_clean_message(world, capsys):
    assert world.cli("verify") == 1  # data dir does not exist yet
    err = capsys.readouterr().err
    assert "no data directory" in err and "Traceback" not in err
    assert not world.data.exists()
    world.data.mkdir()
    assert world.cli("verify") == 1  # exists but empty
    assert "no stored bars" in capsys.readouterr().err


def test_verify_partial_store_and_json_path_guard(world, capsys, tmp_path):
    assert world.cli("run", "--max-jobs", "2") == 0
    capsys.readouterr()
    assert world.cli("verify", "--json", "../escape.json") == 2
    assert "inside the data directory" in capsys.readouterr().err
    assert not (tmp_path / "escape.json").exists()
    # a partial backfill is a report, not an error: missing sessions show up as missing cells
    assert world.cli("verify") == 0
    out = capsys.readouterr().out
    assert "expected sessions 6" in out and "missing" in out
    assert world.cli("run") == 0
    capsys.readouterr()
    assert world.cli("verify", "--start", "2026-03-06", "--end", "2026-03-06") == 0
    assert "expected sessions 1" in capsys.readouterr().out
    with pytest.raises(SystemExit):
        world.cli("verify", "--start", "not-a-date")
