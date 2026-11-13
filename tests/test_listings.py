"""Listing parsers and sources: synthetic fixtures + httpx.MockTransport only (no network)."""

import json
from datetime import date
from pathlib import Path

import httpx
import pytest

from openbars import __version__
from openbars.core.errors import ProviderError
from openbars.core.models import AssetClass, Market
from openbars.normalize.directory import InstrumentDirectory, ListingRecord
from openbars.normalize.instruments import MemoryInstrumentMaster
from openbars.normalize.listings import (
    NasdaqTraderListingSource,
    parse_nasdaq_listed,
    parse_other_listed,
)
from openbars.providers.us.alpaca import AlpacaProvider, parse_assets

FIX = Path(__file__).parent / "fixtures" / "listings"


def text(name):
    return (FIX / name).read_text()


def test_parse_nasdaq_listed_and_other_listed():
    rows = parse_nasdaq_listed(text("nasdaqlisted.txt"))
    assert [(r.symbol, r.exchange, r.asset_class) for r in rows] == [
        ("ZZZA", "XNAS", None), ("ZZZB", "XNAS", AssetClass.ETF),
    ]  # test issue + footer dropped
    assert rows[0].name.startswith("Synthetic Alpha") and rows[0].provider_instrument_id is None
    other = parse_other_listed(text("otherlisted.txt"))
    assert [(r.symbol, r.exchange, r.asset_class) for r in other] == [
        ("TEST1", "XNYS", None), ("ZZZC", "ARCX", AssetClass.ETF), ("ZZZD", "UNKNOWN", None),
    ]
    with pytest.raises(ValueError):
        parse_nasdaq_listed("Foo|Bar\n1|2\n")
    with pytest.raises(ValueError):
        parse_other_listed("")


def make_source(handler, **kw):
    return NasdaqTraderListingSource(
        client=httpx.Client(transport=httpx.MockTransport(handler)), wait=lambda s: None, **kw
    )


def serve(request: httpx.Request) -> httpx.Response:
    name = "nasdaqlisted.txt" if request.url.path.endswith("nasdaqlisted.txt") else "otherlisted.txt"
    return httpx.Response(200, text=text(name))


def test_nasdaq_source_user_agent_is_configurable_and_neutral():
    seen = []

    def handler(request):
        seen.append(request.headers["user-agent"])
        return serve(request)

    rows = make_source(handler).load()
    assert len(rows) == 5 and seen == [f"openbars/{__version__}"] * 2
    seen.clear()
    make_source(handler, user_agent="my-agent/1.0 (ops@example.com)").load()
    assert seen == ["my-agent/1.0 (ops@example.com)"] * 2


def test_nasdaq_source_errors():
    with pytest.raises(ProviderError) as e:
        make_source(lambda r: httpx.Response(503, text="down")).load()
    assert e.value.retryable

    def boom(request):
        raise httpx.ConnectError("no route")

    with pytest.raises(ProviderError) as e:
        make_source(boom).load()
    assert e.value.code == "transport_error"
    with pytest.raises(ProviderError) as e:
        make_source(lambda r: httpx.Response(200, text="garbage")).load()
    assert e.value.code == "invalid_payload"


def test_nasdaq_rows_only_confirm_existing_aliases():
    store = MemoryInstrumentMaster()
    d = InstrumentDirectory(store)
    d.ingest(
        [ListingRecord("prov_a", "ZZZA", exchange="XNAS", ticker_type="CS", provider_instrument_id="n1"),
         ListingRecord("prov_a", "ZZZB", exchange="XNAS", ticker_type="ETF", provider_instrument_id="n2")],
        as_of=date(2024, 1, 2),
    )
    s = d.ingest(make_source(serve).load(), as_of=date(2024, 1, 5))
    assert s.counts["confirmed"] == 2
    assert len(store.instruments()) == 2  # TEST1/ZZZC/ZZZD are not created from symbol-only rows
    assert {r.symbol for r in s.unresolved} == {"TEST1", "ZZZC", "ZZZD"}
    assert {r.reason_code for r in s.unresolved} == {"symbol_only_evidence"}
    assert store.unresolved_count("listing") == 3


# --- Alpaca assets (mock transport) -----------------------------------------------------------------


def alpaca(handler):
    return AlpacaProvider(
        api_key="test-key-" + "id-xx", api_secret="test-sec" + "ret-xx", env={},
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )


def test_alpaca_list_instruments_parses_assets():
    calls = []

    def handler(request):
        calls.append(request)
        assert request.url.host == "api.alpaca.markets" and request.url.path == "/v2/assets"
        assert request.url.params["status"] == "active"
        return httpx.Response(200, text=text("alpaca_assets.json"))

    items = list(alpaca(handler).list_instruments(Market.US))
    assert [(i.symbol, i.exchange, i.active, i.asset_class) for i in items] == [
        ("ZZZA", "XNAS", True, None), ("ZZZB", "ARCX", True, None), ("TEST1", None, False, None),
    ]
    assert items[0].native_id == "00000000-0000-4000-8000-000000000001"
    assert len(calls) == 1
    assert list(alpaca(handler).list_instruments(Market.HK)) == [] and len(calls) == 1


def test_alpaca_assets_errors_and_adapter_into_directory():
    with pytest.raises(ProviderError):
        parse_assets({"not": "a list"})
    with pytest.raises(ProviderError):
        parse_assets([{"id": "x"}])
    with pytest.raises(ProviderError):
        alpaca(lambda r: httpx.Response(200, text="not json")).list_instruments(Market.US)
    items = parse_assets(json.loads(text("alpaca_assets.json")))
    records = [ListingRecord.from_provider_instrument(i, "alpaca") for i in items]
    store = MemoryInstrumentMaster()
    d = InstrumentDirectory(store)
    s = d.ingest(records, as_of=date(2024, 1, 5))
    # the assets API gives no security type, so it can never mint an instrument by itself
    assert not store.instruments() and {r.reason_code for r in s.unresolved} == {"unclassified_security_type"}
    d.ingest([ListingRecord("prov_a", "ZZZA", exchange="XNAS", ticker_type="CS",
                            provider_instrument_id="n1", share_class_figi="TESTFIGI1")], as_of=date(2024, 1, 5))
    # same symbol, different provider-native id and no FIGI: cannot match, cannot claim the open alias
    s = d.ingest(records[:1], as_of=date(2024, 1, 6))
    assert len(store.instruments()) == 1
