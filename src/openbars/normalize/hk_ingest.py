"""First-time registration of Hong Kong instruments (``hk:eq:xhkg:<code>``).

``ensure_hk_instruments`` is what the HK canonical sink uses to turn provider
spellings (``00700``, ``0700.HK``) into stable instrument ids. It is deliberately
minimal: no listing ingest, no delisting, no code-reuse handling. A (venue, code)
that already resolves is reused as is; a new one gets generation 1. A re-listing
(generation >= 2) is a separate, explicit operation.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date

from openbars.core.models import AssetClass, Instrument, Market, SymbolMapping
from openbars.normalize.hk_instruments import (
    hk_instrument_id,
    normalize_hk_symbol,
    to_yahoo_hk_ticker,
)
from openbars.normalize.instruments import InstrumentMaster

__all__ = ["HK_CURRENCY", "ensure_hk_instruments"]

HK_CURRENCY = "HKD"
_EXCHANGE = {"xhkg": "XHKG"}


def ensure_hk_instruments(
    master: InstrumentMaster,
    symbols: Iterable[str],
    *,
    as_of: date,
    listed_on: date | None = None,
    provider: str = "yfinance",
) -> dict[str, str]:
    """Make sure every HK symbol has an instrument; return ``{symbol: instrument_id}``.

    Unrecognised spellings raise ``ValueError`` before any write.
    """
    parsed = [(s, *normalize_hk_symbol(s)) for s in symbols]
    start = listed_on or as_of
    out: dict[str, str] = {}
    new_instruments: dict[str, Instrument] = {}
    new_mappings: list[SymbolMapping] = []
    for symbol, venue, code in parsed:
        yahoo = to_yahoo_hk_ticker(symbol)
        existing = master.resolve(code, on=as_of, provider=provider, market=Market.HK)
        if existing is None:
            existing = master.resolve(code, on=as_of, provider="*", market=Market.HK)
        if existing is None:
            existing = master.resolve(yahoo, on=as_of, provider="*", market=Market.HK)
        if existing is not None:
            out[symbol] = existing
            continue
        iid = hk_instrument_id(venue, code)
        out[symbol] = iid
        if iid in new_instruments:
            continue
        new_instruments[iid] = Instrument(
            instrument_id=iid,
            market=Market.HK,
            asset_class=AssetClass.EQUITY,
            currency=HK_CURRENCY,
            exchange=_EXCHANGE[venue],
            listed_on=listed_on,
        )
        new_mappings += [
            SymbolMapping(iid, code, start, None, provider),
            SymbolMapping(iid, code, start, None, "*"),
            SymbolMapping(iid, yahoo, start, None, provider),
            SymbolMapping(iid, yahoo, start, None, "*"),
        ]
    if new_instruments:
        master.upsert(new_instruments.values(), new_mappings)
    return out
