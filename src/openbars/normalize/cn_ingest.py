"""First-time registration of A-share instruments (``cn:eq:<venue>:<code>``) in an instrument master.

``ensure_cn_instruments`` is what the CN canonical sink uses to turn the symbols a provider
returned (``sh.600000``) into stable instrument ids. It is deliberately minimal: no listing
ingest, no delisting, no code-reuse handling. A (venue, code) that already resolves to an
instrument is reused as is (whatever its generation); a new one gets generation 1 (no ``:gN``
suffix). A retired id is never handed to a new company here: a re-listing (generation >= 2) is
a separate, explicit operation that this helper does not perform.

Mappings written per instrument (all open-ended from ``listed_on`` or ``as_of``): the
BaoStock-native spelling for provider ``baostock`` (``sh.600000``) and the generic ``"*"``
spellings ``sh.600000`` and ``600000.SH`` (so a ticker query resolves without a provider).
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date

from openbars.core.models import AssetClass, Instrument, Market, SymbolMapping
from openbars.normalize.cn_instruments import cn_instrument_id, normalize_cn_symbol
from openbars.normalize.instruments import InstrumentMaster

__all__ = ["CN_CURRENCY", "ensure_cn_instruments"]

CN_CURRENCY = "CNY"
_EXCHANGE = {"xshg": "XSHG", "xshe": "XSHE"}
_BAOSTOCK_PREFIX = {"xshg": "sh", "xshe": "sz"}
_SUFFIX = {"xshg": "SH", "xshe": "SZ"}


def ensure_cn_instruments(
    master: InstrumentMaster,
    symbols: Iterable[str],
    *,
    as_of: date,
    listed_on: date | None = None,
    provider: str = "baostock",
) -> dict[str, str]:
    """Make sure every CN symbol has an instrument; return ``{symbol: instrument_id}``.

    Symbols may be spelled ``sh.600000`` / ``600000.SH`` / ``SZ000001`` (anything
    ``normalize_cn_symbol`` accepts); an unrecognised spelling raises ``ValueError`` (nothing
    is written). An already resolvable (venue, code) is left untouched; otherwise an
    ``Instrument(market=CN, asset_class=EQUITY, currency=CNY)`` with generation-1 id and the
    mappings described in the module docstring are upserted. ``as_of`` opens the mapping
    window when ``listed_on`` is unknown.
    """
    parsed = [(s, *normalize_cn_symbol(s)) for s in symbols]  # raises before any write
    start = listed_on or as_of
    out: dict[str, str] = {}
    new_instruments: dict[str, Instrument] = {}
    new_mappings: list[SymbolMapping] = []
    for symbol, venue, code in parsed:
        native = f"{_BAOSTOCK_PREFIX[venue]}.{code}"
        existing = master.resolve(native, on=as_of, provider=provider, market=Market.CN)
        if existing is None:
            existing = master.resolve(native, on=as_of, provider="*", market=Market.CN)
        if existing is not None:
            out[symbol] = existing
            continue
        iid = cn_instrument_id(venue, code)
        out[symbol] = iid
        if iid in new_instruments:
            continue
        new_instruments[iid] = Instrument(
            instrument_id=iid,
            market=Market.CN,
            asset_class=AssetClass.EQUITY,
            currency=CN_CURRENCY,
            exchange=_EXCHANGE[venue],
            listed_on=listed_on,
        )
        new_mappings += [
            SymbolMapping(iid, native, start, None, provider),
            SymbolMapping(iid, native, start, None, "*"),
            SymbolMapping(iid, f"{code}.{_SUFFIX[venue]}", start, None, "*"),
        ]
    if new_instruments:
        master.upsert(new_instruments.values(), new_mappings)
    return out
