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

``adjust_factor_fetch_window`` / ``ingest_job_adjust_factors`` decide *which* BaoStock adjust
factors to fetch for a job and store them (#93): full history the first time, afterwards only an
incremental window. See their docstrings.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import date
from decimal import Decimal
from typing import Any

from openbars.core.models import AssetClass, DateRange, Instrument, Interval, Market, RawBar, SymbolMapping
from openbars.normalize.cn_adjust import AdjustFactor, from_baostock
from openbars.normalize.cn_instruments import cn_instrument_id, normalize_cn_symbol
from openbars.normalize.instruments import InstrumentMaster
from openbars.providers.base import FetchRequest

__all__ = [
    "CN_CURRENCY",
    "FACTOR_FULL_HISTORY_START",
    "adjust_factor_fetch_window",
    "ensure_cn_instruments",
    "ingest_job_adjust_factors",
]

# BaoStock's own default factor start is 2015-01-01 and its factors are cumulative from listing,
# so a full history starts at the listing date, or at this date (before any A-share listing)
# when that is unknown.
FACTOR_FULL_HISTORY_START = date(1990, 1, 1)

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


def adjust_factor_fetch_window(
    existing: Sequence[AdjustFactor],
    job_window: DateRange,
    *,
    listed_on: date | None = None,
    factor_start: date | None = None,
) -> DateRange:
    """The window to ask ``fetch_adjust_factors`` for: first time full history, then incremental.

    * ``factor_start`` set: the start is ``factor_start`` (an explicit override always wins).
    * no ``existing`` factors: the start is ``listed_on`` or ``1990-01-01`` (BaoStock factors are
      cumulative from listing; a late start silently misses earlier ex-dates).
    * otherwise: the start is the latest stored ``effective_from``. It is inclusive, so the last
      event is refreshed, and every ex-date after it is picked up.

    The end is always ``job_window.end``; the start is clamped to it.
    """
    if factor_start is not None:
        start = factor_start
    elif not existing:
        start = listed_on or FACTOR_FULL_HISTORY_START
    else:
        start = max(f.effective_from for f in existing)
    return DateRange(min(start, job_window.end), job_window.end)


def _stored_factors(factor_store: Any, instrument_id: str, source: str) -> list[AdjustFactor]:
    get = getattr(factor_store, "adjust_factors", None)
    if get is None:
        return []
    try:
        return list(get([instrument_id], source=source))
    except TypeError:  # a store without the ``source`` filter: all rows of the instrument
        return [f for f in get([instrument_id]) if f.source == source]


def ingest_job_adjust_factors(
    fetch: Callable[[FetchRequest], Iterable[Any]],
    factor_store: Any,
    job_window: DateRange,
    by_id: Mapping[str, Sequence[RawBar]],
    *,
    listed_on: Callable[[str], date | None] = lambda _iid: None,
    factor_start: date | None = None,
    errors: list[tuple[str, str]] | None = None,
    source: str | None = None,
    method: str | None = None,
) -> list[tuple[str, str]]:
    """Fetch and store the adjust factors of every instrument in ``by_id`` (``{iid: bars}``).

    The window per instrument comes from ``adjust_factor_fetch_window`` using the factors already
    in ``factor_store`` (``adjust_factors([iid], source=...)``): the first run pulls the full
    history, later runs only the tail. A failure for one symbol is appended to ``errors`` as
    ``(symbol, error code)`` and never raised (the bars are already written); the list is returned.

    **Seed row.** A *successful* full-history fetch that returns ``[]`` for an instrument with no
    stored factors (never had an ex-date) writes one seed ``AdjustFactor`` with all three factors 1,
    ``effective_from = job_window.end`` (the date the empty full-history fetch has checked through),
    and the provider's ``source`` / ``method`` (``baostock`` / ``pct_return``). Sessions before that
    date still use factor 1 (no earlier event). The watermark lets the next job start incrementally
    from ``job_window.end`` instead of re-pulling listing/1990-01-01 every time. That attests
    "no ex-dates through this date" so a later QFQ / HFQ is the identity (scale 1) instead of
    ``missing_adjust_factors``. No ``CorporateAction`` is invented. A seed is only written when the
    window really reaches back to the full-history start (not for a late ``factor_start``), and
    never when the instrument already has factors (a seed included): an incremental fetch that
    finds nothing adds nothing.
    """
    from openbars.providers.cn.baostock import ADJUST_METHOD, ADJUST_SOURCE

    source, method = source or ADJUST_SOURCE, method or ADJUST_METHOD
    errors = [] if errors is None else errors
    for iid, group in sorted(by_id.items()):
        symbol = group[0].symbol_as_returned
        try:
            listed = listed_on(iid)
            existing = _stored_factors(factor_store, iid, source)
            window = adjust_factor_fetch_window(existing, job_window, listed_on=listed, factor_start=factor_start)
            request = FetchRequest(Market.CN, Interval.D1, (symbol,), window)
            found = [from_baostock(f, iid) for f in fetch(request)]
            if not found and not existing:
                full_start = listed or FACTOR_FULL_HISTORY_START
                if window.start <= full_start:
                    one = Decimal(1)
                    # Watermark = job end: later jobs increment from here, not from listing/1990.
                    found = [AdjustFactor(iid, job_window.end, source, method, one, one, one, symbol)]
            if found:
                factor_store.upsert_adjust_factors(found)
        except Exception as exc:  # noqa: BLE001 - bars are written; a factor outage must not fail the job
            errors.append((symbol, getattr(exc, "code", type(exc).__name__)))
    return errors
