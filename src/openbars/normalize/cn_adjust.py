"""A-share 前复权 (QFQ) / 后复权 (HFQ) derived at query time from RAW bars + adjust factors.

Profile ``cn-daily-v1``: the stored series is **unadjusted** (``Adjustment.RAW``). Adjusted
prices are never persisted (a forward adjustment rewrites all history on every new ex-date);
this module derives them from the stored bars and a factor table that lives next to the bars
(``ref/adjust_factors.parquet``). No ``CorporateAction`` rows are invented from factors.

Factors (BaoStock 涨跌幅复权法, ``method="pct_return"``)
-------------------------------------------------------
One ``AdjustFactor`` per ex-date: from ``effective_from`` on, the **cumulative backward
factor** ``back_adjust_factor`` applies (it is 1 before the first event and stays constant
until the next one, so adding a new event never changes an older session's factor). For a
session ``d`` and knowledge date ``A = as_of``::

    f(d)  = back factor of the last event with effective_from <= d and (A is None or <= A)
            (1 when no such event: sessions before the first event, or no event visible at A)
    ref   = back factor of the last event visible at A (A None: the latest event; none: 1)
    HFQ   = raw * f(d)
    QFQ   = raw * f(d) / ref

So with no visible event ``QFQ == HFQ == raw``. ``open/high/low/close`` and ``vwap`` /
``preclose`` (when the row has them) are scaled and rounded half-even to the storage scale (8
digits) once; ``volume`` / ``amount`` stay **as traded** (never scaled). Each derived row's
``adjustment`` is relabelled to the requested mode. All arithmetic is exact ``Decimal``.

Sources are never mixed
-----------------------
Factors from different sources (or methods) are not interchangeable. ``source=None`` takes the
only source an instrument has factors from and raises ``AdjustmentError`` (``ambiguous_factor_source``)
when there are several; ``source="x"`` uses only that source's rows.

Missing factors
---------------
An instrument with **zero** factor rows (for the chosen source) cannot be told apart from one
that never had an ex-date, so its prices are never silently relabelled:

* ``on_missing_factors="raise"`` (default): ``AdjustmentError`` with reason ``missing_adjust_factors``;
* ``on_missing_factors="flag"``: the stored RAW rows are returned unchanged (``adjustment`` stays
  ``raw``) with ``QualityFlag.ADJUSTMENT_UNVERIFIED``; the instrument is listed in
  ``AdjustResult.unverified``.

An instrument that has at least one factor row is attested: sessions before the first event use
factor 1.
"""

from __future__ import annotations

import dataclasses
from bisect import bisect_right
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from decimal import ROUND_HALF_EVEN, Context, Decimal
from typing import TYPE_CHECKING, Any, Iterable

from openbars.core.models import Adjustment, QualityFlag
from openbars.normalize.adjust import AdjustmentError, AdjustResult, with_flags

if TYPE_CHECKING:  # the provider module is heavy; only its attribute names are used
    from openbars.providers.cn.baostock import BaoStockAdjustFactor

__all__ = [
    "AMBIGUOUS_FACTOR_SOURCE",
    "AdjustFactor",
    "MISSING_ADJUST_FACTORS",
    "UNSUPPORTED_CN_ADJUSTMENT",
    "adjust_cn_rows",
    "from_baostock",
]

MISSING_ADJUST_FACTORS = "missing_adjust_factors"
AMBIGUOUS_FACTOR_SOURCE = "ambiguous_factor_source"
UNSUPPORTED_CN_ADJUSTMENT = "unsupported_cn_adjustment"

_CTX = Context(prec=60)
_PRICE_Q = Decimal(1).scaleb(-8)
_PRICE_FIELDS = ("open", "high", "low", "close", "vwap", "preclose")
_ONE = Decimal(1)


@dataclass(frozen=True)
class AdjustFactor:
    """One adjust-factor event: from ``effective_from`` (the ex-date) the factors below apply.

    ``source`` / ``method`` record where and how the factors were computed (e.g. ``baostock`` /
    ``pct_return``); they are never mixed for one instrument. ``symbol`` is informational.
    """

    instrument_id: str
    effective_from: date
    source: str
    method: str
    fore_adjust_factor: Decimal
    back_adjust_factor: Decimal
    adjust_factor: Decimal
    symbol: str | None = None

    def __post_init__(self) -> None:
        if type(self.effective_from) is not date:
            raise TypeError("effective_from must be a date (not datetime)")
        for name in ("instrument_id", "source", "method"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name):
                raise ValueError(f"{name} must be a non-empty string")
        for name in ("fore_adjust_factor", "back_adjust_factor", "adjust_factor"):
            value = getattr(self, name)
            if isinstance(value, (bool, float)) or not isinstance(value, (Decimal, int, str)):
                raise TypeError(f"{name} must be Decimal, int or str, not {type(value).__name__}")
            value = value if isinstance(value, Decimal) else Decimal(value)
            if not value.is_finite() or value <= 0:
                raise ValueError(f"{name} must be a positive finite number, got {value}")
            object.__setattr__(self, name, value)

    @property
    def key(self) -> tuple[str, str, date]:
        return (self.instrument_id, self.source, self.effective_from)


def from_baostock(factor: BaoStockAdjustFactor, instrument_id: str) -> AdjustFactor:
    """``BaoStockAdjustFactor`` (+ the resolved instrument id) -> ``AdjustFactor``.

    ``source`` / ``method`` come from the factor (``baostock`` / ``pct_return``); the BaoStock
    code (``sh.600000``) is kept as ``symbol``.
    """
    return AdjustFactor(
        instrument_id=instrument_id,
        effective_from=factor.divid_operate_date,
        source=factor.source,
        method=factor.method,
        fore_adjust_factor=factor.fore_adjust_factor,
        back_adjust_factor=factor.back_adjust_factor,
        adjust_factor=factor.adjust_factor,
        symbol=factor.code,
    )


def _quant(value: Decimal) -> Decimal:
    return value.quantize(_PRICE_Q, rounding=ROUND_HALF_EVEN, context=_CTX)


class _Timeline:
    """Back factors of one instrument (one source), by effective date."""

    def __init__(self, factors: list[AdjustFactor], as_of: date | None) -> None:
        visible = sorted(
            (f for f in factors if as_of is None or f.effective_from <= as_of),
            key=lambda f: f.effective_from,
        )
        self.days = [f.effective_from for f in visible]
        self.back = [f.back_adjust_factor for f in visible]
        self.ref = self.back[-1] if self.back else _ONE

    def back_at(self, day: date) -> Decimal:
        i = bisect_right(self.days, day)
        return self.back[i - 1] if i else _ONE


def _timelines(
    ids: Iterable[str],
    factors: Iterable[AdjustFactor],
    source: str | None,
    as_of: date | None,
) -> tuple[dict[str, _Timeline], list[str]]:
    """Per-instrument timelines and the instruments without any factor row (for ``source``)."""
    ids = set(ids)
    by_id: dict[str, list[AdjustFactor]] = defaultdict(list)
    for f in factors:
        if f.instrument_id in ids and (source is None or f.source == source):
            by_id[f.instrument_id].append(f)
    for iid, rows in by_id.items():
        sources = sorted({f.source for f in rows})
        methods = sorted({f.method for f in rows})
        if len(sources) > 1:
            raise AdjustmentError(
                f"adjust factors of {iid} come from several sources ({', '.join(sources)}); "
                "pass source= (sources are never mixed)",
                AMBIGUOUS_FACTOR_SOURCE,
            )
        if len(methods) > 1:
            raise AdjustmentError(
                f"adjust factors of {iid} use several methods ({', '.join(methods)}); "
                "they are not interchangeable",
                AMBIGUOUS_FACTOR_SOURCE,
            )
    timelines = {iid: _Timeline(rows, as_of) for iid, rows in by_id.items()}
    return timelines, sorted(ids - set(timelines))


def adjust_cn_rows(
    rows: Iterable[Any],
    factors: Iterable[AdjustFactor],
    mode: Adjustment,
    *,
    as_of: date | None = None,
    source: str | None = None,
    on_missing_factors: str = "raise",
) -> AdjustResult[Any]:
    """Derive ``QFQ`` / ``HFQ`` (at ``as_of``) for stored-RAW ``Bar`` / ``CanonicalBarRow`` rows.

    Rows may span instruments and need not be sorted; output keeps the input order. ``mode``
    ``RAW`` is a no-op (rows returned as is); any other mode than ``QFQ`` / ``HFQ`` is an
    ``AdjustmentError`` (``unsupported_cn_adjustment``), as is a row not stored as ``RAW``
    (an adjusted row cannot be re-based). See the module docstring for the arithmetic, the
    source rule and the missing-factor policy.
    """
    mode = Adjustment(mode)
    if on_missing_factors not in ("raise", "flag"):
        raise ValueError(f"on_missing_factors must be 'raise' or 'flag', got {on_missing_factors!r}")
    rows = list(rows)
    if mode is Adjustment.RAW:
        return AdjustResult(rows)
    if mode not in (Adjustment.QFQ, Adjustment.HFQ):
        raise AdjustmentError(
            f"cannot derive {mode.value} for A-share bars (only raw, qfq and hfq)", UNSUPPORTED_CN_ADJUSTMENT
        )
    for r in rows:
        if Adjustment(r.adjustment) is not Adjustment.RAW:
            raise AdjustmentError(
                f"cannot derive {mode.value} from stored {Adjustment(r.adjustment).value} A-share bars "
                "(the stored basis must be raw)",
                UNSUPPORTED_CN_ADJUSTMENT,
            )
    timelines, missing = _timelines({r.instrument_id for r in rows}, factors, source, as_of)
    if missing and on_missing_factors == "raise":
        shown = ", ".join(missing[:5]) + (" ..." if len(missing) > 5 else "")
        raise AdjustmentError(
            f"cannot derive {mode.value} bars: no adjust-factor rows for {shown} "
            "(no rows is not the same as no ex-dates; ingest factors or pass on_missing_factors='flag')",
            MISSING_ADJUST_FACTORS,
        )
    unverified = set(missing)
    qfq = mode is Adjustment.QFQ
    out: list[Any] = []
    for r in rows:
        if r.instrument_id in unverified:
            out.append(with_flags(r, QualityFlag.ADJUSTMENT_UNVERIFIED))  # stored RAW, never relabelled
            continue
        line = timelines[r.instrument_id]
        scale = line.back_at(r.session_date)
        if qfq:
            scale = _CTX.divide(scale, line.ref)
        changes: dict[str, Any] = {}
        for name in _PRICE_FIELDS:
            v = getattr(r, name, None)
            if v is not None:
                changes[name] = _quant(_CTX.multiply(v, scale))
        changes["adjustment"] = mode if isinstance(r.adjustment, Adjustment) else mode.value
        out.append(dataclasses.replace(r, **changes))
    return AdjustResult(out, unverified=tuple(sorted(unverified)))
