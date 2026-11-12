"""Query-time price adjustment from stored bars + corporate actions (exact ``Decimal``).

Default profile ``research-daily-v2`` (v0.1)
--------------------------------------------
Stored bars are **split-adjusted as delivered** (backward-adjusted to the latest split the
source knew, no dividends): the stored basis is ``Adjustment.SPLIT`` and a plain ``SPLIT``
read with no ``as_of`` is the stored value, returned unchanged. Everything else is derived
here. A later optional profile may store ``RAW`` bars; each row's own ``adjustment`` says
which basis it is on, and this module handles both.

Conventions
-----------
* Split ``ratio`` = new / old shares (4-for-1 -> 4, 1-for-4 reverse -> 0.25). Undoing a split
  multiplies OHLC (and VWAP) by the ratio and divides volume by it. Applying one divides
  prices by the ratio and multiplies volume. Results are rounded half-even to the storage
  scale (8 digits) once, after all factors are combined.
* For a bar on ``session_date = d`` and knowledge date ``A = as_of``::

    requested                stored SPLIT (research-daily-v2)   stored RAW (future profile)
    RAW                      undo every split with ex > d       unchanged
    SPLIT, A is None         unchanged                          apply every split with ex > d
    SPLIT, A                 undo splits with ex > d and ex > A apply splits with d < ex <= A
    SPLIT_DIVIDEND / ALL     the SPLIT result, then dividends   the SPLIT result, then dividends

  So ``as_of`` *applies* only actions with ``ex_date <= A`` and *undoes* later ones on an
  already-adjusted series: the series looks as it did on ``A``.
* Cash dividends (``kind=DIVIDEND`` with ``amount``, no ``ratio``): every bar with
  ``session_date < ex_date`` is multiplied by ``P / (P + amount)`` where ``P`` is the
  **unadjusted** close of the last stored session before ``ex_date`` (the stored close with
  the later splits undone, because ``amount`` is per share as paid). Factors of several
  dividends compound. Only dividends with ``ex_date <= A`` apply when ``A`` is given. A
  dividend needs a stored session before its ex-date in the rows passed in (with none there
  is nothing to adjust), so callers should pass history through the latest bar / ``as_of``.
  Volume is not dividend-adjusted.
* Stock dividends (``kind=DIVIDEND`` with ``ratio`` and no ``amount``) are provider-defined
  (the number may be new/old shares *or* a stock-dividend rate), so they are treated as
  splits only with ``include_stock_dividends=True``; otherwise they are listed in
  ``AdjustResult.skipped`` and bars are left alone. Spin-offs are not adjusted (also
  reported). An action that cannot be applied never changes a bar silently: it appears in
  ``skipped`` with a reason.
* Several sources' rows for one event are first merged by ``merge_corporate_actions``
  (disputed events use the chosen row and are listed in ``AdjustResult.disputed``).

Missing corporate actions
-------------------------
Stored split-adjusted bars cannot be turned into another basis without the corporate-action
rows, and *no rows* cannot be told apart from *a name that never split*. So deriving a basis
other than the stored one (``RAW``, ``SPLIT_DIVIDEND`` / ``ALL`` from stored ``SPLIT``, or
``SPLIT`` / ... from stored ``RAW``) for an instrument that has **no** corporate-action row
at all never relabels the stored prices:

* ``on_missing_actions="raise"`` (default): ``AdjustmentError`` with reason code
  ``missing_corporate_actions`` when the request would *relabel* the stored basis
  (e.g. ``RAW`` from stored ``SPLIT``).
* ``on_missing_actions="flag"``: those bars are returned on the *stored* basis (their
  ``adjustment`` is unchanged) with ``QualityFlag.ADJUSTMENT_UNVERIFIED``, and the
  instruments are listed in ``AdjustResult.unverified``. Other instruments are derived.

A ``SPLIT`` read at an ``as_of`` with no corporate-action rows does not relabel prices
(the stored basis already is ``SPLIT``), but the as-of claim cannot be attested, so the
bars are flagged ``ADJUSTMENT_UNVERIFIED`` and never raise. An instrument that *has* rows
but none after a given bar is attested ("we looked"): the split scale is 1 and ``RAW`` is
exact. A plain ``SPLIT`` read with no ``as_of`` is unaffected.

``adjustment_as_of`` consistency: a call whose rows carry differing ``adjustment_as_of``
values (``RawBarRow``) is an ``adjustment_cutoff_mismatch`` and raises ``AdjustmentError``.
"""

from __future__ import annotations

import dataclasses
from bisect import bisect_left
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from decimal import ROUND_HALF_EVEN, Context, Decimal
from typing import Any, Generic, Iterable, Protocol, Sequence, TypeVar

from openbars.core.models import Adjustment, Bar, CorporateAction, CorporateActionKind, QualityFlag
from openbars.normalize.actions import MergedAction, merge_corporate_actions
from openbars.reconcile.compatibility import ADJUSTMENT_CUTOFF_MISMATCH, SeriesCompatibility

__all__ = [
    "AdjustResult",
    "Adjuster",
    "AdjustmentError",
    "QueryTimeAdjuster",
    "MISSING_CORPORATE_ACTIONS",
    "SkippedAction",
    "adjust_rows",
    "check_adjustment_as_of",
    "requires_derivation",
    "with_flags",
]

MISSING_CORPORATE_ACTIONS = "missing_corporate_actions"

_CTX = Context(prec=60)
_PRICE_Q = Decimal(1).scaleb(-8)
_PRICE_FIELDS = ("open", "high", "low", "close", "vwap")
_ONE = Decimal(1)

Row = TypeVar("Row")


class AdjustmentError(ValueError):
    """The requested adjustment cannot be derived from the stored rows."""

    def __init__(self, message: str, reason_code: str | None = None) -> None:
        super().__init__(message)
        self.reason_code = reason_code


class Adjuster(Protocol):
    def adjust(
        self, bars: Sequence[Bar], actions: Iterable[CorporateAction], mode: Adjustment
    ) -> Sequence[Bar]: ...


@dataclass(frozen=True)
class SkippedAction:
    action: CorporateAction
    reason: str  # stock_dividend_not_applied | spinoff_not_adjusted | invalid_dividend_amount | invalid_prior_close


@dataclass(frozen=True)
class AdjustResult(Generic[Row]):
    rows: list[Row]
    skipped: tuple[SkippedAction, ...] = ()
    disputed: tuple[MergedAction, ...] = ()  # events whose sources disagreed (chosen row used)
    unverified: tuple[str, ...] = ()  # instruments left on the stored basis (no corporate-action rows)


def check_adjustment_as_of(values: Iterable[date | None]) -> SeriesCompatibility:
    """Mixing different ``adjustment_as_of`` values in one window is a mismatch."""
    if len(set(values)) > 1:
        return SeriesCompatibility(False, ADJUSTMENT_CUTOFF_MISMATCH)
    return SeriesCompatibility(True, None)


def with_flags(row: Any, flags: QualityFlag) -> Any:
    """``row`` with ``flags`` OR-ed in (``flags`` may be a tuple of names or a ``QualityFlag``).

    Rows without a ``flags`` field are returned unchanged.
    """
    current = getattr(row, "flags", None)
    if current is None:
        return row
    if isinstance(current, QualityFlag):
        merged = current | flags
        return row if merged == current else dataclasses.replace(row, flags=merged)
    have = QualityFlag.NONE
    for name in current:
        have |= QualityFlag[name]
    merged = have | flags
    if merged == have:
        return row
    return dataclasses.replace(row, flags=tuple(f.name for f in QualityFlag if f is not QualityFlag.NONE and f in merged))


def _prod(values: Iterable[Decimal]) -> Decimal:
    out = _ONE
    for v in values:
        out = _CTX.multiply(out, v)
    return out


def _is_split_like(a: CorporateAction, include_stock_dividends: bool) -> bool:
    if a.ratio is None:
        return False
    if a.kind is CorporateActionKind.SPLIT:
        return True
    return include_stock_dividends and a.kind is CorporateActionKind.DIVIDEND and a.amount is None


def requires_derivation(
    mode: Adjustment,
    *,
    stored: Adjustment = Adjustment.SPLIT,
    as_of: date | None = None,
    actions: Iterable[CorporateAction] = (),
    include_stock_dividends: bool = False,
) -> bool:
    """Can the stored series be returned as is (``False``) for this request?

    ``mode == stored`` needs no derivation, except a stored ``SPLIT`` series read at an
    ``as_of`` before a later split (``actions`` are only consulted for that case).
    """
    mode, stored = Adjustment(mode), Adjustment(stored)
    if mode is not stored:
        return True
    if mode is Adjustment.SPLIT and as_of is not None:
        return any(a.ex_date > as_of and _is_split_like(a, include_stock_dividends) for a in actions)
    return False


class _Instrument:
    """Per-instrument splits, cash dividends and prior-close lookup."""

    def __init__(self, rows: Sequence[Any], actions: Sequence[CorporateAction], include_stock: bool) -> None:
        self.splits = sorted(
            ((a.ex_date, a.ratio) for a in actions if _is_split_like(a, include_stock)),
            key=lambda x: x[0],
        )
        self.dividends = sorted(
            (
                a
                for a in actions
                if a.kind is CorporateActionKind.DIVIDEND
                and a.amount is not None
                and a.amount > 0
                and not _is_split_like(a, include_stock)
            ),
            key=lambda a: a.ex_date,
        )
        last: dict[date, Any] = {}  # session -> its last bar
        for r in rows:
            cur = last.get(r.session_date)
            if cur is None or r.ts_start >= cur.ts_start:
                last[r.session_date] = r
        self.sessions = sorted(last)
        self._last = last

    def raw_close_before(self, ex_date: date) -> Decimal | None:
        """Unadjusted close of the last session strictly before ``ex_date`` (None: no bar)."""
        i = bisect_left(self.sessions, ex_date)
        if i == 0:
            return None
        day = self.sessions[i - 1]
        row = self._last[day]
        if Adjustment(row.adjustment) is Adjustment.SPLIT:
            return _CTX.multiply(row.close, _prod(r for e, r in self.splits if e > day))
        return row.close


def _split_scale(
    stored: Adjustment, mode: Adjustment, d: date, as_of: date | None, inst: _Instrument
) -> tuple[Decimal, bool]:
    """``(scale, invert)``: prices are multiplied by ``scale`` (divided if ``invert``)."""
    if mode is Adjustment.RAW:
        if stored is Adjustment.SPLIT:
            return _prod(r for e, r in inst.splits if e > d), False
        return _ONE, False
    if stored is Adjustment.SPLIT:
        if as_of is None:
            return _ONE, False
        return _prod(r for e, r in inst.splits if e > d and e > as_of), False
    return _prod(r for e, r in inst.splits if e > d and (as_of is None or e <= as_of)), True


def _quant(value: Decimal) -> Decimal:
    return value.quantize(_PRICE_Q, rounding=ROUND_HALF_EVEN, context=_CTX)


def adjust_rows(
    rows: Iterable[Row],
    actions: Iterable[CorporateAction],
    mode: Adjustment,
    *,
    as_of: date | None = None,
    include_stock_dividends: bool = False,
    source_priority: Sequence[str] = (),
    on_missing_actions: str = "raise",
) -> AdjustResult[Row]:
    """Derive ``mode`` (at ``as_of``) for ``Bar`` / ``CanonicalBarRow`` / ``RawBarRow`` rows.

    Rows may span instruments and need not be sorted; the output keeps the input order.
    Rows already on the requested basis (nothing to undo/apply) are returned as the same
    object. ``on_missing_actions`` (``"raise"`` | ``"flag"``) decides what happens to an
    instrument with no corporate-action rows when a different basis is requested. See the
    module docstring for the arithmetic and that rule.
    """
    mode = Adjustment(mode)
    if mode in (Adjustment.QFQ, Adjustment.HFQ):
        raise AdjustmentError(
            f"{mode.value} is derived for A-share bars only (normalize.cn_adjust)", "unsupported_cn_adjustment"
        )
    if on_missing_actions not in ("raise", "flag"):
        raise ValueError(f"on_missing_actions must be 'raise' or 'flag', got {on_missing_actions!r}")
    rows = list(rows)
    mismatch = check_adjustment_as_of(getattr(r, "adjustment_as_of", None) for r in rows)
    if not mismatch.comparable:
        raise AdjustmentError("rows mix different adjustment_as_of values", mismatch.reason_code)

    by_instrument: dict[str, list[Any]] = defaultdict(list)
    for r in rows:
        by_instrument[r.instrument_id].append(r)
    merged = merge_corporate_actions(
        (a for a in actions if a.instrument_id in by_instrument), source_priority=source_priority
    )
    acts: dict[str, list[CorporateAction]] = defaultdict(list)
    for m in merged:
        acts[m.action.instrument_id].append(m.action)

    def _asks_other_basis(rs: list[Any]) -> bool:
        # stored dividend-adjusted series are rejected below, not flagged here
        return any(
            Adjustment(r.adjustment) not in (mode, Adjustment.SPLIT_DIVIDEND, Adjustment.ALL) for r in rs
        )

    unverified: set[str] = set()
    for iid, rs in by_instrument.items():
        if iid in acts:
            continue
        # no rows at all: cannot attest a different basis, nor a SPLIT-at-as_of claim
        if _asks_other_basis(rs) or as_of is not None:
            unverified.add(iid)
    raisable = {iid for iid in unverified if _asks_other_basis(by_instrument[iid])}
    if raisable and on_missing_actions == "raise":
        shown = ", ".join(sorted(raisable)[:5]) + (" ..." if len(raisable) > 5 else "")
        raise AdjustmentError(
            f"cannot derive {mode.value} bars: no corporate-action rows for {shown} "
            "(no rows is not the same as no splits; ingest actions or pass on_missing_actions='flag')",
            MISSING_CORPORATE_ACTIONS,
        )

    skipped: list[SkippedAction] = []
    dividend_mode = mode in (Adjustment.SPLIT_DIVIDEND, Adjustment.ALL)
    insts: dict[str, _Instrument] = {}
    div_factors: dict[str, list[tuple[date, Decimal]]] = {}
    for iid, rs in by_instrument.items():
        inst = insts[iid] = _Instrument(rs, acts[iid], include_stock_dividends)
        for a in acts[iid]:
            if a.kind is CorporateActionKind.SPINOFF:
                skipped.append(SkippedAction(a, "spinoff_not_adjusted"))
            elif a.kind is CorporateActionKind.DIVIDEND and not _is_split_like(a, include_stock_dividends):
                if a.amount is None:
                    skipped.append(SkippedAction(a, "stock_dividend_not_applied"))
                elif a.amount <= 0:
                    skipped.append(SkippedAction(a, "invalid_dividend_amount"))
        factors: list[tuple[date, Decimal]] = []
        if dividend_mode:
            for a in inst.dividends:
                if as_of is not None and a.ex_date > as_of:
                    continue
                p = inst.raw_close_before(a.ex_date)
                if p is None:
                    continue  # no stored session before the ex-date: nothing to adjust
                if p <= 0:
                    skipped.append(SkippedAction(a, "invalid_prior_close"))
                    continue
                factors.append((a.ex_date, _CTX.divide(p, p + a.amount)))
        div_factors[iid] = factors

    out: list[Row] = []
    for r in rows:
        stored = Adjustment(r.adjustment)
        if stored in (Adjustment.SPLIT_DIVIDEND, Adjustment.ALL):
            if stored is mode and as_of is None:
                out.append(r)
                continue
            raise AdjustmentError(f"cannot derive {mode.value} from stored {stored.value} bars")
        if r.instrument_id in unverified and (stored is not mode or as_of is not None):
            out.append(with_flags(r, QualityFlag.ADJUSTMENT_UNVERIFIED))  # stored basis, never relabelled
            continue
        scale, invert = _split_scale(stored, mode, r.session_date, as_of, insts[r.instrument_id])
        div = _prod(f for ex, f in div_factors[r.instrument_id] if ex > r.session_date)
        if stored is mode and scale == _ONE and div == _ONE:
            out.append(r)
            continue
        changes: dict[str, Any] = {}
        for name in _PRICE_FIELDS:
            v = getattr(r, name, None)
            if v is not None:
                v = _CTX.divide(v, scale) if invert else _CTX.multiply(v, scale)
                changes[name] = _quant(_CTX.multiply(v, div))
        if r.volume is not None:
            v = _CTX.multiply(r.volume, scale) if invert else _CTX.divide(r.volume, scale)
            changes["volume"] = _quant(v)
        changes["adjustment"] = mode if isinstance(r.adjustment, Adjustment) else mode.value
        out.append(dataclasses.replace(r, **changes))
    return AdjustResult(out, tuple(skipped), tuple(m for m in merged if m.disputed), tuple(sorted(unverified)))


class QueryTimeAdjuster:
    """``Adjuster`` over ``Bar`` models (the plain functions above do the work).

    ``as_of`` fixes the knowledge date for every ``adjust`` call; ``adjust_with_report``
    also returns skipped / disputed actions.
    """

    def __init__(
        self,
        *,
        as_of: date | None = None,
        include_stock_dividends: bool = False,
        source_priority: Sequence[str] = (),
        on_missing_actions: str = "raise",
    ) -> None:
        self.as_of = as_of
        self.include_stock_dividends = include_stock_dividends
        self.source_priority = tuple(source_priority)
        self.on_missing_actions = on_missing_actions

    def adjust_with_report(
        self, bars: Sequence[Bar], actions: Iterable[CorporateAction], mode: Adjustment
    ) -> AdjustResult[Bar]:
        return adjust_rows(
            bars,
            actions,
            mode,
            as_of=self.as_of,
            include_stock_dividends=self.include_stock_dividends,
            source_priority=self.source_priority,
            on_missing_actions=self.on_missing_actions,
        )

    def adjust(
        self, bars: Sequence[Bar], actions: Iterable[CorporateAction], mode: Adjustment
    ) -> list[Bar]:
        return self.adjust_with_report(bars, actions, mode).rows
