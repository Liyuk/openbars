"""Hong Kong share-adjustment constructors on top of the US query-time adjuster.

The arithmetic is the same as ``openbars.normalize.adjust`` (split-only storage,
query-time derivation of ``RAW`` / ``SPLIT`` / ``SPLIT_DIVIDEND`` / ``ALL``). This
module only names the common HK events and builds ``CorporateAction`` rows with a
defined ``ratio = new / old``:

* **拆股** (``hk_split``): more shares after the event, ``kind=SPLIT``, ratio > 1
  (2-for-1 → ``2``).
* **合股** (``hk_consolidation``): fewer shares after the event, ``kind=SPLIT``,
  ratio < 1 (1-for-5 → ``0.2``). This is the same encoding as a US reverse split.
* **红股** (``hk_bonus``): capitalization / bonus issue, encoded as a stock
  dividend (``kind=DIVIDEND``, ``ratio=(held+bonus)/held``, no cash ``amount``).
  Like US stock dividends, it is applied as split-like **only** when
  ``include_stock_dividends=True`` (see ``hk_adjust_rows``). The default is off
  and matches the US adjuster (Discussion #68 default; can be changed later).
* **现金分红**: use a ``DIVIDEND`` with ``amount``, no ``ratio``, and a
  ``currency`` (default ``HKD``). Factor is ``P/(P+amount)`` as on the US path,
  but only when the dividend currency matches the bar currency. An ADR's USD
  dividend is never applied to HKD bars (Discussion #68 default).
* **供股** (``hk_rights``): ``kind=RIGHTS``. Recorded only; never used as a
  price-adjustment factor (Discussion #68 default).

``hk_adjust_rows`` is a thin wrapper around ``adjust_rows``. It does not invent
a second formula.
Design: https://github.com/Liyuk/openbars/discussions/68
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import date
from decimal import Decimal
from typing import TypeVar

from openbars.core.models import Adjustment, CorporateAction, CorporateActionKind
from openbars.normalize.adjust import AdjustResult, adjust_rows

__all__ = [
    "bonus_ratio",
    "hk_adjust_rows",
    "hk_bonus",
    "hk_cash_dividend",
    "hk_consolidation",
    "hk_rights",
    "hk_split",
    "share_ratio",
]

_Row = TypeVar("_Row")


def share_ratio(new: int, old: int) -> Decimal:
    """Exact ``new / old`` shares (both must be integers >= 1)."""
    if isinstance(new, bool) or isinstance(old, bool):
        raise TypeError("new and old must be positive ints, not bool")
    if not isinstance(new, int) or not isinstance(old, int) or new < 1 or old < 1:
        raise ValueError(f"new and old must be ints >= 1, got new={new!r}, old={old!r}")
    return Decimal(new) / Decimal(old)


def bonus_ratio(bonus: int, held: int) -> Decimal:
    """``(held + bonus) / held`` for a bonus issue of ``bonus`` shares per ``held`` held."""
    if isinstance(bonus, bool) or isinstance(held, bool):
        raise TypeError("bonus and held must be positive ints, not bool")
    if not isinstance(bonus, int) or not isinstance(held, int) or bonus < 1 or held < 1:
        raise ValueError(
            f"bonus and held must be ints >= 1, got bonus={bonus!r}, held={held!r}"
        )
    return Decimal(held + bonus) / Decimal(held)


def hk_split(
    instrument_id: str,
    ex_date: date,
    new: int,
    old: int,
    source: str,
) -> CorporateAction:
    """拆股: ``new`` shares for every ``old`` (ratio > 1)."""
    ratio = share_ratio(new, old)
    if ratio <= 1:
        raise ValueError(
            f"a split must increase the share count, got {new}-for-{old} (ratio {ratio})"
        )
    return CorporateAction(
        instrument_id, ex_date, CorporateActionKind.SPLIT, source, ratio=ratio
    )


def hk_consolidation(
    instrument_id: str,
    ex_date: date,
    new: int,
    old: int,
    source: str,
) -> CorporateAction:
    """合股: ``new`` shares for every ``old`` (ratio < 1). Same kind as a reverse split."""
    ratio = share_ratio(new, old)
    if ratio >= 1:
        raise ValueError(
            f"a consolidation must decrease the share count, got {new}-for-{old} (ratio {ratio})"
        )
    return CorporateAction(
        instrument_id, ex_date, CorporateActionKind.SPLIT, source, ratio=ratio
    )


def hk_bonus(
    instrument_id: str,
    ex_date: date,
    bonus: int,
    held: int,
    source: str,
) -> CorporateAction:
    """红股: ``bonus`` new shares per ``held`` existing, encoded as a stock dividend."""
    return CorporateAction(
        instrument_id,
        ex_date,
        CorporateActionKind.DIVIDEND,
        source,
        ratio=bonus_ratio(bonus, held),
    )


def hk_cash_dividend(
    instrument_id: str,
    ex_date: date,
    amount: Decimal | int | str,
    source: str,
    *,
    currency: str = "HKD",
) -> CorporateAction:
    """Cash dividend per share. ``currency`` defaults to HKD for an XHKG ordinary."""
    return CorporateAction(
        instrument_id,
        ex_date,
        CorporateActionKind.DIVIDEND,
        source,
        amount=amount,
        currency=currency,
    )


def hk_rights(
    instrument_id: str,
    ex_date: date,
    source: str,
    *,
    ratio: Decimal | int | str | None = None,
    amount: Decimal | int | str | None = None,
    currency: str | None = None,
) -> CorporateAction:
    """供股: recorded only; ``adjust_rows`` / ``hk_adjust_rows`` never apply it to prices."""
    return CorporateAction(
        instrument_id,
        ex_date,
        CorporateActionKind.RIGHTS,
        source,
        ratio=ratio,
        amount=amount,
        currency=currency,
    )


def hk_adjust_rows(
    rows: Iterable[_Row],
    actions: Iterable[CorporateAction],
    mode: Adjustment,
    *,
    as_of: date | None = None,
    include_stock_dividends: bool = False,
    include_bonus: bool | None = None,
    source_priority: Sequence[str] = (),
    on_missing_actions: str = "raise",
) -> AdjustResult[_Row]:
    """Query-time adjuster for HK rows; delegates to ``adjust_rows``.

    ``include_bonus`` is an alias of ``include_stock_dividends`` (红股 are stock
    dividends). Default is ``False``, same as the US path. Pass ``True`` to treat
    bonus issues as split-like. The two flags must not disagree.
    """
    if include_bonus is None:
        apply_bonus = include_stock_dividends
    elif (
        include_stock_dividends is not False
        and include_bonus is not include_stock_dividends
    ):
        raise ValueError("include_bonus and include_stock_dividends disagree")
    else:
        apply_bonus = include_bonus
    return adjust_rows(
        rows,
        actions,
        mode,
        as_of=as_of,
        include_stock_dividends=apply_bonus,
        source_priority=source_priority,
        on_missing_actions=on_missing_actions,
    )
