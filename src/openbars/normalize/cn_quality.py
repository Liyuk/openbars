"""A-share daily quality marks: price limits (涨跌停), suspensions (停牌) and missing sessions.

Board from the 6-digit code (leading zeros kept; ``sh.688001`` / ``688001.SH`` /
``cn:eq:xshg:688001`` are all read)::

    688 689                                   STAR 科创板
    300 301                                   ChiNext 创业板
    600 601 603 605 000 001 002 003           main board (002 SME board counts as main, ±10%)

Beijing (``8xxxxx`` / ``43xxxx`` / ``92xxxx``) and everything else (B shares, indices, funds) is out
of scope: ``board_of_code`` raises ``ValueError``; no BSE limits are invented, and
``flags_for_cn_bar`` simply sets no ``LIMIT_*`` flag for such a code.

Daily limit ratio (``limit_ratio``; reference = the unadjusted previous close / ex-rights
reference, prices to ``0.01`` yuan, **ROUND_HALF_UP**, never banker's rounding)::

    main        ±10%
    ChiNext     ±20% from 2020-08-24 (inclusive), ±10% before
    STAR        ±20% from the board launch 2019-07-22
    ST          ±5% (``is_st``; the issue's "ST usually ±5%"). Simplification: one flat ratio
                (``st_ratio``, default 5%); rules that later changed per board (e.g. ChiNext /
                STAR ST names at ±20%, or the main-board risk-warning list moving to ±10% in
                2025) are not modelled by date, so pass ``is_st`` only where ±5% applies.

IPO / new listings follow the rule *in force at listing* (``listed_on``); "sessions" are XSHG
trading days (``openbars.calendars.cn``), the listing day being session 1:

    STAR                    first 5 sessions: no limit
    ChiNext listed >= 2020-08-24   first 5 sessions: no limit
    ChiNext listed before          session 1 special (no regular limit), then ±10%
    main    listed >= 2023-04-10   first 5 sessions: no limit
    main    listed before          session 1 special (no regular limit), then ±10%

Where no limit applies, ``limit_ratio`` returns ``None`` and no ``LIMIT_*`` flag is ever set
(however large the move). Unknown ``listed_on`` means no IPO window is assumed.

Suspensions and missing days
----------------------------
``SUSPENDED`` is ``extra["tradestatus"] in (0, "0")``. A suspended raw row is a placeholder
(volume 0, OHLC = previous close): the raw layer keeps it, the canonical layer drops it (a
calendar hole carrying the mark), and it never gets ``LIMIT_*``. ``missing_sessions`` is the
completeness check: a suspended session is *not* missing and a non-session day never is.

The suspension fact must outlive the dropped canonical bar. Raw bars carry it as
``extra["tradestatus"]`` while in flight, but the Parquet raw layer stores no ``extra``, so
``suspended_dates`` reads ``tradestatus`` when present and otherwise falls back to the
placeholder signature a suspended raw row keeps on disk: ``volume == 0``. Collect suspended
dates from the **raw** layer, not from canonical.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from enum import Enum
from typing import Any

from openbars.calendars import cn as cn_calendar
from openbars.core.models import QualityFlag
from openbars.normalize.cn_instruments import normalize_cn_symbol, parse_cn_instrument_id

__all__ = [
    "CHINEXT_REFORM_DATE",
    "CnBoard",
    "IPO_UNLIMITED_SESSIONS",
    "MAIN_REGISTRATION_DATE",
    "STAR_LAUNCH_DATE",
    "board_of_code",
    "board_of_symbol",
    "code_of",
    "flags_for_cn_bar",
    "flags_for_cn_series",
    "ipo_session_number",
    "is_suspended",
    "limit_prices",
    "limit_ratio",
    "missing_sessions",
    "suspended_dates",
]

STAR_LAUNCH_DATE = date(2019, 7, 22)
CHINEXT_REFORM_DATE = date(2020, 8, 24)
MAIN_REGISTRATION_DATE = date(2023, 4, 10)
IPO_UNLIMITED_SESSIONS = 5  # no-limit sessions after listing under registration-system rules
ST_RATIO = Decimal("0.05")
_TICK = Decimal("0.01")
_IPO_LOOKAHEAD_DAYS = 30  # five sessions never span more calendar days (Spring Festival included)


class CnBoard(str, Enum):
    MAIN = "main"
    CHINEXT = "chinext"
    STAR = "star"


_PREFIX_BOARD: dict[str, CnBoard] = {
    **dict.fromkeys(("688", "689"), CnBoard.STAR),
    **dict.fromkeys(("300", "301"), CnBoard.CHINEXT),
    **dict.fromkeys(("600", "601", "603", "605", "000", "001", "002", "003"), CnBoard.MAIN),
}


# --- boards -------------------------------------------------------------------------------


def code_of(symbol_or_id: str) -> str:
    """6-digit code of a CN symbol (``sh.688001`` / ``688001.SH``) or ``cn:eq:...`` id.

    A bare 6-digit code is rejected (like ``normalize_cn_symbol``). Raises ``ValueError``.
    """
    text = symbol_or_id.strip() if isinstance(symbol_or_id, str) else symbol_or_id
    if isinstance(text, str) and text.lower().startswith("cn:"):
        return parse_cn_instrument_id(text).code
    return normalize_cn_symbol(text)[1]


def board_of_code(code: str) -> CnBoard:
    """Board of a 6-digit code; ``ValueError`` for Beijing and any other out-of-scope prefix."""
    if not isinstance(code, str) or len(code) != 6 or not code.isdigit():
        raise ValueError(f"CN code must be exactly 6 digits, got {code!r}")
    board = _PREFIX_BOARD.get(code[:3])
    if board is None:
        raise ValueError(f"code {code!r} is not a Shanghai/Shenzhen A-share board (Beijing and others are out of scope)")
    return board


def board_of_symbol(symbol_or_id: str) -> CnBoard:
    return board_of_code(code_of(symbol_or_id))


# --- limits -------------------------------------------------------------------------------


def ipo_session_number(listed_on: date, session_date: date, sessions: Sequence[date] | None = None) -> int | None:
    """1-based XSHG session number of ``session_date`` counted from ``listed_on`` (session 1).

    ``None`` before listing. ``sessions`` may be any sorted/unsorted container of XSHG session
    dates covering ``[listed_on, session_date]``; ``None`` asks the CN calendar. A listing day
    that is not a session counts from the next session.
    """
    if session_date < listed_on:
        return None
    days = cn_calendar.sessions(listed_on, session_date) if sessions is None else sessions
    return sum(1 for d in days if listed_on <= d <= session_date) or None


def _ipo_unlimited(
    board: CnBoard, listed_on: date | None, session_date: date, sessions: Sequence[date] | None
) -> bool:
    if listed_on is None or session_date < listed_on:
        return False
    if board is CnBoard.STAR:
        window = IPO_UNLIMITED_SESSIONS
    elif board is CnBoard.CHINEXT:
        window = IPO_UNLIMITED_SESSIONS if listed_on >= CHINEXT_REFORM_DATE else 1
    else:
        window = IPO_UNLIMITED_SESSIONS if listed_on >= MAIN_REGISTRATION_DATE else 1
    if sessions is None and (session_date - listed_on).days > _IPO_LOOKAHEAD_DAYS:
        return False
    number = ipo_session_number(listed_on, session_date, sessions)
    return number is not None and number <= window


def limit_ratio(
    board: CnBoard,
    session_date: date,
    *,
    is_st: bool = False,
    listed_on: date | None = None,
    sessions: Sequence[date] | None = None,
    st_ratio: Decimal = ST_RATIO,
) -> Decimal | None:
    """Daily limit ratio (``Decimal("0.1")`` = ±10%) or ``None`` when no limit applies.

    ``None``: inside the IPO no-limit window, or a STAR session before the board launch. See the
    module docstring for the rules; ``sessions`` / the calendar only matter within ~30 days of
    ``listed_on``.
    """
    board = CnBoard(board)
    if board is CnBoard.STAR and session_date < STAR_LAUNCH_DATE:
        return None
    if _ipo_unlimited(board, listed_on, session_date, sessions):
        return None
    if is_st:
        return st_ratio
    if board is CnBoard.MAIN:
        return Decimal("0.1")
    if board is CnBoard.CHINEXT:
        return Decimal("0.2") if session_date >= CHINEXT_REFORM_DATE else Decimal("0.1")
    return Decimal("0.2")


def limit_prices(preclose: Decimal, ratio: Decimal) -> tuple[Decimal, Decimal]:
    """``(limit_up, limit_down)`` = ``preclose * (1 ± ratio)`` to 0.01 yuan, ROUND_HALF_UP."""
    if isinstance(preclose, float) or isinstance(ratio, float):
        raise TypeError("preclose and ratio must be Decimal, never float")
    preclose, ratio = Decimal(preclose), Decimal(ratio)
    if preclose <= 0:
        raise ValueError(f"preclose must be positive, got {preclose}")
    up = (preclose * (1 + ratio)).quantize(_TICK, rounding=ROUND_HALF_UP)
    down = (preclose * (1 - ratio)).quantize(_TICK, rounding=ROUND_HALF_UP)
    return up, down


# --- bars ---------------------------------------------------------------------------------


def _extra(bar: Any) -> Mapping[str, object]:
    return getattr(bar, "extra", None) or {}


def _tradestatus(bar: Any) -> int | None:
    value = _extra(bar).get("tradestatus")
    if value is None:
        return None
    text = str(value).strip()
    return int(text) if text in ("0", "1") else None


def is_suspended(bar: Any) -> bool:
    """``extra["tradestatus"]`` is 0 / ``"0"`` (BaoStock 停牌); no field means not marked."""
    return _tradestatus(bar) == 0


def _bool_extra(bar: Any, name: str) -> bool | None:
    value = _extra(bar).get(name)
    if value is None:
        return None
    return str(value).strip() in ("1", "true", "True")


def _reference(bar: Any, previous_close: Decimal | None) -> Decimal | None:
    """The ex-rights reference: ``extra["preclose"]`` when given, else the previous close."""
    value = _extra(bar).get("preclose")
    if value is not None and str(value).strip():
        return Decimal(str(value).strip())
    return previous_close


def _session_of(bar: Any) -> date:
    if getattr(bar, "session_date", None) is not None:
        return bar.session_date
    return bar.ts_start.astimezone(cn_calendar.CN_TZ).date()


def _board_of_bar(bar: Any) -> CnBoard | None:
    for name in ("instrument_id", "symbol_as_returned"):
        value = getattr(bar, name, None)
        if value:
            try:
                return board_of_symbol(value)
            except ValueError:
                continue
    return None


def flags_for_cn_bar(
    bar: Any,
    *,
    previous_close: Decimal | None = None,
    listed_on: date | None = None,
    sessions: Sequence[date] | None = None,
    is_st: bool | None = None,
) -> QualityFlag:
    """``SUSPENDED`` / ``LIMIT_UP`` / ``LIMIT_DOWN`` for one raw (unadjusted) A-share bar.

    * suspended (``tradestatus`` 0): ``SUSPENDED`` only, never ``LIMIT_*``;
    * otherwise ``LIMIT_UP`` / ``LIMIT_DOWN`` when ``close`` equals the rounded limit price
      exactly (0.01 yuan away is no flag);
    * no flag when no limit applies (IPO window), the board is out of scope, or there is no
      reference price (``extra["preclose"]``, else ``previous_close``).

    ``is_st`` defaults to ``extra["isST"]``. ``bar`` is a ``RawBar`` (or anything with
    ``extra`` / ``close`` / ``session_date``).
    """
    if is_suspended(bar):
        return QualityFlag.SUSPENDED
    board = _board_of_bar(bar)
    reference = _reference(bar, previous_close)
    if board is None or reference is None or reference <= 0:
        return QualityFlag.NONE
    st = _bool_extra(bar, "isST") if is_st is None else is_st
    ratio = limit_ratio(
        board, _session_of(bar), is_st=bool(st), listed_on=listed_on, sessions=sessions
    )
    if ratio is None:
        return QualityFlag.NONE
    up, down = limit_prices(reference, ratio)
    if bar.close == up:
        return QualityFlag.LIMIT_UP
    if bar.close == down:
        return QualityFlag.LIMIT_DOWN
    return QualityFlag.NONE


def flags_for_cn_series(
    bars: Sequence[Any],
    *,
    listed_on: date | None = None,
    sessions: Sequence[date] | None = None,
    is_st: bool | None = None,
) -> list[QualityFlag]:
    """``flags_for_cn_bar`` over one instrument's bars; result aligned with the input order.

    Bars are walked in session order; the previous *raw* close (suspension placeholders
    included: their close is the unchanged reference) backs up a missing ``extra["preclose"]``.
    """
    order = sorted(range(len(bars)), key=lambda i: (_session_of(bars[i]), bars[i].ts_start))
    out: list[QualityFlag] = [QualityFlag.NONE] * len(bars)
    previous: Decimal | None = None
    for i in order:
        out[i] = flags_for_cn_bar(
            bars[i], previous_close=previous, listed_on=listed_on, sessions=sessions, is_st=is_st
        )
        previous = bars[i].close
    return out


# --- missing sessions ---------------------------------------------------------------------


def suspended_dates(raw_bars: Iterable[Any]) -> set[date]:
    """Sessions of ``raw_bars`` that were trading suspensions.

    ``extra["tradestatus"]`` decides when present (``0`` suspended, ``1`` traded even with zero
    volume); a bar without it (e.g. read back from Parquet, which stores no ``extra``) counts as
    suspended when its ``volume`` is 0, the placeholder a suspended raw row keeps.
    """
    out: set[date] = set()
    for bar in raw_bars:
        status = _tradestatus(bar)
        if status == 0 or (status is None and bar.volume is not None and bar.volume == 0):
            out.add(_session_of(bar))
    return out


def missing_sessions(
    calendar_sessions: Iterable[date],
    present_session_dates: Iterable[date],
    suspended_session_dates: Iterable[date] = (),
) -> list[date]:
    """In-calendar sessions with no bar that were not suspensions, ascending.

    Holidays (not in ``calendar_sessions``) and suspended sessions are never missing; bars on
    non-session dates are ignored.
    """
    present = set(present_session_dates)
    suspended = set(suspended_session_dates)
    return sorted(d for d in set(calendar_sessions) if d not in present and d not in suspended)
