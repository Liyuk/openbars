"""A-share (SSE / SZSE) instrument ids.

Format::

    cn:eq:<venue>:<code>            first listing of (venue, code)   cn:eq:xshg:600000
    cn:eq:<venue>:<code>:g<N>       N-th listing, N >= 2             cn:eq:xshg:600000:g2

* ``venue``: ``xshg`` (Shanghai incl. STAR Market) or ``xshe`` (Shenzhen incl. ChiNext).
  Beijing (``xbse``) is reserved and rejected for now.
* ``code``: exactly 6 ASCII digits, leading zeros kept. A bare code is never an id: the exchange
  is required (the same 6 digits may exist on different venues).
* ``asset_class`` is not part of the id.
* Code reuse: an id that has been retired is never handed to a new company. When the same
  (venue, code) lists again, the new id carries a generation suffix, starting at ``g2``.
  Generation 1 has no suffix (``g1`` is not a valid spelling).

Pure helpers; no listing ingest and no providers. Design:
https://github.com/Liyuk/openbars/discussions/54
"""

from __future__ import annotations

import re
from typing import NamedTuple

__all__ = [
    "CN_VENUES",
    "CNInstrumentId",
    "RESERVED_CN_VENUES",
    "cn_instrument_id",
    "normalize_cn_symbol",
    "parse_cn_instrument_id",
]

CN_VENUES = ("xshg", "xshe")
RESERVED_CN_VENUES = ("xbse",)  # Beijing: not implemented
_PREFIX = "cn:eq"
_CODE_RE = re.compile(r"[0-9]{6}")
_ID_RE = re.compile(r"cn:eq:([a-z0-9]+):([0-9]{6})(?::g([0-9]+))?")

# Spellings (case-insensitive) -> venue
_EXCHANGE_ALIASES = {
    "SH": "xshg",
    "SS": "xshg",
    "SSE": "xshg",
    "XSHG": "xshg",
    "SZ": "xshe",
    "SZSE": "xshe",
    "XSHE": "xshe",
}
_ALIAS_PATTERN = "|".join(sorted(_EXCHANGE_ALIASES, key=len, reverse=True))
_SUFFIX_RE = re.compile(rf"([0-9]{{6}})\.({_ALIAS_PATTERN})", re.IGNORECASE)
_PREFIX_DOT_RE = re.compile(rf"({_ALIAS_PATTERN})\.([0-9]{{6}})", re.IGNORECASE)
_PREFIX_RE = re.compile(rf"({_ALIAS_PATTERN})([0-9]{{6}})", re.IGNORECASE)


class CNInstrumentId(NamedTuple):
    venue: str
    code: str
    generation: int = 1


def _check_venue(venue: str) -> str:
    if not isinstance(venue, str) or venue not in CN_VENUES:
        hint = " (reserved, not supported yet)" if venue in RESERVED_CN_VENUES else ""
        raise ValueError(f"unsupported CN venue {venue!r}{hint}; expected one of {CN_VENUES}")
    return venue


def _check_code(code: str) -> str:
    if not isinstance(code, str) or not _CODE_RE.fullmatch(code):
        raise ValueError(f"CN code must be exactly 6 digits, got {code!r}")
    return code


def _check_generation(generation: int) -> int:
    if isinstance(generation, bool) or not isinstance(generation, int) or generation < 1:
        raise ValueError(f"generation must be an int >= 1, got {generation!r}")
    return generation


def cn_instrument_id(venue: str, code: str, generation: int = 1) -> str:
    """Build ``cn:eq:<venue>:<code>[:g<N>]``; ``generation`` 1 has no suffix."""
    _check_venue(venue)
    _check_code(code)
    _check_generation(generation)
    base = f"{_PREFIX}:{venue}:{code}"
    return base if generation == 1 else f"{base}:g{generation}"


def parse_cn_instrument_id(instrument_id: str) -> CNInstrumentId:
    """Inverse of :func:`cn_instrument_id`; raises ``ValueError`` on anything non-canonical."""
    m = _ID_RE.fullmatch(instrument_id) if isinstance(instrument_id, str) else None
    if m is None:
        raise ValueError(f"not a CN instrument id: {instrument_id!r}")
    venue, code, gen = m.group(1), m.group(2), m.group(3)
    _check_venue(venue)
    if gen is None:
        return CNInstrumentId(venue, code, 1)
    if gen.startswith("0") or int(gen) < 2:
        raise ValueError(f"generation suffix must be g2 or higher without leading zeros: {instrument_id!r}")
    return CNInstrumentId(venue, code, int(gen))


def normalize_cn_symbol(raw: str) -> tuple[str, str]:
    """Normalize a common spelling to ``(venue, code)``.

    Accepts ``600000.SH`` / ``600000.SS`` / ``sh.600000`` / ``SH600000`` (-> ``xshg``) and
    ``000001.SZ`` / ``000001.SZSE`` / ``sz.000001`` / ``SZ000001`` (-> ``xshe``), case-insensitive.
    A bare 6-digit code is rejected (the exchange is required).
    """
    if not isinstance(raw, str):
        raise ValueError(f"CN symbol must be a string, got {type(raw).__name__}")
    text = raw.strip()
    for pattern, code_group in ((_SUFFIX_RE, 1), (_PREFIX_DOT_RE, 2), (_PREFIX_RE, 2)):
        m = pattern.fullmatch(text)
        if m:
            code = m.group(code_group)
            exchange = m.group(3 - code_group)
            return _EXCHANGE_ALIASES[exchange.upper()], code
    if _CODE_RE.fullmatch(text):
        raise ValueError(f"bare code {raw!r} has no exchange; use e.g. {text}.SH or {text}.SZ")
    raise ValueError(f"unrecognized CN symbol: {raw!r}")
