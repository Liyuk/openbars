"""Hong Kong (HKEX) instrument ids and ADR cross-references.

Format::

    hk:eq:xhkg:<code>            first listing of the 5-digit code   hk:eq:xhkg:00700
    hk:eq:xhkg:<code>:g<N>       N-th listing, N >= 2               hk:eq:xhkg:00700:g2

* ``venue`` is ``xhkg`` (Hong Kong Exchange, including GEM and Main Board).
* ``code`` is exactly 5 ASCII digits, leading zeros kept (``00700``, ``09988``).
* ``asset_class`` is not part of the id. An ADR listed in the US is a *different*
  instrument (typically ``us:eq:…`` with ``asset_class=adr``); the link lives in
  ``Instrument.external_ids`` (see ``HK_ADR_KEY`` / ``US_HK_ORDINARY_KEY``).
* Code reuse: a retired id is never handed to a new company. When the same
  5-digit code lists again, the new id carries a generation suffix starting at
  ``g2``. Generation 1 has no suffix (``g1`` is not a valid spelling).

Pure helpers; no listing ingest and no providers.
Design: https://github.com/Liyuk/openbars/discussions/68
"""

from __future__ import annotations

import re
from typing import NamedTuple

__all__ = [
    "HK_ADR_KEY",
    "HK_VENUES",
    "US_HK_ORDINARY_KEY",
    "HKInstrumentId",
    "hk_adr_external_ids",
    "hk_instrument_id",
    "normalize_hk_symbol",
    "parse_hk_instrument_id",
    "us_hk_ordinary_external_ids",
]

HK_VENUES = ("xhkg",)
_PREFIX = "hk:eq"
_CODE_RE = re.compile(r"[0-9]{5}")
_ID_RE = re.compile(r"hk:eq:([a-z0-9]+):([0-9]{5})(?::g([0-9]+))?")

# Spellings (case-insensitive) -> venue
_EXCHANGE_ALIASES = {
    "HK": "xhkg",
    "HKG": "xhkg",
    "HKEX": "xhkg",
    "XHKG": "xhkg",
}
_ALIAS_PATTERN = "|".join(sorted(_EXCHANGE_ALIASES, key=len, reverse=True))
_SUFFIX_RE = re.compile(rf"([0-9]{{1,5}})\.({_ALIAS_PATTERN})", re.IGNORECASE)
_PREFIX_DOT_RE = re.compile(rf"({_ALIAS_PATTERN})\.([0-9]{{1,5}})", re.IGNORECASE)
_PREFIX_RE = re.compile(rf"({_ALIAS_PATTERN})([0-9]{{5}})", re.IGNORECASE)

# Cross-listing keys on Instrument.external_ids
HK_ADR_KEY = "us_adr"  # on the HK ordinary: value is the US ADR instrument id
US_HK_ORDINARY_KEY = "hk_ordinary"  # on the US ADR: value is the HK ordinary id


class HKInstrumentId(NamedTuple):
    venue: str
    code: str
    generation: int = 1


def _check_venue(venue: str) -> str:
    if not isinstance(venue, str) or venue not in HK_VENUES:
        raise ValueError(f"unsupported HK venue {venue!r}; expected one of {HK_VENUES}")
    return venue


def _pad_code(code: str) -> str:
    if (
        not isinstance(code, str)
        or not code.isascii()
        or not code.isdigit()
        or not (1 <= len(code) <= 5)
    ):
        raise ValueError(f"HK code must be 1 to 5 digits, got {code!r}")
    return code.zfill(5)


def _check_code(code: str) -> str:
    if not isinstance(code, str) or not _CODE_RE.fullmatch(code):
        raise ValueError(f"HK code must be exactly 5 digits, got {code!r}")
    return code


def _check_generation(generation: int) -> int:
    if (
        isinstance(generation, bool)
        or not isinstance(generation, int)
        or generation < 1
    ):
        raise ValueError(f"generation must be an int >= 1, got {generation!r}")
    return generation


def hk_instrument_id(venue: str, code: str, generation: int = 1) -> str:
    venue = _check_venue(venue.strip().lower())
    code = _pad_code(code.strip())
    generation = _check_generation(generation)
    base = f"{_PREFIX}:{venue}:{code}"
    return base if generation == 1 else f"{base}:g{generation}"


def parse_hk_instrument_id(instrument_id: str) -> HKInstrumentId:
    if not isinstance(instrument_id, str):
        raise ValueError(f"HK instrument id must be a string, got {instrument_id!r}")  # noqa: TRY004
    m = _ID_RE.fullmatch(instrument_id)
    if not m:
        raise ValueError(f"not a valid HK instrument id: {instrument_id!r}")
    venue, code, gen = m.group(1), m.group(2), m.group(3)
    _check_venue(venue)
    if gen is not None and gen.startswith("0"):
        raise ValueError(f"not a valid HK instrument id: {instrument_id!r}")
    generation = int(gen) if gen is not None else 1
    if generation == 1 and gen is not None:
        raise ValueError(f"generation 1 must be un-suffixed, got {instrument_id!r}")
    if generation < 1:
        raise ValueError(f"not a valid HK instrument id: {instrument_id!r}")
    return HKInstrumentId(venue, code, generation)


def normalize_hk_symbol(symbol: str) -> tuple[str, str]:
    """Return ``(venue, 5-digit code)`` for a display ticker.

    Accepts ``00700``, ``700``, ``0700.HK``, ``HK.00700``, ``HK00700``, ``xhkg:00700``.
    A single HK venue means a bare 1–5 digit code is unambiguous (unlike A-shares).
    """
    if not isinstance(symbol, str) or not symbol.strip():
        raise ValueError(f"HK symbol must be a non-empty string, got {symbol!r}")
    raw = symbol.strip()
    lower = raw.lower()
    if lower.startswith("hk:eq:"):
        parsed = parse_hk_instrument_id(raw)
        return parsed.venue, parsed.code
    if ":" in raw:
        venue, _, code = raw.partition(":")
        return _check_venue(venue.strip().lower()), _pad_code(code.strip())
    m = _SUFFIX_RE.fullmatch(raw)
    if m:
        return _EXCHANGE_ALIASES[m.group(2).upper()], _pad_code(m.group(1))
    m = _PREFIX_DOT_RE.fullmatch(raw)
    if m:
        return _EXCHANGE_ALIASES[m.group(1).upper()], _pad_code(m.group(2))
    m = _PREFIX_RE.fullmatch(raw)
    if m:
        return _EXCHANGE_ALIASES[m.group(1).upper()], _pad_code(m.group(2))
    if raw.isdigit() and 1 <= len(raw) <= 5:
        return "xhkg", _pad_code(raw)
    raise ValueError(f"cannot normalize HK symbol {symbol!r}")


def hk_adr_external_ids(us_instrument_id: str) -> dict[str, str]:
    """``external_ids`` fragment to put on the Hong Kong ordinary share."""
    if not isinstance(us_instrument_id, str) or not us_instrument_id.strip():
        raise ValueError(
            f"US ADR instrument id must be a non-empty string, got {us_instrument_id!r}"
        )
    return {HK_ADR_KEY: us_instrument_id.strip()}


def us_hk_ordinary_external_ids(hk_id: str) -> dict[str, str]:
    """``external_ids`` fragment to put on the US ADR; ``hk_id`` must be a valid HK id."""
    parse_hk_instrument_id(hk_id)
    return {US_HK_ORDINARY_KEY: hk_id}
