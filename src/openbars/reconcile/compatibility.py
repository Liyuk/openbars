"""Series compatibility gate: may two bar series stand in for each other?"""

from __future__ import annotations

from dataclasses import dataclass

from openbars.core.series import (
    PriceBasis,
    SessionSemantics,
    VolumeBasis,
    BarSeriesIdentity,
)

IDENTITY_MISMATCH = "identity_mismatch"
INTERVAL_MISMATCH = "interval_mismatch"
COVERAGE_MISMATCH = "coverage_mismatch"
SESSION_MISMATCH = "session_mismatch"
CURRENCY_MISMATCH = "currency_mismatch"
PRICE_BASIS_MISMATCH = "price_basis_mismatch"
ADJUSTMENT_CUTOFF_MISMATCH = "adjustment_cutoff_mismatch"
VOLUME_BASIS_MISMATCH = "volume_basis_mismatch"

_UNTRUSTED_PRICE_BASIS = {PriceBasis.UNKNOWN, PriceBasis.PROVIDER_DEFINED}
_UNTRUSTED_VOLUME_BASIS = {VolumeBasis.UNKNOWN, VolumeBasis.PROVIDER_DEFINED}


@dataclass(frozen=True)
class SeriesCompatibility:
    comparable: bool
    reason_code: str | None = None


def _is_iex(series: BarSeriesIdentity) -> bool:
    return series.provider == "alpaca" and series.feed == "iex"


def compare_series(
    left_instrument_id: str,
    left: BarSeriesIdentity,
    right_instrument_id: str,
    right: BarSeriesIdentity,
) -> SeriesCompatibility:
    """Rules are evaluated in order; the first failing rule gives the reason code."""
    if left_instrument_id != right_instrument_id:
        return SeriesCompatibility(False, IDENTITY_MISMATCH)
    if left.interval != right.interval:
        return SeriesCompatibility(False, INTERVAL_MISMATCH)
    # IEX covers only a slice of consolidated volume; it never fills the canonical series.
    if _is_iex(left) != _is_iex(right):
        return SeriesCompatibility(False, COVERAGE_MISMATCH)
    if (
        left.session_semantics != right.session_semantics
        or SessionSemantics.UNKNOWN in (left.session_semantics, right.session_semantics)
    ):
        return SeriesCompatibility(False, SESSION_MISMATCH)
    if left.currency != right.currency:
        return SeriesCompatibility(False, CURRENCY_MISMATCH)
    if left.price_basis != right.price_basis or (
        left.price_basis in _UNTRUSTED_PRICE_BASIS or right.price_basis in _UNTRUSTED_PRICE_BASIS
    ):
        return SeriesCompatibility(False, PRICE_BASIS_MISMATCH)
    if (
        left.adjustment_scope != right.adjustment_scope
        or left.adjustment_as_of != right.adjustment_as_of
    ):
        return SeriesCompatibility(False, ADJUSTMENT_CUTOFF_MISMATCH)
    if left.volume_basis != right.volume_basis or (
        left.volume_basis in _UNTRUSTED_VOLUME_BASIS
        or right.volume_basis in _UNTRUSTED_VOLUME_BASIS
    ):
        return SeriesCompatibility(False, VOLUME_BASIS_MISMATCH)
    return SeriesCompatibility(True, None)
