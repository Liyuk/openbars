"""``crypto-daily-v1``: the crypto daily merge/reconcile profile.

Reconcile is always per ``instrument_id`` and an id ``crypto:spot:<venue>:<BASE>-<QUOTE>``
belongs to exactly one venue. The same base/quote on two venues are two instruments: their
bars are never compared, never fill each other's gaps and never count as independent
corroboration. Only sources of the *same* venue arbitrate (primary first, then same-venue
secondaries). Disagreement beyond tolerance is flagged ``divergent``; values are never averaged.

Tolerances (``Decimal`` percent) are wider than the US ``research-daily-v2`` profile
(0.5 / 5): two feeds of the same pair (live klines vs. a re-published archive) can differ more
than SIP vs. an official US close, e.g. late trades or rounding of thin pairs. 1% price / 10%
volume is the line past which a same-instrument disagreement is surfaced.

``CRYPTO_DAILY_V1`` is an alias of the Binance-venue policy; use ``policy_for_instrument`` for
the venue-correct policy. This module does not touch ``DEFAULT_POLICY`` / ``KNOWN_SOURCES``.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal

from openbars.calendars.crypto import CryptoCalendar
from openbars.core.models import Market
from openbars.core.series import (
    BINANCE_ARCHIVE_RAW,
    BINANCE_SPOT_RAW,
    COINBASE_EXCHANGE_RAW,
    SourceIdentity,
)
from openbars.normalize.crypto_pairs import parse_crypto_instrument_id
from openbars.reconcile.policy import ReconcilePolicy
from openbars.reconcile.projection import SourceObservation
from openbars.reconcile.reconciler import MemoryReconciler, ReconcileResult
from openbars.reconcile.store import CanonicalRowStore

POLICY_VERSION_CRYPTO_DAILY_V1 = "crypto-daily-v1"
POLICY_VERSION_CN_DAILY_V1 = "cn-daily-v1"  # A-share: stored RAW, QFQ/HFQ derived (normalize.cn_adjust)
DEFAULT_POLICY_VERSION = "research-daily-v2"  # US default; same value as ``storage.schema``

CRYPTO_PRICE_TOLERANCE_PCT = Decimal("1")
CRYPTO_VOLUME_TOLERANCE_PCT = Decimal("10")
CRYPTO_CORROBORATION_QUORUM = 2  # only same-venue independent bars could count; none exist yet
CRYPTO_COMPLETION_WAIT_MINUTES = 30  # UTC midnight + 30 (via CryptoCalendar)

REJECT_CROSS_VENUE = "cross_venue"
REJECT_OTHER_INSTRUMENT = "other_instrument"

# venue -> (primary, same-venue secondaries)
_VENUE_SOURCES: dict[str, tuple[SourceIdentity, tuple[SourceIdentity, ...]]] = {
    "binance": (BINANCE_SPOT_RAW, (BINANCE_ARCHIVE_RAW,)),
    "coinbase": (COINBASE_EXCHANGE_RAW, ()),
}


@dataclass(frozen=True)
class RejectedCryptoObservation:
    observation: SourceObservation
    reason: str  # ``cross_venue`` or ``other_instrument``


def instrument_venue(instrument_id: str | None) -> str | None:
    """Venue slug of a ``crypto:spot:<venue>:<BASE>-<QUOTE>`` id; ``None`` for anything else."""
    if not instrument_id:
        return None
    try:
        return parse_crypto_instrument_id(instrument_id).venue
    except ValueError:
        return None


def source_venue(identity: SourceIdentity) -> str | None:
    """Venue a source belongs to (``binance/*`` -> ``binance``); ``None`` if not attachable."""
    return identity.provider if identity.provider in _VENUE_SOURCES else None


def policy_version_for_market(
    market: Market | str | None = None,
    override: str | None = None,
    *,
    instrument_ids: Iterable[str] = (),
) -> str:
    """The canonical ``policy_version`` a read should use.

    ``override`` always wins. Otherwise ``Market.CRYPTO`` (or any ``crypto:`` instrument id)
    -> ``crypto-daily-v1``; ``Market.CN`` (or any ``cn:`` id) -> ``cn-daily-v1``; everything
    else (US, unknown) -> ``research-daily-v2``.
    """
    if override:
        return override
    if market is not None and Market(market) is Market.CRYPTO:
        return POLICY_VERSION_CRYPTO_DAILY_V1
    if market is not None and Market(market) is Market.CN:
        return POLICY_VERSION_CN_DAILY_V1
    ids = list(instrument_ids)
    if any(instrument_venue(i) is not None for i in ids):
        return POLICY_VERSION_CRYPTO_DAILY_V1
    if any(i.startswith("cn:") for i in ids):
        return POLICY_VERSION_CN_DAILY_V1
    return DEFAULT_POLICY_VERSION


def policy_for_instrument(instrument_id: str) -> ReconcilePolicy:
    """The ``crypto-daily-v1`` policy for one instrument's venue.

    Raises ``ValueError`` for a non-crypto id or a venue without a registered primary source.
    """
    venue = instrument_venue(instrument_id)
    if venue is None:
        raise ValueError(f"not a crypto pair instrument id: {instrument_id!r}")
    if venue not in _VENUE_SOURCES:
        raise ValueError(f"no crypto-daily-v1 source for venue {venue!r}")
    primary, secondaries = _VENUE_SOURCES[venue]
    return ReconcilePolicy(
        policy_version=POLICY_VERSION_CRYPTO_DAILY_V1,
        primary=primary,
        secondary_sources=secondaries,
        fill_sources=(),
        price_tolerance_pct=CRYPTO_PRICE_TOLERANCE_PCT,
        volume_tolerance_pct=CRYPTO_VOLUME_TOLERANCE_PCT,
        corroboration_quorum=CRYPTO_CORROBORATION_QUORUM,
        volume_priority=(primary, *secondaries),
        completion_wait_minutes=CRYPTO_COMPLETION_WAIT_MINUTES,
    )


CRYPTO_DAILY_V1 = policy_for_instrument("crypto:spot:binance:TEST-USDT")
"""Alias: the Binance-venue ``crypto-daily-v1`` policy (Coinbase differs only in sources)."""


def reject_cross_venue(
    observations: Iterable[SourceObservation], instrument_id: str
) -> tuple[list[SourceObservation], list[RejectedCryptoObservation]]:
    """Keep only bars of ``instrument_id`` produced by that instrument's own venue.

    Rejected: bars of another instrument (``cross_venue`` when that instrument sits on a
    different venue, else ``other_instrument``) and bars of the target instrument whose source
    belongs to a different venue or is unknown (``cross_venue``).
    """
    venue = instrument_venue(instrument_id)
    if venue is None:
        raise ValueError(f"not a crypto pair instrument id: {instrument_id!r}")
    kept: list[SourceObservation] = []
    rejected: list[RejectedCryptoObservation] = []
    for obs in observations:
        if obs.instrument_id != instrument_id:
            other = instrument_venue(obs.instrument_id)
            reason = REJECT_CROSS_VENUE if other != venue else REJECT_OTHER_INSTRUMENT
            rejected.append(RejectedCryptoObservation(obs, reason))
        elif source_venue(obs.identity) != venue:
            rejected.append(RejectedCryptoObservation(obs, REJECT_CROSS_VENUE))
        else:
            kept.append(obs)
    return kept, rejected


@dataclass(frozen=True)
class CryptoReconcileResult:
    result: ReconcileResult
    cross_venue_rejected: tuple[RejectedCryptoObservation, ...] = ()


def reconcile_crypto(
    instrument_id: str,
    start: date,
    end: date,
    observations: Iterable[SourceObservation],
    *,
    generated_at: datetime | None = None,
    store: CanonicalRowStore | None = None,
) -> CryptoReconcileResult:
    """Reconcile one crypto instrument over ``[start, end]`` (UTC dates, 24/7).

    Cross-venue / foreign-instrument bars are filtered out first and never reach selection;
    the rest goes through ``MemoryReconciler`` under ``policy_for_instrument``.
    """
    policy = policy_for_instrument(instrument_id)
    kept, rejected = reject_cross_venue(observations, instrument_id)
    reconciler = MemoryReconciler(store, policy=policy, calendar=CryptoCalendar())
    result = reconciler.reconcile_range(
        instrument_id, start, end, kept, generated_at=generated_at
    )
    return CryptoReconcileResult(result, tuple(rejected))


__all__ = [
    "CRYPTO_DAILY_V1",
    "CryptoReconcileResult",
    "POLICY_VERSION_CN_DAILY_V1",
    "POLICY_VERSION_CRYPTO_DAILY_V1",
    "REJECT_CROSS_VENUE",
    "REJECT_OTHER_INSTRUMENT",
    "RejectedCryptoObservation",
    "instrument_venue",
    "policy_for_instrument",
    "policy_version_for_market",
    "reconcile_crypto",
    "reject_cross_venue",
    "source_venue",
]
