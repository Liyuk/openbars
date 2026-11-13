"""Point-in-time instrument identity evidence and resolution.

A ticker alone, or an issuer id (CIK) alone, never identifies an instrument: tickers are
recycled by different companies and one issuer lists several share classes. Only
instrument-level evidence (a share-class FIGI or a provider-native instrument id) forms a
stable key. Ported from the upstream internal implementation, as plain dataclasses.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum

__all__ = [
    "IdentityEvidence",
    "IdentityResolution",
    "IdentityResolver",
    "IdentityStatus",
    "is_symbol_fallback_identity",
    "require_aware",
]

SYMBOL_FALLBACK_PREFIX = "symbol:"


class IdentityStatus(str, Enum):
    VERIFIED = "verified"
    PROVISIONAL = "provisional"
    UNRESOLVED = "unresolved"
    DISPUTED = "disputed"


def require_aware(value: datetime, name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")


def is_symbol_fallback_identity(provider_instrument_id: str | None, share_class_figi: str | None) -> bool:
    """A ``symbol:<exchange>:<ticker>`` id only restates the listing.

    It can neither claim nor dispute an instrument (unless a share-class FIGI backs it).
    """
    return (
        not share_class_figi
        and bool(provider_instrument_id)
        and str(provider_instrument_id).startswith(SYMBOL_FALLBACK_PREFIX)
    )


@dataclass(frozen=True)
class IdentityEvidence:
    provider: str
    symbol: str
    exchange: str
    cik: str | None = None
    provider_instrument_id: str | None = None
    share_class_figi: str | None = None
    composite_figi: str | None = None
    effective_from: date | None = None
    observed_at: datetime | None = None
    corroborating_providers: tuple[str, ...] = ()
    conflicting_instrument_keys: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.provider or not self.symbol or not self.exchange:
            raise ValueError("provider, symbol and exchange are required")
        if self.observed_at is not None:
            require_aware(self.observed_at, "observed_at")

    @property
    def stable_key(self) -> str | None:
        """Instrument-level key, or ``None`` when the evidence cannot claim an instrument."""
        if self.share_class_figi:
            return f"figi:{self.share_class_figi}"
        if self.provider_instrument_id and not is_symbol_fallback_identity(
            self.provider_instrument_id, self.share_class_figi
        ):
            return f"{self.provider}:{self.provider_instrument_id}"
        return None


@dataclass(frozen=True)
class IdentityResolution:
    status: IdentityStatus
    instrument_key: str | None
    reason_code: str
    evidence: IdentityEvidence


class IdentityResolver:
    """Resolve only on stable instrument-level evidence; ticker and CIK are insufficient."""

    @staticmethod
    def resolve(
        evidence: IdentityEvidence,
        *,
        as_known_at: datetime | None = None,
        valid_on: date | None = None,
    ) -> IdentityResolution:
        def out(status: IdentityStatus, key: str | None, reason: str) -> IdentityResolution:
            return IdentityResolution(status, key, reason, evidence)

        if as_known_at is not None:
            require_aware(as_known_at, "as_known_at")
            if evidence.observed_at is not None and evidence.observed_at > as_known_at:
                return out(IdentityStatus.UNRESOLVED, None, "evidence_not_yet_known")
        if valid_on is not None and evidence.effective_from and valid_on < evidence.effective_from:
            return out(IdentityStatus.UNRESOLVED, None, "identity_not_yet_valid")
        key = evidence.stable_key
        if key is None:
            reason = "issuer_only_evidence" if evidence.cik else "symbol_only_evidence"
            return out(IdentityStatus.UNRESOLVED, None, reason)
        if evidence.conflicting_instrument_keys:
            return out(IdentityStatus.DISPUTED, key, "conflicting_stable_identity")
        if evidence.corroborating_providers:
            return out(IdentityStatus.VERIFIED, key, "cross_source_stable_identity")
        return out(IdentityStatus.PROVISIONAL, key, "single_source_stable_identity")
