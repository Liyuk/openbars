"""Identity evidence/resolver and security-type mapping (pure, synthetic)."""

from datetime import date, datetime, timedelta, timezone

import pytest

from openbars.core.models import AssetClass
from openbars.normalize.identity import (
    IdentityEvidence,
    IdentityResolver,
    IdentityStatus,
    is_symbol_fallback_identity,
)
from openbars.normalize.security_types import asset_class_for, security_type_for

T0 = datetime(2024, 1, 5, 12, 0, tzinfo=timezone.utc)


def ev(**kw) -> IdentityEvidence:
    base = dict(provider="prov_a", symbol="ZZZA", exchange="XNAS")
    base.update(kw)
    return IdentityEvidence(**base)


def test_symbol_only_and_issuer_only_are_unresolved():
    r = IdentityResolver.resolve(ev())
    assert (r.status, r.instrument_key, r.reason_code) == (
        IdentityStatus.UNRESOLVED, None, "symbol_only_evidence",
    )
    r = IdentityResolver.resolve(ev(cik="0000000001"))
    assert (r.status, r.reason_code) == (IdentityStatus.UNRESOLVED, "issuer_only_evidence")


def test_share_class_figi_or_native_id_forms_a_stable_key():
    r = IdentityResolver.resolve(ev(share_class_figi="TESTFIGI1"))
    assert (r.status, r.instrument_key, r.reason_code) == (
        IdentityStatus.PROVISIONAL, "figi:TESTFIGI1", "single_source_stable_identity",
    )
    r = IdentityResolver.resolve(ev(provider_instrument_id="native-1"))
    assert r.status is IdentityStatus.PROVISIONAL and r.instrument_key == "prov_a:native-1"
    # a share-class FIGI wins over the provider id
    r = IdentityResolver.resolve(ev(share_class_figi="TESTFIGI1", provider_instrument_id="native-1"))
    assert r.instrument_key == "figi:TESTFIGI1"


def test_symbol_fallback_id_cannot_claim_or_dispute():
    assert is_symbol_fallback_identity("symbol:XNAS:ZZZA", None)
    assert not is_symbol_fallback_identity("symbol:XNAS:ZZZA", "TESTFIGI1")
    assert not is_symbol_fallback_identity("native-1", None)
    r = IdentityResolver.resolve(
        ev(provider_instrument_id="symbol:XNAS:ZZZA", conflicting_instrument_keys=("figi:TESTFIGI9",))
    )
    assert r.status is IdentityStatus.UNRESOLVED and r.reason_code == "symbol_only_evidence"
    # ...but a FIGI behind a symbol-shaped id still counts
    r = IdentityResolver.resolve(ev(provider_instrument_id="symbol:XNAS:ZZZA", share_class_figi="TESTFIGI1"))
    assert r.status is IdentityStatus.PROVISIONAL


def test_conflict_is_disputed_and_corroboration_verifies():
    r = IdentityResolver.resolve(
        ev(share_class_figi="TESTFIGI1", conflicting_instrument_keys=("prov_b:native-9",),
           corroborating_providers=("prov_b",))
    )
    assert (r.status, r.reason_code) == (IdentityStatus.DISPUTED, "conflicting_stable_identity")
    r = IdentityResolver.resolve(ev(share_class_figi="TESTFIGI1", corroborating_providers=("prov_b",)))
    assert (r.status, r.reason_code) == (IdentityStatus.VERIFIED, "cross_source_stable_identity")


def test_not_yet_known_and_not_yet_valid():
    e = ev(share_class_figi="TESTFIGI1", observed_at=T0, effective_from=date(2024, 2, 1))
    r = IdentityResolver.resolve(e, as_known_at=T0 - timedelta(seconds=1))
    assert (r.status, r.reason_code) == (IdentityStatus.UNRESOLVED, "evidence_not_yet_known")
    assert IdentityResolver.resolve(e, as_known_at=T0).status is IdentityStatus.PROVISIONAL
    r = IdentityResolver.resolve(e, valid_on=date(2024, 1, 31))
    assert (r.status, r.reason_code) == (IdentityStatus.UNRESOLVED, "identity_not_yet_valid")
    assert IdentityResolver.resolve(e, valid_on=date(2024, 2, 1)).status is IdentityStatus.PROVISIONAL


def test_timestamps_must_be_timezone_aware():
    with pytest.raises(ValueError, match="observed_at"):
        ev(observed_at=datetime(2024, 1, 5, 12, 0))
    with pytest.raises(ValueError, match="as_known_at"):
        IdentityResolver.resolve(ev(share_class_figi="TESTFIGI1"), as_known_at=datetime(2024, 1, 5))
    with pytest.raises(ValueError):
        IdentityEvidence(provider="", symbol="ZZZA", exchange="XNAS")


def test_security_type_map():
    assert security_type_for("cs") == "common_stock"
    assert [security_type_for(t) for t in ("ETF", "ETN", "ETV")] == ["etf"] * 3
    assert security_type_for("ADRC") == "adr" and security_type_for("FUND") == "fund"
    assert security_type_for("NOPE") == "unknown" and security_type_for(None) == "unknown"
    assert asset_class_for("CS") is AssetClass.EQUITY
    assert asset_class_for("ETF") is AssetClass.ETF  # its own class, not a ticker list
    assert asset_class_for("ADRP") is AssetClass.ADR
    # classified but never an instrument
    for t in ("PFD", "WARRANT", "RIGHT", "UNIT", "SP", "NOPE", None):
        assert asset_class_for(t) is None
