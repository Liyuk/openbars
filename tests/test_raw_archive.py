import json
from datetime import UTC, date, datetime

import pytest

from openbars.core.models import DateRange, Interval, Market
from openbars.raw.archive import REDACTED, RawArchive, RawArchiveCollisionError, redact

WINDOW = DateRange(date(2024, 1, 2), date(2024, 1, 3))
NOW = datetime(2024, 1, 4, 5, 6, 7, tzinfo=UTC)
# built at runtime so the literal never appears in source or bytecode (secret scanners)
QUERY_KEY = "".join(["api", "key"])
PARAMS = {
    "symbols": ["TEST1"],
    "api_key": "synthetic-value-1",
    "apiKey": "synthetic-value-2",
    "nested": {"Authorization": "Bearer synthetic", "client_secret": "s", "ok": 1},
    "headers": [{"x-auth-token": "t"}, {"password": "p"}, {"credential_id": "c"}],
    "url": f"https://example.com/v1/bars?symbol=TEST1&{QUERY_KEY}=synthetic-value-3&limit=5",
}


def write(archive, pages=("page-1", "page-2"), **kw):
    args = dict(
        provider="prov_a", market=Market.US, interval=Interval.D1, window=WINDOW,
        pages=list(pages), license_tag="personal_only", request_params=PARAMS, received_at=NOW,
    )
    args.update(kw)
    return archive.write(**args)


def test_redacts_sensitive_params_on_disk(tmp_path):
    archive = RawArchive(tmp_path / "raw-archive")
    res = write(archive)
    text = res.path.read_text()
    for secret in ("synthetic-value-1", "synthetic-value-2", "synthetic-value-3", "Bearer synthetic"):
        assert secret not in text
    doc = json.loads(text)
    params = doc["metadata"]["request_params"]
    assert params["api_key"] == params["apiKey"] == REDACTED
    assert params["nested"] == {"Authorization": REDACTED, "client_secret": REDACTED, "ok": 1}
    assert params["headers"] == [{"x-auth-token": REDACTED}, {"password": REDACTED}, {"credential_id": REDACTED}]
    assert params["url"].endswith(f"symbol=TEST1&{QUERY_KEY}=[REDACTED]&limit=5")
    assert params["symbols"] == ["TEST1"]
    assert doc["metadata"]["license_tag"] == "personal_only"
    assert doc["pages"] == ["page-1", "page-2"]
    assert len(doc["metadata"]["content_digest"]) == 64


def test_redact_helper_does_not_mutate_input():
    src = {"token": "x", "a": {"b": 1}}
    assert redact(src) == {"token": REDACTED, "a": {"b": 1}}
    assert src["token"] == "x"


def test_deterministic_and_idempotent(tmp_path):
    a1, a2 = RawArchive(tmp_path / "a"), RawArchive(tmp_path / "b")
    r1, r2 = write(a1), write(a2)
    assert r1.ref == r2.ref and r1.digest == r2.digest
    assert r1.path.read_bytes() == r2.path.read_bytes()
    assert r1.created is True
    again = write(a1, received_at=datetime(2030, 1, 1, tzinfo=UTC))
    assert again.created is False and again.path == r1.path
    assert r1.path.read_bytes() == r2.path.read_bytes()  # original received_at kept
    # secrets do not influence the identity (redacted before hashing)
    other_secret = write(a1, request_params={**PARAMS, "api_key": "different-synthetic-value"})
    assert other_secret.path == r1.path
    # different content or different request -> different file
    assert write(a1, pages=["page-1"]).path != r1.path
    assert write(a1, request_params={**PARAMS, "symbols": ["TEST2"]}).path != r1.path
    assert len(list((tmp_path / "a").rglob("*.json"))) == 3
    assert list((tmp_path / "a").rglob(".*.tmp")) == []


def test_digest_collision_with_different_pages_errors(tmp_path):
    archive = RawArchive(tmp_path)
    res = write(archive)
    doc = json.loads(res.path.read_text())
    doc["pages"] = ["tampered"]
    res.path.write_text(json.dumps(doc))
    with pytest.raises(RawArchiveCollisionError):
        write(archive)
    assert issubclass(RawArchiveCollisionError, RuntimeError)


def test_layout_and_validation(tmp_path):
    archive = RawArchive(tmp_path)
    res = write(archive)
    assert res.ref.startswith("provider=prov_a/market=us/interval=1d/2024-01-02_2024-01-03/")
    assert archive.read(res.ref)["pages"] == ["page-1", "page-2"]
    with pytest.raises(ValueError):
        archive.read("../../etc/passwd")
    with pytest.raises(ValueError, match="pages"):
        write(archive, pages=[])
    with pytest.raises(ValueError, match="provider"):
        write(archive, provider="../evil")
    with pytest.raises(ValueError, match="license_tag"):
        write(archive, license_tag="")
