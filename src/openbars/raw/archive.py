"""Raw response archive: immutable, content-addressed JSON sidecars.

Layout (relative to the archive root, normally ``data/raw-archive``)::

    provider=<p>/market=<m>/interval=<i>/<window>/<request_digest>-<content_digest>.json

* ``content_digest`` is the SHA-256 of the response pages; ``request_digest`` covers the
  *redacted* request identity. The same request + same content always maps to the same
  file, and writing it again is a no-op (the original ``received_at`` is kept).
* If the target exists but its pages differ (digest collision / tampering) writing
  raises ``RawArchiveCollisionError``.
* Request params are redacted before they reach disk: any key containing ``api_key``,
  ``apikey``, ``secret``, ``token``, ``password``, ``authorization`` or ``credential``
  becomes ``[REDACTED]`` (recursively), and credential-looking ``name=value`` pairs inside
  string values are masked. Response *pages* are stored verbatim - providers must not
  echo credentials, and ``license_tag`` records the terms the payload falls under.
* Writes are atomic: temp file in the same directory, fsync, exclusive ``os.link``.

Ported in spirit from the upstream internal implementation's raw archive (not copied).
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from openbars.core.atomic import atomic_write
from openbars.core.errors import OpenBarsError
from openbars.storage.errors import ReadOnlyError
from openbars.core.models import DateRange, Interval, Market

SENSITIVE_KEY_PARTS = (
    "api_key",
    "apikey",
    "secret",
    "token",
    "password",
    "authorization",
    "credential",
)
REDACTED = "[REDACTED]"
_SAFE_SEGMENT = re.compile(r"^[A-Za-z0-9_.-]+$")
_INLINE_SECRET = re.compile(
    r"(?i)((?:api[_-]?key|apikey|secret|token|password|authorization|credential)[A-Za-z0-9_-]*=)[^&\s\"']+"
)


class RawArchiveCollisionError(OpenBarsError, RuntimeError):
    """Same archive file name, different content."""


@dataclass(frozen=True)
class ArchiveResult:
    path: Path
    ref: str  # posix path relative to the archive root; stored in source_artifact_ref
    digest: str  # SHA-256 of the response pages
    created: bool


def redact(value: Any) -> Any:
    """Recursively redact sensitive keys (and inline ``key=value`` secrets in strings)."""
    if isinstance(value, Mapping):
        return {
            str(key): REDACTED
            if any(part in str(key).lower() for part in SENSITIVE_KEY_PARTS)
            else redact(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [redact(item) for item in value]
    if isinstance(value, str):
        return _INLINE_SECRET.sub(lambda m: m.group(1) + REDACTED, value)
    return value


def _digest(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


class RawArchive:
    def __init__(self, root: str | Path, *, read_only: bool = False) -> None:
        self.root = Path(root)
        self.read_only = read_only

    def write(
        self,
        *,
        provider: str,
        market: Market,
        interval: Interval,
        window: DateRange,
        pages: Sequence[str],
        license_tag: str,
        request_params: Mapping[str, Any] | None = None,
        feed: str = "",
        adjustment: str = "raw",
        request_id: str | None = None,
        payload_capture_kind: str = "provider_response",
        complete: bool = True,
        error_code: str | None = None,
        received_at: datetime | None = None,
    ) -> ArchiveResult:
        if self.read_only:
            raise ReadOnlyError("backend is read-only; raw archive writes are disabled")
        if not pages:
            raise ValueError("raw provider response pages are required before ingestion")
        if not license_tag:
            raise ValueError("license_tag is required")
        market, interval = Market(market), Interval(interval)
        if not _SAFE_SEGMENT.fullmatch(provider):
            raise ValueError("provider must be a safe path segment")
        content_digest = _digest("\n".join(pages))
        safe_params = redact(dict(request_params or {}))
        identity = {
            "provider": provider,
            "market": market.value,
            "interval": interval.value,
            "feed": feed,
            "adjustment": adjustment,
            "start_date": window.start.isoformat(),
            "end_date": window.end.isoformat(),
            "request_params": safe_params,
        }
        request_digest = _digest(json.dumps(identity, sort_keys=True, separators=(",", ":")))
        span = window.start.isoformat()
        if window.end != window.start:
            span += f"_{window.end.isoformat()}"
        directory = (
            self.root
            / f"provider={provider}"
            / f"market={market.value}"
            / f"interval={interval.value}"
            / span
        )
        target = directory / f"{request_digest}-{content_digest}.json"
        ref = target.relative_to(self.root).as_posix()
        page_list = list(pages)

        def check_existing() -> ArchiveResult:
            existing = json.loads(target.read_text(encoding="utf-8"))
            if existing.get("pages") != page_list:
                raise RawArchiveCollisionError(f"raw content digest collision at {ref}")
            return ArchiveResult(target, ref, content_digest, created=False)

        if target.exists():
            return check_existing()

        metadata = {
            **identity,
            "request_id": request_id,
            "license_tag": license_tag,
            "received_at": (received_at or datetime.now(UTC)).astimezone(UTC).isoformat(),
            "complete": complete,
            "error_code": error_code,
            "payload_capture_kind": payload_capture_kind,
            "content_digest": content_digest,
        }
        body = json.dumps({"metadata": metadata, "pages": page_list}, sort_keys=True, indent=2) + "\n"
        try:
            atomic_write(target, lambda fh: fh.write(body.encode("utf-8")), exclusive=True)
        except FileExistsError:
            return check_existing()
        return ArchiveResult(target, ref, content_digest, created=True)

    def read(self, ref: str) -> dict[str, Any]:
        path = (self.root / ref).resolve()
        if self.root.resolve() not in path.parents:
            raise ValueError("ref escapes the archive root")
        return json.loads(path.read_text(encoding="utf-8"))
