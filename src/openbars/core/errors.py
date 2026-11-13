"""Unified provider error model (ported from the upstream internal implementation)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime


class OpenBarsError(Exception):
    """Base error."""


class ConfigError(OpenBarsError):
    pass


class ProviderError(OpenBarsError):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        retryable: bool,
        status_code: int | None = None,
        retry_at: datetime | None = None,
        request_id: str | None = None,
        raw_payload: str | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable
        self.status_code = status_code
        self.retry_at = retry_at
        self.request_id = request_id
        self.raw_payload = raw_payload  # redacted response body, if any


class QuotaExhausted(OpenBarsError):
    def __init__(self, provider: str, retry_at: datetime | None = None) -> None:
        super().__init__(f"quota exhausted for {provider}")
        self.provider = provider
        self.retry_at = retry_at

def provider_error_diagnostic(error: ProviderError, *, max_payload_chars: int = 512) -> str:
    """Return bounded provider context safe for a persisted job error message."""
    details = []
    if error.status_code is not None:
        details.append(f"HTTP {error.status_code}")
    if error.request_id:
        details.append(f"request_id={error.request_id[:128]}")
    message = str(error)
    if details:
        message = f"{message} ({'; '.join(details)})"
    if error.raw_payload:
        payload = error.raw_payload[:max_payload_chars]
        if len(error.raw_payload) > max_payload_chars:
            payload += "…"
        message = f"{message}; response={payload}"
    return message


def parse_retry_after(value: str | None, *, now: datetime | None = None) -> datetime | None:
    """Parse a ``Retry-After`` header (delta-seconds or HTTP-date) into an aware UTC instant.

    Returns None when the header is absent or malformed. A date in the past is clamped
    to ``now`` so callers never see a retry instant that has already gone by.
    """
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    current = (now or datetime.now(UTC)).astimezone(UTC)
    if text.isascii() and text.isdigit():
        return current + timedelta(seconds=int(text))
    try:
        moment = parsedate_to_datetime(text)
    except (TypeError, ValueError):
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    return max(moment.astimezone(UTC), current)
