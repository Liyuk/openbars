"""Generic HTTP helpers shared by provider adapters: payload parsing and error mapping.

Everything here is provider-agnostic. Numbers are parsed via ``str`` so that no float
rounding ever reaches a ``Decimal``; credentials are redacted before any text is kept.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx

from openbars.core.errors import ProviderError

REDACTED = "[REDACTED]"
MIN_REDACTED_SECRET_LENGTH = 8
MAX_RAW_PAYLOAD_CHARS = 2000


def digest_payload(payload: Any) -> str:
    """Stable sha256 hex digest of a JSON-compatible payload."""
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _invalid(message: str) -> ProviderError:
    return ProviderError("invalid_payload", message, retryable=False)


def decimal_value(value: Any, field: str = "value") -> Decimal:
    """Parse a JSON number/string into a finite ``Decimal`` (never through float)."""
    if isinstance(value, bool) or value is None:
        raise _invalid(f"{field} is not a number")
    if isinstance(value, float):
        text = repr(value)
    elif isinstance(value, (int, Decimal, str)):
        text = str(value).strip()
    else:
        raise _invalid(f"{field} is not a number")
    try:
        result = Decimal(text)
    except InvalidOperation:
        raise _invalid(f"{field} is not a number") from None
    if not result.is_finite():
        raise _invalid(f"{field} is not finite")
    return result


def integer_value(value: Any, field: str = "value") -> int:
    """Parse a JSON integer (or an integral number/string) into ``int``."""
    number = decimal_value(value, field)
    if number != number.to_integral_value():
        raise _invalid(f"{field} is not an integer")
    return int(number)


def require_object(value: Any, field: str = "payload") -> dict[str, Any]:
    if not isinstance(value, dict):
        raise _invalid(f"{field} is not a JSON object")
    return value


def redact_response_text(text: str, *secrets: str | None) -> str:
    """Replace each secret of length >= 8 with ``[REDACTED]`` (shorter ones would mangle text)."""
    for secret in sorted({s for s in secrets if s}, key=len, reverse=True):
        if len(secret) >= MIN_REDACTED_SECRET_LENGTH:
            text = text.replace(secret, REDACTED)
    return text


def provider_error_from_response(
    response: httpx.Response, *, secret_values: tuple[str, ...] | list[str] = ()
) -> ProviderError:
    """Map a non-success HTTP response to a ``ProviderError``."""
    status = response.status_code
    if status == 429:
        code, retryable = "rate_limited", True
    elif status >= 500:
        code, retryable = "provider_unavailable", True
    else:
        code, retryable = "provider_rejected_request", False
    try:
        body = response.text
    except Exception:  # undecodable body
        body = ""
    raw = redact_response_text(body, *secret_values)[:MAX_RAW_PAYLOAD_CHARS]
    return ProviderError(
        code,
        f"provider responded with HTTP {status}",
        retryable=retryable,
        status_code=status,
        request_id=response.headers.get("x-request-id"),
        raw_payload=raw,
    )


class TokenLoopGuard:
    """Rejects empty, non-string and already-seen pagination tokens."""

    def __init__(self, initial: str | None = None) -> None:
        self._seen: set[str] = set()
        if initial:
            self._seen.add(initial)

    def accept(self, token: object) -> str:
        if not isinstance(token, str) or not token:
            raise ProviderError(
                "pagination_loop", "pagination token is empty or not a string", retryable=False
            )
        if token in self._seen:
            raise ProviderError(
                "pagination_loop", "pagination token repeated", retryable=False
            )
        self._seen.add(token)
        return token


def json_body(response: httpx.Response) -> Mapping[str, Any]:
    """Decode a response body as a JSON object or raise ``invalid_payload``."""
    try:
        # Floats become Decimal at parse time so numbers never round-trip through float.
        data = json.loads(response.content, parse_float=Decimal)
    except ValueError:
        raise _invalid("response body is not valid JSON") from None
    return require_object(data, "response")
