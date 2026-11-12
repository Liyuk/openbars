"""Base class for HTTP-backed providers: quota gate, hooks, error mapping, redaction."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from typing import Any

import httpx

from openbars.core.errors import ProviderError
from openbars.providers.http import (
    json_body,
    provider_error_from_response,
    redact_response_text,
)
from openbars.quota.gate import Lease, QuotaGate, QuotaUsage

DEFAULT_TIMEOUT_SECONDS = 30.0


class NullQuotaGate:
    """A gate that always grants. For tests and local use; not a rate limiter."""

    def acquire(
        self, provider: str, weight: int = 1, *, scope: str = "default", timeout: float | None = None
    ) -> Lease:
        return Lease(provider=provider, weight=weight, granted_at=datetime.now(timezone.utc))

    def penalize(self, provider: str, until: datetime) -> None:
        return None

    def snapshot(self) -> Mapping[str, QuotaUsage]:
        return {}


class HttpProviderAdapter:
    """Shared plumbing for providers that talk HTTP via ``httpx``.

    Every request first takes a quota token (``quota_gate.acquire``) and then runs the
    optional ``before_request`` hook. Subclasses supply ``provider_id`` and override
    ``secret_values`` so credentials never reach messages or ``raw_payload``.
    """

    provider_id: str = "http"

    def __init__(
        self,
        *,
        client: httpx.Client | None = None,
        quota_gate: QuotaGate | None = None,
        before_request: Callable[[], None] | None = None,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        self._client = client
        self._owns_client = client is None
        self._quota_gate: QuotaGate = quota_gate if quota_gate is not None else NullQuotaGate()
        self._before_request = before_request
        self._timeout = timeout

    # -- credentials / redaction -------------------------------------------------
    def secret_values(self) -> tuple[str, ...]:
        return ()

    def redact(self, text: str) -> str:
        return redact_response_text(text, *self.secret_values())

    # -- error mapping -----------------------------------------------------------
    def http_error(self, response: httpx.Response) -> ProviderError:
        return provider_error_from_response(response, secret_values=self.secret_values())

    def error_for(self, exc: Exception) -> ProviderError:
        """Map a transport-level exception (timeouts vs. everything else)."""
        if isinstance(exc, httpx.TimeoutException):
            code, label = "timeout", "request timed out"
        else:
            code, label = "transport_error", f"transport error ({type(exc).__name__})"
        return ProviderError(code, self.redact(label), retryable=True)

    def json_object(self, response: httpx.Response) -> Mapping[str, Any]:
        return json_body(response)

    # -- requests ----------------------------------------------------------------
    def _http(self) -> httpx.Client:
        if self._client is None:
            self._client = httpx.Client(timeout=self._timeout)
        return self._client

    def close(self) -> None:
        if self._owns_client and self._client is not None:
            self._client.close()
            self._client = None

    def get(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        weight: int = 1,
        scope: str = "default",
    ) -> httpx.Response:
        """Acquire quota, run the hook, GET, and raise ``ProviderError`` on failure."""
        self._quota_gate.acquire(self.provider_id, weight, scope=scope)
        if self._before_request is not None:
            self._before_request()
        try:
            response = self._http().get(url, params=params, headers=headers)
        except httpx.HTTPError as exc:
            raise self.error_for(exc) from None
        if response.status_code >= 400:
            raise self.http_error(response)
        return response
