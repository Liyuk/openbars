"""Unified provider error model (ported in spirit from the equity backend)."""

from __future__ import annotations

from datetime import datetime


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
    ) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable
        self.status_code = status_code
        self.retry_at = retry_at
        self.request_id = request_id


class QuotaExhausted(OpenBarsError):
    def __init__(self, provider: str, retry_at: datetime | None = None) -> None:
        super().__init__(f"quota exhausted for {provider}")
        self.provider = provider
        self.retry_at = retry_at
