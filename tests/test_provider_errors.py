from datetime import UTC, datetime, timedelta

import httpx

from openbars.core.errors import (
    ProviderError,
    parse_retry_after,
    provider_error_diagnostic,
)
from openbars.providers.adapter import HttpProviderAdapter, NullQuotaGate
from openbars.providers.http import provider_error_from_response

NOW = datetime(2026, 10, 15, 12, 0, tzinfo=UTC)


def _response(status: int, text: str = "denied", headers: dict[str, str] | None = None) -> httpx.Response:
    return httpx.Response(status, text=text, headers=headers or {})


def test_http_status_mapping():
    limited = provider_error_from_response(_response(429))
    down = provider_error_from_response(_response(503))
    rejected = provider_error_from_response(_response(400))
    assert (limited.code, limited.retryable) == ("rate_limited", True)
    assert (down.code, down.retryable) == ("provider_unavailable", True)
    assert (rejected.code, rejected.retryable) == ("provider_rejected_request", False)


def test_retry_after_delta_seconds_and_http_date():
    assert parse_retry_after("30", now=NOW) == NOW + timedelta(seconds=30)
    assert parse_retry_after("Thu, 15 Oct 2026 12:05:00 GMT", now=NOW) == NOW + timedelta(minutes=5)


def test_retry_after_edge_cases():
    assert parse_retry_after(None) is None
    assert parse_retry_after("") is None
    assert parse_retry_after("soon", now=NOW) is None
    assert parse_retry_after("-5", now=NOW) is None
    assert parse_retry_after("0", now=NOW) == NOW
    # past dates clamp to now
    assert parse_retry_after("Thu, 15 Oct 2026 11:00:00 GMT", now=NOW) == NOW
    assert parse_retry_after("15 Oct 2026 12:10:00 -0000", now=NOW) == NOW + timedelta(minutes=10)


def test_retry_after_on_retryable_response_is_attached():
    error = provider_error_from_response(_response(429, headers={"Retry-After": "30"}))
    assert error.retryable is True
    assert error.retry_at is not None
    # The clock is wall time; just require a future (or present) instant ~30s out.
    assert error.retry_at >= datetime.now(UTC)


def test_retry_after_is_ignored_for_non_retryable_statuses():
    error = provider_error_from_response(_response(400, headers={"Retry-After": "30"}))
    assert error.retry_at is None
    assert error.retryable is False


def test_diagnostic_is_bounded_and_includes_status():
    error = ProviderError(
        "rate_limited",
        "Provider rate limit reached",
        retryable=True,
        status_code=429,
        request_id="abc123",
        raw_payload="pppppppppp",
    )
    text = provider_error_diagnostic(error)
    assert "HTTP 429" in text
    assert "request_id=abc123" in text
    assert provider_error_diagnostic(ProviderError("x", "boom", retryable=False)) == "boom"
    assert provider_error_diagnostic(error, max_payload_chars=4).endswith("response=pppp…")


class _RecordingGate(NullQuotaGate):
    def __init__(self) -> None:
        self.penalties: list[tuple[str, datetime]] = []

    def penalize(self, provider: str, until: datetime) -> None:
        self.penalties.append((provider, until))


def test_adapter_penalizes_the_gate_when_retry_after_is_present():
    gate = _RecordingGate()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, headers={"Retry-After": "30"}, request=request)

    adapter = HttpProviderAdapter(
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        quota_gate=gate,
    )
    adapter.provider_id = "demo"
    try:
        from openbars.core.errors import ProviderError as Err
        import pytest

        with pytest.raises(Err) as raised:
            adapter.get("https://example.invalid/bars")
    finally:
        adapter.close()
    assert raised.value.code == "rate_limited"
    assert len(gate.penalties) == 1
    assert gate.penalties[0][0] == "demo"
    assert gate.penalties[0][1] == raised.value.retry_at
