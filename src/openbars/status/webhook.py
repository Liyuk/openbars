"""Signed webhook delivery of the run status document (issue #219).

Each configured ``notify.webhooks`` target receives one ``POST`` per completed ``run``:

* ``Content-Type: application/json``, body = the ``runs/<run_id>.json`` bytes as written,
  **without** the ``notifications`` key (the payload is built before results are recorded);
* ``X-OpenBars-Event: run.completed``;
* ``X-OpenBars-Delivery: <uuid4>`` -- unique per target per payload, reused by every retry;
* ``X-OpenBars-Timestamp: <unix seconds>``;
* ``X-OpenBars-Signature: sha256=<hex>`` where ``hex = HMAC-SHA256(secret, "<timestamp>.<body>")``.

A target whose ``secret_env`` is unset or empty is skipped (``skipped: no_secret``): OpenBars
never sends an unsigned request. Deliveries are bounded by one overall budget so a slow or
down target can never hold up the run; failures only show up in the status file and as a
warning on stderr, never in the exit code.
"""

from __future__ import annotations

import hashlib
import hmac
import socket
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from openbars.config.schema import WebhookConfig, webhook_url_hash

EVENT = "run.completed"
DEFAULT_TOTAL_TIMEOUT_SECONDS = 60.0
RETRYABLE_STATUS = frozenset({408, 429})
MAX_ERROR_CHARS = 200

# (url, body, headers, timeout) -> HTTP status code, raising on transport errors.
Poster = Callable[[str, bytes, Mapping[str, str], float], int]


@dataclass(frozen=True)
class DeliveryResult:
    """One target's delivery outcome, as written to ``notifications`` in the status file."""

    url_hash: str
    outcome: str  # delivered | failed | skipped
    attempts: int
    status_code: int | None = None
    error: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "url_hash": self.url_hash,
            "outcome": self.outcome,
            "attempts": self.attempts,
            "status_code": self.status_code,
            "error": self.error,
        }


def sign(secret: str, timestamp: int | str, body: bytes) -> str:
    """``sha256=<hex>`` HMAC-SHA256 over ``<timestamp>.<body>`` (the documented scheme)."""
    message = f"{timestamp}.".encode() + body
    digest = hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def headers_for(secret: str, timestamp: int, body: bytes, delivery: str) -> dict[str, str]:
    return {
        "Content-Type": "application/json",
        "X-OpenBars-Event": EVENT,
        "X-OpenBars-Delivery": delivery,
        "X-OpenBars-Timestamp": str(timestamp),
        "X-OpenBars-Signature": sign(secret, timestamp, body),
    }


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Surface 3xx as an error instead of following it (a redirect is not a delivery)."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def post(url: str, body: bytes, headers: Mapping[str, str], timeout: float) -> int:
    """POST ``body`` and return the status code; 3xx/4xx/5xx are returned, not raised."""
    request = urllib.request.Request(url, data=body, headers=dict(headers), method="POST")
    opener = urllib.request.build_opener(_NoRedirect)
    try:
        with opener.open(request, timeout=timeout) as response:
            return int(response.status)
    except urllib.error.HTTPError as exc:
        exc.close()
        return int(exc.code)


def _describe(exc: BaseException, url: str) -> str:
    """Short, URL-free description of a transport failure."""
    if isinstance(exc, urllib.error.HTTPError):  # pragma: no cover - handled by post()
        return f"HTTP {exc.code}"
    if isinstance(exc, urllib.error.URLError):
        reason = exc.reason
        detail = f"{type(reason).__name__}: {reason}" if isinstance(reason, BaseException) else str(reason)
    elif isinstance(exc, (TimeoutError, socket.timeout)):
        detail = "timeout"
    else:
        detail = f"{type(exc).__name__}: {exc}"
    text = " ".join(detail.split())
    text = text.replace(url, "<url>")
    return text[:MAX_ERROR_CHARS] or type(exc).__name__


def _retryable_status(status: int) -> bool:
    return status in RETRYABLE_STATUS or 500 <= status < 600


def _backoff_delay(hook: WebhookConfig, attempt: int) -> float:
    """Exponential backoff after ``attempt`` failures: base, 2x base, 4x base, capped."""
    delay = hook.backoff_seconds * (2 ** (attempt - 1))
    return min(delay, hook.max_backoff_seconds)


def deliver_one(
    hook: WebhookConfig,
    body: bytes,
    *,
    deadline: float,
    now: Callable[[], float],
    sleep: Callable[[float], None],
    wall_clock: Callable[[], datetime],
    poster: Poster,
    redact: Callable[[str], str],
) -> DeliveryResult:
    """Deliver one payload to one target within ``deadline`` (a monotonic instant)."""
    url_hash = webhook_url_hash(hook.url)
    if not hook.secret:
        return DeliveryResult(url_hash, "skipped", 0, None, "no_secret")
    delivery = str(uuid.uuid4())
    attempts = 0
    status_code: int | None = None
    error: str | None = None
    while attempts < hook.max_attempts:
        remaining = deadline - now()
        if remaining <= 0:
            error = error or "delivery budget exhausted"
            break
        attempts += 1
        timestamp = int(wall_clock().astimezone(UTC).timestamp())
        headers = headers_for(hook.secret, timestamp, body, delivery)
        timeout = max(min(hook.timeout_seconds, remaining), 0.01)
        try:
            status_code = poster(hook.url, body, headers, timeout)
        except Exception as exc:  # noqa: BLE001 - any transport failure is retryable
            status_code, error = None, redact(_describe(exc, hook.url))
        else:
            if 200 <= status_code < 300:
                return DeliveryResult(url_hash, "delivered", attempts, status_code, None)
            error = redact(f"HTTP {status_code}")
            if not _retryable_status(status_code):
                return DeliveryResult(url_hash, "failed", attempts, status_code, error)
        if attempts >= hook.max_attempts:
            break
        delay = _backoff_delay(hook, attempts)
        remaining = deadline - now()
        if remaining <= 0:
            continue
        sleep(min(delay, remaining))
    return DeliveryResult(url_hash, "failed", attempts, status_code, error or "delivery failed")


def deliver_all(
    body: bytes,
    webhooks: Sequence[WebhookConfig],
    *,
    total_timeout_seconds: float = DEFAULT_TOTAL_TIMEOUT_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
    now: Callable[[], float] = time.monotonic,
    wall_clock: Callable[[], datetime] | None = None,
    poster: Poster = post,
    redact: Callable[[str], str] = lambda text: text,
) -> list[DeliveryResult]:
    """Deliver ``body`` to every target within one shared budget (sequential, bounded)."""
    if not webhooks:
        return []
    wall_clock = wall_clock or (lambda: datetime.now(UTC))
    deadline = now() + max(0.0, total_timeout_seconds)
    return [
        deliver_one(
            hook,
            body,
            deadline=deadline,
            now=now,
            sleep=sleep,
            wall_clock=wall_clock,
            poster=poster,
            redact=redact,
        )
        for hook in webhooks
    ]


def describe_results(results: Sequence[DeliveryResult]) -> list[str]:
    """Human-readable, URL-free one-liners for stdout/stderr."""
    lines = []
    for result in results:
        detail = f"status={result.status_code}" if result.status_code is not None else (result.error or "-")
        lines.append(f"notify: webhook[{result.url_hash}] {result.outcome} attempts={result.attempts} ({detail})")
    return lines
