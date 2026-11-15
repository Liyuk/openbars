"""Run completion status files and webhook delivery (issue #219).

Two groups: the ``_status`` documents written by ``openbars run`` (offline, fake providers) and
the signed webhook delivery (injected poster for the retry semantics, one real local HTTP stub
on 127.0.0.1 for the signature). Nothing here opens a non-loopback connection and no secret is
ever written to disk or to the captured output.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
import threading
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest
from fakes import FakeProvider

from openbars.cli.main import main
from openbars.config.loader import load_settings
from openbars.config.schema import WebhookConfig, webhook_url_hash
from openbars.core.errors import ConfigError, ProviderError
from openbars.core.models import Interval, Market
from openbars.providers.base import BatchShape
from openbars.status.run_status import (
    RUN_OUTCOMES,
    RunStatusRecorder,
    latest_status_path,
    session_document,
    write_run_status,
)
from openbars.status.webhook import (
    DeliveryResult,
    deliver_all,
    deliver_one,
    headers_for,
    sign,
)

SCHEMA_FILE = Path(__file__).resolve().parent.parent / "docs" / "schemas" / "run-status.v1.json"
NOW = datetime(2026, 3, 10, 15, 0, tzinfo=UTC)  # latest ready US session: Monday 2026-03-09
API_KEY = "tkn-0123" + "456789abcdef"
WEBHOOK_SECRET = "whsec-0123" + "456789abcdef"
HOOK_PATH = "/hook"
PAYLOAD = b'{"schema": "openbars.run-status"}\n'


# --------------------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------------------


def _config(tmp_path: Path, *, notify: str = "", start: str = "2026-03-02",
            intervals: str = "[1d]", symbols: str = "[TEST1, ZZZA]") -> Path:  # fmt: skip
    path = tmp_path / "config.yaml"
    path.write_text(
        f"""
version: 1
markets: [us]
intervals: {intervals}
history: {{start: {start}, end: null}}
universe:
  us:
    symbols: {symbols}
storage: {{backend: parquet_duckdb, path: {tmp_path / "data"}}}
providers:
  massive:
    enabled: true
    api_key: ${{TEST_API_KEY}}
{notify}"""
    )
    return path


def _notify_block(url: str, *, secret_env: str | None = "OPENBARS_WEBHOOK_SECRET", extra: str = "") -> str:
    secret = f"      secret_env: {secret_env}\n" if secret_env else ""
    return f"""notify:
  webhooks:
    - url: {url}
{secret}{extra}"""


def _factory(**kw):
    def build(settings, gate):
        return {
            "massive": FakeProvider(
                "massive",
                batch=BatchShape.WHOLE_MARKET_PER_DAY,
                rolling_window=timedelta(days=730),
                max_symbols=10**6,
                feed="eod",
                **kw,
            )
        }

    return build


def _run(config: Path, *, env: dict[str, str] | None = None, factory=None) -> int:
    base_env = {"TEST_API_KEY": API_KEY}
    base_env.update(env or {})
    return main(
        ["--config", str(config), "run"],
        provider_factory=factory or _factory(),
        clock=lambda: NOW,
        env=base_env,
    )


def _data(tmp_path: Path) -> Path:
    return tmp_path / "data"


def _latest(tmp_path: Path) -> dict[str, Any]:
    return json.loads(latest_status_path(_data(tmp_path)).read_text(encoding="utf-8"))


def _all_documents(tmp_path: Path) -> list[Path]:
    return sorted((_data(tmp_path) / "_status").rglob("*.json"))


def _validate(instance: Any, schema: dict[str, Any], root: dict[str, Any] | None = None, path: str = "$") -> None:
    """A tiny JSON Schema subset validator: the subset this repository's schema files use."""
    root = root or schema
    if "$ref" in schema:
        ref = schema["$ref"]
        assert ref.startswith("#/$defs/"), f"{path}: unsupported $ref {ref}"
        _validate(instance, root["$defs"][ref.split("/")[-1]], root, path)
        return
    types = schema.get("type")
    if types is not None:
        allowed = [types] if isinstance(types, str) else list(types)
        mapping = {
            "object": dict,
            "array": list,
            "string": str,
            "integer": int,
            "number": (int, float),
            "boolean": bool,
            "null": type(None),
        }
        assert any(
            isinstance(instance, mapping[name]) and not (name == "boolean" and isinstance(instance, bool) is False)
            for name in allowed
        ), f"{path}: expected {allowed}, got {type(instance).__name__}"
    if "const" in schema:
        assert instance == schema["const"], f"{path}: expected const {schema['const']!r}, got {instance!r}"
    if "enum" in schema:
        assert instance in schema["enum"], f"{path}: {instance!r} not in {schema['enum']}"
    if isinstance(instance, str) and "pattern" in schema:
        assert re.search(schema["pattern"], instance), f"{path}: {instance!r} !~ {schema['pattern']}"
    if isinstance(instance, int) and not isinstance(instance, bool) and "minimum" in schema:
        assert instance >= schema["minimum"], f"{path}: {instance} < {schema['minimum']}"
    if isinstance(instance, dict):
        for key in schema.get("required", ()):
            assert key in instance, f"{path}: missing required key {key!r}"
        properties = schema.get("properties", {})
        if schema.get("additionalProperties") is False:
            extra = set(instance) - set(properties)
            assert not extra, f"{path}: unexpected keys {sorted(extra)}"
        for key, value in instance.items():
            if key in properties:
                _validate(value, properties[key], root, f"{path}.{key}")
    if isinstance(instance, list) and "items" in schema:
        for index, item in enumerate(instance):
            _validate(item, schema["items"], root, f"{path}[{index}]")


# --------------------------------------------------------------------------------------
# documents: outcomes, tallies, atomic writes, schema
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("jobs", "expected"),
    [
        ({"planned": 0, "succeeded": 0, "failed": 0, "skipped": 0}, "noop"),
        ({"planned": 3, "succeeded": 3, "failed": 0, "skipped": 0}, "success"),
        ({"planned": 3, "succeeded": 2, "failed": 1, "skipped": 0}, "partial"),
        ({"planned": 3, "succeeded": 0, "failed": 3, "skipped": 0}, "failed"),
        ({"planned": 3, "succeeded": 0, "failed": 0, "skipped": 3}, "skipped"),
        ({"planned": 3, "succeeded": 1, "failed": 0, "skipped": 2}, "success"),
    ],
)
def test_overall_outcome(jobs, expected):
    recorder = RunStatusRecorder(started_at=NOW)
    document = recorder.document(finished_at=NOW, jobs=jobs)
    assert document["outcome"] == expected
    assert document["error"] is None


def test_recorder_counts_rows_sessions_and_providers():
    recorder = RunStatusRecorder(started_at=NOW)
    raw_us = [
        _raw("TEST1", datetime(2026, 3, 9, tzinfo=UTC), Interval.D1),
        _raw("ZZZA", datetime(2026, 3, 9, tzinfo=UTC), Interval.D1),
        _raw("TEST1", datetime(2026, 3, 9, 14, tzinfo=UTC), Interval.H1),
    ]
    recorder.record_raw_bars(Market.US, Interval.D1, raw_us)
    # a row type that only carries the session date (the reconciler's CanonicalDailyRow)
    recorder.record_canonical_rows([_daily("TEST1", datetime(2026, 3, 9).date())], market=Market.US)
    recorder.mark_job_failed(Market.US, Interval.D1, datetime(2026, 3, 10).date(), "massive")
    document = recorder.document(finished_at=NOW, jobs={"planned": 2, "succeeded": 1, "failed": 1})
    sessions = {(s["interval"], s["session_date"]): s for s in document["sessions"]}
    assert set(sessions) == {("1d", "2026-03-09"), ("1h", "2026-03-09"), ("1d", "2026-03-10")}
    assert sessions[("1d", "2026-03-09")]["rows"] == {"raw": 2, "canonical": 1}
    assert sessions[("1d", "2026-03-09")]["outcome"] == "success"
    assert sessions[("1h", "2026-03-09")]["rows"] == {"raw": 1, "canonical": 0}
    assert sessions[("1d", "2026-03-10")]["outcome"] == "failed"
    assert sessions[("1d", "2026-03-10")]["tier"] == "final"
    assert sessions[("1d", "2026-03-09")]["providers"] == ["massive"]


def test_write_is_atomic_and_session_files_track_the_latest_run(tmp_path):
    root = _data(tmp_path)
    first = RunStatusRecorder(started_at=NOW, run_id="20260310T150000Z-aaaaaa")
    first.record_canonical_rows([_daily("TEST1", datetime(2026, 3, 9).date())], market=Market.US)
    _body, paths = write_run_status(root, first.document(finished_at=NOW, jobs={"succeeded": 1}))
    session_path = root / "_status" / "sessions" / "us" / "1d" / "2026-03-09.json"
    assert session_path in paths
    assert json.loads(session_path.read_text())["run_id"] == "20260310T150000Z-aaaaaa"

    later = RunStatusRecorder(started_at=NOW, run_id="20260311T150000Z-bbbbbb")
    later.record_canonical_rows(
        [_daily("TEST1", datetime(2026, 3, 9).date()), _daily("ZZZA", datetime(2026, 3, 9).date())],
        market=Market.US,
    )
    write_run_status(root, later.document(finished_at=NOW, jobs={"succeeded": 1}))
    scoped = json.loads(session_path.read_text())
    assert scoped["run_id"] == "20260311T150000Z-bbbbbb"  # per-session file is updated in place
    assert len(scoped["sessions"]) == 1
    assert scoped["sessions"][0]["rows"] == {"raw": 0, "canonical": 2}
    runs = sorted((root / "_status" / "runs").glob("*.json"))
    assert [p.name for p in runs] == ["20260310T150000Z-aaaaaa.json", "20260311T150000Z-bbbbbb.json"]
    assert not list((root / "_status").rglob("*.tmp"))  # atomic: no temp file left behind


def test_documents_match_the_published_schema(tmp_path):
    schema = json.loads(SCHEMA_FILE.read_text(encoding="utf-8"))
    assert set(schema["properties"]["outcome"]["enum"]) == set(RUN_OUTCOMES)
    root = _data(tmp_path)
    recorder = RunStatusRecorder(started_at=NOW)
    recorder.record_raw_bars(Market.US, Interval.D1, [_raw("TEST1", datetime(2026, 3, 9, tzinfo=UTC), Interval.D1)])
    recorder.record_canonical_rows([_daily("TEST1", datetime(2026, 3, 9).date())], market=Market.US)
    document = recorder.document(finished_at=NOW, jobs={"planned": 1, "succeeded": 1})
    write_run_status(root, document)
    for path in _all_documents(tmp_path):
        _validate(json.loads(path.read_text(encoding="utf-8")), schema)
    # and a document with delivery results is still schema-valid
    document["notifications"] = [DeliveryResult("abc123abc123", "skipped", 0, None, "no_secret").as_dict()]
    _validate(document, schema)
    _validate(session_document(document, document["sessions"][0]), schema)


def test_documents_carry_no_paths_or_secrets(tmp_path):
    root = _data(tmp_path)
    recorder = RunStatusRecorder(started_at=NOW, storage_backend="parquet_duckdb")
    document = recorder.document(finished_at=NOW, jobs={"planned": 1, "succeeded": 1})
    document["notifications"] = [
        DeliveryResult(webhook_url_hash("https://example.invalid/hook?token=" + API_KEY), "failed", 1, 500, "HTTP 500").as_dict()
    ]
    body, _paths = write_run_status(root, document)
    text = body.decode("utf-8")
    assert str(tmp_path) not in text
    assert API_KEY not in text
    assert "https://example.invalid" not in text
    assert "/hook" not in text


# --------------------------------------------------------------------------------------
# the CLI writes the status file on every outcome
# --------------------------------------------------------------------------------------


@pytest.mark.usefixtures("no_network")
class TestRunStatusFiles:
    def test_success_writes_run_latest_and_session_documents(self, tmp_path, capsys):
        assert _run(_config(tmp_path)) == 0
        out = capsys.readouterr().out
        assert "status: success" in out
        document = _latest(tmp_path)
        assert document["schema"] == "openbars.run-status" and document["schema_version"] == 1
        assert document["command"] == "run"
        assert document["outcome"] == "success"
        assert document["jobs"] == {"planned": 6, "succeeded": 6, "failed": 0, "skipped": 0}
        assert document["storage"] == {"backend": "parquet_duckdb"}
        assert document["error"] is None and document["notifications"] == []
        assert re.fullmatch(r"[0-9]{8}T[0-9]{6}Z-[0-9a-f]{6}", document["run_id"])
        latest_session = next(s for s in document["sessions"] if s["session_date"] == "2026-03-09")
        assert latest_session == {
            "market": "us",
            "interval": "1d",
            "session_date": "2026-03-09",
            "tier": "final",
            "outcome": "success",
            "rows": {"raw": 2, "canonical": 2},
            "providers": ["massive"],
        }
        run_files = list((_data(tmp_path) / "_status" / "runs").glob("*.json"))
        assert len(run_files) == 1
        assert json.loads(run_files[0].read_text()) == document
        session_file = _data(tmp_path) / "_status" / "sessions" / "us" / "1d" / "2026-03-09.json"
        assert json.loads(session_file.read_text())["sessions"] == [latest_session]
        assert not list((_data(tmp_path) / "_status").rglob("*.tmp"))

    def test_zero_jobs_run_writes_noop(self, tmp_path, capsys):
        assert _run(_config(tmp_path)) == 0
        capsys.readouterr()
        assert _run(_config(tmp_path)) == 0
        assert "status: noop" in capsys.readouterr().out
        document = _latest(tmp_path)
        assert document["outcome"] == "noop"
        assert document["jobs"] == {"planned": 0, "succeeded": 0, "failed": 0, "skipped": 0}
        assert document["sessions"] == []
        assert document["error"] is None

    def test_failed_run_exits_zero_and_records_failed_sessions(self, tmp_path, capsys):
        factory = _factory(
            behavior=lambda request, call: (_ for _ in ()).throw(
                ProviderError("invalid_request", "rejected by provider", retryable=False, status_code=400)
            )
        )
        assert _run(_config(tmp_path), factory=factory) == 0  # a failure must not change the exit code
        assert "status: failed" in capsys.readouterr().out
        document = _latest(tmp_path)
        assert document["outcome"] == "failed"
        assert document["jobs"]["failed"] == 6 and document["jobs"]["succeeded"] == 0
        assert document["error"] == "6 job(s) did not complete"
        assert {s["outcome"] for s in document["sessions"]} == {"failed"}
        assert all(s["rows"] == {"raw": 0, "canonical": 0} for s in document["sessions"])

    def test_partial_run_records_both_outcomes(self, tmp_path, capsys):
        def behavior(request, call_no):
            if call_no > 4:
                raise ProviderError("invalid_request", "rejected by provider", retryable=False, status_code=400)

        assert _run(_config(tmp_path), factory=_factory(behavior=behavior)) == 0
        assert "status: partial" in capsys.readouterr().out
        document = _latest(tmp_path)
        assert document["outcome"] == "partial"
        assert document["jobs"]["succeeded"] == 4 and document["jobs"]["failed"] == 2
        assert {s["outcome"] for s in document["sessions"]} == {"success", "failed"}

    def test_status_json_prints_the_latest_document(self, tmp_path, capsys):
        config = _config(tmp_path)
        assert _run(config) == 0
        capsys.readouterr()
        document = _latest(tmp_path)
        assert main(["--config", str(config), "status", "--json"], env={"TEST_API_KEY": API_KEY}) == 0
        printed = capsys.readouterr().out
        assert json.loads(printed) == document
        assert '"schema": "openbars.run-status"' in printed

    def test_status_json_without_a_run_is_an_error(self, tmp_path, capsys):
        config = _config(tmp_path)
        code = main(["--config", str(config), "status", "--json"], env={"TEST_API_KEY": API_KEY})
        captured = capsys.readouterr()
        assert code == 1 and "no run status" in captured.err

    def test_status_write_failure_only_warns(self, tmp_path, capsys, monkeypatch):
        import openbars.cli.main as cli

        def boom(*args, **kwargs):
            raise OSError("disk full")

        monkeypatch.setattr("openbars.status.run_status.write_run_status", boom)
        assert _run(_config(tmp_path)) == 0
        assert "warning: could not write run status (OSError)" in capsys.readouterr().err
        assert cli.EXIT_OK == 0

    def test_aborted_run_still_writes_a_status_document(self, tmp_path, capsys, monkeypatch):
        from openbars.storage.errors import StorageError

        def boom(*args, **kwargs):
            raise StorageError("state store unavailable")

        monkeypatch.setattr("openbars.cli.main._make_plan", boom)
        assert _run(_config(tmp_path)) == 1  # the failure keeps its own exit code
        document = _latest(tmp_path)
        assert document["outcome"] == "failed"
        assert document["error"] == "run aborted (StorageError)"
        assert document["jobs"]["succeeded"] == 0 and document["jobs"]["failed"] == 0
        capsys.readouterr()

    def test_provider_init_failure_still_writes_failed_status(self, tmp_path, capsys):
        def factory(settings, gate):
            raise ConfigError("provider could not be built")

        assert _run(_config(tmp_path), factory=factory) == 2  # unchanged ConfigError exit code
        document = _latest(tmp_path)
        assert document["outcome"] == "failed"
        assert document["error"] == "run aborted (ConfigError)"
        assert document["jobs"] == {"planned": 0, "succeeded": 0, "failed": 0, "skipped": 0}
        assert document["sessions"] == []
        assert "status: failed" in capsys.readouterr().out

    def test_check_ready_failure_still_writes_failed_status(self, tmp_path, capsys, monkeypatch):
        def boom(*args, **kwargs):
            raise ConfigError("no usable provider")

        monkeypatch.setattr("openbars.cli.main.check_ready", boom)
        assert _run(_config(tmp_path)) == 2  # unchanged ConfigError exit code
        document = _latest(tmp_path)
        assert document["outcome"] == "failed"
        assert document["error"] == "run aborted (ConfigError)"
        assert "status: failed" in capsys.readouterr().out

    def test_backend_init_failure_still_writes_failed_status(self, tmp_path, capsys, monkeypatch):
        from openbars.storage.errors import StorageError

        def boom(*args, **kwargs):
            raise StorageError("backend unavailable")

        monkeypatch.setattr("openbars.cli.main._open_backend", boom)
        assert _run(_config(tmp_path)) == 1  # unchanged StorageError exit code
        document = _latest(tmp_path)
        assert document["outcome"] == "failed"
        assert document["error"] == "run aborted (StorageError)"
        assert "status: failed" in capsys.readouterr().out

    def test_budget_blocked_run_records_skipped_outcome(self, tmp_path, capsys):
        from dataclasses import replace

        class Hourly(FakeProvider):
            def capabilities(self):
                (cap,) = super().capabilities()
                return (replace(cap, max_bars_per_request=100),)

        def build(settings, gate):
            return {"hourly": Hourly("hourly", interval=Interval.H1, limit_per_minute=1)}

        symbols = "[" + ",".join(f"S{i:03d}" for i in range(100)) + "]"
        config = _config(tmp_path, intervals="[1h]", symbols=symbols, start="2026-01-05")
        assert _run(config, factory=build) == 3  # the budget refusal keeps EXIT_BUDGET
        document = _latest(tmp_path)
        assert document["outcome"] == "skipped"
        assert document["jobs"]["planned"] > 0
        assert document["jobs"]["succeeded"] == 0 and document["jobs"]["failed"] == 0
        assert document["jobs"]["skipped"] == document["jobs"]["planned"]
        assert document["error"] is None
        assert "status: skipped" in capsys.readouterr().out

    def test_config_command_never_prints_the_webhook_url_or_secret(self, tmp_path, capsys):
        config = _config(tmp_path, notify=_notify_block("https://example.invalid/hook?token=abc"))
        code = main(
            ["--config", str(config), "config"],
            env={"TEST_API_KEY": API_KEY, "OPENBARS_WEBHOOK_SECRET": WEBHOOK_SECRET},
        )
        out = capsys.readouterr().out
        assert code == 0
        assert "secret set (OPENBARS_WEBHOOK_SECRET)" in out
        assert "example.invalid" not in out and "token=abc" not in out
        assert WEBHOOK_SECRET not in out
        assert webhook_url_hash("https://example.invalid/hook?token=abc") in out


# --------------------------------------------------------------------------------------
# webhook config
# --------------------------------------------------------------------------------------


def test_notify_config_defaults_and_secret_resolution(tmp_path):
    config = _config(tmp_path, notify=_notify_block("https://example.invalid/hook"))
    settings = load_settings(config, env={"TEST_API_KEY": API_KEY, "OPENBARS_WEBHOOK_SECRET": WEBHOOK_SECRET})
    hook = settings.notify.webhooks[0]
    assert (hook.url, hook.secret_env, hook.secret) == (
        "https://example.invalid/hook",
        "OPENBARS_WEBHOOK_SECRET",
        WEBHOOK_SECRET,
    )
    assert (hook.timeout_seconds, hook.max_attempts) == (10.0, 3)
    assert settings.notify.total_timeout_seconds == 60.0
    assert WEBHOOK_SECRET in settings.secret_values()  # scrubbed from every CLI print


def test_notify_config_accepts_tuning(tmp_path):
    extra = "      timeout_seconds: 4\n      max_attempts: 5\n      backoff_seconds: 1\n      max_backoff_seconds: 7\n"
    block = _notify_block("http://127.0.0.1:9/hook", extra=extra) + "  total_timeout_seconds: 20\n"
    settings = load_settings(_config(tmp_path, notify=block), env={"TEST_API_KEY": API_KEY})
    hook = settings.notify.webhooks[0]
    assert (hook.timeout_seconds, hook.max_attempts, hook.backoff_seconds, hook.max_backoff_seconds) == (4.0, 5, 1.0, 7.0)
    assert settings.notify.total_timeout_seconds == 20.0
    assert hook.secret is None  # unset env: resolved later as "no secret", never invented


@pytest.mark.parametrize(
    "block",
    [
        'notify:\n  webhooks:\n    - url: "ftp://example.invalid/hook"\n',
        "notify:\n  webhooks:\n    - url: https://example.invalid/hook\n      max_attempts: 0\n",
        "notify:\n  webhooks:\n    - url: https://example.invalid/hook\n      timeout_seconds: 0\n",
        "notify:\n  webhooks:\n    - url: https://example.invalid/hook\n      retries: 3\n",
        "notify:\n  webhooks:\n    - url: https://example.invalid/hook\n      backoff_seconds: -1\n",
        "notify:\n  unknown: 1\n",
    ],
)
def test_notify_config_rejects_invalid_settings(tmp_path, block):
    with pytest.raises(ConfigError):
        load_settings(_config(tmp_path, notify=block), env={"TEST_API_KEY": API_KEY})


# --------------------------------------------------------------------------------------
# webhook delivery semantics (injected poster)
# --------------------------------------------------------------------------------------


class Clock:
    def __init__(self, start: datetime = NOW) -> None:
        self.now = start
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now.timestamp()

    def wall(self) -> datetime:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += timedelta(seconds=seconds)


def _hook(**kw) -> WebhookConfig:
    base = {"url": "https://example.invalid/hook", "secret": WEBHOOK_SECRET}
    base.update(kw)
    return WebhookConfig(**base)


def _run_delivery(hook, responses, clock=None, **kw):
    clock = clock or Clock()
    calls: list[dict[str, Any]] = []

    def poster(url, body, headers, timeout):
        calls.append({"url": url, "body": body, "headers": dict(headers), "timeout": timeout})
        return responses[min(len(calls) - 1, len(responses) - 1)]

    results = deliver_all(
        PAYLOAD,
        [hook],
        total_timeout_seconds=kw.pop("total_timeout_seconds", 60.0),
        sleep=clock.sleep,
        now=clock.monotonic,
        wall_clock=clock.wall,
        poster=poster,
        **kw,
    )
    return results, calls, clock


def test_signature_matches_the_documented_scheme():
    body = b'{"a":1}\n'
    expected = "sha256=" + hmac.new(WEBHOOK_SECRET.encode(), b"1700000000." + body, hashlib.sha256).hexdigest()
    assert sign(WEBHOOK_SECRET, 1700000000, body) == expected
    headers = headers_for(WEBHOOK_SECRET, 1700000000, body, "delivery-1")
    assert headers["X-OpenBars-Signature"] == expected
    assert headers["X-OpenBars-Event"] == "run.completed"
    assert headers["X-OpenBars-Timestamp"] == "1700000000"
    assert headers["X-OpenBars-Delivery"] == "delivery-1"
    assert headers["Content-Type"] == "application/json"
    # a different body never verifies
    assert sign(WEBHOOK_SECRET, 1700000000, b'{"a":2}\n') != expected


def test_retries_with_exponential_backoff_until_success():
    results, calls, clock = _run_delivery(_hook(), [500, 503, 200])
    assert len(calls) == 3
    assert results[0].outcome == "delivered" and results[0].attempts == 3 and results[0].status_code == 200
    assert clock.sleeps == [2.0, 4.0]
    # the delivery id is stable across retries, the timestamp/signature are recomputed per attempt
    ids = {call["headers"]["X-OpenBars-Delivery"] for call in calls}
    assert len(ids) == 1
    for call in calls:
        assert call["headers"]["X-OpenBars-Signature"] == sign(
            WEBHOOK_SECRET, call["headers"]["X-OpenBars-Timestamp"], PAYLOAD
        )
        assert call["body"] == PAYLOAD


def test_client_errors_are_not_retried():
    results, calls, clock = _run_delivery(_hook(), [400])
    assert len(calls) == 1 and clock.sleeps == []
    assert results[0].outcome == "failed" and results[0].attempts == 1
    assert results[0].status_code == 400 and results[0].error == "HTTP 400"


def test_redirects_are_not_retried():
    results, calls, _clock = _run_delivery(_hook(), [302])
    assert len(calls) == 1 and results[0].outcome == "failed"


@pytest.mark.parametrize("status", [408, 429, 500, 503])
def test_retryable_statuses_are_retried(status):
    results, calls, _clock = _run_delivery(_hook(), [status, 200])
    assert len(calls) == 2 and results[0].outcome == "delivered"


def test_attempts_are_capped_by_max_attempts():
    results, calls, clock = _run_delivery(_hook(max_attempts=2), [500])
    assert len(calls) == 2 and clock.sleeps == [2.0]
    assert results[0].outcome == "failed" and results[0].attempts == 2 and results[0].error == "HTTP 500"


def test_overall_budget_bounds_delivery_time():
    results, calls, clock = _run_delivery(_hook(max_attempts=10), [500], total_timeout_seconds=5.0)
    assert len(calls) < 10
    assert (clock.now - NOW).total_seconds() <= 5.0
    assert results[0].outcome == "failed"
    assert results[0].error in ("delivery budget exhausted", "HTTP 500")


def test_transport_errors_are_retried_with_a_url_free_message():
    clock = Clock()
    calls = []

    def poster(url, body, headers, timeout):
        calls.append(url)
        raise OSError(f"cannot reach {url}")

    results = deliver_all(
        PAYLOAD,
        [_hook(max_attempts=2)],
        total_timeout_seconds=60.0,
        sleep=clock.sleep,
        now=clock.monotonic,
        wall_clock=clock.wall,
        poster=poster,
    )
    assert results[0].outcome == "failed" and results[0].attempts == 2 and len(calls) == 2
    assert "example.invalid" not in (results[0].error or "")
    assert "<url>" in (results[0].error or "")


def test_missing_secret_is_skipped_without_a_request(capsys):
    clock = Clock()
    calls = []

    def poster(*args, **kwargs):
        calls.append(args)
        raise AssertionError("an unsigned request must never be sent")

    result = deliver_one(
        _hook(secret=None, secret_env="OPENBARS_WEBHOOK_SECRET"),
        PAYLOAD,
        deadline=clock.monotonic() + 60,
        now=clock.monotonic,
        sleep=clock.sleep,
        wall_clock=clock.wall,
        poster=poster,
        redact=lambda text: text,
    )
    assert calls == []
    assert (result.outcome, result.attempts, result.error) == ("skipped", 0, "no_secret")


# --------------------------------------------------------------------------------------
# a real loopback HTTP stub: signature + write-back of results
# --------------------------------------------------------------------------------------


class _Stub:
    """Tiny threaded HTTP server on 127.0.0.1 that records what it received."""

    def __init__(self, statuses: list[int], secret: str) -> None:
        self.requests: list[dict[str, Any]] = []
        self.lock = threading.Lock()
        stub = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(length)
                timestamp = self.headers.get("X-OpenBars-Timestamp", "")
                signature = self.headers.get("X-OpenBars-Signature", "")
                expected = sign(secret, timestamp, body)
                with stub.lock:
                    stub.requests.append(
                        {
                            "path": self.path,
                            "headers": dict(self.headers),
                            "body": body,
                            "signature_ok": hmac.compare_digest(signature, expected) and bool(timestamp),
                        }
                    )
                    index = len(stub.requests) - 1
                status = statuses[min(index, len(statuses) - 1)]
                self.send_response(status)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, *args):  # keep pytest output clean
                return

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self) -> "_Stub":  # noqa: UP037,PYI034 - returns self
        self.thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.server.shutdown()
        self.server.server_close()

    @property
    def url(self) -> str:
        host, port = self.server.server_address[:2]
        return f"http://{host}:{port}{HOOK_PATH}?token={API_KEY}"


def test_local_stub_receives_the_signed_payload_and_results_are_written_back(tmp_path, capsys):
    with _Stub([200], WEBHOOK_SECRET) as stub:
        config = _config(tmp_path, notify=_notify_block(stub.url))
        code = _run(config, env={"OPENBARS_WEBHOOK_SECRET": WEBHOOK_SECRET})
        assert code == 0
        captured = capsys.readouterr()
        requests = stub.requests

    assert len(requests) == 1
    request = requests[0]
    assert request["path"].startswith(HOOK_PATH)
    assert request["signature_ok"] is True
    body = json.loads(request["body"])
    assert body["schema"] == "openbars.run-status" and body["run_id"] == _latest(tmp_path)["run_id"]
    assert "notifications" not in body  # the signed payload is the pre-delivery document

    document = _latest(tmp_path)
    notification = document["notifications"][0]
    assert notification["outcome"] == "delivered"
    assert notification["attempts"] == 1 and notification["status_code"] == 200
    assert notification["url_hash"] == webhook_url_hash(stub.url)
    session_scoped = json.loads(
        (_data(tmp_path) / "_status" / "sessions" / "us" / "1d" / "2026-03-09.json").read_text()
    )
    assert session_scoped["notifications"] == []  # delivery results stay on the run document

    # the run log and the status file never carry the URL, its query string or the secret
    printed = captured.out + captured.err
    assert "notify: webhook[" in printed and "delivered" in printed
    written = "".join(path.read_text(encoding="utf-8") for path in _all_documents(tmp_path))
    for text in (printed, written):
        assert "127.0.0.1" not in text
        assert "token=" not in text
        assert WEBHOOK_SECRET not in text


def test_local_stub_failure_is_recorded_and_does_not_fail_the_run(tmp_path, capsys):
    with _Stub([500], WEBHOOK_SECRET) as stub:
        extra = "      max_attempts: 2\n      backoff_seconds: 0\n"
        config = _config(tmp_path, notify=_notify_block(stub.url, extra=extra))
        assert _run(config, env={"OPENBARS_WEBHOOK_SECRET": WEBHOOK_SECRET}) == 0
        captured = capsys.readouterr()
        attempts = len(stub.requests)

    assert attempts == 2
    notification = _latest(tmp_path)["notifications"][0]
    assert notification["outcome"] == "failed" and notification["attempts"] == 2
    assert notification["status_code"] == 500 and notification["error"] == "HTTP 500"
    assert "notify: webhook[" in captured.out and "failed" in captured.out


def test_unset_secret_skips_the_target_and_never_calls_it(tmp_path, capsys):
    with _Stub([200], WEBHOOK_SECRET) as stub:
        config = _config(tmp_path, notify=_notify_block(stub.url))
        assert _run(config) == 0  # OPENBARS_WEBHOOK_SECRET not set
        captured = capsys.readouterr()
        assert stub.requests == []
    notification = _latest(tmp_path)["notifications"][0]
    assert (notification["outcome"], notification["attempts"], notification["error"]) == ("skipped", 0, "no_secret")
    assert "skipped" in captured.out


def test_early_init_failure_still_delivers_the_failed_status_webhook(tmp_path, capsys):
    def factory(settings, gate):
        raise ConfigError("provider could not be built")

    with _Stub([200], WEBHOOK_SECRET) as stub:
        config = _config(tmp_path, notify=_notify_block(stub.url))
        code = _run(config, factory=factory, env={"OPENBARS_WEBHOOK_SECRET": WEBHOOK_SECRET})
        captured = capsys.readouterr()
        requests = stub.requests

    assert code == 2  # the ConfigError still decides the exit code
    assert len(requests) == 1 and requests[0]["signature_ok"] is True
    body = json.loads(requests[0]["body"])
    document = _latest(tmp_path)
    assert body["run_id"] == document["run_id"] and body["outcome"] == "failed"
    assert "notifications" not in body
    assert document["notifications"][0]["outcome"] == "delivered"
    assert "status: failed" in captured.out and "delivered" in captured.out
    assert WEBHOOK_SECRET not in captured.out + captured.err


# --------------------------------------------------------------------------------------
# small fakes
# --------------------------------------------------------------------------------------


def _raw(symbol: str, ts_start: datetime, interval: Interval):
    from openbars.core.models import RawBar

    return RawBar(
        provider="massive",
        symbol_as_returned=symbol,
        fetch_id="f-1",
        interval=interval,
        ts_start=ts_start,
        open=Decimal(10),
        high=Decimal(11),
        low=Decimal(9),
        close=Decimal(10),
        volume=Decimal(1000),
        feed="eod",
        session_date=ts_start.date(),
    )


def _daily(symbol: str, session_date):
    from openbars.core.series import CanonicalDailyRow

    return CanonicalDailyRow(
        instrument_id=f"us:eq:xnys:{symbol}",
        session_date=session_date,
        policy_version="research-daily-v2",
        selected_source_bar_id=None,
        provider="massive",
        feed="eod",
        adjustment="split",
        revision=1,
        open=Decimal(10),
        high=Decimal(11),
        low=Decimal(9),
        close=Decimal(10),
        volume=Decimal(1000),
        selection_reason="primary_source",
        quality_status="complete",
        capabilities=(),
        source_fingerprint="fp",
        generated_at=NOW,
    )
