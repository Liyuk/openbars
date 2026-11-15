# Run completion status files and webhooks

`openbars run` (and its `backfill` alias) writes a machine-readable completion document every
time it ends: success, partial success, failure, or a run with nothing to do. Consumers can poll
these files instead of parsing logs or the human-readable `openbars status` output. Optionally the
same document is POSTed to the webhook targets configured under `notify.webhooks`; the files are
the fallback when a delivery fails.

OpenBars never contacts the systems that consume these files, holds no state about them, and
writes nothing outside its own data directory. The document is a public, versioned contract
([JSON Schema](schemas/run-status.v1.json)); treat `schema_version` as the compatibility marker.

## Files

All of them live under `<storage.path>/_status/` and are written atomically (temporary file in the
same directory, `fsync`, `os.replace`), so a reader sees either the previous document or the new
one — never a truncated file. Leftover `.<name>.<token>.tmp` files are only possible after a hard
kill and are ignored by readers.

| Path | Contents |
| --- | --- |
| `_status/runs/<run_id>.json` | One document per invocation. Never overwritten. |
| `_status/latest.json` | Copy of the most recent document. |
| `_status/sessions/<market>/<interval>/<session_date>.json` | The most recent result for that session (same fields, `sessions` holds only that session). Updated in place by later runs. |

The files are written for the Parquet/DuckDB backend and also for the Postgres backend: they
always go to the local `storage.path` directory, because they describe the *run*, not the rows.
A status write failure is a warning on stderr; it changes neither the exit code nor any stored data.

## Document, schema v1

```json
{
  "schema": "openbars.run-status",
  "schema_version": 1,
  "run_id": "20261117T051502Z-ab12cd",
  "command": "run",
  "started_at": "2026-11-17T05:15:02Z",
  "finished_at": "2026-11-17T05:15:40Z",
  "outcome": "success",
  "jobs": {"planned": 1, "succeeded": 1, "failed": 0, "skipped": 0},
  "sessions": [
    {"market": "us", "interval": "1d", "session_date": "2026-11-16",
     "tier": "final", "outcome": "success",
     "rows": {"raw": 12500, "canonical": 12300},
     "providers": ["massive"]}
  ],
  "storage": {"backend": "parquet_duckdb"},
  "error": null,
  "notifications": []
}
```

| Field | Meaning |
| --- | --- |
| `schema`, `schema_version` | Document type and version. Bump `schema_version` only for incompatible changes. |
| `run_id` | `<UTC compact timestamp>-<6 hex>`; unique per invocation, a new value on every run. |
| `command` | `run` or `backfill`. |
| `started_at`, `finished_at` | UTC ISO 8601, second resolution, trailing `Z`. |
| `outcome` | `noop` (nothing planned and nothing executed), `success` (every job that ran finished), `partial` (some finished, some did not), `failed` (nothing finished and at least one job did not). |
| `jobs.planned` | Jobs in this invocation's plan. |
| `jobs.succeeded` | Jobs that finished successfully in this run. |
| `jobs.failed` | Jobs that ended `dead` or are waiting for a retry after a failure in this run. |
| `jobs.skipped` | Planned jobs this run did not execute (for example `--max-jobs`, or waiting on a provider). |
| `sessions[]` | One entry per `(market, interval, session_date)`: sessions this run wrote rows for, plus sessions whose jobs failed. |
| `sessions[].tier` | Freshness tier, `provisional` or `final`. Only `final` is produced today; provisional same-day rows are a later step, the field is reserved. |
| `sessions[].outcome` | Summary of the jobs that targeted that session: `success` or `failed`. |
| `sessions[].rows.raw` | Raw bars this run stored for the session. |
| `sessions[].rows.canonical` | Canonical rows this run reconciled and wrote for the session. |
| `sessions[].providers` | Sources that contributed rows for the session in this run, sorted. |
| `storage.backend` | Storage backend the run used. No path is published. |
| `error` | Short top-level summary (`"2 job(s) did not complete"`) when the outcome is `partial` or `failed`, else `null`. Never a path, URL or credential. |
| `notifications[]` | Webhook delivery results, see below. Only in `runs/<run_id>.json` and `latest.json`. |

A failed job is attributed to the last session of its window (`window.end`); per-job detail
(error code, attempts) stays in the local state database (`openbars status`).

The document never contains credentials, DSNs, absolute user paths, provider URLs, or the
configured webhook URLs (which may embed tokens) — only counters, session outcomes and hashed
webhook identifiers.

`openbars status --json` prints `latest.json` verbatim, for scripts that only need the last run.

## Webhooks

```yaml
notify:
  total_timeout_seconds: 60          # overall budget for all deliveries of one run
  webhooks:
    - url: https://example.invalid/hook
      secret_env: OPENBARS_WEBHOOK_SECRET   # name of the variable; the value comes from .env / env
      timeout_seconds: 10
      max_attempts: 3
      backoff_seconds: 2
      max_backoff_seconds: 30
```

`secret_env` names an environment variable (process environment or the dotenv file, same
precedence as provider keys). The value is never written to disk or printed. A target whose
`secret_env` is missing or resolves to an empty string is **skipped** (`outcome: skipped`,
`error: no_secret`) — OpenBars never sends an unsigned request.

After the status file is written, each target receives one `POST` with
`Content-Type: application/json` and the body of `runs/<run_id>.json` **as written**, except that
the `notifications` key is not part of the payload (the signature covers exactly what is sent).

| Header | Value |
| --- | --- |
| `X-OpenBars-Event` | `run.completed` |
| `X-OpenBars-Delivery` | A UUID identifying this delivery; unique per target per payload, identical on every retry (use it to deduplicate). |
| `X-OpenBars-Timestamp` | Unix seconds, regenerated on every attempt. |
| `X-OpenBars-Signature` | `sha256=<hex>`, `hex = HMAC-SHA256(secret, "<timestamp>.<body>")` — the timestamp string, a literal `.`, then the raw request body. |

### Verifying a signature

1. Read `X-OpenBars-Timestamp` and `X-OpenBars-Signature`.
2. Reject the request when the timestamp is more than **5 minutes** away from your clock (replay
   window; tighten it if your clock is well synchronised).
3. Recompute `HMAC-SHA256(secret, f"{timestamp}.{raw_body}")` over the *raw* bytes (do not
   re-serialise the JSON) and compare it to the hex digest using a **constant-time** comparison.
4. Deduplicate on `X-OpenBars-Delivery`: retries and manual re-sends reuse the same value.

```python
import hashlib, hmac, time

def verified(secret: str, headers, body: bytes, *, tolerance: int = 300) -> bool:
    timestamp = headers["X-OpenBars-Timestamp"]
    if abs(time.time() - int(timestamp)) > tolerance:
        return False
    expected = "sha256=" + hmac.new(
        secret.encode(), f"{timestamp}.".encode() + body, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, headers["X-OpenBars-Signature"])
```

### Retries and time budget

* A `2xx` response is a delivery. `408`, `429` and `5xx` are retried; other `3xx`/`4xx` responses
  are not (retrying a rejected body cannot help).
* At most `max_attempts` attempts (default 3), exponential backoff between them
  (`backoff_seconds * 2**(attempt-1)`, capped by `max_backoff_seconds`: 2s, 4s, 8s, ... by default).
* All targets of one run share `notify.total_timeout_seconds` (default 60) so a slow or
  unreachable target can never hold up the daily run. Targets are tried in configuration order;
  when the budget is gone the remaining attempts stop (`error: delivery budget exhausted`).
* Deliveries happen after the data is written and the status file exists. A failed delivery
  affects neither the exit code of `run` nor the stored data; it only appears in
  `notifications[]` and as a warning on stderr.

### Delivery results

```json
"notifications": [
  {"url_hash": "a38229ce7e0e", "outcome": "delivered", "attempts": 1, "status_code": 200, "error": null},
  {"url_hash": "5f0c11d2aa01", "outcome": "failed", "attempts": 3, "status_code": 503, "error": "HTTP 503"},
  {"url_hash": "01b7c4e8d9f2", "outcome": "skipped", "attempts": 0, "status_code": null, "error": "no_secret"}
]
```

`url_hash` is the first 12 hex digits of `SHA-256(url)`: enough to tell targets apart, useless if
leaked. The full URL and the secret never appear in the status files, in logs, or in the
`openbars config` output. Delivery results are written to `runs/<run_id>.json` and `latest.json`
after the deliveries finish; the per-session files keep an empty list.
