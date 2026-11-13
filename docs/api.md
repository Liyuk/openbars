# `/api/v1`: self-hosted read API

`openbars serve` starts a small **read-only** HTTP service over the data that is already
stored on disk. It powers the bundled preview UI, and it is also a **public, self-hosted
API**: whoever downloads OpenBars deploys it on their own machine (or their own private
infrastructure) and calls it there. There is no hosted service and no account.

- Base URL: `http://127.0.0.1:8787/api/v1` by default (`openbars serve --port` changes the
  port; the host is loopback by default and a non-loopback bind needs an explicit opt-in,
  see [Exposure](#exposure-and-security)).
- The service never plans, fetches, or writes. It opens storage read-only and only answers
  queries about rows that are already there.
- Responses are JSON, UTF-8. All prices and volumes are **decimal strings** (for example
  `"187.64000000"`), never binary floats. `null` means "unknown", never `0`.
- Optional dependency: `uv sync --extra http` (FastAPI + Uvicorn).

## Versioning and stability

- **`/api/v1` is the current public API version.** It is the version documented on this page.
- **Breaking changes go to a new major prefix** (`/api/v2`). Removing a field, changing a
  field's type or meaning, or changing a documented default is a breaking change.
- **Documented fields are stable.** New optional fields may be added inside v1 at any time;
  clients must ignore fields they do not know. Fields that exist in a response but are not
  documented here are implementation details: do not depend on them.
- Route shapes, status codes and the error envelope below are part of the contract.
  Parameter validation limits (page sizes, bar caps) may be relaxed in a minor release, not
  tightened for existing defaults.
- The UI's own assets (`/`, `/chart`, `/flags`, `/assets/*`) are not part of the API
  contract; they change with the UI.
- OpenAPI / ReDoc are intentionally disabled (`/docs` and `/openapi.json` are not served).
  This page is the reference.

## Endpoints

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/api/v1/health` | liveness and version |
| GET | `/api/v1/markets` | per-market coverage summary |
| GET | `/api/v1/instruments` | instrument master search / pagination |
| GET | `/api/v1/bars` | OHLCV bars for one instrument (canonical or a raw provider) |
| GET | `/api/v1/flags` | quality-flagged canonical bars |
| GET | `/api/v1/quota` | provider quota usage snapshot (no secrets) |

Common conventions:

- Dates are `YYYY-MM-DD` (ISO 8601). Timestamps are ISO 8601 with an explicit offset.
- `interval` accepts `1d` (default) or `1h`. Anything else is a `400`.
- `market` accepts `us`, `crypto`, `cn`, `hk`.
- Unknown query parameters are ignored; invalid values are rejected with `400`.

### `GET /api/v1/health`

Liveness probe. Always returns `200` with no parameters.

```json
{"ok": true, "version": "0.1.0"}
```

### `GET /api/v1/markets`

Coverage summary for every market that has stored canonical bars.

| Parameter | Type | Default | Notes |
| --- | --- | --- | --- |
| `interval` | string | `1d` | `1d` or `1h` |

```json
{
  "markets": [
    {
      "market": "us",
      "instruments": 103,
      "bars": 268400,
      "first_session": "2015-12-31",
      "last_session": "2026-11-12",
      "latest_session": "2026-11-12",
      "latest_freshness": {"final": 101, "provisional": 2},
      "latest_available_at": "2026-11-13T05:01:12+00:00",
      "observed_session_coverage": {"present": 103, "instruments": 103},
      "flags": {"divergent": 0, "spike_suspect": 1, "volume_missing": 4, "single_source": 103}
    }
  ]
}
```

- `bars` and `instruments` count stored canonical rows / distinct instruments in that market.
- `latest_freshness` counts **the latest stored session only** by freshness tier
  (`final`, `provisional`).
- `observed_session_coverage.present` is how many instruments have a row for the latest
  stored session. It is **observed** coverage of stored data, not a completeness guarantee.
- `flags` counts canonical rows per quality flag.

### `GET /api/v1/instruments`

Search / paginate the instrument master.

| Parameter | Type | Default | Notes |
| --- | --- | --- | --- |
| `market` | string | all | `us` / `crypto` / `cn` / `hk` |
| `q` | string | — | case-insensitive substring of id, name or any known symbol |
| `limit` | int | `50` | 1…200 |
| `offset` | int | `0` | `>= 0` |

```json
{
  "items": [
    {
      "instrument_id": "us:eq:01J8Z4C0S5T0R9X2QK7M3N8P4V",
      "symbol": "TEST1",
      "symbols": ["TEST1"],
      "name": "Test One Inc.",
      "market": "us",
      "listed_on": "2001-06-15",
      "delisted_on": null,
      "last_session": "2026-11-12"
    }
  ],
  "total": 1,
  "limit": 50,
  "offset": 0
}
```

`symbol` is the symbol valid today when one exists; `symbols` lists every symbol ever mapped
to the instrument. `last_session` is the newest stored canonical session for that instrument
(`null` when nothing is stored yet). `total` is the filtered count before pagination.

### `GET /api/v1/bars`

OHLCV bars for one instrument over a window, from the canonical layer (default) or from one
raw provider.

| Parameter | Type | Default | Notes |
| --- | --- | --- | --- |
| `id` | string | — | `instrument_id`; or, with `id_kind=upstream`, an upstream integer id |
| `symbol` | string | — | ticker; resolved per session date |
| `market` | string | inferred | required when the instrument is unknown, and required with `id_kind=upstream` |
| `interval` | string | `1d` | `1d` or `1h` |
| `start` | date | stored range | inclusive |
| `end` | date | latest/today | inclusive |
| `adjustment` | string | `split` | `raw`, `split`, `split_dividend`, `all`, `qfq`, `hfq` (derived at read time) |
| `as_of` | date | — | knowledge date: rows generated after it are hidden, and it is the adjustment as-of |
| `source` | string | `canonical` | `canonical` or a provider id present in `available_sources` |
| `id_kind` | string | `auto` | `auto` or `upstream` |

`id` **or** `symbol` is required. `id_kind=upstream` matches the stored `instrument_id`
verbatim (no symbol mapping, no instrument-master lookup) and returns `symbol: null`.

Response:

```json
{
  "instrument_id": "us:eq:01J8Z4C0S5T0R9X2QK7M3N8P4V",
  "symbol": "TEST1",
  "market": "us",
  "interval": "1d",
  "adjustment": "split",
  "source": "canonical",
  "available_sources": ["canonical", "alpaca"],
  "start": "2026-11-01",
  "end": "2026-11-12",
  "bars": [
    {
      "instrument_id": "us:eq:01J8Z4C0S5T0R9X2QK7M3N8P4V",
      "session_date": "2026-11-12",
      "ts_start": "2026-11-12T14:30:00+00:00",
      "session": "regular",
      "open": "187.64000000",
      "high": "189.10000000",
      "low": "186.90000000",
      "close": "188.75000000",
      "volume": "3210400.00000000",
      "flags": ["single_source"],
      "provider": "alpaca",
      "adjustment": "split",
      "available_at": "2026-11-13T05:01:12+00:00",
      "freshness": "final",
      "missing_fields": []
    }
  ],
  "missing_sessions": ["2026-11-04"],
  "missing_hours": [],
  "note": null
}
```

- `bars` is sorted by `ts_start` / `session_date`. Canonical bars carry `flags`, `provider`,
  `available_at` and `freshness`; raw bars additionally carry `ts_end` and
  `missing_fields` and have no quality flags.
- `missing_sessions` lists expected trading sessions with no stored bar (daily only).
  `missing_hours` lists expected hourly slots that are absent when the hourly alignment is
  unambiguous.
- `note` is a short human-readable caveat (`null` when there is none).
- `available_sources` always contains `canonical` when the instrument has canonical rows, plus
  the raw provider ids stored for that window.
- `freshness` is `provisional` or `final` (a missing value means `final`). It describes
  timeliness only; it is not a quality flag and not a completeness guarantee.
- Request caps: a window is limited to 8000 sessions (or hourly slots). Larger windows return
  `400`; request a smaller `start` / `end`.
- Hourly notes: `as_of` is not supported for canonical hourly bars, and a non-`canonical`
  hourly `source` without `start`/`end` defaults to the most recent sessions that fit the cap.

### `GET /api/v1/flags`

Canonical bars carrying at least one quality flag.

| Parameter | Type | Default | Notes |
| --- | --- | --- | --- |
| `market` | string | all markets | `us` / `crypto` / `cn` / `hk` |
| `flag` | string | all | one of `divergent`, `spike_suspect`, `volume_missing`, `single_source`, `filled_from_fallback`, `stale`, `gap`, `revised`, `adjustment_unverified`, `limit_up`, `limit_down`, `suspended` |
| `interval` | string | `1d` | `1d` or `1h` |
| `start` / `end` | date | stored range | inclusive session bounds |
| `limit` | int | `100` | 1…500 |
| `offset` | int | `0` | `>= 0` |

```json
{
  "counts": {"divergent": 0, "spike_suspect": 1, "volume_missing": 4, "single_source": 103},
  "items": [
    {
      "instrument_id": "us:eq:01J8Z4C0S5T0R9X2QK7M3N8P4V",
      "symbol": "TEST1",
      "market": "us",
      "session_date": "2026-11-12",
      "ts_start": "2026-11-12T14:30:00+00:00",
      "session": "regular",
      "flags": ["single_source"]
    }
  ],
  "total": 108,
  "limit": 100,
  "offset": 0
}
```

`counts` covers every flag found in the scanned window (before the `flag` filter and before
pagination); `total` is the number of matching rows after filtering, before pagination.

### `GET /api/v1/quota`

Sanitized snapshot of the quota gate. Takes no parameters.

```json
{
  "available": true,
  "providers": [
    {
      "id": "massive",
      "provider": "massive",
      "used": {"minute": 1, "day": 42},
      "remaining": {"minute": 3, "day": 730},
      "penalized_until": null
    }
  ]
}
```

- `available` is `false` (with `providers: []`) when no quota store is open. The snapshot
  never creates `quota.db`; it is read only if it already exists.
- Secret-like keys (any key containing `key`, `secret`, `token`, `password`, `dsn` or
  `credential`) are stripped server-side before the response is built. No key material is
  ever returned.

## Errors

Errors use one envelope: `{"detail": "<message>"}`.

| Status | When |
| --- | --- |
| `400` | invalid parameter (unknown market/interval/flag, bad date, unknown symbol, `as_of` on hourly canonical bars, window over the bar cap) |
| `405` | the route exists but the method is not allowed (every route is `GET`; the service is read-only) |
| `503` | no storage backend is open (the service started without readable storage) |
| `500` | unexpected server error |

`detail` is meant for humans and may change wording; branch on the status code, not on the
message text. Omitted optional parameters fall back to their documented defaults rather than
erroring.

Example:

```bash
curl -s 'http://127.0.0.1:8787/api/v1/bars?symbol=TEST1&market=us&start=2026-11-01' | jq '.bars[-1]'
curl -s 'http://127.0.0.1:8787/api/v1/markets' | jq '.markets[].market'
```

## Exposure and security

- The default bind is **loopback only**: `127.0.0.1:8787`. A non-loopback `--host` is
  refused with a usage error (exit code 2) and the server never starts.
- Binding the service to a non-loopback address requires the explicit, opt-in
  `--allow-non-loopback` flag, for example:
  `openbars serve --host 203.0.113.10 --allow-non-loopback`. Passing the flag while binding
  loopback is allowed and changes nothing. When the service really binds a non-loopback
  address it prints a startup warning that it is reachable from the network and has no
  built-in authentication.
- If you expose the service on a non-loopback interface (a network card, a container
  network, a public address), you are publishing a read-only view of your market data to
  that network. **There is no built-in authentication and no built-in TLS.** You are
  responsible for network isolation, a firewall, or an authenticating reverse proxy.
  `--allow-non-loopback` only lifts the loopback restriction; it does not add any security.
- The service never accepts writes, so exposure cannot corrupt data; it can still leak what
  you have stored (coverage, instruments, prices, quota usage).
- `/api/v1` takes no credentials. Treat it as trusted-local unless you put authentication in
  front of it.

## What this API is not

- It is not a data redistribution license. The data it serves was fetched with your own
  provider keys; the provider terms in [provider-keys.md](provider-keys.md) still apply to
  anyone you expose it to.
- It is not a full market-data endpoint: no real-time streaming, no ticks, no fundamentals,
  no corporate-action queries, no writes.
- It does not replace the Python reader. For bulk analysis use
  `open_backend(path, read_only=True)` and `backend.reader.bars(...)`, or read the Parquet
  files / DuckDB views directly (see the README's "Reading Parquet without the service").
