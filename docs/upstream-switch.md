# Optional compatibility backend and downstream adapters

OpenBars writes local Parquet + DuckDB by default and keeps quota state in local SQLite. An
optional Postgres compatibility mode (`storage.postgres` with `mode: upstream_compat`) is
retained for controlled migrations and compatibility tests only. It is **not** the default,
**not** a routine production write path, and OpenBars never switches an explicit configuration
on its own.

## What the mode assumes

The mode mirrors a **pre-existing external schema** instead of owning it: a bar table, a symbol
alias table with validity windows, a canonical series table, and optional request-gate / usage
tables. That schema keeps its original owner, and the concrete table DDL, foreign keys and
deployment checklist stay outside this repository.

- **Introspection, not migration.** The columns are read at startup and a missing table or
  column, or a mismatched type, aborts the run with a report of the differences. OpenBars never
  runs DDL or migrations against that schema.
- **Read-only by default.** Writes need an explicit `storage.postgres.write: true`. A dry-run
  `plan` (no `--submit`) opens the backend read-only even then, so it introspects but writes no
  jobs or bars.
- **Local state.** Jobs, checkpoints and the fetch log stay in `{storage.path}/state.db`. The
  quota gate stays the local SQLite gate unless you explicitly opt into the external gate
  tables for a migration or test.
- **US daily bars only.** Other markets and intervals use the default Parquet + DuckDB path.
- **DSN at runtime.** Supply the Postgres DSN through the environment or an env file and keep it
  out of the repository; `openbars config` only reports whether a secret is set, and never
  prints it.

Instrument identity stays in OpenBars terms: internal ids are opaque strings, while an external
schema usually keys instruments with its own numbers. A symbol can be bootstrapped through the
alias table, but prefer explicit keys in migration and test fixtures — symbol-only bootstrapping
can mis-merge a reused ticker.

## Downstream adapters

If a downstream system needs its own official tables, do not point the compatibility backend at
them. Write an adapter that reads OpenBars data and maps it into your schema:

1. Read canonical rows from the default Parquet + DuckDB store (or the documented read API).
2. Fill provenance columns from the generic contract: source identity
   (`provider` / `feed` / `adjustment`), fetch batch (`source_fetch_id`), freshness
   (`freshness`), availability time (`available_at`), and the canonical `policy_version`.
3. Keep instrument identity mapping in the adapter: an OpenBars `instrument_id` is an opaque
   string, not your official numeric key.
4. Keep your own DDL, foreign keys and runbook in your own repository.

The optional `openbars export-postgres` command is a reference implementation of the generic
`bars + source information` layout: it only **writes** the generic `openbars_bars` table, never
creates, alters or targets caller tables, and OpenBars never reads it back. See
[Optional Postgres export](postgres-export.md).

## Migration checklist (non-production)

1. **Read-only comparison.** Run OpenBars against Parquet and compare coverage, prices, volumes
   and quality flags with the series you are migrating from (`openbars shadow` /
   `openbars verify`).
2. **Writes against a disposable database.** Enable `write: true` only against a throwaway
   database that holds a copy of the target schema, never a live one.
3. **Keep the default.** Once migrated, core writes stay on Parquet + DuckDB with the local
   SQLite quota gate; use the compatibility backend only for controlled migrations and tests.
