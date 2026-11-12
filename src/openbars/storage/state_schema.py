"""State-table specs (jobs, checkpoints, fetch_log) and DDL generation.

Kept free of pyarrow so the SQLite state store works without the ``parquet`` extra.
"""

from __future__ import annotations

from dataclasses import dataclass

SCHEMA_VERSION = 1


def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


# --- state tables (SQLite default; Postgres native mirrors them 1:1) ---------------------
# Logical types: text | int | date | timestamp | json. ``quota_*`` table names are reserved
# for the QuotaGate and intentionally absent here.


@dataclass(frozen=True)
class StateColumn:
    name: str
    kind: str
    nullable: bool = False


@dataclass(frozen=True)
class StateTable:
    name: str
    columns: tuple[StateColumn, ...]
    primary_key: tuple[str, ...]
    indexes: tuple[tuple[str, ...], ...] = ()


def _c(name: str, kind: str = "text", nullable: bool = False) -> StateColumn:
    return StateColumn(name, kind, nullable)


STATE_TABLES: tuple[StateTable, ...] = (
    StateTable(
        "schema_meta",
        (_c("key"), _c("value")),
        ("key",),
    ),
    StateTable(
        "jobs",
        (
            _c("job_id"),
            _c("kind"),
            _c("provider"),
            _c("market"),
            _c("interval"),
            _c("instruments", "json"),  # JSON array; ["*"] = whole market
            _c("window_start", "date"),
            _c("window_end", "date"),
            _c("priority", "int"),
            _c("node", nullable=True),
            _c("state"),
            _c("attempts", "int"),
            _c("retry_at", "timestamp", True),
            _c("last_error", nullable=True),
            _c("created_at", "timestamp"),
            _c("updated_at", "timestamp"),
        ),
        ("job_id",),
        (("state", "priority"),),
    ),
    StateTable(
        "checkpoints",
        (_c("job_id"), _c("cursor", nullable=True), _c("updated_at", "timestamp")),
        ("job_id",),
    ),
    StateTable(
        "fetch_log",
        (
            _c("provider"),
            _c("fetch_id"),
            _c("job_id", nullable=True),
            _c("market"),
            _c("interval"),
            _c("window_start", "date"),
            _c("window_end", "date"),
            _c("status"),  # started | ok | empty | partial | error
            _c("artifact_ref", nullable=True),
            _c("row_count", "int", True),
            _c("error_code", nullable=True),
            _c("error_message", nullable=True),
            _c("started_at", "timestamp"),
            _c("finished_at", "timestamp", True),
        ),
        ("provider", "fetch_id"),
        (("provider", "market", "interval", "window_start"), ("job_id",)),
    ),
)

FETCH_STATUSES = frozenset({"started", "ok", "empty", "partial", "error"})

_SQLITE_KINDS = {"text": "TEXT", "int": "INTEGER", "date": "TEXT", "timestamp": "TEXT", "json": "TEXT"}
_PG_KINDS = {"text": "TEXT", "int": "INTEGER", "date": "DATE", "timestamp": "TIMESTAMPTZ", "json": "JSONB"}


def state_ddl(dialect: str, *, schema: str | None = None) -> list[str]:
    """CREATE TABLE / INDEX statements for the state tables (``sqlite`` or ``postgres``)."""
    kinds = _SQLITE_KINDS if dialect == "sqlite" else _PG_KINDS
    prefix = f"{qident(schema)}." if schema else ""
    out: list[str] = []
    for table in STATE_TABLES:
        cols = [
            f"{qident(c.name)} {kinds[c.kind]}{'' if c.nullable else ' NOT NULL'}"
            for c in table.columns
        ]
        cols.append("PRIMARY KEY (" + ", ".join(qident(c) for c in table.primary_key) + ")")
        out.append(f"CREATE TABLE IF NOT EXISTS {prefix}{qident(table.name)} (\n  " + ",\n  ".join(cols) + "\n)")
        for idx in table.indexes:
            name = f"ix_{table.name}_{'_'.join(idx)}"
            out.append(
                f"CREATE INDEX IF NOT EXISTS {qident(name)} ON {prefix}{qident(table.name)} ("
                + ", ".join(qident(c) for c in idx)
                + ")"
            )
    return out
