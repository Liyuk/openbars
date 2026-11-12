"""Postgres quota store: a row lock (``SELECT ... FOR UPDATE``) per provider gate row.

Works with any DB-API 2.0 connection using the ``%s`` paramstyle (psycopg 3 by default;
the driver is only imported by :meth:`PostgresQuotaStore.from_dsn`). It needs no live
database to import, and the default test-suite does not run it against one.

Two schema modes:

``native``
    openbars' own tables ``quota_gate`` / ``quota_usage`` (same shape as the SQLite
    store, ``timestamptz`` columns). Call :meth:`PostgresQuotaStore.ensure_schema` once;
    it is the only place any DDL is issued.

``upstream_compat``
    Share the tables ``provider_request_gate`` / ``provider_request_usage`` that the
    upstream internal implementation already owns, so both sides draw on one budget when
    they share a key. **No migration is ever run**: the tables must already exist and
    their ownership stays upstream. Limits of that layout, handled here:

    * no ``scope`` column -- only scope ``"default"`` is accepted;
    * no ``weight`` column -- a weight-N reservation inserts N usage rows;
    * no penalty column -- ``penalize`` pushes ``next_request_at`` forward instead, so
      nothing is granted before ``until`` (the penalty is invisible in ``snapshot``).
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, Callable, Iterator, Literal, Sequence

from openbars.quota.store import PROVIDER_WIDE_SCOPE, GateState, QuotaTransaction, UsageRow

SchemaMode = Literal["native", "upstream_compat"]

_NATIVE_DDL = (
    """
    CREATE TABLE IF NOT EXISTS quota_gate (
        provider TEXT NOT NULL,
        scope TEXT NOT NULL,
        next_request_at TIMESTAMPTZ NOT NULL DEFAULT 'epoch',
        penalized_until TIMESTAMPTZ,
        PRIMARY KEY (provider, scope)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS quota_usage (
        id BIGSERIAL PRIMARY KEY,
        provider TEXT NOT NULL,
        scope TEXT NOT NULL,
        reserved_at TIMESTAMPTZ NOT NULL,
        weight INTEGER NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_quota_usage_window ON quota_usage (provider, scope, reserved_at)",
)

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


class PostgresQuotaStore:
    def __init__(
        self,
        connect: Callable[[], Any],
        *,
        mode: SchemaMode = "native",
    ) -> None:
        if mode not in ("native", "upstream_compat"):
            raise ValueError(f"unknown schema mode {mode!r}")
        self._connect = connect
        self.mode = mode
        self.compat = mode == "upstream_compat"
        self.gate_table = "provider_request_gate" if self.compat else "quota_gate"
        self.usage_table = "provider_request_usage" if self.compat else "quota_usage"

    @classmethod
    def from_dsn(cls, dsn: str, *, mode: SchemaMode = "native") -> "PostgresQuotaStore":
        import psycopg  # optional extra: openbars[postgres]

        return cls(lambda: psycopg.connect(dsn), mode=mode)

    def ensure_schema(self) -> None:
        if self.compat:
            raise RuntimeError("upstream_compat never migrates; the tables are owned upstream")
        conn = self._connect()
        try:
            with conn:
                cur = conn.cursor()
                for statement in _NATIVE_DDL:
                    cur.execute(statement)
        finally:
            conn.close()

    def _check_scope(self, scope: str) -> None:
        if self.compat and scope not in ("default", PROVIDER_WIDE_SCOPE):
            raise ValueError("upstream_compat has no scope column; only scope 'default' is supported")

    @contextmanager
    def transaction(self, provider: str, scope: str) -> Iterator[QuotaTransaction]:
        self._check_scope(scope)
        conn = self._connect()
        try:
            cur = conn.cursor()
            self._lock_gate_row(cur, provider, scope)
            yield _PostgresTransaction(self, cur, provider, scope)
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _lock_gate_row(self, cur: Any, provider: str, scope: str) -> None:
        # Make sure the row exists, then take its row lock for the whole transaction.
        if self.compat:
            cur.execute(
                f"INSERT INTO {self.gate_table} (provider, next_request_at) VALUES (%s, %s) "
                "ON CONFLICT (provider) DO NOTHING",
                (provider, _EPOCH),
            )
            cur.execute(
                f"SELECT next_request_at FROM {self.gate_table} WHERE provider = %s FOR UPDATE",
                (provider,),
            )
        else:
            cur.execute(
                f"INSERT INTO {self.gate_table} (provider, scope) VALUES (%s, %s) "
                "ON CONFLICT (provider, scope) DO NOTHING",
                (provider, scope),
            )
            cur.execute(
                f"SELECT next_request_at FROM {self.gate_table} "
                "WHERE provider = %s AND scope = %s FOR UPDATE",
                (provider, scope),
            )
        cur.fetchall()

    def _load(
        self, cur: Any, provider: str, scope: str, since: datetime
    ) -> tuple[GateState, Sequence[UsageRow]]:
        next_at: datetime | None = None
        penalty: datetime | None = None
        if self.compat:
            cur.execute(
                f"SELECT next_request_at FROM {self.gate_table} WHERE provider = %s", (provider,)
            )
            for (value,) in cur.fetchall():
                next_at = _aware(value) if value > _EPOCH else None
            cur.execute(
                f"SELECT reserved_at FROM {self.usage_table} "
                "WHERE provider = %s AND reserved_at >= %s ORDER BY reserved_at, id",
                (provider, since),
            )
            return GateState(next_at, None), [UsageRow(_aware(at), 1) for (at,) in cur.fetchall()]
        cur.execute(
            f"SELECT scope, next_request_at, penalized_until FROM {self.gate_table} "
            "WHERE provider = %s AND scope IN (%s, %s)",
            (provider, scope, PROVIDER_WIDE_SCOPE),
        )
        penalties = []
        for row_scope, next_value, penalized in cur.fetchall():
            if row_scope == scope and next_value > _EPOCH:
                next_at = _aware(next_value)
            if penalized is not None:
                penalties.append(_aware(penalized))
        penalty = max(penalties) if penalties else None
        cur.execute(
            f"SELECT reserved_at, weight FROM {self.usage_table} "
            "WHERE provider = %s AND scope = %s AND reserved_at >= %s ORDER BY reserved_at, id",
            (provider, scope, since),
        )
        return GateState(next_at, penalty), [UsageRow(_aware(at), w) for at, w in cur.fetchall()]

    def penalize(self, provider: str, until: datetime, scope: str | None = None) -> None:
        target = PROVIDER_WIDE_SCOPE if scope is None else scope
        if self.compat:
            if scope not in (None, "default"):
                self._check_scope(target)
            sql = (
                f"INSERT INTO {self.gate_table} (provider, next_request_at) VALUES (%s, %s) "
                "ON CONFLICT (provider) DO UPDATE SET next_request_at = "
                f"GREATEST({self.gate_table}.next_request_at, EXCLUDED.next_request_at)"
            )
            params: tuple[Any, ...] = (provider, until)
        else:
            sql = (
                f"INSERT INTO {self.gate_table} (provider, scope, penalized_until) "
                "VALUES (%s, %s, %s) ON CONFLICT (provider, scope) DO UPDATE SET "
                f"penalized_until = GREATEST({self.gate_table}.penalized_until, "
                "EXCLUDED.penalized_until)"
            )
            params = (provider, target, until)
        conn = self._connect()
        try:
            with conn:
                conn.cursor().execute(sql, params)
        finally:
            conn.close()

    def read(
        self, provider: str, scope: str, since: datetime
    ) -> tuple[GateState, Sequence[UsageRow]]:
        self._check_scope(scope)
        conn = self._connect()
        try:
            with conn:
                return self._load(conn.cursor(), provider, scope, since)
        finally:
            conn.close()

    def scopes(self, provider: str) -> Sequence[str]:
        if self.compat:
            return []
        conn = self._connect()
        try:
            with conn:
                cur = conn.cursor()
                cur.execute(
                    f"SELECT scope FROM {self.gate_table} WHERE provider = %s AND scope != %s "
                    "ORDER BY scope",
                    (provider, PROVIDER_WIDE_SCOPE),
                )
                return [row[0] for row in cur.fetchall()]
        finally:
            conn.close()

    def close(self) -> None:
        """Connections are opened per operation; nothing to release."""


class _PostgresTransaction:
    def __init__(self, store: PostgresQuotaStore, cur: Any, provider: str, scope: str) -> None:
        self._store = store
        self._cur = cur
        self._provider = provider
        self._scope = scope

    def load(self, since: datetime) -> tuple[GateState, Sequence[UsageRow]]:
        return self._store._load(self._cur, self._provider, self._scope, since)

    def record(
        self, slot: datetime, weight: int, next_request_at: datetime, prune_before: datetime
    ) -> None:
        store, cur, provider, scope = self._store, self._cur, self._provider, self._scope
        if store.compat:
            cur.execute(
                f"UPDATE {store.gate_table} SET next_request_at = %s WHERE provider = %s",
                (next_request_at, provider),
            )
            for _ in range(weight):  # one row per unit: the upstream layout has no weight
                cur.execute(
                    f"INSERT INTO {store.usage_table} (provider, reserved_at) VALUES (%s, %s)",
                    (provider, slot),
                )
            cur.execute(
                f"DELETE FROM {store.usage_table} WHERE provider = %s AND reserved_at < %s",
                (provider, prune_before),
            )
            return
        cur.execute(
            f"UPDATE {store.gate_table} SET next_request_at = %s WHERE provider = %s AND scope = %s",
            (next_request_at, provider, scope),
        )
        if weight > 0:
            cur.execute(
                f"INSERT INTO {store.usage_table} (provider, scope, reserved_at, weight) "
                "VALUES (%s, %s, %s, %s)",
                (provider, scope, slot, weight),
            )
        cur.execute(
            f"DELETE FROM {store.usage_table} WHERE provider = %s AND scope = %s AND reserved_at < %s",
            (provider, scope, prune_before),
        )
