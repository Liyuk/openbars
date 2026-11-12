"""Storage-layer errors."""

from __future__ import annotations

from dataclasses import dataclass

from openbars.core.errors import OpenBarsError


class StorageError(OpenBarsError):
    """Generic storage failure (bad layout, unknown instrument market, read-only write ...)."""


class ReadOnlyError(StorageError):
    """A write was attempted on a backend opened read-only."""


class SchemaVersionError(StorageError):
    """A stored file was written by a newer (or unreadable-version) openbars schema.

    The reader refuses it instead of guessing: no migration, no column truncation.
    """

    def __init__(self, path: object, found: object, supported: int) -> None:
        self.path = str(path)
        self.found = found
        self.supported = supported
        super().__init__(
            f"{self.path}: schema version {found!r} is not supported "
            f"(this openbars reads up to v{supported}); upgrade openbars or rewrite the file"
        )


@dataclass(frozen=True)
class SchemaProblem:
    """One difference between the required and the actual Postgres catalog."""

    table: str
    column: str | None
    kind: str  # missing_table | missing_column | type_mismatch
    expected: str
    actual: str | None = None

    def __str__(self) -> str:
        where = self.table if self.column is None else f"{self.table}.{self.column}"
        if self.kind == "type_mismatch":
            return f"{where}: expected {self.expected}, found {self.actual}"
        return f"{where}: {self.kind} (expected {self.expected})"


class SchemaMismatchError(StorageError):
    """The Postgres catalog does not match what the selected mode requires."""

    mode = "native"

    def __init__(self, problems: list[SchemaProblem]) -> None:
        self.problems = list(problems)
        shown = "; ".join(str(p) for p in self.problems[:10])
        more = f" (+{len(self.problems) - 10} more)" if len(self.problems) > 10 else ""
        super().__init__(f"{self.mode} schema check failed: {shown}{more}")


class UpstreamSchemaError(SchemaMismatchError):
    """``upstream_compat`` introspection found missing tables/columns or type mismatches."""

    mode = "upstream_compat"
