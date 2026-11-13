## Links

Closes #
Discussion:

## What changed

## How it was verified

<!-- Only list checks you actually ran, with their results; do not mark unrun checks as passing -->
- [ ] `uv run --extra dev pytest`

## Open items / risks

## Checklist

- [ ] No secret, `.env` or data file is committed (`data/`, `*.parquet`, `*.duckdb`)
- [ ] API or convention changes are reflected in `docs/architecture.md` / `docs/api.md`
- [ ] A new provider declares `SourceCapability`, `QuotaPolicy` and its terms
