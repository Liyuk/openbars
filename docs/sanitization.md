# Sanitization rules and pre-release checklist

This repository is prepared for public release, so everything that lands in it — code, docs,
tests, commit messages, issues and PR text — must be safe to publish. This file defines what
must never enter the repository, how to handle it, and the checks that must pass before a
release.

## 1. Guardrails

| Guardrail | Location | When it runs | What it checks |
| --- | --- | --- | --- |
| gitleaks | `.gitleaks.toml` | pre-commit, local `scripts/check.sh` (full history + working tree) | default rules plus market-data provider credentials (Alpaca, Massive/Polygon, Twelve Data, Alpha Vantage, Tushare, Binance/OKX, etc.) and `apikey=` in URLs |
| Sanitization check | `scripts/check_sanitized.py` | pre-commit, local `scripts/check.sh` (working tree + full history), manual `--stdin` | local home paths, private directories, internal/overlay network addresses and domains, personal email addresses, credential-looking assignments, private terms (hashed) |
| Data-file block | `.pre-commit-config.yaml` | pre-commit | refuses to commit parquet / duckdb / sqlite and similar data files |
| `.gitignore` | repository root | always | `.env*`, `config.yaml`, `data/` stay out of git |

Private terms (unrelated product names, hostnames, user names, ticket prefixes, etc.) are stored
only as **salted SHA-256 hashes** in `scripts/sanitize_hashes.txt`, so the denylist itself does
not disclose them. Maintainers may keep an additional plaintext list outside the repository and
point the `OPENBARS_PRIVATE_DENYLIST` environment variable at it as a supplement. To add a
private term:

```bash
echo "new term" | python3 scripts/check_sanitized.py --hash-term >> scripts/sanitize_hashes.txt
```

The check prints only `file:line: rule`; it never prints the matched value.

## 2. Handling rules

| Category | Examples (described, never literal) | Rule |
| --- | --- | --- |
| Keys / tokens | provider API keys, secrets, database passwords | Always use environment variables or `.env` (not committed); code contains only variable names; `config.example.yaml` / `.env.example` contain empty values or `${VAR}` placeholders only. Resolution priority (highest wins): process environment, then dotenv (`--env-file`, else `OPENBARS_ENV_FILE`, else `config.yaml`'s `env_file`), then `${VAR:-default}`. `openbars config` shows only `set` / `missing` for secrets. Credentials in logs and error messages are replaced with `[REDACTED]` before they are written. |
| User-Agent required by SEC etc. | UA with a contact email | No default; the user supplies it in config and a missing value is an error. Documentation examples use the `example.com` domain. |
| Personal email | commit author email, email in comments | Forbidden in files; commit authors use a GitHub noreply address (run `--strict-authors` before going public; see §4). |
| Local paths | user home directory, private document folders | Replace with a relative path or a config option (such as `data_dir`); examples use `./data`. |
| Internal hosts / IPs | overlay network addresses and domains, private IPs, internal machine aliases | Remove them; remote node addresses may only come from user configuration. |
| Paid-plan information | which tier the maintainer subscribes to | Do not write it. Public **free-tier** names and limits may be documented with official links, as overridable defaults. |
| Unrelated private business code | billing, entitlements / permission logic, internal workflow IDs, internal ticket numbers | Do not commit or reference it. Write the "why" as a neutral technical description; OpenBars' own issue numbers are fine. |
| Third-party product names | other platforms' product, strategy or workflow names | Do not copy them in; use neutral technical names (for example `research` → `canonical`, keeping the OpenBars policy version strings as they are). |
| Hard-coded symbol universes | index constituents, benchmark ETF lists, watchlists | Do not hard-code them; the default universe comes from the provider's whole-market listing plus user configuration. Tests use synthetic symbols only (such as `TEST1`, `ZZZA`). |
| Real market data | captured provider responses, exported bars | Never commit them (including test fixtures). Fixtures are synthetic and follow the shape of the official documentation. |

## 3. Daily workflow

The one-shot check runs at the repository root as `./scripts/check.sh`: it runs `uv run pytest`,
`uv run python scripts/check_sanitized.py` (working tree + `--history`), and gitleaks (full
history + working tree, using `.gitleaks.toml`). If `gitleaks` is installed locally it is used
directly; otherwise the Docker image `zricethezav/gitleaks:v8.30.1` is used. CI runs the same
checks in `.github/workflows/ci.yml` on pull requests, on pushes to `main` and on manual
`workflow_dispatch`, on standard GitHub-hosted runners. Run `./scripts/check.sh` locally before
pushing rather than relying on CI to catch problems.

1. Optionally install the hooks: `pipx install pre-commit && pre-commit install && pre-commit install --hook-types pre-push` (the machine needs Go to build the gitleaks hook, or install gitleaks yourself and switch the hook to `language: system`). The pre-push hook calls `scripts/check.sh`.
2. Before posting an issue / PR / Discussion body: `pbpaste | python3 scripts/check_sanitized.py --stdin`.
3. `./scripts/check.sh` must pass locally before merging. GitHub Actions repeats these checks on pull requests and pushes to `main`; it is a safety net, not a substitute for the local run.

## 4. Pre-publication checklist

- [ ] `python3 scripts/check_sanitized.py` passes (working tree).
- [ ] `python3 scripts/check_sanitized.py --history --release` passes (all history, commit messages, author emails; no baseline). `scripts/sanitize_history_baseline.txt` records known issues in history that was pushed before the guardrails existed; the daily `--history` run reports them as warnings only. They must be zero before going public; how to clear them (and whether to start the public repository from a fresh commit) is a separate maintainer decision — do not rewrite published history as part of ordinary work.
- [ ] `gitleaks git --config .gitleaks.toml --redact -v .` and `gitleaks dir --config .gitleaks.toml --redact -v .` both report no findings.
- [ ] Every issue, PR and Discussion body and comment has been checked with `--stdin`; no home paths, package names, repository names or internal ticket numbers.
- [ ] **Edit history**: GitHub keeps previous versions of issue / PR / Discussion bodies and comments, visible to anyone with read access. Items written before sanitization that still contain private information must be cleaned up (their old versions removed, or the item closed and recreated cleanly) before the repository goes public.
- [ ] The repository contains no data files and no real provider responses; all test fixtures are synthetic.
- [ ] The README's provider-terms section is complete: OpenBars does not distribute data, users bring their own keys, and users follow each provider's terms.
- [ ] GitHub repository settings: enable secret scanning and push protection; if Actions is triggered manually, it must not be configured with any secret.
- [ ] The maintainer has reviewed `scripts/sanitize_hashes.txt` and confirmed it covers all current private terms.
