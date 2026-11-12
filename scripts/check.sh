#!/usr/bin/env bash
# Local one-shot checks for openbars: tests, sanitization, secret scan.
# GitHub Actions is manual-only (workflow_dispatch); run this before every push.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> pytest"
uv run pytest

echo "==> check_sanitized (working tree)"
uv run python scripts/check_sanitized.py

echo "==> check_sanitized (full git history)"
uv run python scripts/check_sanitized.py --history

gitleaks_cmd() {
  if command -v gitleaks >/dev/null 2>&1; then
    echo "    using $(command -v gitleaks)"
    gitleaks "$@"
  elif command -v docker >/dev/null 2>&1; then
    echo "    using docker zricethezav/gitleaks:v8.30.1"
    docker run --rm -v "$PWD:/repo" -w /repo zricethezav/gitleaks:v8.30.1 "$@"
  else
    echo "error: gitleaks not found and docker is unavailable." >&2
    echo "install gitleaks, or start Docker to use zricethezav/gitleaks:v8.30.1" >&2
    exit 1
  fi
}

echo "==> gitleaks (full git history)"
gitleaks_cmd git --config .gitleaks.toml --redact --verbose --exit-code 1 .

echo "==> gitleaks (working tree)"
gitleaks_cmd dir --config .gitleaks.toml --redact --verbose --exit-code 1 .

echo "All checks passed."
