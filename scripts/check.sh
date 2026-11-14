#!/usr/bin/env bash
# Local one-shot checks for openbars: tests, sanitization, secret scan.
# CI (.github/workflows/ci.yml) runs these on pull requests and pushes to main;
# run this locally before every push.
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
    # Worktrees store objects in the main .git dir; mount it at the same host path
    # so gitleaks can walk history (a lone -v "$PWD:/repo" is not enough).
    docker_args=(--rm -v "$PWD:/repo")
    git_common=$(git rev-parse --path-format=absolute --git-common-dir 2>/dev/null || true)
    if [ -n "${git_common}" ] && [ -d "${git_common}" ] && [ "${git_common}" != "$PWD/.git" ]; then
      docker_args+=(-v "${git_common}:${git_common}")
    fi
    docker run "${docker_args[@]}" -w /repo zricethezav/gitleaks:v8.30.1 "$@"
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
