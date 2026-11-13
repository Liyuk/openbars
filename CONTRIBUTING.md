# Contributing

**English** | [中文](README.zh-CN.md)

## From idea to delivery

Discuss the goal, design and acceptance of features and general improvements in [Discussions](https://github.com/Liyuk/openbars/discussions) first. Once there is consensus, open an Issue that links back to the Discussion and states the scope, acceptance criteria and dependencies. Claim the Issue, implement it on a feature branch, and open a PR. Link the Issue with `Closes #<number>`; use `Refs #<number>` when the PR only covers part of the scope.

Bugs can go straight into a Bug Issue with reproduction steps, expected vs. actual behavior, environment and evidence. Do not put credentials, local paths, personal email addresses or internal proprietary names in Issues, PRs or Discussions. Before posting, you can check text with `pbpaste | python3 scripts/check_sanitized.py --stdin`. See [docs/sanitization.md](docs/sanitization.md) for the rules.

## Implementation conventions

Implement locally with your preferred editor or coding agent. Do not mention internal tooling names or private infrastructure in the repository.

Work on a feature branch; do not commit directly to `main`. Keep commit messages short and descriptive. Do not force push and do not rewrite published history.

## Local checks

Before merging, the following must pass at the repository root:

```bash
./scripts/check.sh
```

It runs pytest, the sanitization check (working tree + full history) and gitleaks in sequence. If `gitleaks` is installed locally it is used directly; otherwise the script uses the Docker image `zricethezav/gitleaks:v8.30.1`.

GitHub Actions is not used day to day. The repository keeps `.github/workflows/ci.yml`, but its only trigger is manual `workflow_dispatch`, so it does not consume paid minutes. Do not wait for or trigger paid checks in order to merge.

Optionally install the git hooks (this does not change global git configuration):

```bash
pipx install pre-commit && pre-commit install && pre-commit install --hook-types pre-push
```

The pre-push hook calls `scripts/check.sh`.

## Merging

The maintainer squash-merges OpenBars PRs after `scripts/check.sh` passes locally.

## Local Docker tests (optional)

When you need Postgres or another dependency, you can start a one-off test environment in local Docker. This is development verification only, not a production deployment.

- Use the `openbars-test` prefix for container, volume and network names.
- Do not conflict with services already running on the machine (for example, do not bind `127.0.0.1:8000`).
- Do not touch other compose projects, containers or volumes on the machine.
- Stop and remove your `openbars-test*` containers, volumes and networks as soon as the test is done.
- The repository never commits market data; `data/` is already in `.gitignore`.
