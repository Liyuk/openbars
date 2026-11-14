# Security Policy

## Supported versions

OpenBars is developed on `main`. Security fixes land on `main` and are released
from there, so please make sure you are testing the latest commit or the latest
release before reporting a problem.

## Reporting a vulnerability

**Please do not open a public issue, pull request or Discussion for a security
problem.**

Report vulnerabilities privately through GitHub private vulnerability reporting
for this repository:

<https://github.com/Liyuk/openbars/security/advisories/new>

That form creates a private security advisory that only the maintainers can see.
Please include, as far as you can:

- the affected version, commit or release;
- steps to reproduce, or a minimal proof of concept;
- the impact you observed or expect (for example credential exposure, arbitrary
  code execution, data loss, denial of service);
- any configuration details that matter (provider, Python version, operating
  system);
- whether you intend to publish the finding, and on what timeline.

If the advisory form is unavailable to you for any reason, open a normal issue
that says only that you have a security report and asks the maintainers to open a
private channel — do not put the details of the vulnerability in the issue.

## What to expect

- We aim to acknowledge a report within a few days.
- We will confirm the issue, discuss severity and a fix plan with you in the
  private advisory, and credit you in the advisory and release notes unless you
  prefer to stay anonymous.
- Please give us a reasonable window to ship a fix before any public disclosure;
  we will agree on a disclosure date with you in the advisory thread.

## Scope

In scope: the code and configuration shipped in this repository (the Python
package, CLI, scripts, CI workflows and documented setup paths).

Out of scope: vulnerabilities in third-party dependencies or in the upstream data
providers themselves (report those upstream, and tell us if OpenBars is affected),
and problems that require an already-compromised machine, an attacker-controlled
local configuration file, or a modified installation.

## Handling secrets

OpenBars never asks for credentials to be committed. Provider keys belong in your
local configuration (see `.env.example` and `config.example.yaml`) and must be
passed through environment variables or a file that is kept out of version
control. If you believe a secret was committed to this repository, treat it as a
security report and use the private advisory form above.

## Prevention in this repository

Contributions are checked locally and in CI with `scripts/check.sh`, which runs
the test suite, a sanitization check for private paths, hosts and personal
addresses, and a `gitleaks` secret scan over the working tree and the full git
history. See [docs/sanitization.md](docs/sanitization.md).
