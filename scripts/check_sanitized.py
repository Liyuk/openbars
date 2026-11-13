#!/usr/bin/env python3
"""Sanitization check for openbars: fail when private context would be published.

openbars is developed against an internal upstream implementation and will be made
public. This script blocks content that must never reach the public repository:
personal home-directory paths, private-network / overlay-network addresses,
personal e-mail addresses, credential-looking assignments and a list of private
terms (internal package, product, host and user names).

Private terms are stored only as salted SHA-256 hashes in
``scripts/sanitize_hashes.txt`` so the denylist itself does not disclose them.
Maintainers may additionally point ``OPENBARS_PRIVATE_DENYLIST`` at a plaintext
list kept outside the repository (one term per line).

Usage::

    python scripts/check_sanitized.py                 # all tracked files
    python scripts/check_sanitized.py FILE ...        # given files (pre-commit)
    python scripts/check_sanitized.py --history       # every blob, commit message and author in git history
    python scripts/check_sanitized.py --history --release   # pre-publication: no baseline, personal e-mails are errors
    python scripts/check_sanitized.py --stdin         # text piped in (issue / PR / Discussion bodies)
    echo TERM | python scripts/check_sanitized.py --hash-term   # print the hash line for a new private term

A line containing ``sanitize: allow`` is skipped; use it sparingly and only for
reviewed, intentional examples. Findings print file, line and rule only, never
the matched value.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import subprocess
import sys
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
HASH_FILE = ROOT / "scripts" / "sanitize_hashes.txt"
# Known findings in already-pushed history (commit:path:line:rule). They are reported as
# warnings until the history is rewritten or squashed before going public; --release ignores it.
BASELINE_FILE = ROOT / "scripts" / "sanitize_history_baseline.txt"
SALT = "openbars-sanitize-v1:"
ALLOW_MARKER = "sanitize: allow"
SELF_EXEMPT = {"scripts/sanitize_hashes.txt", "scripts/sanitize_history_baseline.txt"}
SKIP_SUFFIXES = {".png", ".jpg", ".jpeg", ".gif", ".ico", ".pdf", ".parquet", ".duckdb", ".zip", ".gz"}

ALLOWED_EMAIL_DOMAINS = (
    "example.com", "example.org", "example.net", "users.noreply.github.com",
    "noreply.github.com", "localhost",
)
ALLOWED_EMAIL_SUFFIXES = (".example", ".invalid", ".test")
# Placeholder home directories that are fine in docs and CI.
ALLOWED_HOME_NAMES = {"runner", "user", "you", "me", "example", "username", "name", "<you>", "<user>"}

PATTERNS: dict[str, re.Pattern[str]] = {
    "home_path": re.compile(r"(?<![\w.])/(?:Users|home)/([A-Za-z0-9._<>-]+)"),
    "tilde_private_dir": re.compile(r"~/(?:Documents|Desktop|Downloads|Library)/"),
    "overlay_ip": re.compile(r"\b100\.(?:6[4-9]|[7-9]\d|1[01]\d|12[0-7])\.\d{1,3}\.\d{1,3}\b"),
    "overlay_dns": re.compile(r"\.ts\.net\b", re.IGNORECASE),
    "private_ip": re.compile(
        r"\b(?:10\.\d{1,3}\.\d{1,3}\.\d{1,3}|192\.168\.\d{1,3}\.\d{1,3}|172\.(?:1[6-9]|2\d|3[01])\.\d{1,3}\.\d{1,3})\b"
    ),
    "email": re.compile(r"\b[A-Za-z0-9._%+-]+@([A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,})\b"),
    "credential_assignment": re.compile(
        r"(?i)\b[A-Z0-9_]*(?:api[_-]?key|api[_-]?secret|secret|token|password)\b\s*[:=]\s*[\"']?"
        r"(?!\$\{)(?!<)([A-Za-z0-9/+_\-]{16,})"
    ),
}

TOKEN = re.compile(r"[a-z0-9]+(?:[_.\-][a-z0-9]+)*")


@dataclass(frozen=True)
class Finding:
    where: str
    line: int
    rule: str

    def __str__(self) -> str:
        return f"{self.where}:{self.line}: {self.rule}"


def term_hash(term: str) -> str:
    normalized = re.sub(r"[\s.\-]+", "_", term.strip().lower())
    return hashlib.sha256((SALT + normalized).encode()).hexdigest()


def load_hashes(extra_plain: Path | None = None) -> set[str]:
    hashes: set[str] = set()
    if HASH_FILE.exists():
        for raw in HASH_FILE.read_text().splitlines():
            raw = raw.split("#", 1)[0].strip()
            if raw:
                hashes.add(raw)
    plain = extra_plain or (Path(os.environ["OPENBARS_PRIVATE_DENYLIST"]).expanduser()
                            if os.environ.get("OPENBARS_PRIVATE_DENYLIST") else None)
    if plain and plain.exists():
        for raw in plain.read_text().splitlines():
            raw = raw.split("#", 1)[0].strip()
            if raw:
                hashes.add(term_hash(raw))
    return hashes


def load_baseline() -> set[str]:
    if not BASELINE_FILE.exists():
        return set()
    return {
        line.split("#", 1)[0].strip()
        for line in BASELINE_FILE.read_text().splitlines()
        if line.split("#", 1)[0].strip()
    }


def _ngrams(token: str) -> Iterator[str]:
    parts = re.split(r"[_.\-]", token)
    for i in range(len(parts)):
        for j in range(i + 1, min(len(parts), i + 4) + 1):
            yield "_".join(parts[i:j])


def _email_allowed(domain: str) -> bool:
    domain = domain.lower()
    return domain in ALLOWED_EMAIL_DOMAINS or domain.endswith(ALLOWED_EMAIL_SUFFIXES) or any(
        domain.endswith("." + d) for d in ALLOWED_EMAIL_DOMAINS
    )


def scan_text(text: str, where: str, hashes: set[str]) -> list[Finding]:
    findings: list[Finding] = []
    for number, line in enumerate(text.splitlines(), 1):
        if ALLOW_MARKER in line:
            continue
        for rule, pattern in PATTERNS.items():
            for match in pattern.finditer(line):
                if rule == "home_path" and match.group(1).lower() in ALLOWED_HOME_NAMES:
                    continue
                if rule == "email" and _email_allowed(match.group(1)):
                    continue
                # URL userinfo (https://user:pw@host) is not an e-mail address.
                if (
                    rule == "email"
                    and match.start()
                    and line[match.start() - 1] == ":"
                    and "://" in line[: match.start()]
                ):
                    continue
                findings.append(Finding(where, number, rule))
                break
        if hashes:
            tokens = TOKEN.findall(line.lower())
            # Single tokens (with their _-./ sub-parts) plus adjacent word pairs ("two words").
            candidates = [gram for token in tokens for gram in _ngrams(token)]
            candidates += [f"{a}_{b}" for a, b in zip(tokens, tokens[1:])]
            if any(term_hash(gram) in hashes for gram in candidates):
                findings.append(Finding(where, number, "private_term"))
    return findings


def _git(*args: str) -> str:
    return subprocess.run(["git", *args], cwd=ROOT, check=True, capture_output=True, text=True).stdout


def tracked_files() -> list[str]:
    return [p for p in _git("ls-files").splitlines() if p]


def scan_files(paths: Iterable[str], hashes: set[str]) -> list[Finding]:
    findings: list[Finding] = []
    for rel in paths:
        if rel in SELF_EXEMPT or Path(rel).suffix.lower() in SKIP_SUFFIXES:
            continue
        path = ROOT / rel if not os.path.isabs(rel) else Path(rel)
        if not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        findings.extend(scan_text(text, rel, hashes))
    return findings


def scan_history(hashes: set[str], strict_authors: bool) -> tuple[list[Finding], list[Finding]]:
    errors: list[Finding] = []
    warnings: list[Finding] = []
    commits = [c for c in _git("rev-list", "--all").splitlines() if c]
    seen_blobs: set[str] = set()
    for commit in commits:
        short = commit[:7]
        message = _git("log", "-1", "--format=%B", commit)
        errors.extend(scan_text(message, f"commit {short} message", hashes))
        for field, label in (("%ae", "author"), ("%ce", "committer")):
            email = _git("log", "-1", f"--format={field}", commit).strip()
            domain = email.rsplit("@", 1)[-1] if "@" in email else ""
            if domain and not _email_allowed(domain):
                target = errors if strict_authors else warnings
                target.append(Finding(f"commit {short} {label}", 0, "personal_commit_email"))
        for entry in _git("ls-tree", "-r", commit).splitlines():
            meta, path = entry.split("\t", 1)
            _, kind, blob = meta.split()
            if kind != "blob" or blob in seen_blobs:
                continue
            seen_blobs.add(blob)
            if path in SELF_EXEMPT or Path(path).suffix.lower() in SKIP_SUFFIXES:
                continue
            data = subprocess.run(["git", "cat-file", "-p", blob], cwd=ROOT, check=True,
                                  capture_output=True).stdout
            try:
                text = data.decode("utf-8")
            except UnicodeDecodeError:
                continue
            errors.extend(scan_text(text, f"{short}:{path}", hashes))
    return errors, warnings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("paths", nargs="*")
    parser.add_argument("--history", action="store_true", help="scan every commit in git history")
    parser.add_argument("--strict-authors", action="store_true",
                        help="treat personal commit e-mail addresses as errors")
    parser.add_argument("--release", action="store_true",
                        help="pre-publication mode: --strict-authors and ignore the history baseline")
    parser.add_argument("--stdin", action="store_true", help="scan text from standard input")
    parser.add_argument("--denylist", type=Path, help="extra plaintext private-term file (kept outside the repo)")
    parser.add_argument("--hash-term", action="store_true", help="read terms from stdin and print hash lines")
    args = parser.parse_args(argv)

    if args.hash_term:
        for raw in sys.stdin.read().splitlines():
            if raw.strip():
                print(term_hash(raw))
        return 0

    hashes = load_hashes(args.denylist)
    warnings: list[Finding] = []
    if args.stdin:
        findings = scan_text(sys.stdin.read(), "<stdin>", hashes)
    elif args.history:
        findings, warnings = scan_history(hashes, args.strict_authors or args.release)
        if not args.release:
            baseline = load_baseline()
            warnings += [f for f in findings if str(f).replace(": ", ":") in baseline]
            findings = [f for f in findings if str(f).replace(": ", ":") not in baseline]
    else:
        findings = scan_files(args.paths or tracked_files(), hashes)

    for item in warnings:
        print(f"warning: {item}", file=sys.stderr)
    for item in findings:
        print(item, file=sys.stderr)
    if findings:
        print(f"check_sanitized: {len(findings)} finding(s). See docs/sanitization.md.", file=sys.stderr)
        return 1
    print(f"check_sanitized: clean ({len(hashes)} private term hashes loaded"
          f"{', ' + str(len(warnings)) + ' warning(s)' if warnings else ''}).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
