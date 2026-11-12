"""`openbars` CLI. Commands are placeholders until v0.1 issues land."""

from __future__ import annotations

import argparse
import sys

from openbars import __version__

COMMANDS = ("init", "plan", "run", "backfill", "verify", "status")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="openbars", description="openbars (pre-alpha)")
    parser.add_argument("--version", action="version", version=f"openbars {__version__}")
    parser.add_argument("command", nargs="?", choices=COMMANDS)
    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help()
        return 0
    print(f"openbars {args.command}: not implemented yet (pre-alpha)", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
