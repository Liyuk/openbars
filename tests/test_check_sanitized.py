"""Tests for scripts/check_sanitized.py. Trigger strings are assembled at runtime
so this file itself stays clean."""

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("check_sanitized", ROOT / "scripts" / "check_sanitized.py")
cs = importlib.util.module_from_spec(spec)
sys.modules["check_sanitized"] = cs
spec.loader.exec_module(cs)

SLASH = "/"


def rules(text: str, hashes: set[str] | None = None) -> set[str]:
    return {f.rule for f in cs.scan_text(text, "t", hashes or set())}


def test_home_paths_flagged_but_placeholders_allowed():
    assert rules(SLASH + "Users" + SLASH + "alice" + "/repo") == {"home_path"}
    assert rules(SLASH + "home" + SLASH + "bob" + "/x") == {"home_path"}
    assert rules(SLASH + "home" + SLASH + "runner" + "/work") == set()
    assert rules("~" + SLASH + "Documents" + SLASH + "Repos") == {"tilde_private_dir"}


def test_network_addresses():
    assert rules("host " + ".".join(["100", "101", "2", "3"])) == {"overlay_ip"}
    assert rules("node.tail1234" + ".ts" + ".net") == {"overlay_dns"}
    assert rules("db at " + ".".join(["192", "168", "1", "20"])) == {"private_ip"}
    assert rules("api on 127.0.0.1:8000") == set()


def test_email_rule_allows_placeholders():
    assert rules("contact " + "jane" + "@" + "corp-mail.io") == {"email"}
    assert rules("ops@example.com and 1+bot@users.noreply.github.com") == set()
    # URL userinfo is not an e-mail (a pagination next_url test uses this shape).
    assert rules("https://user:pw@api.example.com/v2/path") == set()
    assert rules("contact: jane@" + "corp-mail.io") == {"email"}
    assert rules("contact:" + "jane@" + "corp-mail.io") == {"email"}


def test_credential_assignment_ignores_placeholders():
    assert rules("ALPACA_API_KEY=" + "Q" * 8 + "w3Rt9zXy") == {"credential_assignment"}
    assert rules("ALPACA_API_KEY=${ALPACA_API_KEY}") == set()
    assert rules("api_key: <your-key>") == set()


def test_private_terms_match_by_hash_across_separators():
    term = "zz" + "privcorp_internal"
    hashes = {cs.term_hash(term)}
    assert rules("import " + term.replace("_", "-") + ".module", hashes) == {"private_term"}
    assert rules("see zz" + "privcorp internal docs", hashes) == {"private_term"}
    assert rules("unrelated text", hashes) == set()


def test_allow_marker_skips_line():
    assert rules(SLASH + "Users" + SLASH + "alice  # sanitize: allow") == set()


def test_repository_tree_is_clean():
    hashes = cs.load_hashes()
    assert cs.scan_files(cs.tracked_files(), hashes) == []
