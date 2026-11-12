from __future__ import annotations

from datetime import date

import pytest

from openbars.config.loader import expand_env, load_settings
from openbars.core.errors import ConfigError
from openbars.core.models import Adjustment, Interval, Market

pytestmark = pytest.mark.usefixtures("no_network")


YAML = """
version: 1
markets: [us]
intervals: [1d]
history: {start: 2024-01-02, end: null}
universe:
  us:
    symbols: [ZZZA, TEST1, TEST1]
storage: {backend: parquet_duckdb, path: ./somewhere}
conventions: {adjustment: split}
providers:
  alpaca:
    enabled: true
    feed: sip
    key_id: ${T_KEY_ID}
    secret_key: ${T_SECRET}
    quota: {per_minute: 190}
  massive:
    enabled: false
    api_key: ${T_MISSING:-fallback-value}
nodes:
  - {name: local, kind: local}
schedule: {daily_at: "06:30", timezone: America/New_York}
"""


def write(tmp_path, text=YAML):
    path = tmp_path / "config.yaml"
    path.write_text(text)
    return path


def test_env_expansion_and_mapping(tmp_path):
    key_id = "kid-1234" + "5678"
    secret = "sec-8765" + "4321"
    env = {"T_KEY_ID": key_id, "T_SECRET": secret}
    s = load_settings(write(tmp_path), env=env)
    assert s.selection.markets == frozenset({Market.US})
    assert s.selection.intervals == frozenset({Interval.D1})
    assert (s.selection.history_start, s.selection.history_end) == (date(2024, 1, 2), None)
    assert s.universe == {Market.US: ("TEST1", "ZZZA")}
    assert s.storage_path == "./somewhere" and s.default_adjustment is Adjustment.SPLIT
    alpaca = s.providers["alpaca"]
    assert alpaca.enabled and alpaca.options["key_id"] == key_id and alpaca.options["feed"] == "sip"
    assert alpaca.quota_overrides == {"per_minute": 190.0}
    assert s.providers["massive"].options["api_key"] == "fallback-value" and not s.providers["massive"].enabled
    assert set(s.secret_values()) == {key_id, secret, "fallback-value"}
    assert s.schedule["timezone"] == "America/New_York"


def test_expand_env_rules():
    env = {"A": "x", "EMPTY": ""}
    assert expand_env("${A}-${NOPE}-${NOPE:-d}-${EMPTY:-e}", env) == "x--d-e"
    assert expand_env({"k": ["${A}", 3]}, env) == {"k": ["x", 3]}


def test_history_max_means_provider_max(tmp_path):
    s = load_settings(write(tmp_path, "markets: [us]\nhistory: {start: max}\n"))
    assert s.selection.history_start is None


def test_missing_file_is_a_clean_error(tmp_path):
    with pytest.raises(ConfigError, match="not found"):
        load_settings(tmp_path / "nope.yaml")


@pytest.mark.parametrize(
    "text, message",
    [
        ("markets: [mars]\n", "invalid market"),
        ("markets: [us]\nintervals: [7d]\n", "invalid interval"),
        ("markets: [us]\nconventions: {adjustment: sideways}\n", "adjustment"),
        ("markets: [us]\nhistory: {start: 2024-02-01, end: 2024-01-01}\n", "history.end"),
        ("- just\n- a list\n", "mapping"),
        ("markets: [us\n", "valid YAML"),
    ],
)
def test_invalid_config_errors(tmp_path, text, message):
    with pytest.raises(ConfigError, match=message):
        load_settings(write(tmp_path, text))


def test_example_config_loads():
    from pathlib import Path

    s = load_settings(Path(__file__).resolve().parent.parent / "config.example.yaml", env={})
    assert Market.US in s.selection.markets and "massive" in s.providers
