from __future__ import annotations

from datetime import date

import pytest

from openbars.config.display import format_settings
from openbars.config.loader import ENV_FILE_VAR, expand_env, load_settings, parse_env_file
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
    s = load_settings(write(tmp_path, "markets: [us]\nhistory: {start: max}\n"), env={})
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
        load_settings(write(tmp_path, text), env={})


def test_example_config_loads():
    from pathlib import Path

    s = load_settings(Path(__file__).resolve().parent.parent / "config.example.yaml", env={})
    assert Market.US in s.selection.markets and "massive" in s.providers



def _env_file(tmp_path, text, name="keys.env"):
    path = tmp_path / name
    path.write_text(text)
    return path


def test_parse_env_file_quotes_export_and_comments(tmp_path):
    path = _env_file(
        tmp_path,
        "\n".join(
            [
                "# comment",
                "export FOO=bare",
                'BAR="quoted value"',
                "BAZ='single'",
                "QUX=trail  # note",
                "",
            ]
        ),
    )
    assert parse_env_file(path) == {
        "FOO": "bare",
        "BAR": "quoted value",
        "BAZ": "single",
        "QUX": "trail",
    }


def test_env_file_fills_missing_and_process_env_wins(tmp_path):
    from_file = "file-key-" + "aaaa1111"
    from_proc = "proc-key-" + "bbbb2222"
    fallback = "file-only-" + "cccc3333"
    path = _env_file(tmp_path, f"T_KEY_ID={from_file}\nT_SECRET={fallback}\n")
    yaml = """
markets: [us]
env_file: ignored.env
providers:
  alpaca:
    key_id: ${T_KEY_ID}
    secret_key: ${T_SECRET}
"""
    s = load_settings(
        write(tmp_path, yaml),
        env={"T_KEY_ID": from_proc},
        env_file=path,
    )
    assert s.providers["alpaca"].options["key_id"] == from_proc
    assert s.providers["alpaca"].options["secret_key"] == fallback
    assert s.env_file == str(path)
    assert s.env_file_source == "--env-file"


def test_openbars_env_file_beats_yaml_env_file(tmp_path):
    via_yaml = "yaml-key-" + "dddd4444"
    via_var = "var-key-" + "eeee5555"
    yaml_file = _env_file(tmp_path, f"T_KEY_ID={via_yaml}\n", "from-yaml.env")
    var_file = _env_file(tmp_path, f"T_KEY_ID={via_var}\n", "from-var.env")
    yaml = f"""
markets: [us]
env_file: {yaml_file.name}
providers:
  massive:
    api_key: ${{T_KEY_ID}}
"""
    s = load_settings(write(tmp_path, yaml), env={ENV_FILE_VAR: str(var_file)})
    assert s.providers["massive"].options["api_key"] == via_var
    assert s.env_file_source == ENV_FILE_VAR


def test_yaml_env_file_is_resolved_relative_to_config(tmp_path):
    value = "yaml-rel-" + "ffff6666"
    _env_file(tmp_path, f"T_KEY_ID={value}\n", "relative.env")
    yaml = """
markets: [us]
env_file: relative.env
providers:
  massive:
    api_key: ${T_KEY_ID}
"""
    s = load_settings(write(tmp_path, yaml), env={})
    assert s.providers["massive"].options["api_key"] == value
    assert s.env_file_source == "config.env_file"


def test_alpaca_env_aliases_when_canonical_unset(tmp_path):
    key_id = "kid-alias-" + "12345678"
    secret = "sec-alias-" + "87654321"
    path = _env_file(tmp_path, f"ALPACA_API_KEY={key_id}\nALPACA_API_SECRET={secret}\n")
    yaml = """
markets: [us]
providers:
  alpaca:
    key_id: ${ALPACA_API_KEY_ID}
    secret_key: ${ALPACA_API_SECRET_KEY}
"""
    s = load_settings(write(tmp_path, yaml), env={}, env_file=path)
    assert s.providers["alpaca"].options["key_id"] == key_id
    assert s.providers["alpaca"].options["secret_key"] == secret


def test_canonical_env_wins_over_alias(tmp_path):
    canonical = "kid-canon-" + "aaaa9999"
    alias = "kid-alias-" + "bbbb8888"
    yaml = """
markets: [us]
providers:
  alpaca:
    key_id: ${ALPACA_API_KEY_ID}
"""
    s = load_settings(
        write(tmp_path, yaml),
        env={"ALPACA_API_KEY_ID": canonical, "ALPACA_API_KEY": alias},
    )
    assert s.providers["alpaca"].options["key_id"] == canonical


def test_missing_env_file_is_a_clean_error(tmp_path):
    with pytest.raises(ConfigError, match="env file not found"):
        load_settings(write(tmp_path, "markets: [us]\n"), env={}, env_file=tmp_path / "nope.env")


def test_format_settings_redacts_secrets(tmp_path):
    key_id = "kid-show-" + "11112222"
    secret = "sec-show-" + "33334444"
    yaml = """
markets: [us]
providers:
  alpaca:
    enabled: true
    feed: sip
    key_id: ${T_KEY_ID}
    secret_key: ${T_SECRET}
"""
    s = load_settings(write(tmp_path, yaml), env={"T_KEY_ID": key_id, "T_SECRET": secret})
    text = format_settings(s, config_path="config.yaml")
    assert key_id not in text and secret not in text
    assert "key_id: set" in text and "secret_key: set" in text
    assert "feed: sip" in text
    assert "env_file: (none)" in text
    assert "process environment" in text


DEFAULT_YAML = """
markets: [us]
providers:
  massive:
    api_key: ${T_KEY_ID}
"""


def test_default_dotenv_next_to_config_is_loaded(tmp_path):
    value = "dot-key-" + "11aa22bb"
    _env_file(tmp_path, f"T_KEY_ID={value}\n", ".env")
    s = load_settings(write(tmp_path, DEFAULT_YAML), env={})
    assert s.providers["massive"].options["api_key"] == value
    assert s.env_file == str(tmp_path / ".env") and s.env_file_source == "./.env"
    text = format_settings(s)
    assert value not in text and "(./.env)" in text and "api_key: set" in text


def test_default_dotenv_is_not_read_from_the_cwd(tmp_path, monkeypatch):
    other = tmp_path / "elsewhere"
    other.mkdir()
    _env_file(other, "T_KEY_ID=" + "cwd-key-" + "99887766\n", ".env")
    cfg = tmp_path / "cfg"
    cfg.mkdir()
    monkeypatch.chdir(other)
    s = load_settings(write(cfg, DEFAULT_YAML), env={})
    assert s.env_file is None and not s.providers["massive"].options["api_key"]


def test_missing_default_dotenv_is_fine(tmp_path):
    s = load_settings(write(tmp_path, DEFAULT_YAML), env={})
    assert s.env_file is None and s.env_file_source is None
    assert not s.providers["massive"].options["api_key"]
    assert "env_file: (none)" in format_settings(s)


def test_process_env_beats_default_dotenv(tmp_path):
    from_file = "dot-key-" + "33cc44dd"
    from_proc = "proc-key-" + "55ee66ff"
    _env_file(tmp_path, f"T_KEY_ID={from_file}\n", ".env")
    s = load_settings(write(tmp_path, DEFAULT_YAML), env={"T_KEY_ID": from_proc})
    assert s.providers["massive"].options["api_key"] == from_proc


def test_explicit_env_sources_beat_default_dotenv(tmp_path):
    default = "dot-key-" + "77aa88bb"
    via_flag = "flag-key-" + "99cc00dd"
    via_var = "var-key-" + "11ee22ff"
    via_yaml = "yaml-key-" + "33aa44bb"
    _env_file(tmp_path, f"T_KEY_ID={default}\n", ".env")
    flag_file = _env_file(tmp_path, f"T_KEY_ID={via_flag}\n", "flag.env")
    var_file = _env_file(tmp_path, f"T_KEY_ID={via_var}\n", "var.env")
    _env_file(tmp_path, f"T_KEY_ID={via_yaml}\n", "yaml.env")
    path = write(tmp_path, DEFAULT_YAML)
    s = load_settings(path, env={ENV_FILE_VAR: str(var_file)}, env_file=flag_file)
    assert s.providers["massive"].options["api_key"] == via_flag and s.env_file_source == "--env-file"
    s = load_settings(path, env={ENV_FILE_VAR: str(var_file)})
    assert s.providers["massive"].options["api_key"] == via_var and s.env_file_source == ENV_FILE_VAR
    s = load_settings(write(tmp_path, "env_file: yaml.env\n" + DEFAULT_YAML), env={})
    assert s.providers["massive"].options["api_key"] == via_yaml and s.env_file_source == "config.env_file"


def test_yaml_default_beats_nothing_but_loses_to_default_dotenv(tmp_path):
    yaml = "markets: [us]\nproviders:\n  massive:\n    api_key: ${T_KEY_ID:-yaml-default}\n"
    assert load_settings(write(tmp_path, yaml), env={}).providers["massive"].options["api_key"] == "yaml-default"
    value = "dot-key-" + "55cc66dd"
    _env_file(tmp_path, f"T_KEY_ID={value}\n", ".env")
    assert load_settings(write(tmp_path, yaml), env={}).providers["massive"].options["api_key"] == value


def test_env_refs_record_variable_names_only(tmp_path):
    value = "ref-key-" + "77ee88ff"
    s = load_settings(write(tmp_path, DEFAULT_YAML), env={"T_KEY_ID": value})
    assert s.providers["massive"].env_refs == {"api_key": ("T_KEY_ID",)}
    assert value not in repr(s.providers["massive"].env_refs)


@pytest.mark.parametrize(
    "yaml",
    ["markets: [${T_KEY_ID}]\n", "markets: [us]\nintervals: [${T_KEY_ID}]\n",
     "markets: [us]\nconventions: {adjustment: ${T_KEY_ID}}\n"],
)  # fmt: skip
def test_secret_values_never_appear_in_config_errors(tmp_path, yaml):
    secret = "leak-key-" + "99aa00bb"
    with pytest.raises(ConfigError) as info:
        load_settings(write(tmp_path, yaml), env={"T_KEY_ID": secret})
    assert secret not in str(info.value) and secret not in repr(info.value)


def test_secret_values_never_appear_in_env_file_errors(tmp_path):
    secret = "leak-key-" + "11cc22dd"
    bad = _env_file(tmp_path, f"GOOD=1\nthis line has {secret} but no equals\n", "bad.env")
    with pytest.raises(ConfigError) as info:
        load_settings(write(tmp_path, DEFAULT_YAML), env={}, env_file=bad)
    assert secret not in str(info.value) and "line 2" in str(info.value)


def test_quota_override_flag_is_parsed_separately_from_limits(tmp_path):
    yaml = "markets: [us]\nproviders:\n  alpaca:\n    quota: {per_minute: 999, override: true}\n  massive:\n    quota: {per_minute: 2}\n"
    s = load_settings(write(tmp_path, yaml), env={})
    assert s.providers["alpaca"].quota_overrides == {"per_minute": 999.0}
    assert s.providers["alpaca"].quota_override is True
    assert s.providers["massive"].quota_override is False
    assert "override: true" in format_settings(s)


@pytest.mark.parametrize("quota", ["{override: yes please}", "{per_minute: true}", "{per_minute: lots}"])
def test_bad_quota_values_are_config_errors(tmp_path, quota):
    yaml = f"markets: [us]\nproviders:\n  alpaca:\n    quota: {quota}\n"
    with pytest.raises(ConfigError, match="quota"):
        load_settings(write(tmp_path, yaml), env={})


def test_unknown_market_and_interval_messages_list_valid_choices(tmp_path):
    with pytest.raises(ConfigError, match=r"invalid market 'mars'.*us"):
        load_settings(write(tmp_path, "markets: [mars]\n"), env={})
    with pytest.raises(ConfigError, match=r"invalid interval '7d'.*1d"):
        load_settings(write(tmp_path, "markets: [us]\nintervals: [7d]\n"), env={})


def test_postgres_settings_and_redacted_dsn(tmp_path):
    dsn = "postgresql://openbars_test:super-secret-dsn-value@127.0.0.1:55442/openbars_test"
    yaml = """
markets: [us]
storage:
  backend: postgres
  path: ./pgdata
  postgres:
    dsn: ${OPENBARS_PG_DSN}
    mode: upstream_compat
    write: true
"""
    s = load_settings(write(tmp_path, yaml), env={"OPENBARS_PG_DSN": dsn})
    assert s.storage_backend == "postgres"
    assert s.postgres_mode == "upstream_compat" and s.postgres_write is True
    assert s.postgres_dsn == dsn
    assert dsn in s.secret_values()
    text = format_settings(s)
    assert dsn not in text
    assert "super-secret-dsn-value" not in text
    assert "postgres.dsn: set" in text
    assert "postgres.mode: upstream_compat" in text
    assert "postgres.write: true" in text


def test_postgres_mode_must_be_known(tmp_path):
    yaml = "markets: [us]\nstorage: {backend: postgres, postgres: {mode: other}}\n"
    with pytest.raises(ConfigError, match="upstream_compat"):
        load_settings(write(tmp_path, yaml), env={})


def test_open_configured_backend_requires_dsn(tmp_path):
    from openbars.storage.factory import open_configured_backend

    yaml = "markets: [us]\nstorage: {backend: postgres}\n"
    s = load_settings(write(tmp_path, yaml), env={})
    with pytest.raises(ConfigError, match="dsn is required"):
        open_configured_backend(s, data_dir=tmp_path)
