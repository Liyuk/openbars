-- DDL only (no data) for the tables the upstream_compat adapter reads or introspects.
-- Column names and types follow docs/architecture.md section 11.3. Used by the opt-in
-- live Postgres tests; the checker itself is covered offline with a fake catalog.
CREATE TABLE instrument (
  id BIGSERIAL PRIMARY KEY, issuer_id BIGINT, asset_class VARCHAR(32) NOT NULL,
  security_type VARCHAR(32) NOT NULL, primary_exchange VARCHAR(32),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE symbol_alias (
  id BIGSERIAL PRIMARY KEY, instrument_id BIGINT NOT NULL REFERENCES instrument(id),
  symbol VARCHAR(32) NOT NULL, exchange VARCHAR(32) NOT NULL, valid_from DATE NOT NULL,
  valid_from_basis VARCHAR(24) NOT NULL, valid_to DATE, valid_to_basis VARCHAR(24),
  source_ref TEXT, verification_status VARCHAR(24) NOT NULL, known_at TIMESTAMPTZ,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE source_artifact (
  id BIGSERIAL PRIMARY KEY, provider VARCHAR(32) NOT NULL, session_date DATE, request_id TEXT,
  request_params TEXT NOT NULL, content_digest VARCHAR(128) NOT NULL, storage_path TEXT NOT NULL,
  license_tag VARCHAR(64) NOT NULL, received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE data_ingest_run (
  id BIGSERIAL PRIMARY KEY, provider VARCHAR(32) NOT NULL, feed VARCHAR(32) NOT NULL,
  adjustment VARCHAR(32) NOT NULL, scope VARCHAR(32) NOT NULL, start_date DATE, end_date DATE,
  status VARCHAR(16) NOT NULL, expected_count INTEGER NOT NULL DEFAULT 0,
  received_count INTEGER NOT NULL DEFAULT 0, rejected_count INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), finished_at TIMESTAMPTZ
);
CREATE TABLE daily_bar (
  id BIGSERIAL PRIMARY KEY, instrument_id BIGINT NOT NULL REFERENCES instrument(id),
  session_date DATE NOT NULL, provider VARCHAR(32) NOT NULL, feed VARCHAR(32) NOT NULL,
  adjustment VARCHAR(32) NOT NULL, price_basis VARCHAR(32) NOT NULL DEFAULT 'unknown',
  adjustment_scope VARCHAR(32) NOT NULL DEFAULT 'unknown', adjustment_as_of DATE,
  session_semantics VARCHAR(32) NOT NULL DEFAULT 'unknown', currency VARCHAR(3),
  volume_basis VARCHAR(32) NOT NULL DEFAULT 'unknown', revision INTEGER NOT NULL,
  open NUMERIC(20,8) NOT NULL, high NUMERIC(20,8) NOT NULL, low NUMERIC(20,8) NOT NULL,
  close NUMERIC(20,8) NOT NULL, volume NUMERIC(28,8) NOT NULL, vwap NUMERIC(20,8),
  source_artifact_id BIGINT REFERENCES source_artifact(id),
  ingest_run_id BIGINT REFERENCES data_ingest_run(id),
  UNIQUE (instrument_id, session_date, provider, feed, adjustment, revision)
);
CREATE TABLE research_daily_bar (
  id BIGSERIAL PRIMARY KEY, instrument_id BIGINT NOT NULL REFERENCES instrument(id),
  session_date DATE NOT NULL, policy_version VARCHAR(48) NOT NULL,
  selected_source_bar_id BIGINT REFERENCES daily_bar(id), provider VARCHAR(32) NOT NULL,
  feed VARCHAR(32) NOT NULL, adjustment VARCHAR(32) NOT NULL, revision INTEGER NOT NULL,
  open NUMERIC(20,8) NOT NULL, high NUMERIC(20,8) NOT NULL, low NUMERIC(20,8) NOT NULL,
  close NUMERIC(20,8) NOT NULL, volume NUMERIC(28,8) NOT NULL,
  selection_reason VARCHAR(40) NOT NULL, quality_status VARCHAR(24) NOT NULL,
  capabilities JSONB NOT NULL, source_fingerprint VARCHAR(64) NOT NULL,
  generated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (instrument_id, session_date, policy_version)
);
CREATE TABLE research_series_watermark (
  policy_version VARCHAR(48) PRIMARY KEY, version INTEGER NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE research_series_date_watermark (
  policy_version VARCHAR(48) NOT NULL, session_date DATE NOT NULL,
  version INTEGER NOT NULL DEFAULT 0, updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (policy_version, session_date)
);
CREATE TABLE provider_request_gate (
  provider VARCHAR(32) PRIMARY KEY, next_request_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE provider_request_usage (
  id BIGSERIAL PRIMARY KEY, provider VARCHAR(32) NOT NULL, reserved_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE corporate_action_observation (
  id BIGSERIAL PRIMARY KEY, provider VARCHAR(32) NOT NULL, dataset VARCHAR(80) NOT NULL,
  external_id VARCHAR(128), event_type VARCHAR(64) NOT NULL, payload_hash VARCHAR(64) NOT NULL,
  payload JSONB NOT NULL, normalization_status VARCHAR(32) NOT NULL,
  source_artifact_id BIGINT REFERENCES source_artifact(id), observed_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE corporate_action_event (
  id BIGSERIAL PRIMARY KEY, event_key VARCHAR(160) NOT NULL UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE corporate_action_revision (
  id BIGSERIAL PRIMARY KEY, event_id BIGINT NOT NULL REFERENCES corporate_action_event(id),
  revision INTEGER NOT NULL, action_type VARCHAR(32) NOT NULL, verification_status VARCHAR(24) NOT NULL,
  valid_from DATE, known_at TIMESTAMPTZ NOT NULL, announced_at TIMESTAMPTZ, ex_date DATE,
  record_date DATE, payable_at DATE, old_symbol VARCHAR(32), new_symbol VARCHAR(32),
  instrument_ids JSONB NOT NULL, observation_ids JSONB NOT NULL, details JSONB NOT NULL,
  actor VARCHAR(128) NOT NULL, supersedes_id BIGINT REFERENCES corporate_action_revision(id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), UNIQUE (event_id, revision)
);
CREATE TABLE corporate_action_observation_link (
  revision_id BIGINT NOT NULL REFERENCES corporate_action_revision(id),
  observation_id BIGINT NOT NULL REFERENCES corporate_action_observation(id),
  PRIMARY KEY (revision_id, observation_id)
);
