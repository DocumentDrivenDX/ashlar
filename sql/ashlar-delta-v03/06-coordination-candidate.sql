-- Proposed ashlar-delta/0.3. Fresh-schema deployment template; not a migration.
-- Replace both quoted target placeholders after reviewing target and permissions.
-- No automatic application, overwrite, IF NOT EXISTS or cleanup is provided.
USE CATALOG `__ASHLAR_TARGET_CATALOG__`;
USE SCHEMA `__ASHLAR_TARGET_SCHEMA__`;

CREATE TABLE publisher_fence (
  stream STRING NOT NULL, fence_epoch BIGINT NOT NULL, owner STRING NOT NULL,
  pending_batch_id STRING, sequence BIGINT NOT NULL
) USING DELTA;

CREATE TABLE apply_receipt (
  apply_batch_id STRING NOT NULL, stream STRING NOT NULL,
  fence_epoch BIGINT NOT NULL, sequence BIGINT NOT NULL,
  stage_id STRING NOT NULL, payload_digest STRING NOT NULL,
  expected_count BIGINT NOT NULL, source_progress_json STRING NOT NULL,
  schema_revisions_json STRING NOT NULL, recorded_at TIMESTAMP NOT NULL
) USING DELTA;
