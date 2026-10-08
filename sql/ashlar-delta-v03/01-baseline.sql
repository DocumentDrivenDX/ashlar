-- Proposed ashlar-delta/0.3. Fresh-schema deployment template; not a migration.
-- Replace both quoted target placeholders after reviewing target and permissions.
-- No automatic application, overwrite, IF NOT EXISTS or cleanup is provided.
USE CATALOG `__ASHLAR_TARGET_CATALOG__`;
USE SCHEMA `__ASHLAR_TARGET_SCHEMA__`;

CREATE TABLE object_current (
  source_system STRING NOT NULL, type_id BIGINT NOT NULL, id BIGINT NOT NULL,
  logical_key_json STRING NOT NULL, schema_revision STRING NOT NULL,
  entity_version BIGINT NOT NULL, props_json STRING NOT NULL,
  retained_json STRING NOT NULL, root_id BIGINT,
  source_feed STRING NOT NULL, source_epoch STRING NOT NULL,
  source_position BIGINT, published_at TIMESTAMP NOT NULL,
  lookup_hash STRING NOT NULL, apply_batch_id STRING,
  source_cursor_json STRING, source_delivery_id STRING
) USING DELTA CLUSTER BY (lookup_hash)
TBLPROPERTIES ('delta.dataSkippingStatsColumns'='lookup_hash,source_system,type_id,id',
'delta.targetFileSize'='67108864','delta.parquet.compression.codec'='zstd');

CREATE TABLE edge_current (
  source_system STRING NOT NULL, rel_type_id BIGINT NOT NULL, id BIGINT NOT NULL,
  source_type BIGINT NOT NULL, source_id BIGINT NOT NULL,
  target_type BIGINT NOT NULL, target_id BIGINT NOT NULL,
  schema_revision STRING NOT NULL, entity_version BIGINT NOT NULL,
  props_json STRING NOT NULL, retained_json STRING NOT NULL, order_key STRING,
  source_feed STRING NOT NULL, source_epoch STRING NOT NULL,
  source_position BIGINT, published_at TIMESTAMP NOT NULL,
  lookup_hash STRING NOT NULL, apply_batch_id STRING,
  source_cursor_json STRING, source_delivery_id STRING
) USING DELTA CLUSTER BY (lookup_hash)
TBLPROPERTIES ('delta.dataSkippingStatsColumns'='lookup_hash,source_system,rel_type_id,id,entity_version,apply_batch_id',
'delta.targetFileSize'='67108864','delta.parquet.compression.codec'='zstd');

CREATE TABLE source_record (
  source_feed STRING NOT NULL,
  source_epoch STRING NOT NULL,
  delivery_id STRING NOT NULL,
  record_kind STRING NOT NULL,
  source_cursor_json STRING NOT NULL,
  payload_json STRING NOT NULL,
  payload_digest STRING NOT NULL,
  schema_revision STRING,
  received_at TIMESTAMP NOT NULL,
  apply_batch_id STRING
) USING DELTA CLUSTER BY (source_feed, source_epoch, delivery_id)
TBLPROPERTIES ('delta.dataSkippingStatsColumns'='source_feed,source_epoch,delivery_id');

CREATE TABLE property_journal (
  source_system STRING NOT NULL, entity_kind STRING NOT NULL,
  type_id BIGINT NOT NULL, id BIGINT NOT NULL, property_id BIGINT,
  entity_version BIGINT NOT NULL, operation STRING NOT NULL,
  old_present BOOLEAN NOT NULL, old_json STRING,
  new_present BOOLEAN NOT NULL, new_json STRING,
  schema_revision STRING NOT NULL, source_feed STRING NOT NULL,
  source_epoch STRING NOT NULL, source_position BIGINT,
  event_ordinal BIGINT NOT NULL, source_time_text STRING,
  published_at TIMESTAMP NOT NULL, apply_batch_id STRING,
  source_cursor_json STRING, source_delivery_id STRING
) USING DELTA CLUSTER BY (source_feed, source_epoch, source_position, id)
-- r100: batch statistics improve pinned-version journal file pruning;
-- no measured freshness gain. Backfill existing statistics separately.
TBLPROPERTIES ('delta.dataSkippingStatsColumns'='source_feed,source_epoch,source_position,id,apply_batch_id');

CREATE TABLE tombstone (
  source_system STRING NOT NULL, entity_kind STRING NOT NULL,
  type_id BIGINT NOT NULL, id BIGINT NOT NULL, entity_version BIGINT NOT NULL,
  source_feed STRING NOT NULL, source_epoch STRING NOT NULL,
  source_position BIGINT,
  source_cursor_json STRING, source_delivery_id STRING
) USING DELTA CLUSTER BY (source_system, type_id, id);

CREATE TABLE publication_manifest (
  publication_id STRING NOT NULL, profile_version STRING NOT NULL,
  table_versions_json STRING NOT NULL, source_progress_json STRING NOT NULL,
  schema_revisions_json STRING NOT NULL, validation_report_json STRING NOT NULL,
  recorded_at TIMESTAMP NOT NULL
) USING DELTA;
