-- Supplemental candidate, not part of executed ashlar-delta/0.2.
-- Producer cursor components are strings inside exact JSON, never a flattened
-- BIGINT or publication counter. Cursor order is validated by source profile.
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
-- Logical uniqueness is (feed, epoch, delivery_id); publisher enforced.
-- Byte-identical retries are idempotent; conflicting payloads are refused.
-- Unknown records/fields stay here even when no current-state projection exists.
