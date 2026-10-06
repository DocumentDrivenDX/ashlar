-- Experimental 17-column canonical edge candidate; not normative approval.
-- Native source_system/rel_type_id/id remain identity; exact predicates mandatory.
CREATE TABLE edge_current_lookup (
  source_system STRING NOT NULL, rel_type_id BIGINT NOT NULL, id BIGINT NOT NULL,
  source_type BIGINT NOT NULL, source_id BIGINT NOT NULL,
  target_type BIGINT NOT NULL, target_id BIGINT NOT NULL,
  schema_revision STRING NOT NULL, entity_version BIGINT NOT NULL,
  props_json STRING NOT NULL, retained_json STRING NOT NULL, order_key STRING,
  source_feed STRING NOT NULL, source_epoch STRING NOT NULL,
  source_position BIGINT NOT NULL, published_at TIMESTAMP NOT NULL,lookup_hash STRING NOT NULL
) USING DELTA CLUSTER BY (lookup_hash)
TBLPROPERTIES ('delta.dataSkippingStatsColumns'='lookup_hash,source_system,rel_type_id,id','delta.targetFileSize'='16777216','delta.feature.catalogManaged'='supported');
-- lookup_hash=sha2(to_json(named_struct('source_system',source_system,'rel_type_id',rel_type_id,'id',id)),256)
-- Never deduplicate or resolve typed endpoints through this hash.
