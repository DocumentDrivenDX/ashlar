-- Experimental 14-column physical lookup candidate; not CONTRACT-003 approval.
-- Keep source_system/type_id/id as identity. lookup_hash is only a pruning key.
CREATE TABLE object_current_lookup (
  source_system STRING NOT NULL, type_id BIGINT NOT NULL, id BIGINT NOT NULL,
  logical_key_json STRING NOT NULL, schema_revision STRING NOT NULL,
  entity_version BIGINT NOT NULL, props_json STRING NOT NULL,
  retained_json STRING NOT NULL, root_id BIGINT,
  source_feed STRING NOT NULL, source_epoch STRING NOT NULL,
  source_position BIGINT NOT NULL, published_at TIMESTAMP NOT NULL,lookup_hash STRING NOT NULL
) USING DELTA CLUSTER BY (lookup_hash)
TBLPROPERTIES ('delta.dataSkippingStatsColumns'='lookup_hash,source_system,type_id,id','delta.targetFileSize'='16777216','delta.feature.catalogManaged'='supported');

-- Publisher derives and verifies lookup_hash for every published row:
-- sha2(to_json(named_struct('source_system',source_system,'type_id',type_id,'id',id)),256)
-- Hash equality MUST be accompanied by exact native source/type/id predicates.
-- Never enforce logical uniqueness or join graph endpoints using hash alone.
-- This is STRING hex SHA256; typed BIGINT JSON inputs and field order are fixed.
