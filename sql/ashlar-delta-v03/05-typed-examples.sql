-- Proposed ashlar-delta/0.3. Fresh-schema deployment template; not a migration.
-- Replace both quoted target placeholders after reviewing target and permissions.
-- No automatic application, overwrite, IF NOT EXISTS or cleanup is provided.
USE CATALOG `__ASHLAR_TARGET_CATALOG__`;
USE SCHEMA `__ASHLAR_TARGET_SCHEMA__`;

CREATE TABLE node_type_a (
  node_key STRING NOT NULL, source_system STRING NOT NULL,
  type_id BIGINT NOT NULL, id BIGINT NOT NULL,
  group_value STRING, group_present BOOLEAN NOT NULL,
  rank_value BIGINT, rank_present BOOLEAN NOT NULL,
  props_json STRING NOT NULL, retained_json STRING NOT NULL
) USING DELTA CLUSTER BY (id, group_value)
TBLPROPERTIES ('delta.dataSkippingStatsColumns'='node_key,source_system,id,group_value,rank_value');

CREATE TABLE edge_ab (
  edge_key STRING NOT NULL, source_system STRING NOT NULL,
  rel_type_id BIGINT NOT NULL, id BIGINT NOT NULL,
  src STRING NOT NULL, dst STRING NOT NULL,
  source_id BIGINT NOT NULL, target_id BIGINT NOT NULL,
  score DOUBLE, score_present BOOLEAN NOT NULL,
  props_json STRING NOT NULL, retained_json STRING NOT NULL
) USING DELTA CLUSTER BY (source_id, target_id)
TBLPROPERTIES ('delta.dataSkippingStatsColumns'='edge_key,src,dst,source_id,target_id,score');
