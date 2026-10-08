-- Proposed ashlar-delta/0.3. Fresh-schema deployment template; not a migration.
-- Replace both quoted target placeholders after reviewing target and permissions.
-- No automatic application, overwrite, IF NOT EXISTS or cleanup is provided.
USE CATALOG `__ASHLAR_TARGET_CATALOG__`;
USE SCHEMA `__ASHLAR_TARGET_SCHEMA__`;

CREATE TABLE adjacency_forward (
  source_system STRING NOT NULL, rel_type_id BIGINT NOT NULL, edge_id BIGINT NOT NULL,
  source_type BIGINT NOT NULL, source_id BIGINT NOT NULL,
  target_type BIGINT NOT NULL, target_id BIGINT NOT NULL,
  structural_version BIGINT NOT NULL
) USING DELTA CLUSTER BY (source_system, source_type, source_id, rel_type_id)
TBLPROPERTIES ('delta.dataSkippingStatsColumns'='source_system,source_type,source_id,rel_type_id,target_type,target_id,edge_id');
