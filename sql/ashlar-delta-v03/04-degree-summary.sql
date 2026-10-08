-- Proposed ashlar-delta/0.3. Fresh-schema deployment template; not a migration.
-- Replace both quoted target placeholders after reviewing target and permissions.
-- No automatic application, overwrite, IF NOT EXISTS or cleanup is provided.
USE CATALOG `__ASHLAR_TARGET_CATALOG__`;
USE SCHEMA `__ASHLAR_TARGET_SCHEMA__`;

CREATE TABLE degree_summary (
  source_system STRING NOT NULL, type_id BIGINT NOT NULL, id BIGINT NOT NULL,
  rel_type_id BIGINT NOT NULL, direction STRING NOT NULL, edge_count BIGINT NOT NULL
) USING DELTA CLUSTER BY (source_system, type_id, id, rel_type_id);
