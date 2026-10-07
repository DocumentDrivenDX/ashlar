-- Experimental physical alternative, not the selected canonical DDL or a migration.
-- Native r158 executes this shape only on100k immutable stage r139 carriers.
-- Source/target names below are placeholders;20M/billion and incremental apply unqualified.
-- Do not combine partitioning/Z-order with liquid clustering.
CREATE TABLE edge_bucket64_candidate USING DELTA
PARTITIONED BY (lookup_bucket)
TBLPROPERTIES ('delta.parquet.compression.codec'='zstd',
'delta.targetFileSize'='67108864',
'delta.dataSkippingStatsColumns'='lookup_hash,source_system,rel_type_id,id,entity_version,apply_batch_id')
AS SELECT source_system,rel_type_id,id,source_type,source_id,target_type,target_id,
schema_revision,entity_version,props_json,retained_json,order_key,source_feed,
source_epoch,source_position,published_at,lookup_hash,apply_batch_id,
source_cursor_json,source_delivery_id,
cast(pmod(cast(conv(substr(lookup_hash,1,15),16,10) AS BIGINT),64) AS INT) lookup_bucket
FROM immutable_carrier_input;

-- First60 bits fit signed BIGINT. Require lowercase64-hex SHA256 before derivation.
-- Bucket is physical metadata: identity and every lookup retain the full native tuple.
-- Query binds computed lookup_bucket plus lookup_hash/source_system/rel_type_id/id.
-- Rebuildable exports may omit bucket; they still need a complete pinned descriptor.
OPTIMIZE edge_bucket64_candidate ZORDER BY (lookup_hash);
-- Both r158 OPTIMIZE commands were no-ops. No actual Z-order rewrite evidence yet.
-- Target file size is configured, not an enforced CTAS/MERGE file-size limit.
-- Native feature differences include LC rowTracking, absent in this partitioned pilot.
-- Match explicit feature settings before causal/future ingest comparisons.
-- Existing20-column UPDATE SET * templates need an explicit bucket-aware source or
-- assignment policy for this21-column table. Never silently drop or drift the bucket.
