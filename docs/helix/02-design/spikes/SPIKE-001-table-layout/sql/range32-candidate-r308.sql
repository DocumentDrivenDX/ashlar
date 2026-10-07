-- Private full39.99M-edge physical comparator. No production pointer change.
-- Exact original20carrier fields remain unchanged; physical lookup_partition
-- is derived from first hash byte divided by8, giving32contiguous hash ranges.
CREATE TABLE client_dev.ashlar_entropy_20261006_r86.edge_range32_r308
USING DELTA PARTITIONED BY (lookup_partition)
TBLPROPERTIES ('delta.targetFileSize'='67108864',
 'delta.parquet.compression.codec'='zstd','delta.enableDeletionVectors'='true')
AS SELECT *,CAST(floor(CAST(conv(substr(lookup_hash,1,2),16,10) AS BIGINT)/8) AS INT) AS lookup_partition
FROM client_dev.ashlar_entropy_20261006_r86.maintained_edge_r287 VERSION AS OF 4;
ALTER TABLE client_dev.ashlar_entropy_20261006_r86.edge_range32_r308 DISABLE PREDICTIVE OPTIMIZATION;
-- Execute maintenance in owned recorded stages; never blindly replay outcomes.
-- Example one partition; controller must cover0..31before whole-candidate claim.
OPTIMIZE client_dev.ashlar_entropy_20261006_r86.edge_range32_r308 WHERE lookup_partition=0 ZORDER BY (lookup_hash);
-- Singleton must bind partition plus full hash AND original semantic identity.
-- WHERE lookup_partition=:partition AND lookup_hash=:hash
-- AND source_system=:source AND rel_type_id=CAST(:type AS BIGINT)
-- AND id=CAST(:id AS BIGINT)
