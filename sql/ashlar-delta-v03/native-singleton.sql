-- Read template governed by CONTRACT-002/CONTRACT-003; never execute as-is.
-- Resolve one immutable publication, allowlist catalog/schema/table identity,
-- validate retained version and effective caller policy, then substitute only
-- the quoted target identifiers and integer __PINNED_DELTA_VERSION__ literal.
-- Bind values; lookup_hash follows the exact CONTRACT-003 tuple recipe.
-- Duplicate visible identities invalidate the snapshot; do not LIMIT 1.
SELECT * FROM `__ASHLAR_TARGET_CATALOG__`.`__ASHLAR_TARGET_SCHEMA__`.object_current
VERSION AS OF __PINNED_DELTA_VERSION__
WHERE lookup_hash = :lookup_hash
  AND source_system = :source_system
  AND type_id = CAST(:type_id AS BIGINT)
  AND id = CAST(:id AS BIGINT);

SELECT * FROM `__ASHLAR_TARGET_CATALOG__`.`__ASHLAR_TARGET_SCHEMA__`.edge_current
VERSION AS OF __PINNED_DELTA_VERSION__
WHERE lookup_hash = :lookup_hash
  AND source_system = :source_system
  AND rel_type_id = CAST(:rel_type_id AS BIGINT)
  AND id = CAST(:id AS BIGINT);
