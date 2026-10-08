-- CONTRACT-001/002/003 publication resolver input; manual template.
-- Replace allowlisted catalog/schema identifiers after authorization review.
-- Bind a known publication ID. Do not select latest physical table heads.
-- Zero descriptors -> unavailable/recovery handling under governing contracts.
-- More than one descriptor for an ID -> invalid state; never LIMIT 1 or DISTINCT.
-- A descriptor row is not validation: adapter must verify profile/revisions,
-- role table UUIDs and retained Delta versions, source progress and caller policy.
SELECT publication_id, profile_version, table_versions_json,
       source_progress_json, schema_revisions_json, validation_report_json,
       recorded_at
FROM `__ASHLAR_TARGET_CATALOG__`.`__ASHLAR_TARGET_SCHEMA__`.publication_manifest
WHERE publication_id = :publication_id;
