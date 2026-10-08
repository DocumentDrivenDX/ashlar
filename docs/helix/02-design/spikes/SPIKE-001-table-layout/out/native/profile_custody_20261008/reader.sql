BEGIN; SET LOCAL ROLE ashlar_profile_custody_reader;
SELECT row_to_json(r) FROM (SELECT identity,version,encode(definition_bytes,'hex') AS definition_hex,encode(definition_sha256,'hex') AS definition_sha256,encode(source_bundle_bytes,'hex') AS bundle_hex,encode(source_bundle_sha256,'hex') AS bundle_sha256,original_database_role,original_session_role,original_xid::text AS original_xid,original_at::text AS original_at FROM ashlar_profile_custody.original ORDER BY identity COLLATE "C",version COLLATE "C") r;
COMMIT;
