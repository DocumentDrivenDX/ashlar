-- Fresh isolated custody deployment. Not Truss profile registration or readiness.
BEGIN;
CREATE ROLE ashlar_profile_custody_owner NOLOGIN;
CREATE ROLE ashlar_profile_custody_writer NOLOGIN;
CREATE ROLE ashlar_profile_custody_reader NOLOGIN;
CREATE SCHEMA ashlar_profile_custody AUTHORIZATION ashlar_profile_custody_owner;
REVOKE ALL ON SCHEMA ashlar_profile_custody FROM PUBLIC;
SET LOCAL ROLE ashlar_profile_custody_owner;
CREATE TABLE ashlar_profile_custody.original (
  identity text COLLATE pg_catalog."C" NOT NULL,
  version text COLLATE pg_catalog."C" NOT NULL,
  definition_bytes bytea NOT NULL,
  definition_sha256 bytea GENERATED ALWAYS AS (pg_catalog.sha256(definition_bytes)) STORED,
  source_bundle_bytes bytea NOT NULL,
  source_bundle_sha256 bytea GENERATED ALWAYS AS (pg_catalog.sha256(source_bundle_bytes)) STORED,
  original_database_role text NOT NULL,
  original_session_role text NOT NULL,
  original_xid xid8 NOT NULL,
  original_at timestamptz NOT NULL,
  PRIMARY KEY(identity,version),
  CHECK(octet_length(identity) BETWEEN 1 AND 1024),
  CHECK(octet_length(version) BETWEEN 1 AND 1024),
  CHECK(octet_length(definition_bytes) BETWEEN 1 AND 1048576),
  CHECK(octet_length(source_bundle_bytes) BETWEEN 1 AND 4194304)
);
CREATE FUNCTION ashlar_profile_custody.retain(p_identity text,p_version text,p_definition bytea,p_bundle bytea)
RETURNS ashlar_profile_custody.original
LANGUAGE plpgsql VOLATILE SECURITY DEFINER PARALLEL UNSAFE CALLED ON NULL INPUT
SET search_path=pg_catalog,pg_temp AS $$
DECLARE original ashlar_profile_custody.original%ROWTYPE; acting text;
BEGIN
  IF p_identity IS NULL OR octet_length(p_identity) NOT BETWEEN 1 AND 1024
    OR p_version IS NULL OR octet_length(p_version) NOT BETWEEN 1 AND 1024
    OR p_definition IS NULL OR octet_length(p_definition) NOT BETWEEN 1 AND 1048576
    OR p_bundle IS NULL OR octet_length(p_bundle) NOT BETWEEN 1 AND 4194304
  THEN RAISE EXCEPTION 'Explicit bounded original profile artifacts required'; END IF;
  LOCK TABLE ashlar_profile_custody.original IN SHARE ROW EXCLUSIVE MODE;
  SELECT * INTO original FROM ashlar_profile_custody.original AS p
    WHERE p.identity=p_identity AND p.version=p_version;
  IF FOUND THEN
    IF original.definition_bytes IS DISTINCT FROM p_definition OR original.source_bundle_bytes IS DISTINCT FROM p_bundle
    THEN RAISE EXCEPTION 'Original profile version/artifact conflict'; END IF;
    RETURN original;
  END IF;
  -- Native role GUC records a real SET ROLE; callers cannot supply role text.
  acting=pg_catalog.current_setting('role');
  IF acting='none' THEN acting=session_user; END IF;
  INSERT INTO ashlar_profile_custody.original(identity,version,definition_bytes,source_bundle_bytes,original_database_role,original_session_role,original_xid,original_at)
    VALUES(p_identity,p_version,p_definition,p_bundle,acting,session_user,pg_catalog.pg_current_xact_id(),pg_catalog.clock_timestamp())
    RETURNING * INTO original;
  RETURN original;
END $$;
REVOKE ALL ON ALL TABLES IN SCHEMA ashlar_profile_custody FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA ashlar_profile_custody FROM PUBLIC;
GRANT USAGE ON SCHEMA ashlar_profile_custody TO ashlar_profile_custody_writer,ashlar_profile_custody_reader;
GRANT EXECUTE ON FUNCTION ashlar_profile_custody.retain(text,text,bytea,bytea) TO ashlar_profile_custody_writer;
GRANT SELECT ON ashlar_profile_custody.original TO ashlar_profile_custody_reader;
COMMENT ON SCHEMA ashlar_profile_custody IS 'ashlar-profile-custody/0.1; exact originals only; no Truss profile admission, execution or readiness';
RESET ROLE;
COMMIT;
