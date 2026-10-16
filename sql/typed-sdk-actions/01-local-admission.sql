-- Dedicated local PostgreSQL18 issuer. No Truss or managed Spark authority claim.
-- Install once on an owned development cluster as its administrator.
BEGIN;
CREATE ROLE ashlar_sdk_action_owner NOLOGIN;
CREATE ROLE ashlar_sdk_action_registrar NOLOGIN;
CREATE ROLE ashlar_sdk_action_caller NOLOGIN;
CREATE SCHEMA ashlar_sdk_actions AUTHORIZATION ashlar_sdk_action_owner;
REVOKE ALL ON SCHEMA ashlar_sdk_actions FROM PUBLIC;
SET ROLE ashlar_sdk_action_owner;
CREATE TABLE ashlar_sdk_actions.program (
 program_sha text PRIMARY KEY CHECK(program_sha ~ '^[0-9a-f]{64}$'),
 original_program bytea NOT NULL CHECK(octet_length(original_program) BETWEEN 1 AND 4194304),
 table_uuid text NOT NULL,
 epoch text NOT NULL,
 original_path text NOT NULL,
 native_executor text NOT NULL,
 native_roles text[] NOT NULL CHECK(cardinality(native_roles) BETWEEN 1 AND 64),
 CHECK(program_sha=encode(sha256(original_program),'hex'))
);
CREATE TABLE ashlar_sdk_actions.subject (
 role_oid oid PRIMARY KEY,
 principal_identity bytea NOT NULL UNIQUE CHECK(octet_length(principal_identity) BETWEEN 1 AND 4096),
 subject_kind text NOT NULL CHECK(subject_kind IN ('human','service'))
);
CREATE FUNCTION ashlar_sdk_actions.admit(p_sha text,p_original bytea) RETURNS TABLE(allowed boolean,authenticated text,table_uuid text,epoch text,original_path text,native_executor text,nonce text,subject_identity text,subject_kind text,role_oid text)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE original ashlar_sdk_actions.program%ROWTYPE; actual ashlar_sdk_actions.subject%ROWTYPE;
BEGIN
 LOCK TABLE pg_catalog.pg_authid,pg_catalog.pg_auth_members,pg_catalog.pg_proc,pg_catalog.pg_namespace,pg_catalog.pg_class,pg_catalog.pg_attribute IN SHARE MODE;
 LOCK TABLE ashlar_sdk_actions.subject IN SHARE MODE;
 SELECT * INTO original FROM ashlar_sdk_actions.program p WHERE p.program_sha=p_sha AND p.original_program=p_original FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION 'Original enrolled SDK action program required' USING ERRCODE='42501';END IF;
 -- Human/service identity classification is a trusted installation duty. The
 -- executing principal here is nevertheless the actual native session user;
 -- neither actor strings nor arbitrary effective-role claims are accepted.
 IF NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=session_user AND rolcanlogin AND NOT rolsuper AND NOT rolbypassrls AND NOT rolcreaterole AND NOT rolcreatedb AND NOT rolreplication) THEN
  RAISE EXCEPTION 'Original ordinary native actor required' USING ERRCODE='42501';
 END IF;
 SELECT s.* INTO actual FROM ashlar_sdk_actions.subject s JOIN pg_roles r ON r.oid=s.role_oid WHERE r.rolname=session_user;
 IF NOT FOUND THEN RAISE EXCEPTION 'Original enrolled native Subject required' USING ERRCODE='42501';END IF;
 RETURN QUERY SELECT actual.subject_kind='human' AND EXISTS(SELECT 1 FROM unnest(original.native_roles)r WHERE pg_has_role(session_user,r,'MEMBER')),
 session_user::text,original.table_uuid,original.epoch,original.original_path,original.native_executor,gen_random_uuid()::text,encode(actual.principal_identity,'hex'),actual.subject_kind,actual.role_oid::text;
END $$;
REVOKE ALL ON ALL TABLES IN SCHEMA ashlar_sdk_actions FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA ashlar_sdk_actions FROM PUBLIC;
GRANT USAGE ON SCHEMA ashlar_sdk_actions TO ashlar_sdk_action_registrar,ashlar_sdk_action_caller;
GRANT SELECT,INSERT ON ashlar_sdk_actions.program,ashlar_sdk_actions.subject TO ashlar_sdk_action_registrar;
GRANT EXECUTE ON FUNCTION ashlar_sdk_actions.admit(text,bytea) TO ashlar_sdk_action_caller;
RESET ROLE;
-- Local privileged issuer must hold native role/catalog closure locks.
ALTER FUNCTION ashlar_sdk_actions.admit(text,bytea) OWNER TO CURRENT_USER;
COMMIT;
