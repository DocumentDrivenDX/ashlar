-- Ordinary-role paths; each negative case has a private rollback boundary.
BEGIN;
SET LOCAL ROLE ashlar_profile_custody_writer;
DO $$ DECLARE first ashlar_profile_custody.original%ROWTYPE; again ashlar_profile_custody.original%ROWTYPE;
BEGIN
  first=ashlar_profile_custody.retain('fixture-native-control','0.1',decode('00ff01','hex'),decode('fe0002','hex'));
  again=ashlar_profile_custody.retain('fixture-native-control','0.1',decode('00ff01','hex'),decode('fe0002','hex'));
  IF first IS DISTINCT FROM again OR first.original_database_role<>'ashlar_profile_custody_writer' OR first.original_session_role<>'postgres'
    OR first.definition_sha256<>sha256(decode('00ff01','hex')) THEN RAISE EXCEPTION 'Original native custody mismatch'; END IF;
  BEGIN
    PERFORM ashlar_profile_custody.retain('fixture-native-control','0.1',decode('00ff02','hex'),decode('fe0002','hex'));
    RAISE EXCEPTION 'Changed definition escaped refusal';
  EXCEPTION WHEN raise_exception THEN
    IF SQLERRM <> 'Original profile version/artifact conflict' THEN RAISE; END IF;
  END;
  BEGIN
    PERFORM ashlar_profile_custody.retain('fixture-native-control','0.1',decode('00ff01','hex'),decode('fe0003','hex'));
    RAISE EXCEPTION 'Changed bundle escaped refusal';
  EXCEPTION WHEN raise_exception THEN
    IF SQLERRM <> 'Original profile version/artifact conflict' THEN RAISE; END IF;
  END;
  BEGIN
    DELETE FROM ashlar_profile_custody.original;
    RAISE EXCEPTION 'Writer direct DELETE escaped refusal';
  EXCEPTION WHEN insufficient_privilege THEN NULL; END;
  BEGIN
    UPDATE ashlar_profile_custody.original SET definition_bytes='changed';
    RAISE EXCEPTION 'Writer direct UPDATE escaped refusal';
  EXCEPTION WHEN insufficient_privilege THEN NULL; END;
  BEGIN
    INSERT INTO ashlar_profile_custody.original(identity,version,definition_bytes,source_bundle_bytes,original_database_role,original_session_role,original_xid,original_at)
      VALUES('bypass','0.1','bytes','bundle','pretend','pretend',pg_current_xact_id(),clock_timestamp());
    RAISE EXCEPTION 'Writer direct INSERT escaped refusal';
  EXCEPTION WHEN insufficient_privilege THEN NULL; END;
END $$;
RESET ROLE;
SET LOCAL ROLE ashlar_profile_custody_reader;
DO $$ BEGIN
  IF NOT EXISTS(SELECT FROM ashlar_profile_custody.original WHERE identity='fixture-native-control' AND encode(definition_bytes,'hex')='00ff01')
    THEN RAISE EXCEPTION 'Reader original lookup unavailable'; END IF;
  BEGIN
    PERFORM ashlar_profile_custody.retain('reader-bypass','0.1','bytes','bundle');
    RAISE EXCEPTION 'Reader retain escaped refusal';
  EXCEPTION WHEN insufficient_privilege THEN NULL; END;
END $$;
ROLLBACK;
SELECT json_build_object('state','native controls passed; fixture rolled back','fixture_rows',(SELECT count(*) FROM ashlar_profile_custody.original WHERE identity='fixture-native-control'));
