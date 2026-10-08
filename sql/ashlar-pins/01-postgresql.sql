-- Fresh private deployment only. No TTL, deletion, VACUUM or Delta policy changes.
BEGIN;
CREATE SCHEMA ashlar_pins;
REVOKE ALL ON SCHEMA ashlar_pins FROM PUBLIC;
CREATE ROLE ashlar_pin_writer NOLOGIN;
CREATE ROLE ashlar_pin_reader NOLOGIN;
CREATE ROLE ashlar_pin_maintenance NOLOGIN;
CREATE TABLE ashlar_pins.pin (
  pin_id text COLLATE "C" PRIMARY KEY,
  authority text NOT NULL,
  scope_kind text NOT NULL CHECK (scope_kind IN ('manifest','cursor','recovery','release')),
  scope_id text NOT NULL,
  table_name text NOT NULL,
  table_uuid text NOT NULL,
  version bigint NOT NULL CHECK (version >= 0),
  custody_digest bytea NOT NULL CHECK (octet_length(custody_digest)=32),
  released boolean NOT NULL DEFAULT false,
  CHECK (octet_length(pin_id) BETWEEN 1 AND 1024),
  CHECK (octet_length(authority) BETWEEN 1 AND 1024),
  CHECK (octet_length(scope_id) BETWEEN 1 AND 1024),
  CHECK (octet_length(table_uuid) BETWEEN 1 AND 1024),
  CHECK (table_name ~ '^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$')
);
CREATE FUNCTION ashlar_pins.register(p_id text,p_authority text,p_kind text,p_scope text,p_table text,p_uuid text,p_version bigint,p_digest bytea)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE existing ashlar_pins.pin%ROWTYPE;
BEGIN
  LOCK TABLE ashlar_pins.pin IN SHARE ROW EXCLUSIVE MODE;
  SELECT * INTO existing FROM ashlar_pins.pin WHERE pin_id=p_id;
  IF FOUND THEN
    IF ROW(existing.authority,existing.scope_kind,existing.scope_id,existing.table_name,existing.table_uuid,existing.version,existing.custody_digest)
       IS DISTINCT FROM ROW(p_authority,p_kind,p_scope,p_table,p_uuid,p_version,p_digest) OR existing.released THEN
      RAISE EXCEPTION 'Immutable pin conflict or released original';
    END IF;
    RETURN;
  END IF;
  INSERT INTO ashlar_pins.pin(pin_id,authority,scope_kind,scope_id,table_name,table_uuid,version,custody_digest)
    VALUES(p_id,p_authority,p_kind,p_scope,p_table,p_uuid,p_version,p_digest);
END $$;
CREATE FUNCTION ashlar_pins.release(p_id text,p_authority text,p_digest bytea)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE existing ashlar_pins.pin%ROWTYPE;
BEGIN
  LOCK TABLE ashlar_pins.pin IN SHARE ROW EXCLUSIVE MODE;
  SELECT * INTO existing FROM ashlar_pins.pin WHERE pin_id=p_id;
  IF NOT FOUND OR existing.authority IS DISTINCT FROM p_authority OR existing.custody_digest IS DISTINCT FROM p_digest THEN
    RAISE EXCEPTION 'Missing or mismatched original pin release';
  END IF;
  UPDATE ashlar_pins.pin SET released=true WHERE pin_id=p_id;
END $$;
CREATE FUNCTION ashlar_pins.assert_unpinned(p_table text,p_uuid text,p_version bigint)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
BEGIN
  -- Hold this transaction open throughout admitted retention work. Share lock
  -- excludes registration/release until maintenance completes or rolls back.
  LOCK TABLE ashlar_pins.pin IN SHARE MODE;
  IF p_table IS NULL OR p_uuid IS NULL OR p_version IS NULL OR p_version < 0 THEN
    RAISE EXCEPTION 'Explicit retention target required';
  END IF;
  IF EXISTS (SELECT 1 FROM ashlar_pins.pin WHERE table_name=p_table AND table_uuid=p_uuid AND version=p_version AND NOT released) THEN
    RAISE EXCEPTION 'Active pin refuses retention';
  END IF;
END $$;
REVOKE ALL ON ALL TABLES IN SCHEMA ashlar_pins FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA ashlar_pins FROM PUBLIC;
GRANT USAGE ON SCHEMA ashlar_pins TO ashlar_pin_writer,ashlar_pin_reader,ashlar_pin_maintenance;
GRANT SELECT ON ashlar_pins.pin TO ashlar_pin_reader;
GRANT EXECUTE ON FUNCTION ashlar_pins.register(text,text,text,text,text,text,bigint,bytea),ashlar_pins.release(text,text,bytea) TO ashlar_pin_writer;
GRANT EXECUTE ON FUNCTION ashlar_pins.assert_unpinned(text,text,bigint) TO ashlar_pin_maintenance;
COMMIT;
