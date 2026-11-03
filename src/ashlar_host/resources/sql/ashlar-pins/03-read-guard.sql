-- Add read-scope custody to an installed ashlar_pins registry.
BEGIN;
CREATE FUNCTION ashlar_pins.assert_active(p_id text,p_authority text,p_kind text,p_scope text,p_table text,p_uuid text,p_version bigint,p_digest bytea)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
BEGIN
  -- Caller retains this transaction throughout resolution and native reads.
  LOCK TABLE ashlar_pins.pin IN SHARE MODE;
  IF NOT EXISTS (SELECT 1 FROM ashlar_pins.pin WHERE pin_id=p_id
      AND authority=p_authority AND scope_kind=p_kind AND scope_id=p_scope
      AND table_name=p_table AND table_uuid=p_uuid AND version=p_version
      AND custody_digest=p_digest AND NOT released) THEN
    RAISE EXCEPTION 'Original active pin custody unavailable';
  END IF;
END $$;
REVOKE ALL ON FUNCTION ashlar_pins.assert_active(text,text,text,text,text,text,bigint,bytea) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ashlar_pins.assert_active(text,text,text,text,text,text,bigint,bytea) TO ashlar_pin_reader;
COMMIT;
