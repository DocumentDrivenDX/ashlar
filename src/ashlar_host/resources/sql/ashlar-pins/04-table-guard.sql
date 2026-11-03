-- Conservative whole-table cleanup guard; no cleanup/TTL/retention change.
BEGIN;
CREATE FUNCTION ashlar_pins.assert_table_unpinned(p_table text,p_uuid text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
BEGIN
  LOCK TABLE ashlar_pins.pin IN SHARE MODE;
  IF p_table IS NULL OR p_uuid IS NULL OR octet_length(p_uuid) NOT BETWEEN 1 AND 1024
     OR p_table !~ '^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$' THEN
    RAISE EXCEPTION 'Explicit cleanup table identity required';
  END IF;
  -- Check every authority/scope/version, not a caller-selected single version.
  IF EXISTS (SELECT 1 FROM ashlar_pins.pin WHERE table_name=p_table
             AND table_uuid=p_uuid AND NOT released) THEN
    RAISE EXCEPTION 'Active table pin refuses cleanup';
  END IF;
END $$;
REVOKE ALL ON FUNCTION ashlar_pins.assert_table_unpinned(text,text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ashlar_pins.assert_table_unpinned(text,text) TO ashlar_pin_maintenance;
COMMIT;
