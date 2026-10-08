-- UUID-wide unresolved-maintenance refusal under held pin custody.
BEGIN;
CREATE FUNCTION ashlar_pins.assert_uuid_available(p_uuid text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 LOCK TABLE ashlar_pins.pin IN SHARE MODE;
 IF p_uuid IS NULL OR octet_length(p_uuid) NOT BETWEEN 1 AND 1024 THEN
   RAISE EXCEPTION 'Explicit original UUID required';
 END IF;
 IF EXISTS(SELECT 1 FROM ashlar_pins.maintenance_quarantine
           WHERE table_uuid=p_uuid AND terminal_custody IS NULL) THEN
   RAISE EXCEPTION 'Unresolved cleanup refuses pin admission';
 END IF;
END $$;
REVOKE ALL ON FUNCTION ashlar_pins.assert_uuid_available(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ashlar_pins.assert_uuid_available(text) TO ashlar_pin_reader,ashlar_pin_writer;
COMMIT;
