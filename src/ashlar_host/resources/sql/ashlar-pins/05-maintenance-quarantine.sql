-- Durable cleanup uncertainty custody. No cleanup, expiry or pin release.
BEGIN;
CREATE ROLE ashlar_pin_recovery NOLOGIN;
CREATE TABLE ashlar_pins.maintenance_quarantine (
 operation_id text COLLATE "C" PRIMARY KEY CHECK(octet_length(operation_id) BETWEEN 1 AND 1024),
 table_name text NOT NULL,
 table_uuid text NOT NULL CHECK(octet_length(table_uuid) BETWEEN 1 AND 1024),
 original_intent bytea NOT NULL CHECK(octet_length(original_intent) BETWEEN 1 AND 1048576),
 terminal_custody bytea CHECK(octet_length(terminal_custody) BETWEEN 1 AND 1048576),
 CHECK(table_name ~ '^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$')
);
REVOKE ALL ON ashlar_pins.maintenance_quarantine FROM PUBLIC;
CREATE FUNCTION ashlar_pins.quarantine_cleanup(p_operation text,p_table text,p_uuid text,p_intent bytea)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE original ashlar_pins.maintenance_quarantine%ROWTYPE;
BEGIN
 LOCK TABLE ashlar_pins.pin IN SHARE ROW EXCLUSIVE MODE;
 LOCK TABLE ashlar_pins.maintenance_quarantine IN SHARE ROW EXCLUSIVE MODE;
 SELECT * INTO original FROM ashlar_pins.maintenance_quarantine WHERE operation_id=p_operation;
 IF FOUND THEN
   IF ROW(original.table_name,original.table_uuid,original.original_intent)
      IS DISTINCT FROM ROW(p_table,p_uuid,p_intent) OR original.terminal_custody IS NOT NULL THEN
     RAISE EXCEPTION 'Original cleanup custody conflicts or is closed';
   END IF;
   RETURN;
 END IF;
 -- UUID-wide refusal conservatively includes renamed aliases and every scope.
 IF EXISTS(SELECT 1 FROM ashlar_pins.pin WHERE table_uuid=p_uuid AND NOT released) THEN
   RAISE EXCEPTION 'Active pin refuses cleanup quarantine';
 END IF;
 INSERT INTO ashlar_pins.maintenance_quarantine(operation_id,table_name,table_uuid,original_intent)
 VALUES(p_operation,p_table,p_uuid,p_intent);
END $$;
CREATE FUNCTION ashlar_pins.close_cleanup_quarantine(p_operation text,p_intent bytea,p_terminal bytea)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE original ashlar_pins.maintenance_quarantine%ROWTYPE;
BEGIN
 LOCK TABLE ashlar_pins.pin IN SHARE ROW EXCLUSIVE MODE;
 LOCK TABLE ashlar_pins.maintenance_quarantine IN SHARE ROW EXCLUSIVE MODE;
 SELECT * INTO original FROM ashlar_pins.maintenance_quarantine WHERE operation_id=p_operation;
 IF NOT FOUND OR original.original_intent IS DISTINCT FROM p_intent OR p_terminal IS NULL
    OR octet_length(p_terminal) NOT BETWEEN 1 AND 1048576 THEN
   RAISE EXCEPTION 'Original cleanup and verified terminal custody required';
 END IF;
 IF original.terminal_custody IS NOT NULL AND original.terminal_custody IS DISTINCT FROM p_terminal THEN
   RAISE EXCEPTION 'Original terminal custody conflict';
 END IF;
 UPDATE ashlar_pins.maintenance_quarantine SET terminal_custody=p_terminal WHERE operation_id=p_operation;
END $$;
CREATE FUNCTION ashlar_pins.pin_quarantine_check()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 IF EXISTS(SELECT 1 FROM ashlar_pins.maintenance_quarantine
           WHERE table_uuid=NEW.table_uuid AND terminal_custody IS NULL) THEN
   RAISE EXCEPTION 'Unresolved cleanup refuses new pin';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER pin_quarantine_check BEFORE INSERT ON ashlar_pins.pin
FOR EACH ROW EXECUTE FUNCTION ashlar_pins.pin_quarantine_check();
REVOKE ALL ON FUNCTION ashlar_pins.quarantine_cleanup(text,text,text,bytea),
 ashlar_pins.close_cleanup_quarantine(text,bytea,bytea),ashlar_pins.pin_quarantine_check() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ashlar_pins.quarantine_cleanup(text,text,text,bytea) TO ashlar_pin_maintenance;
GRANT USAGE ON SCHEMA ashlar_pins TO ashlar_pin_recovery;
GRANT EXECUTE ON FUNCTION ashlar_pins.close_cleanup_quarantine(text,bytea,bytea) TO ashlar_pin_recovery;
GRANT SELECT ON ashlar_pins.maintenance_quarantine TO ashlar_pin_reader;
COMMIT;
