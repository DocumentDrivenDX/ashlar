-- Private development source profile ashlar-postgresql-outbox/0.1.
-- Fresh namespace only. This does not install Truss or alter its catalog.
BEGIN;
CREATE SCHEMA ashlar_outbox;
CREATE ROLE ashlar_outbox_writer NOLOGIN;
CREATE ROLE ashlar_outbox_reader NOLOGIN;
CREATE TABLE ashlar_outbox.head (id integer PRIMARY KEY CHECK(id=1), position bigint NOT NULL CHECK(position>=0));
INSERT INTO ashlar_outbox.head VALUES(1,0);
CREATE TABLE ashlar_outbox.batch (
 position bigint PRIMARY KEY CHECK(position>0),
 batch_id text COLLATE "C" UNIQUE NOT NULL CHECK(octet_length(batch_id)>0),
 payload text NOT NULL CHECK(octet_length(payload) BETWEEN 1 AND 1048576),
 digest bytea NOT NULL CHECK(octet_length(digest)=32)
);
CREATE FUNCTION ashlar_outbox.append(batch_identity text, original_payload text) RETURNS bigint
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE prior ashlar_outbox.batch%ROWTYPE; next_position bigint;
BEGIN
 IF batch_identity IS NULL OR octet_length(batch_identity)=0 OR original_payload IS NULL
    OR octet_length(original_payload) NOT BETWEEN 1 AND 1048576 THEN
   RAISE EXCEPTION 'OUTBOX_INPUT_INVALID';
 END IF;
 -- Lock remains held through the caller's actual outer commit/rollback. This
 -- serializes allocation and commit order; the returned position is pending.
 SELECT position INTO next_position FROM ashlar_outbox.head WHERE id=1 FOR UPDATE;
 SELECT * INTO prior FROM ashlar_outbox.batch WHERE batch_id=batch_identity COLLATE "C";
 IF FOUND THEN
   IF convert_to(prior.payload,'UTF8')<>convert_to(original_payload,'UTF8') THEN
     RAISE EXCEPTION 'OUTBOX_BATCH_CONFLICT';
   END IF;
   RETURN prior.position;
 END IF;
 IF next_position=9223372036854775807 THEN RAISE EXCEPTION 'OUTBOX_POSITION_EXHAUSTED'; END IF;
 next_position:=next_position+1;
 INSERT INTO ashlar_outbox.batch VALUES(next_position,batch_identity,original_payload,sha256(convert_to(original_payload,'UTF8')));
 UPDATE ashlar_outbox.head SET position=next_position WHERE id=1;
 RETURN next_position;
END $$;
REVOKE ALL ON SCHEMA ashlar_outbox FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA ashlar_outbox FROM PUBLIC;
REVOKE ALL ON FUNCTION ashlar_outbox.append(text,text) FROM PUBLIC;
GRANT USAGE ON SCHEMA ashlar_outbox TO ashlar_outbox_writer,ashlar_outbox_reader;
GRANT EXECUTE ON FUNCTION ashlar_outbox.append(text,text) TO ashlar_outbox_writer;
GRANT SELECT ON ashlar_outbox.head,ashlar_outbox.batch TO ashlar_outbox_reader;
COMMIT;
