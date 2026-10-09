-- Explicit fresh private PostgreSQL source ACK service; render safe identifiers.
-- ACK EXECUTE credentials belong to a trusted publication-admitting host.
-- PostgreSQL binds original source custody, not external Delta publication truth.
BEGIN;
CREATE SCHEMA __ACK_SCHEMA__;
CREATE ROLE __ACK_ROLE__ NOLOGIN;
REVOKE ALL ON SCHEMA __ACK_SCHEMA__ FROM PUBLIC;
CREATE TABLE __ACK_SCHEMA__.installation (
 installation_id uuid PRIMARY KEY,
 source_schema name UNIQUE NOT NULL,
 signature jsonb NOT NULL
);
CREATE TABLE __ACK_SCHEMA__.consumer_scope (
 scope_id uuid PRIMARY KEY,
 installation_id uuid NOT NULL REFERENCES __ACK_SCHEMA__.installation,
 consumer text COLLATE "C" NOT NULL CHECK(octet_length(consumer) BETWEEN 1 AND 1024),
 feed text COLLATE "C" NOT NULL CHECK(octet_length(feed) BETWEEN 1 AND 1024),
 epoch text COLLATE "C" NOT NULL CHECK(octet_length(epoch) BETWEEN 1 AND 1024),
 position bigint NOT NULL DEFAULT 0 CHECK(position>=0),
 UNIQUE(installation_id,consumer,feed,epoch)
);
CREATE TABLE __ACK_SCHEMA__.receipt (
 scope_id uuid NOT NULL REFERENCES __ACK_SCHEMA__.consumer_scope,
 position bigint NOT NULL CHECK(position>0),
 previous bigint NOT NULL CHECK(previous>=0 AND position=previous+1),
 batch_id text COLLATE "C" NOT NULL,
 payload_digest bytea NOT NULL CHECK(octet_length(payload_digest)=32),
 request_bytes bytea NOT NULL CHECK(octet_length(request_bytes) BETWEEN 1 AND 4194304),
 manifest_bytes bytea NOT NULL CHECK(octet_length(manifest_bytes) BETWEEN 1 AND 4194304),
 receipt_bytes bytea NOT NULL CHECK(octet_length(receipt_bytes) BETWEEN 1 AND 1048576),
 PRIMARY KEY(scope_id,position),UNIQUE(scope_id,batch_id)
);
CREATE FUNCTION __ACK_SCHEMA__.source_signature(source_namespace name) RETURNS jsonb
LANGUAGE plpgsql SET search_path=pg_catalog,pg_temp AS $$
DECLARE ns record;b record;h record;f record;attrs jsonb;
BEGIN
 IF source_namespace::text !~ '^ashlar_ack_source_[a-z0-9_]+$' THEN RAISE EXCEPTION 'ACK_SOURCE_NAMESPACE_UNSUPPORTED'; END IF;
 SELECT oid,nspname,nspowner,nspacl INTO STRICT ns FROM pg_namespace WHERE nspname=source_namespace;
 SELECT oid,relowner,relkind,relpersistence,relrowsecurity,relforcerowsecurity,relacl,reloptions INTO STRICT b FROM pg_class WHERE relnamespace=ns.oid AND relname='batch';
 SELECT oid,relowner,relkind,relpersistence,relrowsecurity,relforcerowsecurity,relacl,reloptions INTO STRICT h FROM pg_class WHERE relnamespace=ns.oid AND relname='head';
 SELECT oid,proowner,prosecdef,proconfig,prosrc,proacl,prolang,provolatile INTO STRICT f FROM pg_proc WHERE pronamespace=ns.oid AND proname='append' AND proargtypes='25 25'::oidvector AND prorettype=20;
 IF b.relkind<>'r' OR h.relkind<>'r' OR b.relpersistence<>'p' OR h.relpersistence<>'p' OR b.relrowsecurity OR h.relrowsecurity OR b.relowner<>ns.nspowner OR h.relowner<>ns.nspowner OR f.proowner<>ns.nspowner OR NOT f.prosecdef THEN RAISE EXCEPTION 'ACK_SOURCE_OWNER_OR_KIND_UNSUPPORTED'; END IF;
 SELECT jsonb_agg(jsonb_build_array(attname,format_type(atttypid,atttypmod),attnotnull) ORDER BY attnum) INTO attrs FROM pg_attribute WHERE attrelid=b.oid AND attnum>0 AND NOT attisdropped;
 IF attrs<>'[["position","bigint",true],["batch_id","text",true],["payload","text",true],["digest","bytea",true]]'::jsonb THEN RAISE EXCEPTION 'ACK_SOURCE_SCHEMA_UNSUPPORTED'; END IF;
 RETURN jsonb_build_object('namespace',to_jsonb(ns),'batch',to_jsonb(b),'head',to_jsonb(h),'append',to_jsonb(f),'attributes',
   (SELECT jsonb_agg(to_jsonb(a) ORDER BY a.attrelid,a.attnum) FROM pg_attribute a WHERE a.attrelid IN(b.oid,h.oid) AND a.attnum>0),
   'constraints',(SELECT jsonb_agg(jsonb_build_array(c.oid,c.conrelid,c.conname,pg_get_constraintdef(c.oid)) ORDER BY c.oid) FROM pg_constraint c WHERE c.conrelid IN(b.oid,h.oid)),
   'triggers',(SELECT jsonb_agg(jsonb_build_array(t.oid,pg_get_triggerdef(t.oid)) ORDER BY t.oid) FROM pg_trigger t WHERE t.tgrelid IN(b.oid,h.oid)),
   'indexes',(SELECT jsonb_agg(jsonb_build_array(i.indexrelid,pg_get_indexdef(i.indexrelid)) ORDER BY i.indexrelid) FROM pg_index i WHERE i.indrelid IN(b.oid,h.oid)));
END $$;
-- Owner-only registration. Installation/scope rows cannot be replaced or bootstrapped.
CREATE FUNCTION __ACK_SCHEMA__.register_source(installation uuid,source_namespace name) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 INSERT INTO __ACK_SCHEMA__.installation VALUES(installation,source_namespace,__ACK_SCHEMA__.source_signature(source_namespace));
END $$;
CREATE FUNCTION __ACK_SCHEMA__.register_consumer(scope uuid,installation uuid,consumer_identity text,source_feed text,source_epoch text) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
BEGIN
 INSERT INTO __ACK_SCHEMA__.consumer_scope(scope_id,installation_id,consumer,feed,epoch) VALUES(scope,installation,consumer_identity,source_feed,source_epoch);
END $$;
CREATE FUNCTION __ACK_SCHEMA__.ack(scope uuid,previous_position bigint,next_position bigint,original_batch text,original_digest bytea,original_request bytea,original_manifest bytea,original_receipt bytea) RETURNS bytea
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE s __ACK_SCHEMA__.consumer_scope%ROWTYPE;i __ACK_SCHEMA__.installation%ROWTYPE;prior __ACK_SCHEMA__.receipt%ROWTYPE;batch record;q jsonb;m jsonb;c jsonb;r jsonb;
BEGIN
 IF NOT pg_has_role(session_user,'__ACK_ROLE__','MEMBER') THEN RAISE EXCEPTION 'ACK_CALLER_UNAUTHORIZED'; END IF;
 IF previous_position IS NULL OR next_position IS NULL OR previous_position<0 OR previous_position=9223372036854775807 OR next_position<>previous_position+1 OR original_batch IS NULL OR octet_length(original_batch) NOT BETWEEN 1 AND 1024 OR original_digest IS NULL OR octet_length(original_digest)<>32 OR original_request IS NULL OR octet_length(original_request) NOT BETWEEN 1 AND 4194304 OR original_manifest IS NULL OR octet_length(original_manifest) NOT BETWEEN 1 AND 4194304 OR original_receipt IS NULL OR octet_length(original_receipt) NOT BETWEEN 1 AND 1048576 THEN RAISE EXCEPTION 'ACK_INPUT_INVALID'; END IF;
 SELECT * INTO STRICT s FROM __ACK_SCHEMA__.consumer_scope WHERE scope_id=scope FOR UPDATE;
 SELECT * INTO STRICT i FROM __ACK_SCHEMA__.installation WHERE installation_id=s.installation_id;
 -- Table locks hold through caller commit and exclude conflicting table/data operations.
 -- Catalog/function/role changes require trusted-owner coordination; observed
 -- signature checks do not claim an atomic fence over every catalog permission.
 EXECUTE format('LOCK TABLE %I.head IN SHARE MODE',i.source_schema);
 EXECUTE format('LOCK TABLE %I.batch IN SHARE MODE',i.source_schema);
 IF __ACK_SCHEMA__.source_signature(i.source_schema)<>i.signature THEN RAISE EXCEPTION 'ACK_SOURCE_INSTALLATION_CHANGED'; END IF;
 EXECUTE format('SELECT position,batch_id,payload,digest FROM %I.batch WHERE position=$1',i.source_schema) INTO STRICT batch USING next_position;
 IF batch.batch_id COLLATE "C"<>original_batch COLLATE "C" OR batch.digest<>original_digest OR sha256(convert_to(batch.payload,'UTF8'))<>original_digest THEN RAISE EXCEPTION 'ACK_ORIGINAL_BATCH_MISMATCH'; END IF;
 q:=convert_from(original_request,'UTF8')::jsonb;m:=convert_from(original_manifest,'UTF8')::jsonb;r:=convert_from(original_receipt,'UTF8')::jsonb;
 IF jsonb_typeof(q->'request_digest') IS DISTINCT FROM 'string' OR q->>'request_digest' !~ '^[0-9a-f]{64}$' OR jsonb_typeof(m->'publication_id') IS DISTINCT FROM 'string' OR octet_length(m->>'publication_id') NOT BETWEEN 1 AND 1024 OR jsonb_typeof(m->'validation_report_json') IS DISTINCT FROM 'string' OR jsonb_typeof(q->'source_checkpoint_json') IS DISTINCT FROM 'string' THEN RAISE EXCEPTION 'ACK_REQUIRED_CUSTODY_FIELDS_INVALID'; END IF;
 IF jsonb_typeof(((m->>'validation_report_json')::jsonb)->'request_digest') IS DISTINCT FROM 'string' OR ((m->>'validation_report_json')::jsonb)->>'request_digest' !~ '^[0-9a-f]{64}$' THEN RAISE EXCEPTION 'ACK_REQUIRED_CUSTODY_FIELDS_INVALID'; END IF;
 c:=(q->>'source_checkpoint_json')::jsonb;
 IF c IS DISTINCT FROM jsonb_build_object('profile','ashlar-postgresql-outbox/0.1','feed',s.feed,'epoch',s.epoch,'previous',previous_position::text,'position',next_position::text,'batch_id',original_batch,'payload_digest',encode(original_digest,'hex')) OR m->'source_progress_json' IS NULL OR ((m->>'source_progress_json')::jsonb)->s.feed IS DISTINCT FROM c OR q->>'batch_id' IS DISTINCT FROM original_batch OR ((m->>'validation_report_json')::jsonb)->>'request_digest' IS DISTINCT FROM q->>'request_digest' THEN RAISE EXCEPTION 'ACK_PUBLICATION_CORRESPONDENCE_MISMATCH'; END IF;
 IF r IS DISTINCT FROM jsonb_build_object('format','ashlar-protected-source-ack/0.1','scope_id',scope::text,'installation_id',s.installation_id::text,'consumer',s.consumer,'feed',s.feed,'epoch',s.epoch,'previous',previous_position::text,'position',next_position::text,'batch_id',original_batch,'payload_digest',encode(original_digest,'hex'),'request_sha256',encode(sha256(original_request),'hex'),'manifest_sha256',encode(sha256(original_manifest),'hex'),'publication_id',m->>'publication_id') THEN RAISE EXCEPTION 'ACK_RECEIPT_CORRESPONDENCE_MISMATCH'; END IF;
 SELECT * INTO prior FROM __ACK_SCHEMA__.receipt WHERE scope_id=scope AND position=next_position;
 IF FOUND THEN
   IF prior.previous<>previous_position OR prior.batch_id<>original_batch OR prior.payload_digest<>original_digest OR prior.request_bytes<>original_request OR prior.manifest_bytes<>original_manifest OR prior.receipt_bytes<>original_receipt THEN RAISE EXCEPTION 'ACK_IMMUTABLE_RECEIPT_CONFLICT'; END IF;
   RETURN prior.receipt_bytes;
 END IF;
 IF s.position<>previous_position THEN RAISE EXCEPTION 'ACK_CONTIGUOUS_CAS_CONFLICT'; END IF;
 INSERT INTO __ACK_SCHEMA__.receipt VALUES(scope,next_position,previous_position,original_batch,original_digest,original_request,original_manifest,original_receipt);
 UPDATE __ACK_SCHEMA__.consumer_scope SET position=next_position WHERE scope_id=scope AND position=previous_position;
 IF NOT FOUND THEN RAISE EXCEPTION 'ACK_CONTIGUOUS_CAS_CONFLICT'; END IF;
 RETURN original_receipt;
END $$;
CREATE FUNCTION __ACK_SCHEMA__.scope_binding(scope uuid) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
DECLARE s __ACK_SCHEMA__.consumer_scope%ROWTYPE;i __ACK_SCHEMA__.installation%ROWTYPE;
BEGIN
 SELECT * INTO STRICT s FROM __ACK_SCHEMA__.consumer_scope WHERE scope_id=scope;
 SELECT * INTO STRICT i FROM __ACK_SCHEMA__.installation WHERE installation_id=s.installation_id;
 IF __ACK_SCHEMA__.source_signature(i.source_schema)<>i.signature THEN RAISE EXCEPTION 'ACK_SOURCE_INSTALLATION_CHANGED'; END IF;
 RETURN jsonb_build_object('scope_id',s.scope_id::text,'installation_id',s.installation_id::text,'consumer',s.consumer,'feed',s.feed,'epoch',s.epoch,'source_schema',i.source_schema,'source_signature_sha256',encode(sha256(convert_to(i.signature::text,'UTF8')),'hex'));
END $$;
CREATE FUNCTION __ACK_SCHEMA__.observe(scope uuid,at_position bigint) RETURNS TABLE("position" text,request_hex text,manifest_hex text,receipt_hex text)
LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog,pg_temp AS $$
 SELECT s.position::text,encode(r.request_bytes,'hex'),encode(r.manifest_bytes,'hex'),encode(r.receipt_bytes,'hex') FROM __ACK_SCHEMA__.consumer_scope s LEFT JOIN __ACK_SCHEMA__.receipt r ON r.scope_id=s.scope_id AND r.position=at_position WHERE s.scope_id=scope FOR SHARE OF s;
$$;
REVOKE ALL ON ALL TABLES IN SCHEMA __ACK_SCHEMA__ FROM PUBLIC,__ACK_ROLE__;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA __ACK_SCHEMA__ FROM PUBLIC;
GRANT USAGE ON SCHEMA __ACK_SCHEMA__ TO __ACK_ROLE__;
GRANT EXECUTE ON FUNCTION __ACK_SCHEMA__.ack(uuid,bigint,bigint,text,bytea,bytea,bytea,bytea),__ACK_SCHEMA__.observe(uuid,bigint),__ACK_SCHEMA__.scope_binding(uuid) TO __ACK_ROLE__;
COMMIT;
