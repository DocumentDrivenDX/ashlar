"""Small private PostgreSQL outbox → local Delta → protected ACK qualification.

Explicit fixture IDs/source bindings and process-local retained pins only. Object
values use genuine UMF current-core Record checks; the separately declared edge
control is not UMF relationship admission, accepted Truss identity or UC policy.
"""
import argparse, fcntl, hashlib, importlib.metadata, json, os, subprocess, uuid
from contextlib import contextmanager
from pathlib import Path
from dataclasses import asdict
from ashlar.apply import empty_state
from ashlar.attempt_store import DeltaAttemptStore
from ashlar.manifest import DeltaManifestStore, FIELDS, manifest_pin_vector
from ashlar.outbox import PostgresOutbox, publish_outbox_transaction
from ashlar.publication import Snapshot
from ashlar.source import jsonl_batches, records_digest
from ashlar.source_checkpoint import outbox_checkpoint
from ashlar.staging import batch_row
from ashlar.stored_publisher import StoredPublisherBackend
from ashlar.whole_entity import changes_from_batch
from fixture_oracle import fixture_columns, fixture_inventory
from local_delta_custody import DeltaTarget, LocalDeltaTransport, LocalDeltaEffects, LocalOperationExecutor, child_operation, encoded, sha, LocalDeltaError
from local_outbox_connection import connect, CONTAINER
from postgres_transactions import Session
from protected_outbox_ack import AckScope, ProtectedOutboxAck, AckOutcomeUncertain, render_ddl
from run_graph_release_graphframes import JARS, VERSIONS
from run_local_example import fixture_inputs, check_value_request, fixture_value_entries, RECORD_CHECK_PIN
from whole_graph_sql import graph_sql_plan
ROOT=Path(__file__).resolve().parents[1]
CLOCK='2026-10-09T12:00:00+00:00'
PROFILE='ashlar-delta/0.3'

def raw_transaction(identity,events):
    def line(value):return (json.dumps(value,ensure_ascii=False,separators=(',',':'))+'\n').encode()
    records=[line(event) for event in events]
    return line({'kind':'begin','batch_id':identity})+b''.join(records)+line({'kind':'commit','batch_id':identity,'record_count':len(records),'records_sha256':records_digest(records)})

def fixture():
    """Closed source profile; IDs overlap but full source/feed/epoch bindings do not."""
    result=[]
    for label in ('a','b'):
        feed='local-outbox-'+label;epoch='original-'+label
        source=json.dumps(['private-outbox-control',feed,epoch],separators=(',',':'))
        def event(kind,identity,version,operation,props):
            value={'kind':'event','source_profile':'ashlar-whole-entity/0.1','source_system':source,'schema_revision':'3','retained_json':' {"opaque":18446744073709551615} ',
                   'delivery_id':label+'-'+kind+'-'+identity+'-'+version,'entity_kind':kind,'type_id':'17' if kind=='object' else '2','id':identity,'entity_version':version,'operation':operation,'props_json':props}
            if kind=='edge':value['endpoints']=[{'type_id':'17','id':'1'},{'type_id':'17','id':'2'}]
            return value
        first=[event('object','1','1','create','{"23":"'+label+' first","24":"雪"}'),event('object','2','1','create','{"23":"'+label+' second"}'),event('edge','1','1','create',' {} ')]
        second=[event('object','1','2','replace','{"23":"'+label+' updated"}'),event('edge','1','2','delete',' {} '),event('object','2','2','delete','{"23":"'+label+' second"}')]
        batches=[]
        for ordinal,events in enumerate((first,second),1):
            raw=raw_transaction(label+'-'+str(ordinal),events)
            batch=next(jsonl_batches(raw.splitlines(keepends=True),feed=feed,epoch=epoch))
            batches.append((raw,batch))
        result.append({'label':label,'feed':feed,'epoch':epoch,'source':source,'batches':batches})
    return result

def progress_union(previous,checkpoint):
    result=json.loads(encoded(previous));old=result.get(checkpoint['feed'])
    if old is not None and (old['epoch']!=checkpoint['epoch'] or old['position']!=checkpoint['previous']):raise ValueError('Original contiguous source epoch/progress required')
    if old is None and checkpoint['previous']!='0':raise ValueError('Fresh source must begin at zero')
    result[checkpoint['feed']]=dict(checkpoint)
    return result

def validate_union(previous,request,manifest):
    checkpoint=json.loads(request['source_checkpoint_json'])
    if json.loads(manifest['source_progress_json'])!=progress_union(previous,checkpoint):raise ValueError('Complete independent source progress union differs')
    report=json.loads(manifest['validation_report_json'])
    if report['predecessor']!=request['predecessor'] or report['request_digest']!=request['request_digest']:raise ValueError('Global predecessor/request custody differs')

def local_effect_plan(generated,prior_rows,tables):
    """Elide only closed generated zero-match DELETE before any submission.

    Caller must independently verify complete original native prior rows/UUIDs
    under the held writer. This pure correspondence function grants no admission.
    """
    selected=[];elisions=[]
    for ordinal,step in enumerate(generated):
        found=None
        for role,typed in (('object_current','type_id'),('edge_current','rel_type_id')):
            quoted='`'+'`.`'.join(tables[role].split('.'))+'`'
            expected='MERGE INTO '+quoted+" t USING (SELECT k.* FROM (SELECT explode(from_json(:keys,'ARRAY<STRUCT<source_system:STRING,type_id:STRING,id:STRING>>')) k)) s ON t.source_system=s.source_system AND t."+typed+'=cast(s.type_id AS BIGINT) AND t.id=cast(s.id AS BIGINT) WHEN MATCHED THEN DELETE'
            if step['statement']!=expected:continue
            if set(step['parameters'])!={'keys'}:raise ValueError('Closed generated delete key inventory required')
            keys=json.loads(step['parameters']['keys'])
            if not isinstance(keys,list) or not keys or any(set(k)!={'source_system','type_id','id'} or any(type(v)is not str for v in k.values()) for k in keys):raise ValueError('Exact generated source-qualified delete keys required')
            matches=[row for row in prior_rows[role] if any((row['source_system'],row[typed],row['id'])==(k['source_system'],k['type_id'],k['id']) for k in keys)]
            if not matches:found={'ordinal':ordinal,'original_step':json.loads(encoded(step)),'table':tables[role],'source_keys':keys,'matched_rows':[],'complete_prior_rows_sha256':sha(encoded(sorted(prior_rows[role],key=encoded)))}
        if found is None:selected.append(json.loads(encoded(step)))
        else:elisions.append(found)
    return selected,elisions

class LostAfterManifest(RuntimeError):pass

class PrivatePolicy:
    def __init__(self,context,targets):self.context=context;self.targets=targets;self.active=None;self.initializing=True
    @contextmanager
    def writer(self,*args):
        if args[-1] is not self.context:raise PermissionError('Original held private writer context required')
        yield
    def admit(self,intent,context):
        if context is not self.context:raise PermissionError('Original private writer context required')
        profile=intent.get('profile')
        if profile=='ashlar-local-delta-installation/0.1':
            expected=[{'table':t.table,'path':str(t.path),'uuid':t.uuid} for t in sorted(self.targets,key=lambda t:t.table)]
            if not self.initializing or intent['targets']!=expected:raise PermissionError('Original fresh target registry required')
            return
        active=self.active
        if active is None:raise PermissionError('Original native request admission missing')
        if profile=='ashlar-local-delta-effects/0.1':
            if intent['steps']!=active['steps'] or intent['intent_digest']!=active['request']['request_digest']:raise PermissionError('Exact original effect plan required')
        elif profile=='ashlar-local-delta-operation/0.1':
            if intent['request_digest']!=active['request']['request_digest']:raise PermissionError('Original native request digest differs')
            role=intent['table'].split('.')[-1]
            if role in ('attempts','manifest'):
                raw=intent['parameters'].get('payload' if role=='attempts' else 'row')
                if raw is None:raise PermissionError('Original closed metadata mutation required')
                value=json.loads(raw)
                if role=='attempts' and json.loads(value['payload_json'])['request']!=active['request']:raise PermissionError('Original phase request differs')
                if role=='manifest' and value!=active.get('manifest'):raise PermissionError('Original manifest proposal differs')
            elif {'statement':intent['statement'],'parameters':intent['parameters']} not in active['steps']:raise PermissionError('Original graph mutation missing exact plan')
        else:raise PermissionError('Unrecognized local original intent')

class CarrierPolicy:
    def __init__(self,owner,kind):self.owner=owner;self.kind=kind
    @contextmanager
    def writer(self,table,native_uuid,context):
        self.owner.require(context);target=self.owner.transport.targets[table]
        if native_uuid!=target.uuid:raise PermissionError('Original metadata target differs')
        yield
    def admit(self,row,context):
        self.owner.require(context)
        if self.kind=='manifest':self.owner.validate_manifest(row,context)

class RecoveryExecutor:
    """Reads plus exact original recover dispatch; never creates a mutation."""
    def __init__(self,original):self.original=original
    def query(self,sql,parameters):
        original=self.original
        if sql.lstrip().split(None,1)[0].upper() in ('SELECT','DESCRIBE','SHOW'):return original.transport.query(sql,parameters)
        return original.transport.recover(child_operation(original.operation,sql,parameters),sql,parameters,intent_digest=original.intent_digest,context=original.context)

class ManifestPort:
    """Generic manifest store, with recovery restricted to retained original SQL."""
    def __init__(self,owner,request):
        self.owner=owner;self.executor=LocalOperationExecutor(owner.transport,'manifest:'+request['request_digest'],request['request_digest'],owner.context)
        target=owner.transport.targets[owner.tables['manifest']]
        self.store=DeltaManifestStore(self.executor,CarrierPolicy(owner,'manifest'),target.table,target.uuid)
    def commit(self,row,*,context):return self.store.commit(row,context=context)
    def recover(self,row,*,context):
        # Existing original manifest mutation must already have durable custody.
        if not self.owner.transport.db.execute('SELECT 1 FROM local_operation WHERE operation LIKE ?',('manifest:'+self.executor.intent_digest+':%',)).fetchone():raise LocalDeltaError('Original manifest submission absent; recovery cannot replace it')
        store=DeltaManifestStore(RecoveryExecutor(self.executor),self.store.policy,self.store.table,self.store.uuid)
        return store.commit(row,context=context)

class FixedStringAdmission:
    """Explicit finite current-core Record plus declared fixture edge control."""
    def __init__(self,changes,receipt,*,receipt_bytes):
        self.changes=tuple(changes)
        if type(receipt_bytes)is not bytes or json.loads(receipt_bytes)!=receipt:raise PermissionError('Original public receipt bytes required')
        positives=[(c.delivery_id,c.raw_digest) for c in self.changes if c.state.key.kind=='object' and c.operation!='delete']
        if [(r['deliveryId'],r['recordSha256']) for r in receipt['records']]!=positives or receipt['sourceSha256']!=hashlib.sha256((ROOT/'examples/end-to-end/schema-v3.umf.json').read_bytes()).hexdigest():raise PermissionError('Original fixed-string Record receipt custody differs')
        if any(c.state.key.kind=='object' and c.state.key.type_id!=17 or c.state.schema_revision!='3' for c in self.changes):raise PermissionError('Original fixed-string source binding required')
        if receipt['producerRevision']!=RECORD_CHECK_PIN or any(r['result']['validation']['valid'] is not True or r['result']['validation']['complete'] is not True for r in receipt['records']):raise PermissionError('Actual pinned fixed-string Record receipt required')
        self.facts={'profile':'ashlar-fixed-string-and-finite-edge-admission/0.1','qualification':__doc__,'umf_revision':receipt['producerRevision'],'model_sha256':receipt['sourceSha256'],'public_record_request_sha256':receipt['requestSha256'],'public_record_receipt_sha256':hashlib.sha256(receipt_bytes).hexdigest(),
                    'record_deliveries':[{'delivery_id':r['deliveryId'],'record_sha256':r['recordSha256']} for r in receipt['records']],'edge_scope':'Separately declared exact finite fixture changes; no UMF relationship support claim'}
    def admit(self,change):
        if change not in self.changes:raise PermissionError('Original finite string/edge source change not admitted')
    def metadata(self):return json.loads(encoded(self.facts))

class NativeDriver:
    def __init__(self,transport,policy,context,tables,scope_ports,allowed_changes,columns,*,source_admission):
        self.transport=transport;self.policy=policy;self.context=context;self.tables=tables;self.scope_ports=scope_ports;self.allowed_changes=allowed_changes;self.columns=columns
        if not callable(getattr(source_admission,'admit',None)) or not callable(getattr(source_admission,'metadata',None)):raise PermissionError('Explicit admitted source profile required')
        self.source_admission=source_admission;self.original_admission=encoded(source_admission.metadata())
        facts=json.loads(self.original_admission)
        if type(facts)is not dict or not isinstance(facts.get('profile'),str) or not facts['profile'] or not isinstance(facts.get('qualification'),str) or not facts['qualification'] or len(self.original_admission.encode())>1048576:raise PermissionError('Bounded explicit immutable source profile facts required')
        self.held=False;self.pin_held=False;self.lose_manifest=False;self.lose_ack=False;self.acks=[]
        if not transport.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='local_publication_artifact'").fetchone():
            if transport.db.execute('SELECT 1 FROM local_plan LIMIT 1').fetchone():raise LocalDeltaError('Original artifact custody table absent after effects; no recreation')
            with transport.db:transport.db.execute('CREATE TABLE local_publication_artifact (request_digest TEXT PRIMARY KEY,request TEXT NOT NULL,artifact TEXT NOT NULL)')
        if not transport.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='local_source_plan'").fetchone():
            if transport.db.execute('SELECT 1 FROM local_plan LIMIT 1').fetchone():raise LocalDeltaError('Original source plan custody missing; no recreation')
            with transport.db:transport.db.execute('CREATE TABLE local_source_plan (request_digest TEXT PRIMARY KEY,original TEXT NOT NULL)')
    def admission_facts(self):
        if encoded(self.source_admission.metadata())!=self.original_admission:raise PermissionError('Original admitted source profile metadata changed')
        return json.loads(self.original_admission)
    def require(self,context):
        if context is not self.context or not self.held:raise PermissionError('Original private writer interval required')
    @contextmanager
    def writer(self,stream,context):
        if context is not self.context or self.held:raise PermissionError('Exclusive original process writer required')
        with (self.transport.journal_path.parent/'.pipeline-writer.lock').open('a') as gate:
            fcntl.flock(gate,fcntl.LOCK_EX);self.held=True
            try:yield
            finally:self.held=False;fcntl.flock(gate,fcntl.LOCK_UN)
    def schema_admit(self,change):
        self.admission_facts()
        if change not in self.allowed_changes:raise PermissionError('No independently admitted original source change')
        if self.source_admission.admit(change) is not None:raise PermissionError('Original source semantic admission incomplete')
        self.admission_facts()
    def source_admit(self,request):
        self.admission_facts()
        checkpoint=json.loads(request['source_checkpoint_json']);port=self.scope_ports[checkpoint['feed']]
        connection=connect(port['reader'])
        try:
            tx=PostgresOutbox(Session(connection),feed=port['scope'].feed,epoch=port['scope'].epoch,schema=port['source']).read(checkpoint['previous'],limit=1)
            if len(tx)!=1 or outbox_checkpoint(tx[0])!=request['source_checkpoint_json'] or batch_row(tx[0].batch)['batch_json']!=request['source_batch_json']:raise PermissionError('Native original outbox group differs')
            connection.rollback()
        finally:connection.close()
    def _apply(self,request,context,recovery):
        self.require(context);self.source_admit(request);active=self.policy.active
        if active['request']!=request:raise PermissionError('Complete original applying request differs')
        effects=LocalDeltaEffects(self.transport)
        source_plan={'request':request,'generated_steps':active['generated_steps'],'selected_steps':active['steps'],'zero_match_elisions':active['elisions'],'complete_prior_oracle':active['previous_expected']}
        retained_plan=self.transport.db.execute('SELECT original FROM local_source_plan WHERE request_digest=?',(request['request_digest'],)).fetchone()
        if retained_plan is None:
            if recovery:raise LocalDeltaError('Original source plan custody absent; no replacement')
            anchors={}
            for role,rows in active['previous_expected'].items():
                target=self.transport.targets[self.tables[role]];version=int(self.transport._history(target)[0]['version']);snapshot=self.transport._snapshot(target,version)
                if snapshot['rows']!=sorted(rows,key=encoded):raise ValueError('Complete original prior native state differs: '+role)
                anchors[target.table]={'uuid':target.uuid,'version':version,'snapshot':snapshot}
            for role in active['previous_expected']:self.transport._detail(self.transport.targets[self.tables[role]])
            source_plan['observed_native_prior']=anchors;original_plan=encoded(source_plan)
            with self.transport.db:self.transport.db.execute('INSERT INTO local_source_plan VALUES(?,?)',(request['request_digest'],original_plan))
        else:
            original_plan=retained_plan[0];retained=json.loads(original_plan)
            if {k:v for k,v in retained.items() if k!='observed_native_prior'}!=source_plan:raise LocalDeltaError('Original exact source plan differs')
            for table,anchor in retained['observed_native_prior'].items():
                target=self.transport.targets[table]
                if anchor['uuid']!=target.uuid or encoded(self.transport._snapshot(target,anchor['version']))!=encoded(anchor['snapshot']):raise LocalDeltaError('Original no-op prior native evidence differs')
        if not self.transport.db.execute('SELECT 1 FROM local_plan WHERE operation=?',('effects:'+request['request_digest'],)).fetchone() and recovery:raise LocalDeltaError('Original whole effect plan absent; no replacement')
        proof=(effects.recover if recovery else effects.run)('effects:'+request['request_digest'],request['request_digest'],active['steps'],context=context)
        row=self.transport.db.execute('SELECT request,artifact FROM local_publication_artifact WHERE request_digest=?',(request['request_digest'],)).fetchone()
        if row:
            if row[0]!=encoded(request):raise PermissionError('Retained artifact original request differs')
            artifact=json.loads(row[1]);active['manifest']=artifact['manifest'];self.validate_manifest(artifact['manifest'],context);return row[1]
        versions={};parity={}
        for role in ('object_current','edge_current','tombstone','whole_source_history'):
            target=self.transport.targets[self.tables[role]];version=int(self.transport._history(target)[0]['version']);snapshot=self.transport._snapshot(target,version)
            if snapshot['rows']!=sorted(active['expected'][role],key=encoded):raise ValueError('Independent complete source oracle differs: '+role)
            versions[target.table]=version;parity[role]=snapshot['row_sha256']
        proof={**proof,'original_generated_steps':active['generated_steps'],'zero_match_elisions':active['elisions'],'original_source_plan_sha256':sha(original_plan),'observed_native_prior':json.loads(original_plan)['observed_native_prior']}
        manifest={'publication_id':active['publication_id'],'profile_version':PROFILE,'table_versions_json':encoded(versions),'schema_revisions_json':request['schema_revisions_json'],'source_progress_json':encoded(active['progress']),
                  'validation_report_json':encoded({'complete':True,'request_digest':request['request_digest'],'predecessor':request['predecessor'],'effect_parity':parity,'original_source_plan_sha256':sha(original_plan),'zero_match_elisions':active['elisions'],'source_oracle_sha256':sha(encoded(active['expected'])),'source_admission':self.admission_facts(),'scope':self.admission_facts()['qualification']}),'recorded_at':'1791547200000000'}
        active['manifest']=manifest;artifact=encoded({'effects':proof,'manifest':manifest})
        with self.transport.db:self.transport.db.execute('INSERT INTO local_publication_artifact VALUES(?,?,?)',(request['request_digest'],encoded(request),artifact))
        self.validate_manifest(manifest,context);return artifact
    def apply(self,request,context):return self._apply(request,context,False)
    def recover_apply(self,request,context):return self._apply(request,context,True)
    def validate_manifest(self,row,context):
        self.require(context);active=self.policy.active
        if row!=active['manifest']:raise PermissionError('Immutable original manifest differs')
        if json.loads(row['validation_report_json']).get('source_admission')!=self.admission_facts():raise PermissionError('Manifest original admitted source profile differs')
        validate_union(active['previous_progress'],active['request'],row)
        self.source_admit(active['request'])
        for role,expected in active['expected'].items():
            target=self.transport.targets[self.tables[role]];version=json.loads(row['table_versions_json'])[target.table]
            if self.transport._snapshot(target,version)['rows']!=sorted(expected,key=encoded):raise ValueError('Original exact publication snapshot differs: '+role)
        for role in active['expected']:self.transport._detail(self.transport.targets[self.tables[role]])
    def validate(self,request,artifact,descriptor,context):
        if json.loads(artifact)['manifest']!=dict(descriptor.raw):raise ValueError('Retained original applied manifest differs')
        self.validate_manifest(dict(descriptor.raw),context)
    @contextmanager
    def hold(self,vector,*,context):
        self.require(context)
        if self.pin_held:raise PermissionError('No nested private pin interval')
        row=self.policy.active['manifest'];expected=manifest_pin_vector(row,{t:self.transport.targets[t].uuid for t in json.loads(row['table_versions_json'])},authority='private-local-process')
        if vector!=expected:raise PermissionError('Original complete held native vector differs')
        self.pin_held=True
        try:
            self.validate_manifest(row,context);yield;self.validate_manifest(row,context)
        finally:self.pin_held=False
    def authorize(self,context,publication_id,tables):
        self.require(context)
        if not self.pin_held or publication_id!=self.policy.active['publication_id'] or set(tables)!=set(json.loads(self.policy.active['manifest']['table_versions_json'])):raise PermissionError('Original complete resolver interval required')
    def descriptors(self,publication_id):
        fields=','.join('cast(unix_micros(recorded_at) AS STRING) AS recorded_at' if k=='recorded_at' else k for k in FIELDS)
        target=self.transport.targets[self.tables['manifest']];self.transport._detail(target)
        rows=self.transport.query('SELECT '+fields+' FROM `'+ '`.`'.join(self.tables['manifest'].split('.'))+'` WHERE publication_id=:id',{'id':publication_id}).rows
        self.transport._detail(target);return rows
    def validate_descriptor(self,descriptor,context):self.validate_manifest(dict(descriptor.raw),context)
    def inspect_snapshot(self,table,version):
        target=self.transport.targets[table];self.transport._snapshot(target,version);return Snapshot(table,target.uuid,version)
    def admit_scope(self,scope,session,context):
        self.require(context);port=self.scope_ports[scope.feed]
        if scope!=port['scope']:raise PermissionError('Original native source scope differs')
        binding=session.query('SELECT "'+scope.service_schema+'".scope_binding(CAST(:scope AS uuid)) AS binding',{'scope':scope.scope_id}).rows[0]['binding']
        if binding['source_schema']!=port['source'] or binding['source_signature_sha256']!=port['signature']:raise PermissionError('Original namespace/source installation changed')
    def admit_publication(self,scope,request,resolved,session,context):
        if request!=self.policy.active['request']:raise PermissionError('Original ACK request differs')
        self.validate_manifest(dict(resolved.descriptor.raw),context)
    def acknowledge(self,request,descriptor,context):
        self.require(context)
        if self.lose_manifest:self.lose_manifest=False;raise LostAfterManifest('Injected loss after real immutable manifest commit before PG ACK')
        port=self.scope_ports[json.loads(request['source_checkpoint_json'])['feed']]
        retained=self.transport.db.execute('SELECT request,artifact FROM local_publication_artifact WHERE request_digest=?',(request['request_digest'],)).fetchone()
        if retained is None or retained[0]!=encoded(request):raise LocalDeltaError('Original applied artifact custody absent; no ACK')
        original_manifest=json.loads(retained[1])['manifest']
        if original_manifest!=dict(descriptor.raw):raise LocalDeltaError('Original retained manifest differs before ACK')
        self.policy.active['manifest']=original_manifest
        row=dict(descriptor.raw);vector=manifest_pin_vector(row,{t:self.transport.targets[t].uuid for t in descriptor.versions},authority='private-local-process')
        def factory(supplied):
            self.require(supplied);connection=connect(port['role'])
            if not self.lose_ack:return connection
            self.lose_ack=False
            class LostCommit:
                def __getattr__(self,name):return getattr(connection,name)
                def commit(self):connection.commit();raise OSError('Injected response loss after actual PostgreSQL COMMIT')
            return LostCommit()
        ack=ProtectedOutboxAck(factory,self,self,self,port['scope'],supported_profiles=[PROFILE],supported_revisions={k:[v] for k,v in json.loads(request['schema_revisions_json']).items()})
        raw_request=encoded(request).encode();raw_manifest=encoded(row).encode()
        try:receipt=ack.acknowledge(raw_request,raw_manifest,vector,context=context)
        except AckOutcomeUncertain:
            receipt=ack.reconcile(raw_request,raw_manifest,vector,context=context)
            if receipt is None:raise ValueError('Actual committed original ACK did not reconcile')
            self.acks.append({'publication_id':row['publication_id'],'uncertain_commit_fresh_reconciled':True})
        else:self.acks.append({'publication_id':row['publication_id'],'uncertain_commit_fresh_reconciled':False})
        if not receipt:raise ValueError('Exact protected native ACK receipt missing')

class AttemptExecutor:
    def __init__(self,driver):self.driver=driver
    def query(self,sql,parameters):
        if sql.lstrip().split(None,1)[0].upper() in ('SELECT','DESCRIBE','SHOW'):return self.driver.transport.query(sql,parameters)
        request=self.driver.policy.active['request']
        return LocalOperationExecutor(self.driver.transport,'attempt:'+request['request_digest'],request['request_digest'],self.driver.context).query(sql,parameters)

def request_for(stream,transaction,predecessor,revisions):
    row=batch_row(transaction.batch)
    request={'stream':stream,'batch_id':transaction.batch.batch_id,'predecessor':predecessor,'schema_revisions_json':encoded(revisions),'source_batch_json':row['batch_json'],'source_batch_digest':row['batch_digest'],'source_checkpoint_json':outbox_checkpoint(transaction)}
    request['request_digest']=hashlib.sha256(json.dumps(request,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    return request

def provision_sources(output,fixtures):
    """Fresh dedicated ordinary-role sources only; retains exact submitted DDL/logs."""
    observed=[]
    label=subprocess.check_output(['docker','inspect',CONTAINER,'--format','{{index .Config.Labels "ashlar.purpose"}}']).decode().strip()
    if label!='end-to-end-development':raise PermissionError('Original labeled private PostgreSQL required')
    def pg(statement):
        process=subprocess.run(['docker','exec','-i',CONTAINER,'psql','-qAt','-U','postgres','-d','truss_e2e','-v','ON_ERROR_STOP=1'],input=statement,text=True,capture_output=True)
        observed.append({'statement':statement,'exit':process.returncode,'stdout':process.stdout,'stderr':process.stderr});(output/'postgres-installation.json').write_text(encoded(observed)+'\n')
        if process.returncode:raise RuntimeError('Fresh native installation failed; preserve original namespaces: '+process.stderr)
    suffix=uuid.uuid4().hex[:12];service='ashlar_ack_pipeline_'+suffix;role='ashlar_ack_operator_'+suffix
    pg(render_ddl(service,role));ports={}
    for selected in fixtures:
        source='ashlar_ack_source_'+suffix+'_'+selected['label'];writer=source+'_writer';reader=source+'_reader'
        ddl=(ROOT/'sql/ashlar-outbox/01-postgresql.sql').read_text().replace('ashlar_outbox_writer',writer).replace('ashlar_outbox_reader',reader).replace('ashlar_outbox',source)
        pg(ddl);scope=AckScope(service,str(uuid.uuid4()),str(uuid.uuid4()),'private-local-pipeline',selected['feed'],selected['epoch'])
        pg(f"SELECT {service}.register_source('{scope.installation_id}','{source}'); SELECT {service}.register_consumer('{scope.scope_id}','{scope.installation_id}','{scope.consumer}','{scope.feed}','{scope.epoch}');")
        connection=connect(role)
        try:
            binding=Session(connection).query('SELECT "'+service+'".scope_binding(CAST(:scope AS uuid)) AS binding',{'scope':scope.scope_id}).rows[0]['binding'];connection.rollback()
        finally:connection.close()
        ports[selected['feed']]={'source':source,'writer':writer,'reader':reader,'scope':scope,'role':role,'signature':binding['source_signature_sha256']}
    return ports

def run(output,jars,umf_source):
    output=Path(output)
    if output.exists():raise ValueError('Exclusive fresh private output required; never overwrite original recovery custody')
    if {k:importlib.metadata.version(k) for k in VERSIONS}!=VERSIONS:raise ValueError('Existing qualified runtime versions required')
    jar_paths=[Path(jars)/name for name in JARS]
    if not all(path.is_file() for path in jar_paths):raise ValueError('Explicit existing cached native jars required')
    output.mkdir(parents=True,mode=0o700);fixtures=fixture();intake,_,_=fixture_inputs()
    originals=[];records=[];allowed_changes=[]
    for selected in fixtures:
        for raw,batch in selected['batches']:
            path=output/(batch.batch_id+'.source.jsonl');path.write_bytes(raw);originals.append({'path':str(path),'sha256':hashlib.sha256(raw).hexdigest(),'feed':batch.feed,'epoch':batch.epoch})
            allowed_changes.extend(changes_from_batch(batch))
            for record in batch.records:
                event=json.loads(record.raw)
                if event['entity_kind']=='object' and event['operation']!='delete':records.append({'deliveryId':record.delivery_id,'recordSha256':record.sha256,'values':fixture_value_entries(json.loads(event['props_json']))})
    record_receipt=check_value_request(umf_source,intake,records,output/'umf-record-check.json')
    admission=FixedStringAdmission(tuple(allowed_changes),record_receipt,receipt_bytes=(output/'umf-record-check.json').read_bytes())
    from pyspark.sql import SparkSession
    spark=(SparkSession.builder.master('local[1]').appName('Ashlar small private publication ACK').config('spark.driver.memory','512m').config('spark.sql.shuffle.partitions','1').config('spark.databricks.delta.snapshotPartitions','1').config('spark.ui.enabled','false').config('spark.sql.session.timeZone','UTC').config('spark.sql.ansi.enabled','true').config('spark.jars',','.join(str(p) for p in jar_paths)).config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension').config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog').getOrCreate())
    transport=None
    try:
        columns=fixture_columns(ROOT);columns['attempts']=tuple((name,'STRING') for name in ('stream','batch_id','phase','request_digest','payload_json','payload_digest'));columns['manifest']=tuple((name,'TIMESTAMP' if name=='recorded_at' else 'STRING') for name in FIELDS)
        tables={role:'local.pipeline.'+role for role in columns};targets=[]
        for role,schema in columns.items():
            path=output/role;definition=','.join(name+' '+typ for name,typ in schema)
            spark.sql('CREATE TABLE delta.`'+str(path)+'` ('+definition+') USING DELTA')
            targets.append(DeltaTarget(tables[role],path,spark.sql('DESCRIBE DETAIL delta.`'+str(path)+'`').first().id))
        context=object();policy=PrivatePolicy(context,tuple(targets));transport=LocalDeltaTransport.initialize(spark,output/'operations.sqlite','private-publication-pipeline',tuple(targets),policy,context=context);policy.initializing=False
        ports=provision_sources(output,fixtures)
        for selected in fixtures:
            port=ports[selected['feed']];connection=connect(port['writer'])
            try:
                for ordinal,(raw,batch) in enumerate(selected['batches'],1):
                    position=connection.execute('SELECT "'+port['source']+'".append(%s,%s)',(batch.batch_id,raw.decode())).fetchone()[0]
                    if position!=ordinal:raise ValueError('Fresh native source did not start at original position0')
                    connection.commit()
            finally:connection.close()
        driver=NativeDriver(transport,policy,context,tables,ports,tuple(allowed_changes),{k:columns[k] for k in ('object_current','edge_current','tombstone','whole_source_history')},source_admission=admission)
        def backend():
            target=driver.transport.targets[tables['attempts']]
            return StoredPublisherBackend(DeltaAttemptStore(AttemptExecutor(driver),CarrierPolicy(driver,'attempts'),target.table,target.uuid),driver,lambda request,supplied:ManifestPort(driver,request))
        order=[(fixtures[0],0),(fixtures[1],0),(fixtures[0],1),(fixtures[1],1)];state=empty_state();processed=[];progress={};revisions={};predecessor='private-explicit-origin';publications=[];first_rows=None;first_versions=None
        graph_tables={k:tables[k] for k in ('object_current','edge_current','tombstone','whole_source_history')}
        for ordinal,(selected,index) in enumerate(order,1):
            port=ports[selected['feed']];connection=connect(port['reader'])
            try:
                transaction=PostgresOutbox(Session(connection),feed=selected['feed'],epoch=selected['epoch'],schema=port['source']).read(str(index),limit=1)[0];connection.rollback()
            finally:connection.close()
            expected_raw,batch=selected['batches'][index]
            if transaction.batch!=batch:raise ValueError('Independent original PostgreSQL transaction bytes differ')
            # Complete prior state and oracle are rebuilt from original admitted sources,
            # never from generated SQL outputs. Closing native parity verifies all roles.
            previous_expected,_=fixture_inventory(processed,driver.columns,materialized_at=CLOCK)
            state,steps=graph_sql_plan(state,batch,graph_tables,materialized_at=CLOCK,schema_policy=driver.schema_admit)
            generated_steps=steps;steps,elisions=local_effect_plan(generated_steps,previous_expected,graph_tables)
            processed.append(batch);expected,_=fixture_inventory(processed,driver.columns,materialized_at=CLOCK)
            revisions={**revisions,selected['source']:'3'};request=request_for('private-global-pipeline',transaction,predecessor,revisions);next_progress=progress_union(progress,json.loads(outbox_checkpoint(transaction)))
            active={'request':request,'steps':steps,'expected':expected,'publication_id':'local-publication-'+str(ordinal),'previous_progress':progress,'progress':next_progress,'previous_expected':previous_expected,'generated_steps':generated_steps,'elisions':elisions};policy.active=active
            if ordinal==1:driver.lose_manifest=True
            if ordinal==2:driver.lose_ack=True
            try:descriptor=publish_outbox_transaction(backend(),'private-global-pipeline',transaction,predecessor=predecessor,schema_revisions_json=encoded(revisions),context=context)
            except LostAfterManifest:
                if ordinal!=1:raise
                # New journal/transport/backend objects inspect original actual commits.
                transport.close();context=object();policy=PrivatePolicy(context,tuple(targets));policy.initializing=False;policy.active=json.loads(encoded(active));policy.active.pop('manifest',None)
                transport=LocalDeltaTransport(spark,output/'operations.sqlite','private-publication-pipeline',tuple(targets),policy)
                driver=NativeDriver(transport,policy,context,tables,ports,tuple(allowed_changes),{k:columns[k] for k in ('object_current','edge_current','tombstone','whole_source_history')},source_admission=admission)
                descriptor=publish_outbox_transaction(backend(),'private-global-pipeline',transaction,predecessor=predecessor,schema_revisions_json=encoded(revisions),context=context)
            publications.append(dict(descriptor.raw));progress=next_progress;predecessor=descriptor.publication_id
            if ordinal==1:first_rows=expected;first_versions=dict(descriptor.versions)
            if ordinal==1:
                history_before={t:driver.transport._history(driver.transport.targets[t]) for t in descriptor.versions}
                repeated=publish_outbox_transaction(backend(),'private-global-pipeline',transaction,predecessor=request['predecessor'],schema_revisions_json=request['schema_revisions_json'],context=context)
                if repeated!=descriptor or any(driver.transport._history(driver.transport.targets[t])!=history_before[t] for t in descriptor.versions):raise ValueError('Committed exact replay changed graph snapshot')
        # Real historical versions remain complete after later source changes.
        for role,rows in first_rows.items():
            target=transport.targets[tables[role]]
            if transport._snapshot(target,first_versions[target.table])['rows']!=sorted(rows,key=encoded):raise ValueError('Old retained publication snapshot changed')
        acknowledgements=[]
        for port in ports.values():
            connection=connect(port['role'])
            try:
                rows=Session(connection).query('SELECT * FROM "'+port['scope'].service_schema+'".observe(CAST(:scope AS uuid),CAST(:position AS bigint))',{'scope':port['scope'].scope_id,'position':'2'}).rows
                if len(rows)!=1 or rows[0]['position']!='2' or rows[0]['receipt_hex'] is None:raise ValueError('Durable original source ACK progress differs')
                acknowledgements.append({'scope':asdict(port['scope']),'source_schema':port['source'],'source_signature_sha256':port['signature'],'position':rows[0]['position'],'receipt_sha256':hashlib.sha256(bytes.fromhex(rows[0]['receipt_hex'])).hexdigest()});connection.rollback()
            finally:connection.close()
        report={'format':'ashlar-local-outbox-publication/0.1','versions':VERSIONS,'original_sources':originals,'table_registry':[{'table':t.table,'path':str(t.path),'uuid':t.uuid} for t in targets],
                'publications':publications,'acknowledgements':acknowledgements,'ack_events':driver.acks,'manifest_before_ack_fresh_restart':True,'two_source_progress':progress,'complete_final_oracle':expected,'old_snapshot_unchanged':True,'scope':__doc__}
        (output/'report.json').write_text(encoded(report)+'\n');return report
    finally:
        if transport is not None:transport.close()
        spark.stop()

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True);parser.add_argument('--jars',type=Path,required=True);parser.add_argument('--umf-source',type=Path,required=True);args=parser.parse_args();print(encoded(run(args.output,args.jars,args.umf_source)))
