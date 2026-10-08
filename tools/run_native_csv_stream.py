"""Small actual CSV ingestion -> durable native phases -> publication -> local progress.

Fixed development source/mapping only, on existing compute in a private namespace.
Trusted administrators and same-host cooperating writers; no remote Truss ACK.
"""
import argparse
from contextlib import contextmanager
import datetime
import fcntl
import hashlib
import json
from pathlib import Path
import sys
import time
ROOT=Path(__file__).resolve().parents[1]
B=ROOT/'docs/helix/02-design/spikes/SPIKE-001-table-layout'
sys.path.insert(0,str(ROOT/'src'));sys.path.insert(0,str(B))
from ashlar.apply import empty_state
from ashlar.authority import validate_writer_inventory
from ashlar.attempt_store import DeltaAttemptStore
from ashlar.csv_source import csv_batches,validate_csv_batch
from ashlar.effect_validation import validate_effect_snapshot
from ashlar.manifest import manifest_pin_vector,bind_manifest_pins
from ashlar.native import NativeBackend,_quoted
from ashlar.pins import PostgresPins
from ashlar.protocol import ReaderProtocolProfile,inspect_protocol
from ashlar.publication import Descriptor,_decode,_freeze,resolve_publication
from ashlar.publisher import publish_batch,PublicationError
from ashlar.retention import publication_retention_report
from ashlar.retention_policy import SQLRetentionProvider,RetentionGate
from ashlar.source_checkpoint import csv_checkpoint
from ashlar.singleton import read_singleton
from ashlar.staging import batch_row
from ashlar.stored_publisher import StoredPublisherBackend
from databricks_transport import DatabricksTransport,sql_result
from durable_effects import DurableEffects
from durable_sql import DurableSQL,SQLPending
from effective_grants import effective_grants
from fixture_oracle import fixture_batches,fixture_columns,fixture_inventory
from journaled_attempts import JournaledAttemptExecutor
from journaled_csv_progress import JournaledCsvProgress
from journaled_manifest import JournaledManifestStore
from journaled_publisher_driver import JournaledPublisherDriver
from journaled_snapshot_artifacts import JournaledSnapshotArtifacts
from materialization_clock import materialization_clock
from native_artifact_validation import NativeArtifactValidator
from pinned_artifact_validation import PinnedArtifactValidation
from persistent_sql import Client
from run_local_example import fixture_inputs,check_original_records
from sandbox_pins import PrivatePinTransactions
from whole_graph_sql import graph_sql_plan

NAMESPACE='ashlar_e2e_private_20261008.runtime_csv_stream'
STREAM='native-csv-stream:'+NAMESPACE
PROFILE=ReaderProtocolProfile('private-databricks-sql-fixture/2026-10-08',(3,),(7,),
    {'appendOnly','clustering','deletionVectors','domainMetadata','invariants','rowTracking','v2Checkpoint'})


def encoded(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False)
def descriptor(row):
    return Descriptor(row['publication_id'],row['profile_version'],_freeze(_decode(row['table_versions_json'])),
        _freeze(_decode(row['schema_revisions_json'])),_freeze(_decode(row['source_progress_json'])),_freeze(_decode(row['validation_report_json'])),_freeze(row))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('installation','intake-proof','journal','output','umf-source'):parser.add_argument('--'+name,type=Path,required=True)
    parser.add_argument('--limit',type=int,choices=range(1,5),default=1,help='Process through this original CSV ordinal; default one row')
    parser.add_argument('--query-only',action='store_true',help='Read the last retained publication; no ingestion, publication or progress advancement')
    parser.add_argument('--entity-id',type=int,default=1,help='Signed 64-bit local-example object identity for query-only mode')
    args=parser.parse_args()
    if args.query_only and not args.journal.is_file():parser.error('Query-only requires the original retained publication journal')
    if not -(2**63)<=args.entity_id<2**63:parser.error('Entity identity must fit signed 64 bits')
    if args.output.exists():parser.error('Fresh output directory required; preserve previous receipts')
    installation=json.loads(args.installation.read_bytes());intake_proof=json.loads(args.intake_proof.read_bytes())
    if installation['namespace']!=NAMESPACE:raise PublicationError('Only the fresh private CSV streaming namespace is admitted')
    intake,semantic_policy,_=fixture_inputs()
    if (intake.source_sha256,intake.artifact_sha256,intake.document_revision,intake.validator_revision)!=(
        intake_proof['source_sha256'],intake_proof['artifact_sha256'],intake_proof['revision'],intake_proof['validator_revision']):
        raise PublicationError('Exact original native UMF intake proof required')
    source,oracle_batches=fixture_batches(ROOT,'csv');source_sha=hashlib.sha256(source.read_bytes()).hexdigest()
    batches=tuple(csv_batches(source.read_bytes().splitlines(keepends=True),feed='csv-example',epoch='immutable-example-1',
        source_system='local-example',schema_revision='3',type_id='17',properties={'label':'23','caption':'24'}))
    if tuple(oracle_batches)!=batches or len(batches)!=4:raise PublicationError('Independent original CSV custody differs')
    args.output.mkdir(parents=True)
    umf=check_original_records(args.umf_source,intake,batches,args.output/'umf-record-check.json')
    client=Client(args.output,profile='aidev-cus',warehouse_id='2439e1f2e37ac563');user=client.w.current_user.me()
    if user.user_name!=installation['authenticated_owner']:raise PermissionError('Original private installation owner differs')
    journal=DurableSQL(str(args.journal),client.w.api_client,client.warehouse_id,user.id)
    transport=DatabricksTransport(client,journal)
    tables={role:NAMESPACE+'.'+role for role in ('object_current','edge_current','tombstone','whole_source_history')}
    uuids={name:entry['uuid'] for name,entry in installation['tables'].items()}
    manifest_table=NAMESPACE+'.publication_manifest';phase_table=NAMESPACE+'.publication_attempt_phase'
    context=object();held=False;active=None;current=None;pin_vector=None;pin_held=False;targets=None;read_interval=None
    permission_reads=[]
    def custody():
        if not held or hashlib.sha256(source.read_bytes()).hexdigest()!=source_sha:raise PermissionError('Original admitted CSV writer/source lane required')
        fresh=client.w.current_user.me()
        if (fresh.id,fresh.user_name)!=(user.id,user.user_name):raise PermissionError('Current authenticated actor differs')
    def authority():
        custody();catalog,schema=NAMESPACE.split('.')
        resources=[('catalog',catalog,client.w.catalogs.get(name=catalog).owner),('schema',NAMESPACE,client.w.schemas.get(full_name=NAMESPACE).owner)]
        for name in [*uuids,intake_proof['table']]:resources.append(('table',name,client.w.tables.get(full_name=name).owner))
        for kind,name,owner in resources:
            def retain(page,ordinal):
                observation={'kind':kind,'name':name,'ordinal':ordinal,'page':page}
                with (args.output/'effective-permissions.jsonl').open('a') as file:file.write(encoded(observation)+'\n')
                permission_reads.append(observation)
            grants=effective_grants(client.w.api_client,kind,name,record_page=retain)
            validate_writer_inventory(owner,grants['rows'],trusted_writers=[user.user_name])
    def original_request(request):
        custody()
        if active is None or dict(request)!=active['request']:raise PublicationError('Exact admitted original publication request required')
    class LanePolicy:
        @contextmanager
        def writer(self,*values):
            supplied=values[-1]
            if supplied is not context:raise PermissionError('Original held native context required')
            custody();yield;custody()
    lane_policy=LanePolicy()
    class EffectPolicy(LanePolicy):
        def admit(self,plan,supplied):
            if supplied is not context or active is None or plan['intent_digest']!=active['request']['request_digest'] or plan['steps']!=active['steps']:
                raise PublicationError('Original complete admitted effect plan required')
            custody()
            for role,table in tables.items():
                inspect_protocol(transport,table,uuids[table],profile=PROFILE)
                shape=transport.query('SELECT * FROM '+_quoted(table)+' LIMIT 0',{})
                if shape.rows or tuple(shape.columns)!=columns[role]:raise PublicationError('Original effect target schema differs before effects')
    def original_read(operation,sql,parameters):
        deadline=time.monotonic()+180
        while True:
            try:return sql_result(journal.query(operation,sql,parameters))
            except SQLPending:
                if time.monotonic()>=deadline:raise
                time.sleep(.2)
    class PinPolicy:
        def authorize_read(self,value,supplied):
            if supplied is not context or value!=pin_vector:raise PermissionError('Original complete pin vector required')
            custody()
        def admit_registration(self,value,supplied):
            self.authorize_read(value,supplied)
            for table,target in targets.items():
                inspect_protocol(transport,table,target['uuid'],profile=PROFILE)
                validate_effect_snapshot(transport,table,target['uuid'],target['version'],target['columns'],target['rows'])
            gate.check(current,supplied)
    pins_reader=PostgresPins(PrivatePinTransactions(context,'ashlar_pin_reader'),PinPolicy())
    pins_writer=PostgresPins(PrivatePinTransactions(context,'ashlar_pin_writer'),PinPolicy())
    provider=SQLRetentionProvider(transport,{t:uuids[t] for t in tables.values()},
        defaults={'data_retention':'interval 7 days','log_retention':'interval 30 days'},
        default_profile='azure-databricks-delta-documented-defaults/2026-09-11',max_observation_span_us=180_000_000)
    gate=RetentionGate(provider,minimum_margin_us=600_000_000)
    def source_schema():
        table=intake_proof['table'];native=transport.query('DESCRIBE DETAIL '+_quoted(table),{}).rows
        if len(native)!=1 or native[0]['id']!=intake_proof['table_uuid']:raise PublicationError('Original native UMF registry identity differs')
        row=intake.row();projection=','.join('cast(complete_interpretation AS STRING) AS complete_interpretation' if k=='complete_interpretation' else k for k in row)
        native=transport.query('SELECT '+projection+' FROM '+_quoted(table)+' WHERE document_id=:id AND document_revision=:revision',{'id':intake.document_id,'revision':'3'}).rows
        if native!=[dict(row,complete_interpretation=str(row['complete_interpretation']).lower())]:raise PublicationError('Original raw native UMF intake differs')
    class CapturePolicy:
        def admit(self,request,effects,snapshots,supplied):
            if supplied is not context:raise PermissionError('Original capture context required')
            original_request(request)
            if set(snapshots)!=set(tables.values()):raise PublicationError('Complete native snapshot vector required')
    def snapshot_targets(request,effects,supplied):
        original_request(request);observed={};anchors={}
        prefix='stream-snapshot:'+request['request_digest']+':'
        for table in tables.values():
            detail=original_read(prefix+table+':detail','DESCRIBE DETAIL '+_quoted(table),{}).rows
            if len(detail)!=1 or detail[0]['id']!=uuids[table]:raise PublicationError('Original effect target UUID differs')
            history=original_read(prefix+table+':history','DESCRIBE HISTORY '+_quoted(table)+' LIMIT 1',{}).rows
            if len(history)!=1:raise PublicationError('Original native commit observation required')
            token=history[0]['version']
            if not isinstance(token,str) or not token.isdecimal() or str(int(token))!=token:raise PublicationError('Canonical original native version required')
            instant=datetime.datetime.fromisoformat(history[0]['timestamp'].replace('Z','+00:00'))
            if instant.tzinfo is None:raise PublicationError('Native commit clock must be timezone-qualified')
            elapsed=instant-datetime.datetime(1970,1,1,tzinfo=datetime.timezone.utc)
            committed=str((elapsed.days*86400+elapsed.seconds)*1000000+elapsed.microseconds)
            role=table.rsplit('.',1)[1];version=int(token)
            observed[table]={'uuid':uuids[table],'version':version,'columns':columns[role],'rows':active['expected'][role]}
            anchors[table]={'uuid':uuids[table],'version':version,'committed_at':committed}
        with journal.db:
            journal.db.execute('CREATE TABLE IF NOT EXISTS stream_snapshot_anchor (request_digest TEXT PRIMARY KEY,original_json TEXT NOT NULL)')
            journal.db.execute('INSERT OR IGNORE INTO stream_snapshot_anchor VALUES (?,?)',(request['request_digest'],encoded(anchors)))
            if journal.db.execute('SELECT original_json FROM stream_snapshot_anchor WHERE request_digest=?',(request['request_digest'],)).fetchone()[0]!=encoded(anchors):raise PublicationError('Original snapshot commit anchors changed')
        return observed
    def manifest_row(request,effects,witnesses,supplied):
        anchors=json.loads(journal.db.execute('SELECT original_json FROM stream_snapshot_anchor WHERE request_digest=?',(request['request_digest'],)).fetchone()[0])
        versions={table:anchor['version'] for table,anchor in anchors.items()}
        observed=provider.observe(Descriptor(active['publication_id'],'ashlar-delta/0.3',versions,{'fixture':'3'},{},{},{}),context)
        report=publication_retention_report(anchors,observed['configurations'],margin_us=600_000_000)
        checkpoint=json.loads(request['source_checkpoint_json'])
        return {'publication_id':active['publication_id'],'profile_version':'ashlar-delta/0.3',
            'table_versions_json':encoded(versions),'schema_revisions_json':request['schema_revisions_json'],
            'source_progress_json':encoded({checkpoint['feed']:checkpoint}),'recorded_at':observed['now_us'],
            'validation_report_json':encoded({'complete':True,'request_digest':request['request_digest'],
                'source_sha256':source_sha,'source_groups':active['ordinal'],'intake_source_sha256':intake.source_sha256,
                'intake_artifact_sha256':intake.artifact_sha256,'upstream_record_check_revision':umf['producerRevision'],
                'effect_parity':witnesses,'retention':report,'qualification':'Private CSV fixture native publication; explicit local IDs and source custody, actual UMF logical checks. No accepted Truss IDs, remote source ACK or production remote fencing.'})}
    class SourcePolicy:
        def admit(self,value,snapshots,supplied):
            if supplied is not context or active is None or value.publication_id!=active['publication_id']:raise PermissionError('Original complete publication scope required')
            if value.validation_report.get('request_digest')!=active['request']['request_digest'] or value.validation_report.get('source_sha256')!=source_sha:
                raise PublicationError('Original publication source/request binding differs')
            for table,target in snapshots.items():
                role=table.rsplit('.',1)[1]
                if target['uuid']!=uuids[table] or target['columns']!=[list(c) for c in columns[role]] or target['rows']!=active['expected'][role]:raise PublicationError('Independent complete snapshot expectations differ')
            authority();source_schema()
    def pin_admission(value,supplied):
        nonlocal current,pin_vector,targets
        if supplied is not context:raise PermissionError('Original native pin context required')
        current=value;pin_vector=manifest_pin_vector(dict(value.raw),{t:uuids[t] for t in tables.values()},authority=user.user_name)
        targets=loaded_targets(active['request'])
        pins_writer.register(pin_vector,context=context)
    def loaded_targets(request):
        operation='stream-artifact:'+request['request_digest']
        raw=journal.db.execute('SELECT targets_json FROM snapshot_artifact WHERE operation=?',(operation,)).fetchone()
        if raw is None:raise PublicationError('Original native artifact target custody missing')
        return json.loads(raw[0])
    def validator(request,text,value,supplied):
        original_request(request)
        service=NativeArtifactValidator(transport,loaded_targets(request),SourcePolicy(),gate,pin_admission,
            reader_profile=PROFILE,manifest_table=manifest_table,manifest_uuid=uuids[manifest_table])
        service(request,text,value,supplied)
    class ReadPolicy:
        def authorize(self,supplied,publication_id,requested):
            if supplied is not context or active is None or publication_id!=active['publication_id'] or set(requested)!=set(tables.values()):raise PermissionError('Original complete native resolver request required')
            custody()
        def validate_descriptor(self,value,supplied):
            nonlocal current,pin_vector,targets
            current=value;pin_vector=manifest_pin_vector(dict(value.raw),{t:uuids[t] for t in tables.values()},authority=user.user_name)
            targets=loaded_targets(active['request'])
            if read_interval is not None:
                if targets!=json.loads(read_interval.inventory):raise PublicationError('Original retained snapshot expectations changed during read')
                return read_interval.validate_descriptor(value,supplied)
            def held_pins(value,supplied):
                if not pin_held:raise PermissionError('Actual original pin guards required')
                bind_manifest_pins(value,pin_vector,{t:uuids[t] for t in tables.values()},authority=user.user_name)
            NativeArtifactValidator(transport,targets,SourcePolicy(),gate,held_pins,
                reader_profile=PROFILE,manifest_table=manifest_table,manifest_uuid=uuids[manifest_table]).validate_descriptor(value,supplied)
        def validate_snapshot(self,table,uuid,version,actual_columns):
            target=targets[table]
            if (uuid,version,tuple(actual_columns))!=(target['uuid'],target['version'],tuple(tuple(c) for c in target['columns'])):raise PublicationError('Original resolver snapshot differs')
            inspect_protocol(transport,table,uuid,profile=PROFILE)
        def bind_descriptor(self,value,vector,supplied):
            if supplied is not context or not pin_held:raise PermissionError('Actual complete query pin interval required')
            bind_manifest_pins(value,vector,{t:uuids[t] for t in tables.values()},authority=user.user_name)
            custody()
        def authorize_row(self,value,table,row,supplied):
            self.bind_descriptor(value,pin_vector,supplied)
            if table!=tables['object_current']:raise PermissionError('Only the admitted fixture object carrier is queryable')
            expected=[item for item in active['expected']['object_current'] if item['id']==str(args.entity_id)]
            if len(expected)>1 or (row is None)!=(not expected):raise PublicationError('Independent singleton presence differs')
            if row is not None:
                # The full pinned inventory checks both timestamps and all other
                # fields. Preserve the query's original native timestamp text.
                names={name for name,kind in columns['object_current']}
                if set(row)!=names or any(row[name]!=expected[0][name] for name,kind in columns['object_current'] if kind!='TIMESTAMP'):
                    raise PublicationError('Independent singleton carrier differs')
    class QueryPins:
        @contextmanager
        def hold(self,vector,*,context):
            nonlocal pin_held,read_interval
            def held_pins(value,supplied):
                if not pin_held:raise PermissionError('Actual original pin guards required')
                bind_manifest_pins(value,vector,{t:uuids[t] for t in tables.values()},authority=user.user_name)
            service=NativeArtifactValidator(transport,targets,SourcePolicy(),gate,held_pins,
                reader_profile=PROFILE,manifest_table=manifest_table,manifest_uuid=uuids[manifest_table])
            interval=PinnedArtifactValidation(pins_reader,vector,service,context)
            with interval.hold(vector,context=context):
                pin_held=True
                read_interval=interval
                try:yield
                finally:pin_held=False;read_interval=None
    @contextmanager
    def native_resolution(value,supplied):
        nonlocal current,pin_vector,targets,pin_held
        current=value;pin_vector=manifest_pin_vector(dict(value.raw),{t:uuids[t] for t in tables.values()},authority=user.user_name)
        targets=loaded_targets(active['request'])
        with QueryPins().hold(pin_vector,context=context):
            pin_held=True
            try:
                backend=NativeBackend(transport,ReadPolicy(),manifest_table,uuids[manifest_table],{table:columns[table.rsplit('.',1)[1]] for table in tables.values()})
                resolved=resolve_publication(backend,value.publication_id,{t:uuids[t] for t in tables.values()},context=context,
                    supported_profiles=['ashlar-delta/0.3'],supported_revisions={'fixture':['3']})
                if resolved.descriptor!=value:raise PublicationError('Original committed native manifest differs')
                yield resolved.descriptor
                ReadPolicy().validate_descriptor(value,supplied)
            finally:pin_held=False
    class ProgressPolicy:
        def admit(self,request,value,supplied):
            if supplied is not context or not pin_held and current is not None and value!=current:raise PermissionError('Original local progress scope required')
            original_request(request)
    class ManifestPolicy(LanePolicy):
        def admit(self,row,supplied):
            from ashlar.stored_publisher import _artifact
            operation='publisher-effects:'+NAMESPACE+':'+active['request']['request_digest']
            text=journal.db.execute('SELECT artifact_text FROM publisher_effect_artifact WHERE operation=?',(operation,)).fetchone()[0]
            _,value=_artifact(text,active['request'])
            if dict(value.raw)!=row:raise PublicationError('Original applied manifest proposal required')
            validator(active['request'],text,value,supplied)
    try:
        with open(str(args.journal)+'.native-csv-stream-lock','a') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX);held=True;authority();source_schema()
            workload=hashlib.sha256(encoded({'namespace':NAMESPACE,'source_sha256':source_sha,'uuids':uuids,'intake_source_sha256':intake.source_sha256,'intake_table_uuid':intake_proof['table_uuid']}).encode()).hexdigest()
            with journal.db:
                journal.db.execute('CREATE TABLE IF NOT EXISTS stream_scope (id INTEGER PRIMARY KEY,workload TEXT NOT NULL)')
                created=journal.db.execute('INSERT OR IGNORE INTO stream_scope VALUES (1,?)',(workload,)).rowcount==1
                if journal.db.execute('SELECT workload FROM stream_scope WHERE id=1').fetchone()!=(workload,):raise PublicationError('Original native streaming scope differs')
            # Every empty-state observation has original query/handle custody.
            for table in [*tables.values(),manifest_table,phase_table]:
                rows=original_read('stream-bootstrap:'+workload+':'+table,'SELECT count(*) AS n FROM '+_quoted(table),{}).rows
                if rows!=[{'n':'0'}]:raise PublicationError('Original private bootstrap is not empty; no overwrite/replacement permitted')
            clock=materialization_clock(journal,workload);columns=fixture_columns(ROOT);state=empty_state();predecessor='empty-native-csv-stream';specs=[]
            for ordinal,batch in enumerate(batches,1):
                row=batch_row(batch);request={'stream':STREAM,'batch_id':batch.batch_id,'predecessor':predecessor,'schema_revisions_json':'{"fixture":"3"}',
                    'source_batch_json':row['batch_json'],'source_batch_digest':row['batch_digest'],'source_checkpoint_json':csv_checkpoint(batch)}
                request['request_digest']=hashlib.sha256(encoded(request).encode()).hexdigest()
                publication_id='csv-stream-'+request['request_digest']
                state,steps=graph_sql_plan(state,batch,tables,materialized_at=clock,schema_policy=semantic_policy)
                expected,_=fixture_inventory(oracle_batches[:ordinal],columns,materialized_at=clock)
                specs.append({'ordinal':ordinal,'batch':batch,'request':request,'publication_id':publication_id,'steps':steps,'expected':expected})
                predecessor=publication_id
            artifacts=JournaledSnapshotArtifacts(journal,transport,CapturePolicy(),snapshot_targets,manifest_row,namespace='stream-artifact')
            progress=JournaledCsvProgress(journal,source,source_sha,ProgressPolicy(),native_resolution,stream=STREAM,feed='csv-example',epoch='immutable-example-1',
                source_system='local-example',schema_revision='3',type_id='17',properties={'label':'23','caption':'24'})
            start=int(progress.position());results=[]
            singleton=None
            if args.query_only:
                if start==0 or start!=args.limit:raise PublicationError('Query-only requires the exact last retained CSV ordinal')
                active=specs[start-1]
                original=journal.db.execute('SELECT original_json FROM csv_consumer_progress WHERE stream=? AND feed=? AND epoch=? AND position=?',
                    (STREAM,'csv-example','immutable-example-1',start)).fetchone()
                current=descriptor(json.loads(original[0])['descriptor'])
                pin_vector=manifest_pin_vector(dict(current.raw),{t:uuids[t] for t in tables.values()},authority=user.user_name)
                targets=loaded_targets(active['request']);policy=ReadPolicy()
                backend=NativeBackend(transport,policy,manifest_table,uuids[manifest_table],{table:columns[table.rsplit('.',1)[1]] for table in tables.values()})
                row=read_singleton(transport,backend,QueryPins(),pin_vector,policy,publication_id=current.publication_id,
                    table=tables['object_current'],kind='object',source='local-example',type_id=17,entity_id=args.entity_id,context=context,
                    supported_profiles=['ashlar-delta/0.3'],supported_revisions={'fixture':['3']})
                singleton=dict(row) if row is not None else None
            for active in ([] if args.query_only else specs[start:args.limit]):
                driver=JournaledPublisherDriver(DurableEffects(journal,EffectPolicy()),lane_policy,lambda request,supplied:active['steps'],
                    artifacts,validator,progress.acknowledge,namespace=NAMESPACE)
                attempts=DeltaAttemptStore(JournaledAttemptExecutor(transport,namespace=NAMESPACE),lane_policy,phase_table,uuids[phase_table])
                backend=StoredPublisherBackend(attempts,driver,lambda request,supplied:JournaledManifestStore(transport,ManifestPolicy(),manifest_table,uuids[manifest_table],operation='stream-manifest:'+request['request_digest']))
                deadline=time.monotonic()+180
                while True:
                    try:
                        value=publish_batch(backend,STREAM,active['batch'],predecessor=active['request']['predecessor'],schema_revisions_json='{"fixture":"3"}',
                            source_checkpoint_json=active['request']['source_checkpoint_json'],context=context)
                        break
                    except SQLPending:
                        if time.monotonic()>=deadline:raise
                        time.sleep(.2)
                results.append({'ordinal':active['ordinal'],'descriptor':dict(value.raw)})
            authority();source_schema()
            summary={'state':'queried' if args.query_only else 'published','namespace':NAMESPACE,'local_consumer_position':progress.position(),'published':results,
                'query_only':args.query_only,'singleton':singleton,'query_publication_id':current.publication_id if args.query_only else None,
                'native_read_statements':len(client.records),'effective_permission_pages':len(permission_reads),'source_sha256':source_sha,'materialized_at':clock,
                'qualification':'Actual CSV fixture ingestion through immutable native attempt phases, journaled original effects/artifact/manifest and resolver-bound durable local consumer progress. Actual UMF logical checks, raw native intake, finite retention and PG pins. Fixture IDs; trusted admins and same-host cooperating writer/source lane. No real Truss producer/catalog, remote writer fence or remote source ACK. Predictive optimization unchanged; no scale workload.'}
            (args.output/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
            print('Native CSV singleton query completed.' if args.query_only else 'Native CSV stream reached local consumer position '+progress.position()+'.')
    finally:held=False;journal.close()

if __name__=='__main__':main()
