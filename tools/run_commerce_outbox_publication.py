"""Original UMF0.8 commerce finite dataset → private local Delta publication/ACK.

Genuine public finite-dataset admission is separate from original individual
Record context warnings. IDs/bindings are explicit development controls, never
Truss accepted IDs. No remote source/Unity Catalog/Databricks authority claim.
"""
import argparse,base64,datetime,hashlib,importlib.metadata,json,subprocess
from pathlib import Path
from ashlar.apply import empty_state
from ashlar.attempt_store import DeltaAttemptStore
from ashlar.outbox import PostgresOutbox,publish_outbox_transaction
from ashlar.stored_publisher import StoredPublisherBackend
from ashlar.whole_entity import changes_from_batch
from commerce_source_transaction import ROOT as ORIGINAL,SOURCE_SHA,GRAPH_SHA,build_transaction
from fixture_oracle import fixture_columns
from local_delta_custody import DeltaTarget,LocalDeltaTransport,encoded,sha
from local_outbox_connection import connect
from postgres_transactions import Session
from run_graph_release_graphframes import JARS,VERSIONS
from run_local_outbox_publication import ROOT,CLOCK,FIELDS,PrivatePolicy,NativeDriver,AttemptExecutor,CarrierPolicy,ManifestPort,local_effect_plan,progress_union,request_for,provision_sources
from whole_graph_sql import graph_sql_plan
UMF_PIN='c7c95e1c4ea5b72541f47fa0350ca467ff02f395'
SOURCE_SYSTEM='private-original-commerce-fixture'

class CommerceAdmission:
    """Explicit original source binding plus an actually recomputed public receipt.

    This host port admits only its exact original finite transaction. It does not
    emulate UMF validation, infer canonical Values, or mint accepted storage IDs.
    """
    def __init__(self,batch,bindings,model_path,graph_path,bindings_path,receipt_path):
        self.batch=batch;self.changes=changes_from_batch(batch)
        paths=[Path(p) for p in (model_path,graph_path,bindings_path,receipt_path)]
        self.originals=tuple((p,p.read_bytes()) for p in paths)
        model_bytes,graph_bytes,binding_bytes,receipt_bytes=[raw for _,raw in self.originals]
        if hashlib.sha256(model_bytes).hexdigest()!=SOURCE_SHA or hashlib.sha256(graph_bytes).hexdigest()!=GRAPH_SHA:raise PermissionError('Exact original commerce model/graph custody required')
        if json.loads(binding_bytes)!=bindings:raise PermissionError('Original development binding bytes differ')
        document=json.loads(model_bytes);graph=json.loads(graph_bytes);public=json.loads(receipt_bytes);receipt=public['receipt']
        if document['umf']!='0.8.0' or public['umfRevision']!=UMF_PIN or public['sourceSha256']!=SOURCE_SHA or public['graphSha256']!=GRAPH_SHA:raise PermissionError('Exact original public0.8 receipt custody required')
        if (receipt['scope']!='supplied-dataset-only' or receipt['input']['scope']!={'id':'ashlar-original-commerce-fixture','closure':'supplied-dataset-only'}) or receipt['datasetValidation']['valid'] is not True or receipt['datasetValidation']['complete'] is not True:raise PermissionError('Actual complete supplied finite dataset required')
        expected_records={r['key'] for r in graph['objects']};actual_records=[r['instanceId'] for r in receipt['records']]
        if len(actual_records)!=len(expected_records) or set(actual_records)!=expected_records or len(receipt['keys'])!=len(expected_records):raise PermissionError('Original complete finite Record/key inventory differs')
        if any(r['result']['validation']['valid'] is not True for r in receipt['records']):raise PermissionError('Original finite individual Record invalid')
        expected_edges={r['key']:(r['source'],r['target']) for r in graph['edges']}
        actual_edges={r['instanceId']:(r['sourceInstanceId'],r['targetInstanceId']) for r in receipt['relationships']}
        if len(receipt['relationships'])!=len(expected_edges) or actual_edges!=expected_edges:raise PermissionError('Original public relationship occurrence endpoints differ')
        rebuilt,expected_bindings=build_transaction(model_bytes,graph_bytes,source_system=batch.feed,binding_profile=bindings['profile'])
        if rebuilt!=batch or expected_bindings!=bindings or any(c.operation!='create' or c.state.schema_revision!=SOURCE_SHA for c in self.changes):raise PermissionError('Original fresh development source projection differs')
        self.facts={'profile':'ashlar-original-commerce-public-dataset-admission/0.1','qualification':__doc__,'umf_revision':UMF_PIN,'declared_umf':'0.8.0','source_sha256':SOURCE_SHA,'graph_sha256':GRAPH_SHA,
                    'public_receipt_sha256':hashlib.sha256(receipt_bytes).hexdigest(),'public_receipt_path':str(paths[3]),'public_provenance':receipt['provenance'],'dataset_validation':receipt['datasetValidation'],'finite_scope':receipt['input']['scope'],'receipt_scope':receipt['scope'],
                    'individual_record_validations':[{'instance_id':r['instanceId'],'validation':r['result']['validation']} for r in receipt['records']],
                    'original_model_base64':base64.b64encode(model_bytes).decode(),'original_graph_base64':base64.b64encode(graph_bytes).decode(),'original_bindings_base64':base64.b64encode(binding_bytes).decode(),'development_bindings_sha256':hashlib.sha256(binding_bytes).hexdigest(),
                    'source_transaction_sha256':hashlib.sha256(batch.begin+b''.join(r.raw for r in batch.records)+batch.commit).hexdigest(),'scope_note':'Supplied dataset closure only; original warnings retained, public provenance remains unverified as emitted. Host private source/writer/retention policy is separately required.'}
    def metadata(self):
        if any(p.read_bytes()!=raw for p,raw in self.originals):raise PermissionError('Original commerce admission bytes changed')
        return json.loads(encoded(self.facts))
    def admit(self,change):
        self.metadata()
        if change not in self.changes:raise PermissionError('No admitted original finite commerce source change')

from ashlar_host.commerce_admission import original_commerce_oracle

def finalize_publication(transport, spark, report, output: Path) -> None:
    """Attempt both native cleanup steps; expose success only after both succeed.

    Failed/interrupted work may retain native data and journals for recovery,
    but never obtains a successful setup report from this finalizer.
    """
    failures=[]
    if transport is not None:
        try:transport.close()
        except BaseException as error:failures.append(error)
    try:spark.stop()
    except BaseException as error:failures.append(error)
    if failures:raise failures[0]
    if report is not None:
        (output/'report.json').write_text(encoded(report)+'\n')


def run(output,jars,umf_source):
    output=Path(output)
    if output.exists():raise ValueError('Exclusive fresh local output required; retain every original attempted installation')
    if {k:importlib.metadata.version(k) for k in VERSIONS}!=VERSIONS:raise ValueError('Exact qualified existing Spark runtime required')
    paths=[Path(jars)/name for name in JARS]
    if not all(p.is_file() for p in paths):raise ValueError('Explicit existing jars required')
    output.mkdir(parents=True,mode=0o700)
    model_bytes=(ORIGINAL/'ontology.json').read_bytes();graph_bytes=(ORIGINAL/'graph/fixture.json').read_bytes()
    batch,bindings=build_transaction(model_bytes,graph_bytes,source_system=SOURCE_SYSTEM)
    model_path=output/'original-ontology.json';graph_path=output/'original-graph.json';binding_path=output/'development-bindings.json';receipt_path=output/'public-dataset.json'
    model_path.write_bytes(model_bytes);graph_path.write_bytes(graph_bytes);binding_path.write_text(encoded(bindings)+'\n')
    raw=batch.begin+b''.join(r.raw for r in batch.records)+batch.commit;(output/'source.jsonl').write_bytes(raw)
    process=subprocess.run(['bun',str(ROOT/'tools/check_commerce_dataset.ts'),str(umf_source),str(receipt_path)],text=True,capture_output=True,timeout=60)
    (output/'public-dataset-call.json').write_text(encoded({'exit':process.returncode,'stdout':process.stdout,'stderr':process.stderr,'umf_pin':UMF_PIN,'umf_source':str(umf_source)})+'\n')
    if process.returncode:raise PermissionError('Genuine original public finite-dataset recomputation failed; no native installation')
    admission=CommerceAdmission(batch,bindings,model_path,graph_path,binding_path,receipt_path)
    from pyspark.sql import SparkSession
    spark=(SparkSession.builder.master('local[1]').appName('Ashlar original commerce local publication').config('spark.driver.memory','512m').config('spark.sql.shuffle.partitions','1').config('spark.databricks.delta.snapshotPartitions','1').config('spark.ui.enabled','false').config('spark.sql.session.timeZone','UTC').config('spark.sql.ansi.enabled','true').config('spark.jars',','.join(str(p) for p in paths)).config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension').config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog').getOrCreate())
    transport=None;report=None
    try:
        graph_columns=fixture_columns(ROOT);columns={**graph_columns,'attempts':tuple((name,'STRING') for name in ('stream','batch_id','phase','request_digest','payload_json','payload_digest')),'manifest':tuple((name,'TIMESTAMP' if name=='recorded_at' else 'STRING') for name in FIELDS)}
        tables={role:'local.commerce.'+role for role in columns};targets=[]
        for role,schema in columns.items():
            path=output/role;spark.sql('CREATE TABLE delta.`'+str(path)+'` ('+','.join(name+' '+family for name,family in schema)+') USING DELTA')
            targets.append(DeltaTarget(tables[role],path,spark.sql('DESCRIBE DETAIL delta.`'+str(path)+'`').first().id))
        context=object();policy=PrivatePolicy(context,tuple(targets));transport=LocalDeltaTransport.initialize(spark,output/'operations.sqlite','private-original-commerce',tuple(targets),policy,context=context);policy.initializing=False
        ports=provision_sources(output,[{'label':'commerce','feed':batch.feed,'epoch':batch.epoch}]);port=ports[batch.feed]
        connection=connect(port['writer'])
        try:
            position=connection.execute('SELECT "'+port['source']+'".append(%s,%s)',(batch.batch_id,raw.decode())).fetchone()[0]
            if position!=1:raise ValueError('Fresh original source must start at zero')
            connection.commit()
        finally:connection.close()
        connection=connect(port['reader'])
        try:
            transaction,=PostgresOutbox(Session(connection),feed=batch.feed,epoch=batch.epoch,schema=port['source']).read('0',limit=1);connection.rollback()
        finally:connection.close()
        if transaction.batch!=batch:raise ValueError('Actual original source bytes differ')
        driver=NativeDriver(transport,policy,context,tables,ports,admission.changes,graph_columns,source_admission=admission)
        graph_tables={role:tables[role] for role in graph_columns};state,generated=graph_sql_plan(empty_state(),batch,graph_tables,materialized_at=CLOCK,schema_policy=driver.schema_admit)
        empty={role:[] for role in graph_columns};steps,elisions=local_effect_plan(generated,empty,graph_tables)
        expected=original_commerce_oracle(model_bytes,graph_bytes,bindings,batch,graph_columns)
        request=request_for('private-original-commerce',transaction,'explicit-original-commerce-origin',{SOURCE_SYSTEM:SOURCE_SHA})
        checkpoint=json.loads(request['source_checkpoint_json']);progress=progress_union({},checkpoint)
        policy.active={'request':request,'steps':steps,'expected':expected,'publication_id':'original-commerce-publication-1','previous_progress':{},'progress':progress,'previous_expected':empty,'generated_steps':generated,'elisions':elisions}
        target=transport.targets[tables['attempts']]
        backend=StoredPublisherBackend(DeltaAttemptStore(AttemptExecutor(driver),CarrierPolicy(driver,'attempts'),target.table,target.uuid),driver,lambda original,supplied:ManifestPort(driver,original))
        descriptor=publish_outbox_transaction(backend,'private-original-commerce',transaction,predecessor=request['predecessor'],schema_revisions_json=request['schema_revisions_json'],context=context)
        history_before={table:transport._history(transport.targets[table]) for table in descriptor.versions}
        repeated=publish_outbox_transaction(backend,'private-original-commerce',transaction,predecessor=request['predecessor'],schema_revisions_json=request['schema_revisions_json'],context=context)
        if repeated!=descriptor or any(transport._history(transport.targets[table])!=history_before[table] for table in descriptor.versions):raise ValueError('Exact commerce replay changed native publication')
        connection=connect(port['role'])
        try:
            observation,=Session(connection).query('SELECT * FROM "'+port['scope'].service_schema+'".observe(CAST(:scope AS uuid),CAST(:position AS bigint))',{'scope':port['scope'].scope_id,'position':'1'}).rows
            if observation['position']!='1' or bytes.fromhex(observation['request_hex'])!=encoded(request).encode() or json.loads(bytes.fromhex(observation['manifest_hex']))!=dict(descriptor.raw):raise ValueError('Actual protected original ACK bytes differ')
            connection.rollback()
        finally:connection.close()
        report={'format':'ashlar-original-commerce-publication/0.1','runtime_versions':VERSIONS,'public_umf_revision':UMF_PIN,'public_dataset_receipt_sha256':hashlib.sha256(receipt_path.read_bytes()).hexdigest(),'original_model_sha256':SOURCE_SHA,'original_graph_sha256':GRAPH_SHA,'bindings_sha256':hashlib.sha256(binding_path.read_bytes()).hexdigest(),
                'source_transaction_sha256':hashlib.sha256(raw).hexdigest(),'native_manifest':dict(descriptor.raw),'table_registry':[{'table':t.table,'uuid':t.uuid,'path':str(t.path)} for t in targets],'complete_original_oracle':expected,'record_count':11,'edge_count':10,'history_count':21,'protected_ack_scope':port['scope'].__dict__,'source_schema':port['source'],'source_signature_sha256':port['signature'],'protected_ack_observation':observation,'exact_replay_unchanged':True,'qualification':__doc__}
    except BaseException:
        report=None
        raise
    finally:
        finalize_publication(transport,spark,report,output)
    return report

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',required=True,type=Path);parser.add_argument('--jars',required=True,type=Path);parser.add_argument('--umf-source',required=True,type=Path);args=parser.parse_args();print(encoded(run(args.output,args.jars,args.umf_source)))
