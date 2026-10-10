"""Two separate local phases for the authored finite path fixture.

Private development source/writer policy and ordinary protected PostgreSQL ACK
are explicit. No Truss IDs, cloud, installed release or production qualification.
Success is persisted only after all reader/transport and Spark cleanup succeeds.
"""
import argparse
from contextlib import contextmanager
from dataclasses import dataclass, asdict, is_dataclass
from decimal import Decimal
import hashlib
import importlib.metadata
import json
import os
import stat
from pathlib import Path
import subprocess
from types import SimpleNamespace

from ashlar.apply import empty_state
from ashlar.attempt_store import DeltaAttemptStore
from ashlar.outbox import PostgresOutbox, publish_outbox_transaction
from ashlar.stored_publisher import StoredPublisherBackend
from ashlar.whole_entity import changes_from_batch
from local_delta_custody import DeltaTarget, LocalDeltaTransport, encoded
from local_outbox_connection import connect
from postgres_transactions import Session
from protected_outbox_ack import AckScope
from fixture_oracle import fixture_columns
from run_local_outbox_publication import (ROOT, CLOCK, FIELDS, PrivatePolicy, NativeDriver,
    AttemptExecutor, CarrierPolicy, ManifestPort, local_effect_plan, progress_union,
    request_for, provision_sources)
from whole_graph_sql import graph_sql_plan
from run_commerce_publication_weft import ReadOnlyTransport, PublicationProvider
from run_commerce_path_weft import PathExecutionConfig, execute_commerce_path
from weft_path_fixture import expected_cases
from weft_path_fixture_request import authored_path_request, INPUT_HASHES
from weft_path_fixture_source import build_path_transaction, AuthoredPathAdmission, independent_rows
from weft_path_compiler import PathCompilerConfig, compile_path_request
from weft_path_schema import make_offline_path_schema_validation
from weft_path_plan import PathAdmissionConfig, SCHEMA_SHA256
from weft_path_capture import PathCaptureConfig
from ashlar.weft_path_decode import PathDecodeConfig

INSTALLATION = 'private-authored-path-fixture'
PUBLICATION_ID = '30000000-0000-0000-0000-000000000001'
UMF_PIN = 'c7c95e1c4ea5b72541f47fa0350ca467ff02f395'
ACK = ('ashlar-e2e-truss-pg17','127.0.0.1',15432,'truss_e2e')


@dataclass(frozen=True)
class FixtureNativeConfig:
    publication: Path
    output: Path
    model: Path
    graph: Path
    registry: Path
    producer: Path
    umf_source: Path
    bun: Path
    jars: Path
    schemas: tuple
    compiler: PathCompilerConfig
    capture: PathCaptureConfig
    decoder: PathDecodeConfig
    maximum_input_bytes: int
    maximum_artifact_bytes: int
    maximum_native_files: int
    maximum_native_bytes: int
    ack: tuple

    def __post_init__(self):
        for p in (self.publication,self.output,self.model,self.graph,self.registry,self.producer,self.umf_source,self.bun,self.jars):
            if not isinstance(p,Path) or not p.is_absolute():raise ValueError('Explicit absolute operator paths required')
        if self.ack!=ACK:raise ValueError('Explicit qualified private ACK profile required')
        if type(self.schemas)is not tuple or len(self.schemas)!=3 or {p.name for p in self.schemas}!=set(SCHEMA_SHA256) or any(not isinstance(p,Path) or not p.is_absolute() for p in self.schemas):raise ValueError('Exact explicit schema paths required')
        if type(self.compiler)is not PathCompilerConfig or type(self.capture)is not PathCaptureConfig or type(self.decoder)is not PathDecodeConfig:raise ValueError('Explicit typed finite ports required')
        if type(self.maximum_native_files)is not int or not 1<=self.maximum_native_files<=10000 or type(self.maximum_native_bytes)is not int or not 1<=self.maximum_native_bytes<=1073741824:raise ValueError('Explicit finite native inventory bounds required')
        if any(p.is_symlink() for p in (self.output,*self.output.parents)):raise ValueError('Owned regular output parent required')
        for n in (self.maximum_input_bytes,self.maximum_artifact_bytes):
            if type(n)is not int or not 1<=n<=16000000:raise ValueError('Explicit finite byte bounds required')


def read_bounded(path,maximum):
    if any(p.is_symlink() for p in (path,*path.parents)) or not path.is_file() or path.stat().st_size>maximum:raise ValueError('Bounded regular file required')
    with path.open('rb') as stream:raw=stream.read(maximum+1)
    if len(raw)>maximum:raise ValueError('File capacity exceeded')
    return raw


def json_value(raw):
    def pairs(items):
        out={}
        for k,v in items:
            if k in out:raise ValueError('Duplicate protocol member')
            out[k]=v
        return out
    def integer(s):
        if len(s.lstrip('-'))>20:raise ValueError('Protocol integer capacity exceeded')
        return int(s)
    def refused(s):raise ValueError('Unsupported numeric atom')
    return json.loads(raw,object_pairs_hook=pairs,parse_int=integer,parse_float=refused,parse_constant=refused)


def preflight(config,phase):
    if type(config)is not FixtureNativeConfig or phase not in ('publish','query'):raise ValueError('Explicit phase configuration required')
    if subprocess.check_output(['git','-C',str(config.umf_source),'rev-parse','HEAD'],text=True).strip()!=UMF_PIN or subprocess.check_output(['git','-C',str(config.umf_source),'status','--porcelain'],text=True).strip():raise ValueError('Exact clean public UMF source required')
    if phase=='publish':
        from run_graph_release_graphframes import JARS,VERSIONS
        if {k:importlib.metadata.version(k) for k in VERSIONS}!=VERSIONS:raise ValueError('Qualified publication runtime required')
        from run_pack_release_graphframes import QUALIFIED_JAR_SHA
        hashes=QUALIFIED_JAR_SHA
    else:
        from run_indexed_commerce_weft import DELTA4_JARS
        if importlib.metadata.version('pyspark')!='4.0.1' or importlib.metadata.version('delta-spark')!='4.0.0':raise ValueError('Qualified query runtime required')
        hashes=DELTA4_JARS
    paths=[config.jars/name for name in sorted(hashes)]
    if set(p.name for p in config.jars.glob('*.jar'))!=set(hashes) or any(hashlib.sha256(read_bounded(p,32000000)).hexdigest()!=hashes[p.name] for p in paths):raise ValueError('Exact selected runtime JAR set required')
    return paths


def fresh_public(config,output,m,g):
    producer=read_bounded(config.producer,config.maximum_input_bytes)
    if hashlib.sha256(producer).hexdigest()!=PRODUCER_SHA:raise ValueError('Exact reviewed public dataset producer required')
    path=output/'fresh-public-dataset.json'
    call=subprocess.run([str(config.bun),str(config.producer),str(config.umf_source),str(path)],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=60)
    if call.returncode:raise PermissionError('Actual public dataset operation refused')
    receipt=read_bounded(path,config.maximum_input_bytes)
    # Callback is closed and uses bytes freshly produced by genuine public APIs.
    def verify(m0,g0,r0):
        if (m0,g0,r0)!=(m,g,receipt):raise PermissionError('Exact freshly verified public source bytes required')
    return receipt,verify


def create_fixture_spark(config,jars,phase):
    from pyspark.sql import SparkSession
    return (SparkSession.builder.master('local[1]').appName('Authored path '+phase)
        .config('spark.driver.memory','512m').config('spark.ui.enabled','false')
        .config('spark.sql.shuffle.partitions','1').config('spark.databricks.delta.snapshotPartitions','1')
        .config('spark.sql.session.timeZone','UTC').config('spark.sql.ansi.enabled','true')
        .config('spark.sql.decimalOperations.allowPrecisionLoss','false')
        .config('spark.jars',','.join(str(p) for p in jars))
        .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
        .config('spark.sql.warehouse.dir',str(config.output/'warehouse')).getOrCreate())


def fixture_oracle(model,graph,request):
    if hashlib.sha256(model).hexdigest()!=INPUT_HASHES['model'] or hashlib.sha256(graph).hexdigest()!=INPUT_HASHES['graph']:raise ValueError('Exact original oracle source bytes required')
    expected=expected_cases(model,graph);sql=request['sql'];rows=None;case_name=None
    for case in expected['cases']:
        if sql!=case['sql']:continue
        case_name=case['name']
        if case_name=='left-presence':rows=[[name,json.dumps(value,ensure_ascii=False,separators=(',',':'))] for name,value in case['rows']]
        else:rows=[['1' if case_name=='self-loop' else case['start'],json.dumps(case['paths'],ensure_ascii=False,separators=(',',':'))]]
    for name,query in expected['expansionQueries'].items():
        if sql==query:case_name=name;rows=[] if name=='noGroups' else [[str({'six':6,'oneTarget':1,'zero':0}[name])]]
    if rows is None:raise ValueError('Unselected original authored query')
    return {'rows':rows,'witnesses':{'case':case_name,'completeOriginalPathOracle':expected},'scope':'Raw authored graph occurrence oracle, independently of compiler/native rows.'}


def _inputs(config):
    raw=tuple(read_bounded(p,config.maximum_input_bytes) for p in (config.model,config.graph,config.registry))
    build_path_transaction(*raw)  # Exact original hash/canonical envelope fence.
    return raw


def _json(value):
    if is_dataclass(value):return _json(asdict(value))
    if type(value)is bytes:return {'hex':value.hex()}
    if isinstance(value,Decimal):return {'decimal':str(value)}
    if isinstance(value,dict):return {k:_json(v) for k,v in value.items()}
    if isinstance(value,(list,tuple)):return [_json(v) for v in value]
    return value


@contextmanager
def owned_connection(connection):
    """Close the owned session without replacing its primary body failure."""
    primary = None
    try:
        yield connection
    except BaseException as error:
        primary = error
        raise
    finally:
        try:
            connection.close()
        except BaseException:
            if primary is None:
                raise
            primary.cleanup_failed = True


def finalize_owned(spark,transport,primary,result,output):
    failures=[]
    if transport is not None:
        try:transport.close()
        except BaseException as e:failures.append(e)
    try:spark.stop()
    except BaseException as e:failures.append(e)
    if primary is not None:
        if failures:primary.cleanup_failed=True
        raise primary
    if failures:raise failures[0]
    if result is not None:
        payload = (encoded(_json(result))+'\n').encode()
        with (output/'report.json').open('xb') as stream:stream.write(payload)
    return result

PRODUCER_SHA = '3ded4d61ab86b7ba8f63acdf3d99975afd353fcf29c4e21d0dfc7e211be984c2'


def native_files(targets,config):
    files={};total=0;entries=0
    for target in targets:
        directories=[target.path]
        while directories:
            directory=directories.pop()
            if directory.is_symlink():raise ValueError('Native symlink refused')
            with os.scandir(directory) as children:
                for entry in children:
                    entries+=1
                    if entries>config.maximum_native_files*2:raise ValueError('Native traversal capacity exceeded')
                    path=Path(entry.path);info=entry.stat(follow_symlinks=False)
                    if stat.S_ISDIR(info.st_mode):directories.append(path);continue
                    if not stat.S_ISREG(info.st_mode):raise ValueError('Native regular file required')
                    if len(files)>=config.maximum_native_files:raise ValueError('Native inventory capacity exceeded')
                    size=info.st_size;total+=size
                    if total>config.maximum_native_bytes:raise ValueError('Native byte capacity exceeded')
                    digest=hashlib.sha256();remaining=size
                    with path.open('rb') as stream:
                        while remaining:
                            chunk=stream.read(min(65536,remaining))
                            if not chunk:raise ValueError('Native file changed during hash')
                            digest.update(chunk);remaining-=len(chunk)
                        if stream.read(1):raise ValueError('Native file changed during hash')
                    files[str(path)]={'sha256':digest.hexdigest(),'bytes':size}
    return files


def publish_fixture(config: FixtureNativeConfig) -> dict:
    jars=preflight(config,'publish');m,g,r=_inputs(config)
    if config.output!=config.publication or config.output.exists():raise ValueError('Fresh owned publication output required')
    config.output.mkdir(parents=False,exist_ok=False)
    receipt,verify=fresh_public(config,config.output,m,g)
    batch=build_path_transaction(m,g,r);admission=AuthoredPathAdmission(batch,model_bytes=m,graph_bytes=g,registry_bytes=r,public_receipt_bytes=receipt,verify_public_receipt=verify)
    raw=batch.begin+b''.join(x.raw for x in batch.records)+batch.commit
    for name,bytes_ in [('model.umf.json',m),('graph.json',g),('registry.json',r),('public-dataset.json',receipt),('source.jsonl',raw)]:
        with (config.output/name).open('xb') as stream:stream.write(bytes_)
    spark=create_fixture_spark(config,jars,'publish');transport=None;result=None;primary=None
    try:
        if spark.version!='3.5.3':raise ValueError('Exact publication Spark required')
        graph_columns=fixture_columns(ROOT);columns={**graph_columns,
            'attempts':tuple((name,'STRING') for name in ('stream','batch_id','phase','request_digest','payload_json','payload_digest')),
            'manifest':tuple((name,'TIMESTAMP' if name=='recorded_at' else 'STRING') for name in FIELDS)}
        tables={role:'local.authored_paths.'+role for role in columns};targets=[]
        for role,schema in columns.items():
            path=config.publication/role
            spark.sql('CREATE TABLE delta.`'+str(path)+'` ('+','.join(name+' '+family for name,family in schema)+') USING DELTA')
            targets.append(DeltaTarget(tables[role],path,spark.sql('DESCRIBE DETAIL delta.`'+str(path)+'`').first().id))
        context=object();policy=PrivatePolicy(context,tuple(targets))
        transport=LocalDeltaTransport.initialize(spark,config.publication/'operations.sqlite',INSTALLATION,tuple(targets),policy,context=context);policy.initializing=False
        ports=provision_sources(config.publication,[{'label':'authored_paths','feed':batch.feed,'epoch':batch.epoch}]);port=ports[batch.feed]
        with owned_connection(connect(port['writer'])) as connection:
            position=connection.execute('SELECT "'+port['source']+'".append(%s,%s)',(batch.batch_id,raw.decode())).fetchone()[0]
            if position!=1:raise ValueError('Fresh source origin required')
            connection.commit()
        with owned_connection(connect(port['reader'])) as connection:
            transaction,=PostgresOutbox(Session(connection),feed=batch.feed,epoch=batch.epoch,schema=port['source']).read('0',limit=1)
            connection.rollback()
        if transaction.batch!=batch:raise ValueError('Original source transport changed bytes')
        driver=NativeDriver(transport,policy,context,tables,ports,changes_from_batch(batch),graph_columns,source_admission=admission)
        graph_tables={role:tables[role] for role in graph_columns}
        state,generated=graph_sql_plan(empty_state(),batch,graph_tables,materialized_at=CLOCK,schema_policy=driver.schema_admit)
        empty={role:[] for role in graph_columns};steps,elisions=local_effect_plan(generated,empty,graph_tables)
        from datetime import datetime,timezone
        stamp=str(int((datetime.fromisoformat(CLOCK)-datetime(1970,1,1,tzinfo=timezone.utc)).total_seconds())*1000000)
        expected=independent_rows(m,g,r,batch=batch,columns=graph_columns,published_at_micros=stamp)
        request=request_for(INSTALLATION,transaction,'explicit-authored-path-origin',{batch.feed:INPUT_HASHES['model']})
        progress=progress_union({},json.loads(request['source_checkpoint_json']))
        policy.active={'request':request,'steps':steps,'expected':expected,'publication_id':PUBLICATION_ID,'previous_progress':{},'progress':progress,'previous_expected':empty,'generated_steps':generated,'elisions':elisions}
        target=transport.targets[tables['attempts']]
        backend=StoredPublisherBackend(DeltaAttemptStore(AttemptExecutor(driver),CarrierPolicy(driver,'attempts'),target.table,target.uuid),driver,lambda original,supplied:ManifestPort(driver,original))
        descriptor=publish_outbox_transaction(backend,INSTALLATION,transaction,predecessor=request['predecessor'],schema_revisions_json=request['schema_revisions_json'],context=context)
        before=native_files(targets,config)
        repeated=publish_outbox_transaction(backend,INSTALLATION,transaction,predecessor=request['predecessor'],schema_revisions_json=request['schema_revisions_json'],context=context)
        if repeated!=descriptor or native_files(targets,config)!=before:raise ValueError('Exact replay changed native publication')
        with owned_connection(connect(port['role'])) as connection:
            observation,=Session(connection).query('SELECT * FROM "'+port['scope'].service_schema+'".observe(CAST(:scope AS uuid),CAST(:position AS bigint))',{'scope':port['scope'].scope_id,'position':'1'}).rows
            if observation['position']!='1' or bytes.fromhex(observation['request_hex'])!=encoded(request).encode() or json.loads(bytes.fromhex(observation['manifest_hex']))!=dict(descriptor.raw):raise PermissionError('Protected original ACK correspondence refused')
            connection.rollback()
        for p,original in zip((config.model,config.graph,config.registry),(m,g,r)):
            if read_bounded(p,config.maximum_input_bytes)!=original:raise ValueError('Original source input changed')
        result={'format':'ashlar-authored-path-publication/0.1','qualification':__doc__,'sourceAdmission':admission.metadata(),
                'native_manifest':dict(descriptor.raw),'table_registry':[{'table':t.table,'uuid':t.uuid,'path':str(t.path)} for t in targets],
                'protected_ack_scope':asdict(port['scope']),'source_schema':port['source'],'source_signature_sha256':port['signature'],'protected_ack_observation':observation,
                'expected_full_rows':expected,'source_transaction_sha256':hashlib.sha256(raw).hexdigest(),'native_files':before,'exact_replay_unchanged':True,
                'counts':{'objects':10,'edges':10,'history':20},'runtime':{'spark':'3.5.3','delta':'3.2.1'},
                'request':request,'original_source_hashes':{k:hashlib.sha256(b).hexdigest() for k,b in [('model',m),('graph',g),('registry',r),('publicReceipt',receipt)]}}
    except BaseException as error:primary=error;result=None
    return finalize_owned(spark,transport,primary,result,config.output)


@contextmanager
def open_fixture_reader(spark,config,originals,verify):
    m,g,r=[originals[k] for k in ('model.umf.json','graph.json','registry.json')]
    report=json_value(originals['report.json']);batch=build_path_transaction(m,g,r)
    if originals['source.jsonl']!=batch.begin+b''.join(x.raw for x in batch.records)+batch.commit:raise ValueError('Original source envelope drift')
    admission=AuthoredPathAdmission(batch,model_bytes=m,graph_bytes=g,registry_bytes=r,public_receipt_bytes=originals['public-dataset.json'],verify_public_receipt=verify)
    from datetime import datetime,timezone
    stamp=str(int((datetime.fromisoformat(CLOCK)-datetime(1970,1,1,tzinfo=timezone.utc)).total_seconds())*1000000)
    expected=independent_rows(m,g,r,batch=batch,columns=fixture_columns(ROOT),published_at_micros=stamp)
    if expected!=report['expected_full_rows'] or admission.metadata()!=report['sourceAdmission']:raise ValueError('Original publication oracle/admission differs')
    targets=[DeltaTarget(row['table'],Path(row['path']),row['uuid']) for row in report['table_registry']]
    if len(targets)!=6 or any(t.path!=config.publication/t.table.split('.')[-1] for t in targets):raise ValueError('Original target inventory differs')
    before=native_files(targets,config)
    if before!=report['native_files']:raise ValueError('Original native vector changed')
    context=object();policy=PrivatePolicy(context,targets);policy.initializing=False;transport=None;provider=None
    try:
        transport=ReadOnlyTransport.open(spark,config.publication/'operations.sqlite',INSTALLATION,targets,policy)
        request_raw,artifact_raw=transport.db.execute('SELECT request,artifact FROM local_publication_artifact').fetchone()
        request=json_value(request_raw);manifest=json_value(artifact_raw)['manifest']
        if manifest!=report['native_manifest'] or request!=report['request']:raise ValueError('Original immutable journal artifact differs')
        scope=AckScope(**report['protected_ack_scope']);source=report['source_schema']
        port={'scope':scope,'source':source,'reader':source+'_reader','role':scope.service_schema.replace('pipeline_','operator_'),'signature':report['source_signature_sha256']}
        tables={t.table.split('.')[-1]:t.table for t in targets}
        driver=NativeDriver(transport,policy,context,tables,{batch.feed:port},changes_from_batch(batch),fixture_columns(ROOT),source_admission=admission)
        plan=json_value(transport.db.execute('SELECT original FROM local_source_plan WHERE request_digest=?',(request['request_digest'],)).fetchone()[0])
        policy.active={'request':request,'steps':plan['selected_steps'],'expected':expected,'manifest':manifest,'publication_id':manifest['publication_id'],
            'previous_progress':{},'progress':json.loads(manifest['source_progress_json']),'previous_expected':plan['complete_prior_oracle'],'generated_steps':plan['generated_steps'],'elisions':plan['zero_match_elisions']}
        aliases={name:name.replace('local.','spark_catalog.',1) for name in json.loads(manifest['table_versions_json'])}
        provider=PublicationProvider(driver,aliases,port,request_raw.encode(),encoded(manifest).encode(),{},context=context)
        yield SimpleNamespace(provider=provider,driver=driver,context=context,model=m,graph=g,registry=r,manifest=manifest,original_report=report,
                              native_files=lambda:native_files(targets,config),original_native_files=before,aliases=aliases)
        if native_files(targets,config)!=before:raise ValueError('Original native files changed at reader close')
    finally:
        if provider is not None:provider._reader_closed=True
        if transport is not None:transport.close()


def query_fixture(config: FixtureNativeConfig) -> dict:
    jars=preflight(config,'query');m,g,r=_inputs(config)
    if config.output.exists() or config.output==config.publication:raise ValueError('Fresh query output required')
    names=('report.json','model.umf.json','graph.json','registry.json','public-dataset.json','source.jsonl')
    originals={n:read_bounded(config.publication/n,config.maximum_input_bytes) for n in names}
    if (originals['model.umf.json'],originals['graph.json'],originals['registry.json'])!=(m,g,r):raise ValueError('Explicit source/publication mismatch')
    schemas={p.name:read_bounded(p,config.maximum_artifact_bytes) for p in config.schemas}
    schema_port=make_offline_path_schema_validation(schemas)
    config.output.mkdir(parents=False,exist_ok=False)
    receipt,verify=fresh_public(config,config.output,m,g)
    if receipt!=originals['public-dataset.json']:raise ValueError('Fresh public receipt differs')
    opening_jars={str(p):hashlib.sha256(read_bounded(p,32000000)).hexdigest() for p in jars}
    spark=create_fixture_spark(config,jars,'query');result=None;primary=None;body_failure=None
    try:
        if spark.version!='4.0.1':raise ValueError('Exact query Spark required')
        with open_fixture_reader(spark,config,originals,verify) as opened:
            try:
                with opened.provider.interval(opened.context):
                    try:
                        for native,alias in opened.aliases.items():
                            parts=alias.split('.')
                            if len(parts)!=3 or any(not p.replace('_','').isalnum() for p in parts):raise ValueError('Original alias inventory refused')
                            spark.sql('CREATE DATABASE IF NOT EXISTS '+parts[-2]).collect()
                            path=opened.driver.transport.targets[native].path
                            spark.sql('CREATE TABLE '+alias+" USING DELTA LOCATION '"+str(path).replace("'","''")+"'").collect()
                        if opened.native_files()!=opened.original_native_files:raise ValueError('Alias setup changed native bytes')
                    except BaseException as error:
                        body_failure=error
                        raise
                if body_failure is not None:raise body_failure
                alias_interval=opened.provider.closed_interval_custody(opened.context)
                manifest=opened.manifest;versions=json.loads(manifest['table_versions_json'])
                registered={t['table']:t for t in opened.original_report['table_registry']}
                publication={'id':manifest['publication_id'],'manifestUuid':registered[opened.driver.tables['manifest']]['uuid'],
                    'tables':[{'name':opened.aliases[name].split('.'),'uuid':registered[name]['uuid'],'version':version} for name,version in sorted(versions.items())]}
                aliases={t['name'][-1]:t['name'] for t in publication['tables']}
                expected=expected_cases(m,g);queries=[(c['name'],c['sql']) for c in expected['cases']]+list(expected['expansionQueries'].items())
                results=[]
                execution=PathExecutionConfig(PathAdmissionConfig(config.maximum_artifact_bytes,schema_port),config.capture,config.decoder,None,None)
                for name,sql in queries:
                    request=authored_path_request(sql,model_bytes=m,graph_bytes=g,development_registry_bytes=r,public_receipt_bytes=receipt,verify_public_receipt=verify,publication=publication,aliases=aliases)
                    raw=(json.dumps(request,ensure_ascii=False,separators=(',',':'))+'\n').encode()
                    response=compile_path_request(raw,config=config.compiler);recompiled=compile_path_request(raw,config=config.compiler)
                    if opened.provider.active:raise ValueError('Inactive binding selection required')
                    opened.provider.expected_binding=encoded(json_value(request['target']['bindingJson']))
                    provisional=execute_commerce_path(opened,request,json_value(response),json_value(recompiled),config=execution,original_oracle=fixture_oracle)
                    results.append({'case':name,'request':raw,'response':response,'recompiled':recompiled,'provisional':provisional})
                closing=dict(opened.native_files())
                if closing!=opened.original_native_files:raise ValueError('Full closing native vector differs')
                for name,raw in originals.items():
                    if read_bounded(config.publication/name,config.maximum_input_bytes)!=raw:raise ValueError('Original publication inputs drift')
                for path,raw in zip((config.model,config.graph,config.registry),(m,g,r)):
                    if read_bounded(path,config.maximum_input_bytes)!=raw:raise ValueError('Original operator source bytes drift')
                for path in config.schemas:
                    if read_bounded(path,config.maximum_artifact_bytes)!=schemas[path.name]:raise ValueError('Original schema bytes drift')
                if {str(p):hashlib.sha256(read_bounded(p,32000000)).hexdigest() for p in jars}!=opening_jars:raise ValueError('Original JAR bytes drift')
                result={'format':'ashlar-authored-path-native-query/0.1','qualification':__doc__,'cases':results,'alias_interval':alias_interval,
                        'original_manifest':manifest,'opening_native_files':dict(opened.original_native_files),'closing_native_files':closing,
                        'original_source_hashes':{n:hashlib.sha256(raw).hexdigest() for n,raw in originals.items()},
                        'original_publication_report_sha256':hashlib.sha256(originals['report.json']).hexdigest(),'original_publication_report':opened.original_report,
                        'schema_hashes':{n:hashlib.sha256(raw).hexdigest() for n,raw in schemas.items()},'jars':opening_jars,'runtime':{'spark':'4.0.1','delta':'4.0.0'}}
            except BaseException as error:
                if body_failure is None:body_failure=error
                elif error is not body_failure:body_failure.cleanup_failed=True
                raise
        if body_failure is not None:raise body_failure
    except BaseException as error:
        primary=body_failure if body_failure is not None else error;result=None
        if body_failure is not None and error is not body_failure:primary.reader_cleanup_failed=True
    return finalize_owned(spark,None,primary,result,config.output)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('phase',choices=('publish','query'))
    for name in ('publication','output','model','graph','registry','producer','umf-source','bun','jars','compiler'):
        parser.add_argument('--'+name,type=Path,required=True)
    parser.add_argument('--schema',type=Path,action='append',required=True)
    for name in ('maximum-input-bytes','maximum-artifact-bytes','maximum-request-bytes','maximum-response-bytes','timeout-seconds',
                 'maximum-rows','maximum-cell-bytes','maximum-total-cell-bytes','maximum-native-files','maximum-native-bytes'):
        parser.add_argument('--'+name,type=int,required=True)
    parser.add_argument('--ack-container',required=True);parser.add_argument('--ack-host',required=True)
    parser.add_argument('--ack-port',type=int,required=True);parser.add_argument('--ack-database',required=True)
    args=parser.parse_args()
    config=FixtureNativeConfig(args.publication,args.output,args.model,args.graph,args.registry,args.producer,args.umf_source,args.bun,args.jars,
        tuple(args.schema),PathCompilerConfig(args.compiler,args.maximum_request_bytes,args.maximum_response_bytes,args.timeout_seconds),
        PathCaptureConfig(args.maximum_rows,args.maximum_cell_bytes,args.maximum_total_cell_bytes),PathDecodeConfig(args.maximum_cell_bytes),
        args.maximum_input_bytes,args.maximum_artifact_bytes,args.maximum_native_files,args.maximum_native_bytes,
        (args.ack_container,args.ack_host,args.ack_port,args.ack_database))
    report=publish_fixture(config) if args.phase=='publish' else query_fixture(config)
    print(json.dumps({'phase':args.phase,'reportPersisted':True,'cases':len(report.get('cases',[]))}))


if __name__=='__main__':main()
