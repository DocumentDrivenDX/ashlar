"""Bounded private commerce A/B evolution and original-operation recovery.

Genuine public UMF finite datasets/source preservation; explicit development IDs.
Local Delta/process writer and protected ordinary PostgreSQL ACK only: no Truss
accepted identity, production source installation, Unity Catalog or cloud claim.
"""
import argparse
from dataclasses import asdict
import hashlib
import importlib.metadata
import json
from pathlib import Path
import zlib

from ashlar.apply import empty_state
from ashlar.attempt_store import DeltaAttemptStore
from ashlar.manifest import FIELDS
from ashlar.outbox import PostgresOutbox,publish_outbox_transaction
from ashlar.source_checkpoint import outbox_checkpoint
from ashlar.stored_publisher import StoredPublisherBackend
from commerce_evolution_batch_scope import ScopedCommerceEvolutionAdmission,scoped_oracle
from commerce_evolution_admission import bounded_bytes
from fixture_oracle import fixture_columns
from local_delta_custody import DeltaTarget,LocalDeltaTransport,LocalDeltaError,encoded
from local_outbox_connection import connect
from postgres_transactions import Session
from run_graph_release_graphframes import JARS,VERSIONS
from run_local_outbox_publication import (NativeDriver,PrivatePolicy,AttemptExecutor,CarrierPolicy,ManifestPort,LostAfterManifest,provision_sources,local_effect_plan,request_for,progress_union,CLOCK,ROOT)

INSTALLATION='private-commerce-evolution'
STREAM='private-commerce-evolution'
ORDER=tuple((label,index)for index in range(4)for label in ('a','b'))

def admit_batch_inventory(fixtures):
    identities=[batch.batch_id for selected in fixtures.values()for _,batch in selected['batches']]
    if len(identities)!=8 or len(set(identities))!=8:raise PermissionError('Eight distinct source-qualified original batch identities required in the global stream')

def original_inputs(paths):
    candidate,compressed,model=(bounded_bytes(path)for path in paths)
    decoder=zlib.decompressobj(31);proof=decoder.decompress(compressed,4_000_001)
    if len(proof)>4_000_000 or not decoder.eof or decoder.unused_data:raise PermissionError('Bounded single original public receipt required before native startup')
    return candidate,proof,model

def assert_recovery_inventory(recovery,native,acks):
    if recovery!=[{'ordinal':1,'boundary':'LostAfterManifest'},{'ordinal':3,'boundary':'LostNativeCommitResponse'}]:raise ValueError('Exact original manifest/native recovery boundaries did not occur')
    if native.statement is not None or len(native.events)!=1 or native.events[0]['boundary']!='after real native collect; original journal remains submitted':raise ValueError('Exactly one consumed actual native collect-loss boundary required')
    reconciled=[a for a in acks if a.get('uncertain_commit_fresh_reconciled')is True]
    if reconciled!=[{'publication_id':'commerce-evolution-publication-2','uncertain_commit_fresh_reconciled':True}]:raise ValueError('Exact original protected PostgreSQL uncertain commit must reconcile freshly')

class CombinedAdmissions:
    def __init__(self,admissions):
        self.admissions=tuple(admissions)
        self.by_source={(a.prepared.source_system,a.prepared.epoch):a for a in admissions}
        if len(self.by_source)!=len(self.admissions):raise PermissionError('Distinct immutable source/epoch scopes required')
        self.changes=tuple(c for a in self.admissions for c in a.changes)
    def metadata(self):
        return {'profile':'ashlar-commerce-evolution-two-source/0.1','qualification':__doc__,'sources':[a.metadata()for a in self.admissions]}
    def _source(self,change):
        try:return self.by_source[(change.feed,change.epoch)]
        except KeyError as error:raise PermissionError('Original source/epoch admission required')from error
    def admit(self,change):return self._source(change).admit(change)
    def admit_transition(self,previous,change):return self._source(change).admit_transition(previous,change)

def prefix_union(fixtures,prefixes,columns):
    """Concatenate independent original source oracles, never SQL result rows."""
    if set(prefixes)!=set(fixtures):raise ValueError('Complete explicit source prefix inventory required')
    result={role:[]for role in columns}
    for label,selected in fixtures.items():
        prefix=prefixes[label]
        if type(prefix)is not int or not 0<=prefix<=4:raise ValueError('Original finite source prefix required')
        if not prefix:continue
        expected=scoped_oracle(*selected['originals'],selected['admission'].prepared,columns,prefix=prefix,materialized_at=CLOCK)
        for role in result:result[role].extend(expected[role])
    return result

class LostNativeCommitResponse(OSError):pass

class NativeResponseLoss:
    """Inject response loss only after the real selected Delta command collects."""
    def __init__(self,spark):self.spark=spark;self.statement=None;self.events=[]
    def __getattr__(self,name):return getattr(self.spark,name)
    def arm(self,statement):
        if self.statement is not None:raise ValueError('Only one explicit original loss boundary permitted')
        self.statement=statement
    def sql(self,statement,*args,**kwargs):
        frame=self.spark.sql(statement,*args,**kwargs)
        if statement!=self.statement:return frame
        self.statement=None;owner=self
        class CompletedFrame:
            def collect(self):
                result=frame.collect()
                owner.events.append({'statement':statement,'parameters':kwargs.get('args'),'boundary':'after real native collect; original journal remains submitted','native_result_rows':len(result)})
                raise LostNativeCommitResponse('Injected loss after real original Delta commit')
        return CompletedFrame()

def close_resources(transport,spark):
    errors=[]
    for close in (transport.close if transport is not None else None,spark.stop):
        if close is not None:
            try:close()
            except BaseException as error:errors.append(error)
    if errors:raise errors[0]

def run(output,jars,umf_source):
    output=Path(output)
    if output.exists():raise ValueError('Exclusive fresh private installation required')
    if {name:importlib.metadata.version(name)for name in VERSIONS}!=VERSIONS:raise ValueError('Existing qualified runtime required')
    jar_paths=[Path(jars)/name for name in JARS]
    if not all(p.is_file()for p in jar_paths):raise ValueError('Explicit existing cached jars required')
    output.mkdir(parents=True,mode=0o700)
    archive=ROOT/'docs/helix/04-build/evidence';preparation=archive/'commerce-present-field-preparation-20261009'
    paths=[output/name for name in ('candidate.json','public-presence.json.gz','original-model.json')]
    for target,original in zip(paths,(preparation/'candidate.json',preparation/'public-presence.json.gz',archive/'commerce-evolution-preparation-20261009/original-model.json')):target.write_bytes(bounded_bytes(original))
    originals=original_inputs(paths)
    fixtures={}
    for label in ('a','b'):
        # Source installation/consumer facts are independently established below.
        admission=ScopedCommerceEvolutionAdmission(*paths,umf_repo=umf_source,source_system='commerce-evolution-'+label,epoch='private-original-'+label)
        selected={'label':label,'feed':admission.prepared.source_system,'epoch':admission.prepared.epoch,'source':admission.prepared.source_system,'admission':admission,'originals':originals,'batches':[]}
        for index,batch in enumerate(admission.prepared.batches,1):
            raw=batch.begin+b''.join(r.raw for r in batch.records)+batch.commit;(output/(label+'-'+str(index)+'.source.jsonl')).write_bytes(raw);selected['batches'].append((raw,batch))
        fixtures[label]=selected
    admit_batch_inventory(fixtures)
    admission=CombinedAdmissions([f['admission']for f in fixtures.values()]);(output/'admission.json').write_text(encoded(admission.metadata())+'\n')
    from pyspark.sql import SparkSession
    spark=(SparkSession.builder.master('local[1]').appName('Ashlar tiny commerce evolution recovery').config('spark.driver.memory','512m').config('spark.sql.shuffle.partitions','1').config('spark.databricks.delta.snapshotPartitions','1').config('spark.ui.enabled','false').config('spark.sql.session.timeZone','UTC').config('spark.sql.ansi.enabled','true').config('spark.jars',','.join(str(p)for p in jar_paths)).config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension').config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog').getOrCreate())
    native=NativeResponseLoss(spark);transport=None;report=None
    try:
        graph_columns=fixture_columns(ROOT);columns={**graph_columns,'attempts':tuple((k,'STRING')for k in ('stream','batch_id','phase','request_digest','payload_json','payload_digest')),'manifest':tuple((k,'TIMESTAMP'if k=='recorded_at'else'STRING')for k in FIELDS)}
        tables={role:'local.commerce_evolution.'+role for role in columns};targets=[]
        for role,schema in columns.items():
            path=output/role;spark.sql('CREATE TABLE delta.`'+str(path)+'` ('+','.join(k+' '+family for k,family in schema)+') USING DELTA')
            targets.append(DeltaTarget(tables[role],path,spark.sql('DESCRIBE DETAIL delta.`'+str(path)+'`').first().id))
        context=object();policy=PrivatePolicy(context,tuple(targets));transport=LocalDeltaTransport.initialize(native,output/'operations.sqlite',INSTALLATION,tuple(targets),policy,context=context);policy.initializing=False
        ports=provision_sources(output,list(fixtures.values()))
        for selected in fixtures.values():
            port=ports[selected['feed']];connection=connect(port['writer'])
            try:
                for position,(raw,batch)in enumerate(selected['batches'],1):
                    actual=connection.execute('SELECT "'+port['source']+'".append(%s,%s)',(batch.batch_id,raw.decode())).fetchone()[0]
                    if actual!=position:raise ValueError('Original contiguous source must start at zero')
                    connection.commit()
            finally:connection.close()
        def driver_for():return NativeDriver(transport,policy,context,tables,ports,admission.changes,graph_columns,source_admission=admission)
        driver=driver_for()
        def backend():
            target=driver.transport.targets[tables['attempts']]
            return StoredPublisherBackend(DeltaAttemptStore(AttemptExecutor(driver),CarrierPolicy(driver,'attempts'),target.table,target.uuid),driver,lambda request,supplied:ManifestPort(driver,request))
        prefixes={'a':0,'b':0};progress={};revisions={};state=empty_state();predecessor='explicit-private-commerce-evolution-origin';publications=[];recovery=[];all_acks=[];first=None
        graph_tables={k:tables[k]for k in graph_columns}
        for ordinal,(label,index)in enumerate(ORDER,1):
            selected=fixtures[label];port=ports[selected['feed']];connection=connect(port['reader'])
            try:
                transaction,=PostgresOutbox(Session(connection),feed=selected['feed'],epoch=selected['epoch'],schema=port['source']).read(str(index),limit=1);connection.rollback()
            finally:connection.close()
            raw,batch=selected['batches'][index]
            if transaction.batch!=batch:raise ValueError('Exact native original source bytes differ')
            previous=prefix_union(fixtures,prefixes,graph_columns)
            state,generated=driver.plan_graph(state,batch,graph_tables,materialized_at=CLOCK)
            steps,elisions=local_effect_plan(generated,previous,graph_tables);prefixes[label]=index+1;expected=prefix_union(fixtures,prefixes,graph_columns)
            revisions={**revisions,selected['source']:selected['admission'].prepared.schema_revisions[index]}
            request=request_for(STREAM,transaction,predecessor,revisions);next_progress=progress_union(progress,json.loads(outbox_checkpoint(transaction)))
            active={'request':request,'steps':steps,'expected':expected,'publication_id':'commerce-evolution-publication-'+str(ordinal),'previous_progress':progress,'progress':next_progress,'previous_expected':previous,'generated_steps':generated,'elisions':elisions};policy.active=active
            if ordinal==1:driver.lose_manifest=True
            if ordinal==2:driver.lose_ack=True
            if ordinal==3:
                insert=next(s['statement']for s in steps if s['statement'].startswith('INSERT INTO `local`.`commerce_evolution`.`object_current`'))
                native.arm(transport._render(insert))
            try:descriptor=publish_outbox_transaction(backend(),STREAM,transaction,predecessor=predecessor,schema_revisions_json=encoded(revisions),context=context)
            except (LostAfterManifest,LostNativeCommitResponse)as failure:
                # Reopen only the retained original installation/journal. Original
                # submitted native operation metadata is reconciled, never replaced.
                recovery.append({'ordinal':ordinal,'boundary':type(failure).__name__})
                transport.close();transport=None;context=object();policy=PrivatePolicy(context,tuple(targets));policy.initializing=False;policy.active=json.loads(encoded(active));policy.active.pop('manifest',None)
                transport=LocalDeltaTransport(native,output/'operations.sqlite',INSTALLATION,tuple(targets),policy);driver=driver_for()
                descriptor=publish_outbox_transaction(backend(),STREAM,transaction,predecessor=predecessor,schema_revisions_json=encoded(revisions),context=context)
            publications.append({'request':request,'manifest':dict(descriptor.raw),'expected_rows':expected,'prefixes':dict(prefixes)});progress=next_progress;predecessor=descriptor.publication_id
            if ordinal==1:first={'expected':expected,'versions':dict(descriptor.versions)}
            # Every original publication has an exact replay with no new commit.
            before={table:transport._history(transport.targets[table])for table in descriptor.versions}
            repeated=publish_outbox_transaction(backend(),STREAM,transaction,predecessor=request['predecessor'],schema_revisions_json=request['schema_revisions_json'],context=context)
            if repeated!=descriptor or any(transport._history(transport.targets[t])!=before[t]for t in descriptor.versions):raise ValueError('Exact original replay changed native versions')
            all_acks.extend(driver.acks);driver.acks=[]
        for role,rows in first['expected'].items():
            target=transport.targets[tables[role]]
            if transport._snapshot(target,first['versions'][target.table])['rows']!=sorted(rows,key=encoded):raise ValueError('Original R1 snapshot changed')
        # Hide, never delete/recreate, the retained original journal. Constructor
        # must refuse before any native mutation; restore only that same file.
        history_before={t.table:transport._history(t)for t in targets};transport.close();transport=None
        journal=output/'operations.sqlite';retained=output/'original-retained-operations.sqlite';journal_digest=hashlib.sha256(journal.read_bytes()).hexdigest();journal.rename(retained)
        refused=False
        try:
            try:
                unexpected=LocalDeltaTransport(native,journal,INSTALLATION,tuple(targets),policy);unexpected.close()
            except LocalDeltaError:refused=True
            if not refused or journal.exists():raise ValueError('Missing journal permitted replacement custody')
        finally:retained.rename(journal)
        if hashlib.sha256(journal.read_bytes()).hexdigest()!=journal_digest:raise ValueError('Original journal bytes changed during refusal control')
        transport=LocalDeltaTransport(native,journal,INSTALLATION,tuple(targets),policy);driver=driver_for()
        if any(transport._history(t)!=history_before[t.table]for t in targets):raise ValueError('Missing-journal refusal changed native history')
        acknowledgements=[]
        for selected in fixtures.values():
            port=ports[selected['feed']];connection=connect(port['role'])
            try:
                observed=[]
                for position in range(1,5):
                    row,=Session(connection).query('SELECT * FROM "'+port['scope'].service_schema+'".observe(CAST(:scope AS uuid),CAST(:position AS bigint))',{'scope':port['scope'].scope_id,'position':str(position)}).rows
                    original=publications[(position-1)*2+('a','b').index(selected['label'])]
                    if row['position']!='4'or bytes.fromhex(row['request_hex'])!=encoded(original['request']).encode()or bytes.fromhex(row['manifest_hex'])!=encoded(original['manifest']).encode():raise ValueError('Exact protected receipt/progress differs')
                    observed.append(row)
                connection.rollback();acknowledgements.append({'scope':asdict(port['scope']),'source_schema':port['source'],'source_signature_sha256':port['signature'],'observations':observed})
            finally:connection.close()
        assert_recovery_inventory(recovery,native,all_acks)
        admission.metadata()
        for target in targets:transport._detail(target)
        report={'format':'ashlar-commerce-evolution-native/0.1','qualification':__doc__,'runtime_versions':VERSIONS,'jar_sha256':{p.name:hashlib.sha256(p.read_bytes()).hexdigest()for p in jar_paths},'table_registry':[{'table':t.table,'path':str(t.path),'uuid':t.uuid}for t in targets],'admission':admission.metadata(),'publications':publications,'source_progress':progress,'acknowledgements':acknowledgements,'ack_events':all_acks,'recovery_boundaries':recovery,'native_commit_response_loss':native.events,'missing_original_journal_refused':True,'old_R1_snapshot_unchanged':True,'exact_replays_unchanged':True,'final_oracle':expected,'scope':'Same-process Spark with fresh original transport/backend/context on recoverable loss; ordinary protected PG sessions. No absent/ambiguous native submission is replaced; historical absent-commit refusal remains separately qualified.'}
    finally:close_resources(transport,spark)
    (output/'report.json').write_text(encoded(report)+'\n');return report

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',required=True,type=Path);parser.add_argument('--jars',required=True,type=Path);parser.add_argument('--umf-source',required=True,type=Path);args=parser.parse_args();result=run(args.output,args.jars,args.umf_source);print('Completed original commerce interleaved publications: '+str(len(result['publications'])))
