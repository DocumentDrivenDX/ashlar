# SPDX-License-Identifier: MIT OR Apache-2.0
import json
import base64
import hashlib
import os
from pathlib import Path
import unittest
from types import SimpleNamespace
from ashlar.protocol import ReaderProtocolProfile
from ashlar_host.managed_current_carriers import ROLES,load_current_managed_carriers
from ashlar_host.managed_current_publication import (ManagedCurrentSparkTransport,
    ManagedCurrentTargetOwner,ManagedCurrentGraphPublicationDriver,
    ManagedCurrentIncrementalPublicationDriver)

class Row(dict):
    def asDict(self,recursive=True):return dict(self)
class Frame:
    def __init__(self,rows):
        self.rows=[Row(row) for row in rows]
        self.limit_value=None
        names=list(rows[0]) if rows else []
        self.schema=SimpleNamespace(fields=[SimpleNamespace(name=n,dataType=SimpleNamespace(simpleString=lambda n=n:'string')) for n in names])
    def limit(self,count):
        self.limit_value=count
        return self
    def collect(self):return self.rows
class Spark:
    def __init__(self):
        self.conf=SimpleNamespace(get=lambda key:'true' if key=='spark.sql.legacy.parameterSubstitution.constantsOnly' else None)
        self.calls=[];self.carriers=load_current_managed_carriers();self.versions={role:0 for role in ROLES};self.manifests=[]
    def sql(self,sql,args=None):
        self.calls.append((sql,args))
        if sql.startswith('SHOW TABLES'):return Frame([{'tableName':role} for role in ROLES])
        if 'information_schema.tables' in sql:return Frame([{'table_type':'MANAGED'}])
        if sql.startswith('SELECT') and 'publication_manifest' in sql:return Frame(self.manifests)
        if sql.startswith('SELECT * FROM'):return Frame([])
        if sql.startswith('DESCRIBE DETAIL'):
            role=sql.split('`')[-2]
            return Frame([{'id':f'00000000-0000-0000-0000-{ROLES.index(role)+1:012d}','format':'delta','minReaderVersion':1,'minWriterVersion':2,'tableFeatures':[]}])
        if sql.startswith('DESCRIBE HISTORY'):
            role=sql.split('`')[-2]
            return Frame([{'version':self.versions[role]}])
        return Frame([{'value':'exact'}])
    def table(self,name):
        role=name.rsplit('.',1)[1]
        fields=[]
        for column,kind,nullable in self.carriers.ordered_fields(role):
            native={'STRING':'string','BIGINT':'long','TIMESTAMP':'timestamp','BOOLEAN':'boolean'}[kind]
            fields.append(SimpleNamespace(name=column,dataType=SimpleNamespace(simpleString=lambda n=native:n),nullable=nullable))
        return SimpleNamespace(schema=SimpleNamespace(fields=fields))

class EffectSpark(Spark):
    """Inert Delta port that applies bound INSERT payloads to memory only."""
    def __init__(self):
        super().__init__();self.data={role:[] for role in ROLES}
    def sql(self,sql,args=None):
        if sql.startswith('SELECT * FROM'):
            role=sql.split('`')[-2]
            return Frame(self.data[role])
        if sql.startswith('INSERT INTO '):
            role=sql.split('`')[-2]
            rows=json.loads(args['rows'])
            for row in rows:
                for name,kind,_nullable in self.carriers.ordered_fields(role):
                    if kind=='BIGINT' and row.get(name) is not None:row[name]=int(row[name])
                    if kind=='TIMESTAMP' and row.get(name) is not None:
                        from datetime import datetime,timezone
                        value=datetime.fromisoformat(row[name].replace('Z','+00:00'))
                        if value.tzinfo is None:value=value.replace(tzinfo=timezone.utc)
                        row[name]=value.astimezone(timezone.utc)
            self.data[role].extend(rows)
            self.versions[role]+=1
            self.calls.append((sql,args))
            return Frame([])
        return super().sql(sql,args)

class RuntimeSpark(Spark):
    version='4.2.0'
    def __init__(self):
        super().__init__();self.runtime_tag='16.4.x-scala2.13';self.principal='trial-principal'
    @property
    def conf(self):
        return SimpleNamespace(get=lambda key:self.runtime_tag if key=='spark.databricks.clusterUsageTags.sparkVersion' else None)
    @conf.setter
    def conf(self,value):pass
    def sql(self,sql,args=None):
        if 'current_user()' in sql:
            return Frame([{'principal':self.principal,'catalog':'client_dev','schema':'trial_schema'}])
        if 'version() AS sparkBuildText' in sql:
            return Frame([{'sparkBuildText':'Spark SQL build text'}])
        return super().sql(sql,args)


def _nonempty_current_admission():
    """Load the retained actual accepted Truss xid809 page from private evidence."""
    evidence_root=os.environ.get('ASHLAR_CURRENT_NATIVE_PRIVATE_EVIDENCE')
    if not evidence_root:
        raise RuntimeError('Set ASHLAR_CURRENT_NATIVE_PRIVATE_EVIDENCE to opt into the retained-page oracle')
    root=Path(evidence_root)
    archive=json.loads((root/'lifecycle-page-retry-original.json').read_text())
    page=base64.b64decode(archive['content'],validate=True)
    if hashlib.sha256(page).hexdigest()!='e4c606d3eb7a5c93cdc66416221d6024494ae84a9e5875b9158ff8d0334bec35':
        raise AssertionError('Retained actual Truss page digest changed')
    payload=json.loads(page)
    prerequisite=next(item for item in payload['catalogPrerequisites'] if item.get('reportBytesBase64'))
    report=base64.b64decode(prerequisite['reportBytesBase64'],validate=True)
    view_path=root/'accepted-current-r1-attempt3-catalog-view.json'
    source_path=root/'accepted-current-r1-attempt3-source.umf.json'
    view_raw=view_path.read_bytes();source_raw=source_path.read_bytes()
    if (hashlib.sha256(report).hexdigest()!='0c24c223606489a2d5df37ed19744ae5f4ed065c39c44c956381abce47b5ec63'
            or hashlib.sha256(view_raw).hexdigest()!='9857e981688334c76ab72524e25d67e7fec0376e8b97a3451b550aaa04b16796'
            or hashlib.sha256(source_raw).hexdigest()!='19ab3cc44efd769346b88fb7c5dc6792b59fd0057ed8846e4a0eec744f8e716b'):
        raise AssertionError('Retained exact accepted Truss catalog evidence digest changed')
    report_obj=json.loads(report);doc=report_obj['acceptedInput']['documents'][0]
    source=SimpleNamespace(document_id=doc['documentId'],document_revision=doc['documentRevision'],
        artifact_identity=doc['artifact']['identity'],registration_id=report_obj['registrationId'],
        sha256=doc['artifact']['sha256'],content=source_raw,catalog_revision=int(prerequisite['revision']))
    from ashlar.truss_native_feed import native_feed_batch
    prior=json.dumps(payload['prior'],separators=(',',':')).encode()
    following=json.dumps(payload['next'],separators=(',',':')).encode()
    batch=native_feed_batch(page,sha256=hashlib.sha256(page).hexdigest(),prior=prior,next=following,
                            feed=archive['feed'])
    from ashlar_host.truss_current_catalog import CurrentCatalogEvidence
    evidence=CurrentCatalogEvidence('0.0.44',{prerequisite['revision']:report},
        {prerequisite['revision']:view_raw},{(prerequisite['revision'],doc['documentId']):source})
    from ashlar_host.truss_graph_admission import TrussGraphAdmission
    return TrussGraphAdmission(batch,evidence)

class Tests(unittest.TestCase):
    def test_task_environment_records_separate_public_runtime_observations(self):
        from ashlar_host.managed_current_publication import observe_managed_current_environment
        spark=RuntimeSpark();profile=ReaderProtocolProfile('test-protocol',(1,),(2,),())
        opening=observe_managed_current_environment(spark,profile)
        self.assertEqual(opening['sparkVersion'],'4.2.0')
        self.assertEqual(opening['sparkBuildText'],'Spark SQL build text')
        self.assertEqual(opening['runtimeTag'],'16.4.x-scala2.13')
        self.assertNotIn('buildId',opening)
        spark.runtime_tag='changed-runtime'
        self.assertNotEqual(observe_managed_current_environment(spark,profile),opening)
        spark.principal=None
        with self.assertRaisesRegex(ValueError,'actor'):
            observe_managed_current_environment(spark,profile)

    def test_parameter_bindings_use_whole_collect_schema_path(self):
        spark=Spark();transport=ManagedCurrentSparkTransport(spark)
        result=transport.query('SELECT :value AS value',{'value':"x' OR 1=1 --"})
        self.assertEqual(result.rows,({'value':'exact'},))
        self.assertEqual(result.columns,(('value','string'),))
        self.assertEqual(spark.calls[-1],('SELECT :value AS value',{'value':"x' OR 1=1 --"}))

    def test_six_role_inventory_and_four_role_data_vector_stay_distinct(self):
        spark=Spark();profile=ReaderProtocolProfile('test-current-managed-reader',(1,),(2,),())
        owner=ManagedCurrentTargetOwner(spark,catalog='client_dev',schema='trial_schema',protocol_profile=profile)
        opening=owner.open_existing()
        self.assertEqual(set(opening),set(ROLES))
        data_vector=owner.publication_data_vector(opening)
        self.assertEqual(len(data_vector),4)
        self.assertEqual(set(name.rsplit('.',1)[1] for name in data_vector),
                         {'object_current','edge_current','tombstone','whole_source_history'})
        closing=owner.closing()
        self.assertEqual({role:item.uuid for role,item in closing.items()},
                         {role:item.uuid for role,item in opening.items()})

    def test_current_truss_page_plan_is_canonical_and_reconstructs_exactly(self):
        import hashlib
        from tests.test_managed_current_core import _current_admission
        from ashlar_host.publication_planning import truss_request_for
        admission=_current_admission();spark=Spark()
        profile=ReaderProtocolProfile('test-current-managed-reader',(1,),(2,),())
        target=ManagedCurrentTargetOwner(spark,catalog='client_dev',schema='trial_schema',protocol_profile=profile)
        target.open_existing();context=object()
        driver=ManagedCurrentGraphPublicationDriver(target,admission,stream='dev-feed',
            materialized_at='1',context=context)
        request=truss_request_for('dev-feed',admission.batch,'managed-current-genesis/0.1',driver.revisions)
        driver.prepare_truss_apply(request,context)
        retained=driver.prepared_plan(request,context)
        self.assertEqual(json.loads(retained)['profile'],'ashlar-managed-current-exact-plan/0.1')
        self.assertEqual(json.loads(retained)['sourcePageSha256'],hashlib.sha256(admission.batch.begin).hexdigest())
        self.assertEqual(json.loads(retained)['steps'],[])

        replay=ManagedCurrentGraphPublicationDriver(target,admission,stream='dev-feed',
            materialized_at='1',context=context)
        self.assertIsNone(replay.load_prepared_plan(request,retained,context))
        self.assertEqual(replay.plan_json,retained)
        self.assertEqual(replay.plan['request'],request)

    def _predecessor_fixture(self, plan_profile='ashlar-managed-current-incremental-plan/0.1'):
        roles=ROLES
        data_roles=('object_current','edge_current','tombstone','whole_source_history')
        names={role:'client_dev.trial_schema.'+role for role in roles}
        request={'stream':'dev-feed','batch_id':'batch-2','predecessor':'prior-publication',
            'source_batch_digest':'a'*64,'source_checkpoint_json':'{}','schema_revisions_json':'{}'}
        request['request_digest']=hashlib.sha256(json.dumps(request,sort_keys=True,separators=(',',':')).encode()).hexdigest()
        canonical=lambda value:json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False)
        inventory={role:{'name':names[role],'uuid':role+'-uuid','version':0,'schema':[['id','string',False]],'protocol':{'reader':1}}
                   for role in roles}
        request_anchor={key:request[key] for key in ('request_digest','stream','batch_id','predecessor',
            'source_batch_digest','source_checkpoint_json','schema_revisions_json')}
        predecessor={'publication_id':'prior-publication','profile_version':'ashlar-managed-current-incremental/0.1',
            'table_versions_json':canonical({names[role]:0 for role in data_roles}),'source_progress_json':'{}'}
        intent={'profile':'ashlar-managed-current-incremental/0.1','request':request_anchor,
            'predecessorManifest':predecessor,'fourRoleHeads':{names[role]:0 for role in data_roles},
            'sixRoleInventory':inventory,'touchedKeys':[],
            'expectedAffectedAfterimages':{'rows':{role:[] for role in data_roles}},
            'rawDeliveryDigests':[],'effects':[],'sourceProgressTransition':{}}
        plan={'profile':plan_profile,'requestAnchor':request_anchor,'sourceAdmission':{},'sourcePageSha256':'b'*64,
            'publicationId':'ashlar-current-incremental:'+hashlib.sha256(request['request_digest'].encode()).hexdigest(),'recordedAt':'1',
            'baselineInstallation':{role:{'name':names[role],'uuid':inventory[role]['uuid'],'version':0} for role in roles},
            'predecessorManifest':predecessor,'intent':intent,'steps':[],
            'expectedRows':{role:[] for role in data_roles}}
        plan_text=canonical(plan)
        report={'complete':True,'request_digest':request['request_digest'],'predecessor':'prior-publication',
            'publication_plan_profile':plan_profile,'original_plan_sha256':hashlib.sha256(plan_text.encode()).hexdigest(),
            'effect_rows_sha256':hashlib.sha256(canonical(plan['expectedRows']).encode()).hexdigest()}
        manifest={'publication_id':plan['publicationId'],'profile_version':'ashlar-delta/0.3',
            'table_versions_json':canonical({names[role]:0 for role in data_roles}),
            'schema_revisions_json':request['schema_revisions_json'],'source_progress_json':'{}',
            'validation_report_json':canonical(report),'recorded_at':'1'}
        result={'effects':{'profile':'ashlar-managed-current-incremental-effect-proof/0.1',
            'intentSha256':hashlib.sha256(plan_text.encode()).hexdigest(),
            'affectedRowsSha256':report['effect_rows_sha256']},'manifest':manifest}
        payload={'request':request,'result_json':canonical(result),'descriptor_json':canonical(manifest),'plan_json':plan_text}
        payload_text=canonical(payload)
        row={'stream':request['stream'],'batch_id':request['batch_id'],'phase':'committed',
            'request_digest':request['request_digest'],'payload_json':payload_text,
            'payload_digest':hashlib.sha256(payload_text.encode()).hexdigest()}
        current={role:SimpleNamespace(name=names[role],uuid=inventory[role]['uuid'],version=0,
                    schema=(('id','string',False),),protocol={'reader':1}) for role in roles}
        driver=object.__new__(ManagedCurrentIncrementalPublicationDriver)
        driver.names=names;driver.table_roles=data_roles
        return driver,manifest,report,current,row

    def test_incremental_predecessor_refuses_historical_exact_plan(self):
        driver,manifest,report,current,row=self._predecessor_fixture(
            'ashlar-managed-current-exact-plan/0.1')
        with self.assertRaisesRegex(ValueError,'Exact versioned incremental predecessor intent'):
            driver._validate_predecessor_attempt(manifest,report,current,row)

    def test_incremental_predecessor_refuses_replaced_uuid_at_same_version(self):
        driver,manifest,report,current,row=self._predecessor_fixture()
        driver._validate_predecessor_attempt(manifest,report,current,row)
        current['object_current'].uuid='replacement-uuid'
        with self.assertRaisesRegex(ValueError,'six-role UUID'):
            driver._validate_predecessor_attempt(manifest,report,current,row)

    def test_incremental_empty_page_uses_compact_intent_and_four_role_manifest(self):
        from tests.test_managed_current_core import _current_admission
        from ashlar_host.publication_planning import truss_request_for
        admission=_current_admission();spark=EffectSpark()
        profile=ReaderProtocolProfile('test-current-managed-reader',(1,),(2,),())
        target=ManagedCurrentTargetOwner(spark,catalog='client_dev',schema='trial_schema',protocol_profile=profile)
        target.open_existing();context=object()
        driver=ManagedCurrentIncrementalPublicationDriver(target,admission,stream='dev-feed',
            materialized_at='1',context=context)
        request=truss_request_for('dev-feed',admission.batch,driver.GENESIS,driver.revisions)
        driver.prepare_truss_apply(request,context)
        plan=json.loads(driver.prepared_plan(request,context))
        self.assertEqual(plan['profile'],'ashlar-managed-current-incremental-plan/0.1')
        self.assertEqual(plan['intent']['profile'],'ashlar-managed-current-incremental/0.1')
        self.assertNotIn('baselineDataRows',plan)
        self.assertNotIn('expectedRows',plan['intent'])
        self.assertEqual(plan['intent']['fourRoleHeads'],
            {target.tables[role]:0 for role in ('object_current','edge_current','tombstone','whole_source_history')})
        with driver.writer('dev-feed',context):
            result=json.loads(driver.apply(request,context))
        self.assertEqual(result['effects']['profile'],'ashlar-managed-current-incremental-effect-proof/0.1')
        self.assertEqual(json.loads(driver.manifest['table_versions_json']),
            {target.tables[role]:0 for role in ('object_current','edge_current','tombstone','whole_source_history')})
        self.assertEqual(spark.data['whole_source_history'],[])
        selected_data=[sql for sql,_args in spark.calls if sql.startswith('SELECT')
                       and any(target.tables[role] in sql for role in driver.table_roles)]
        self.assertTrue(all(' WHERE ' in sql for sql in selected_data))
        self.assertFalse(any(sql.startswith('SELECT * FROM') for sql,_args in spark.calls))
        with driver.writer('dev-feed',context):
            with self.assertRaisesRegex(ValueError,'no resubmission is allowed'):
                driver.recover_apply(request,context)

    @unittest.skipUnless(os.environ.get('ASHLAR_CURRENT_NATIVE_PRIVATE_EVIDENCE'),
        'retained actual xid809 page requires explicit private-evidence opt-in')
    def test_actual_truss_xid809_page_has_exact_net_effects_and_committed_replay(self):
        from ashlar_host.publication_planning import truss_request_for
        from ashlar.source_checkpoint import truss_checkpoint
        admission=_nonempty_current_admission();spark=EffectSpark()
        self.assertEqual(len(admission.changes),7)
        self.assertEqual([change.delivery_id for change in admission.changes],
            ['809:1','809:2','809:3','809:5','809:6','809:7','809:8'])
        profile=ReaderProtocolProfile('test-current-managed-reader',(1,),(2,),())
        target=ManagedCurrentTargetOwner(spark,catalog='client_dev',schema='trial_schema',protocol_profile=profile)
        target.open_existing()
        checkpoint=json.loads(truss_checkpoint(admission.batch))
        vector={target.tables[role]:0 for role in ('object_current','edge_current','tombstone','whole_source_history')}
        spark.manifests=[{'publication_id':'prior-publication','source_progress_json':json.dumps(
            {checkpoint['feed']:{'epoch':checkpoint['epoch'],'position':checkpoint['previous']}},
            sort_keys=True,separators=(',',':')),
            'table_versions_json':json.dumps(vector,sort_keys=True,separators=(',',':')),
            'recorded_at':'2026-10-21T00:00:00.000000Z'}]
        context=object();driver=ManagedCurrentGraphPublicationDriver(target,admission,
            stream=admission.batch.feed,materialized_at='1791547200000000',context=context)
        request=truss_request_for(admission.batch.feed,admission.batch,'prior-publication',driver.revisions)
        driver.prepare_truss_apply(request,context)
        plan=json.loads(driver.plan_json)
        self.assertEqual(plan['previousProgress'][admission.batch.feed],
            {'epoch':checkpoint['epoch'],'position':checkpoint['previous']})
        # Five planner statements include two empty-target MERGE elisions;
        # the three retained INSERTs are the exact net current/tombstone/history state.
        self.assertEqual(len(plan['steps'])+len(plan['zeroMatchElisions']),5)
        self.assertEqual(len(plan['zeroMatchElisions']),2)
        self.assertEqual([step['statement'].split(' ',2)[0] for step in plan['steps']],
            ['INSERT','INSERT','INSERT'])
        row=plan['expectedRows']['object_current'][0]
        self.assertEqual((row['type_id'],row['id'],row['entity_version']),(1,1,2))
        self.assertEqual(row['props_json'],
            '{"1":"native-lifecycle-person-20261021","2":"Native Lifecycle Person","3":9007199254740993}')
        self.assertEqual(plan['expectedRows']['edge_current'],[])
        self.assertEqual({(row['entity_kind'],row['type_id'],row['id'],row['entity_version'])
            for row in plan['expectedRows']['tombstone']},
            {('edge',2,3,3),('object',2,2,2)})
        self.assertEqual([row['delivery_id'] for row in plan['expectedRows']['whole_source_history']],
            ['809:1','809:2','809:3','809:5','809:6','809:7','809:8'])
        with driver.writer(admission.batch.feed,context):
            effect_proof=json.loads(driver.apply(request,context))
        self.assertEqual(len(effect_proof['effects']['planSha256']),64)
        self.assertEqual({role:len(spark.data[role]) for role in driver.table_roles},
            {'object_current':1,'edge_current':0,'tombstone':2,'whole_source_history':7})
        manifest=driver.manifest
        expected_manifest_vector={target.tables[role]:spark.versions[role]
            for role in ('object_current','edge_current','tombstone','whole_source_history')}
        self.assertEqual(json.loads(manifest['table_versions_json']),expected_manifest_vector)
        self.assertEqual(json.loads(manifest['source_progress_json'])[admission.batch.feed],checkpoint)
        replay_spark=EffectSpark();replay_spark.manifests=[manifest]
        replay_spark.data={role:list(rows) for role,rows in spark.data.items()}
        replay_spark.versions.update(spark.versions)
        replay_spark.versions['publication_manifest']=2
        replay_spark.versions['publication_attempt_phase']=3
        replay_target=ManagedCurrentTargetOwner(replay_spark,catalog='client_dev',schema='trial_schema',protocol_profile=profile)
        replay_target.open_existing()
        replay=ManagedCurrentGraphPublicationDriver(replay_target,admission,stream=admission.batch.feed,
            materialized_at='1791547200000000',context=context)
        from types import SimpleNamespace
        self.assertIsNone(replay.prepare_truss_observation(request,context,SimpleNamespace(phase='committed')))
        self.assertIsNone(replay.load_prepared_plan(request,driver.plan_json,context))
        self.assertEqual(replay.plan_json,driver.plan_json)
        self.assertIsNone(replay.validate_prepared_plan(request,driver.plan_json,context))

    def test_task_requires_canonical_schema_vector_before_touching_spark(self):
        from ashlar_host.managed_current_core import ManagedCurrentCoreConfig
        from ashlar_host.managed_current_publication import run_managed_current_publication_task
        profile=ReaderProtocolProfile('test-current-managed-reader',(1,),(2,),())
        config=ManagedCurrentCoreConfig('client_dev','trial_schema',None,None,10,100)
        environment={key:'selected' for key in ('runtimeName','sparkVersion','sparkBuildText',
            'runtimeTag','principal','catalog','schema','protocol')}
        environment['catalog']='client_dev';environment['schema']='trial_schema'
        with self.assertRaisesRegex(ValueError,'Canonical complete current source schema'):
            run_managed_current_publication_task(object(),binding={},core_config=config,
                protocol_profile=profile,stream='feed',predecessor='genesis',
                schema_revisions_json='{"1": "pin"}',materialized_at='1',expected_environment=environment)

if __name__=='__main__':unittest.main()
