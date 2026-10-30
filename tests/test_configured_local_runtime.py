# SPDX-License-Identifier: MIT OR Apache-2.0
"""Inert Spark metadata ports and actual private lease/source lifecycle only."""
from dataclasses import replace
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
import hashlib,json,tempfile,unittest,uuid
from types import MappingProxyType
from unittest.mock import patch,MagicMock
import test_configured_source_admission as fixtures
from ashlar_host.configured_local_runtime import ConfiguredLocalRuntimeConfig,ConfiguredLocalAdmission,initialize_configured_local,publish_configured_local,read_configured_local,run_configured_local_delta4_transaction1,_fields
from ashlar_host.typed_graph_sdk058_admission import TypedGraphSdk058AdmissionConfig
from ashlar_host.delta_custody import DeltaTarget

class Row(dict):
    def asDict(self,recursive=False):return dict(self)
class Frame:
    def __init__(self,rows):self.rows=rows
    def limit(self,n):return Frame(self.rows[:n])
    def collect(self):return self.rows
class Spark:
    def __init__(self):
        self.closed=False;self.detail_reads=0;self.details={};self.fields=_fields();self.version='4.2.0'
        system=SimpleNamespace(getProperty=lambda key:'21')
        self.sparkContext=SimpleNamespace(master='local[1]',applicationId='inert-session',setLogLevel=lambda level:None,_jvm=SimpleNamespace(java=SimpleNamespace(lang=SimpleNamespace(System=system))))
        self.conf=SimpleNamespace(get=lambda key:'512m'if key=='spark.driver.memory'else ''if key=='spark.jars'else 'true'if key=='spark.sql.legacy.parameterSubstitution.constantsOnly'else 'zstd'if key=='spark.sql.parquet.compression.codec'else 'UTC');self.read=self
    def sql(self,sql):
        path=sql.split('delta.`')[1].split('`')[0];role=Path(path).name
        if sql.startswith('CREATE'):
            Path(path).mkdir()
            self.details[path]={'id':str(uuid.uuid5(uuid.NAMESPACE_URL,path)),'location':path,'format':'delta','minReaderVersion':1,'minWriterVersion':2,'tableFeatures':[],'properties':{}}
            self.assert_no_reuse=path;return Frame([])
        self.detail_reads+=1;return Frame([Row(self.details[path])])
    def format(self,name):return self
    def load(self,path):
        fields=[SimpleNamespace(name=n,dataType=SimpleNamespace(typeName=lambda f=f:{'STRING':'string','BIGINT':'long','TIMESTAMP':'timestamp'}[f]),nullable=nullable)for n,f,nullable in self.fields[Path(path).name]]
        return SimpleNamespace(schema=SimpleNamespace(fields=fields))
    def stop(self):self.closed=True

class Tests(unittest.TestCase):
    def test_sdk058_runtime_config_refuses_unqualified_reader_and_publish_routes_pre_runtime(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory).resolve();root.chmod(0o700)
            config=self.config(root)
            sdk=TypedGraphSdk058AdmissionConfig(
                node=root/'node',sdk_package_root=root/'sdk',sdk_archive=root/'sdk.tgz',
                producer_closure=root/'closure.json',producer_receipt=root/'producer.json',
                install_custody=root/'install.json',source_admission_manifest=root/'manifest.json')
            selected=replace(config,carrier_profile='ashlar-delta/0.4-xxhash64',
                installation_sha256='0'*64,sdk058_admission=sdk)
            with patch('ashlar_host.configured_local_runtime._runtime') as runtime:
                with self.assertRaises(ValueError):publish_configured_local(selected,ordinal=1)
            runtime.assert_not_called()

    def config(self,root,carrier_profile='ashlar-delta/0.3'):
        admission=fixtures.Tests().fixture(root)
        output=root/'output';admission=replace(admission,output=output/'admission')
        inventory={'delta-spark_4.2_2.13-4.4.1.jar':'a'*64,'delta-storage-4.4.1.jar':'b'*64}
        config=ConfiguredLocalRuntimeConfig(root/'installation',output,root/'jars',root/'java',admission,'configured-original','1791547200000000',None,'original-local-installation',None,inventory,carrier_profile)
        inventory['delta-spark_4.2_2.13-4.4.1.jar']='c'*64
        self.assertEqual(config.jar_inventory['delta-spark_4.2_2.13-4.4.1.jar'],'a'*64)
        self.assertIsInstance(config.jar_inventory,MappingProxyType)
        return config
    def test_initialize_actual_private_lease_registry_and_close_before_report(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();root.chmod(0o700);config=self.config(root);spark=Spark();transport=SimpleNamespace(close=lambda:closed.append('transport'));closed=[]
            owners=[]
            def owner(*a,**kw):
                current=ConfiguredLocalAdmission(*a,**kw);owners.append(current);return current
            def initialize(*a,**kw):
                current=owners[0];before=spark.detail_reads
                for _ in range(3):current(current.source,None,None,current.context)
                self.assertEqual(spark.detail_reads,before)
                return transport
            with patch('ashlar_host.configured_source_admission.admit_managed_records',return_value=(b'{}',)),patch('ashlar_host.configured_local_runtime._runtime',return_value=[]),patch('ashlar_host.configured_local_runtime.attest_runtime',return_value=('profile',{'python':'3.12'},[])),patch('ashlar_host.configured_local_runtime.verify_loaded_delta_class'),patch('ashlar_host.configured_local_runtime.open_finite_spark',return_value=spark),patch('ashlar_host.configured_local_runtime.ConfiguredLocalAdmission',side_effect=owner),patch('ashlar_host.configured_local_runtime.LocalDeltaTransport.initialize',side_effect=initialize):
                result=initialize_configured_local(config)
            self.assertEqual(result['operation'],'initialize');self.assertFalse(result['published']);self.assertFalse(result['protected_ack'])
            self.assertEqual(len(result['installation']['targets']),6);self.assertTrue(spark.closed);self.assertEqual(closed,['transport'])
            registry=(config.installation/'installation.json').read_bytes();self.assertEqual(hashlib.sha256(registry).hexdigest(),result['installation_sha256'])
            self.assertEqual(json.loads((config.output/'runtime-report.json').read_bytes()),result)
            self.assertFalse((config.output/'report.json').exists())
            self.assertEqual(spark.detail_reads,18) #6create identities+6opening+6mandatoryclosing
            # Route a retained read through the existing registry, never the publisher.
            read_config=replace(config,output=root/'read-output',admission=replace(config.admission,output=root/'read-output/admission'),installation_sha256=result['installation_sha256'])
            from test_manifest import ROW
            from ashlar.publication import descriptor_from_manifest_row
            progress=SimpleNamespace(retained_entries=lambda:(SimpleNamespace(descriptor=descriptor_from_manifest_row(ROW)),),position=lambda:'1828',completed_batches=lambda:3)
            current=MagicMock(return_value=None);current.closing.return_value=None;spark.closed=False
            retained_row={'id':'1','retained_json':'x'*300000}
            with patch('ashlar_host.configured_source_admission.admit_managed_records',return_value=(b'{}',)),patch('ashlar_host.configured_local_runtime._runtime',return_value=[]),patch('ashlar_host.configured_local_runtime.attest_runtime',return_value=('profile',{'python':'3.12'},[])),patch('ashlar_host.configured_local_runtime.verify_loaded_delta_class'),patch('ashlar_host.configured_local_runtime.open_finite_spark',return_value=spark),patch('ashlar_host.configured_local_runtime.ConfiguredLocalAdmission',return_value=current),patch('ashlar_host.configured_local_runtime.LocalDeltaTransport',return_value=transport),patch('ashlar_host.file_progress.JournaledJsonlProgress.open_existing',return_value=progress),patch('ashlar_host.configured_local_read.read_configured_local_singleton',return_value=retained_row)as read,patch('ashlar_host.configured_local_runtime.ConfiguredLocalPublicationDriver')as publisher:
                answer=read_configured_local(read_config,ordinal=1,type_id=1017,entity_id=1)
            publisher.assert_not_called();self.assertEqual(read.call_args.kwargs['ordinal'],1)
            self.assertEqual(answer['row'],retained_row);self.assertGreater((read_config.output/'runtime-report.json').stat().st_size,262144);self.assertEqual(answer['local_checkpoint'],'1828');self.assertEqual(answer['completed_batches'],3)
            self.assertTrue(spark.closed);current.closing.assert_called_once()
    def test_native_schema_drift_refuses_success_and_still_stops_session(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();root.chmod(0o700);config=self.config(root);spark=Spark();spark.fields={**spark.fields,'object_current':(('foreign','STRING',True),)}
            with patch('ashlar_host.configured_source_admission.admit_managed_records',return_value=(b'{}',)),patch('ashlar_host.configured_local_runtime._runtime',return_value=[]),patch('ashlar_host.configured_local_runtime.attest_runtime',return_value=('profile',{'python':'3.12'},[])),patch('ashlar_host.configured_local_runtime.verify_loaded_delta_class'),patch('ashlar_host.configured_local_runtime.open_finite_spark',return_value=spark),patch('ashlar_host.configured_local_runtime.LocalDeltaTransport.initialize')as initialize:
                with self.assertRaises(ValueError):initialize_configured_local(config)
            self.assertTrue(spark.closed);initialize.assert_not_called();self.assertFalse((config.output/'runtime-report.json').exists())

    def test_initialized_runtime_refuses_changed_parameter_binding_setting(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();root.chmod(0o700);config=self.config(root);spark=Spark()
            spark.conf=SimpleNamespace(get=lambda key:'512m'if key=='spark.driver.memory'else ''if key=='spark.jars'else 'false'if key=='spark.sql.legacy.parameterSubstitution.constantsOnly'else 'UTC')
            with patch('ashlar_host.configured_source_admission.admit_managed_records',return_value=(b'{}',)),patch('ashlar_host.configured_local_runtime._runtime',return_value=[]),patch('ashlar_host.configured_local_runtime.attest_runtime',return_value=('profile',{'python':'3.12'},[])),patch('ashlar_host.configured_local_runtime.verify_loaded_delta_class'),patch('ashlar_host.configured_local_runtime.open_finite_spark',return_value=spark),patch('ashlar_host.configured_local_runtime.LocalDeltaTransport.initialize')as initialize:
                with self.assertRaises(ValueError):initialize_configured_local(config)
            self.assertTrue(spark.closed);initialize.assert_not_called();self.assertFalse((config.installation/'object_current').exists())

    def test_xxhash64_initialize_uses_separate_umf_realization_and_binds_six_role_identity(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();root.chmod(0o700);config=self.config(root,'ashlar-delta/0.4-xxhash64')
            spark=Spark();spark.fields=_fields('ashlar-delta/0.4-xxhash64')
            targets=[]
            def realize(_spark,installation,tables,*,table_prefix):
                self.assertEqual(table_prefix,'local.configured')
                for role,table in tables.items():
                    path=installation/role;path.mkdir()
                    uuid_value=str(uuid.uuid5(uuid.NAMESPACE_URL,str(path)))
                    spark.details[str(path)]={'id':uuid_value,'location':str(path),'format':'delta','minReaderVersion':1,'minWriterVersion':2,'tableFeatures':[], 'properties':{}}
                    targets.append(DeltaTarget(table,path,uuid_value))
                by_table={target.table:target for target in targets}
                profile={'profile':'ashlar-local-current-installation-profile/0.1','physical_profile':'ashlar-delta/0.4-xxhash64',
                    'lookup_hash_profile':'spark-xxhash64-typed-key/0.1','generated_carriers_sha256':'a'*64,'umf_revision':'b'*40,
                    'local_realization':'spark-delta4/0.1','umf_realization_profile':'ashlar-local-current-delta4-umf-realization/0.1',
                    'umf_candidate_commit':'c'*40,'umf_realization_manifest_sha256':'d'*64,'umf_bridge_sha256':'e'*64,
                    'umf_build_inputs_sha256':'f'*64,'writer_settings':[{'key':'spark.sql.parquet.compression.codec','value':'zstd'}],
                    'roles':{role:{'carrier_role':{'attempts':'publication_attempt_phase','manifest':'publication_manifest'}.get(role,role),'table':target.table,'path':str(target.path),'uuid':target.uuid,
                        'fields':[],'field_metadata':[],'protocol':{},'clustering_columns':[],'partition_columns':[],
                        'layout_properties':{},'umf_source_bytes_sha256':'1'*64,'umf_source_sha256':'2'*64,
                        'umf_schema_sha256':'3'*64,'umf_target_sha256':'4'*64,'umf_proposal_sha256':'5'*64,
                        'umf_ddl_sha256':'6'*64,'umf_dispositions':[],'umf_writer_settings':[], 'umf_proposal':{}}
                        for role,target in zip(tables,targets)}}
                return targets,profile,{}
            transport=SimpleNamespace(close=lambda:None)
            with patch('ashlar_host.configured_source_admission.admit_managed_records',return_value=(b'{}',)),patch('ashlar_host.configured_local_runtime._runtime',return_value=[]),patch('ashlar_host.configured_local_runtime.attest_runtime',return_value=('runtime',{'python':'3.12'},[])),patch('ashlar_host.configured_local_runtime.verify_loaded_delta_class'),patch('ashlar_host.configured_local_runtime.open_finite_spark',return_value=spark),patch('ashlar_host.configured_local_runtime.LocalDeltaTransport.initialize',return_value=transport)as initialize_transport,patch('ashlar_host.local_current_carrier_profile.create_local_current_xxhash64_tables',side_effect=realize):
                result=initialize_configured_local(config)
            self.assertEqual(result['profile'],'ashlar-configured-local-initialized/0.3')
            self.assertFalse(result['published']);self.assertFalse(result['protected_ack'])
            registry=result['installation'];self.assertEqual(registry['profile'],'ashlar-configured-local-installation/0.3')
            self.assertEqual(registry['physical_profile'],'ashlar-delta/0.4-xxhash64')
            self.assertEqual(len(registry['targets']),6)
            self.assertEqual(len({item['uuid'] for item in registry['targets']}),6)
            profile_raw=(config.installation/'carrier-profile.json').read_bytes()
            self.assertEqual(hashlib.sha256(profile_raw).hexdigest(),registry['carrier_profile_sha256'])
            self.assertEqual(json.loads(profile_raw)['lookup_hash_profile'],'spark-xxhash64-typed-key/0.1')
            self.assertEqual(initialize_transport.call_args.kwargs['installation_profile'],json.loads(profile_raw))
            self.assertTrue(spark.closed)

    def test_xxhash64_continuation_is_refused_before_output_creation(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();root.chmod(0o700)
            config=self.config(root,'ashlar-delta/0.4-xxhash64')
            config=replace(config,installation_sha256='f'*64)
            with self.assertRaises(ValueError):publish_configured_local(config,ordinal=1)
            self.assertFalse(config.output.exists())

    def test_delta4_transaction1_composition_refuses_wrong_profile_before_output(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();root.chmod(0o700)
            config=self.config(root,'ashlar-delta/0.3')
            config=replace(config,installation_sha256='a'*64)
            with self.assertRaises(ValueError):run_configured_local_delta4_transaction1(config)
            self.assertFalse(config.output.exists())

    def test_delta4_transaction1_composition_closes_owners_if_spark_open_fails(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();root.chmod(0o700)
            config=self.config(root,'ashlar-delta/0.4-xxhash64')
            roles=('object_current','edge_current','tombstone','whole_source_history','attempts','manifest')
            config.installation.mkdir(mode=0o700)
            for role in roles:(config.installation/role).mkdir(mode=0o700)
            columns={role:() for role in roles}
            scope={'pinned':'g03-source-scope'}
            targets=[{'table':'local.configured.'+role,'path':str(config.installation/role),
                      'uuid':str(uuid.uuid5(uuid.NAMESPACE_URL,role))}for role in roles]
            carrier={'profile':'ashlar-local-current-installation-profile/0.1',
                     'physical_profile':'ashlar-delta/0.4-xxhash64',
                     'roles':{role:{'carrier_role':{'attempts':'publication_attempt_phase','manifest':'publication_manifest'}.get(role,role),
                                 'uuid':targets[index]['uuid']}for index,role in enumerate(roles)}}
            carrier_raw=(json.dumps(carrier,separators=(',',':'))+'\n').encode()
            registry={'profile':'ashlar-configured-local-installation/0.3','scope':scope,
                'targets':targets,'columns':{role:[] for role in roles},
                'runtime_identity':{'profile':'runtime','python':'3.12'},
                'physical_profile':'ashlar-delta/0.4-xxhash64',
                'carrier_profile_sha256':hashlib.sha256(carrier_raw).hexdigest()}
            registry_raw=json.dumps(registry,separators=(',',':')).encode()
            registry_file=SimpleNamespace(raw=registry_raw,renew=lambda _limit:events.append('registry'))
            carrier_file=SimpleNamespace(raw=carrier_raw,renew=lambda _limit:events.append('carrier'))
            events=[]
            lease=SimpleNamespace(renew=lambda:events.append('lease-renew'),
                                  close=lambda:events.append('lease-close'))
            source=SimpleNamespace(graph=True,context=None,renew=lambda _context:events.append('source-renew'))
            class SourceManager:
                def __enter__(self):return source
                def __exit__(self,*_args):events.append('source-close')
            with (patch('ashlar_host.configured_local_runtime._runtime',return_value=[]),
                  patch('ashlar_host.configured_local_runtime._scope',return_value=scope),
                  patch('ashlar_host.configured_local_runtime._columns',return_value=columns),
                  patch('ashlar_host.configured_local_runtime.attest_runtime',return_value=('runtime',{'python':'3.12'},[])),
                  patch('ashlar_host.configured_local_runtime.InvocationFile.read',side_effect=[registry_file,carrier_file]),
                  patch('ashlar_host.configured_local_runtime.PrivateFiniteLease',return_value=lease),
                  patch('ashlar_host.configured_local_runtime.open_configured_local_source',
                        side_effect=lambda _admission,stream,context:(setattr(source,'context',context) or SourceManager())),
                  patch('ashlar_host.configured_local_runtime.open_finite_spark',side_effect=RuntimeError('inert open failure'))):
                with self.assertRaisesRegex(RuntimeError,'inert open failure'):
                    run_configured_local_delta4_transaction1(
                        replace(config,installation_sha256=hashlib.sha256(registry_raw).hexdigest()))
            self.assertEqual(set(events),{'source-renew','registry','carrier','source-close',
                                          'lease-renew','lease-close'})
            self.assertTrue((config.output).is_dir())
            self.assertFalse((config.output/'runtime-report.json').exists())

    def test_delta4_transaction1_composition_runs_only_the_coordinated_first_batch(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();root.chmod(0o700)
            config=self.config(root,'ashlar-delta/0.4-xxhash64')
            roles=('object_current','edge_current','tombstone','whole_source_history','attempts','manifest')
            config.installation.mkdir(mode=0o700)
            for role in roles:(config.installation/role).mkdir(mode=0o700)
            columns={role:(('id','BIGINT'),)for role in roles}
            scope={'pinned':'g03-source-scope'}
            targets=[{'table':'local.configured.'+role,'path':str(config.installation/role),
                      'uuid':str(uuid.uuid5(uuid.NAMESPACE_URL,role))}for role in roles]
            carrier={'profile':'ashlar-local-current-installation-profile/0.1',
                     'physical_profile':'ashlar-delta/0.4-xxhash64',
                     'lookup_hash_profile':'spark-xxhash64-typed-key/0.1',
                     'roles':{role:{'carrier_role':{'attempts':'publication_attempt_phase','manifest':'publication_manifest'}.get(role,role),
                                 'uuid':targets[index]['uuid']}for index,role in enumerate(roles)}}
            carrier_raw=(json.dumps(carrier,separators=(',',':'))+'\n').encode()
            registry={'profile':'ashlar-configured-local-installation/0.3','scope':scope,
                'targets':targets,'columns':{role:[['id','BIGINT']]for role in roles},
                'runtime_identity':{'profile':'runtime','python':'3.12'},
                'physical_profile':'ashlar-delta/0.4-xxhash64',
                'carrier_profile_sha256':hashlib.sha256(carrier_raw).hexdigest()}
            registry_raw=json.dumps(registry,separators=(',',':')).encode()
            events=[]
            registry_file=SimpleNamespace(raw=registry_raw,renew=lambda _limit:events.append('registry'))
            carrier_file=SimpleNamespace(raw=carrier_raw,renew=lambda _limit:events.append('carrier'))
            lease=SimpleNamespace(renew=lambda:events.append('lease-renew'),
                                  close=lambda:events.append('lease-close'))
            request={'request_digest':'a'*64}
            class Source:
                graph=True
                configuration=object()
                def renew(self,_context):events.append('source-renew')
                def spec(self,ordinal,_context):
                    self.assert_first_batch(ordinal)
                    return SimpleNamespace(request=lambda:request)
                @staticmethod
                def assert_first_batch(ordinal):
                    if ordinal!=1:raise AssertionError('transaction 1 only')
            source=Source()
            class SourceManager:
                def __enter__(self):return source
                def __exit__(self,*_args):events.append('source-close')
            class Admission:
                def __call__(self,*_args):
                    if spark.conf.get('spark.sql.parquet.compression.codec')!='zstd':
                        raise AssertionError('writer setting must be effective before admission')
                    events.append('admit-open')
                def closing(self,*_args):events.append('admit-close')
            admission=Admission()
            class SparkLike:
                version='4.2.0'
                def __init__(self):
                    self.conf_values={
                        'spark.sql.session.timeZone':'UTC',
                        'spark.sql.legacy.parameterSubstitution.constantsOnly':'true',
                        'spark.jars':'/delta/jars',
                        'spark.sql.parquet.compression.codec':'snappy',
                    }
                    def get(key):return self.conf_values.get(key,'512m')
                    def set(key,value):
                        events.append(('conf-set',key,value))
                        self.conf_values[key]=value
                    system=SimpleNamespace(getProperty=lambda key:'21')
                    self.sparkContext=SimpleNamespace(master='local[1]',applicationId='inert',
                        setLogLevel=lambda _level:None,_jvm=SimpleNamespace(java=SimpleNamespace(
                            lang=SimpleNamespace(System=system))))
                    self.conf=SimpleNamespace(get=get,set=set)
                def stop(self):events.append('spark-stop')
            spark=SparkLike()
            carrier_by_local={role:{'attempts':'publication_attempt_phase','manifest':'publication_manifest'}.get(role,role)
                             for role in roles}
            baseline={carrier_by_local[role]:{'uuid':targets[index]['uuid'],'version':1}
                      for index,role in enumerate(roles)}
            heads={carrier_by_local[role]:{'uuid':targets[index]['uuid'],
                         'version':3 if role=='attempts' else
                                   2 if role in ('object_current','edge_current','whole_source_history') else 1}
                   for index,role in enumerate(roles)}
            observation={'heads':heads,'lookup_hashes':[],'committed_prefix':3}
            class Held:
                def prepare_and_execute_effect_plan(self):events.append('coordinator-run')
                def _observe_effect_prefix(self):events.append('full-bag-observe');return observation
            held=Held()
            class Authority:
                @contextmanager
                def hold(self):
                    events.append('authority-open')
                    try:yield held
                    finally:events.append('authority-close')
            transport=SimpleNamespace(close=lambda:events.append('transport-close'))
            policy=SimpleNamespace(initializing=True)
            plan=SimpleNamespace(batch_id='typed-graph-1',request=request)
            fresh=SimpleNamespace(heads=baseline)
            receipt={'profile':'ashlar-configured-local-delta4-output-receipt-ref/0.1',
                     'request_digest':'a'*64,
                     'receipt_sha256':'b'*64}
            def fail_close():
                events.append('lease-close-failed')
                raise RuntimeError('inert close failure')
            failing_lease=SimpleNamespace(renew=lambda:events.append('lease-renew'),
                                          close=fail_close)
            with (patch('ashlar_host.configured_local_runtime._runtime',return_value=[Path('/delta/jars')]),
                  patch('ashlar_host.configured_local_runtime._scope',return_value=scope),
                  patch('ashlar_host.configured_local_runtime._columns',return_value=columns),
                  patch('ashlar_host.configured_local_runtime.attest_runtime',return_value=('runtime',{'python':'3.12'},[])),
                  patch('ashlar_host.configured_local_runtime.InvocationFile.read',side_effect=[registry_file,carrier_file,registry_file,carrier_file]),
                  patch('ashlar_host.configured_local_runtime.PrivateFiniteLease',side_effect=[lease,failing_lease]),
                  patch('ashlar_host.configured_local_runtime.open_configured_local_source',
                        side_effect=lambda _admission,stream,context:(setattr(source,'context',context) or SourceManager())),
                  patch('ashlar_host.configured_local_runtime.open_finite_spark',return_value=spark),
                  patch('ashlar_host.configured_local_runtime.verify_driver_memory'),
                  patch('ashlar_host.configured_local_runtime.verify_loaded_delta_class'),
                  patch('ashlar_host.configured_local_runtime.PrivatePolicy',return_value=policy),
                  patch('ashlar_host.configured_local_runtime.ConfiguredLocalAdmission',return_value=admission),
                  patch('ashlar_host.configured_local_runtime.LocalDeltaTransport',return_value=transport),
                  patch('ashlar_host.configured_local_delta4_publication.plan_configured_local_delta4_transaction1',return_value=plan)as make_plan,
                  patch('ashlar_host.configured_local_delta4_publication.admit_delta4_fresh_empty_installation',return_value=fresh),
                  patch('ashlar_host.configured_local_delta4_publication.build_delta4_transaction1_effect_plan',return_value=object()),
                  patch('ashlar_host.configured_local_delta4_publication.open_configured_local_delta4_authority',return_value=Authority()),
                  patch('ashlar_host.configured_local_delta4_publication.retain_delta4_output_completion_receipt',return_value=receipt)):
                result=run_configured_local_delta4_transaction1(
                    replace(config,installation_sha256=hashlib.sha256(registry_raw).hexdigest()))
                failed_output=root/'output-close-failure'
                failed_config=replace(config,output=failed_output,
                    admission=replace(config.admission,output=failed_output/'admission'),
                    installation_sha256=hashlib.sha256(registry_raw).hexdigest())
                with self.assertRaisesRegex(RuntimeError,'inert close failure'):
                    run_configured_local_delta4_transaction1(failed_config)
            self.assertEqual(result['operation'],'transaction1-effects-validated')
            self.assertEqual(result['batch_id'],'typed-graph-1')
            self.assertFalse(result['publication']);self.assertFalse(result['protected_ack'])
            self.assertEqual(result['completion_receipt'],receipt)
            self.assertEqual(json.loads((config.output/'runtime-report.json').read_bytes()),result)
            self.assertEqual(make_plan.call_count,2)
            self.assertIn('coordinator-run',events);self.assertIn('full-bag-observe',events)
            self.assertIn('admit-close',events);self.assertIn('transport-close',events)
            self.assertIn('spark-stop',events);self.assertIn('lease-close',events)
            writer_sets=[i for i,event in enumerate(events)
                         if event==('conf-set','spark.sql.parquet.compression.codec','zstd')]
            admissions=[i for i,event in enumerate(events) if event=='admit-open']
            self.assertEqual(len(writer_sets),2) # one per newly opened bridge session
            self.assertEqual(len(admissions),2)
            self.assertTrue(all(writer<admit for writer,admit in zip(writer_sets,admissions)))
            self.assertEqual(spark.conf.get('spark.sql.parquet.compression.codec'),'zstd')
            self.assertFalse((failed_output/'runtime-report.json').exists())

    def test_closing_admission_refuses_changed_parameter_binding_setting(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();root.chmod(0o700);config=self.config(root);spark=Spark();context=object()
            spark.conf=SimpleNamespace(get=lambda key:'false'if key=='spark.sql.legacy.parameterSubstitution.constantsOnly'else 'UTC')
            source=SimpleNamespace(context=context,renew=lambda _context:None)
            lease=SimpleNamespace(renew=lambda:None);registry=SimpleNamespace(renew=lambda _limit:None)
            admission=ConfiguredLocalAdmission(config,lease,spark,source,(),{},registry)
            with self.assertRaises(ValueError):admission.closing(source,None,context)

    def test_delta4_admission_does_not_repair_writer_setting_drift_after_open(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();root.chmod(0o700)
            config=self.config(root,'ashlar-delta/0.4-xxhash64')
            context=object();values={'spark.sql.session.timeZone':'UTC',
                'spark.sql.legacy.parameterSubstitution.constantsOnly':'true',
                'spark.jars':'','spark.sql.parquet.compression.codec':'zstd'};sets=[]
            def get(key):return values.get(key,'512m')
            def set_value(key,value):sets.append((key,value));values[key]=value
            spark=SimpleNamespace(
                sparkContext=SimpleNamespace(master='local[1]',applicationId='same-session',
                    _jvm=SimpleNamespace(java=SimpleNamespace(lang=SimpleNamespace(
                        System=SimpleNamespace(getProperty=lambda _key:'21'))))),
                conf=SimpleNamespace(get=get,set=set_value),version='4.2.0')
            source=SimpleNamespace(context=context,renew=lambda _context:None)
            lease=SimpleNamespace(renew=lambda:None)
            registry=SimpleNamespace(renew=lambda _limit:None)
            admission=ConfiguredLocalAdmission(config,lease,spark,source,(),{},registry)
            with patch('ashlar_host.configured_local_runtime.attest_runtime',return_value=('runtime',{},[])), \
                    patch('ashlar_host.configured_local_runtime.verify_loaded_delta_class'):
                admission(source,None,None,context)
                values['spark.sql.parquet.compression.codec']='snappy'
                with self.assertRaisesRegex(ValueError,'ZSTD Spark writer setting'):
                    admission.closing(source,None,context)
            self.assertEqual(sets,[]) # admission verifies changed settings, never repairs them
