# SPDX-License-Identifier: MIT OR Apache-2.0
"""Inert Spark metadata ports and actual private lease/source lifecycle only."""
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
import hashlib,json,tempfile,unittest,uuid
from types import MappingProxyType
from unittest.mock import patch,MagicMock
import test_configured_source_admission as fixtures
from ashlar_host.configured_local_runtime import ConfiguredLocalRuntimeConfig,ConfiguredLocalAdmission,initialize_configured_local,publish_configured_local,read_configured_local,_fields
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
                    'roles':{role:{'carrier_role':role,'table':target.table,'path':str(target.path),'uuid':target.uuid,
                        'fields':[],'field_metadata':[],'protocol':{},'clustering_columns':[],'partition_columns':[],
                        'layout_properties':{},'umf_source_bytes_sha256':'1'*64,'umf_source_sha256':'2'*64,
                        'umf_schema_sha256':'3'*64,'umf_target_sha256':'4'*64,'umf_proposal_sha256':'5'*64,
                        'umf_ddl_sha256':'6'*64,'umf_dispositions':[],'umf_writer_settings':[], 'umf_proposal':{}}
                        for role,target in zip(tables,targets)}}
                return targets,profile,{}
            transport=SimpleNamespace(close=lambda:None)
            with patch('ashlar_host.configured_source_admission.admit_managed_records',return_value=(b'{}',)),patch('ashlar_host.configured_local_runtime._runtime',return_value=[]),patch('ashlar_host.configured_local_runtime.attest_runtime',return_value=('runtime',{'python':'3.12'},[])),patch('ashlar_host.configured_local_runtime.verify_loaded_delta_class'),patch('ashlar_host.configured_local_runtime.open_finite_spark',return_value=spark),patch('ashlar_host.configured_local_runtime.LocalDeltaTransport.initialize',return_value=transport),patch('ashlar_host.local_current_carrier_profile.create_local_current_xxhash64_tables',side_effect=realize):
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
            self.assertTrue(spark.closed)

    def test_xxhash64_continuation_is_refused_before_output_creation(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();root.chmod(0o700)
            config=self.config(root,'ashlar-delta/0.4-xxhash64')
            config=replace(config,installation_sha256='f'*64)
            with self.assertRaises(ValueError):publish_configured_local(config,ordinal=1)
            self.assertFalse(config.output.exists())

    def test_closing_admission_refuses_changed_parameter_binding_setting(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();root.chmod(0o700);config=self.config(root);spark=Spark();context=object()
            spark.conf=SimpleNamespace(get=lambda key:'false'if key=='spark.sql.legacy.parameterSubstitution.constantsOnly'else 'UTC')
            source=SimpleNamespace(context=context,renew=lambda _context:None)
            lease=SimpleNamespace(renew=lambda:None);registry=SimpleNamespace(renew=lambda _limit:None)
            admission=ConfiguredLocalAdmission(config,lease,spark,source,(),{},registry)
            with self.assertRaises(ValueError):admission.closing(source,None,context)
