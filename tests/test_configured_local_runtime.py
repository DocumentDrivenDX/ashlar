"""Inert Spark metadata ports and actual private lease/source lifecycle only."""
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
import hashlib,json,tempfile,unittest,uuid
from unittest.mock import patch,MagicMock
import test_configured_source_admission as fixtures
from ashlar_host.configured_local_runtime import ConfiguredLocalRuntimeConfig,ConfiguredLocalAdmission,initialize_configured_local,publish_configured_local,read_configured_local,_fields

class Row(dict):
    def asDict(self,recursive=False):return dict(self)
class Frame:
    def __init__(self,rows):self.rows=rows
    def limit(self,n):return Frame(self.rows[:n])
    def collect(self):return self.rows
class Spark:
    def __init__(self):
        self.closed=False;self.detail_reads=0;self.details={};self.fields=_fields();self.sparkContext=SimpleNamespace(master='local[1]',applicationId='inert-session',setLogLevel=lambda level:None)
        self.conf=SimpleNamespace(get=lambda key:'512m'if key=='spark.driver.memory'else 'UTC');self.read=self
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
    def config(self,root):
        admission=fixtures.Tests().fixture(root)
        output=root/'output';admission=replace(admission,output=output/'admission')
        return ConfiguredLocalRuntimeConfig(root/'installation',output,root/'jars',root/'java',admission,'configured-original','1791547200000000',None,'original-local-installation',None)
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
            with patch('ashlar_host.configured_source_admission.admit_managed_records',return_value=(b'{}',)),patch('ashlar_host.configured_local_runtime._runtime',return_value=[]),patch('ashlar_host.configured_local_runtime.open_finite_spark',return_value=spark),patch('ashlar_host.configured_local_runtime.ConfiguredLocalAdmission',side_effect=owner),patch('ashlar_host.configured_local_runtime.LocalDeltaTransport.initialize',side_effect=initialize):
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
            with patch('ashlar_host.configured_source_admission.admit_managed_records',return_value=(b'{}',)),patch('ashlar_host.configured_local_runtime._runtime',return_value=[]),patch('ashlar_host.configured_local_runtime.open_finite_spark',return_value=spark),patch('ashlar_host.configured_local_runtime.ConfiguredLocalAdmission',return_value=current),patch('ashlar_host.configured_local_runtime.LocalDeltaTransport',return_value=transport),patch('ashlar_host.file_progress.JournaledJsonlProgress.open_existing',return_value=progress),patch('ashlar_host.configured_local_read.read_configured_local_singleton',return_value=retained_row)as read,patch('ashlar_host.configured_local_runtime.ConfiguredLocalPublicationDriver')as publisher:
                answer=read_configured_local(read_config,ordinal=1,type_id=1017,entity_id=1)
            publisher.assert_not_called();self.assertEqual(read.call_args.kwargs['ordinal'],1)
            self.assertEqual(answer['row'],retained_row);self.assertGreater((read_config.output/'runtime-report.json').stat().st_size,262144);self.assertEqual(answer['local_checkpoint'],'1828');self.assertEqual(answer['completed_batches'],3)
            self.assertTrue(spark.closed);current.closing.assert_called_once()
    def test_native_schema_drift_refuses_success_and_still_stops_session(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();root.chmod(0o700);config=self.config(root);spark=Spark();spark.fields={**spark.fields,'object_current':(('foreign','STRING',True),)}
            with patch('ashlar_host.configured_source_admission.admit_managed_records',return_value=(b'{}',)),patch('ashlar_host.configured_local_runtime._runtime',return_value=[]),patch('ashlar_host.configured_local_runtime.open_finite_spark',return_value=spark),patch('ashlar_host.configured_local_runtime.LocalDeltaTransport.initialize')as initialize:
                with self.assertRaises(ValueError):initialize_configured_local(config)
            self.assertTrue(spark.closed);initialize.assert_not_called();self.assertFalse((config.output/'runtime-report.json').exists())
