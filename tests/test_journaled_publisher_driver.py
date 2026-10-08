from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from durable_effects import DurableEffects,EffectPlanError
from durable_sql import DurableSQL,SQLPending
from journaled_publisher_driver import JournaledPublisherDriver
from journaled_csv_progress import JournaledCsvProgress
from journaled_snapshot_artifacts import JournaledSnapshotArtifacts
from native_artifact_validation import NativeArtifactValidator
from test_native_artifact_validation import NativeExecutor,Gate
from ashlar.protocol import ReaderProtocolProfile
from ashlar.native import SQLResult
from test_effect_validation import Executor,COLS,ROW
from ashlar.publisher import PublicationError
from ashlar.stored_publisher import _artifact
from test_durable_effects import API,Policy,STEPS
from test_stored_publisher import Driver,PhaseExecutor,Transport,Policy as ManifestPolicy
import test_stored_publisher as stored

class SnapshotAdmission:
    def admit(self,request,effects,targets,context):
        if context!='admitted' or set(targets)!={'c.s.object_current'}:
            raise PermissionError('Test snapshot admission denied')

class Artifacts:
    def __init__(self):self.capture_calls=0;self.recover_calls=0
    def capture(self,request,effects,context):
        self.capture_calls+=1
        artifact=json.loads(Driver().artifact(request));artifact['effects']=effects
        self.original=json.dumps(artifact,separators=(',',':'))
        return self.original
    def recover(self,request,effects,context):
        self.recover_calls+=1
        if not hasattr(self,'original'):raise PublicationError('Original artifact observations unresolved')
        return self.original

class JournaledDriverTests(unittest.TestCase):
    def test_full_stored_publisher_composition_and_reopen_no_replacement(self):
        # Test transport only; actual durable effect and publisher implementations.
        helper=stored.StoredPublisherTests()
        with tempfile.TemporaryDirectory() as temporary:
            from test_stored_publisher import API as ManifestAPI
            class CombinedAPI(API):
                def __init__(self):super().__init__();self.manifest=ManifestAPI();self.pending=False
                def do(self,method,path,**kwargs):
                    if method=='POST' and kwargs['body']['statement'].startswith('MERGE'):
                        return self.manifest.do(method,path,**kwargs)
                    return super().do(method,path,**kwargs)
            api=CombinedAPI();path=str(Path(temporary)/'original.sqlite');phases=PhaseExecutor()
            csv_source=Path(temporary)/'source.csv'
            csv_source.write_bytes(b'id,entity_version,operation,label,caption,future\n1,1,create,label,,opaque\n')
            snapshot_executor=Executor();acks=[];validations=[]
            for iteration in range(2):
                journal=DurableSQL(path,api,'2439e1f2e37ac563','actor')
                try:
                    artifacts=JournaledSnapshotArtifacts(journal,snapshot_executor,SnapshotAdmission(),
                        lambda *args:{'c.s.object_current':{'uuid':'uuid','version':7,'columns':COLS,'rows':[ROW]}},
                        lambda request,*args:json.loads(Driver().artifact(request))['manifest'],namespace='snapshot')
                    class NativeRows(NativeExecutor):
                        def query(self,sql,parameters):
                            if sql.startswith('DESCRIBE') and '`manifest`' in sql:
                                self.calls.append(sql)
                                return SQLResult([{'id':'manifest-uuid','format':'delta','minReaderVersion':'3','minWriterVersion':'7','tableFeatures':'[]'}])
                            return super().query(sql,parameters)
                    class SourceAdmission:
                        def admit(self,descriptor,targets,context):
                            if context!='admitted':raise PermissionError('Test source admission denied')
                            validations.append(descriptor.validation_report['request_digest'])
                    validator=NativeArtifactValidator(NativeRows(),
                        {'c.s.object_current':{'uuid':'uuid','version':7,'columns':COLS,'rows':[ROW]}},
                        SourceAdmission(),Gate(),lambda *args:None,
                        reader_profile=ReaderProtocolProfile('test',(3,),(7,),()),manifest_table='c.s.manifest',manifest_uuid='manifest-uuid')
                    class LocalSourceAdmission:
                        def admit(self,request,descriptor,context):
                            if context!='admitted':raise PermissionError('Local test source custody denied')
                    @contextmanager
                    def resolved(descriptor,context):
                        if api.manifest.rows!=[dict(descriptor.raw)]:raise PermissionError('Original committed test manifest missing')
                        yield descriptor
                    progress=JournaledCsvProgress(journal,csv_source,hashlib.sha256(csv_source.read_bytes()).hexdigest(),
                        LocalSourceAdmission(),resolved,stream='stream',feed='csv',epoch='original',source_system='example',
                        schema_revision='3',type_id='17',properties={'label':'23','caption':'24'})
                    def acknowledge(request,descriptor,context):
                        progress.acknowledge(request,descriptor,context)
                        acks.append(request['request_digest'])
                    driver=JournaledPublisherDriver(DurableEffects(journal,Policy()),ManifestPolicy(),
                        lambda request,context:STEPS,artifacts,validator,acknowledge,namespace='test')
                    transport=Transport(journal,api.manifest)
                    backend=helper.backend(phases,driver,transport,ManifestPolicy())
                    descriptor=helper.publish(backend,'admitted')
                    self.assertTrue(descriptor.validation_report['complete'])
                    self.assertEqual(progress.position(),'1')
                    self.assertEqual(journal.db.execute('SELECT count(*) FROM snapshot_artifact').fetchone()[0],1)
                finally:journal.close()
            self.assertEqual(len(snapshot_executor.calls),4)
            self.assertEqual([method for method,_ in api.calls],['POST','POST'])
            self.assertEqual(api.manifest.calls,['POST'])
            self.assertEqual(len(acks),2)
            self.assertEqual(len(validations),4)

    def test_interrupted_effect_recovery_requires_original_artifact_observations(self):
        helper=stored.StoredPublisherTests();artifacts=Artifacts()
        with tempfile.TemporaryDirectory() as temporary:
            api=API();path=str(Path(temporary)/'journal');phases=PhaseExecutor()
            journal=DurableSQL(path,api,'2439e1f2e37ac563','actor')
            def driver():return JournaledPublisherDriver(DurableEffects(journal,Policy()),Policy(),
                lambda request,context:STEPS,artifacts,lambda *args:None,lambda *args:None,namespace='test')
            backend=helper.backend(phases,driver(),Transport(journal,api),ManifestPolicy())
            with self.assertRaises(SQLPending):helper.publish(backend,'admitted')
            request=json.loads(phases.rows[-1]['payload_json'])['request']
            api.pending=False
            with self.assertRaises(PublicationError):
                with backend.driver.writer('stream','admitted'):backend.driver.recover_apply(request,'admitted')
            self.assertEqual([method for method,_ in api.calls],['POST','POST','GET'])
            # Provider reconciles original proposal independently; driver doesn't synthesize one.
            effects=DurableEffects(journal,Policy()).recover('publisher-effects:test:'+request['request_digest'],request['request_digest'],STEPS,context='admitted')
            artifacts.capture(request,effects,'admitted')
            with backend.driver.writer('stream','admitted'):
                recovered=backend.driver.recover_apply(request,'admitted')
                _,descriptor=_artifact(recovered,request)
                backend.driver.validate(request,recovered,descriptor,'admitted')
            self.assertEqual(artifacts.capture_calls,1)
            before=list(api.calls)
            with journal.db:journal.db.execute('DELETE FROM effect_plan')
            with backend.driver.writer('stream','admitted'):
                with self.assertRaises(EffectPlanError):backend.driver.recover_apply(request,'admitted')
                with self.assertRaises(EffectPlanError):backend.driver.apply(request,'admitted')
            self.assertEqual(api.calls,before)
            journal.close()

    def test_native_validation_denial_keeps_original_applied_custody_without_manifest(self):
        helper=stored.StoredPublisherTests();artifacts=Artifacts()
        with tempfile.TemporaryDirectory() as temporary:
            api=API();api.pending=False
            journal=DurableSQL(str(Path(temporary)/'journal'),api,'2439e1f2e37ac563','actor')
            phases=PhaseExecutor();acks=[]
            driver=JournaledPublisherDriver(DurableEffects(journal,Policy()),ManifestPolicy(),
                lambda request,context:STEPS,artifacts,lambda *args:False,
                lambda *args:acks.append('ack'),namespace='test')
            backend=helper.backend(phases,driver,Transport(journal,api),ManifestPolicy())
            with self.assertRaises(PublicationError):helper.publish(backend,'admitted')
            self.assertEqual(phases.rows[-1]['phase'],'applied')
            self.assertEqual(acks,[])
            self.assertEqual([method for method,_ in api.calls],['POST','POST'])
            request=json.loads(phases.rows[-1]['payload_json'])['request']
            artifact=json.loads(phases.rows[-1]['payload_json'])['result_json']
            _,descriptor=_artifact(artifact,request)
            with self.assertRaises(PublicationError):driver.acknowledge(request,descriptor,'admitted')
            with driver.writer('stream','admitted'):
                with self.assertRaises(PublicationError):driver.validate(request,artifact,descriptor,object())
                with journal.db:journal.db.execute("UPDATE publisher_effect_artifact SET artifact_digest=?",('0'*64,))
                with self.assertRaises(PublicationError):driver.acknowledge(request,descriptor,'admitted')
            self.assertEqual(acks,[])
            journal.close()

if __name__=='__main__':unittest.main()
