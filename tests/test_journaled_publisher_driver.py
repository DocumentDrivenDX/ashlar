import json
from pathlib import Path
import sys
import tempfile
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from durable_effects import DurableEffects,EffectPlanError
from durable_sql import DurableSQL,SQLPending
from journaled_publisher_driver import JournaledPublisherDriver
from ashlar.publisher import PublicationError
from ashlar.stored_publisher import _artifact
from test_durable_effects import API,Policy,STEPS
from test_stored_publisher import Driver,PhaseExecutor,Transport,Policy as ManifestPolicy
import test_stored_publisher as stored

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
            artifacts=Artifacts();acks=[];validations=[]
            for iteration in range(2):
                journal=DurableSQL(path,api,'2439e1f2e37ac563','actor')
                try:
                    driver=JournaledPublisherDriver(DurableEffects(journal,Policy()),ManifestPolicy(),
                        lambda request,context:STEPS,artifacts,
                        lambda *args:validations.append(args[0]['request_digest']),
                        lambda *args:acks.append(args[0]['request_digest']),namespace='test')
                    transport=Transport(journal,api.manifest)
                    backend=helper.backend(phases,driver,transport,ManifestPolicy())
                    descriptor=helper.publish(backend,'admitted')
                    self.assertTrue(descriptor.validation_report['complete'])
                finally:journal.close()
            self.assertEqual(artifacts.capture_calls,1)
            self.assertEqual(artifacts.recover_calls,0)
            self.assertEqual([method for method,_ in api.calls],['POST','POST'])
            self.assertEqual(api.manifest.calls,['POST'])
            self.assertEqual(len(acks),2)
            self.assertEqual(len(validations),2)

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
