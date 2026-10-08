import json
from pathlib import Path
import sys
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from native_artifact_validation import NativeArtifactValidator
from test_effect_validation import Executor,COLS,ROW
import test_journaled_snapshot_artifacts as artifacts
from test_stored_publisher import Driver
from ashlar.native import SQLResult
from ashlar.protocol import ReaderProtocolProfile,ProtocolError
from ashlar.publisher import PublicationError
from ashlar.stored_publisher import _artifact
from ashlar.effect_validation import EffectValidationError

class NativeExecutor(Executor):
    def __init__(self):super().__init__();self.feature='[]'
    def query(self,sql,params):
        if sql.startswith('DESCRIBE'):
            self.calls.append(sql)
            return SQLResult([{'id':'uuid','format':'delta','minReaderVersion':'3','minWriterVersion':'7','tableFeatures':self.feature}])
        return super().query(sql,params)
class Policy:
    def __init__(self):self.calls=0;self.denied=False
    def admit(self,descriptor,targets,context):
        self.calls+=1
        if context!='held' or self.denied:raise PermissionError('Denied')
class Gate:
    def __init__(self):self.calls=0;self.denied=False
    def check(self,*args):
        self.calls+=1
        if self.denied:raise PermissionError('Retention expired')

class NativeArtifactValidatorTests(unittest.TestCase):
    def inputs(self):
        request=artifacts.SnapshotArtifactTests().request();artifact=Driver().artifact(request)
        return request,artifact,_artifact(artifact,request)[1]
    def validator(self,executor,policy,gate,pins=lambda *args:None):
        return NativeArtifactValidator(executor,{'c.s.object_current':{'uuid':'uuid','version':7,'columns':COLS,'rows':[ROW]}},
            policy,gate,pins,reader_profile=ReaderProtocolProfile('test',(3,),(7,),()),manifest_table='c.s.manifest',manifest_uuid='uuid')
    def test_full_checks_renew_retention_and_source_admission(self):
        request,artifact,descriptor=self.inputs();ex=NativeExecutor();p=Policy();g=Gate();pins=[]
        validator=self.validator(ex,p,g,lambda *args:pins.append(args[0].publication_id))
        self.assertIsNone(validator(request,artifact,descriptor,'held'))
        self.assertEqual((p.calls,g.calls,len(pins)),(2,2,1))
        self.assertEqual(len(ex.calls),6)
    def test_unknown_protocol_wrong_rows_and_missing_pins_refuse(self):
        request,artifact,descriptor=self.inputs()
        for mode in ('protocol','rows','pins','retention','source'):
            ex=NativeExecutor();p=Policy();g=Gate()
            if mode=='protocol':ex.feature='["future"]'
            if mode=='rows':ex.rows=[]
            if mode=='retention':g.denied=True
            if mode=='source':p.denied=True
            validator=self.validator(ex,p,g,lambda *args:False if mode=='pins' else None)
            with self.assertRaises((ProtocolError,EffectValidationError,PublicationError,PermissionError)):
                validator(request,artifact,descriptor,'held')

    def test_raw_descriptor_mismatch_refuses_before_native_reads(self):
        from dataclasses import replace
        _,_,descriptor=self.inputs();ex=NativeExecutor()
        validator=self.validator(ex,Policy(),Gate())
        with self.assertRaises(PublicationError):validator.validate_descriptor(replace(descriptor,publication_id='replacement'),'held')
        self.assertEqual(ex.calls,[])

if __name__=='__main__':unittest.main()
