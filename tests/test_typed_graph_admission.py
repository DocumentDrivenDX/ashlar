"""Inert producer transport; real graph byte/receipt/core-apply correspondence.

No UMF semantic, native, catalog or source authority is established by fixtures.
"""
import base64
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from ashlar.typed_graph_source import build_typed_graph_request,TypedGraphSourcePolicy
from ashlar_host.config import ProducerConfig
from ashlar_host.typed_graph_admission import admit_typed_graph,UMF_REVISION
from test_typed_graph_source import fixture,inert_receipt


class Tests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name).resolve();self.configuration=fixture(self.root)
        self.request=build_typed_graph_request(self.configuration)
        source=self.root/'producer';source.mkdir();bun=self.root/'bun';git=self.root/'git'
        for path in (bun,git):path.write_bytes(b'inert selected executable');path.chmod(0o700)
        self.config=ProducerConfig(source,bun,git,30,1048576,4194304)
        self.calls=[];self.mode=None;self.cancel=KeyboardInterrupt('inert cancellation')

    def capture(self,argv,**kwargs):
        self.calls.append(argv)
        self.assertEqual(kwargs['cwd'],self.config.source);self.assertNotIn('PATH',kwargs['environment'])
        self.assertEqual(kwargs['environment']['GIT_CONFIG_GLOBAL'],'/dev/null')
        if argv[0]==str(self.config.git):
            if 'rev-parse'in argv:return UMF_REVISION.encode()+b'\n',b''
            self.assertIn('--untracked-files=normal',argv)
            closing=len([c for c in self.calls if c[0]==str(self.config.git)])>2
            return (b' M src/index.ts\n'if closing and self.mode in ('drift','cancel')else b''),b''
        self.assertEqual(argv[0],str(self.config.bun));self.assertEqual(argv[-1],str(self.config.git))
        self.assertEqual(Path(argv[1]).name,'typed_graph_values.ts')
        self.assertEqual(Path(argv[3]).read_bytes(),self.configuration.model_source)
        self.assertEqual(Path(argv[4]).read_bytes(),self.request)
        if self.mode=='cancel':raise self.cancel
        receipt=inert_receipt(self.configuration,self.request)
        if self.mode=='input-drift':Path(argv[3]).write_bytes(b'changed')
        if self.mode=='unknown-warning':
            value=json.loads(receipt);validation=value['groups'][0]['records'][0]['result']['validation']
            validation['complete']=False;validation['diagnostics'].append({'code':'UNKNOWN_WARNING','path':'/identity','message':'unresolved','severity':'warning'})
            receipt=json.dumps(value,separators=(',',':')).encode()
        if self.mode=='refusal':return b'{}',b''
        return receipt,b''

    def invoke(self,request=None):
        return admit_typed_graph(self.config,self.configuration.model_source,self.request if request is None else request)

    def test_original_complete_joint_receipt_and_strict_core_policy(self):
        with patch('ashlar_host.typed_graph_admission.capture',side_effect=self.capture):receipt=self.invoke()
        parsed=json.loads(self.request)
        self.assertEqual(len(parsed['groups']),3);self.assertEqual(len(parsed['groups'][-1]['records']),0)
        self.assertEqual(len(parsed['groups'][-1]['events']),1)
        self.assertEqual(json.loads(receipt)['requestBase64'],base64.b64encode(self.request).decode())
        # Real policy performs strict plan_apply and exact replay only after the
        # original joint receipt has been checked; no permissive apply callback.
        admitted=self.configuration.with_receipt(receipt)
        self.assertIs(type(admitted.policy),TypedGraphSourcePolicy);admitted.verify()
        self.assertEqual(len(self.calls),5)

    def test_original_source_bytes_and_failure_closing(self):
        for member,value in (('sourceSha256','0'*64),('cursorAfter','999999')):
            altered=json.loads(self.request)
            if member=='sourceSha256':altered[member]=value
            else:altered['groups'][0]['batch'][member]=value
            with self.subTest(member=member),patch('ashlar_host.typed_graph_admission.capture',side_effect=self.capture):
                with self.assertRaises(ValueError):self.invoke(json.dumps(altered,separators=(',',':')).encode())
            self.assertEqual(self.calls,[])
        for mode in ('refusal','unknown-warning','drift','input-drift','cancel'):
            self.mode=mode;self.calls=[]
            with self.subTest(mode=mode),patch('ashlar_host.typed_graph_admission.capture',side_effect=self.capture):
                with self.assertRaises(KeyboardInterrupt if mode=='cancel'else ValueError)as raised:self.invoke()
                if mode=='cancel':self.assertIs(raised.exception,self.cancel);self.assertTrue(self.cancel.cleanup_failed)
            self.assertEqual(len(self.calls),5)
