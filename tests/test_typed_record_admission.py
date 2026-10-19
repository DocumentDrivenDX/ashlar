# SPDX-License-Identifier: MIT OR Apache-2.0
"""Actual receipt correspondence with inert producer transport; no UMF execution."""
import base64
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from ashlar.source import jsonl_batches
from ashlar.typed_source_policy import TypedSourcePolicy,RECORD_CHECK_PIN
from ashlar.whole_entity import changes_from_batch
from ashlar_host.config import ProducerConfig
from ashlar_host.typed_record_admission import admit_typed_records

ROOT=Path(__file__).resolve().parents[1]/'examples/end-to-end'

class Tests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        root=Path(self.temp.name);producer=root/'producer';producer.mkdir()
        bun=root/'bun';git=root/'git'
        for path in (bun,git):path.write_bytes(b'inert selected executable');path.chmod(0o700)
        self.config=ProducerConfig(producer,bun,git,30,1048576,1048576)
        self.model=(ROOT/'typed-model.umf.json').read_bytes()
        selected=json.loads((ROOT/'typed-binding.json').read_bytes())
        self.bindings={selected['type_id']:selected['binding']}
        self.batches=tuple(jsonl_batches((ROOT/'typed-source.jsonl').read_bytes().splitlines(keepends=True),feed='typed-jsonl',epoch='typed-1'))
        self.calls=[];self.mode=None;self.cancel=KeyboardInterrupt('inert cancellation')

    def transport(self,argv,**kwargs):
        self.calls.append(argv)
        self.assertEqual(kwargs['cwd'],self.config.source)
        self.assertNotIn('PATH',kwargs['environment']);self.assertEqual(kwargs['environment']['GIT_CONFIG_GLOBAL'],'/dev/null')
        if argv[0]==str(self.config.git):
            if 'rev-parse'in argv:return RECORD_CHECK_PIN.encode()+b'\n',b''
            self.assertIn('--untracked-files=normal',argv)
            closing=len([c for c in self.calls if c[0]==str(self.config.git)])>2
            return (b' M src/index.ts\n'if closing and self.mode in ('drift','cancel')else b''),b''
        self.assertEqual(argv[0],str(self.config.bun));self.assertEqual(argv[-1],str(self.config.git))
        self.assertEqual(Path(argv[1]).name,'typed_record_values.ts')
        model=Path(argv[3]).read_bytes();request=Path(argv[4]).read_bytes();parsed=json.loads(request)
        if self.mode=='cancel':raise self.cancel
        source=json.loads(model);records=[];valid={'valid':True,'complete':True,'diagnostics':[]}
        for item in parsed['records']:
            owner=next(e for m in source['modules']if m['id']==item['identity']['module']for e in m['elements']if e['id']==item['identity']['element'])
            states={(v['field']['module'],v['field']['element']):v['state']for v in item['values']}
            result={'operation':'validate-core-record-values','version':'1.0.0','source':source,'identity':item['identity'],'values':item['values'],
                    'documentValidation':valid,'validation':valid,'fields':[{'field':member,'state':states.get((member['module'],member['element']),'absent'),'validation':valid}for member in owner['members']]}
            records.append({'request':item,'result':result})
        if self.mode=='refusal':records[-1]['result']['validation']={'valid':False,'complete':True,'diagnostics':[]}
        receipt={'format':'ashlar-typed-record-check/0.1','producerRevision':RECORD_CHECK_PIN,
                 'sourceBase64':base64.b64encode(model).decode(),'sourceSha256':hashlib.sha256(model).hexdigest(),
                 'requestBase64':base64.b64encode(request).decode(),'requestSha256':hashlib.sha256(request).hexdigest(),'records':records}
        if self.mode=='input-drift':Path(argv[3]).write_bytes(b'changed')
        return json.dumps(receipt,separators=(',',':')).encode(),b''

    def invoke(self,bindings=None):
        return admit_typed_records(self.config,self.model,self.bindings if bindings is None else bindings,self.batches,source_system='typed-source',schema_revision='1')

    def test_all_original_events_and_tokens_including_delete(self):
        with patch('ashlar_host.typed_record_admission.capture',side_effect=self.transport):raw=self.invoke()
        receipt=json.loads(raw);request=json.loads(base64.b64decode(receipt['requestBase64']))
        self.assertEqual(len(request['records']),4)
        self.assertEqual(request['records'][-1]['deliveryId'],'typed-delete-2')
        self.assertEqual(request['records'][0]['values'][0]['value'],{'integerToken':'18446744073709551615'})
        self.assertEqual(request['records'][0]['values'][1]['value'],{'decimalToken':'12.5000'})
        self.assertEqual(request['records'][1]['values'][-1]['value'],None)
        policy=TypedSourcePolicy(self.model,raw,self.bindings,source_system='typed-source',schema_revision='1',trusted_producer_revision=RECORD_CHECK_PIN)
        for batch in self.batches:
            for change in changes_from_batch(batch):self.assertIsNone(policy(change))
        self.assertEqual(len(self.calls),5)

    def test_malformed_refusal_drift_and_cancellation_close(self):
        bad=copy.deepcopy(self.bindings);del bad[17]['fields']['21']
        with patch('ashlar_host.typed_record_admission.capture',side_effect=self.transport):
            with self.assertRaises(ValueError):self.invoke(bad)
        self.assertEqual(self.calls,[])
        for mode in ('refusal','drift','input-drift','cancel'):
            self.mode=mode;self.calls=[]
            with self.subTest(mode=mode),patch('ashlar_host.typed_record_admission.capture',side_effect=self.transport):
                if mode=='cancel':
                    with self.assertRaises(KeyboardInterrupt)as raised:self.invoke()
                    self.assertIs(raised.exception,self.cancel)
                else:
                    with self.assertRaises(ValueError):self.invoke()
            self.assertEqual(len(self.calls),5)
