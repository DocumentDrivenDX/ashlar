# SPDX-License-Identifier: MIT OR Apache-2.0
"""Portable typed source composition; only the public producer port is inert.

These controls establish original receipt/custody/apply wiring, not UMF semantic
validation, native publication, development-ID acceptance or protected ACK.
"""
import base64
from dataclasses import replace
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from ashlar.apply import plan_apply
from ashlar.attempt_store import verify_request_digest
from ashlar.source_checkpoint import jsonl_checkpoint
from ashlar.typed_source_policy import typed_values
from ashlar.whole_entity import changes_from_batch
from ashlar_host.config import ProducerConfig
from ashlar_host.configured_source_admission_config import ConfiguredSourceAdmissionConfig
from ashlar_host.configured_source_local import ConfiguredLocalSource,open_configured_local_source

EXAMPLE=Path(__file__).resolve().parents[1]/'examples/end-to-end/configured-typed-source.json'
PRODUCER='ashlar_host.configured_typed_source_admission.admit_typed_records'


def inert_receipt(config,model,bindings,batches,*,source_system,schema_revision):
    """Exact public response shape; deliberately supplies no real validation."""
    requests=[]
    for batch in batches:
        for change in changes_from_batch(batch):
            binding=bindings[change.state.key.type_id];module,element=binding['record']
            requests.append({'deliveryId':change.delivery_id,'recordSha256':change.raw_digest,'typeId':change.state.key.type_id,
                'propsSha256':hashlib.sha256(change.state.props_json.encode()).hexdigest(),
                'identity':{'module':module,'element':element},'values':typed_values(change.state.props_json,binding['fields'])})
    request=json.dumps({'records':requests},ensure_ascii=False,separators=(',',':')).encode()
    document=json.loads(model);valid={'valid':True,'complete':True,'diagnostics':[]};observed=[]
    for item in requests:
        owner=next(e for m in document['modules']if m['id']==item['identity']['module']for e in m['elements']if e['id']==item['identity']['element'])
        present={(v['field']['module'],v['field']['element'])for v in item['values']}
        observed.append({'request':item,'result':{'operation':'validate-core-record-values','version':'1.0.0','source':document,
            'identity':item['identity'],'values':item['values'],'documentValidation':valid,'validation':valid,
            'fields':[{'field':member,'state':'present'if(member['module'],member['element'])in present else'absent','validation':valid}for member in owner['members']]}})
    return json.dumps({'format':'ashlar-typed-record-check/0.1','producerRevision':'c45c72a2a8a3c4fba61c40c5927dd9091acf8cc3',
        'sourceBase64':base64.b64encode(model).decode(),'sourceSha256':hashlib.sha256(model).hexdigest(),
        'requestBase64':base64.b64encode(request).decode(),'requestSha256':hashlib.sha256(request).hexdigest(),'records':observed},separators=(',',':')).encode()


class Tests(unittest.TestCase):
    def fixture(self,root):
        value=json.loads(EXAMPLE.read_bytes())
        for name in ('source','schema','bindings'):
            original=EXAMPLE.parent/value[name]['path'];target=root/(name+'.input');target.write_bytes(original.read_bytes())
            value[name]['path']=target.name
        selected=root/'configuration.json';selected.write_text(json.dumps(value))
        producer=ProducerConfig(root/'producer',root/'bun',root/'git',30,1048576,4194304)
        return ConfiguredSourceAdmissionConfig(selected,producer,root/'receipts',4194304)

    def test_actual_owner_snapshots_original_tokens_specs_replay_and_restart(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();root.chmod(0o700);config=self.fixture(root);context=object()
            with patch(PRODUCER,side_effect=inert_receipt)as producer:
                with open_configured_local_source(config,stream='typed-雪',context=context)as source:
                    self.assertIs(type(source),ConfiguredLocalSource);self.assertTrue(source.typed);self.assertEqual(source.type_ids,(17,))
                    snapshots=[source.state_at(n,context)for n in range(4)]
                    self.assertEqual([len(s.current)for s in snapshots],[0,2,2,1])
                    self.assertEqual([len(s.history)for s in snapshots],[0,2,3,4])
                    self.assertEqual([len(s.tombstones)for s in snapshots],[0,0,0,1])
                    first={k.id:v for k,v in snapshots[1].current.items()};updated={k.id:v for k,v in snapshots[2].current.items()}
                    self.assertEqual(first[1].props_json,'{"21":18446744073709551615,"22":12.5000,"23":true}')
                    self.assertNotIn('24',json.loads(first[1].props_json));self.assertIn('24',json.loads(first[2].props_json));self.assertIsNone(json.loads(first[2].props_json)['24'])
                    self.assertEqual(first[2].props_json,'{"21":9007199254740993,"22":-0.00,"23":false,"24":null}')
                    self.assertEqual(updated[1].props_json,'{"21":9007199254740994,"22":40.00,"23":false,"24":"雪"}')
                    self.assertEqual(updated[1].version,2);self.assertEqual(updated[1].retained_json,'{"unknown":{"original":18446744073709551615}}')
                    deleted=next(iter(snapshots[3].tombstones.values()));self.assertEqual(deleted.operation,'delete');self.assertEqual(deleted.state.key.id,2)
                    self.assertEqual(deleted.state.props_json,first[2].props_json)
                    replay=snapshots[3]
                    for batch in source.batches:replay=plan_apply(replay,changes_from_batch(batch),schema_policy=source.configuration.policy)
                    self.assertEqual(replay,snapshots[3]);self.assertEqual(len(replay.deliveries),4)
                    specs=[source.spec(n,context)for n in (1,2,3)]
                    for n,spec in enumerate(specs):
                        request=spec.request();verify_request_digest(request);source.admit_request(request,context)
                        self.assertEqual(request['source_checkpoint_json'],jsonl_checkpoint(source.batches[n]))
                        self.assertEqual(json.loads(request['schema_revisions_json']),{'typed-items':'1'})
                        self.assertEqual(request['predecessor'],specs[n-1].publication_id if n else'empty-local-configured-jsonl-stream')
                        request['predecessor']='caller mutation';self.assertNotEqual(request,spec.request())
                    receipt=json.loads(source.receipt_files[0][1]);records=json.loads(base64.b64decode(receipt['requestBase64']))['records']
                    self.assertEqual(len(records),4);self.assertEqual(records[-1]['deliveryId'],'typed-delete-2')
                    report=json.loads((config.output/'report.json').read_bytes());self.assertEqual(report['records'],4)
                    self.assertFalse(report['published']);self.assertFalse(report['acknowledged'])
                with self.assertRaises(ValueError):source.state_at(1,context)
                with open_configured_local_source(replace(config,output=root/'restart'),stream='typed-雪',context=object())as reopened:
                    self.assertEqual([reopened.spec(n,reopened.context)for n in (1,2,3)],specs)
                    self.assertEqual(reopened.state_at(3,reopened.context),snapshots[3])
                self.assertEqual(producer.call_count,2)

    def test_closing_drift_refuses_and_preserves_original_cancellation(self):
        for cancel in (False,True):
            with self.subTest(cancel=cancel),tempfile.TemporaryDirectory()as directory:
                root=Path(directory).resolve();root.chmod(0o700);config=self.fixture(root);context=object();primary=KeyboardInterrupt()
                with patch(PRODUCER,side_effect=inert_receipt):
                    with self.assertRaises(KeyboardInterrupt if cancel else ValueError)as raised:
                        with open_configured_local_source(config,stream='typed-雪',context=context)as source:
                            source.spec(1,context)
                            (root/'source.input').write_bytes((root/'source.input').read_bytes()+b'\n')
                            if cancel:raise primary
                    if cancel:self.assertIs(raised.exception,primary);self.assertTrue(primary.cleanup_failed)
                    with self.assertRaises(ValueError):source.metadata(context)
