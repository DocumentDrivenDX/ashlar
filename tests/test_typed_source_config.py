"""Byte/binding and inert receipt-shape controls; no UMF producer/constraint proof."""
import base64,hashlib,json,tempfile,unittest
from dataclasses import replace
from pathlib import Path
from ashlar.source import jsonl_batches
from ashlar.whole_entity import changes_from_batch
from ashlar.typed_source_policy import TypedSourcePolicy,typed_values
from ashlar.typed_source_config import load_typed_jsonl_configuration,TypedConfigurationError
ROOT=Path(__file__).resolve().parents[1];EXAMPLE=ROOT/'examples/end-to-end/configured-typed-source.json'
def digest(raw):return hashlib.sha256(raw).hexdigest()

class Tests(unittest.TestCase):
    def copied(self,root):
        value=json.loads(EXAMPLE.read_bytes())
        for name in ('source','schema','bindings'):
            source=EXAMPLE.parent/value[name]['path'];target=root/source.name;target.write_bytes(source.read_bytes());value[name]['path']=target.name
        path=root/'configuration.json';path.write_text(json.dumps(value));return path,value
    def test_original_exact_numeric_bytes_immutable_bindings_and_no_default_policy(self):
        config=load_typed_jsonl_configuration(EXAMPLE);self.assertIsNone(config.policy);self.assertEqual(config.type_ids,(17,))
        self.assertEqual(config.model_source,(EXAMPLE.parent/'typed-model.umf.json').read_bytes())
        self.assertEqual(config.binding_source,(EXAMPLE.parent/'typed-binding.json').read_bytes())
        self.assertIn(b'18446744073709551615',config.source_bytes);self.assertIn(b'12.5000',config.source_bytes);self.assertIn(b'-0.00',config.source_bytes)
        self.assertEqual(config.bindings[17]['fields']['21'],('m','quantity','integer'));config.verify()
        with self.assertRaises(TypeError):config.bindings[17]['fields']['21']=('m','quantity','decimal')
        with self.assertRaises(ValueError):config.with_receipt(b'{}')
        with self.assertRaises(ValueError):replace(config,source_bytes=b'changed').verify()
    def test_foreign_absent_field_family_metadata_and_reference_drift_refuse(self):
        with tempfile.TemporaryDirectory()as temp:
            root=Path(temp).resolve();path,value=self.copied(root);config=load_typed_jsonl_configuration(path)
            source=config.source;source.write_bytes(source.read_bytes()+b'\n')
            with self.assertRaises(ValueError):config.verify()
            path,value=self.copied(root);binding_path=root/value['bindings']['path'];binding=json.loads(binding_path.read_bytes())
            # note is absent in the first event; its declared family still binds.
            binding['binding']['fields']['24'][2]='integer';raw=json.dumps(binding).encode();binding_path.write_bytes(raw);value['bindings']['sha256']=digest(raw);path.write_text(json.dumps(value))
            with self.assertRaises(ValueError):load_typed_jsonl_configuration(path)
            path,value=self.copied(root);value['documentRevision']='other';path.write_text(json.dumps(value))
            with self.assertRaises(ValueError):load_typed_jsonl_configuration(path)
    def test_exact_public_policy_construction_from_inert_ordered_receipt_shape(self):
        config=load_typed_jsonl_configuration(EXAMPLE);model=json.loads(config.model_source);fields=dict(config.bindings[17]['fields']);record=config.bindings[17]['record']
        changes=[c for b in jsonl_batches(config.source_bytes.splitlines(keepends=True),feed=config.feed,epoch=config.epoch)for c in changes_from_batch(b)]
        requests=[{'deliveryId':c.delivery_id,'recordSha256':c.raw_digest,'typeId':17,'propsSha256':digest(c.state.props_json.encode()),'identity':{'module':record[0],'element':record[1]},'values':typed_values(c.state.props_json,fields)}for c in changes]
        request=json.dumps({'records':requests},separators=(',',':')).encode();valid={'valid':True,'complete':True,'diagnostics':[]};members=next(e['members']for m in model['modules']for e in m['elements']if e['id']==record[1])
        observations=[]
        for wanted in requests:
            present={tuple(v['field'][k]for k in ('module','element'))for v in wanted['values']}
            observations.append({'request':wanted,'result':{'operation':'validate-core-record-values','version':'1.0.0','source':model,'identity':wanted['identity'],'values':wanted['values'],'documentValidation':valid,'validation':valid,'fields':[{'field':m,'state':'present'if(m['module'],m['element'])in present else'absent','validation':valid}for m in members]}})
        report={'format':'ashlar-typed-record-check/0.1','producerRevision':config.producer_revision,'sourceBase64':base64.b64encode(config.model_source).decode(),'sourceSha256':config.model_sha256,'requestBase64':base64.b64encode(request).decode(),'requestSha256':digest(request),'records':observations}
        raw=json.dumps(report).encode();admitted=config.with_receipt(raw)
        self.assertIs(type(admitted.policy),TypedSourcePolicy);self.assertIsNone(config.policy)
        for change in changes:self.assertIsNone(admitted.policy(change))
        self.assertEqual(load_typed_jsonl_configuration(EXAMPLE,receipt=raw).policy.receipt,raw)
        self.assertIs(admitted.original_receipt,raw);admitted.verify()
        admitted.policy.receipt=b'changed'
        with self.assertRaises(ValueError):admitted.verify()
        admitted=config.with_receipt(raw)
        admitted.policy._checks.pop(next(iter(admitted.policy._checks)))
        with self.assertRaises(ValueError):admitted.verify()
        admitted=config.with_receipt(raw)
        admitted.policy._checks[('foreign','delivery')]={}
        with self.assertRaises(ValueError):admitted.verify()
        with self.assertRaises(ValueError):replace(config,original_receipt=raw).verify()
        report['requestBase64']=base64.b64encode(json.dumps({'records':requests[:-1]}).encode()).decode()
        with self.assertRaises(ValueError):config.with_receipt(json.dumps(report).encode())

if __name__=='__main__':unittest.main()
