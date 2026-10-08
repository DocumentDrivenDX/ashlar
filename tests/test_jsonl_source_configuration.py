import json,tempfile,unittest
from pathlib import Path
from dataclasses import replace
from jsonl_source_configuration import load_jsonl_configuration
from run_local_example import ROOT
from ashlar.source import jsonl_batches
from ashlar.apply import empty_state,plan_apply
from ashlar.whole_entity import changes_from_batch

class ConfigurationTests(unittest.TestCase):
    def test_explicit_model_ids_source_and_replay(self):
        config=load_jsonl_configuration(ROOT/'examples/end-to-end/configured-source.json')
        self.assertEqual(config.policy.record_identities[1017],('fixture','item'))
        self.assertEqual(set(config.policy.property_fields[1017]),{'1023','1024'})
        batches=tuple(jsonl_batches(config.source.read_bytes().splitlines(keepends=True),feed=config.feed,epoch=config.epoch))
        state=empty_state()
        for batch in batches:state=plan_apply(state,changes_from_batch(batch),schema_policy=config.policy)
        self.assertEqual((len(state.current),len(state.history),len(state.tombstones)),(1,4,1))
        original=state
        for batch in batches:state=plan_apply(state,changes_from_batch(batch),schema_policy=config.policy)
        self.assertEqual(state,original)
        with self.assertRaises(ValueError):replace(config,original=b'changed').verify()
    def test_unknown_configuration_stale_digests_and_foreign_ids_refuse(self):
        original=json.loads((ROOT/'examples/end-to-end/configured-source.json').read_bytes())
        for ref in ('source','schema','intake','interpretation'):
            original[ref]['path']=str(ROOT/'examples/end-to-end'/original[ref]['path'])
        for kind in ('unknown','digest','bindings','source'):
            candidate=json.loads(json.dumps(original))
            if kind=='unknown':candidate['future']=True
            elif kind=='digest':candidate['source']['sha256']='0'*64
            elif kind=='bindings':candidate['bindings']=candidate['bindings'][:-1]
            else:candidate['sourceSystem']=False
            with tempfile.TemporaryDirectory() as tmp:
                path=Path(tmp)/'config.json';path.write_text(json.dumps(candidate))
                with self.assertRaises(ValueError):load_jsonl_configuration(path)
