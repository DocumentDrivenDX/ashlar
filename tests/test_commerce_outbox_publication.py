import base64,json,tempfile,unittest
from decimal import Decimal
from pathlib import Path
from dataclasses import replace
from commerce_source_transaction import ROOT as ORIGINAL,build_transaction
from fixture_oracle import fixture_columns
from run_commerce_outbox_publication import CommerceAdmission,original_commerce_oracle,SOURCE_SYSTEM,ROOT
from run_local_outbox_publication import FixedStringAdmission
from local_delta_custody import encoded

class Tests(unittest.TestCase):
    def setUp(self):
        self.model=(ORIGINAL/'ontology.json').read_bytes();self.graph=(ORIGINAL/'graph/fixture.json').read_bytes();self.batch,self.bindings=build_transaction(self.model,self.graph,source_system=SOURCE_SYSTEM)
    def admission(self):
        temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup);directory=Path(temp.name)
        paths=[directory/name for name in ('model.json','graph.json','bindings.json','public.json')]
        paths[0].write_bytes(self.model);paths[1].write_bytes(self.graph);paths[2].write_text(encoded(self.bindings)+'\n');paths[3].write_bytes((ROOT/'docs/helix/04-build/evidence/commerce-public-dataset-20261009.json').read_bytes())
        return CommerceAdmission(self.batch,self.bindings,*paths),paths
    def test_genuine_record_warnings_remain_separate_from_complete_finite_dataset(self):
        admission,paths=self.admission();facts=admission.metadata()
        self.assertTrue(facts['dataset_validation']['complete']);self.assertEqual(facts['declared_umf'],'0.8.0')
        self.assertEqual(len(facts['individual_record_validations']),11)
        self.assertTrue(all(not r['validation']['complete'] for r in facts['individual_record_validations']))
        self.assertTrue(all(r['validation']['diagnostics'] for r in facts['individual_record_validations']))
        self.assertEqual(base64.b64decode(facts['original_model_base64']),self.model);self.assertEqual(base64.b64decode(facts['original_graph_base64']),self.graph)
        self.assertEqual(base64.b64decode(facts['original_bindings_base64']),paths[2].read_bytes())
        admission.admit(admission.changes[0])
        changed=replace(admission.changes[0],state=replace(admission.changes[0].state,props_json='{"forged":true}'))
        with self.assertRaises(PermissionError):admission.admit(changed)
    def test_legacy_named_profile_and_numeric_field_mapping_are_distinct(self):
        legacy,bindings=build_transaction(self.model,self.graph,source_system=SOURCE_SYSTEM,binding_profile='ashlar-commerce-development-bindings/0.1')
        original_commerce_oracle(self.model,self.graph,bindings,legacy,fixture_columns(ROOT))
        self.assertNotIn('properties',bindings)
        self.assertEqual({p['identity'][2] for p in self.bindings['properties']},{e['id'] for m in json.loads(self.model)['modules'] for e in m['elements'] if e['kind']=='field'})
        broken=json.loads(encoded(self.bindings));broken['properties'][1]['property_id']=broken['properties'][0]['property_id']
        with self.assertRaises(ValueError):original_commerce_oracle(self.model,self.graph,broken,self.batch,fixture_columns(ROOT))

    def test_changed_custody_or_incomplete_dataset_refuses(self):
        admission,paths=self.admission();paths[3].write_bytes(paths[3].read_bytes()+b' ')
        with self.assertRaises(PermissionError):admission.metadata()
        public=json.loads(paths[3].read_bytes());public['receipt']['datasetValidation']['complete']=False;paths[3].write_text(encoded(public))
        with self.assertRaises(PermissionError):CommerceAdmission(self.batch,self.bindings,*paths)
    def test_independent_original_oracle_preserves_decimal_tokens_and_original_endpoints(self):
        oracle=original_commerce_oracle(self.model,self.graph,self.bindings,self.batch,fixture_columns(ROOT));self.assertEqual({k:len(v) for k,v in oracle.items()},{'object_current':11,'edge_current':10,'tombstone':0,'whole_source_history':21})
        graph=json.loads(self.graph);assigned={(b['kind'],b['originalKey']):b for b in self.bindings['entities']}
        for original,row in zip(graph['objects'],oracle['object_current']):
            props=json.loads(row['props_json'],parse_float=Decimal)
            inverse={p['property_id']:p['identity'][2] for p in self.bindings['properties']}
            self.assertEqual({inverse[k]:str(v) for k,v in props.items()},original['values'])
            self.assertEqual(json.loads(row['retained_json'])['original'],original)
        for original,row in zip(graph['edges'],oracle['edge_current']):
            source=assigned[('object',original['source'])];target=assigned[('object',original['target'])]
            self.assertEqual((row['source_type'],row['source_id'],row['target_type'],row['target_id']),(source['type_id'],source['id'],target['type_id'],target['id']))
        duplicate=json.loads(encoded(self.bindings));duplicate['entities'][1]['id']=duplicate['entities'][0]['id'];duplicate['entities'][1]['type_id']=duplicate['entities'][0]['type_id']
        with self.assertRaises(ValueError):original_commerce_oracle(self.model,self.graph,duplicate,self.batch,fixture_columns(ROOT))
