import copy,hashlib,json,shutil,sys,tempfile,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from check_commerce_graphframes_scenarios import scenario_oracle,ROOT
class CommerceScenarioOracleTests(unittest.TestCase):
    def test_original_four_authored_bags(self):
        expected,sources,pack=scenario_oracle()
        self.assertEqual(expected,{'partial-return':[[6,2,6]],'fulfillment':[['F2']],'settlement':[['PAY1']],'refund':[['RF1']]})
        self.assertEqual(len(sources),10)
        self.assertEqual({s['id'] for s in pack['scenario_checks']},set(expected))
    def test_corrupted_template_is_not_expected_self_proof(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);shutil.copy(ROOT/'pack.json',root/'pack.json');shutil.copytree(ROOT/'data',root/'data')
            p=root/'data'/'fulfillments.csv';p.write_bytes(p.read_bytes().replace(b'"11"',b'"10"'))
            with self.assertRaisesRegex(ValueError,'authored expected'):scenario_oracle(root)
    def profile(self):
        from puppy_commerce_fixture import admission
        from ashlar.graph_release import GraphRelease
        from ashlar_host.commerce_graphframes_scenarios import prepare_commerce_graphframes_profile
        repo=Path(__file__).resolve().parents[1]
        raw=(repo/'docs/helix/04-build/evidence/commerce-graph-export-20261009/release.graph.json').read_bytes()
        return prepare_commerce_graphframes_profile(admission(self),(repo/'examples/domain-packs/commerce/upstream/pack.json').read_bytes(),GraphRelease(raw,hashlib.sha256(raw).hexdigest()))
    def test_actual_relationship_reference_id_not_element(self):
        from ashlar_host.commerce_graphframes_scenarios import commerce_relationship_types
        profile=self.profile()
        self.assertEqual(len(profile['edges']),10)
        refs=[json.loads(row['retained_json'])['original']['relationship']for row in profile['edges']]
        self.assertTrue(all(set(ref)=={'document','module','id'}for ref in refs))
        actual=commerce_relationship_types(profile)
        self.assertEqual(set(actual),{ref['id']for ref in refs})
        self.assertTrue({'fulfillments.line_id','returns.line_id','payments.invoice_id','refunds.return_id','order_lines.product_id'}<=set(actual))
        for row,ref in zip(profile['edges'],refs):self.assertEqual(actual[ref['id']],row['rel_type_id'])
    def test_relationship_identity_drift_and_native_collision_refuse(self):
        from ashlar_host.commerce_graphframes_scenarios import commerce_relationship_types
        original=self.profile()
        for change in ('document','module','id','element','native'):
            profile=copy.deepcopy(original);row=profile['edges'][0];retained=json.loads(row['retained_json']);ref=retained['original']['relationship']
            if change=='element':ref['element']=ref.pop('id')
            elif change=='native':
                duplicate=copy.deepcopy(row);duplicate['rel_type_id']='forged';profile['edges'].append(duplicate)
            else:ref[change]='absent'
            row['retained_json']=json.dumps(retained)
            with self.subTest(change=change),self.assertRaises(ValueError):commerce_relationship_types(profile)
    def test_parallel_identity_preserved_and_cross_module_name_refuses(self):
        import base64
        from ashlar_host.commerce_graphframes_scenarios import commerce_relationship_types
        profile=self.profile();before=commerce_relationship_types(profile)
        profile['edges'].append(copy.deepcopy(profile['edges'][0]))
        self.assertEqual(commerce_relationship_types(profile),before)
        self.assertEqual(len(profile['edges']),11)
        row=copy.deepcopy(profile['edges'][0]);retained=json.loads(row['retained_json']);ref=retained['original']['relationship'];ref['module']='other'
        model=json.loads(base64.b64decode(profile['source_admission']['original_model_base64']))
        model['modules'].append({'id':'other','elements':[],'relationships':[{'id':ref['id']}]})
        profile['source_admission']['original_model_base64']=base64.b64encode(json.dumps(model).encode()).decode()
        row['retained_json']=json.dumps(retained);profile['edges'].append(row)
        with self.assertRaisesRegex(ValueError,'Unambiguous'):commerce_relationship_types(profile)
    def test_execute_entry_uses_relationship_resolution_before_native_effects(self):
        from types import ModuleType
        from unittest.mock import patch
        from ashlar_host import commerce_graphframes_scenarios as owner
        profile=self.profile();error=ValueError('selected original relationship gate')
        pyspark=ModuleType('pyspark');sql=ModuleType('pyspark.sql');sql.functions=object();pyspark.sql=sql
        with patch.dict(sys.modules,{'pyspark':pyspark,'pyspark.sql':sql}),patch.object(owner,'commerce_relationship_types',side_effect=error)as gate:
            with self.assertRaises(ValueError)as raised:owner.execute_commerce_graphframes_scenarios(None,profile,None)
        self.assertIs(raised.exception,error);gate.assert_called_once_with(profile)
if __name__=='__main__':unittest.main()
