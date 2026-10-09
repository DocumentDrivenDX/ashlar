import json,shutil,sys,tempfile,unittest
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
if __name__=='__main__':unittest.main()
