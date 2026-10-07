"""Reject missing/altered query, baseline, output and lineage evidence."""
import copy,json,unittest
from composed_structure_r126 import prove,load_evidence
class Checks(unittest.TestCase):
 def setUp(self):self.args=load_evidence()
 def test_actual(self):self.assertTrue(prove(*self.args)['structural_reuse'])
 def test_missing_evidence(self):
  for label in self.args[0]['owned_query_sha256']:
   with self.subTest(label=label):
    args=copy.deepcopy(self.args);args[1][:]=[r for r in args[1] if r['label']!=label]
    with self.assertRaises(AssertionError):prove(*args)
 def test_wrong_sql_result_history(self):
  for failure in ('sql','result','history','profile','lineage'):
   with self.subTest(failure=failure):
    args=copy.deepcopy(self.args);r=next(r for r in args[1] if r['label']=='output-parity-r123-b1')
    if failure=='sql':r['sql']+=' OR true'
    if failure=='result':r['response']['result']['data_array']=[['1']]
    if failure=='history':args[2][r['statement_id']]['is_final']=False
    if failure=='profile':args[0]['adjacency_version']=99
    if failure=='lineage':
     m=json.loads(args[3][0]['operationMetrics']);m['numTargetRowsUpdated']='100001';args[3][0]['operationMetrics']=json.dumps(m)
    with self.assertRaises(AssertionError):prove(*args)
if __name__=='__main__':unittest.main()
