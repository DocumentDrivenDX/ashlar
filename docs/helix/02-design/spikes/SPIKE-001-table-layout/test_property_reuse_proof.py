"""Real evidence equivalence and fail-closed proof inputs."""
import copy,json,unittest
from pathlib import Path
from property_apply_queries import PropertyApply
from property_reuse_proof import prove
B=Path(__file__).resolve().parent;F='client_dev.ashlar_entropy_20261006_r86'
def evidence():
 records=[];history={}
 for run in ('r121','r123'):
  p=B/f'out/native/ashlar_isolation_{run}';records.extend(json.loads(l) for l in (p/'statements.jsonl').read_text().splitlines());history.update({h['query_id']:h for h in json.loads((p/'query-history.json').read_text())})
 s=json.loads((B/'out/native/ashlar_isolation_r123/audited-summary.json').read_text())
 return records,history,s['batches'][0]['commits']
def args():return [PropertyApply(F+'.edge_current',F+'.schedule_r123_1',17,11,'r123-b1','r121-b1','schedule-r123',9007199254741033),18,F+'.adjacency_canonical_r120',1,1,100000,20000000,*evidence()]
class Checks(unittest.TestCase):
 def test_actual(self):self.assertEqual(prove(*args(),mode='serialized-synthetic-property105')['new_version'],18)
 def test_rejected_boundaries(self):
  for index,value in [(1,19),(2,F+'.wrong'),(3,2),(4,2),(5,99999),(6,19999999)]:
   with self.subTest(index=index):
    values=args();values[index]=value
    with self.assertRaises((AssertionError,ValueError)):prove(*values,mode='serialized-synthetic-property105')
  with self.assertRaises(ValueError):prove(*args(),mode='production')
 def test_missing_and_mutated_checks(self):
  for label in ['adjacency-reuse-r121-b1','intended-r123-b1','output-parity-r123-b1','apply-r123-b1']:
   with self.subTest(label=label):
    values=args();values[7][:]=[r for r in values[7] if r['label']!=label]
    with self.assertRaises(AssertionError):prove(*values,mode='serialized-synthetic-property105')
  values=copy.deepcopy(args());r=next(r for r in values[7] if r['label']=='intended-r123-b1');r['sql']+=' OR false'
  with self.assertRaises(AssertionError):prove(*values,mode='serialized-synthetic-property105')
if __name__=='__main__':unittest.main()
