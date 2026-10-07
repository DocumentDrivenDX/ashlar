"""Actual recorded lineage and deliberately corrupted controls."""
import copy,json,unittest
from pathlib import Path
from changed_structure_guard import guard
B=Path(__file__).resolve().parent
class Checks(unittest.TestCase):
 def setUp(self):
  p=B/'out/native/ashlar_isolation_r123';s=json.loads((p/'audited-summary.json').read_text());self.r=s['batches'][0]
  rs=[json.loads(l) for l in (p/'statements.jsonl').read_text().splitlines()]
  self.query=next(r['statement_id'] for r in rs if r['label']=='apply-r123-b1')
 def run_guard(self,rows):return guard(rows,self.r['old_current_version'],self.r['versions']['client_dev.ashlar_entropy_20261006_r86.edge_current'],self.query,100000)
 def test_actual(self):self.assertTrue(self.run_guard(self.r['commits']))
 def test_rejections(self):
  for field,value in [('queryHistoryStatementId','other'),('readVersion','16'),('operation','DELETE'),('version','99')]:
   with self.subTest(field=field):
    rows=copy.deepcopy(self.r['commits']);rows[0][field]=value
    with self.assertRaises((AssertionError,KeyError)):self.run_guard(rows)
  for field,value in [('numSourceRows',99999),('numTargetRowsInserted',1),('numTargetRowsUpdated',99999),('numTargetRowsCopied',1),('numTargetRowsDeleted',1)]:
   with self.subTest(field=field):
    rows=copy.deepcopy(self.r['commits']);m=json.loads(rows[0]['operationMetrics']);m[field]=str(value);rows[0]['operationMetrics']=json.dumps(m)
    with self.assertRaises(AssertionError):self.run_guard(rows)
  with self.assertRaises(AssertionError):self.run_guard(self.r['commits']*2)
if __name__=='__main__':unittest.main()
