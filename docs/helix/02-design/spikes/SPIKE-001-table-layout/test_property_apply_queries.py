"""Exact native SQL equivalence and invalid input/replay controls."""
import dataclasses,json,unittest
from pathlib import Path
from property_apply_queries import PropertyApply
B=Path(__file__).resolve().parent;F='client_dev.ashlar_entropy_20261006_r86'
class Checks(unittest.TestCase):
 def setUp(self):self.q=PropertyApply(F+'.edge_current',F+'.schedule_r123_1',17,11,'r123-b1','r121-b1','schedule-r123',9007199254741033)
 def test_recorded_native_equivalence(self):
  p=B/'out/native/ashlar_isolation_r123';rs=[json.loads(l) for l in (p/'statements.jsonl').read_text().splitlines()]
  expected={'intended-r123-b1':self.q.intended(),'apply-r123-b1':self.q.apply(),'output-parity-r123-b1':self.q.output(18),'input-membership-r123-b1':self.q.membership(),'global-identities-r123-b1':self.q.identities(18)}
  history={h['query_id']:h for h in json.loads((p/'query-history.json').read_text())}
  for label,sql in expected.items():
   with self.subTest(label=label):
    record=next(r for r in rs if r['label']==label);self.assertEqual(sql,record['sql']);self.assertTrue(history[record['statement_id']]['query_text'].endswith(sql))
 def test_invalid_parameters(self):
  for key,value in [('canonical','a.b.c;DROP TABLE x'),('stage',self.q.canonical),('old_version',True),('old_version',-1),('xid',9007199254741033.0),('previous_entity_version',9223372036854775807),('batch',"x' OR true"),('predecessor',self.q.batch),('epoch',"x'"),('xid',0)]:
   with self.subTest(key=key,value=value):
    with self.assertRaises(ValueError):dataclasses.replace(self.q,**{key:value})
  for version in (17,16,True,18.0):
   with self.subTest(version=version):
    with self.assertRaises(ValueError):self.q.output(version)
 def test_pruned_native_equivalence(self):
  p=B/'out/native/ashlar_merge_pruning_r131'
  summary=json.loads((p/'audited-summary.json').read_text())
  q=PropertyApply(summary['results']['candidate']['table'],F+'.schedule_r128_1',summary['results']['candidate']['old_version'],12,'r128-b1','r123-b1','schedule-r128',9007199254741035,eligibility_placement='on')
  records=[json.loads(l) for l in (p/'statements.jsonl').read_text().splitlines()]
  record=next(r for r in records if r['label']=='candidate-merge')
  self.assertEqual(q.apply(),record['sql'])
  history={h['query_id']:h for h in json.loads((p/'query-history.json').read_text())}
  self.assertTrue(history[record['statement_id']]['query_text'].endswith(q.apply()))
  with self.assertRaises(ValueError):dataclasses.replace(q,eligibility_placement='insert')
 def test_range_input_preserves_native_predicates(self):
  for placement in ('matched','on'):
   base=dataclasses.replace(self.q,eligibility_placement=placement)
   ranged=dataclasses.replace(base,input_ranges=6)
   self.assertEqual(ranged.apply().replace('/*+ REPARTITION_BY_RANGE(6,lookup_hash) */ ',''),base.apply())
   self.assertEqual(ranged.intended(),base.intended())
   self.assertEqual(ranged.output(18),base.output(18))
  for value in (True,0,-1,65,6.0,'6'):
   with self.subTest(value=value):
    with self.assertRaises(ValueError):dataclasses.replace(self.q,input_ranges=value)
if __name__=='__main__':unittest.main()
