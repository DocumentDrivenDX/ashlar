"""Pure public checkpoint and closed effect selection; no native authority."""
import ast
from dataclasses import replace
import hashlib,json,unittest
from pathlib import Path
from ashlar.apply import empty_state
from ashlar.outbox import OutboxTransaction
from ashlar_host.graph_sql import graph_sql_plan
from ashlar_host.publication_planning import request_for,local_effect_plan
from ashlar_host.delta_custody import encoded,sha
import ashlar_host.driver as driver
import ashlar_host.publication_planning as owner
import test_original_medical_admission as medical

class Tests(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  medical.MedicalAdmissionTests.setUpClass();cls.batch=medical.MedicalAdmissionTests.batch
  raw=cls.batch.begin+b''.join(r.raw for r in cls.batch.records)+cls.batch.commit
  cls.transaction=OutboxTransaction('ashlar-postgresql-outbox/0.1',cls.batch.feed,cls.batch.epoch,'0','1',hashlib.sha256(raw).hexdigest(),cls.batch)
  cls.tables={r:'local.medical.'+r for r in ('object_current','edge_current','tombstone','whole_source_history')}
 def generated(self):return graph_sql_plan(empty_state(),self.batch,self.tables,materialized_at='2026-10-09T12:00:00+00:00',schema_policy=lambda change:None)[1]
 def test_driver_reexports_exact_lower_functions_without_backedge(self):
  self.assertIs(driver.request_for,request_for);self.assertIs(driver.local_effect_plan,local_effect_plan)
  tree=ast.parse(Path(owner.__file__).read_text())
  self.assertFalse(any(isinstance(n,ast.ImportFrom)and n.module in ('driver','original_medical_local_publication')for n in ast.walk(tree)))
 def test_complete_native_checkpoint_and_unsigned_request_digest(self):
  value=request_for('stream',self.transaction,'origin',{self.batch.feed:'original'})
  checkpoint=json.loads(value['source_checkpoint_json'])
  self.assertEqual(checkpoint,{'profile':self.transaction.profile,'feed':self.batch.feed,'epoch':self.batch.epoch,'previous':'0','position':'1','payload_digest':self.transaction.payload_digest,'batch_id':self.batch.batch_id})
  unsigned={k:v for k,v in value.items()if k!='request_digest'}
  self.assertEqual(value['request_digest'],hashlib.sha256(json.dumps(unsigned,sort_keys=True,separators=(',',':')).encode()).hexdigest())
  self.assertEqual(value['source_batch_digest'],hashlib.sha256(value['source_batch_json'].encode()).hexdigest())
  with self.assertRaises(ValueError):request_for('stream',replace(self.transaction,payload_digest='0'*64),'origin',{})
 def test_real_generated_zero_match_only_and_complete_prior_digest(self):
  generated=self.generated();original=encoded(generated);prior={r:[]for r in self.tables}
  selected,elisions=local_effect_plan(generated,prior,self.tables)
  self.assertEqual(len(elisions),2);self.assertEqual(len(selected),len(generated)-2);self.assertEqual(encoded(generated),original)
  for e in elisions:
   self.assertEqual(e['original_step'],generated[e['ordinal']]);self.assertEqual(e['matched_rows'],[]);self.assertEqual(e['complete_prior_rows_sha256'],sha(encoded([])))
  key=elisions[0]['source_keys'][0];role='object_current';prior[role]=[dict(source_system='foreign',type_id=key['type_id'],id=key['id'])]
  self.assertEqual(len(local_effect_plan(generated,prior,self.tables)[1]),2)
  prior[role][0]['source_system']=key['source_system'];selected,elisions=local_effect_plan(generated,prior,self.tables)
  self.assertEqual(len(elisions),1);self.assertEqual(len(selected),len(generated)-1)
 def test_malformed_closed_keys_refuse_and_unrecognized_delete_not_elided(self):
  generated=self.generated();prior={r:[]for r in self.tables};delete=next(s for s in generated if s['statement'].endswith('WHEN MATCHED THEN DELETE'))
  for mutation in ('extra','bool'):
   step=json.loads(encoded(delete));keys=json.loads(step['parameters']['keys'])
   if mutation=='extra':keys[0]['extra']='x'
   else:keys[0]['id']=True
   step['parameters']['keys']=json.dumps(keys)
   with self.assertRaises(ValueError):local_effect_plan([step],prior,self.tables)
  step=json.loads(encoded(delete));step['statement']+=' '
  self.assertEqual(local_effect_plan([step],prior,self.tables),([step],[]))
