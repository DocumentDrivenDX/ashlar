"""Exact original medical plan; actual admission with inert producer only."""
import ast
from dataclasses import replace
import hashlib,json,unittest
from pathlib import Path
from unittest.mock import patch
from ashlar.medical_source import build_transaction,SOURCE_SHA
from ashlar.outbox import OutboxTransaction
from ashlar_host.medical_publication_plan import MedicalPublicationPlan,FEED,EPOCH,ROLES
import ashlar_host.medical_publication_plan as owner
import test_original_medical_admission as medical

class Tests(unittest.TestCase):
 def setUp(self):
  medical.MedicalAdmissionTests.setUpClass();self.fixture=medical.MedicalAdmissionTests();self.fixture.setUp();self.addCleanup(self.fixture.doCleanups)
  m=self.fixture;m.batch,m.bindings=build_transaction(m.model,m.graph,m.baseline,source_system=FEED)
  p=patch('ashlar_host.medical_producer.capture',side_effect=m.capture);p.start();self.addCleanup(p.stop)
  self.admission=m.admission();raw=m.batch.begin+b''.join(r.raw for r in m.batch.records)+m.batch.commit
  self.transaction=OutboxTransaction('ashlar-postgresql-outbox/0.1',FEED,EPOCH,'0','1',hashlib.sha256(raw).hexdigest(),m.batch)
  self.tables={r:'local.original_medical.'+r for r in ROLES}
 def plan(self):return MedicalPublicationPlan(self.admission,self.transaction,self.tables)
 def test_complete_independent_original_bags_generated_schemas_and_request(self):
  plan=self.plan();value=plan.snapshot()
  self.assertEqual(tuple(len(value['expected'][r])for r in ROLES[:4]),(51,62,0,113))
  self.assertEqual(json.loads(value['request']['schema_revisions_json']),{FEED:SOURCE_SHA})
  self.assertEqual(value['progress'][FEED]['payload_digest'],self.transaction.payload_digest)
  self.assertEqual(value['progress'][FEED]['position'],'1');self.assertEqual(value['previous_progress'],{})
  self.assertEqual(len(value['zero_match_elisions']),2);self.assertEqual(len(value['generated_steps'])-len(value['selected_steps']),2)
  for role in ROLES[:4]:
   fields=json.loads(value['schemas'][role])['fields']
   self.assertEqual([f['name']for f in fields],[c[0]for c in value['columns'][role]])
   self.assertTrue(all(f['nullable']is True and f['metadata']=={} for f in fields))
   self.assertEqual(set(value['expected'][role][0])if value['expected'][role]else set(c[0]for c in value['columns'][role]),set(c[0]for c in value['columns'][role]))
  plan.require_request(value['request']);plan.require_admission(self.admission)
  self.assertIs(plan.admission,self.admission);self.assertIs(plan.transaction,self.transaction)
 def test_detached_snapshots_and_original_request_refusals(self):
  plan=self.plan();value=plan.snapshot();value['expected']['object_current'].clear();value['selected_steps'].clear();self.tables['object_current']='foreign'
  self.assertEqual(len(plan.snapshot()['expected']['object_current']),51)
  request=plan.snapshot()['request'];request['predecessor']='replacement'
  with self.assertRaises(PermissionError):plan.require_request(request)
  with self.assertRaises(PermissionError):plan.require_admission(object())
  with self.assertRaises(AttributeError):plan.transaction=self.transaction
 def test_exact_nominal_transaction_feed_epoch_cursor_digest_and_tables(self):
  for changes in ({'feed':'foreign'},{'epoch':'foreign'},{'previous':'1'},{'position':'2'},{'payload_digest':'0'*64},{'profile':'foreign'},{'batch':object()}):
   with self.subTest(changes=changes),self.assertRaises((ValueError,PermissionError)):MedicalPublicationPlan(self.admission,replace(self.transaction,**changes),self.tables)
  with self.assertRaises(ValueError):MedicalPublicationPlan(object(),self.transaction,self.tables)
  for tables in ({r:v for r,v in self.tables.items()if r!='manifest'},dict(self.tables,object_current='local.commerce.object_current')):
   with self.assertRaises(ValueError):MedicalPublicationPlan(self.admission,self.transaction,tables)
 def test_matching_transaction_replacement_is_not_original_construction_owner(self):
  plan=self.plan();replacement=replace(self.transaction)
  self.assertEqual(replacement,self.transaction);self.assertIsNot(replacement,self.transaction)
  object.__setattr__(plan,'_transaction',replacement)
  with self.assertRaises(PermissionError):plan.renew()
 def test_source_file_and_replaced_transaction_refuse_renewal(self):
  plan=self.plan();original=self.transaction.payload_digest
  object.__setattr__(self.transaction,'payload_digest','0'*64)
  with self.assertRaises((ValueError,PermissionError)):plan.renew()
  object.__setattr__(self.transaction,'payload_digest',original)
  self.fixture.paths[0].write_bytes(b'changed')
  with self.assertRaises((ValueError,PermissionError)):plan.renew()
 def test_effect_and_independent_oracle_drift_cannot_replace_original_plan(self):
  plan=self.plan();original=owner.graph_sql_plan
  def changed(*args,**kwargs):
   after,steps=original(*args,**kwargs);steps[-1]['parameters']['rows']='[]';return after,steps
  with patch.object(owner,'graph_sql_plan',side_effect=changed),self.assertRaises(PermissionError):plan.renew()
  original_oracle=owner.original_medical_oracle
  def wrong(*args,**kwargs):
   rows=original_oracle(*args,**kwargs);rows['object_current'][0]['props_json']='{}';return rows
  with patch.object(owner,'original_medical_oracle',side_effect=wrong),self.assertRaises(PermissionError):plan.renew()
 def test_graph_failure_cancel_still_attempts_admission_closing(self):
  plan=self.plan();primary=KeyboardInterrupt('private');original=self.admission.metadata;calls=[]
  def metadata():
   calls.append(True)
   if len(calls)>1:raise OSError('closing secondary')
   return original()
  with patch.object(self.admission,'metadata',side_effect=metadata),patch.object(owner,'graph_sql_plan',side_effect=primary),self.assertRaises(KeyboardInterrupt)as error:plan.renew()
  self.assertIs(error.exception,primary);self.assertEqual(len(calls),2)
 def test_pure_graph_membership_uses_original_batch_not_mutable_changes_or_callback(self):
  self.admission.changes=()
  original=self.admission.metadata
  with patch.object(self.admission,'admit',side_effect=AssertionError('No caller callback')),patch.object(self.admission,'metadata',wraps=original)as calls:
   value=self.plan().snapshot()
  self.assertEqual(calls.call_count,4);self.assertEqual(len(value['expected']['whole_source_history']),113)
 def test_nominal_membership_refuses_numeric_boolean_equivalence(self):
  from ashlar.whole_entity import changes_from_batch
  original=owner.graph_sql_plan;change=changes_from_batch(self.admission.batch)[0]
  def checked(*args,**kwargs):
   invalid=replace(change,state=replace(change.state,version=True))
   with self.assertRaises(PermissionError):kwargs['schema_policy'](invalid)
   with self.assertRaises(PermissionError):kwargs['schema_policy'](object())
   return original(*args,**kwargs)
  with patch.object(owner,'graph_sql_plan',side_effect=checked):self.plan()
 def test_graph_transaction_replacement_and_cursor_drift_withhold_plan(self):
  original=owner.graph_sql_plan
  for change in ('replacement','cursor'):
   with self.subTest(change=change):
    original_previous=self.transaction.previous;original_position=self.transaction.position
    # Intercept the owning assembly only to model drift during the pure graph call.
    original_assemble=MedicalPublicationPlan._assemble
    def assemble(plan,metadata):
     def graph(*args,**kwargs):
      result=original(*args,**kwargs)
      if change=='replacement':object.__setattr__(plan,'_transaction',replace(self.transaction))
      else:
       object.__setattr__(self.transaction,'previous','1');object.__setattr__(self.transaction,'position','2')
      return result
     with patch.object(owner,'graph_sql_plan',side_effect=graph):return original_assemble(plan,metadata)
    try:
     with patch.object(MedicalPublicationPlan,'_assemble',assemble),self.assertRaises(PermissionError):self.plan()
    finally:
     object.__setattr__(self.transaction,'previous',original_previous);object.__setattr__(self.transaction,'position',original_position)
 def test_changed_original_during_graph_cannot_release_snapshot(self):
  original=owner.graph_sql_plan
  def changed(*args,**kwargs):
   result=original(*args,**kwargs);self.fixture.paths[0].write_bytes(b'changed');return result
  with patch.object(owner,'graph_sql_plan',side_effect=changed),self.assertRaises((ValueError,PermissionError)):self.plan()
 def test_lower_owner_has_no_driver_command_or_native_import(self):
  tree=ast.parse(Path(owner.__file__).read_text())
  self.assertFalse(any(isinstance(n,ast.ImportFrom)and n.module in ('driver','original_medical_local_publication','pyspark','delta')for n in ast.walk(tree)))
