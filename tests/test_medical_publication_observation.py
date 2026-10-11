"""Real driver/medical/source owners; lower native and ordinary SQL ports inert."""
from contextlib import ExitStack
from dataclasses import replace
import hashlib,json,unittest
from types import SimpleNamespace
from unittest.mock import patch
from ashlar.publication import Descriptor
from ashlar.outbox import OutboxTransaction,PostgresOutbox
from ashlar_host.driver import NativeDriver,PROFILE
from ashlar_host.delta_custody import encoded,sha
from ashlar_host.medical_publication_plan import MedicalPublicationPlan
from ashlar_host.evolution_verification_types import VerificationBounds
import test_medical_publication_sources as sources

class Tests(unittest.TestCase):
 def fixture(self,stack,*,total_bytes=16777216):
  f=sources.Tests();f.setUp();self.addCleanup(f.doCleanups)
  f.selection=replace(f.selection,targets=tuple(replace(t,table='local.original_medical.'+t.table.split('.')[-1])for t in f.selection.targets))
  owner,acks=f.owner(stack);d=owner.driver;d.held=False;d.pin_held=False;f.head=1
  transaction=OutboxTransaction('ashlar-postgresql-outbox/0.1',f.batch.feed,f.batch.epoch,'0','1',hashlib.sha256(f.raw).hexdigest(),f.batch)
  plan=MedicalPublicationPlan(f.admission,transaction,{r:'local.original_medical.'+r for r in sources.ROLES});value=plan.snapshot()
  d.tables=value['tables'];d.columns={r:[tuple(c)for c in cols]for r,cols in value['columns'].items()}
  active={k:value[v]for k,v in {'request':'request','steps':'selected_steps','expected':'expected','publication_id':'publication_id','previous_progress':'previous_progress','progress':'progress','previous_expected':'previous_expected','generated_steps':'generated_steps','elisions':'zero_match_elisions','recorded_at':'recorded_at'}.items()}
  versions={d.tables[r]:1 for r in value['expected']}
  row=dict(publication_id=value['publication_id'],profile_version=PROFILE,table_versions_json=encoded(versions),schema_revisions_json=value['request']['schema_revisions_json'],source_progress_json=encoded(value['progress']),validation_report_json=encoded(dict(complete=True,predecessor=value['request']['predecessor'],request_digest=value['request']['request_digest'],source_admission=value['source_admission'])),recorded_at=value['recorded_at'])
  active['manifest']=row;d.policy=SimpleNamespace(active=active);d.scope_ports={f.batch.feed:{'scope':f.scope,'source':'medical_source','signature':'a'*64}}
  d.source_sessions=owner.sources();d.original_source_sessions=encoded(d.source_sessions.metadata());d.ack_sessions=acks;d.original_ack_sessions=encoded(acks.metadata())
  bounds=VerificationBounds(262144,10000,1048576,total_bytes,1048576,1048576)
  transport=d.transport;transport._registered=True;transport.observation_bounds=None;transport.bounded_calls=[];transport.remaining=[];transport.native_calls=[];transport.fail_role=None;transport.cancel=None
  def detail(target):
   self.assertIs(transport.observation_bounds,bounds);transport.native_calls.append(target.table)
   if transport.fail_role==target.table:raise OSError('native detail')
   return {'id':target.uuid,'location':target.path.as_uri()}
  def capture(target,version,remaining_bytes=None):
   self.assertIs(transport.observation_bounds,bounds);self.assertIs(type(remaining_bytes),int);self.assertGreater(remaining_bytes,0);transport.remaining.append(remaining_bytes);role=target.table.split('.')[-1];transport.bounded_calls.append(role)
   if transport.cancel is not None:raise transport.cancel
   schema=json.loads(value['schemas'][role]);rows=json.loads(encoded(value['expected'][role]));rows.sort(key=encoded)
   return {'schema':[[c[0],c[1].lower()]for c in d.columns[role]],'rows':rows,'row_sha256':sha(encoded(rows))},schema
  transport._detail=detail;transport.original_history=lambda target:[{'version':1},{'version':0}];transport.bounded_snapshot=capture
  transport._snapshot=lambda *args: (_ for _ in ()).throw(AssertionError('Medical legacy snapshot forbidden'))
  stack.enter_context(patch.object(PostgresOutbox,'read',return_value=[transaction]))
  descriptor=Descriptor(value['publication_id'],PROFILE,versions,json.loads(value['request']['schema_revisions_json']),value['progress'],json.loads(row['validation_report_json']),row)
  d.bind_original_medical_publication(plan,bounds=bounds)
  return f,d,plan,bounds,descriptor,value
 def test_real_registered_owner_complete_generated_schemas_and_four_bags(self):
  with ExitStack()as stack:
   f,d,plan,bounds,descriptor,value=self.fixture(stack)
   with d.writer('original',d.context):result=d.observe_original_medical_publication(plan,descriptor,context=d.context,bounds=bounds)
   self.assertEqual(tuple(len(result['bags'][r]['rows'])for r in ('object_current','edge_current','tombstone','whole_source_history')),(51,62,0,113));self.assertEqual(result['schemas'],{r:json.loads(s)for r,s in value['schemas'].items()})
   self.assertFalse(d.pin_held);self.assertIsNone(d.transport.observation_bounds)
   for start in range(0,len(d.transport.remaining),4):
    budget=d.transport.remaining[start:start+4];self.assertEqual(len(budget),4);self.assertEqual(budget[0],bounds.total_bytes);self.assertTrue(all(a>b for a,b in zip(budget,budget[1:])))
  self.assertTrue(f.connections);self.assertTrue(all(c.commits==0 and c.closes==1 for c in f.connections))
 def test_binder_duplicate_query_foreign_bounds_and_active_drift_refuse(self):
  with ExitStack()as stack:
   f,d,plan,bounds,descriptor,value=self.fixture(stack)
   with self.assertRaises(PermissionError):d.bind_original_medical_publication(plan,bounds=bounds)
   with d.writer('original',d.context):
    for replacement in (replace(bounds),object()):
     with self.assertRaises(PermissionError):d.observe_original_medical_publication(plan,descriptor,context=d.context,bounds=replacement)
    d.policy.active['steps']=[]
    with self.assertRaises(PermissionError):d.observe_original_medical_publication(plan,descriptor,context=d.context,bounds=bounds)
 def test_full_schema_metadata_nullability_and_duplicate_bag_drift_refuse(self):
  for drift in ('metadata','nullable','bag','tuple-schema'):
   with self.subTest(drift=drift),ExitStack()as stack:
    f,d,plan,bounds,descriptor,value=self.fixture(stack);original=d.transport.bounded_snapshot
    def changed(*args,**kwargs):
     snapshot,schema=original(*args,**kwargs)
     if args[0].table.endswith('object_current'):
      if drift=='tuple-schema':snapshot['schema']=[tuple(column)for column in snapshot['schema']]
      elif drift=='bag':snapshot['rows'].append(snapshot['rows'][0])
      elif drift=='metadata':schema['fields'][0]['metadata']={'foreign':True}
      else:schema['fields'][0]['nullable']=False
     return snapshot,schema
    d.transport.bounded_snapshot=changed
    with d.writer('original',d.context),self.assertRaises((PermissionError,ValueError)):d.observe_original_medical_publication(plan,descriptor,context=d.context,bounds=bounds)
    self.assertFalse(d.pin_held);self.assertIsNone(d.transport.observation_bounds)
 def test_cancel_attempts_independent_six_native_and_ordinary_closing(self):
  with ExitStack()as stack:
   f,d,plan,bounds,descriptor,value=self.fixture(stack);primary=KeyboardInterrupt('private');d.transport.cancel=primary
   before=len(f.connections)
   with d.writer('original',d.context),self.assertRaises(KeyboardInterrupt)as caught:d.observe_original_medical_publication(plan,descriptor,context=d.context,bounds=bounds)
   self.assertIs(caught.exception,primary);self.assertEqual(set(d.transport.native_calls),set(d.transport.targets));self.assertGreater(len(f.connections),before)
   self.assertFalse(d.pin_held);self.assertTrue(all(c.closes==1 for c in f.connections))
 def test_actual_ack_callback_requires_bound_current_observation_inside_pins(self):
  with ExitStack()as stack:
   f,d,plan,bounds,descriptor,value=self.fixture(stack)
   with d.writer('original',d.context):
    with self.assertRaises(PermissionError):d.admit_publication(f.scope,value['request'],SimpleNamespace(descriptor=descriptor),None,d.context)
    # Actual protected resolver acquired this exact interval, so the observer must reuse it.
    with d.transport.bounded_observations(bounds):
     from ashlar.manifest import manifest_pin_vector
     vector=manifest_pin_vector(dict(descriptor.raw),{t:d.transport.targets[t].uuid for t in descriptor.versions},authority='private-local-process')
     with d.hold(vector,context=d.context):d.admit_publication(f.scope,value['request'],SimpleNamespace(descriptor=descriptor),None,d.context)
   self.assertGreaterEqual(len(d.transport.bounded_calls),8)

 def test_opening_first_native_refusal_still_closes_all_six_and_source_ack(self):
  with ExitStack()as stack:
   f,d,plan,bounds,descriptor,value=self.fixture(stack);original=d.transport._detail;calls=[]
   def detail(target):
    calls.append(target.table)
    if len(calls)==1:raise OSError('opening first native')
    return original(target)
   d.transport._detail=detail
   with d.writer('original',d.context),self.assertRaises(OSError):d.observe_original_medical_publication(plan,descriptor,context=d.context,bounds=bounds)
   self.assertEqual(set(calls[1:]),set(d.transport.targets));self.assertEqual(set(d.transport.bounded_calls),set(value['expected']));self.assertTrue(any(c.role=='medical_reader'for c in f.connections));self.assertTrue(any(c.role=='medical_ack'for c in f.connections))
   self.assertTrue(all(c.closes==1 for c in f.connections));self.assertIsNone(d.transport.observation_bounds)
 def test_opening_capture_cancel_closes_all_four_roles_before_pin_release(self):
  with ExitStack()as stack:
   f,d,plan,bounds,descriptor,value=self.fixture(stack);original=d.transport.bounded_snapshot;calls=[];primary=GeneratorExit('private')
   def capture(target,*args,**kwargs):
    self.assertTrue(d.pin_held);role=target.table.split('.')[-1];calls.append(role)
    if len(calls)==1:raise primary
    if len(calls)==2:raise OSError('first role closing')
    return original(target,*args,**kwargs)
   d.transport.bounded_snapshot=capture
   with d.writer('original',d.context),self.assertRaises(GeneratorExit)as caught:d.observe_original_medical_publication(plan,descriptor,context=d.context,bounds=bounds)
   self.assertIs(caught.exception,primary);self.assertEqual(calls[1:5],['object_current','edge_current','tombstone','whole_source_history']);self.assertFalse(d.pin_held)
   self.assertIsNone(d._held_original_vector);self.assertEqual(set(d.transport.native_calls),set(d.transport.targets));self.assertTrue(all(c.closes==1 for c in f.connections))

 def test_replaced_binding_owner_factory_or_native_identity_refuses(self):
  with ExitStack()as stack:
   f,d,plan,bounds,descriptor,value=self.fixture(stack)
   original=d._medical_publication_binding
   from types import MappingProxyType
   d._medical_publication_binding=MappingProxyType(dict(original))
   with d.writer('original',d.context),self.assertRaises(PermissionError):d.observe_original_medical_publication(plan,descriptor,context=d.context,bounds=bounds)
   d._medical_publication_binding=original
   registration=d.source_sessions.registrations[0];factory=registration.connection_factory
   object.__setattr__(registration,'connection_factory',lambda context: (_ for _ in ()).throw(AssertionError('foreign callback')))
   with d.writer('original',d.context),self.assertRaises(PermissionError):d.observe_original_medical_publication(plan,descriptor,context=d.context,bounds=bounds)
   object.__setattr__(registration,'connection_factory',factory)
   target=original['targets'][0];uuid=target.uuid;object.__setattr__(target,'uuid','replacement')
   with d.writer('original',d.context),self.assertRaises(PermissionError):d.observe_original_medical_publication(plan,descriptor,context=d.context,bounds=bounds)
   object.__setattr__(target,'uuid',uuid)
 def test_public_resolver_frozen_descriptor_complete_field_correspondence(self):
  with ExitStack()as stack:
   f,d,plan,bounds,descriptor,value=self.fixture(stack)
   from ashlar.publication import resolve_publication,Snapshot
   from ashlar.manifest import manifest_pin_vector
   d.descriptors=lambda publication_id:[dict(descriptor.raw)]
   d.inspect_snapshot=lambda table,version:Snapshot(table,d.transport.targets[table].uuid,version)
   vector=manifest_pin_vector(dict(descriptor.raw),{t:d.transport.targets[t].uuid for t in descriptor.versions},authority='private-local-process')
   with d.writer('original',d.context),d.hold(vector,context=d.context):
    resolved=resolve_publication(d,descriptor.publication_id,{t:d.transport.targets[t].uuid for t in descriptor.versions},context=d.context,supported_profiles=(PROFILE,),supported_revisions={feed:(revision,)for feed,revision in descriptor.revisions.items()})
    self.assertIsNot(type(resolved.descriptor.source_progress),dict)
    d.observe_original_medical_publication(plan,resolved.descriptor,context=d.context,bounds=bounds)
    wrong=replace(resolved.descriptor,source_progress={})
    with self.assertRaises(PermissionError):d.observe_original_medical_publication(plan,wrong,context=d.context,bounds=bounds)

 def test_changed_manifest_cannot_reuse_another_original_held_vector(self):
  with ExitStack()as stack:
   f,d,plan,bounds,descriptor,value=self.fixture(stack)
   from ashlar.manifest import manifest_pin_vector
   vector=manifest_pin_vector(dict(descriptor.raw),{t:d.transport.targets[t].uuid for t in descriptor.versions},authority='private-local-process')
   with d.writer('original',d.context),d.hold(vector,context=d.context):
    row=dict(descriptor.raw);row['recorded_at']='1791547200000001';new=replace(descriptor,raw=row);d.policy.active['manifest']=row
    before=len(d.transport.bounded_calls)
    with self.assertRaises(PermissionError):d.observe_original_medical_publication(plan,new,context=d.context,bounds=bounds)
    self.assertEqual(len(d.transport.bounded_calls),before)
    d.policy.active['manifest']=dict(descriptor.raw)
 def test_deleted_binding_cannot_suppress_original_pin_closings(self):
  with ExitStack()as stack:
   f,d,plan,bounds,descriptor,value=self.fixture(stack)
   from ashlar.manifest import manifest_pin_vector
   vector=manifest_pin_vector(dict(descriptor.raw),{t:d.transport.targets[t].uuid for t in descriptor.versions},authority='private-local-process')
   primary=KeyboardInterrupt('private')
   with d.writer('original',d.context),self.assertRaises(KeyboardInterrupt)as caught:
    with d.hold(vector,context=d.context):
     d.transport.bounded_calls.clear();d.transport.native_calls.clear();del d._medical_publication_binding
     raise primary
   self.assertIs(caught.exception,primary);self.assertEqual(d.transport.bounded_calls,['object_current','edge_current','tombstone','whole_source_history']);self.assertEqual(set(d.transport.native_calls),set(d.transport.targets));self.assertTrue(all(c.closes==1 for c in f.connections))
 def test_medical_read_dispatch_does_not_require_publication_binding(self):
  import test_medical_read_sources as reads
  reader=reads.Tests();self.addCleanup(reader.doCleanups);f=reader.fixture();reader.authority(f);d=f.driver
  self.assertFalse(hasattr(d,'_medical_publication_binding'))
  # Isolate only the body below the real profile/query dispatch: the acquired actual
  # reader owner still renews all native/source/ACK gates around this invocation.
  with d.query_read_interval(d.context):
   with patch.object(NativeDriver,'_validate_manifest_body',return_value=None)as body:
    d.validate_manifest(d.policy.active['manifest'],d.context);body.assert_called_once()
   d.pin_held=True
   d.renew_original_graph_read_source(f.vector,context=d.context);d.pin_held=False
  self.assertTrue(f.events)

 def test_factory_drift_and_cancel_close_original_reader_ack_and_every_native_owner(self):
  with ExitStack()as stack:
   f,d,plan,bounds,descriptor,value=self.fixture(stack)
   from ashlar.manifest import manifest_pin_vector
   vector=manifest_pin_vector(dict(descriptor.raw),{t:d.transport.targets[t].uuid for t in descriptor.versions},authority='private-local-process')
   primary=KeyboardInterrupt('private');foreign=[]
   def forbidden(context):foreign.append(True);raise AssertionError('substituted callback')
   with d.writer('original',d.context),self.assertRaises(KeyboardInterrupt)as caught:
    with d.hold(vector,context=d.context):
     f.connections.clear();d.transport.bounded_calls.clear();d.transport.native_calls.clear()
     object.__setattr__(d.source_sessions.registrations[0],'connection_factory',forbidden)
     object.__setattr__(d.ack_sessions.registrations[0],'connection_factory',forbidden)
     object.__setattr__(d.ack_sessions.registrations[0],'source',object())
     raise primary
   self.assertIs(caught.exception,primary);self.assertEqual(foreign,[])
   self.assertEqual(set(d.transport.bounded_calls),set(value['expected']));self.assertEqual(set(d.transport.native_calls),set(d.transport.targets))
   self.assertTrue(any(c.role=='medical_reader'for c in f.connections));self.assertTrue(any(c.role=='medical_ack'for c in f.connections));self.assertTrue(all(c.closes==1 for c in f.connections))

 def test_malformed_mutated_manifest_cannot_skip_original_closing_owners(self):
  with ExitStack()as stack:
   f,d,plan,bounds,descriptor,value=self.fixture(stack)
   from ashlar.manifest import manifest_pin_vector
   vector=manifest_pin_vector(dict(descriptor.raw),{t:d.transport.targets[t].uuid for t in descriptor.versions},authority='private-local-process')
   primary=KeyboardInterrupt('private')
   with d.writer('original',d.context),self.assertRaises(KeyboardInterrupt)as caught:
    with d.hold(vector,context=d.context):
     f.connections.clear();d.transport.bounded_calls.clear();d.transport.native_calls.clear()
     d.policy.active['manifest']['table_versions_json']='{'
     raise primary
   self.assertIs(caught.exception,primary);self.assertEqual(d.transport.bounded_calls,['object_current','edge_current','tombstone','whole_source_history']);self.assertEqual(set(d.transport.native_calls),set(d.transport.targets))
   self.assertTrue(any(c.role=='medical_reader'for c in f.connections));self.assertTrue(any(c.role=='medical_ack'for c in f.connections));self.assertTrue(all(c.closes==1 for c in f.connections))
   self.assertFalse(d.pin_held);self.assertIsNone(d._held_original_vector)

if __name__=='__main__':unittest.main()
