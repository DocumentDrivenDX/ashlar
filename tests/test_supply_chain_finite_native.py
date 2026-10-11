"""Inert finite native-owner controls; no public producer or Spark qualification."""
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase,mock
import copy,json,tempfile,os
from dataclasses import replace
from ashlar.staging import batch_row
from ashlar_host import supply_chain_finite_source as source_owner
from ashlar_host import supply_chain_publication as publication
from ashlar_host import finite_publication as shared_publication
from ashlar_host import supply_chain_native as native
from ashlar_host.resources import RESOURCE_ROOT
from ashlar_host.schema_rows import fixture_columns
from ashlar_host.supply_chain_projection import original_supply_chain_oracle
from ashlar_host.supply_chain_request import SOURCE_SYSTEM
from ashlar_host.supply_chain_producer import SupplyChainProducerConfig
from ashlar.weft_count_star_distribution import CountStarDistributionPaths
ROOT=Path(__file__).resolve().parents[1]
class Controls(TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name).resolve();self.root.chmod(0o700)
  original=ROOT/'examples/domain-packs/supply-chain/upstream';self.model=(original/'ontology.json').read_bytes();self.graph=(original/'graph/fixture.json').read_bytes()
  self.model_path=self.root/'model';self.graph_path=self.root/'graph';self.model_path.write_bytes(self.model);self.graph_path.write_bytes(self.graph)
  graph=json.loads(self.graph)
  # Separately authored inert producer shape, never actual public validation.
  self.receipt={'profile':'ashlar-supply-chain-public-dataset/0.1','umfRevision':source_owner.UMF_REVISION,'sourceSha256':source_owner.SOURCE_SHA,'graphSha256':source_owner.GRAPH_SHA,
   'receipt':{'scope':'supplied-dataset-only','input':{'scope':{'id':'ashlar-original-supply-chain-fixture','closure':'supplied-dataset-only'}},'datasetValidation':{'valid':True,'complete':True,'diagnostics':[]},
    'records':[{'instanceId':obj['key'],'result':{'validation':{'valid':True}}}for obj in graph['objects']],
    'keys':[{'instanceId':str(i)}for i in range(20)],'relationships':[{'instanceId':edge['key'],'sourceInstanceId':edge['source'],'targetInstanceId':edge['target']}for edge in graph['edges']]}}
  self.producer=lambda *args:json.dumps(self.receipt,separators=(',',':')).encode()
  self.source=source_owner.FiniteSupplyChainSource(self.model_path,self.graph_path,current_dataset=self.producer)
  self.columns=fixture_columns(RESOURCE_ROOT)
 def test_complete_independent_original_native_bags_and_nulls(self):
  rows=original_supply_chain_oracle(self.model,self.graph,self.source.bindings,self.source.batch,self.columns)
  self.assertEqual({r:len(v)for r,v in rows.items()},{'object_current':20,'edge_current':23,'tombstone':0,'whole_source_history':43})
  props=[json.loads(r['props_json'])for r in rows['object_current']];self.assertEqual(sum(value is None for row in props for value in row.values()),2)
  source_events=[obj for obj in json.loads(self.graph)['objects']if obj['type']['element']=='events']
  self.assertEqual(len({obj['key']for obj in source_events}),2)
  self.assertEqual(sum(obj['values']['events.upstream_event_id']=='[42,0,"SOURCE1"]'for obj in source_events),2)
 def test_source_missing_relationship_or_invalid_record_refuses(self):
  for alteration in ('edge','record'):
   value=copy.deepcopy(self.receipt)
   if alteration=='edge':value['receipt']['relationships'].pop()
   else:value['receipt']['records'][0]['result']['validation']['valid']=False
   with self.assertRaises(ValueError):source_owner.FiniteSupplyChainSource(self.model_path,self.graph_path,current_dataset=lambda *a:json.dumps(value).encode())
 def test_closing_source_identity_and_actual_receipt_drift(self):
  self.model_path.rename(self.root/'saved');self.model_path.write_bytes(self.model)
  with self.assertRaises(ValueError):self.source.renew()
  source=source_owner.FiniteSupplyChainSource(self.model_path,self.graph_path,current_dataset=self.producer)
  self.receipt['uninterpreted_extension']={'meaning':'retained'}
  with self.assertRaises(ValueError):source.renew()
 def test_generic_request_never_acquires_outbox_checkpoint(self):
  request=native._request(self.source.batch);self.source.admit_request(request)
  request['source_checkpoint_json']='{}'
  with self.assertRaises(ValueError):self.source.admit_request(request)
 def test_actual_lease_replacement_and_conflicting_owner_refuse(self):
  lease=native.PrivateFiniteLease(self.root,create=True);self.addCleanup(lease.close);lease.renew()
  with self.assertRaises(BlockingIOError):native.PrivateFiniteLease(self.root,create=False)
  (self.root/'.finite-installation.lock').rename(self.root/'saved-lock');(self.root/'.finite-installation.lock').write_bytes(b'')
  with self.assertRaises(PermissionError):lease.renew()
 def test_restore_missing_committed_phases_does_not_apply(self):
  tables={r:'local.finite.'+r for r in publication.GRAPH_ROLES|{'attempts','manifest'}};context=object();transport=SimpleNamespace(journal_path=self.root/'journal',targets={tables['attempts']:SimpleNamespace(table=tables['attempts'],uuid='original')})
  driver=publication.FiniteSupplyChainDriver(transport,SimpleNamespace(active=None),context,tables,self.columns,self.source,'pub',current_admission=lambda *a:None)
  request=native._request(self.source.batch)
  session=mock.MagicMock();session.read.return_value=[];store=mock.MagicMock();store.session.return_value.__enter__.return_value=session
  with mock.patch.object(shared_publication,'DeltaAttemptStore',return_value=store),mock.patch.object(shared_publication.LocalDeltaEffects,'run')as effects:
   with self.assertRaises(ValueError):driver.restore_committed(request)
   effects.assert_not_called()
 def test_complete_projection_bag_rejects_duplicate_or_missing_occurrence(self):
  tables={r:'local.finite.'+r for r in publication.GRAPH_ROLES|{'attempts','manifest'}};driver=publication.FiniteSupplyChainDriver(SimpleNamespace(),SimpleNamespace(),object(),tables,self.columns,self.source,'pub',current_admission=lambda *a:None)
  versions={tables[r]:1 for r in publication.GRAPH_ROLES};altered=copy.deepcopy(driver.expected);altered['edge_current'][-1]=copy.deepcopy(altered['edge_current'][0])
  with mock.patch.object(driver,'_rows',side_effect=lambda r,v:altered[r]):
   with self.assertRaises(ValueError):driver._check(versions,driver.expected)
 def test_producer_receipt_bound_and_capacity_exact_configuration(self):
  producer=SupplyChainProducerConfig(Path('/umf'),Path('/bun'),Path('/git'),30,1024,8388608)
  capacity={'profile':'ashlar-private-local-operation-capacity/0.1','max_intent_bytes':8388608}
  config=native.SupplyChainNativeConfig(self.root/'out',self.root/'jars',self.root/'pack',self.model_path,self.graph_path,producer,CountStarDistributionPaths(Path('/index'),Path('/installed')),capacity)
  capacity['max_intent_bytes']=1;self.assertEqual(config.operation_capacity['max_intent_bytes'],8388608)
  with self.assertRaises(ValueError):SupplyChainProducerConfig(Path('/umf'),Path('/bun'),Path('/git'),True,1024,8388608)

 def test_writer_independent_admission_refusal_before_effects(self):
  tables={r:'local.finite.'+r for r in publication.GRAPH_ROLES|{'attempts','manifest'}}
  transport=SimpleNamespace(journal_path=self.root/'journal')
  refused=PermissionError('current retention owner refused')
  def admit(*args):raise refused
  driver=publication.FiniteSupplyChainDriver(transport,SimpleNamespace(),object(),tables,self.columns,self.source,'pub',current_admission=admit)
  driver.request=native._request(self.source.batch)
  with mock.patch.object(shared_publication.LocalDeltaEffects,'run')as effects:
   with self.assertRaises(PermissionError)as caught:
    with driver.writer(driver.request['stream'],driver.context):self.fail('Refused authority reached body')
   self.assertIs(caught.exception,refused);effects.assert_not_called()
  self.assertFalse(driver.held)
 def test_writer_closing_cancellation_priority_and_original_identity(self):
  tables={r:'local.finite.'+r for r in publication.GRAPH_ROLES|{'attempts','manifest'}}
  for original,closing in ((ValueError('body'),KeyboardInterrupt('closing')),(SystemExit('body'),KeyboardInterrupt('closing'))):
   driver=publication.FiniteSupplyChainDriver(SimpleNamespace(journal_path=self.root/'journal'),SimpleNamespace(),object(),tables,self.columns,self.source,'pub',current_admission=lambda *a:None)
   driver.request=native._request(self.source.batch)
   with mock.patch.object(self.source,'renew',side_effect=[None,closing]):
    with self.assertRaises(BaseException)as caught:
     with driver.writer(driver.request['stream'],driver.context):raise original
   self.assertIs(caught.exception,closing if isinstance(original,Exception)else original)
   self.assertFalse(driver.held)

 def test_actual_producer_shape_and_diagnostics_refusal(self):
  self.assertEqual(json.loads(self.source.receipt)['receipt']['datasetValidation'],{'valid':True,'complete':True,'diagnostics':[]})
  self.receipt['receipt']['datasetValidation']['diagnostics']=[{'code':'independent-refusal'}]
  with self.assertRaises(ValueError):source_owner.FiniteSupplyChainSource(self.model_path,self.graph_path,current_dataset=self.producer)

 def test_exact_original_changes_reject_executable_equality_and_subclasses(self):
  class Forged:
   def __eq__(self,other):return True
  with self.assertRaises(ValueError):self.source.admit(Forged())
  original=self.source.changes[0]
  class ForgedString(str):
   def __eq__(self,other):return True
  with self.assertRaises(ValueError):self.source.admit(replace(original,operation=ForgedString(original.operation)))
  self.source.admit(original)
 def test_lease_original_root_replacement_cannot_reuse_original_lock(self):
  lease=native.PrivateFiniteLease(self.root,create=True);self.addCleanup(lease.close)
  retained=self.root.parent/(self.root.name+'-retained');self.root.rename(retained)
  self.addCleanup(lambda:__import__('shutil').rmtree(retained))
  self.root.mkdir(mode=0o700);os.link(retained/'.finite-installation.lock',self.root/'.finite-installation.lock')
  with self.assertRaises(PermissionError):lease.renew()
 def test_opening_lease_cancellation_survives_fresh_cleanup_cancellation(self):
  original_close=os.close
  for body,closing in ((KeyboardInterrupt('opening'),SystemExit('closing')),(ValueError('opening'),KeyboardInterrupt('closing'))):
   opened=[];original_open=os.open
   def opening(*args,**kwargs):
    fd=original_open(*args,**kwargs);opened.append(fd);return fd
   def close(fd):original_close(fd);raise closing
   with mock.patch.object(native.fcntl,'flock',side_effect=body),mock.patch.object(native.os,'open',side_effect=opening),mock.patch.object(native.os,'close',side_effect=close):
    with self.assertRaises(BaseException)as caught:native.PrivateFiniteLease(self.root,create=True)
   self.assertIs(caught.exception,closing if isinstance(body,Exception)else body)
   self.assertEqual(len(opened),1)
   with self.assertRaises(OSError):os.fstat(opened[0])
   (self.root/'.finite-installation.lock').unlink()

 def test_public_request_gate_rejects_forged_batch_json_subclass(self):
  import hashlib
  class ForgedBatch(str):
   def __ne__(self,other):return False
  request=native._request(self.source.batch);request['source_batch_json']=ForgedBatch('{"forged":true}')
  request['request_digest']=hashlib.sha256(publication.encoded({k:v for k,v in request.items()if k!='request_digest'}).encode()).hexdigest()
  with mock.patch.object(self.source,'metadata')as metadata:
   with self.assertRaises(ValueError):self.source.admit_request(request)
   metadata.assert_not_called()
  self.source.admit_request(native._request(self.source.batch))
