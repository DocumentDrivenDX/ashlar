"""Owned inert publication controls and actual POSIX leases; no native claim."""
import base64,copy,hashlib,json,unittest
from types import SimpleNamespace
from unittest import mock
import test_finite_pack as fixtures
from ashlar_host.finite_projection import original_finite_pack_oracle
from ashlar_host.finite_publication import FiniteFileDriver,GRAPH_ROLES
from ashlar_host import finite_publication as owner
from ashlar_host.finite_native import finite_pack_request,FiniteInstallationSourcePolicy
from ashlar_host.supply_chain_native import PrivateFiniteLease
from ashlar_host.schema_rows import fixture_columns
from ashlar_host.resources import RESOURCE_ROOT
from ashlar_host.evolution_admission import original_equal

class Controls(unittest.TestCase):
 def setUp(self):
  self.helper=fixtures.Tests();self.helper.setUp();self.addCleanup(self.helper.doCleanups);self.root=self.helper.root;self.root.chmod(0o700);self.columns=fixture_columns(RESOURCE_ROOT)
 def source(self,name):return self.helper.source(name)[0]
 def driver(self,source,admit=lambda *a:None):
  tables={r:'local.finite.'+r for r in GRAPH_ROLES|{'attempts','manifest'}}
  transport=SimpleNamespace(journal_path=self.root/'journal',targets={tables['attempts']:SimpleNamespace(table=tables['attempts'],uuid='original')})
  return FiniteFileDriver(transport,SimpleNamespace(active=None),object(),tables,self.columns,source,'original-publication',current_admission=admit)
 def test_complete_original_bags_equal_independent_event_state_and_preserve_raw_history(self):
  for name,nodes,edges in (('archaeology',39,46),('ecology',40,54)):
   source=self.source(name);rows=original_finite_pack_oracle(source.definition,source.model,source.graph,source.bindings,source.batch,self.columns)
   self.assertEqual({r:len(v)for r,v in rows.items()},{'object_current':nodes,'edge_current':edges,'tombstone':0,'whole_source_history':nodes+edges})
   states={ (c.state.key.kind,str(c.state.key.type_id),str(c.state.key.id)):c.state for c in source.changes}
   for role,kind,typed in (('object_current','object','type_id'),('edge_current','edge','rel_type_id')):
    for row in rows[role]:
     state=states[(kind,row[typed],row['id'])];self.assertEqual(row['props_json'],state.props_json);self.assertEqual(row['retained_json'],state.retained_json);self.assertEqual(row['entity_version'],str(state.version));self.assertEqual(row['schema_revision'],state.schema_revision)
     if kind=='edge':self.assertEqual([(row['source_type'],row['source_id']),(row['target_type'],row['target_id'])],[(str(x.type_id),str(x.id))for x in state.endpoints])
   for row,record in zip(rows['whole_source_history'],source.batch.records):
    self.assertEqual(base64.b64decode(row['raw_base64']),record.raw);self.assertEqual(row['digest'],hashlib.sha256(record.raw).hexdigest());self.assertEqual(json.loads(row['change_json'])['raw_digest'],row['digest'])
   self.assertEqual(sum(v is None for row in rows['object_current']for v in json.loads(row['props_json']).values()),source.definition.facts()['present_nulls'])
 def test_original_projection_refuses_changed_binding_batch_and_model(self):
  source=self.source('ecology');bindings=copy.deepcopy(source.bindings);bindings['entities'][0]['id']=True
  with self.assertRaises(ValueError):original_finite_pack_oracle(source.definition,source.model,source.graph,bindings,source.batch,self.columns)
  with self.assertRaises(ValueError):original_finite_pack_oracle(source.definition,source.model+b' ',source.graph,source.bindings,source.batch,self.columns)
 def test_actual_source_lease_conflict_loss_and_context_refuse(self):
  source=self.source('archaeology');lease=PrivateFiniteLease(self.root,create=True);self.addCleanup(lease.close);context=object();policy=FiniteInstallationSourcePolicy(lease,source.definition,context)
  policy.admit_source(source.definition.facts(),context)
  with self.assertRaises(PermissionError):policy.admit_source(source.definition.facts(),object())
  with self.assertRaises(BlockingIOError):PrivateFiniteLease(self.root,create=False)
  lease.close()
  with self.assertRaises(PermissionError):policy.admit_source(source.definition.facts(),context)
 def test_opening_refusal_and_closing_cancellation_withhold_effects(self):
  source=self.source('ecology');opening=PermissionError('current writer refusal')
  def refuse(*a):raise opening
  driver=self.driver(source,refuse);driver.request=finite_pack_request(source)
  with mock.patch.object(owner.LocalDeltaEffects,'run')as effects:
   with self.assertRaises(PermissionError)as caught:
    with driver.writer(driver.request['stream'],driver.context):self.fail('Unauthorized writer')
   self.assertIs(caught.exception,opening);effects.assert_not_called();self.assertFalse(driver.held)
  count=0;cancel=KeyboardInterrupt('closing current authority')
  def closing(*a):
   nonlocal count
   count+=1
   if count>1:raise cancel
  driver=self.driver(source,closing);driver.request=finite_pack_request(source)
  with self.assertRaises(KeyboardInterrupt)as caught:
   with driver.writer(driver.request['stream'],driver.context):pass
  self.assertIs(caught.exception,cancel);self.assertFalse(driver.held)
 def test_actual_writer_stream_close_cancellation_preserves_body_and_releases_lock(self):
  from pathlib import Path
  import fcntl
  source=self.source('ecology');driver=self.driver(source);driver.request=finite_pack_request(source);original_open=Path.open
  lockpath=self.root/'.finite-source-writer.lock'
  for body,closing in ((KeyboardInterrupt('body'),SystemExit('lock close')),(ValueError('body'),KeyboardInterrupt('lock close'))):
   class Stream:
    def __init__(self,stream):self.stream=stream
    def fileno(self):return self.stream.fileno()
    def __enter__(self):return self
    def __exit__(self,*args):self.stream.close();raise closing
   def opened(path,*args,**kwargs):
    stream=original_open(path,*args,**kwargs);return Stream(stream)if path==lockpath else stream
   with mock.patch.object(Path,'open',opened):
    with self.assertRaises(type(body if isinstance(body,BaseException)and not isinstance(body,Exception)else closing))as caught:
     with driver.writer(driver.request['stream'],driver.context):raise body
   self.assertIs(caught.exception,body if not isinstance(body,Exception)else closing);self.assertFalse(driver.held)
   with original_open(lockpath,'a')as stream:fcntl.flock(stream,fcntl.LOCK_EX|fcntl.LOCK_NB);fcntl.flock(stream,fcntl.LOCK_UN)
 def test_missing_committed_reopen_never_applies_and_complete_bag_refuses_loss(self):
  source=self.source('archaeology');driver=self.driver(source);request=finite_pack_request(source)
  session=mock.MagicMock();session.read.return_value=[];store=mock.MagicMock();store.session.return_value.__enter__.return_value=session
  with mock.patch.object(owner,'DeltaAttemptStore',return_value=store),mock.patch.object(owner.LocalDeltaEffects,'run')as effects:
   with self.assertRaises(ValueError):driver.restore_committed(request)
   effects.assert_not_called()
  driver=self.driver(source);rows=copy.deepcopy(driver.expected);rows['edge_current'].pop()
  with mock.patch.object(driver,'_rows',side_effect=lambda r,v:rows[r]):
   with self.assertRaises(ValueError):driver._check({driver.tables[r]:1 for r in GRAPH_ROLES},driver.expected)

class NativeCompositionControls(unittest.TestCase):
 def test_source_factory_cancellation_closes_actual_acquired_lease_before_spark(self):
  import tempfile
  from pathlib import Path
  from ashlar_host import finite_native as native
  from ashlar_host.finite_pack import FinitePackDefinition
  from ashlar_host.finite_dataset import FiniteDatasetConfig
  with tempfile.TemporaryDirectory()as directory:
   root=Path(directory).resolve();config=native.FinitePackNativeConfig(root/'output',root/'jars',root/'pack',root/'model',root/'graph',FiniteDatasetConfig(FinitePackDefinition('ecology'),root,Path('/usr/bin/true'),Path('/usr/bin/git'),5,1048576,33554432))
   originals=Path(__file__).resolve().parents[1]/'examples/domain-packs/ecology/upstream'
   for destination,file in ((config.pack,'pack.json'),(config.model,'ontology.json'),(config.graph,'graph/fixture.json')):destination.write_bytes((originals/file).read_bytes())
   cancel=KeyboardInterrupt('actual producer construction');acquired=[];original=native.PrivateFiniteLease
   def lease(*args,**kwargs):
    value=original(*args,**kwargs);acquired.append(value);return value
   with mock.patch.object(native,'finite_runtime',return_value=[]),mock.patch.object(native,'PrivateFiniteLease',side_effect=lease),mock.patch.object(native,'HeldFiniteDataset',side_effect=cancel),mock.patch.object(native,'open_finite_spark')as spark:
    with self.assertRaises(KeyboardInterrupt)as caught:native.publish_finite_pack_native(config)
    self.assertIs(caught.exception,cancel);spark.assert_not_called();self.assertIsNone(acquired[0].fd)
   reopened=original(config.output,create=False);reopened.close()
 def test_closing_runtime_cancellation_withholds_report_and_releases_actual_lease(self):
  import tempfile
  from pathlib import Path
  from ashlar_host import finite_native as native
  from ashlar_host.finite_pack import FinitePackDefinition
  from ashlar_host.finite_dataset import FiniteDatasetConfig
  with tempfile.TemporaryDirectory()as directory:
   root=Path(directory).resolve();config=native.FinitePackNativeConfig(root/'output',root/'jars',root/'pack',root/'model',root/'graph',FiniteDatasetConfig(FinitePackDefinition('archaeology'),root,Path('/usr/bin/true'),Path('/usr/bin/git'),5,1048576,33554432))
   originals=Path(__file__).resolve().parents[1]/'examples/domain-packs/archaeology/upstream'
   for destination,file in ((config.pack,'pack.json'),(config.model,'ontology.json'),(config.graph,'graph/fixture.json')):destination.write_bytes((originals/file).read_bytes())
   cancel=SystemExit('closing selected runtime drift');opening=ValueError('producer construction failure')
   with mock.patch.object(native,'finite_runtime',side_effect=[[],cancel]),mock.patch.object(native,'HeldFiniteDataset',side_effect=opening),mock.patch.object(native,'open_finite_spark')as spark:
    with self.assertRaises(SystemExit)as caught:native.publish_finite_pack_native(config)
    self.assertIs(caught.exception,cancel);spark.assert_not_called();self.assertFalse((config.output/'report.json').exists())
   reopened=PrivateFiniteLease(config.output,create=False);reopened.close()
 def test_public_spark_construction_refuses_invalid_configuration_before_sdk(self):
  from ashlar_host import supply_chain_native as runtime
  failure=ValueError('configuration drift')
  class Configuration:
   def __post_init__(self):raise failure
  with mock.patch.object(runtime,'_spark')as sdk:
   with self.assertRaises(ValueError)as caught:runtime.open_finite_spark(Configuration(),[])
   self.assertIs(caught.exception,failure);sdk.assert_not_called()
 def test_legacy_wrapper_preserves_exact_expected_bags_effect_plan_and_applied_bytes(self):
  import test_supply_chain_finite_native as legacy
  from ashlar_host import supply_chain_native
  from ashlar_host.supply_chain_publication import FiniteSupplyChainDriver
  from ashlar_host.supply_chain_projection import original_supply_chain_oracle
  fixture=legacy.Controls();fixture.setUp();self.addCleanup(fixture.doCleanups)
  tables={r:'local.finite.'+r for r in GRAPH_ROLES|{'attempts','manifest'}};context=object();transport=SimpleNamespace(journal_path=fixture.root/'journal');policy=SimpleNamespace(active=None);request=supply_chain_native._request(fixture.source.batch)
  driver=FiniteSupplyChainDriver(transport,policy,context,tables,fixture.columns,fixture.source,'original-pub',current_admission=lambda *a:None)
  expected=original_supply_chain_oracle(fixture.model,fixture.graph,fixture.source.bindings,fixture.source.batch,fixture.columns);self.assertEqual(driver.expected,expected)
  prior={tables[r]:0 for r in GRAPH_ROLES};current={tables[r]:1 for r in GRAPH_ROLES}
  with mock.patch.object(driver,'_pins',return_value=prior),mock.patch.object(driver,'_rows',return_value=[]):driver.prepare_original(request)
  from ashlar.apply import empty_state
  from ashlar_host.graph_sql import graph_sql_plan
  from ashlar_host.driver import local_effect_plan
  from ashlar_host.supply_chain_projection import CLOCK
  _,sql=graph_sql_plan(empty_state(),fixture.source.batch,{r:tables[r]for r in GRAPH_ROLES},materialized_at=CLOCK,schema_policy=fixture.source.admit)
  steps,elisions=local_effect_plan(sql,{r:[]for r in GRAPH_ROLES},{r:tables[r]for r in GRAPH_ROLES});self.assertEqual(driver.steps,steps);self.assertEqual(driver.elisions,elisions)
  proof={'original':'effect-proof'}
  with mock.patch.object(owner.LocalDeltaEffects,'run',return_value=proof),mock.patch.object(driver,'_pins',return_value=current),mock.patch.object(driver,'_check'):
   with driver.writer(request['stream'],context):artifact=driver.apply(request,context)
  manifest=json.loads(artifact)['manifest'];metadata=fixture.source.metadata()
  self.assertEqual(json.loads(artifact)['effects'],proof);self.assertEqual(manifest['recorded_at'],'1791547200000000');self.assertEqual(json.loads(manifest['source_progress_json']),{fixture.source.batch.feed:{'profile':'ashlar-immutable-file-replay/0.1','source_transaction_sha256':metadata['source_transaction_sha256'],'request_digest':request['request_digest']}})
  report=json.loads(manifest['validation_report_json']);self.assertEqual(report['source_admission'],metadata);self.assertEqual(report['source_oracle_sha256'],hashlib.sha256(owner.encoded(expected).encode()).hexdigest());self.assertEqual(report['zero_match_elisions'],elisions);self.assertEqual(report['ack_profile'],'immutable-file-replay-only')
