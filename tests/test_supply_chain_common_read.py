"""Real leases/journal/producer lifecycle and core restore; SDK/producer ports inert."""
from pathlib import Path
from contextlib import contextmanager
from types import SimpleNamespace
import json,hashlib,uuid,sqlite3,sys,os,unittest
from unittest import mock
import test_retained_supply_chain_producer as producer_fixture
from ashlar_host import supply_chain_common_read as owner
from ashlar_host import supply_chain_native as native
from ashlar_host import finite_publication
from ashlar_host.delta_custody import encoded,LOCAL_OPERATION_CAPACITY_8M
from ashlar_host.evolution_verification_types import VerificationBounds
from ashlar_host.supply_chain_projection import original_supply_chain_oracle
from ashlar_host.supply_chain_request import SOURCE_SYSTEM,SOURCE_SHA
from ashlar_host.supply_chain_producer import reopen_original_supply_chain_source
from ashlar.attempt_store import PHASES

class Tests(unittest.TestCase):
 def setUp(self):
  self.f=producer_fixture.Controls();self.f.setUp();self.addCleanup(self.f.doCleanups);self.root=self.f.root/'native';self.root.mkdir(mode=0o700);self.runtime=self.f.root/'runtime';self.runtime.mkdir(mode=0o700);self.jars=self.f.root/'jars';self.jars.mkdir();self.java=self.f.root/'java';self.java.mkdir();(self.java/'release').write_text('JAVA_VERSION="21.0.12"\n')
  invocation=self.f.producer().original_invocation();self.source=reopen_original_supply_chain_source(invocation);self.columns=native.finite_materialization_columns();self.expected=original_supply_chain_oracle(self.source.model,self.source.graph,self.source.bindings,self.source.batch,self.columns);self.calls=[]
  self.tables={r:'local.supplychainfinite.'+r for r in finite_publication.GRAPH_ROLES|{'attempts','manifest'}};self.target_rows=[]
  for role,table in self.tables.items():
   p=self.root/role;p.mkdir();(p/'_delta_log').mkdir();(p/'_delta_log/00000000000000000000.json').write_bytes(b'inert Delta witness');(p/'.ashlar-local-installation-reservation.json').write_bytes(b'inert reserved target');self.target_rows.append({'table':table,'path':str(p),'uuid':str(uuid.uuid4())})
  self.registry={'installation':'original-finite-supplychain','tables':self.tables,'targets':self.target_rows,'operation_capacity':dict(LOCAL_OPERATION_CAPACITY_8M),'publication_id':'original-finite-supplychain-publication'};(self.root/'registry.json').write_text(encoded(self.registry));(self.root/'.finite-source-writer.lock').write_bytes(b'');(self.root/'.finite-installation.lock').write_bytes(b'');(self.root/'.finite-installation.lock').chmod(0o600)
  request=native.finite_original_request(self.source.batch);self.request=request;(self.root/'request.json').write_text(encoded(request));self.manifest={'publication_id':self.registry['publication_id'],'profile_version':'ashlar-delta/0.3','table_versions_json':encoded({self.tables[r]:2 for r in finite_publication.GRAPH_ROLES}),'source_progress_json':encoded({SOURCE_SYSTEM:{'profile':'ashlar-immutable-file-replay/0.1'}}),'schema_revisions_json':request['schema_revisions_json'],'validation_report_json':encoded({'complete':True,'request_digest':request['request_digest'],'source_admission':self.source.metadata()}),'recorded_at':'1791547200000000'}
  artifact=encoded({'effects':{},'manifest':self.manifest});self.phase_rows=[]
  for i,phase in enumerate(PHASES):
   raw=encoded({'request':request,'result_json':artifact if i>=2 else None,'descriptor_json':encoded(self.manifest)if i==4 else None});self.phase_rows.append({'phase':phase,'request_digest':request['request_digest'],'payload_json':raw,'payload_digest':hashlib.sha256(raw.encode()).hexdigest()})
  self.journal=self.root/'operations.sqlite';db=sqlite3.connect(self.journal)
  definitions=('CREATE TABLE local_installation(id INTEGER PRIMARY KEY CHECK(id=1), original TEXT NOT NULL)','CREATE TABLE local_operation(operation TEXT PRIMARY KEY,intent TEXT NOT NULL,intent_sha TEXT NOT NULL,before_version INTEGER NOT NULL,state TEXT NOT NULL,receipt TEXT)','CREATE TABLE local_plan(operation TEXT PRIMARY KEY,original TEXT NOT NULL,digest TEXT NOT NULL)','CREATE TABLE local_publication_artifact (request_digest TEXT PRIMARY KEY,request TEXT NOT NULL,artifact TEXT NOT NULL)','CREATE TABLE local_source_plan (request_digest TEXT PRIMARY KEY,original TEXT NOT NULL)')
  for ddl in definitions:db.execute(ddl)
  db.execute('INSERT INTO local_installation VALUES(1,?)',(encoded({'installation':self.registry['installation'],'targets':sorted(self.target_rows,key=lambda r:r['table']),'operation_capacity':dict(LOCAL_OPERATION_CAPACITY_8M)}),));db.commit();db.close()
  self.config=owner.SupplyChainCommonReadConfig(invocation,self.root,self.root/'registry.json',self.journal,owner.SupplyChainCommonRuntime(Path(sys.executable).resolve(),self.java,self.runtime),self.jars,producer_fixture.ROOT/'examples/domain-packs/supply-chain/upstream/pack.json',LOCAL_OPERATION_CAPACITY_8M,VerificationBounds(262144,128,1048576,16777216,1048576,1048576))
 def transport(self,spark,journal,installation,targets,policy,**kwargs):
  self.calls.append('transport');test=self
  class Transport:
   def __init__(self):self.journal_path=journal;self.targets={t.table:t for t in targets};self.spark=spark;self.policy=policy;self.db=sqlite3.connect('file:'+str(journal)+'?mode=ro',uri=True)
   @contextmanager
   def bounded_observations(self,bounds):
    self.observation_bounds=bounds;test.calls.append('bounds-open')
    try:yield
    finally:self.observation_bounds=None;test.calls.append('bounds-close')
   def original_history(self,target):test.calls.append('history:'+target.table);return [{'version':2}]
   def query(self,sql,parameters):
    test.calls.append(sql)
    if sql.startswith('DESCRIBE'):
     target=next(t for t in targets if '`'+t.table.split('.')[-1]+'`'in sql);return SimpleNamespace(rows=[{'id':target.uuid,'location':str(target.path)}])
    if sql.startswith('SELECT 1 AS present'):return SimpleNamespace(rows=[{'present':1}])
    if sql.startswith('SELECT phase'):return SimpleNamespace(rows=test.phase_rows)
    if '`manifest`'in sql:return SimpleNamespace(rows=[test.manifest])
    role=next(r for r in finite_publication.GRAPH_ROLES if '`'+r+'`'in sql);return SimpleNamespace(rows=test.expected[role])
   def close(self):test.calls.append('transport-close');self.db.close()
   def mutation(self,*a,**kw):raise AssertionError('mutation forbidden')
   def recover(self,*a,**kw):raise AssertionError('recovery forbidden')
  return Transport()
 def spark(self,*args):
  self.calls.append('spark');test=self
  class Spark:
   version='4.0.1'
   class Read:
    def format(self,*args):return self
    def option(self,*args):return self
    def load(self,*args):return self
    def limit(self,*args):return self
    def collect(self):return []
   read=Read()
   def sql(self,sql):
    test.calls.append(sql)
    if 'COLLATE'in sql:return SimpleNamespace(first=lambda:{'equal':False})
    role=next((r for r in finite_publication.GRAPH_ROLES if r in sql),None)
    target=next((t for t in test.target_rows if t['table'].endswith('.'+str(role))),None)
    return SimpleNamespace(first=lambda:SimpleNamespace(asDict=lambda:{'id':target['uuid'],'location':target['path']}))
   def stop(self):test.calls.append('spark-stop')
  return Spark()
 @contextmanager
 def ports(self):
  with mock.patch.dict(os.environ,{'JAVA_HOME':str(self.java)}),mock.patch.object(native,'finite_runtime',return_value=[]),mock.patch.object(native,'open_finite_spark',side_effect=self.spark),mock.patch('ashlar_host.read_only_transport.ReadOnlyTransport.open',side_effect=self.transport),mock.patch.object(finite_publication.FiniteFileDriver,'writer',side_effect=AssertionError('writer forbidden')),mock.patch.object(finite_publication.FiniteFileDriver,'prepare_original',side_effect=AssertionError('prepare forbidden')):yield
 def test_real_restore_existing_only_and_ephemeral_aliases_no_production(self):
  before=self.journal.read_bytes();bun_calls=sum(a[0][0]==str(self.f.bun)for a in self.f.calls)
  with self.ports():
   with owner.open_supply_chain_common_read(self.config)as opened:
    self.assertIs(type(opened),owner.OpenedSupplyChainCommonRead);self.assertEqual(len(opened.vector),4);opened.require_current(opened.context)
    with opened.read_interval(opened.context):opened.require_current(opened.context)
   with self.assertRaises(PermissionError):opened.require_current(opened.context)
  self.assertEqual(self.journal.read_bytes(),before);self.assertEqual(sum(a[0][0]==str(self.f.bun)for a in self.f.calls),bun_calls);self.assertIn('transport-close',self.calls);self.assertIn('spark-stop',self.calls)
  ddl=[s for s in self.calls if s.startswith('CREATE')];self.assertEqual(len(ddl),5);self.assertTrue(all('LOCATION'in s for s in ddl));self.assertFalse(any('CREATE TABLE delta.'in s for s in ddl))
 def test_missing_journal_or_lock_refuses_before_sdk_and_no_repair(self):
  for name in ('operations.sqlite','.finite-source-writer.lock'):
   with self.subTest(name=name):
    p=self.root/name;saved=p.read_bytes();p.unlink();self.calls=[]
    try:
     with self.ports(),self.assertRaises((ValueError,FileNotFoundError)):
      with owner.open_supply_chain_common_read(self.config):self.fail('yielded')
     self.assertNotIn('spark',self.calls);self.assertFalse(p.exists())
    finally:p.write_bytes(saved)
 def test_incomplete_phases_refuses_releases_existing_lease(self):
  self.phase_rows.pop()
  with self.ports(),self.assertRaises(ValueError):
   with owner.open_supply_chain_common_read(self.config):self.fail('yielded')
  self.assertIn('transport-close',self.calls);self.assertIn('spark-stop',self.calls);lease=native.PrivateFiniteLease(self.root,create=False);lease.close()
 def test_body_cancel_preserved_and_no_effects_cleanup(self):
  primary=KeyboardInterrupt()
  with self.ports(),self.assertRaises(KeyboardInterrupt)as caught:
   with owner.open_supply_chain_common_read(self.config):raise primary
  self.assertIs(caught.exception,primary);self.assertIn('transport-close',self.calls);self.assertIn('spark-stop',self.calls)
 def test_missing_native_target_refuses_before_sdk_without_creation(self):
  import shutil
  path=self.root/'object_current';shutil.rmtree(path)
  with self.ports(),self.assertRaises(ValueError):
   with owner.open_supply_chain_common_read(self.config):self.fail('yielded')
  self.assertFalse(path.exists());self.assertNotIn('spark',self.calls)
 def test_phase_drift_withholds_normal_exit_and_cleanup(self):
  with self.ports(),self.assertRaises(ValueError):
   with owner.open_supply_chain_common_read(self.config):self.phase_rows.pop()
  self.assertIn('transport-close',self.calls);self.assertIn('spark-stop',self.calls)
 def test_changed_config_does_not_redirect_original_file_closing(self):
  primary=GeneratorExit()
  with self.ports(),self.assertRaises(GeneratorExit)as caught:
   with owner.open_supply_chain_common_read(self.config):object.__setattr__(self.config,'pack',self.root/'absent');raise primary
  self.assertIs(caught.exception,primary);self.assertIn('transport-close',self.calls);self.assertIn('spark-stop',self.calls)
 def test_original_native_closings_independent_after_driver_construction_cancel(self):
  primary=GeneratorExit()
  with self.ports(),mock.patch('ashlar_host.supply_chain_publication.FiniteSupplyChainDriver',side_effect=primary),self.assertRaises(GeneratorExit)as caught:
   with owner.open_supply_chain_common_read(self.config):self.fail('yielded')
  self.assertIs(caught.exception,primary);self.assertIn('transport-close',self.calls);self.assertIn('spark-stop',self.calls)
  for role in self.tables:self.assertTrue(any(s.startswith('DESCRIBE DETAIL')and '`'+role+'`'in s for s in self.calls))
 def test_captured_cleanup_ports_run_after_replacement_and_refuse_result(self):
  foreign=[]
  with self.ports(),self.assertRaises(ValueError):
   with owner.open_supply_chain_common_read(self.config)as opened:
    opened.driver.transport.close=lambda:foreign.append('foreign-close');opened.driver.transport.spark.stop=lambda:foreign.append('foreign-stop')
  self.assertEqual(foreign,[]);self.assertIn('transport-close',self.calls);self.assertIn('spark-stop',self.calls)
 def test_original_history_replacement_refuses_and_closes_all_originals(self):
  foreign=[];primary=KeyboardInterrupt()
  with self.ports(),self.assertRaises(KeyboardInterrupt)as caught:
   with owner.open_supply_chain_common_read(self.config)as opened:
    self.calls.clear();opened.driver.transport.original_history=lambda target:foreign.append(target)
    raise primary
  self.assertIs(caught.exception,primary);self.assertEqual(foreign,[])
  for table in self.tables.values():self.assertIn('history:'+table,self.calls)
 def test_query_provider_real_inactive_interval_and_closed_custody(self):
  with self.ports():
   with owner.open_supply_chain_common_read(self.config)as opened:
    provider,request=opened.query_provider(opened.context,family='one-hop',part='relationships-0-6',ordinal=0)
    self.assertFalse(provider.active);self.assertIn('relationships',json.loads(request['target']['bindingJson']))
    with provider.interval(opened.context):
     self.assertTrue(provider.active);self.assertFalse(opened.provider.active);opened.require_current(opened.context)
     with self.assertRaises(PermissionError):opened.query_provider(opened.context,family='singleton',part='complete',ordinal=0)
    self.assertEqual(provider.closed_interval_custody(opened.context)['profile'],'ashlar-immutable-file-publication-interval/0.1')
    opened.require_current(opened.context)
 def test_query_selection_foreign_context_boolean_and_missing_ordinal(self):
  with self.ports():
   with owner.open_supply_chain_common_read(self.config)as opened:
    for context,ordinal in ((object(),0),(opened.context,True),(opened.context,20)):
     with self.assertRaises((PermissionError,ValueError)):opened.query_provider(context,family='singleton',part='complete',ordinal=ordinal)
 def test_query_cancel_unregisters_and_fixed_request_drift_refuses(self):
  with self.ports():
   with owner.open_supply_chain_common_read(self.config)as opened:
    provider,request=opened.query_provider(opened.context,family='singleton',part='complete',ordinal=0)
    primary=GeneratorExit()
    with self.assertRaises(GeneratorExit)as caught:
     with provider.interval(opened.context):raise primary
    self.assertIs(caught.exception,primary);self.assertFalse(provider.active);opened.require_current(opened.context)
    request['sql']='foreign'
    with self.assertRaises(ValueError):
     with provider.interval(opened.context):self.fail('yielded')
 def test_reconstructed_query_provider_refuses_before_any_callback(self):
  with self.ports():
   with owner.open_supply_chain_common_read(self.config)as opened:
    provider,request=opened.query_provider(opened.context,family='singleton',part='complete',ordinal=0)
    reconstructed=type(provider)(opened.driver,dict(opened.aliases),{'foreign_binding':True})
    before=list(self.calls)
    with self.assertRaises(PermissionError):
     with reconstructed.interval(opened.context):self.fail('foreign provider yielded')
    self.assertEqual(self.calls,before);self.assertFalse(reconstructed.active)
    with provider.interval(opened.context):opened.require_current(opened.context)
    self.assertIsNotNone(provider.closed_interval_custody(opened.context))
if __name__=='__main__':unittest.main()
