# SPDX-License-Identifier: MIT OR Apache-2.0
"""Actual typed custody orchestration with inert policies; no engine authority."""
import gzip,hashlib,json
from contextlib import contextmanager
from pathlib import Path
from unittest import TestCase,mock
from ashlar.graph_release import GraphRelease
from ashlar_host.finite_dataset import FinitePackSource
from ashlar_host.finite_pack import FinitePackDefinition
from ashlar_host.graphframes_release import GraphFramesOfflineSession,hold_graphframes_session,capture_graphframes_worker
ROOT=Path(__file__).resolve().parents[1]
class RuntimePolicy:
 def admit_graphframes_runtime(self,*args):raise AssertionError('No SDK execution in source hold control')
 def renew_graphframes_runtime(self,*args):raise AssertionError('No SDK execution in source hold control')
class Tests(TestCase):
 def session(self,policy):
  p=ROOT/'examples/domain-packs/archaeology/upstream';source=object.__new__(FinitePackSource)
  source.definition=FinitePackDefinition('archaeology');source.model=(p/'ontology.json').read_bytes();source.graph=(p/'graph/fixture.json').read_bytes();source.raw=b'owned-public-receipt'
  p=ROOT/'docs/helix/04-build/evidence/original17-graphframes-native-20261009/original';raw=(p/'1-release.graph.json').read_bytes();receipt=gzip.decompress((p/'0-custody.json.gz').read_bytes())
  return GraphFramesOfflineSession(GraphRelease(raw,hashlib.sha256(raw).hexdigest()),receipt,hashlib.sha256(receipt).hexdigest(),source,policy,object(),RuntimePolicy(),object())
 def test_explicit_current_policy_held_through_closing(self):
  events=[]
  class Policy:
   @contextmanager
   def writer(self,context):
    events.append('open')
    try:yield
    finally:events.append('close')
   def admit_original_export(self,*args):events.append('admit')
  with mock.patch.object(FinitePackSource,'metadata',return_value=None):
   session=self.session(Policy())
   with hold_graphframes_session(session)as(release,renew):
    events.append('worker');renew()
   self.assertEqual(events,['open','admit','worker','admit','admit','close'])
 def test_source_closing_change_withholds_success(self):
  class Policy:
   @contextmanager
   def writer(self,context):yield
   def admit_original_export(self,*args):pass
  with mock.patch.object(FinitePackSource,'metadata',return_value=None):
   session=self.session(Policy())
   with self.assertRaises(ValueError):
    with hold_graphframes_session(session):session.source.graph+=b' '
 def test_outer_cancel_identity_survives_ordinary_cleanup(self):
  cancel=KeyboardInterrupt('body');closed=[]
  class Policy:
   @contextmanager
   def writer(self,context):
    try:yield
    finally:closed.append(True);raise ValueError('closing')
   def admit_original_export(self,*args):pass
  with mock.patch.object(FinitePackSource,'metadata',return_value=None):
   session=self.session(Policy())
   with self.assertRaises(KeyboardInterrupt)as raised:
    with hold_graphframes_session(session):raise cancel
   self.assertIs(raised.exception,cancel);self.assertEqual(closed,[True])
 def test_refusal_cannot_substitute_current_policy(self):
  class Policy:
   @contextmanager
   def writer(self,context):yield
   def admit_original_export(self,*args):return True
  with mock.patch.object(FinitePackSource,'metadata',return_value=None):
   session=self.session(Policy())
   with self.assertRaises(PermissionError):
    with hold_graphframes_session(session):self.fail('Refused policy yielded')
 def test_renewal_cancellation_before_child_preserves_identity(self):
  cancel=SystemExit('renew')
  def renew():raise cancel
  with mock.patch('ashlar_host.graphframes_release.subprocess.Popen')as popen:
   with self.assertRaises(SystemExit)as raised:capture_graphframes_worker(['/absolute/python'],cwd=Path('/tmp'),environment={},renew=renew,timeout_seconds=1,maximum_output_bytes=1)
   self.assertIs(raised.exception,cancel);popen.assert_not_called()

 def test_live_actual_public_reader_signatures_and_owned_revision_mapping(self):
  from ashlar_host.graphframes_release import GraphFramesPublicationSession
  from ashlar.pins import PinVector
  from test_graph_release import Executor,Policy
  from test_publication import FakeBackend,TABLE,OTHER
  class Pins:
   def __init__(self):self.depth=0
   @property
   def active(self):return self.depth>0
   @contextmanager
   def hold(self,vector,*,context):
    self.depth+=1
    try:yield
    finally:self.depth-=1
  @contextmanager
  def outer(context):yield
  class SourcePolicy:
   @contextmanager
   def writer(self,context):yield
   def admit_original_export(self,*args):pass
  with mock.patch.object(FinitePackSource,'metadata',return_value=None):
   source=self.session(SourcePolicy()).source;pins=Pins();executor=Executor(pins);backend=FakeBackend();policy=Policy(pins)
   vector=PinVector('a','manifest','p1','a'*64,{TABLE:('trusted-uuid',6),OTHER:('trusted-uuid',2)})
   borrowed={'source':['r1']}
   session=GraphFramesPublicationSession(executor,backend,pins,vector,policy,source,'authorized','p1',TABLE,OTHER,outer,('ashlar-delta/0.3',),borrowed,10,10,RuntimePolicy(),object())
   borrowed['source'].append('changed')
   with hold_graphframes_session(session)as(release,renew):
    self.assertTrue(pins.active);self.assertEqual(len(json.loads(release.payload)['nodes']),4);renew()
   self.assertFalse(pins.active);self.assertEqual(session.supported_revisions['source'],('r1',))

 def test_owned_stdlib_subprocess_success_cap_and_live_cancel_cleanup(self):
  """Tiny Python children only, never Spark/JVM/SDK or publication access."""
  import sys,tempfile,subprocess
  from unittest import mock
  selected=str(Path(sys.executable).resolve());owned=[];real_popen=subprocess.Popen
  def start(*args,**kwargs):
   self.assertEqual(args[0][:4],[selected,'-I','-B','-c'])
   process=real_popen(*args,**kwargs);owned.append(process);return process
  with tempfile.TemporaryDirectory()as directory,mock.patch('ashlar_host.graphframes_release.subprocess.Popen',side_effect=start):
   cwd=Path(directory).resolve();environment={'PATH':'/usr/bin:/bin'}
   out,err=capture_graphframes_worker([selected,'-c',"print('ok')"],cwd=cwd,environment=environment,renew=lambda:None,timeout_seconds=2,maximum_output_bytes=64)
   self.assertEqual((out,err),(b'ok\n',b''));self.assertIsNotNone(owned[-1].returncode)
   with self.assertRaises(ValueError):capture_graphframes_worker([selected,'-c',"print('x'*1000)"],cwd=cwd,environment=environment,renew=lambda:None,timeout_seconds=2,maximum_output_bytes=8)
   self.assertIsNotNone(owned[-1].returncode)
   cancel=KeyboardInterrupt('held source drift cancellation');calls=[]
   def renew():
    calls.append(True)
    if len(calls)>1:raise cancel
   with self.assertRaises(KeyboardInterrupt)as raised:capture_graphframes_worker([selected,'-c','import time;time.sleep(10)'],cwd=cwd,environment=environment,renew=renew,timeout_seconds=2,maximum_output_bytes=64)
   self.assertIs(raised.exception,cancel);self.assertIsNotNone(owned[-1].returncode)

 def test_owned_worker_environment_prevents_nested_python_bytecode(self):
  import ast,sys,tempfile,subprocess,types
  import ashlar_host.graphframes_release as owner
  tree=ast.parse(Path(owner.__file__).read_text())
  function=next(n for n in tree.body if isinstance(n,ast.FunctionDef)and n.name=='_execute_prepared_graphframes')
  statement=next(n for n in function.body if isinstance(n,ast.Assign)and any(isinstance(t,ast.Name)and t.id=='environment'for t in n.targets))
  with tempfile.TemporaryDirectory()as directory:
   work=Path(directory).resolve();selected=Path(sys.executable).resolve()
   namespace={'runtime':types.SimpleNamespace(java_home=work,work=work,python=selected),'str':str}
   exec(compile(ast.Module(body=[statement],type_ignores=[]),owner.__file__,'exec'),namespace)
   environment=namespace['environment'];self.assertEqual(environment['PYTHONDONTWRITEBYTECODE'],'1')
   module=work/'bytecode_witness.py';module.write_text('value=7\n')
   child="import subprocess,sys;subprocess.run([sys.executable,'-X','pycache_prefix=owned-cache','-c','import bytecode_witness'],check=True)"
   result=subprocess.run([str(selected),'-B','-c',child],cwd=work,env=environment,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=5)
   self.assertEqual(result.returncode,0);self.assertFalse((work/'owned-cache').exists())
   comparison=dict(environment);comparison.pop('PYTHONDONTWRITEBYTECODE')
   result=subprocess.run([str(selected),'-B','-c',child],cwd=work,env=comparison,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=5)
   self.assertEqual(result.returncode,0);self.assertTrue(list((work/'owned-cache').rglob('bytecode_witness.*.pyc')))

 def test_actual_worker_spark_builder_sets_executor_bytecode_policy_before_session(self):
  import ast,types
  import ashlar_host.graphframes_release as owner
  tree=ast.parse(Path(owner.__file__).read_text())
  function=next(n for n in tree.body if isinstance(n,ast.FunctionDef)and n.name=='graphframes_worker')
  statement=next(n for n in function.body if isinstance(n,ast.Assign)and any(isinstance(t,ast.Name)and t.id=='spark'for t in n.targets))
  calls=[]
  class Builder:
   def master(self,value):calls.append(('master',value));return self
   def appName(self,value):calls.append(('appName',value));return self
   def config(self,key,value):calls.append(('config',key,value));return self
   def getOrCreate(self):calls.append(('getOrCreate',));return self
  namespace={'SparkSession':types.SimpleNamespace(builder=Builder()),'request':{'jars':{'selected':'/selected.jar'}},'output':Path('/owned/worker'),'str':str}
  exec(compile(ast.Module(body=[statement],type_ignores=[]),owner.__file__,'exec'),namespace)
  self.assertEqual(calls.count(('config','spark.executorEnv.PYTHONDONTWRITEBYTECODE','1')),1)
  self.assertLess(calls.index(('config','spark.executorEnv.PYTHONDONTWRITEBYTECODE','1')),calls.index(('getOrCreate',)))
  self.assertIn(('master','local[1]'),calls);self.assertIn(('config','spark.driver.memory','512m'),calls)

 def test_derived_protocol_original_vector_and_raw_closing_witnesses(self):
  import copy
  from ashlar_host.graphframes_release import validate_derived_evidence
  work=Path('/operator/engine');original={'snapshots':{'original':{'uuid':'00000000-0000-0000-0000-000000000001','version':1}}}
  result={'format':'ashlar-original-pack-graphframes/0.1','pack':'archaeology','cases':[{'original_scenario':{'id':str(i),'sql':'structural fixture only'},'schema':[],'rows':[],'logical_rows':[],'independent_expected':[]}for i in FinitePackDefinition('archaeology').facts()['case_ids']],'common':{},'all_original_cases_passed':8,'authority_scope':'Structural protocol control only','original_snapshots':copy.deepcopy(original['snapshots']),'full_carrier_parity':True,'derived_snapshots':{role:{'uuid':'00000000-0000-0000-0000-00000000000'+str(i),'version':0,'location':(work/role).as_uri(),'minReaderVersion':1,'minWriterVersion':2,'tableFeatures':['appendOnly','invariants']}for role,i in [('nodes',2),('edges',3)]}}
  witnesses=[{'kind':'derived-snapshot','phase':phase,'role':role,'detail':copy.deepcopy(detail),'history':[[0]]}for role,detail in result['derived_snapshots'].items()for phase in ('opening','before-query','closing')]
  validate_derived_evidence(result,original,witnesses,work)
  for change in [lambda r:r['original_snapshots']['original'].update(version=True),lambda r:r['derived_snapshots']['nodes'].update(minReaderVersion=True),lambda r:r['derived_snapshots']['nodes'].update(tableFeatures=['unknown']),lambda r:r['derived_snapshots']['nodes'].update(uuid=original['snapshots']['original']['uuid']),lambda r:r.update(full_carrier_parity=1)]:
   changed=copy.deepcopy(result);change(changed)
   with self.assertRaises(ValueError):validate_derived_evidence(changed,original,witnesses,work)
  for altered in [witnesses[:-1],witnesses+witnesses[:1]]:
   with self.assertRaises(ValueError):validate_derived_evidence(result,original,altered,work)

 def test_actual_inventory_empty_member_and_changed_bytes_refuse(self):
  """Finite dummy files prove correspondence only, not SDK completeness."""
  import tempfile
  from ashlar_host.graphframes_release import GraphFramesRuntimeFile,GraphFramesRuntimeInventory,GraphFramesRuntimeConfig,SDK_INVENTORIES
  with tempfile.TemporaryDirectory()as directory:
   root=Path(directory).resolve();worker=root/'worker';worker.write_bytes(b'')
   sdkfiles=[]
   for name in SDK_INVENTORIES:
    path=root/name;path.write_bytes(name.encode());sdkfiles.append((name,(GraphFramesRuntimeFile(path,hashlib.sha256(name.encode()).hexdigest(),len(name)),)))
   inventory=GraphFramesRuntimeInventory('a'*40,(GraphFramesRuntimeFile(worker,hashlib.sha256(b'').hexdigest(),0),),tuple(sdkfiles))
   def descriptors(files):return [{'path':str(f.path),'sha256':f.sha256,'size':f.size}for f in files]
   def digest(v):return hashlib.sha256(json.dumps(v,sort_keys=True,separators=(',',':')).encode()).hexdigest()
   config=GraphFramesRuntimeConfig(root/'python','b'*64,root/'java','c'*64,'a'*40,digest(descriptors(inventory.worker)),digest({n:descriptors(fs)for n,fs in inventory.sdks}),root/'jars',root/'output')
   inventory.verify(config)
   from ashlar_host.graphframes_release import renew_graphframes_runtime
   calls=[]
   class Policy:
    def admit_graphframes_runtime(self,*args):return inventory
    def renew_graphframes_runtime(self,*args):calls.append('renew');return True
   with mock.patch.object(GraphFramesRuntimeConfig,'renew',return_value=None):
    with self.assertRaises(PermissionError):renew_graphframes_runtime(config,inventory,Policy(),object())
   self.assertEqual(calls,['renew'])
   worker.write_bytes(b'changed')
   with self.assertRaises(ValueError):inventory.verify(config)
   worker.write_bytes(b'');(root/'pyspark').write_bytes(b'changed')
   with self.assertRaises(ValueError):inventory.verify(config)

 def test_common_raw_witnesses_cannot_be_replaced_by_report_claims(self):
  import base64,copy
  from ashlar.graph_release import release_columns
  from ashlar_host.graphframes_pack_scenarios import common_oracle
  from ashlar_host.graphframes_release import validate_common_evidence
  from test_graphframes_pack_scenarios import Tests as Fixture
  profile=Fixture().profile('ecology');path=ROOT/'docs/helix/04-build/evidence/original17-graphframes-native-20261009/original/9-release.graph.json';release=json.loads(path.read_bytes())
  graph=json.loads(base64.b64decode(profile['source_admission']['original_graph_base64']));expected=common_oracle(graph)
  nodes={row['original_key']:row['graph_id']for row in profile['nodes']};edges={json.loads(row['retained_json'])['original']['key']:row['graph_id']for row in profile['edges']}
  relationships={json.dumps(json.loads(row['retained_json'])['original']['relationship'],sort_keys=True,separators=(',',':')):row['rel_type_id']for row in profile['edges']}
  observations=[];actual={}
  def witness(name,names,types,rows):observations.append({'kind':'common-query','case':name,'schema':[{'name':n,'type':t}for n,t in zip(names,types)],'rows':rows})
  for kind,role,name in [('node','nodes','all-objects'),('edge','edges','all-edges')]:
   names=release_columns(kind);actual[name]=release[role];witness(name,names,['string']*len(names),[[row[n]for n in names]for row in release[role]])
  for name in ('one-hop','two-hop'):
   count=3 if name=='one-hop' else 5
   raw=[[nodes[token]if i%2==0 else edges[token]for i,token in enumerate(row)]for row in expected[name]]
   witness(name,['id']*count,['string']*count,raw);actual[name]=expected[name]
  witness('grouped-count',['src','rel_type_id','occurrences','destinations'],['string','string','bigint','bigint'],[[nodes[src],relationships[rel],count,distinct]for src,rel,count,distinct in expected['grouped-count']]);actual['grouped-count']=expected['grouped-count']
  originals={row['graph_id']:row for row in release['nodes']}
  for name in ('singleton','filtered-limit','isolates'):
   names=release_columns('node');witness(name,names,['string']*len(names),[[originals[nodes[key]][n]for n in names]for key in expected[name]]);actual[name]=expected[name]
  filtered=[obj['key']for obj in graph['objects']if json.dumps(obj['type'],sort_keys=True,separators=(',',':'))==expected['filtered-type']]
  witness('filtered-total',['id'],['string'],[[nodes[key]]for key in filtered]);actual['filtered-total']=len(filtered)
  result={'common':actual};validate_common_evidence(result,release,profile,observations)
  changed=copy.deepcopy(observations);next(v for v in changed if v['case']=='one-hop')['rows'].pop()
  with self.assertRaises(ValueError):validate_common_evidence(result,release,profile,changed)
  changed=copy.deepcopy(result);changed['common']['all-objects'][0]['props_json']='forged'
  with self.assertRaises(ValueError):validate_common_evidence(changed,release,profile,observations)

 def test_final_scenario_logical_oracle_claims_bind_actual_original_cells(self):
  import copy
  from test_graphframes_pack_scenarios import Tests as Fixture
  from ashlar_host.graphframes_release import validate_scenario_evidence
  fixture=Fixture();profile=fixture.profile('archaeology');case=next(c for c in profile['cases']if c['original_graph_expected'])
  record={'original_scenario':case['original_scenario'],'schema':fixture.schema(profile,case),'rows':fixture.rows(profile,case),'logical_rows':case['original_graph_expected'],'independent_expected':case['original_graph_expected']}
  # Expected source order may differ for non-ORDER-BY bags; retain actual order.
  from ashlar_host.graphframes_pack_scenarios import validate_case
  record['logical_rows']=validate_case(profile,case['original_scenario']['id'],record['schema'],record['rows'])
  validate_scenario_evidence(profile,record)
  for field in ('logical_rows','independent_expected'):
   forged=copy.deepcopy(record);forged[field]=[['forged']]
   with self.assertRaises(ValueError):validate_scenario_evidence(profile,forged)
  forged=copy.deepcopy(record);forged['unknown']='claim'
  with self.assertRaises(ValueError):validate_scenario_evidence(profile,forged)
 def test_direct_inventory_configuration_refuses_before_borrowed_access(self):
  from ashlar_host.graphframes_release import GraphFramesRuntimeInventory
  class Forged:
   def __getattr__(self,name):raise AssertionError('Borrowed configuration field was read')
  inventory=object.__new__(GraphFramesRuntimeInventory)
  with self.assertRaises(ValueError):inventory.verify(Forged())

 def test_complete_projected_witnesses_bind_unused_fields_and_both_phases(self):
  import copy
  from test_graphframes_pack_scenarios import Tests as Fixture
  from ashlar_host.graphframes_release import validate_projected_carrier_evidence
  for pack in ('archaeology','ecology'):
   profile=Fixture().profile(pack);integers={f['signed64_column']for f in profile['fields']if f['signed64_column']};observations=[]
   for phase in ('before-query','closing'):
    for role in ('nodes','edges'):
     names=sorted(profile[role][0]);observations.append({'kind':'full-carrier','phase':phase,'role':role,'schema':[{'name':n,'type':'bigint'if n in integers else 'string'}for n in names],'rows':[[row[n]for n in names]for row in profile[role]]})
   validate_projected_carrier_evidence(profile,observations)
   for prefix in ('field_','presence_'):
    changed=copy.deepcopy(observations);witness=changed[2];column=next(i for i,f in enumerate(witness['schema'])if f['name'].startswith(prefix));witness['rows'][0][column]='forged-unused-cell'
    with self.assertRaises(ValueError):validate_projected_carrier_evidence(profile,changed)
   for changed in (observations[:-1],observations+[observations[0]]):
    with self.assertRaises(ValueError):validate_projected_carrier_evidence(profile,changed)
   changed=copy.deepcopy(observations);changed[0]['schema'].reverse()
   with self.assertRaises(ValueError):validate_projected_carrier_evidence(profile,changed)
