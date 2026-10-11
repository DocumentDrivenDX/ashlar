"""Original commerce source/carrier/finite evidence controls, without an engine."""
import copy,hashlib,json
from pathlib import Path
import unittest
from unittest.mock import patch,Mock
from ashlar.graph_release import GraphRelease
from ashlar_host import commerce_graphframes_scenarios as scenarios
from ashlar_host.graphframes_release import validate_projected_carrier_evidence,validate_scenario_evidence
from ashlar_host.graphframes_pack_scenarios import common_oracle
from puppy_commerce_fixture import admission,ROOT

PACK=(ROOT/'examples/domain-packs/commerce/upstream/pack.json').read_bytes()
RELEASE=(ROOT/'docs/helix/04-build/evidence/commerce-graph-export-20261009/release.graph.json').read_bytes()

class ScenarioTests(unittest.TestCase):
 def profile(self):return scenarios.prepare_commerce_graphframes_profile(admission(self),PACK,GraphRelease(RELEASE,hashlib.sha256(RELEASE).hexdigest()))
 def test_actual_original0_8_allfour_and_seven_common(self):
  profile=self.profile();self.assertEqual(len(profile['nodes']),11);self.assertEqual(len(profile['edges']),10)
  self.assertEqual([c['original_scenario']['id']for c in profile['cases']],['partial-return','fulfillment','settlement','refund'])
  self.assertEqual(profile['cases'][0]['original_graph_expected'],[['6','2','6']])
  for case in profile['cases']:
   names=['fulfilled','returned','remaining']if case['original_scenario']['id']=='partial-return'else['id']
   record={'original_scenario':case['original_scenario'],'schema':[{'name':n,'type':'string'}for n in names],'rows':case['original_graph_expected'],'logical_rows':case['original_graph_expected'],'independent_expected':case['original_graph_expected']}
   self.assertEqual(validate_scenario_evidence(profile,record),case['original_graph_expected'])
   record['logical_rows']=[['forged']]
   with self.assertRaises(ValueError):validate_scenario_evidence(profile,record)
  graph=json.loads(__import__('base64').b64decode(profile['source_admission']['original_graph_base64']))
  common=common_oracle(graph);self.assertEqual(len(common['one-hop']),10);self.assertEqual(len(common['two-hop']),11);self.assertGreater(common['filtered-total'],1);self.assertEqual(len(common['filtered-limit']),1)
 def test_originalpack_source_completecarrier_and_presence_drift_refuse(self):
  owner=admission(self);release=json.loads(RELEASE)
  release['nodes'][0]['props_json']='{}';raw=json.dumps(release).encode()
  with self.assertRaises(ValueError):scenarios.prepare_commerce_graphframes_profile(owner,PACK,GraphRelease(raw,hashlib.sha256(raw).hexdigest()))
  with self.assertRaises(ValueError):scenarios.prepare_commerce_graphframes_profile(owner,PACK+b' ',GraphRelease(RELEASE,hashlib.sha256(RELEASE).hexdigest()))
  path,raw=owner.originals[1];path.write_bytes(raw+b' ')
  with self.assertRaises(PermissionError):scenarios.prepare_commerce_graphframes_profile(owner,PACK,GraphRelease(RELEASE,hashlib.sha256(RELEASE).hexdigest()))
 def test_allunused_projected_fields_and_occurrences_bound(self):
  profile=self.profile();observations=[]
  for phase in ('before-query','closing'):
   for role in ('nodes','edges'):
    names=sorted(profile[role][0]);observations.append({'kind':'full-carrier','phase':phase,'role':role,'schema':[{'name':n,'type':'string'}for n in names],'rows':[[row[n]for n in names]for row in profile[role]]})
  validate_projected_carrier_evidence(profile,observations)
  for select in ('field_','presence_'):
   changed=copy.deepcopy(observations);index=next(i for i,c in enumerate(changed[0]['schema'])if c['name'].startswith(select));changed[0]['rows'][0][index]='forged-unused'
   with self.assertRaises(ValueError):validate_projected_carrier_evidence(profile,changed)
  changed=copy.deepcopy(observations);changed[1]['rows'].append(changed[1]['rows'][0])
  with self.assertRaises(ValueError):validate_projected_carrier_evidence(profile,changed)
 def test_numeric_guards_exact_lexical_bounds_never_round(self):
  self.assertEqual(scenarios.exact_number('-0.00','decimal(18,2)'),0)
  for token,representation in [('9'*39,'decimal(38,0)'),('1.0','decimal(38,0)'),('1.001','decimal(18,2)'),('9'*17,'decimal(18,2)'),('NaN','decimal(18,2)'),('01','decimal(38,0)'),(True,'decimal(38,0)')]:
   with self.assertRaises(ValueError):scenarios.exact_number(token,representation)
 def test_raw_numeric_substitution_refuses_before_positive_report(self):
  profile=self.profile();guards=[{'kind':'numeric-guard','case':i['key']+':'+i['field'][2],'schema':[{'name':'token','type':'string'},{'name':'exact','type':'string'}],'rows':[[i['token'],i['token']]]}for i in profile['numeric_guards']]
  changed=copy.deepcopy(guards);changed[0]['rows'][0][0]='0'
  with self.assertRaises(ValueError):scenarios.validate_arithmetic_evidence(profile,changed)
 def test_complete_intermediate_independent_occurrences(self):
  profile=self.profile()
  partial=[['order_lines:OL1','fulfillments:F1','returns:RET1','4','6']]
  # Build exact source occurrence IDs from the unchanged original graph, not native results.
  import base64
  graph=json.loads(base64.b64decode(profile['source_admission']['original_graph_base64']));by_type={o['type']['element']:o for o in graph['objects']}
  line=next(o for o in graph['objects']if o['type']['element']=='order_lines'and o['values']['order_lines.quantity']=='10')
  fulfillment=next(o for o in graph['objects']if o['type']['element']=='fulfillments'and o['values']['fulfillments.quantity']=='6')
  returned=by_type['returns'];partial=[[line['key'],fulfillment['key'],returned['key'],'4','6']]
  # Original graph also has the overfulfillment match in the unfiltered guard bag.
  other=next(o for o in graph['objects']if o['type']['element']=='fulfillments'and o['values']['fulfillments.quantity']!='6')
  other_line=next(o for o in graph['objects']if o['type']['element']=='order_lines'and o['values']['order_lines.id']==other['values']['fulfillments.line_id'])
  if returned['values']['returns.line_id']==other_line['values']['order_lines.id']:
   a=int(other_line['values']['order_lines.quantity'])-int(other['values']['fulfillments.quantity']);partial.append([other_line['key'],other['key'],returned['key'],str(a),str(a+2)])
  schema=[{'name':n,'type':'string'}for n in ('line','fulfillment','return','subtract','add')]
  scenarios.validate_intermediate(profile,'partial-return',schema,partial)
  partial[0][-1]='7'
  with self.assertRaises(ValueError):scenarios.validate_intermediate(profile,'partial-return',schema,partial)

class ReaderTests(unittest.TestCase):
 """Actual public graph reader/resolver over explicit inert driver observations."""
 def opened(self):
  import tempfile
  from types import SimpleNamespace,MappingProxyType
  from ashlar_host.publication_reader import OpenedCommerceReader,PublicationProvider
  from ashlar_host.driver import NativeDriver
  from ashlar_host.delta_custody import DeltaTarget,encoded
  from ashlar_host.ordinary_query_connection import OrdinaryQueryConnectionFactory
  from ashlar_host.commerce_query_sources import CommerceQuerySourceFactory
  from ashlar_host.source_sessions import OutboxSourceRegistration
  from ashlar_host.existing_outbox_authority import OutboxAuthorityRegistration
  from ashlar_host.ack import AckScope
  from ashlar.publication import Snapshot
  from ashlar.native import SQLResult
  temp=__import__('tempfile').TemporaryDirectory();self.addCleanup(temp.cleanup);root=Path(temp.name)
  release=json.loads(RELEASE);owner=admission(self);model,graph,bindings,_=[raw for _,raw in owner.originals];manifest=release['publication'];context=object()
  targets={}
  for i,(table,snapshot)in enumerate(release['snapshots'].items()):
   path=root/str(i);path.mkdir();targets[table]=DeltaTarget(table,path,snapshot['uuid'])
  d=object.__new__(NativeDriver);d.context=context;d.source_admission=owner;d.held=True;d.pin_held=True;d.source_sessions=object();d.tables={'object_current':release['roles']['nodes'],'edge_current':release['roles']['edges']};d.policy=SimpleNamespace(active={'manifest':manifest,'publication_id':manifest['publication_id']})
  checkpoint={'feed':owner.batch.feed,'epoch':owner.batch.epoch,'position':'1'};request={'source_checkpoint_json':encoded(checkpoint)}
  scope=AckScope('ashlar_ack_fixture','11111111-1111-4111-8111-111111111111','22222222-2222-4222-8222-222222222222','consumer',owner.batch.feed,owner.batch.epoch)
  registration=OutboxSourceRegistration(scope,'fixture_source','a'*64,lambda context:None)
  d.query_source_factory=CommerceQuerySourceFactory(OutboxAuthorityRegistration(registration,'reader','ack','truss_e2e',lambda context:None))
  def query(sql,parameters):
   if sql.startswith('DESCRIBE DETAIL '):
    table=sql.split(' ',2)[2].replace('`','');return SQLResult([{'id':targets[table].uuid}])
   role='nodes'if release['roles']['nodes'].replace('.','`.`')in sql else'edges'
   rows=[{k:v for k,v in row.items()if k not in ('graph_id','src','dst')}for row in release[role]]
   from ashlar.graph_release import release_columns
   names=tuple(n for n in release_columns('node'if role=='nodes'else'edge')if n not in ('graph_id','src','dst'))
   return SQLResult(rows,tuple((n,'STRING')for n in names))
  d.transport=SimpleNamespace(targets=MappingProxyType(targets),query=query)
  d.validate_manifest=Mock();d.source_admit=Mock();d.descriptors=lambda publication_id:[manifest]
  d.inspect_snapshot=lambda table,version:Snapshot(table,targets[table].uuid,version)
  p=object.__new__(PublicationProvider);p.driver=d;p.context=context;p.active=True;p._reader_closed=False;p.ordinary_session=OrdinaryQueryConnectionFactory(lambda role:None);p.request_bytes=encoded(request).encode();p.manifest_bytes=encoded(manifest).encode();p.renew_current_ack=Mock()
  report={'native_manifest':manifest};(root/'report.json').write_text(encoded(report))
  expected={role:[{k:v for k,v in row.items()if k not in ('graph_id','src','dst')}for row in release[graphrole]]for role,graphrole in [('object_current','nodes'),('edge_current','edges')]}
  return OpenedCommerceReader(p,d,owner,expected,context,{},lambda:{},{},report,model,graph,json.loads(bindings),manifest,p.request_bytes,p.manifest_bytes,root)
 def test_actual_public_reader_complete_vector_and_current_renewal(self):
  from ashlar_host.commerce_graphframes_release import _CommerceGraphPolicy,_read_release
  from ashlar.manifest import manifest_pin_vector
  opened=self.opened();vector=manifest_pin_vector(opened.manifest,{t:v.uuid for t,v in opened.driver.transport.targets.items()},authority='private-local-process');policy=_CommerceGraphPolicy(opened,vector)
  release=_read_release(opened,policy);value=json.loads(release.payload)
  self.assertEqual(len(value['nodes']),11);self.assertEqual(len(value['edges']),10);self.assertEqual(value['snapshots'],json.loads(RELEASE)['snapshots'])
  self.assertGreater(opened.provider.renew_current_ack.call_count,1);self.assertGreater(opened.driver.source_admit.call_count,1)
 def test_forged_mixed_released_missingpins_and_drift_refuse(self):
  from ashlar_host.commerce_graphframes_release import CommerceGraphFramesSession
  from dataclasses import replace
  opened=self.opened();runtime=Mock()
  with self.assertRaises(ValueError):CommerceGraphFramesSession(object(),runtime,object())
  with self.assertRaises(ValueError):replace(opened,context=object())
  for field in ('active','_reader_closed'):
   old=getattr(opened.provider,field);setattr(opened.provider,field,not old)
   with self.assertRaises(PermissionError):opened.require_current(opened.context)
   setattr(opened.provider,field,old)
  opened.driver.pin_held=False
  with self.assertRaises(PermissionError):opened.require_current(opened.context)
  opened.driver.pin_held=True;opened.driver.held=False
  with self.assertRaises(PermissionError):opened.require_current(opened.context)
  opened.driver.held=True;opened.driver.source_admit.side_effect=PermissionError('current source revoked')
  with self.assertRaises(PermissionError):opened.require_current(opened.context)
  opened.driver.source_admit.side_effect=None;opened.manifest['profile_version']='forged'
  with self.assertRaises(PermissionError):opened.require_current(opened.context)
 def test_current_ack_cancellation_preserves_identity_and_closes_both(self):
  from ashlar_host.publication_reader import PublicationProvider
  from ashlar_host.ordinary_query_connection import OrdinaryQueryConnectionFactory
  opened=self.opened();events=[];cancel=KeyboardInterrupt('body')
  class Connection:
   def rollback(self):events.append('rollback');raise ValueError('rollback')
   def close(self):events.append('close');raise ValueError('close')
  opened.provider.ordinary_session=OrdinaryQueryConnectionFactory(lambda role:Connection());opened.provider.port={'role':'ack'}
  opened.provider._ack=Mock(side_effect=cancel)
  with self.assertRaises(KeyboardInterrupt)as raised:PublicationProvider.renew_current_ack(opened.provider,opened.context)
  self.assertIs(raised.exception,cancel);self.assertEqual(events,['rollback','close'])
 def test_actual_provider_interval_closing_source_failure_and_cancel_identity(self):
  from contextlib import contextmanager
  from dataclasses import replace
  from ashlar_host.graphframes_release import execute_commerce_graphframes_release,GraphFramesRuntimeConfig
  from ashlar_host.commerce_graphframes_release import CommerceGraphFramesSession
  from ashlar_host.ordinary_query_connection import OrdinaryQueryConnectionFactory
  from ashlar_host.ack import AckScope
  events=[];opened=self.opened();p=opened.provider;d=opened.driver
  class Connection:
   def rollback(self):events.append('rollback')
   def close(self):events.append('connection-close')
  p.ordinary_session=OrdinaryQueryConnectionFactory(lambda role:Connection());p.port={'scope':AckScope('ashlar_ack_fixture','11111111-1111-4111-8111-111111111111','22222222-2222-4222-8222-222222222222','consumer',opened.admission.batch.feed,opened.admission.batch.epoch),'source':'fixture_source','signature':'a'*64,'role':'ack'}
  p.active=False;p._closed_custody=None;p.cleanup_state=None;d.held=False;d.pin_held=False
  del p.renew_current_ack
  p._ack=lambda session:events.append('ack')or{'session':{},'observation':{}}
  @contextmanager
  def writer(stream,context):
   self.assertIs(context,d.context);d.held=True;events.append('writer-open')
   try:yield
   finally:events.append('writer-close');d.held=False
  d.writer=writer;opened=replace(opened)
  runtime=GraphFramesRuntimeConfig(Path('/operator/python'),'a'*64,Path('/operator/java'),'b'*64,'c'*40,'d'*64,'e'*64,Path('/operator/jars'),Path('/operator/work'),300,1048576,16777216)
  session=CommerceGraphFramesSession(opened,Mock(),object());cancel=KeyboardInterrupt('child')
  def child(*args):
   events.append('child');d.source_admit.side_effect=PermissionError('closing source revoked');raise cancel
  with patch.object(GraphFramesRuntimeConfig,'renew'),patch('ashlar_host.graphframes_release._execute_prepared_graphframes',side_effect=child):
   with self.assertRaises(KeyboardInterrupt)as raised:execute_commerce_graphframes_release(session,runtime,PACK,Mock())
  self.assertIs(raised.exception,cancel);self.assertIn('ack',events);self.assertEqual(events[-3:],['rollback','connection-close','writer-close']);self.assertFalse(p.active);self.assertFalse(d.pin_held);self.assertIsNone(p._closed_custody)
