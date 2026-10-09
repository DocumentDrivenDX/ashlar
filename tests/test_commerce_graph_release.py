import copy,json,unittest
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch
from ashlar.graph_release import GraphRelease
from run_commerce_graph_release import CommerceGraphPolicy,HeldCommercePins,export_commerce_release
from local_delta_custody import encoded

class Admission:
 def __init__(self):self.facts={'original':'public receipt'}
 def metadata(self):return copy.deepcopy(self.facts)
class Provider:
 def __init__(self,driver,context):self.driver=driver;self.context=context;self.active=False;self.closing_refuse=False;self.suppress=False;self.calls=[]
 @contextmanager
 def interval(self,context):
  if context is not self.context:raise PermissionError('Wrong context')
  self.active=True;self.driver.pin_held=True;self.calls.append('held ACK open')
  try:
   try:yield
   except PermissionError:
    if not self.suppress:raise
   if self.closing_refuse:raise PermissionError('Closing protected ACK refused')
   self.calls.append('held ACK closed')
  finally:self.active=False;self.driver.pin_held=False
class Tests(unittest.TestCase):
 def fixture(self):
  context=object();manifest={'publication_id':'original','schema_revisions_json':encoded({'source':'r1'}),'table_versions_json':encoded({t:1 for t in ['local.c.object_current','local.c.edge_current','local.c.tombstone','local.c.whole_source_history']}),'source_progress_json':encoded({'source':{'epoch':'original','position':1}}),'profile_version':'ashlar-delta/0.3','recorded_at':'1','validation_report_json':'{"complete":true}'}
  driver=SimpleNamespace(policy=SimpleNamespace(active={'manifest':manifest}),tables={r:'local.c.'+r for r in ['object_current','edge_current','tombstone','whole_source_history']},source_admission=Admission(),pin_held=False,transport=SimpleNamespace(targets={t:SimpleNamespace(uuid='actual-fixture-uuid')for t in json.loads(manifest['table_versions_json'])}))
  provider=Provider(driver,context);expected={'object_current':[{'id':'1','props_json':' {"13":9007199254740993} ','retained_json':'{"source":"original"}'}],'edge_current':[{'id':'2','props_json':'{}','retained_json':'original edge','source_id':'1','target_id':'1','order_key':None}]}
  vector=SimpleNamespace(identity='whole fixture vector');policy=CommerceGraphPolicy(provider,driver,vector,context,expected);descriptor=SimpleNamespace(raw=manifest)
  return provider,driver,context,expected,vector,policy,descriptor
 def test_complete_exact_original_bag_and_independent_edges(self):
  p,d,c,e,v,policy,desc=self.fixture()
  with p.interval(c):
   policy.bind_descriptor(desc,v,c)
   for role,rows in e.items():
    for row in rows:policy.authorize_graph_row(desc,d.tables[role],row,c)
   nodes=[dict(e['object_current'][0],graph_id='node')];edges=[dict(e['edge_current'][0],graph_id='edge',src='node',dst='node')]
   policy.authorize_graph_result(desc,nodes,edges,c)
  self.assertFalse(d.pin_held);self.assertFalse(p.active)
 def test_changed_values_endpoints_missing_duplicate_bags_refuse(self):
  for change in ['props_json','retained_json','target_id','order_key','missing','duplicate']:
   p,d,c,e,v,policy,desc=self.fixture();nodes=copy.deepcopy(e['object_current']);edges=copy.deepcopy(e['edge_current'])
   if change=='missing':nodes=[]
   elif change=='duplicate':edges+=copy.deepcopy(edges)
   else:edges[0][change]='changed'
   with self.subTest(change=change),p.interval(c),self.assertRaises(PermissionError):policy.authorize_graph_result(desc,nodes,edges,c)
 def test_original_source_descriptor_context_and_whole_vector_are_required(self):
  for change in ['source','descriptor','vector','context','closed']:
   p,d,c,e,v,policy,desc=self.fixture()
   with p.interval(c):
    if change=='source':d.source_admission.facts['original']='changed'
    if change=='descriptor':desc=SimpleNamespace(raw=dict(desc.raw,publication_id='changed'))
    if change=='vector':v=SimpleNamespace(identity='partial')
    if change=='context':c=object()
    if change=='closed':d.pin_held=False
    with self.subTest(change=change),self.assertRaises(PermissionError):policy.bind_descriptor(desc,v,c)
 def test_pin_reuse_never_opens_unheld_or_different_vector(self):
  p,d,c,e,v,policy,desc=self.fixture();pins=HeldCommercePins(policy)
  with self.assertRaises(PermissionError):
   with pins.hold(v,context=c):pass
  with p.interval(c):
   with self.assertRaises(PermissionError):
    with pins.hold(SimpleNamespace(identity='wrong'),context=c):pass
   with pins.hold(v,context=c):self.assertTrue(d.pin_held)
 def test_export_only_returns_after_whole_provider_closure(self):
  for mode in ['pass','closing-refusal','suppressed-read-refusal']:
   p,d,c,e,v,policy,desc=self.fixture();p.closing_refuse=mode=='closing-refusal';p.suppress=mode=='suppressed-read-refusal'
   def read(executor,backend,pins,vector,actual_policy,**args):
    self.assertTrue(p.active);self.assertTrue(d.pin_held);self.assertEqual(set(vector.targets),set(json.loads(desc.raw['table_versions_json'])));self.assertEqual((args['max_nodes'],args['max_edges']),(1,1))
    with pins.hold(vector,context=c):
     if p.suppress:raise PermissionError('Read refused')
    return GraphRelease(b'exact original bytes','fixture digest')
   with patch('run_commerce_graph_release.read_graph_release',read):
    if mode=='pass':self.assertEqual(export_commerce_release(p,d,e,context=c).payload,b'exact original bytes');self.assertEqual(p.calls,['held ACK open','held ACK closed'])
    else:
     with self.assertRaises(PermissionError):export_commerce_release(p,d,e,context=c)
   self.assertFalse(p.active);self.assertFalse(d.pin_held)
 def test_outer_reader_closing_failure_withholds_release_and_custody(self):
  from run_commerce_graph_release import read_commerce_export,persist_commerce_export
  from test_private_graph_custody import Tests as CustodyFixture
  import tempfile
  from pathlib import Path
  release,receipt=CustodyFixture().fixture();value=json.loads(receipt);ack=value['ack'];source=json.loads(value['originalManifest']['validation_report_json'])['source_admission']
  captured={'format':'ashlar-private-local-protected-ack-interval/0.1','scope':ack['scope'],'source_schema':ack['opening']['session']['source_schema'],'source_signature_sha256':ack['opening']['session']['source_signature_sha256'],'original_request_hex':ack['requestHex'],'original_manifest_hex':ack['manifestHex'],'opening_ack':ack['opening'],'closing_ack':ack['closing'],'qualification':'Fixture only'}
  for failure in ['pass','outer-close','suppressed-inner']:
   state={'active':False};context=object();opened=SimpleNamespace(provider=SimpleNamespace(closed_interval_custody=lambda c:copy.deepcopy(captured)),driver=object(),independent_expected={},context=context,admission=SimpleNamespace(metadata=lambda:source),original_native_files={'original':'exact hash'})
   @contextmanager
   def factory():
    state['active']=True
    try:
     try:yield opened
     except PermissionError:
      if failure!='suppressed-inner':raise
     if failure=='outer-close':raise PermissionError('Outer original native file parity refused')
    finally:state['active']=False
   def export(*args,**kw):
    self.assertTrue(state['active'])
    if failure=='suppressed-inner':raise PermissionError('Read failed')
    return release
   with tempfile.TemporaryDirectory()as t,patch('run_commerce_graph_release.export_commerce_release',export):
    output=Path(t)/'export'
    if failure=='pass':
     actual,custody,_=read_commerce_export(factory);self.assertFalse(state['active']);self.assertFalse(output.exists());persist_commerce_export(output,actual,custody);self.assertEqual((output/'release.graph.json').read_bytes(),release.payload)
    else:
     with self.assertRaises(PermissionError):read_commerce_export(factory)
     self.assertFalse(output.exists())
 def test_pair_persistence_failure_removes_both_new_artifacts(self):
  from run_commerce_graph_release import persist_commerce_export
  from test_private_graph_custody import Tests as CustodyFixture
  import tempfile
  from pathlib import Path
  release,receipt=CustodyFixture().fixture()
  with tempfile.TemporaryDirectory()as t:
   output=Path(t)/'export'
   with patch('os.fsync',side_effect=OSError('Interrupted custody persistence')):
    with self.assertRaises(OSError):persist_commerce_export(output,release,receipt)
   self.assertFalse(output.exists())
   persist_commerce_export(output,release,receipt)
   with self.assertRaises(FileExistsError):persist_commerce_export(output,release,receipt)
   self.assertEqual((output/'custody.json').read_bytes(),receipt)
if __name__=='__main__':unittest.main()
