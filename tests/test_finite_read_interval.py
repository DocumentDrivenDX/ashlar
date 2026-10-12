"""Actual filesystem interval/provider owners with inert source and native ports."""
import tempfile,unittest,json,os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from ashlar_host.finite_publication import GRAPH_ROLES
from ashlar_host.supply_chain_publication import FiniteSupplyChainDriver
from ashlar_host.supply_chain_reader import FiniteSupplyChainProvider

class Tests(unittest.TestCase):
 def fixture(self,root,lock=True):
  d=object.__new__(FiniteSupplyChainDriver); calls=[];roles=sorted(GRAPH_ROLES|{'attempts','manifest'}); d.tables={r:'local.finite.'+r for r in roles};d.context=object();d.held=False
  d.columns={r:(('value','STRING'),)for r in GRAPH_ROLES};d.expected={r:[{'value':r}]for r in GRAPH_ROLES};d.request={'stream':'original'};d.manifest={'publication_id':'original','table_versions_json':json.dumps({d.tables[r]:2 for r in GRAPH_ROLES})};d.publication_id='original'
  def renewal():calls.append('source')
  d.source=SimpleNamespace(renew=renewal,metadata=lambda:{'original':True});d.current_admission=lambda *args:calls.append('current')
  targets={d.tables[r]:SimpleNamespace(table=d.tables[r],uuid=r,path=root/r)for r in roles}
  def query(sql,params):
   if sql.startswith('DESCRIBE'):
    r=next(r for r in roles if '`'+r+'`' in sql);calls.append('native:'+r);return SimpleNamespace(rows=[{'id':r,'location':str(root/r)}])
   r=next(r for r in GRAPH_ROLES if '`'+r+'`' in sql);calls.append('bag:'+r);return SimpleNamespace(rows=[{'value':r}])
  def sql(query):
   r=next(r for r in GRAPH_ROLES if 'alias_'+r in query);calls.append('alias:'+r);return SimpleNamespace(first=lambda:SimpleNamespace(asDict=lambda:{'id':r,'location':str(root/r)}))
  d.transport=SimpleNamespace(journal_path=root/'journal.sqlite',targets=targets,query=query,spark=SimpleNamespace(sql=sql))
  if lock:(root/'.finite-source-writer.lock').write_bytes(b'')
  return d,calls
 def test_existing_only_missing_lock(self):
  with tempfile.TemporaryDirectory() as t:
   root=Path(t).resolve();d,c=self.fixture(root,False)
   with self.assertRaises(FileNotFoundError):
    with d.read_interval('original',d.context):self.fail('yielded')
   self.assertEqual(c,[]);self.assertFalse((root/'.finite-source-writer.lock').exists());self.assertFalse(d.held)
 def test_success_and_nonnested(self):
  with tempfile.TemporaryDirectory() as t:
   root=Path(t).resolve();d,c=self.fixture(root);p=root/'.finite-source-writer.lock';before=p.stat()
   with d.read_interval('original',d.context):
    self.assertTrue(d.held)
    with self.assertRaises(PermissionError):
     with d.read_interval('original',d.context):pass
   self.assertFalse(d.held);self.assertEqual(before.st_ino,p.stat().st_ino);self.assertEqual(p.read_bytes(),b'');self.assertEqual(c.count('source'),2)
   for role in d.tables:self.assertEqual(c.count('native:'+role),2)
 def test_symlink_refused_without_mutation(self):
  with tempfile.TemporaryDirectory() as t:
   root=Path(t).resolve();d,c=self.fixture(root,False);other=root/'other';other.write_bytes(b'original');(root/'.finite-source-writer.lock').symlink_to(other)
   with self.assertRaises(OSError):
    with d.read_interval('original',d.context):pass
   self.assertEqual(other.read_bytes(),b'original');self.assertEqual(c,[])
 def test_body_cancel_independent_closings_and_release(self):
  with tempfile.TemporaryDirectory() as t:
   root=Path(t).resolve();d,c=self.fixture(root);primary=KeyboardInterrupt();original=d.source.renew
   def fail():c.append('source-fail');raise ValueError('closing')
   with self.assertRaises(KeyboardInterrupt) as caught:
    with d.read_interval('original',d.context):d.source.renew=fail;raise primary
   self.assertIs(caught.exception,primary);self.assertFalse(d.held)
   for role in d.tables:self.assertEqual(c.count('native:'+role),2)
   self.assertNotIn('source-fail',c)
 def test_opening_failure_still_all_native_closings(self):
  with tempfile.TemporaryDirectory() as t:
   d,c=self.fixture(Path(t).resolve());primary=ValueError('opening')
   def fail():c.append('source-fail');raise primary
   d.source.renew=fail
   with self.assertRaises(ValueError) as caught:
    with d.read_interval('original',d.context):self.fail('yielded')
   self.assertIs(caught.exception,primary);self.assertFalse(d.held)
   for role in d.tables:self.assertEqual(c.count('native:'+role),1)
 def test_named_lock_replacement_refuses_and_closes(self):
  with tempfile.TemporaryDirectory() as t:
   root=Path(t).resolve();d,c=self.fixture(root);p=root/'.finite-source-writer.lock'
   with self.assertRaises(ValueError):
    with d.read_interval('original',d.context):p.rename(root/'original-lock');p.write_bytes(b'')
   self.assertFalse(d.held)
   for role in d.tables:self.assertEqual(c.count('native:'+role),2)
 def test_provider_cancel_original_aliases_even_driver_drift(self):
  with tempfile.TemporaryDirectory() as t:
   d,c=self.fixture(Path(t).resolve());aliases={d.tables[r]:'local.finite.alias_'+r for r in GRAPH_ROLES};p=FiniteSupplyChainProvider(d,aliases,{});primary=GeneratorExit();resolved=SimpleNamespace(descriptor=SimpleNamespace(publication_id='original',raw={}))
   with patch.object(FiniteSupplyChainProvider,'resolve',return_value=resolved):
    with self.assertRaises(GeneratorExit) as caught:
     with p.interval(d.context):p.driver=object();raise primary
   self.assertIs(caught.exception,primary);self.assertFalse(p.active);self.assertIsNone(p.closed);self.assertFalse(d.held)
   for role in GRAPH_ROLES:self.assertEqual(c.count('alias:'+role),1)
   for role in d.tables:self.assertEqual(c.count('native:'+role),2)
 def test_provider_opening_resolve_failure_attempts_aliases(self):
  with tempfile.TemporaryDirectory() as t:
   d,c=self.fixture(Path(t).resolve());p=FiniteSupplyChainProvider(d,{d.tables[r]:'local.finite.alias_'+r for r in GRAPH_ROLES},{});primary=ValueError('resolve')
   with patch.object(FiniteSupplyChainProvider,'resolve',side_effect=primary):
    with self.assertRaises(ValueError) as caught:
     with p.interval(d.context):self.fail('yielded')
   self.assertIs(caught.exception,primary);self.assertFalse(p.active);self.assertIsNone(p.closed)
   for role in GRAPH_ROLES:self.assertEqual(c.count('alias:'+role),1)
 def test_first_native_close_failure_does_not_skip_original_bags(self):
  with tempfile.TemporaryDirectory() as t:
   d,c=self.fixture(Path(t).resolve());query=d.transport.query;primary=ValueError('body');closing=[]
   def fail(sql,params):
    if closing and sql.startswith('DESCRIBE'):
     c.append('failed-native');raise KeyboardInterrupt()
    return query(sql,params)
   d.transport.query=fail
   with self.assertRaises(ValueError) as caught:
    with d.read_interval('original',d.context):closing.append(True);raise primary
   self.assertIs(caught.exception,primary);self.assertEqual(c.count('failed-native'),6)
   for role in GRAPH_ROLES:self.assertEqual(c.count('bag:'+role),2)
   self.assertFalse(d.held)
 def test_parent_alias_drift_refuses_and_original_closings_attempted(self):
  with tempfile.TemporaryDirectory() as t:
   parent=Path(t).resolve()/'parent';parent.mkdir();d,c=self.fixture(parent);moved=parent.with_name('moved')
   with self.assertRaises(ValueError):
    with d.read_interval('original',d.context):parent.rename(moved);parent.symlink_to(moved)
   self.assertFalse(d.held)
   for role in d.tables:self.assertEqual(c.count('native:'+role),2)
 def test_malformed_manifest_cancel_closes_original_vector(self):
  with tempfile.TemporaryDirectory() as t:
   d,c=self.fixture(Path(t).resolve());primary=KeyboardInterrupt()
   with self.assertRaises(KeyboardInterrupt) as caught:
    with d.read_interval('original',d.context):d.manifest['table_versions_json']='not-json';raise primary
   self.assertIs(caught.exception,primary);self.assertFalse(d.held)
   for role in d.tables:self.assertEqual(c.count('native:'+role),2)
   for role in GRAPH_ROLES:self.assertEqual(c.count('bag:'+role),2)
 def test_registry_drift_refuses_original_targets_still_close(self):
  with tempfile.TemporaryDirectory() as t:
   d,c=self.fixture(Path(t).resolve())
   with self.assertRaises(ValueError):
    with d.read_interval('original',d.context):d.transport.targets[d.tables['object_current']]=SimpleNamespace(table=d.tables['object_current'],uuid='replacement',path=Path(t))
   for role in d.tables:self.assertEqual(c.count('native:'+role),2)
   self.assertFalse(d.held)
 def test_provider_drift_resolves_original_bound_nominal_owner(self):
  with tempfile.TemporaryDirectory() as t:
   d,c=self.fixture(Path(t).resolve());aliases={d.tables[r]:'local.finite.alias_'+r for r in GRAPH_ROLES};p=FiniteSupplyChainProvider(d,aliases,{});primary=ValueError('body');seen=[];resolved=SimpleNamespace(descriptor=SimpleNamespace(publication_id='original',raw={}))
   def resolve(owner,publication_id,vector,*,context,**kwargs):
    self.assertEqual(publication_id,'original');self.assertEqual(vector,{t:d.transport.targets[t].uuid for t in aliases});seen.append((owner,owner.driver,dict(owner.aliases),context));return resolved
   with patch('ashlar_host.supply_chain_reader.resolve_publication',side_effect=resolve):
    with self.assertRaises(ValueError) as caught:
     with p.interval(d.context):p.driver=object();p.aliases={};raise primary
   self.assertIs(caught.exception,primary);self.assertEqual(len(seen),2);self.assertIsNot(seen[1][0],p);self.assertIs(seen[1][1],d);self.assertEqual(seen[1][2],aliases);self.assertIs(seen[1][3],d.context)
   for role in GRAPH_ROLES:self.assertEqual(c.count('alias:'+role),1)
   self.assertFalse(d.held);self.assertFalse(p.active);self.assertIsNone(p.closed)
 def test_provider_context_and_binding_drift_withhold_receipt_close_original(self):
  for field,value in (('context',object()),('expected_binding','{"substituted":true}')):
   with self.subTest(field=field),tempfile.TemporaryDirectory() as t:
    d,c=self.fixture(Path(t).resolve());aliases={d.tables[r]:'local.finite.alias_'+r for r in GRAPH_ROLES};p=FiniteSupplyChainProvider(d,aliases,{'original':True});seen=[];resolved=SimpleNamespace(descriptor=SimpleNamespace(publication_id='original',raw={}))
    def resolve(owner,publication_id,vector,*,context,**kwargs):
     seen.append((owner,context,owner.expected_binding));self.assertEqual(vector,{native:d.transport.targets[native].uuid for native in aliases});return resolved
    with patch('ashlar_host.supply_chain_reader.resolve_publication',side_effect=resolve):
     with self.assertRaises(ValueError):
      with p.interval(d.context):setattr(p,field,value)
    self.assertIsNone(p.closed);self.assertFalse(p.active);self.assertFalse(d.held);self.assertEqual(len(seen),2);self.assertIsNot(seen[1][0],p);self.assertIs(seen[1][1],d.context);self.assertEqual(seen[1][2],'{"original":true}')
    for role in GRAPH_ROLES:self.assertEqual(c.count('alias:'+role),1)
    for role in d.tables:self.assertEqual(c.count('native:'+role),2)
 def test_provider_preopening_binding_drift_no_acquisition(self):
  with tempfile.TemporaryDirectory() as t:
   d,c=self.fixture(Path(t).resolve());p=FiniteSupplyChainProvider(d,{d.tables[r]:'local.finite.alias_'+r for r in GRAPH_ROLES},{});p.expected_binding='{"substituted":true}'
   with self.assertRaises(PermissionError):
    with p.interval(d.context):self.fail('yielded')
   self.assertEqual(c,[]);self.assertIsNone(p.closed)
 def test_transport_query_port_drift_original_closings_and_no_foreign(self):
  with tempfile.TemporaryDirectory() as t:
   d,c=self.fixture(Path(t).resolve());foreign=[];primary=ValueError('body')
   def substituted(*args):foreign.append(args);raise AssertionError('foreign query')
   with self.assertRaises(ValueError) as caught:
    with d.read_interval('original',d.context):d.transport.query=substituted;raise primary
   self.assertIs(caught.exception,primary);self.assertEqual(foreign,[]);self.assertFalse(d.held)
   for role in d.tables:self.assertEqual(c.count('native:'+role),2)
   for role in GRAPH_ROLES:self.assertEqual(c.count('bag:'+role),2)
 def test_provider_sql_port_drift_withholds_receipt_original_resolution_and_aliases(self):
  with tempfile.TemporaryDirectory() as t:
   d,c=self.fixture(Path(t).resolve());aliases={d.tables[r]:'local.finite.alias_'+r for r in GRAPH_ROLES};p=FiniteSupplyChainProvider(d,aliases,{});seen=[];foreign=[];resolved=SimpleNamespace(descriptor=SimpleNamespace(publication_id='original',raw={}))
   def resolve(owner,publication_id,vector,*,context,**kwargs):seen.append((owner,vector));return resolved
   def substituted(*args):foreign.append(args);raise AssertionError('foreign SQL')
   with patch('ashlar_host.supply_chain_reader.resolve_publication',side_effect=resolve):
    with self.assertRaises(ValueError):
     with p.interval(d.context):d.transport.spark.sql=substituted
   self.assertEqual(foreign,[]);self.assertEqual(len(seen),2);self.assertIsNone(p.closed);self.assertFalse(d.held);self.assertFalse(p.active)
   for role in GRAPH_ROLES:self.assertEqual(c.count('alias:'+role),1)
   for role in d.tables:self.assertEqual(c.count('native:'+role),2)
 def test_runtime_port_drift_refuses_before_foreign_callback(self):
  with tempfile.TemporaryDirectory() as t:
   d,c=self.fixture(Path(t).resolve());p=FiniteSupplyChainProvider(d,{d.tables[r]:'local.finite.alias_'+r for r in GRAPH_ROLES},{});foreign=[]
   with d.read_interval('original',d.context):
    p.active=True;original=d.transport.spark.sql
    try:
     d.transport.spark.sql=lambda *args:foreign.append(args)
     with self.assertRaises(ValueError):p.runtime(d.context)
     self.assertEqual(foreign,[])
    finally:d.transport.spark.sql=original;p.active=False
 def test_active_bounds_snapshot_uses_fixed_scalar_public_query_no_collect(self):
  with tempfile.TemporaryDirectory() as t:
   d,c=self.fixture(Path(t).resolve());p=FiniteSupplyChainProvider(d,{d.tables[r]:'local.finite.alias_'+r for r in GRAPH_ROLES},{});d.transport.observation_bounds=SimpleNamespace(total_bytes=1024);d.transport.spark.read=mock_read=SimpleNamespace(format=lambda *args:(_ for _ in ()).throw(AssertionError('unbounded SDK collect')));query=d.transport.query
   def capture(sql,params):
    if sql.startswith('SELECT 1 AS present'):c.append('bounded-existence');return SimpleNamespace(rows=[])
    return query(sql,params)
   d.transport.query=capture;p._original_query=capture
   snapshot=p.inspect_snapshot(d.tables['object_current'],2);self.assertEqual(snapshot.version,2);self.assertIn('bounded-existence',c)
 def test_active_bounds_schema_uses_complete_public_bounded_snapshot(self):
  with tempfile.TemporaryDirectory() as t:
   d,c=self.fixture(Path(t).resolve());aliases={d.tables[r]:'local.finite.alias_'+r for r in GRAPH_ROLES};p=FiniteSupplyChainProvider(d,aliases,{});d.transport.observation_bounds=SimpleNamespace(total_bytes=1024);p.active=True;native=d.tables['object_current'];schema={'type':'struct','fields':[{'name':'value','type':'string','nullable':True,'metadata':{}}]};calls=[]
   def bounded(target,version,*,remaining_bytes):calls.append((target,version,remaining_bytes));return {'schema':[['value','string']],'rows':[]},schema
   d.transport.bounded_snapshot=bounded;d.transport.spark.read=SimpleNamespace(format=lambda *args:(_ for _ in ()).throw(AssertionError('unbounded schema JSON')))
   table={'name':aliases[native].split('.'),'uuid':d.transport.targets[native].uuid,'version':2};resolved=SimpleNamespace(snapshots={native:SimpleNamespace(uuid=table['uuid'],version=2)})
   with patch.object(FiniteSupplyChainProvider,'runtime'):
    result=p.native_table_schema(table,resolved,d.context)
   self.assertEqual(result,{'table':table,'schema':schema,'nativeTypes':[['value','STRING']]});self.assertEqual(calls[0][1:],(2,1024))
 def test_active_bounds_alias_admission_and_closing_use_public_query(self):
  with tempfile.TemporaryDirectory() as t:
   d,c=self.fixture(Path(t).resolve());aliases={d.tables[r]:'local.finite.alias_'+r for r in GRAPH_ROLES};binding={'publication':{'id':'original','manifestUuid':d.transport.targets[d.tables['manifest']].uuid,'tables':[{'name':aliases[n].split('.'),'uuid':d.transport.targets[n].uuid,'version':2}for n in sorted(aliases)]}};p=FiniteSupplyChainProvider(d,aliases,binding);d.transport.observation_bounds=SimpleNamespace(total_bytes=1024);query=d.transport.query
   def bounded(sql,params):
    for n,alias in aliases.items():
     if 'alias_'+n.split('.')[-1] in sql:c.append('bounded-alias');return SimpleNamespace(rows=[{'id':d.transport.targets[n].uuid,'location':str(d.transport.targets[n].path)}])
    return query(sql,params)
   d.transport.query=bounded;p._original_query=bounded;p.active=True;resolved=SimpleNamespace(descriptor=SimpleNamespace(publication_id='original',raw={}),snapshots={n:SimpleNamespace(uuid=d.transport.targets[n].uuid,version=2)for n in aliases})
   with patch.object(FiniteSupplyChainProvider,'runtime'):p.admit_binding(binding,resolved,d.context)
   self.assertEqual(c.count('bounded-alias'),4);p.active=False
   with patch.object(FiniteSupplyChainProvider,'resolve',return_value=resolved):
    with p.interval(d.context):pass
   self.assertEqual(c.count('bounded-alias'),8);self.assertFalse(any(v.startswith('alias:')for v in c))
if __name__=='__main__':unittest.main()
