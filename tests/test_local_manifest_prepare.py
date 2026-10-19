# SPDX-License-Identifier: MIT OR Apache-2.0
"""Exact local intent write-ahead boundaries using owned SQLite and inert native engine."""
import unittest
from unittest.mock import patch
import test_local_delta_effect_plan as harness
from ashlar_host.delta_custody import LocalDeltaError,LocalDeltaUncertain

class Tests(unittest.TestCase):
 def setUp(self):
  self.host=harness.Tests();self.host.setUp();self.addCleanup(self.host.doCleanups)
  self.transport=self.host.transport;self.sql=harness.SQL;self.params={'id':'1'};self.digest='b'*64
 def prepare(self):return self.transport.prepare_mutation('original',self.sql,self.params,intent_digest=self.digest,context='held')
 def recover(self):return self.transport.recover('original',self.sql,self.params,intent_digest=self.digest,context='held')
 def test_prepare_is_durable_idempotent_and_recovery_first_submits_once(self):
  self.prepare();before=self.transport.db.execute('SELECT * FROM local_operation').fetchall()
  self.assertEqual(before[0][-2:],('prepared',None));self.assertEqual(self.host.engine.writes,0)
  self.prepare();self.assertEqual(before,self.transport.db.execute('SELECT * FROM local_operation').fetchall())
  self.recover();self.recover();self.assertEqual(self.host.engine.writes,1)
 def test_missing_original_recovery_never_prepares_or_submits(self):
  with self.assertRaises(LocalDeltaError):self.recover()
  self.assertEqual(self.host.engine.writes,0);self.assertEqual(self.transport.db.execute('SELECT * FROM local_operation').fetchall(),[])
 def test_conflicting_intent_and_changed_predecessor_refuse_without_native_write(self):
  self.prepare();self.params={'id':'replacement'}
  with self.assertRaises(LocalDeltaError):self.prepare()
  self.params={'id':'1'};self.host.engine.history[0]['version']=3
  with self.assertRaises(LocalDeltaError):self.prepare()
  with self.assertRaises(LocalDeltaError):self.recover()
  self.assertEqual(self.host.engine.writes,0)
 def test_preparation_does_not_resubmit_submitted_uncertainty(self):
  self.prepare();self.transport.db.execute("UPDATE local_operation SET state='submitted'")
  with self.assertRaises(LocalDeltaError):self.prepare()
  with self.assertRaises(LocalDeltaUncertain):self.recover()
  self.assertEqual(self.host.engine.writes,0)
 def test_lost_journal_row_with_native_commit_refuses_replacement(self):
  self.prepare();self.recover();self.transport.db.execute('DELETE FROM local_operation')
  with self.assertRaises(LocalDeltaError):self.prepare()
  self.assertEqual(self.host.engine.writes,1)
 def test_replacement_during_closing_history_refuses_preparation(self):
  history=self.transport.original_history;sql=self.host.engine.sql;reads=[];replaced=[]
  def observed(target):
   rows=history(target);reads.append(True)
   if len(reads)==2:replaced.append(True)
   return rows
  def native(statement,args=None):
   result=sql(statement,args)
   if replaced and statement.startswith('DESCRIBE DETAIL'):
    from types import SimpleNamespace
    return SimpleNamespace(first=lambda:harness.Row(id='replacement',properties=self.host.engine.properties))
   return result
  with patch.object(self.transport,'original_history',observed),patch.object(self.host.engine,'sql',native),self.assertRaises(LocalDeltaError):
   self.prepare()
  self.assertEqual(len(reads),2);self.assertEqual(self.host.engine.writes,0)
  self.assertEqual(self.transport.db.execute('SELECT state FROM local_operation').fetchall(),[('prepared',)])
 def test_closing_admission_cannot_change_prepared_row_or_native_predecessor(self):
  for control in ('row','history'):
   with self.subTest(control=control):
    self.transport.db.execute('DELETE FROM local_operation');self.host.engine.history[0]['version']=0
    admission=self.transport.policy.admit;calls=[]
    def changed(intent,context):
     calls.append(context)
     if len(calls)==2:
      if control=='row':self.transport.db.execute("UPDATE local_operation SET intent_sha='changed'")
      else:self.host.engine.history[0]['version']=1
     return admission(intent,context)
    with patch.object(self.transport.policy,'admit',changed),self.assertRaises(LocalDeltaError):self.prepare()
    self.assertEqual(self.host.engine.writes,0)

class StoreTests(unittest.TestCase):
 def test_closing_admission_identity_change_refuses_preparation(self):
  import test_manifest as fixture
  from ashlar.manifest import DeltaManifestStore,ManifestError
  from ashlar.native import SQLResult
  executor=fixture.Executor();executor.prepare=lambda *args:None
  original=executor.query;changed=[]
  def query(sql,parameters):
   if changed and sql.startswith('DESCRIBE'):return SQLResult([{'id':'replacement'}])
   return original(sql,parameters)
  executor.query=query
  class Policy(fixture.Policy):
   calls=0
   def admit(self,row,context):
    super().admit(row,context);self.calls+=1
    if self.calls==2:changed.append(True)
  with self.assertRaisesRegex(ManifestError,'identity changed'):
   DeltaManifestStore(executor,Policy(),'c.s.manifest','uuid').prepare(fixture.ROW,context='admitted')
  self.assertIsNone(executor.row)
 def test_core_prepare_uses_identical_commit_statement_without_submission(self):
  import test_manifest as fixture
  from ashlar.manifest import DeltaManifestStore
  executor=fixture.Executor();prepared=[]
  executor.prepare=lambda sql,parameters:prepared.append((sql,parameters))
  store=DeltaManifestStore(executor,fixture.Policy(),'c.s.manifest','uuid')
  store.prepare(fixture.ROW,context='admitted')
  self.assertIsNone(executor.row);self.assertEqual(prepared,[store.statement(fixture.ROW)])
  store.commit(fixture.ROW,context='admitted')
  self.assertIn(prepared[0][0],executor.calls)
 def test_backend_prepare_failure_keeps_applied_and_never_commits_or_acks(self):
  import test_stored_publisher as fixture
  helper=fixture.StoredPublisherTests()
  with helper.fixture()as(phases,driver,policy,transport,api):
   backend=helper.backend(phases,driver,transport,policy);factory=backend.manifest_factory;events=[]
   def selected(request,context):
    store=factory(request,context)
    def prepare(row,*,context):
     events.append('prepare');raise PermissionError('inert preparation refusal')
    store.prepare=prepare;return store
   backend.manifest_factory=selected
   with self.assertRaises(PermissionError):helper.publish(backend,object())
   self.assertEqual(phases.rows[-1]['phase'],'applied');self.assertEqual(events,['prepare']);self.assertEqual(api.calls,[]);self.assertNotIn('ack',driver.calls)
 def test_unknown_prepare_capability_refuses_before_committing(self):
  import test_stored_publisher as fixture
  from ashlar.publisher import PublicationError
  helper=fixture.StoredPublisherTests()
  with helper.fixture()as(phases,driver,policy,transport,api):
   backend=helper.backend(phases,driver,transport,policy);factory=backend.manifest_factory
   def selected(request,context):
    store=factory(request,context);store.prepare=None;return store
   backend.manifest_factory=selected
   with self.assertRaises(PublicationError):helper.publish(backend,object())
   self.assertEqual(phases.rows[-1]['phase'],'applied');self.assertEqual(api.calls,[])
 def test_core_preparation_cancellation_attempts_both_closing_gates(self):
  from contextlib import contextmanager
  import test_manifest as fixture
  from ashlar.manifest import DeltaManifestStore
  executor=fixture.Executor();events=[];primary=KeyboardInterrupt('inert original cancellation')
  def prepare(*args):events.append('prepare');raise primary
  executor.prepare=prepare
  class Policy:
   @contextmanager
   def writer(self,*args):
    try:yield
    except BaseException:pass
   def admit(self,*args):events.append('admit')
  store=DeltaManifestStore(executor,Policy(),'c.s.manifest','uuid')
  with self.assertRaises(KeyboardInterrupt)as caught:store.prepare(fixture.ROW,context='admitted')
  self.assertIs(caught.exception,primary);self.assertEqual(events,['admit','prepare','admit'])
  self.assertEqual(len(executor.calls),2)

class PublisherBoundaryTests(unittest.TestCase):
 def test_actual_local_manifest_preparation_survives_both_phase_crash_boundaries(self):
  import json
  from types import SimpleNamespace
  import test_stored_publisher as fixture
  from ashlar.native import SQLResult
  from ashlar.attempt_store import DeltaAttemptStore
  from ashlar.stored_publisher import StoredPublisherBackend
  from ashlar_host.driver import ManifestPort
  helper=fixture.StoredPublisherTests()
  for boundary in ('before-phase','after-phase'):
   with self.subTest(boundary=boundary):
    host=harness.Tests();host.setUp()
    try:
     transport=host.transport;phases=fixture.PhaseExecutor();driver=fixture.Driver();context='held';events=[]
     owner=SimpleNamespace(transport=transport,tables={'manifest':harness.TABLE},context=context,
       require=lambda supplied:self.assertEqual(supplied,context),validate_manifest=lambda row,supplied:None)
     row=None;original_sql=host.engine.sql
     def sql(statement,args=None):
      nonlocal row
      if statement.startswith('MERGE'):row=json.loads(args['row']);events.append('native-manifest')
      return original_sql(statement,args)
     host.engine.sql=sql
     def query(statement,parameters):
      if statement.startswith('DESCRIBE'):return SQLResult([{'id':'original-uuid'}])
      return SQLResult([]if row is None else[row])
     transport.query=query
     def backend():
      return StoredPublisherBackend(DeltaAttemptStore(phases,fixture.Policy(),'c.s.attempt','attempt-uuid'),driver,
       lambda request,supplied:ManifestPort(owner,request))
     first=backend();original_phase=phases.query
     def phase(statement,parameters):
      if statement.startswith('MERGE')and json.loads(parameters['payload'])['phase']=='committing':
       self.assertEqual(transport.db.execute('SELECT state FROM local_operation').fetchall(),[('prepared',)])
       events.append('committing')
       if boundary=='before-phase':raise RuntimeError('inert crash before phase append')
      return original_phase(statement,parameters)
     phases.query=phase
     if boundary=='after-phase':first.commit=lambda *args:(_ for _ in()).throw(RuntimeError('inert crash after phase append'))
     with self.assertRaises(RuntimeError):helper.publish(first,context)
     self.assertEqual(host.engine.writes,0)
     original=transport.db.execute('SELECT intent,intent_sha FROM local_operation').fetchall()
     self.assertEqual(phases.rows[-1]['phase'],'applied'if boundary=='before-phase'else'committing')
     phases.query=original_phase
     helper.publish(backend(),context);helper.publish(backend(),context)
     self.assertEqual(host.engine.writes,1);self.assertEqual(events.count('native-manifest'),1)
     self.assertEqual(original,transport.db.execute('SELECT intent,intent_sha FROM local_operation').fetchall())
     self.assertEqual(phases.rows[-1]['phase'],'committed');self.assertIn('ack',driver.calls)
    finally:host.doCleanups()
