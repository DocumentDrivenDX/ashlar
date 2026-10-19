# SPDX-License-Identifier: MIT OR Apache-2.0
"""The existing phase owner delegates only the explicit prospective store port."""
import json,unittest
from types import SimpleNamespace
from ashlar.publisher import PublicationError
import test_stored_publisher as fixture

class ProspectiveStoredPublisherTests(unittest.TestCase):
 def test_explicit_reconciliation_reuses_original_committing_and_ack(self):
  helper=fixture.StoredPublisherTests();context=object()
  with helper.fixture()as(phases,driver,policy,remote,api):
   backend=helper.backend(phases,driver,remote,policy);driver.fail='validate'
   with self.assertRaises(PermissionError):helper.publish(backend,context)
   with backend.writer('stream',context):
    last=backend.observe('stream',helper.batch().batch_id)
    backend.start_commit('stream',helper.batch().batch_id,last.request_digest)
   original=json.loads(phases.rows[-1]['payload_json']);events=[];driver.fail=None
   def reconcile(row,*,context):
    events.append('prospective-native-commit');return row
   backend.manifest_factory=lambda request,context:SimpleNamespace(reconcile=reconcile)
   with backend.writer('stream',context):
    with self.assertRaises(PublicationError):backend.reconcile_commit('stream',original['request'],original['result_json'],object())
    attempt=backend.reconcile_commit('stream',original['request'],original['result_json'],context)
    self.assertEqual(attempt.phase,'committed')
    backend.acknowledge('stream',original['request'],attempt.descriptor,context)
   self.assertEqual(events,['prospective-native-commit']);self.assertEqual(driver.calls[-1],'ack')
   self.assertEqual([row['phase']for row in phases.rows],['prepared','applying','applied','committing','committed'])
 def test_prospective_failure_cannot_append_committed_or_ack(self):
  helper=fixture.StoredPublisherTests();context=object()
  with helper.fixture()as(phases,driver,policy,remote,api):
   backend=helper.backend(phases,driver,remote,policy);driver.fail='validate'
   with self.assertRaises(PermissionError):helper.publish(backend,context)
   with backend.writer('stream',context):
    last=backend.observe('stream',helper.batch().batch_id);backend.start_commit('stream',helper.batch().batch_id,last.request_digest)
   original=json.loads(phases.rows[-1]['payload_json']);driver.fail=None
   def refuse(*args,**kwargs):raise PermissionError('Wrong prospective authorization')
   backend.manifest_factory=lambda request,context:SimpleNamespace(reconcile=refuse)
   with backend.writer('stream',context):
    with self.assertRaises(PermissionError):backend.reconcile_commit('stream',original['request'],original['result_json'],context)
   self.assertEqual(phases.rows[-1]['phase'],'committing');self.assertNotIn('ack',driver.calls)
