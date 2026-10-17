"""Existing publication/ACK orchestration with explicit prospective manifest owner."""
import json,unittest
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import patch
from ashlar.attempt_store import DeltaAttemptStore
import test_stored_publisher as fixture
from ashlar_host.original_medical_local_publication import _MedicalReconciliationBackend

class MedicalProspectiveCompositionTests(unittest.TestCase):
 def test_original_committing_then_committed_replay_reuses_only_new_manifest_owner(self):
  helper=fixture.StoredPublisherTests();context=object()
  with helper.fixture()as(phases,driver,policy,remote,api):
   old=helper.backend(phases,driver,remote,policy);driver.fail='validate'
   with self.assertRaises(PermissionError):helper.publish(old,context)
   with old.writer('stream',context):
    attempt=old.observe('stream',helper.batch().batch_id)
    old.start_commit('stream',helper.batch().batch_id,attempt.request_digest)
   original=json.loads(phases.rows[-1]['payload_json']);driver.fail=None;calls=[]
   def reconcile(row,*,context):calls.append(dict(row));return dict(row)
   backend=_MedicalReconciliationBackend(DeltaAttemptStore(phases,policy,'c.s.attempt','attempt-uuid'),driver,
    lambda request,context:SimpleNamespace(reconcile=reconcile),context=context,request=original['request'])
   first=helper.publish(backend,context);second=helper.publish(backend,context)
   self.assertEqual(first,second);self.assertEqual(len(calls),2)
   self.assertEqual(driver.calls.count('apply'),1);self.assertNotIn('recover-apply',driver.calls)
   self.assertEqual(driver.calls.count('ack'),2);self.assertEqual(phases.rows[-1]['phase'],'committed')
   self.assertEqual(len(phases.rows),5)
 def test_public_cli_dispatches_explicit_prospective_command(self):
  from ashlar.cli import main
  with patch('sys.argv',['ashlar','reconcile-original-medical-commit','--configuration','/new-operation.json']),patch('ashlar_host.original_medical_local_cli.run_original_medical_commit_reconciliation_command',return_value={})as run,patch('builtins.print'):
   main()
  run.assert_called_once_with(Path('/new-operation.json'))
 def test_fresh_attempt_cannot_start_effects_or_new_manifest(self):
  helper=fixture.StoredPublisherTests();context=object()
  with helper.fixture()as(phases,driver,policy,remote,api):
   backend=_MedicalReconciliationBackend(DeltaAttemptStore(phases,policy,'c.s.attempt','attempt-uuid'),driver,
    lambda *args:None,context=context,request={})
   with self.assertRaisesRegex(Exception,'Original committing or committed'):helper.publish(backend,context)
   self.assertEqual(driver.calls,[]);self.assertEqual(phases.rows,[])
