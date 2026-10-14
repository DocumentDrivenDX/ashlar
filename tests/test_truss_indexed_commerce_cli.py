"""Pack custody across outer cleanup; native execution is mocked."""
import tempfile,unittest
from pathlib import Path
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import patch
import ashlar_host.truss_indexed_commerce_cli as command

ROOT=Path(__file__).resolve().parents[1]

class Tests(unittest.TestCase):
 def driver(self):
  return SimpleNamespace(writer=lambda *args:nullcontext(),context=object(),source_sessions=SimpleNamespace(batch=lambda:SimpleNamespace(feed='feed')))
 def answer(self):
  return {'publication_id':'original','rows':[['PAY1']],'request_bytes':b'{"original":"request"}\n','response_bytes':b'{"original":"response"}\n','recompiled_bytes':b'{"original":"response"}\n'}
 def run_command(self,output):
  pack=output.parent/'pack.json';pack.write_bytes((ROOT/'examples/domain-packs/commerce/upstream/pack.json').read_bytes());pack.chmod(0o600)
  return command.run_query_truss_commerce_command('/configuration',output,index='/index',installation='/installation',pack_path=pack,case_id='settlement',revision='2',document_id='urn:umf:domain:commerce',maximum_rows=1)
 def test_pack_renewal_follows_outer_cleanup_before_private_report(self):
  events=[];renew=command.InvocationFile.renew
  def renewal(lease,maximum):events.append('pack-renew');return renew(lease,maximum)
  with tempfile.TemporaryDirectory()as directory:
   output=Path(directory).resolve()/'report.json'
   def owner(configuration,destination,operation,*,report_writer):
    result=operation(self.driver(),{'request_digest':'original-digest'});self.assertFalse(output.exists())
    events.append('outer-cleanup');return report_writer(result,destination)
   with patch.object(command.InvocationFile,'renew',autospec=True,side_effect=renewal),patch.object(command,'run_selected_truss_command',side_effect=owner),patch.object(command,'restore_applied_truss_publication')as restore,patch.object(command,'execute_truss_indexed_commerce_query',return_value=self.answer()):
    result=self.run_command(output)
   self.assertEqual(events,['pack-renew','pack-renew','outer-cleanup','pack-renew'])
   self.assertTrue(output.is_file());self.assertEqual(result['request_original_json'],'{"original":"request"}\n');self.assertEqual(result['recompiled_original_json'],'{"original":"response"}\n');restore.assert_called_once()
 def test_primary_failure_survives_failed_closing_pack_renewal(self):
  events=[];primary=RuntimeError('primary outer cleanup failure');secondary=ValueError('closing pack custody changed');renew=command.InvocationFile.renew
  def renewal(lease,maximum):
   events.append('pack-renew')
   if 'outer-cleanup'in events:raise secondary
   return renew(lease,maximum)
  with tempfile.TemporaryDirectory()as directory:
   output=Path(directory).resolve()/'report.json'
   def owner(configuration,destination,operation,*,report_writer):
    operation(self.driver(),{'request_digest':'original-digest'});events.append('outer-cleanup');raise primary
   with patch.object(command.InvocationFile,'renew',autospec=True,side_effect=renewal),patch.object(command,'run_selected_truss_command',side_effect=owner),patch.object(command,'restore_applied_truss_publication'),patch.object(command,'execute_truss_indexed_commerce_query',return_value=self.answer()):
    with self.assertRaises(RuntimeError)as captured:self.run_command(output)
   self.assertIs(captured.exception,primary);self.assertTrue(primary.cleanup_failed);self.assertFalse(output.exists());self.assertEqual(events,['pack-renew','pack-renew','outer-cleanup','pack-renew'])
