"""Basic owned wrapper/dispatch ports; no native execution."""
import io,unittest
from contextlib import nullcontext,redirect_stdout
from types import SimpleNamespace
from unittest.mock import patch
from ashlar_host.truss_two_hop_count_cli import run_query_truss_two_hop_count_command

class Tests(unittest.TestCase):
 def test_restore_and_preserve_all_original_protocol_bytes(self):
  driver=SimpleNamespace(writer=lambda *args:nullcontext(),context=object(),source_sessions=SimpleNamespace(batch=lambda:SimpleNamespace(feed='feed')))
  def owner(config,output,operation):return operation(driver,{'request_digest':'digest'})
  answer={'rows':[{'paths':2}],'request_bytes':b'{"original":1}\n','response_bytes':b'{"compiled":1}\n','recompiled_bytes':b'{"compiled":1}\n'}
  with patch('ashlar_host.truss_two_hop_count_cli.run_selected_truss_command',side_effect=owner),patch('ashlar_host.truss_two_hop_count_cli.restore_applied_truss_publication')as restore,patch('ashlar_host.truss_two_hop_count_cli.execute_truss_two_hop_count_query',return_value=answer)as execute:
   result=run_query_truss_two_hop_count_command('/config','/out',index='/index',installation='/installation',sql='SELECT COUNT(*) AS paths FROM TypedRecord r CROSS JOIN EXPAND_PATHS(r.related,related) AS p',revision='1',document_id='typed-query-record')
  restore.assert_called_once_with(driver,'digest');self.assertEqual(result['request_original_json'],'{"original":1}\n');self.assertEqual(result['response_original_json'],'{"compiled":1}\n');self.assertEqual(result['recompiled_original_json'],'{"compiled":1}\n');self.assertNotIn('recompiled_bytes',result)
 def test_dispatch_exact_installed_profile_options(self):
  from ashlar.cli import main
  argv=['ashlar','query-truss-two-hop-count','--configuration','/config','--output','/out','--weft-index','/index','--weft-installation','/installation','--sql','SELECT COUNT(*) AS paths FROM TypedRecord r CROSS JOIN EXPAND_PATHS(r.related,related) AS p','--revision','1','--document-id','typed-query-record']
  with patch('sys.argv',argv),patch('ashlar_host.truss_two_hop_count_cli.run_query_truss_two_hop_count_command')as command,redirect_stdout(io.StringIO()):main()
  self.assertEqual(command.call_args.kwargs['index'],'/index');self.assertEqual(command.call_args.kwargs['installation'],'/installation')
