"""Basic wrapper and dispatch ports, without engine execution."""
import io,unittest
from contextlib import nullcontext,redirect_stdout
from types import SimpleNamespace
from unittest.mock import patch
from ashlar_host.truss_scalar_join_count_cli import run_query_truss_scalar_join_count_command

class Tests(unittest.TestCase):
 def test_restore_and_preserve_original_protocol(self):
  driver=SimpleNamespace(writer=lambda *args:nullcontext(),context=object(),source_sessions=SimpleNamespace(batch=lambda:SimpleNamespace(feed='feed')))
  def owner(config,output,operation):return operation(driver,{'request_digest':'digest'})
  answer={'rows':[{'label':'Alpha'}],'request_bytes':b'{"original":1}\n','response_bytes':b'{"compiled":1}\n'}
  with patch('ashlar_host.truss_scalar_join_count_cli.run_selected_truss_command',side_effect=owner),patch('ashlar_host.truss_scalar_join_count_cli.restore_applied_truss_publication')as restore,patch('ashlar_host.truss_scalar_join_count_cli.execute_truss_scalar_join_count_query',return_value=answer)as execute:
   result=run_query_truss_scalar_join_count_command('/config','/out',compiler='/compiler',sql='SELECT label FROM A',revision='2',document_id='cohort',physical_revisions=('1','2'))
  restore.assert_called_once_with(driver,'digest');self.assertEqual(execute.call_args.kwargs['physical_revisions'],('1','2'));self.assertEqual(result['request_original_json'],'{"original":1}\n');self.assertEqual(result['response_original_json'],'{"compiled":1}\n')
 def test_dispatch_repeated_revisions(self):
  from ashlar.cli import main
  argv=['ashlar','query-truss-scalar-join-count','--configuration','/config','--output','/out','--compiler','/compiler','--sql','SELECT label FROM A','--revision','2','--document-id','cohort','--physical-revision','1','--physical-revision','2']
  with patch('sys.argv',argv),patch('ashlar_host.truss_scalar_join_count_cli.run_query_truss_scalar_join_count_command')as command,redirect_stdout(io.StringIO()):main()
  self.assertEqual(command.call_args.kwargs['physical_revisions'],('1','2'))
