"""Basic command lifecycle using inert public HTTP/policy ports, no cloud I/O."""
from contextlib import contextmanager,redirect_stdout,redirect_stderr
from pathlib import Path
from types import SimpleNamespace
import base64,hashlib,io,json,tempfile,unittest
from unittest.mock import patch
from ashlar import cli
from ashlar_host.fabric_gql_cli import FabricGqlSession,run_fabric_gql_command

EVENTS=[];FAIL_CLOSE=False
RAW=json.dumps({'status':{'code':'00000','cause':None},'result':{'kind':'TABLE','columns':[{'name':'count','gqlType':'INT64'}],'data':[{'count':'3'}]}}).encode()
@contextmanager
def open_session(invocation):
 context=object();EVENTS.append('open')
 class Policy:
  @contextmanager
  def hold(self,binding,supplied):
   assert supplied is context and binding==invocation.binding;EVENTS.append('hold')
   try:yield None
   finally:EVENTS.append('hold-close')
  def admit(self,binding,query,supplied):
   assert supplied is context and binding==invocation.binding and query==invocation.query;EVENTS.append('admit')
 class Http:
  def post(self,url,body,supplied,timeout):
   assert supplied is context and json.loads(body)['query']==invocation.query;EVENTS.append('post');return 200,RAW
 try:yield FabricGqlSession(invocation.binding,Http(),Policy(),context)
 finally:
  EVENTS.append('provider-close')
  if FAIL_CLOSE:raise LookupError('inert close failure')

class Tests(unittest.TestCase):
 def setUp(self):
  global FAIL_CLOSE
  EVENTS.clear();FAIL_CLOSE=False
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name).resolve()
  self.config=self.root/'query.json';self.provider=self.root/'provider.py'
  self.value={'profile':'ashlar-fabric-gql-invocation/0.1','binding':{'workspace_id':'11111111-1111-1111-1111-111111111111','graph_model_id':'22222222-2222-2222-2222-222222222222','release_sha256':'a'*64},'query':'MATCH (n) RETURN count(n) AS count','limits':{'maximum_response_bytes':1048576,'maximum_total_bytes':8388608,'maximum_rows':1000,'maximum_requests':32,'deadline_seconds':60}}
  self.config.write_text(json.dumps(self.value));self.provider.write_text('from test_fabric_gql_cli import open_session as open_fabric_gql\n');self.digest=hashlib.sha256(self.provider.read_bytes()).hexdigest()
 def test_actual_scalar_executor_and_provider_close_before_result(self):
  result=run_fabric_gql_command(str(self.config),str(self.provider),self.digest)
  self.assertEqual(result['rows'],[[3]]);self.assertEqual(base64.b64decode(result['responses_base64'][0]),RAW)
  self.assertEqual(EVENTS[-1],'provider-close');self.assertIn('hold-close',EVENTS)
 def test_provider_close_failure_withholds_cli_stdout(self):
  global FAIL_CLOSE
  FAIL_CLOSE=True;output=io.StringIO();error=io.StringIO()
  with patch('sys.argv',['ashlar','query-fabric-gql','--configuration',str(self.config),'--provider',str(self.provider),'--provider-sha256',self.digest]),redirect_stdout(output),redirect_stderr(error),self.assertRaises(SystemExit)as caught:cli.main()
  self.assertEqual(caught.exception.code,2);self.assertEqual(output.getvalue(),'');self.assertIn('operation refused',error.getvalue());self.assertEqual(EVENTS[-1],'provider-close')
 def test_invalid_configuration_and_wrong_provider_digest_no_acquisition(self):
  with self.assertRaises(ValueError):run_fabric_gql_command(str(self.config),str(self.provider),'b'*64)
  self.value['limits']['maximum_rows']=True;self.config.write_text(json.dumps(self.value))
  with self.assertRaises(ValueError):run_fabric_gql_command(str(self.config),str(self.provider),self.digest)
  self.assertEqual(EVENTS,[])
