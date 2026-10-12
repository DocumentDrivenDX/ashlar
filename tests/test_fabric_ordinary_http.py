import contextlib,io,json,tempfile,time,unittest
from pathlib import Path
from unittest.mock import patch
from ashlar_host.fabric_ordinary_http import FabricOrdinaryHttpConfig,open_fabric_ordinary_http
class Tests(unittest.TestCase):
 def fixture(self,tmp):
  identifier='11111111-1111-1111-1111-111111111111'
  cfg=FabricOrdinaryHttpConfig(Path('/usr/bin/true'),Path(tmp).resolve(),identifier,identifier,'user@example.com',identifier,identifier)
  account={'id':identifier,'tenantId':identifier,'user':{'name':cfg.account_user,'type':'user'},'state':'Enabled','environmentName':'AzureCloud'}
  token={'tenant':identifier,'subscription':identifier,'tokenType':'Bearer','accessToken':'private-token','expires_on':str(int(time.time())+3600)}
  calls=[]
  def capture(argv,**kwargs):
   calls.append(argv);return json.dumps(token if 'get-access-token'in argv else account).encode(),b''
  return cfg,capture,calls,account
 def test_authenticated_post_and_closing_account(self):
  with tempfile.TemporaryDirectory()as tmp:
   cfg,capture,calls,_=self.fixture(tmp);response=io.BytesIO(b'{"ok":true}');response.code=200;response.headers={}
   class Opener:
    def open(self,request,timeout):
     self.request=request;return response
   opener=Opener()
   with patch('ashlar_host.fabric_ordinary_http.capture',capture),patch('urllib.request.build_opener',return_value=opener):
    with open_fabric_ordinary_http(cfg)as transport:
     endpoint='https://api.fabric.microsoft.com/v1/workspaces/'+cfg.workspace_id+'/graphModels/'+cfg.graph_model_id+'/executeQuery?beta=true'
     self.assertEqual(transport.post(endpoint,b'{"query":"MATCH (n) RETURN n"}',transport.context,1),(200,b'{"ok":true}'))
     self.assertEqual(opener.request.get_header('Authorization'),'Bearer private-token')
    self.assertTrue(response.closed);self.assertTrue(transport._closed);self.assertEqual(len(calls),3)
 def test_wrong_account_refuses_before_http(self):
  with tempfile.TemporaryDirectory()as tmp:
   cfg,capture,calls,account=self.fixture(tmp);account['tenantId']='22222222-2222-2222-2222-222222222222'
   with patch('ashlar_host.fabric_ordinary_http.capture',capture),patch('urllib.request.build_opener')as opener:
    with self.assertRaises(PermissionError):
     with open_fabric_ordinary_http(cfg):self.fail('opened')
    opener.assert_not_called()
 def test_response_overflow_closes_response_and_account(self):
  with tempfile.TemporaryDirectory()as tmp:
   cfg,capture,calls,_=self.fixture(tmp);object.__setattr__(cfg,'maximum_response_bytes',2)
   response=io.BytesIO(b'oversize');response.code=200;response.headers={}
   with patch('ashlar_host.fabric_ordinary_http.capture',capture),patch('urllib.request.OpenerDirector.open',return_value=response):
    with self.assertRaises(ValueError):
     with open_fabric_ordinary_http(cfg)as transport:
      endpoint='https://api.fabric.microsoft.com/v1/workspaces/'+cfg.workspace_id+'/graphModels/'+cfg.graph_model_id+'/executeQuery?beta=true'
      transport.post(endpoint,b'{}',transport.context,1)
    self.assertTrue(response.closed);self.assertTrue(transport._closed);self.assertEqual(len(calls),3)
    self.assertEqual(transport.observations(),((200,b'ov',False),))
