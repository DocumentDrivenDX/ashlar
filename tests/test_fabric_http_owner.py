# SPDX-License-Identifier: MIT OR Apache-2.0
"""Actual tiny inert credential processes and loopback socket controls only."""
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
import json,os,sys,tempfile,threading,time,unittest
from dataclasses import replace
from ashlar_host.fabric_http_configuration import FabricHttpConfig
from ashlar_host.fabric_cli_runtime import FabricInertCliRuntimeSelection,FabricLoopbackRuntimeSelection
from ashlar_host.fabric_http import FabricLoopbackTestConfig,open_fabric_http

ID='11111111-1111-4111-8111-111111111111'
TOKEN='inert-private-token'

class Tests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name).resolve();self.install=self.root/'install';self.install.mkdir();self.configdir=self.root/'config';self.configdir.mkdir()
  self.cli=self.install/'az';self.cli.write_text('#!'+str(Path(sys.executable).resolve())+'\nimport sys,json,time\nif "get-access-token" in sys.argv: v={"accessToken":"'+TOKEN+'","tokenType":"Bearer","tenant":"'+ID+'","subscription":"'+ID+'","expires_on":int(time.time())+3600}\nelse: v={"id":"'+ID+'","tenantId":"'+ID+'","user":{"name":"inert@example.invalid","type":"user"},"state":"Enabled","environmentName":"AzureCloud"}\nprint(json.dumps(v))\n');self.cli.chmod(0o700)
  self.responses=[];self.requests=[]
  owner=self
  class Handler(BaseHTTPRequestHandler):
   def log_message(self,*args):pass
   def do_POST(self):self.handle_query()
   def do_GET(self):self.handle_query()
   def handle_query(self):
    raw=self.rfile.read(int(self.headers.get('Content-Length','0')));owner.requests.append((self.command,self.path,raw,self.headers.get('Authorization')))
    status,headers,body,delay=owner.responses.pop(0)
    if delay:time.sleep(delay)
    try:
     self.send_response(status)
     for k,v in headers.items():self.send_header(k,v)
     self.end_headers();self.wfile.write(body)
    except (BrokenPipeError,ConnectionResetError):pass
  self.server=ThreadingHTTPServer(('127.0.0.1',0),Handler);self.server.daemon_threads=True
  self.thread=threading.Thread(target=self.server.serve_forever,daemon=True);self.thread.start();self.addCleanup(self.close_server)
  cli=FabricInertCliRuntimeSelection(self.install,self.cli,(('PATH','/usr/bin:/bin'),));http=FabricLoopbackRuntimeSelection(Path(sys.executable).resolve(),self.server.server_port)
  cfg=FabricHttpConfig(ID,ID,ID,ID,'inert@example.invalid','user',self.cli,self.configdir,'2.90.0',cli,http,65536,1024,4096,8,5,65536,2)
  self.selection=FabricLoopbackTestConfig(cfg,cli,http);self.url='http://127.0.0.1:'+str(http.port)+'/v1/workspaces/'+ID+'/graphModels/'+ID+'/executeQuery?beta=true'
 def close_server(self):self.server.shutdown();self.server.server_close();self.thread.join(1)
 def test_actual_private_credentials_and_exact_socket_request(self):
  self.responses=[(200,{},b'{"answer":"exact"}',0)]
  with open_fabric_http(self.selection)as owner:
   context=owner.context
   self.assertEqual(owner.post(self.url,b'{ "query" : "MATCH" }',context,2),(200,b'{"answer":"exact"}'))
   self.assertEqual(owner.observations(),({'route':'executeQuery','method':'POST','status':200,'bytes':18,'complete':True,'submitted':True,'failure':None},))
   with self.assertRaises(PermissionError):owner.post(self.url,b'{}',object(),1)
  self.assertEqual(self.requests[0][2],b'{ "query" : "MATCH" }');self.assertEqual(self.requests[0][3],'Bearer '+TOKEN)
  with self.assertRaises(PermissionError):owner.post(self.url,b'{}',context,1)
 def test_redirect_and_secret_echo_are_withheld(self):
  for status,headers,body in [(302,{'Location':self.url},b''),(200,{},TOKEN.encode())]:
   self.responses=[(status,headers,body,0)];before=len(self.requests)
   with open_fabric_http(self.selection)as owner:
    with self.assertRaises(PermissionError):owner.post(self.url,b'{}',owner.context,1)
    self.assertNotIn(TOKEN,str(owner.observations()))
   self.assertEqual(len(self.requests),before+1)
 def test_oversize_body_and_parent_deadline_refuse(self):
  self.responses=[(200,{},b'x'*1100,0)]
  with open_fabric_http(self.selection)as owner:
   with self.assertRaises(ValueError):owner.post(self.url,b'{}',owner.context,1)
   self.assertFalse(owner.observations()[0]['complete'])
   with self.assertRaises(ValueError):owner.post(self.url,b'x'*65537,owner.context,1)
  self.responses=[(200,{},b'late',.6)]
  with open_fabric_http(self.selection)as owner:
   start=time.monotonic()
   with self.assertRaises((TimeoutError,ValueError)):owner.post(self.url,b'{}',owner.context,.15)
   self.assertLess(time.monotonic()-start,1)
 def test_definition_operation_routes_are_derived_from_actual_response(self):
  base='http://127.0.0.1:'+str(self.server.server_port);state=base+'/v1/operations/'+ID
  self.responses=[(202,{'Location':state,'x-ms-operation-id':ID,'Retry-After':'0'},b'',0),(200,{},b'{"status":"Succeeded"}',0),(200,{},b'{"definition":"original"}',0)]
  with open_fabric_http(self.selection)as owner:self.assertEqual(owner.get_definition(),b'{"definition":"original"}')
  self.assertEqual([row[0]for row in self.requests],['POST','GET','GET'])
 def test_foreign_operation_and_unbounded_inputs_refuse_before_submission(self):
  self.responses=[(202,{'Location':'http://example.invalid/v1/operations/'+ID,'x-ms-operation-id':ID},b'',0)]
  with open_fabric_http(self.selection)as owner:
   with self.assertRaises(PermissionError):owner.get_definition()
   count=len(self.requests)
   for url,timeout in [(self.url+'&continuationToken=x&foreign=y',1),(self.url,float('nan')),(self.url.replace('127.0.0.1','localhost'),1)]:
    with self.assertRaises((PermissionError,ValueError)):owner.post(url,b'{}',owner.context,timeout)
   self.assertEqual(len(self.requests),count)
 def test_token_anchor_and_capacity_settings_are_not_caller_grants(self):
  for name,value in [('maximum_request_bytes',True),('maximum_requests',0),('account_user','bad\nname'),('azure_cli_version','latest')]:
   with self.assertRaises(ValueError):replace(self.selection.config,**{name:value})
  with self.assertRaises(PermissionError):
   with open_fabric_http(self.selection)as owner:
    with self.assertRaises(AttributeError):owner._anchor=owner._anchor
    owner._token='foreign';owner._original_token='foreign'
    with self.assertRaises(PermissionError):owner.post(self.url,b'{}',owner.context,1)
 def test_live_profile_and_mutated_runtime_refuse(self):
  with self.assertRaises(PermissionError):
   with open_fabric_http(self.selection.config):self.fail('live auth')
  self.responses=[(200,{},b'{}',0)]
  with self.assertRaises(PermissionError):
   with open_fabric_http(self.selection):self.cli.write_text(self.cli.read_text()+'\n# changed\n')

 def test_same_config_object_mutation_refuses_and_closes_original_account(self):
  from unittest.mock import patch
  import ashlar_host.fabric_http as module
  original=self.selection.config.account_id;foreign='22222222-2222-4222-8222-222222222222'
  capture=module._capture
  with patch.object(module,'_capture',wraps=capture)as calls:
   with self.assertRaises(PermissionError):
    with open_fabric_http(self.selection)as owner:
     object.__setattr__(self.selection.config,'account_id',foreign)
     owner.post(self.url,b'{}',owner.context,1)
  self.assertEqual(len(self.requests),0)
  self.assertIn(original,calls.call_args.args[0]);self.assertNotIn(foreign,calls.call_args.args[0])
 def test_counters_cannot_reset_or_borrow_and_worker_failure_is_diagnostic(self):
  from unittest.mock import patch
  import ashlar_host.fabric_http as module
  self.responses=[(200,{},b'{}',0)]
  with open_fabric_http(self.selection)as owner:
   owner.post(self.url,b'{}',owner.context,1)
   for name in ('_requests','_bytes','_accounting'):
    with self.assertRaises(AttributeError):setattr(owner,name,0)
   with self.assertRaises(PermissionError):owner._update_accounting(requests=0)
   with patch.object(module,'_capture',side_effect=TimeoutError('inert timeout')):
    with self.assertRaises(TimeoutError):owner.post(self.url,b'{}',owner.context,1)
   row=owner.observations()[-1]
   self.assertEqual((row['route'],row['failure'],row['status'],row['submitted']),('executeQuery','deadline',None,True))
   self.assertNotIn(TOKEN,str(owner.observations()))
 def test_conflicting_operation_state_and_result_identity_are_refused(self):
  base='http://127.0.0.1:'+str(self.server.server_port);state=base+'/v1/operations/'+ID;foreign='22222222-2222-4222-8222-222222222222'
  for phase,headers in [('state',{'x-ms-operation-id':foreign}),('result',{'x-ms-operation-id':foreign}),('result',{'Location':base+'/v1/operations/'+foreign+'/result'})]:
   self.responses=[(202,{'Location':state,'x-ms-operation-id':ID,'Retry-After':'0'},b'',0),(200,headers if phase=='state'else{},b'{"status":"Succeeded"}',0)]
   if phase=='result':self.responses.append((200,headers,b'{"definition":"original"}',0))
   with open_fabric_http(self.selection)as owner:
    with self.assertRaises(PermissionError):owner.get_definition()
    self.assertEqual(owner.observations()[-1]['failure'],'operation-refusal')

 def test_accounting_cannot_be_borrowed_from_another_context(self):
  with open_fabric_http(self.selection)as first,open_fabric_http(self.selection)as second:
   original=first._accounting
   object.__setattr__(first,'_accounting',second._accounting)
   try:
    with self.assertRaises(PermissionError):first.post(self.url,b'{}',first.context,1)
    self.assertEqual(len(self.requests),0)
   finally:object.__setattr__(first,'_accounting',original)
