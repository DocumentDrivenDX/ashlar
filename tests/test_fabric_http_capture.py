# SPDX-License-Identifier: MIT OR Apache-2.0
"""Finite real private-pipe and retained-group supervision controls."""
from pathlib import Path
import os,sys,tempfile,time,unittest
from ashlar_host.fabric_http import _capture,_decode

class Tests(unittest.TestCase):
 def test_private_stdin_and_original_child_status(self):
  with tempfile.TemporaryDirectory()as folder:
   status,out,err=_capture([sys.executable,'-I','-S','-B','-c','import sys; data=sys.stdin.buffer.read();sys.stdout.buffer.write(data);sys.exit(7)'],b'inert pipe bytes',environment={'PATH':'/usr/bin:/bin'},cwd=Path(folder),timeout=2,maximum=1024,deadline=time.monotonic()+2)
   self.assertEqual((status,out,err),(7,b'inert pipe bytes',b''))
 def test_timeout_stops_actual_same_group_descendant(self):
  with tempfile.TemporaryDirectory()as folder:
   marker=Path(folder)/'heartbeat'
   code='import os,time\nos.fork()\nf=open('+repr(str(marker))+',"ab",buffering=0)\nwhile True: f.write(b"x");time.sleep(.025)'
   with self.assertRaises(TimeoutError):_capture([sys.executable,'-I','-S','-B','-c',code],b'',environment={'PATH':'/usr/bin:/bin'},cwd=Path(folder),timeout=.35,maximum=1024,deadline=time.monotonic()+.35)
   before=marker.read_bytes();self.assertTrue(before);time.sleep(.15);self.assertEqual(marker.read_bytes(),before)
 def test_stream_cumulative_overflow_and_closed_json(self):
  with tempfile.TemporaryDirectory()as folder:
   with self.assertRaises(ValueError):_capture([sys.executable,'-I','-S','-B','-c','import sys;sys.stdout.write("x"*40);sys.stderr.write("y"*40)'],b'',environment={'PATH':'/usr/bin:/bin'},cwd=Path(folder),timeout=2,maximum=64,deadline=time.monotonic()+2,total_maximum=64)
  for raw in (b'{"x":1,"x":2}',b'{"x":NaN}',b'{"x":1.2}'):
   with self.assertRaises(ValueError):_decode(raw)

class NamedRuntimeControls(unittest.TestCase):
 """Real owner scan on tiny substituted profile resources; no genuine SDK claim."""
 def fixture(self):
  from unittest.mock import patch
  from contextlib import ExitStack
  import hashlib
  import ashlar_host.fabric_cli_runtime as runtime
  temporary=tempfile.TemporaryDirectory();self.addCleanup(temporary.cleanup);root=Path(temporary.name).resolve();cli=root/'cli';python=root/'python';cli.mkdir();python.mkdir()
  (cli/'bin').mkdir();entry=cli/'bin/az';entry.write_bytes(b'original selected wrapper')
  executable=python/'python3.14';executable.write_bytes(b'inert selected interpreter')
  logical=cli/'python3.14';logical.symlink_to(executable)
  ca=cli/'ca.pem';ca.write_bytes(b'inert selected CA')
  config=root/'config';tmp=root/'tmp';extensions=root/'extensions'
  for p in (config,tmp,extensions):p.mkdir(mode=0o700)
  stack=ExitStack();self.addCleanup(stack.close)
  stack.enter_context(patch.object(runtime,'_ambient_netrc_paths',return_value=(root/'.netrc',root/'_netrc')))
  stack.enter_context(patch.object(runtime.sys,'platform','darwin'));stack.enter_context(patch.object(runtime.platform,'machine',return_value='arm64'))
  for name,value in [('CLI_ROOT',cli),('PYTHON_ROOT',python),('CLI_PYTHON',logical),('HTTP_PYTHON',executable),('CA_BUNDLE',ca),('NATIVE_DEPENDENCIES',()),('NATIVE_PINS',()),('PROVENANCE_PINS',((str(entry),hashlib.sha256(entry.read_bytes()).hexdigest()),(str(ca),hashlib.sha256(ca.read_bytes()).hexdigest())))]:stack.enter_context(patch.object(runtime,name,value))
  # The selected interpreter above is intentionally inert artifact data.
  # Exercise the real lower scanner under the test interpreter, not a fake
  # executable fallback in production. This supplies no genuine SDK authority.
  from ashlar_host.fabric_runtime_file_scan import scan_runtime_files
  test_interpreter=Path(sys.executable).resolve();self.file_tasks=[]
  def scan(tasks,**kwargs):
   self.file_tasks.extend(path for path,_ in tasks)
   kwargs['interpreter']=test_interpreter
   return scan_runtime_files(tasks,**kwargs)
  stack.enter_context(patch.object(runtime,'scan_runtime_files',side_effect=scan))
  selection=runtime.FabricCliRuntimeSelection('homebrew-azure-cli-2.90.0-python-3.14.8',100,1048576,2,tmp,extensions)
  return runtime,selection,config,cli,python,logical,executable,ca
 def test_named_positive_owner_full_bytes_inventory_and_link_guards(self):
  runtime,selection,config,cli,python,logical,executable,ca=self.fixture()
  owner=runtime.FabricRuntimeCustody(selection,configuration=config)
  self.assertEqual(owner.command_prefix,(str(logical),'-I','-B','-m','azure.cli'));owner.renew()
  self.assertTrue(any(row[0]=='file'and row[1]==str(executable)for row in owner.original))
  executable.write_bytes(b'changed selected interpreter')
  with self.assertRaises(PermissionError):owner.renew()
  with self.assertRaises(AttributeError):owner._anchor=owner._anchor
 def test_separate_http_profile_and_new_file_config_inventory_refuse(self):
  runtime,selection,config,cli,python,logical,executable,ca=self.fixture()
  http=runtime.FabricHttpRuntimeSelection('stdlib-python-3.14.8-fabric-http',100,1048576,2,ca)
  owner=runtime.FabricRuntimeCustody(http);owner.renew();self.assertEqual(owner.command_prefix,(str(executable),'-I','-S','-B'))
  (python/'undeclared').write_bytes(b'import addition')
  with self.assertRaises(PermissionError):owner.renew()
  original=runtime.FabricRuntimeCustody(selection,configuration=config);(config/'config').write_text('[core]\n')
  with self.assertRaises(PermissionError):original.renew()
 def test_fixed_provenance_and_remaining_deadline_are_required(self):
  runtime,selection,config,cli,python,logical,executable,ca=self.fixture()
  (cli/'bin/az').write_bytes(b'forged wrapper')
  with self.assertRaises(PermissionError):runtime.FabricRuntimeCustody(selection,configuration=config)
  with self.assertRaises(TimeoutError):runtime.FabricRuntimeCustody(selection,configuration=config,deadline=time.monotonic()-1)
 def test_positive_production_factory_uses_distinct_original_runtime_paths(self):
  # Resource substitution and inert capture prove composition only, not genuine SDK/auth.
  from unittest.mock import patch
  import base64,json
  import ashlar_host.fabric_http as http
  from ashlar_host.fabric_http_configuration import FabricHttpConfig
  runtime,selection,config,cli,python,logical,executable,ca=self.fixture()
  selected_http=runtime.FabricHttpRuntimeSelection('stdlib-python-3.14.8-fabric-http',100,1048576,2,ca)
  identity='11111111-1111-4111-8111-111111111111'
  cfg=FabricHttpConfig(identity,identity,identity,identity,'inert@example.invalid','user',cli/'bin/az',config,'2.90.0',selection,selected_http,65536,1024,4096,8,10,65536,2)
  requests=[]
  def capture(argv,payload,**kwargs):
   self.assertNotIn('inert-token',str(argv));self.assertNotIn('inert-token',str(kwargs['environment']))
   if payload:
    requests.append(json.loads(payload));self.assertEqual(argv[0],str(executable))
    return 0,json.dumps({'status':200,'raw':base64.b64encode(b'{}').decode(),'headers':{},'overflow':False}).encode(),b''
   self.assertEqual(argv[:5],[str(logical),'-I','-B','-m','azure.cli'])
   if 'get-access-token'in argv:value={'accessToken':'inert-token','tokenType':'Bearer','tenant':identity,'subscription':identity,'expires_on':int(time.time())+1000}
   else:value={'id':identity,'tenantId':identity,'user':{'name':'inert@example.invalid','type':'user'},'state':'Enabled','environmentName':'AzureCloud'}
   return 0,json.dumps(value).encode(),b''
  with patch.object(http,'CLI_ROOT',cli),patch.object(http,'HTTP_PYTHON',executable),patch.object(http,'_capture',capture):
   with http.open_fabric_http(cfg)as owner:
    self.assertEqual(owner.post('https://api.fabric.microsoft.com/v1/workspaces/'+identity+'/graphModels/'+identity+'/executeQuery?beta=true',b'{}',owner.context,2),(200,b'{}'))
  self.assertEqual(requests[0]['ca'],str(ca));self.assertTrue(requests[0]['url'].startswith('https://api.fabric.microsoft.com/'))
 def test_ambient_netrc_presence_refuses_metadata_only(self):
  from unittest.mock import patch
  runtime,selection,config,cli,python,logical,executable,ca=self.fixture()
  owner=runtime.FabricRuntimeCustody(selection,configuration=config)
  (config.parent/'.netrc').write_bytes(b'inert content must never be opened')
  with patch.object(runtime.os,'open',side_effect=AssertionError('Credential contents must not be opened'))as opened:
   with self.assertRaises(PermissionError):owner.renew()
  opened.assert_not_called()
 def test_guard_drift_still_scans_original_configuration_not_foreign(self):
  from unittest.mock import patch
  runtime,selection,config,cli,python,logical,executable,ca=self.fixture()
  (config/'config').write_text('[core]\n')
  owner=runtime.FabricRuntimeCustody(selection,configuration=config)
  foreign=config.parent/'foreign';foreign.mkdir();(foreign/'config').write_bytes(b'must not be read')
  owner.configuration=foreign;self.file_tasks.clear()
  with self.assertRaises(PermissionError):owner.renew()
  self.assertIn(config/'config',self.file_tasks);self.assertIn(executable,self.file_tasks);self.assertNotIn(foreign/'config',self.file_tasks)
 def test_ordinary_mutable_credential_cache_is_not_frozen_or_read(self):
  from unittest.mock import patch
  runtime,selection,config,cli,python,logical,executable,ca=self.fixture()
  owner=runtime.FabricRuntimeCustody(selection,configuration=config);cache=config/'msal_token_cache.bin';cache.write_bytes(b'inert mutable tool cache')
  self.file_tasks.clear();owner.renew();cache.write_bytes(b'updated ordinary tool cache');owner.renew()
  self.assertNotIn(cache,self.file_tasks)
 def test_http_closure_consumes_deadline_without_forwarding_credentials(self):
  from unittest.mock import patch
  import json
  import ashlar_host.fabric_http as http
  from ashlar_host.fabric_http_configuration import FabricHttpConfig
  runtime,selection,config,cli,python,logical,executable,ca=self.fixture()
  selected_http=runtime.FabricHttpRuntimeSelection('stdlib-python-3.14.8-fabric-http',100,1048576,2,ca)
  identity='11111111-1111-4111-8111-111111111111'
  cfg=FabricHttpConfig(identity,identity,identity,identity,'inert@example.invalid','user',cli/'bin/az',config,'2.90.0',selection,selected_http,65536,1024,4096,8,10,65536,2)
  forwarded=[]
  def capture(argv,payload,**kwargs):
   if payload:forwarded.append(payload);raise AssertionError('Elapsed deadline cannot forward token')
   if 'get-access-token'in argv:value={'accessToken':'inert-token','tokenType':'Bearer','tenant':identity,'subscription':identity,'expires_on':int(time.time())+1000}
   else:value={'id':identity,'tenantId':identity,'user':{'name':'inert@example.invalid','type':'user'},'state':'Enabled','environmentName':'AzureCloud'}
   return 0,json.dumps(value).encode(),b''
  with patch.object(http,'CLI_ROOT',cli),patch.object(http,'HTTP_PYTHON',executable),patch.object(http,'_capture',capture):
   with http.open_fabric_http(cfg)as owner:
    clock=[time.monotonic()];renew=owner._http_runtime.renew
    def consume():renew();clock[0]=owner._anchor.deadline+1
    with patch.object(owner._http_runtime,'renew',side_effect=consume),patch.object(http.time,'monotonic',side_effect=lambda:clock[0]):
     with self.assertRaises(TimeoutError):owner.post('https://api.fabric.microsoft.com/v1/workspaces/'+identity+'/graphModels/'+identity+'/executeQuery?beta=true',b'{}',owner.context,2)
    self.assertEqual(owner.observations()[-1]['failure'],'deadline');self.assertFalse(owner.observations()[-1]['submitted'])
  self.assertEqual(forwarded,[])

class AbsoluteDeadlineControls(unittest.TestCase):
 def test_spawn_setup_elapsed_deadline_never_forwards_private_payload_and_reaps(self):
  from unittest.mock import patch
  import ashlar_host.fabric_http as owner
  actual_spawn=owner.subprocess.Popen;actual_clock=time.monotonic
  with tempfile.TemporaryDirectory()as folder:
   for phase,payload in [('auth',b''),('http',b'inert private credential payload')]:
    with self.subTest(phase=phase):
     marker=Path(folder)/phase;clock=[actual_clock()];end=clock[0]+.2;handles=[]
     def spawn(*args,**kwargs):
      process=actual_spawn(*args,**kwargs);handles.append(process);clock[0]=end+1;return process
     code='import sys,pathlib; data=sys.stdin.buffer.read(); pathlib.Path('+repr(str(marker))+').write_bytes(data)'
     with patch.object(owner.subprocess,'Popen',side_effect=spawn),patch.object(owner.time,'monotonic',side_effect=lambda:clock[0]):
      with self.assertRaises(TimeoutError):owner._capture([sys.executable,'-I','-S','-B','-c',code],payload,environment={'PATH':'/usr/bin:/bin'},cwd=Path(folder),timeout=2,maximum=1024,deadline=end)
     self.assertEqual(len(handles),1);self.assertIsNotNone(handles[0].returncode);self.assertFalse(marker.exists())
 def test_elapsed_absolute_owner_deadline_refuses_before_spawn(self):
  from unittest.mock import patch
  import ashlar_host.fabric_http as owner
  with tempfile.TemporaryDirectory()as folder,patch.object(owner.subprocess,'Popen')as spawned:
   with self.assertRaises(TimeoutError):owner._capture([sys.executable,'-I','-S','-B','-c','pass'],b'inert secret',environment={'PATH':'/usr/bin:/bin'},cwd=Path(folder),timeout=2,maximum=1024,deadline=time.monotonic()-1)
   spawned.assert_not_called()
 def test_select_consumes_deadline_before_private_write(self):
  from unittest.mock import patch
  import ashlar_host.fabric_http as owner
  actual_spawn=owner.subprocess.Popen;factory=owner.selectors.DefaultSelector;clock=[time.monotonic()];end=clock[0]+.3;handles=[]
  def spawn(*args,**kwargs):
   process=actual_spawn(*args,**kwargs);handles.append(process);return process
  def selected():
   selector=factory();original=selector.select
   def wait(timeout):
    events=original(timeout);clock[0]=end+1;return events
   selector.select=wait;return selector
  with tempfile.TemporaryDirectory()as folder:
   marker=Path(folder)/'private-payload'
   code='import sys,pathlib; pathlib.Path('+repr(str(marker))+').write_bytes(sys.stdin.buffer.read())'
   with patch.object(owner.subprocess,'Popen',side_effect=spawn),patch.object(owner.selectors,'DefaultSelector',side_effect=selected),patch.object(owner.time,'monotonic',side_effect=lambda:clock[0]):
    with self.assertRaises(TimeoutError):owner._capture([sys.executable,'-I','-S','-B','-c',code],b'inert private token',environment={'PATH':'/usr/bin:/bin'},cwd=Path(folder),timeout=2,maximum=1024,deadline=end)
   self.assertEqual(len(handles),1);self.assertIsNotNone(handles[0].returncode);self.assertFalse(marker.exists())
