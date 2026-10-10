from pathlib import Path
import hashlib, importlib.util, io, json, os, sys, time
from unittest.mock import patch
ROOT=Path('/private/tmp/ashlar-otel-worker-source-freeze-20261010-a')
REPO=Path('/Users/erik/Projects/ashlar')
sys.path.insert(0,str(REPO/'src'))
spec=importlib.util.spec_from_file_location('ashlar_host._otel_worker',ROOT/'src/ashlar_host/_otel_worker.py')
w=importlib.util.module_from_spec(spec);sys.modules[spec.name]=w;spec.loader.exec_module(w)
tspec=importlib.util.spec_from_file_location('frozen_worker_tests',ROOT/'tests/test_otel_worker.py')
t=importlib.util.module_from_spec(tspec);tspec.loader.exec_module(t)
results=[]
class Response:
 status=200
 def getheader(self,name,default=None):return default
 def read1(self,n):return b''
 def close(self):pass
class Connection:
 sock=None
 def __init__(self):self.path=None
 def request(self,method,path,**kwargs):self.path=path
 def getresponse(self):return Response()
 def close(self):pass
for signal in ('logs','spans','metrics'):
 c=Connection()
 with patch.object(w.http.client,'HTTPConnection',return_value=c):
  status=w.HTTPTransport(w.Settings.from_wire(t.settings_wire()))(signal,b'',time.monotonic()+1)
 results.append({'control':'signal_route','signal':signal,'actual_path':c.path,'status':status})
# Real stdlib HTTPResponse parser over an inert file-like socket: no DNS/socket/network.
clock=[0.0];lines=[]
class DripFile(io.BytesIO):
 def readline(self,*args):
  clock[0]+=.04;lines.append(clock[0]);return super().readline(*args)
class InertSocket:
 def makefile(self,*args):return DripFile(b'HTTP/1.1 200 OK\r\nX-A: a\r\nX-B: b\r\nContent-Length: 0\r\n\r\n')
 def settimeout(self,n):pass
class SlowHeaders(Connection):
 sock=InertSocket()
 def getresponse(self):
  self.response=w.http.client.HTTPResponse(self.sock);self.response.begin();return self.response
c=SlowHeaders()
with patch.object(w.time,'monotonic',side_effect=lambda:clock[0]),patch.object(w.http.client,'HTTPConnection',return_value=c):
 try:w.HTTPTransport(w.Settings.from_wire(t.settings_wire()))('logs',b'',1.0)
 except w.WorkerError:pass
 else:raise AssertionError('Expected timeout refusal after parser eventually returned')
results.append({'control':'whole_request_header_deadline','configured_seconds':.1,'actual_inert_clock_seconds':clock[0],'header_read_instants':lines,'dns_or_network':False,'result':'overrun detected only after getresponse returns'})
# The generic cleanup helper is the helper used by SDKWorker.close.
for canceltype in (KeyboardInterrupt,SystemExit,GeneratorExit):
 ordinary=OSError('fixture-only');cancel=canceltype();attempted=[]
 def first():attempted.append('first');raise ordinary
 def second():attempted.append('second');raise cancel
 def third():attempted.append('third')
 try:w.finish(None,(first,second,third))
 except BaseException as caught:
  results.append({'control':'cleanup_cancellation_after_ordinary_failure','cancellation_type':canceltype.__name__,'raised_type':type(caught).__name__,'preserved_cancellation_identity':caught is cancel,'first_ordinary_identity':caught is ordinary,'attempted':attempted})
# Baseline owner suite on precisely frozen module/test bytes.
import unittest
suite=unittest.defaultTestLoader.loadTestsFromModule(t)
out=io.StringIO();r=unittest.TextTestRunner(stream=out,verbosity=1).run(suite)
print(out.getvalue())
assert r.wasSuccessful()
receipt={'scope':'Frozen A source; inert HTTP/file/clock ports and real SDK owner tests only; no receiver/network/native/secret use','source':{'path':str(ROOT/'src/ashlar_host/_otel_worker.py'),'sha256':hashlib.sha256((ROOT/'src/ashlar_host/_otel_worker.py').read_bytes()).hexdigest()},'controls':results,'ownerTests':{'run':r.testsRun,'failures':len(r.failures),'errors':len(r.errors),'skipped':len(r.skipped)}}
p=Path('/private/tmp/astra-otel-worker-transport-controls-20261010-a.json');p.write_text(json.dumps(receipt,indent=2)+'\n')
print(json.dumps(receipt,indent=2))
