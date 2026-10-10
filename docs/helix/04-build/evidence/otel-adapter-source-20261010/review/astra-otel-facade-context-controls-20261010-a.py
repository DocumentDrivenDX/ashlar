from pathlib import Path
import hashlib,importlib.util,json,os,sys,time
from unittest.mock import patch
R=Path('/Users/erik/Projects/ashlar');sys.path.insert(0,str(R/'src'))
source=R/'src/ashlar_host/otel.py';raw=source.read_bytes();snapshot=Path('/private/tmp/astra-otel-facade-context-controls-20261010-a.source.py');snapshot.write_bytes(raw)
spec=importlib.util.spec_from_file_location('ashlar_host.otel_review',snapshot);facade=importlib.util.module_from_spec(spec);spec.loader.exec_module(facade)
W=Path('/private/tmp/ashlar-otel-worker-source-freeze-20261010-a')
spec=importlib.util.spec_from_file_location('ashlar_host._otel_worker_review',W/'src/ashlar_host/_otel_worker.py');w=importlib.util.module_from_spec(spec);sys.modules[spec.name]=w;spec.loader.exec_module(w)
from dataclasses import asdict
from ashlar_host.config import DiagnosticsLimits
from opentelemetry.trace import SpanContext,TraceFlags
wire={'endpoint':'http://127.0.0.1:4318/','headers':[],'tls':'loopback-test','ca_file':None,'environment':'test','service_version':'0.1.0.dev0','limits':asdict(DiagnosticsLimits(4096,128,524288,4096,16384,10000,64,100,1000,60))}
actual=w.metadata.version
results=[]
with patch.dict(os.environ,{},clear=True),patch.object(w.metadata,'version',side_effect=lambda name:'0.1.0.dev0' if name=='ashlar-graph-toolkit' else actual(name)):
 for mode in ('parent_context','retry_link'):
  worker=w.SDKWorker(w.Settings.from_wire(wire),lambda *args:'success')
  run=object.__new__(facade.OtelRun);run._local=facade.threading.local();requests=[]
  def invoke(request):requests.append(request);return worker.context(request)
  run._invoke=invoke
  context=SpanContext(int('c'*32,16),int('d'*16,16),True,TraceFlags(3))
  observed=[]
  with run.operation_context(**{mode:context}):
   for event in ('started','phase','finished'):
    try:run.trace_context('b'*32,'held-read','ashlar.operation.'+event);observed.append({'event':event,'accepted':True})
    except w.WorkerError:observed.append({'event':event,'accepted':False})
  worker.close(time.monotonic()+.5)
  results.append({'mode':mode,'events':observed,'requests':requests})
receipt={'observedFacade':{'path':str(source),'snapshot':str(snapshot),'sha256':hashlib.sha256(raw).hexdigest()},'workerFreeze':'07b9f67ce3755597dfd4ed12213f09eecee5c13cfd5d81acd3939da1e15c9361','scope':'Direct facade context port to real pinned SDK worker, injected send, no processes/network/native; source version fixture only','results':results}
p=Path('/private/tmp/astra-otel-facade-context-controls-20261010-a.json');p.write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(receipt,indent=2))
