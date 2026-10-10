from pathlib import Path
import hashlib,importlib.util,io,json,os,sys,time,unittest
from unittest.mock import patch
R=Path('/Users/erik/Projects/ashlar');sys.path.insert(0,str(R/'src'))
F=Path('/private/tmp/ashlar-otel-worker-source-freeze-20261010-d')
spec=importlib.util.spec_from_file_location('ashlar_host._otel_worker',F/'src/ashlar_host/_otel_worker.py');w=importlib.util.module_from_spec(spec);sys.modules[spec.name]=w;spec.loader.exec_module(w)
spec=importlib.util.spec_from_file_location('frozen_worker_tests_d',F/'tests/test_otel_worker.py');t=importlib.util.module_from_spec(spec);spec.loader.exec_module(t)
actual=w.metadata.version;checks=[]
with patch.dict(os.environ,{},clear=True),patch.object(w.metadata,'version',side_effect=lambda name:'0.1.0.dev0' if name=='ashlar-graph-toolkit' else actual(name)):
 for exc_type in (KeyboardInterrupt,SystemExit,GeneratorExit):
  worker=w.SDKWorker(w.Settings.from_wire(t.settings_wire()),lambda *args:'success')
  ordinary=OSError('fixture-only');cancel=exc_type();cleanup=[]
  with patch.object(worker.reader,'collect',side_effect=ordinary),patch.object(worker.logs,'shutdown',side_effect=cancel),patch.object(worker.traces,'shutdown',side_effect=lambda:cleanup.append('traces')),patch.object(worker.metrics,'shutdown',side_effect=lambda **kw:cleanup.append('metrics')):
   try:worker.close(time.monotonic()+.5)
   except BaseException as caught:checks.append({'control':'close ordinary collection failure then cancellation','cancel':exc_type.__name__,'caught':type(caught).__name__,'cancelIdentity':caught is cancel,'ordinaryIdentity':caught is ordinary,'cleanup':cleanup})
  # Patched providers were inert; complete real provider cleanup after the assertion.
  worker.logs.shutdown();worker.traces.shutdown();worker.metrics.shutdown()
 # Exercise same policy at transport boundary, without network or opened SDK transport.
 for exc_type in (KeyboardInterrupt,SystemExit,GeneratorExit):
  ordinary=OSError('fixture-only');cancel=exc_type();cleanup=[]
  class Connection:
   sock=None
   def request(self,*a,**k):raise ordinary
   def close(self):cleanup.append('close');raise cancel
  with patch.object(w.http.client,'HTTPConnection',return_value=Connection()):
   try:w.HTTPTransport(w.Settings.from_wire(t.settings_wire()))('logs',b'',time.monotonic()+1)
   except BaseException as caught:checks.append({'control':'transport ordinary request failure then cancellation','cancel':exc_type.__name__,'caught':type(caught).__name__,'cancelIdentity':caught is cancel,'ordinaryIdentity':caught is ordinary,'cleanup':cleanup})
stream=io.StringIO();result=unittest.TextTestRunner(stream=stream,verbosity=1).run(unittest.defaultTestLoader.loadTestsFromModule(t));print(stream.getvalue());assert result.wasSuccessful()
assert all(c['cancelIdentity'] and not c['ordinaryIdentity'] for c in checks)
receipt={'sourceSha256':hashlib.sha256((F/'src/ashlar_host/_otel_worker.py').read_bytes()).hexdigest(),'freezeSha256':hashlib.sha256((F/'manifest.json').read_bytes()).hexdigest(),'scope':'Six bounded real SDK or inert transport source controls, no network/receiver/secret/native','checks':checks,'ownerTests':result.testsRun}
p=Path('/private/tmp/astra-otel-worker-cleanup-controls-20261010-d.json');p.write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(receipt,indent=2))
