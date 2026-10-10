"""Independent inert lifecycle controls only; never imports a real native runtime."""
import hashlib,json
from pathlib import Path
from unittest.mock import patch
import test_host_paths_query as fixtures
from test_host_paths_diagnostics import Run
from ashlar_host import paths_query as module
from ashlar_host.config import HostError
root=Path('/Users/erik/Projects/ashlar')
expected={'src/ashlar_host/paths_query.py':'f3219de96c6c3822f8bce589f60332283c55fe38580bc48a495fd768cde4c005','tests/test_host_paths_diagnostics.py':'91c10b73bc3888eb3b781d2890535a1c1e5ad0d0d35cbf3a4fbff9c9f3695084'}
for name,sha in expected.items():assert hashlib.sha256((root/name).read_bytes()).hexdigest()==sha
controls=[]
phases=[('prepare','completed'),('guard','started'),('guard','completed'),('capture','started'),('capture','completed'),('closing','started'),('cleanup','started'),('cleanup','completed'),('closing','completed'),('commit','started'),('commit','completed')]
for typ in (KeyboardInterrupt,SystemExit,GeneratorExit):
 for phase,state in phases:
  fixture=fixtures.LifecycleTests();fixture.setUp();primary=typ()
  try:
   def hook(p,s=None):
    if (p,s)==(phase,state):raise primary
   run=Run(hook);original=module.query_commerce_paths
   with patch.object(module,'query_commerce_paths',lambda config:original(config,diagnostics=run)):
    try:fixture.compose()
    except BaseException as caught:assert caught is primary
    else:raise AssertionError('cancellation-lost')
   fixture.spark.stop.assert_called_once()
   assert not hasattr(primary,'cleanup_failed')
   assert (fixture.config.output/'report.json').exists()==((phase,state)==('commit','completed'))
   controls.append({'cancellation':typ.__name__,'phase':phase,'state':state,'identityPreserved':True,'sparkStopped':True})
  finally:fixture.doCleanups()
fixture=fixtures.LifecycleTests();fixture.setUp()
try:
 run=Run();original=module.query_commerce_paths
 def runtime(*args,**kwargs):
  if kwargs.get('require_fresh')is False:raise HostError('paths-source-drift')
  return []
 with patch.object(module,'query_commerce_paths',lambda config:original(config,diagnostics=run)):
  try:fixture.compose(runtime_paths=runtime)
  except HostError as error:assert str(error)=='paths-source-drift'
  else:raise AssertionError('closing-drift-not-refused')
 fixture.spark.stop.assert_called_once()
 assert not (fixture.config.output/'report.json').exists()
 final=run.events[-1]
 assert final[0:2]==('finish','failed')
 assert final[2]['error_category']=='closing' and final[2]['cleanup_failed']is False
 finding={'id':'PATH-DIAG-001','observed':final,'expectedCategory':'closing','resolution':'Closing runtime/source/jar/schema validation now reports closing after successful cleanup.'}
finally:fixture.doCleanups()
for name,sha in expected.items():assert hashlib.sha256((root/name).read_bytes()).hexdigest()==sha
result={'scope':'Independent inert lifecycle controls, no actual native/SDK/receiver workflow qualification','sourceHashes':expected,'cancellationControls':controls,'finding':finding}
Path('/private/tmp/astra-paths-diagnostics-independent-20261010-b.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({'cancellationControls':len(controls),'finding':finding}))
