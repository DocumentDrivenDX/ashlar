"""Two inert cleanup counterexamples; no native operation."""
from contextlib import contextmanager
from pathlib import Path
import hashlib,json
from unittest.mock import patch
import test_host_paths_query as fixtures
from test_host_paths_diagnostics import Run
from ashlar_host import paths_query as module
from ashlar_host.config import HostError
pins={'src/ashlar_host/paths_query.py':'f3219de96c6c3822f8bce589f60332283c55fe38580bc48a495fd768cde4c005','tests/test_host_paths_diagnostics.py':'91c10b73bc3888eb3b781d2890535a1c1e5ad0d0d35cbf3a4fbff9c9f3695084'}
r=Path('/Users/erik/Projects/ashlar')
for n,sha in pins.items():assert hashlib.sha256((r/n).read_bytes()).hexdigest()==sha
observations=[]
for kind in ('spark-stop-only','reader-close-only'):
 fixture=fixtures.LifecycleTests();fixture.setUp();run=Run();failure=HostError('synthetic-native-cleanup')
 try:
  overrides={}
  if kind=='spark-stop-only':fixture.spark.stop.side_effect=failure
  else:
   @contextmanager
   def reader(*args):
    yield fixture.opened
    raise failure
   overrides['open_commerce_reader']=reader
  original=module.query_commerce_paths
  with patch.object(module,'query_commerce_paths',lambda config:original(config,diagnostics=run)):
   try:fixture.compose(**overrides)
   except BaseException as caught:assert caught is failure
   else:raise AssertionError('cleanup-failure-lost')
  fixture.spark.stop.assert_called_once()
  assert not (fixture.config.output/'report.json').exists()
  assert not hasattr(failure,'cleanup_failed')
  observed=run.events[-1]
  assert observed[0:2]==('finish','failed')and observed[2]['cleanup_failed']is False
  observations.append({'kind':kind,'observed':observed,'expectedCleanupFailed':True,'expectedCleanupOnlyCategory':'cleanup','businessExceptionUnmodified':True})
 finally:fixture.doCleanups()
for n,sha in pins.items():assert hashlib.sha256((r/n).read_bytes()).hexdigest()==sha
value={'id':'PATH-DIAG-002','scope':'Two actual source/inert native-port counterexamples only, not real native workflow qualification','sourceHashes':pins,'observations':observations}
Path('/private/tmp/astra-paths-cleanup-independent-20261010-b.json').write_text(json.dumps(value,indent=2)+'\n')
print(json.dumps(value))
