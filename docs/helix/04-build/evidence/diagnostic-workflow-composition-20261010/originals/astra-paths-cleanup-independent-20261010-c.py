"""Finite actual-source/inert cleanup fact distinction; no native workflow."""
from contextlib import contextmanager
from pathlib import Path
import hashlib,json
from test_host_paths_diagnostics import Run,DiagnosticWorkflowTests
from ashlar_host import paths_query as module,publication_reader as owner
from ashlar_host.config import HostError
pins={'src/ashlar_host/paths_query.py':'71c090cdb05fbf30ce875a1189f16c045acad47fedf0efff33d617a6e3578d9c','src/ashlar_host/publication_reader.py':'f023b4a0ba0da047b1c404b8ab095938e3afea4db7b9430ab2f1155e0c33d0a6','tests/test_host_paths_diagnostics.py':'c6d98debc97b8c61b34f4bd173765ba6fddbd656cdaf4bb87bff00a6313c57ce'}
r=Path('/Users/erik/Projects/ashlar')
for n,sha in pins.items():assert hashlib.sha256((r/n).read_bytes()).hexdigest()==sha
observations=[]
for kind in ('spark-stop-only','reader-close-only','reader-closing-guard','post-stop-closing-guard'):
 test=DiagnosticWorkflowTests();test.setUp();fixture=test.fixture;run=Run();failure=HostError('synthetic-native-or-guard')
 try:
  overrides={}
  if kind=='spark-stop-only':fixture.spark.stop.side_effect=failure
  elif kind in ('reader-close-only','reader-closing-guard'):
   @contextmanager
   def reader(*args):
    yield fixture.opened
    if kind=='reader-close-only':
     def close():raise failure
     owner._close_observed(close,fixture.reader_cleanup,None)
    else:
     owner._close_observed(lambda:None,fixture.reader_cleanup,None)
     raise failure
   overrides['open_commerce_reader']=reader
  else:
   def runtime(*args,**kwargs):
    if kwargs.get('require_fresh')is False:raise failure
    return []
   overrides['runtime_paths']=runtime
  try:test.compose(run,**overrides)
  except BaseException as caught:assert caught is failure
  else:raise AssertionError('failure-lost')
  fixture.spark.stop.assert_called_once();assert not (fixture.config.output/'report.json').exists()
  assert not hasattr(failure,'cleanup_failed')
  final=run.events[-1];cleanup=kind in ('spark-stop-only','reader-close-only')
  assert final[0:2]==('finish','failed')and final[2]=={'error_category':'cleanup'if cleanup else'closing','cleanup_failed':cleanup}
  observations.append({'kind':kind,'observed':final,'businessExceptionUnmodified':True})
 finally:test.doCleanups()
# Public factory exact type and one claim are checked before fixture path access.
class Subclass(owner.ReaderCleanupState):pass
for bad in (object(),Subclass()):
 try:
  with owner.open_commerce_reader(None,'/synthetic-missing',cleanup_state=bad):pass
 except ValueError as error:assert str(error)=='invalid-reader-cleanup-state'
 else:raise AssertionError('untyped-state-admitted')
state=owner.ReaderCleanupState()
try:
 with owner.open_commerce_reader(None,'/synthetic-missing',cleanup_state=state):pass
except FileNotFoundError:pass
try:
 with owner.open_commerce_reader(None,'/synthetic-missing',cleanup_state=state):pass
except ValueError as error:assert str(error)=='invalid-reader-cleanup-state'
else:raise AssertionError('state-reused')
for n,sha in pins.items():assert hashlib.sha256((r/n).read_bytes()).hexdigest()==sha
value={'scope':'Four finite actual-source/inert workflow cases plus exacttype/singlefactoryclaim; no native/installed qualification','sourceHashes':pins,'observations':observations,'stateExactTypeAndSingleClaim':True,'PGPortEvidence':'Actual publicprovider interval guard/rollback/close distinction exercised by reviewed source test suite; mocked connection only.'}
Path('/private/tmp/astra-paths-cleanup-independent-20261010-c.json').write_text(json.dumps(value,indent=2)+'\n');print(json.dumps(value))
