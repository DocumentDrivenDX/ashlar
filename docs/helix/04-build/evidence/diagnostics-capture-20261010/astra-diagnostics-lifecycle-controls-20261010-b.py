"""Independent public capture lifecycle probes, inert sinks and local files only."""
import contextlib
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import sys
from unittest.mock import patch

ROOT=Path('/Users/erik/Projects/ashlar')
FREEZE=Path('/private/tmp/ashlar-diagnostics-source-freeze-20261010-b')
sys.path.insert(0,str(ROOT/'src'))
import ashlar_host
def load(name,path):
    spec=importlib.util.spec_from_file_location(name,path)
    module=importlib.util.module_from_spec(spec);sys.modules[name]=module
    spec.loader.exec_module(module);return module
d=load('ashlar_host.diagnostics',FREEZE/'src/ashlar_host/diagnostics.py')
ashlar_host.diagnostics=d
t=load('astra_owner_diagnostic_fixture',FREEZE/'tests/test_host_diagnostics.py')

@contextlib.contextmanager
def fixture():
    case=t.DiagnosticsTests();case.setUp()
    try:yield case
    finally:case.doCleanups()

rows=[]
with fixture() as f:
    run=f.run_owner();a=run.begin_attempt('held-read');run.finish_attempt(a,'succeeded')
    private_failure=OSError('secret-endpoint-payload')
    with patch.object(d.os,'replace',side_effect=private_failure):
        result=run.close()
        assert result is None and f.sink.closed
        assert json.loads((run.directory/'run.json').read_bytes())['state']=='open'
        assert 'secret-endpoint-payload' not in f.console.getvalue()
        assert f.console.getvalue().count('ashlar diagnostics incomplete')==1
        rows.append({'id':'DL-001','status':'pass','unavailableResultIsNone':True,'diskState':'open','safeNoticeOnly':True,'sinkShutdownAttempted':True})

for kind in (KeyboardInterrupt,SystemExit,GeneratorExit):
    with fixture() as f:
        run=f.run_owner();a=run.begin_attempt('held-read');run.finish_attempt(a,'cancelled','cleanup')
        primary=kind('private-not-stringified')
        with patch.object(d.os,'replace',side_effect=OSError('private-close')):
            try:run.close(primary)
            except BaseException as actual:assert actual is primary and primary.cleanup_failed
            else:raise AssertionError('primary swallowed')
        assert f.sink.closed
        rows.append({'id':kind.__name__,'status':'pass','primaryIdentityAndCleanupMarker':True})

with fixture() as f:
    run=f.run_owner();a=run.begin_attempt('held-read');run.finish_attempt(a,'succeeded')
    with patch.object(d.os,'getpid',return_value=run.pid+1):
        try:run.phase(a,'guard','completed')
        except d.DiagnosticsError:pass
        else:raise AssertionError('foreign process admitted')
    run.close()
    assert d.read_diagnostics(run.directory)['capture_complete']
    rows.append({'id':'cross-process-refusal','status':'pass'})

with fixture() as f:
    run=f.run_owner();a=run.begin_attempt('held-read');run.finish_attempt(a,'succeeded')
    original=run.sink
    def fail_shutdown(deadline):raise RuntimeError('private-exporter')
    run.sink=d.DiagnosticSignalSink(original.emit,original.trace_context,fail_shutdown)
    result=run.close()
    assert result['complete'] and result['loss']['logs']['unknown']
    assert 'private-exporter' not in f.console.getvalue()
    rows.append({'id':'ordinary-port-shutdown-failure','status':'pass','classificationPreserved':True})

def descriptor(path):
    b=path.read_bytes();return {'path':str(path),'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()}
result={'format':'astra-diagnostics-lifecycle-probes/0.1','freeze':descriptor(FREEZE/'manifest.json'),'source':descriptor(FREEZE/'src/ashlar_host/diagnostics.py'),'rows':rows,'scope':'Frozen source; real owned local files, mocked installed version and signal ports. No SDK/network/native invocation.','contractBasis':'C006 error semantics: ordinary diagnostic-only shutdown failures do not change completed operation classification; SDK/exporter failures fixed categories; primary failures preserved.'}
out=Path('/private/tmp/astra-diagnostics-lifecycle-controls-20261010-b.json');out.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(descriptor(out)))
