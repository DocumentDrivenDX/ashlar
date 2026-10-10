"""Source-only independent profile-selection controls; no SDK or compiler use."""
import contextlib
import dataclasses
import hashlib
import io
import json
from pathlib import Path
import sys
from unittest.mock import Mock, patch

ROOT = Path('/Users/erik/Projects/ashlar')
FREEZE = Path('/private/tmp/ashlar-paths-profile-selection-freeze-20261010-b')
EXPECTED = 'a9729b8a1dd37acd404e9df9f6d1deaf0244e5c57d601b782ff87d25c2a8dff3'
OUT = Path('/private/tmp/astra-paths-profile-selection-review-20261010-b.json')
sha = lambda raw: hashlib.sha256(raw).hexdigest()

def custody():
    raw = (FREEZE/'manifest.json').read_bytes()
    assert sha(raw) == EXPECTED
    manifest = json.loads(raw)
    for entry in manifest['files']:
        frozen = (FREEZE/'files'/entry['path']).read_bytes()
        assert len(frozen) == entry['bytes'] and sha(frozen) == entry['sha256']
        assert (ROOT/entry['path']).read_bytes() == frozen
    return manifest

manifest = custody()
sys.path.insert(0, str(ROOT/'src'))
from ashlar.cli import main
from ashlar.weft_path_decode import PathDecodeConfig
from ashlar_host.config import HostError, ProducerConfig, PrivatePostgresConfig, QueryCommercePathsConfig
from ashlar_host.path_capture import PathCaptureConfig
from ashlar_host import paths_query

base = Path('/explicit-profile-review-no-access')
positions = [base/name for name in ('index', 'installation', 'publication', 'output', 'jars', 'model', 'graph')]
producer = ProducerConfig(base/'umf', base/'bun', base/'git', 20, 1048576, 4194304)
postgres = PrivatePostgresConfig('ashlar-e2e-truss-pg17', '127.0.0.1', 15432, 'truss_e2e')
# The original twelve positional arguments still construct the default profile.
config = QueryCommercePathsConfig(*positions, producer, postgres, 16777216,
                                 PathCaptureConfig(10, 1024, 4096), PathDecodeConfig(1024))
assert config.profile == 'paths'
args = ['ashlar', 'query-commerce-paths']
for name in ('index', 'installation', 'publication', 'output', 'jars', 'model', 'graph', 'umf-source', 'bun', 'git'):
    args += ['--'+name, str(base/name)]
options = dict(postgres_container='ashlar-e2e-truss-pg17', postgres_host='127.0.0.1',
    postgres_port=15432, postgres_database='truss_e2e', producer_timeout_seconds=20,
    producer_maximum_output_bytes=1048576, producer_maximum_receipt_bytes=4194304,
    maximum_artifact_bytes=16777216, maximum_rows=10, maximum_cell_bytes=1024,
    maximum_total_cell_bytes=4096)
for name,value in options.items():
    args += ['--'+name.replace('_','-'),str(value)]

effects = ('runtime_paths', 'original_inputs', 'read_bounded', 'PathsDistributionPaths',
    'installed_paths_schema_bundle', 'make_offline_path_schema_validation',
    'recompute_dataset', 'compile_paths_distribution', 'open_commerce_reader',
    'execute_commerce_path', '_publish_report')

@contextlib.contextmanager
def traps(runtime_exception=None):
    with contextlib.ExitStack() as stack:
        mocks = {}
        for name in effects:
            error = runtime_exception if name == 'runtime_paths' and runtime_exception is not None else AssertionError('effect reached: '+name)
            mocks[name] = stack.enter_context(patch.object(paths_query, name, side_effect=error))
        mocks['mkdir'] = stack.enter_context(patch.object(Path, 'mkdir', side_effect=AssertionError('mkdir reached')))
        yield mocks

checks = []
with traps() as mocks:
    try:
        paths_query.query_commerce_paths(dataclasses.replace(config, profile='paths-keys'))
        raise AssertionError('missing API refusal')
    except HostError as error:
        assert str(error) == 'paths-profile-not-installed'
    assert all(mock.call_count == 0 for mock in mocks.values())
checks.append('actual public API keys refusal precedes all 12 trapped effects')

out,err = io.StringIO(),io.StringIO()
with traps() as mocks, patch.object(sys,'argv',args+['--profile','paths-keys']), contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
    try:
        main()
        raise AssertionError('missing CLI refusal')
    except SystemExit as error:
        assert error.code == 2
    assert all(mock.call_count == 0 for mock in mocks.values())
assert out.getvalue() == '' and err.getvalue() == 'ashlar-host: refused\n'
checks.append('actual public CLI keys refusal: exit2, empty stdout, constant stderr, all effects0')

for explicit in (False, True):
    for route in ('api','cli'):
        primary = KeyboardInterrupt('review cancellation')
        primary.cleanup_failed = True
        out,err = io.StringIO(),io.StringIO()
        with traps(primary) as mocks, patch.object(sys,'argv',args+(['--profile','paths'] if explicit else [])), contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            try:
                if route == 'api':
                    selected = dataclasses.replace(config, profile='paths') if explicit else config
                    paths_query.query_commerce_paths(selected)
                else:
                    main()
                raise AssertionError('cancellation swallowed')
            except KeyboardInterrupt as error:
                assert error is primary and error.cleanup_failed is True
            assert mocks['runtime_paths'].call_count == 1
            assert all(mock.call_count == 0 for name,mock in mocks.items() if name != 'runtime_paths')
        assert out.getvalue() == '' and err.getvalue() == ''
        checks.append('%s %s paths reaches original dispatch and preserves cancellation identity/marker without output' % (route,'explicit' if explicit else 'default'))

assert not any(name.split('.')[0] in {'pyspark','delta','psycopg','jsonschema'} for name in sys.modules)
assert custody() == manifest
result = dict(format='independent-source-review/0.1', verdict='approved-scoped-source-only',
    freeze={'path':str(FREEZE/'manifest.json'),'sha256':EXPECTED}, sourceFiles=manifest['files'],
    baseline=manifest['baseCommit'], script={'path':str(Path(__file__)), 'sha256':sha(Path(__file__).read_bytes())},
    runtime=sys.version, commands=[
      'PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:tests /usr/bin/python3 -B -S -m unittest test_paths_host_configuration test_host_cli -v',
      'PYTHONDONTWRITEBYTECODE=1 /usr/bin/python3 -B -S /private/tmp/astra-paths-profile-selection-review-20261010-b.py'],
    portableTests={'passed':9,'failures':0}, independentControls=checks,
    staticChecks=['new field appended with paths default preserves former positional API',
      'closed exact string profile accepted only paths or paths-keys',
      'CLI flag limited to query-commerce-paths; other routes unchanged',
      'unknown argparse profile and help terminate before host composition',
      'keys guard immediately after exact configuration type and before runtime paths',
      'no fallback from uninstalled keys selection'],
    effects={'nativeImports':False,'compiler':False,'installation':False,'nativeOrDatabase':False,'sourceMutation':False},
    openingClosingSourceHashesEqual=True,
    limits=['Selection API only: paths-keys intentionally remains unavailable.',
      'No new profile installation, compiler, SDK, native execution, or support qualification is inferred.'])
OUT.write_text(json.dumps(result,indent=2,sort_keys=True)+'\n')
print(json.dumps({'receipt':str(OUT),'sha256':sha(OUT.read_bytes()),'independentControls':len(checks),'verdict':result['verdict']}))
