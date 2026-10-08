"""Small integration checks against pinned UMF; no database or scale workload."""
import base64
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile

root, revision = sys.argv[1:]
cli = ['bun', 'tools/inspect_umf.ts', root, revision]
checks = []
for name in ['v1', 'v2', 'unknown']:
    source = Path('examples/end-to-end/schema-' + name + '.umf.json')
    result = subprocess.run(cli + [str(source)], check=True, capture_output=True, text=True)
    artifact = json.loads(result.stdout)
    raw = base64.b64decode(artifact['sourceBase64'], validate=True)
    assert raw == source.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == artifact['sourceSha256']
    assert artifact['validatedStructure'] is True
    assert artifact['validatorRevision'] == revision
    if name == 'unknown':
        assert artifact['completeInterpretation'] is False
        assert any(d['code'] == 'UNKNOWN_CORE_FIELD' for d in artifact['validation']['diagnostics'])
    checks.append(name + ': valid UMF and exact retained bytes')
with tempfile.TemporaryDirectory(prefix='ashlar-schema-intake-') as directory:
    bad = Path(directory) / 'invalid.json'
    bad.write_text('{"umf":"0.7.0","umf":"0.7.0"}')
    result = subprocess.run(cli + [str(bad)], capture_output=True, text=True)
    assert result.returncode != 0 and not result.stdout
    checks.append('duplicate members refused')
    bad.write_bytes(b'\xff')
    result = subprocess.run(cli + [str(bad)], capture_output=True, text=True)
    assert result.returncode != 0 and not result.stdout
    checks.append('invalid UTF8 refused')
    source = 'examples/end-to-end/schema-v1.umf.json'
    result = subprocess.run(['bun', 'tools/inspect_umf.ts', root, '0' * 40, source], capture_output=True, text=True)
    assert result.returncode != 0 and not result.stdout
    checks.append('wrong validator revision refused')
print(json.dumps({'state': 'Passed six bounded schema-intake integration checks',
                  'validatorRevision': revision, 'checks': checks,
                  'qualification': 'Actual UMF reader/validator; no native registry, Truss runtime, feed or target enforcement claim'}, indent=2))
