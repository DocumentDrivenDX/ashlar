"""Actual upstream Record checks across fixture schema evolution; no native migration."""
import base64
import json
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
with tempfile.TemporaryDirectory(prefix='ashlar-umf-evolution-') as temporary:
    directory = Path(temporary)
    process = subprocess.run([sys.executable, str(ROOT / 'tools/run_schema_evolution.py'),
                              '--umf-source', str(Path(sys.argv[1]).resolve()),
                              '--umf-check-output-dir', str(directory)],
                             check=True, capture_output=True, text=True)
    result = json.loads(process.stdout)
    first = json.loads((directory / 'schema-1.json').read_bytes())
    third = json.loads((directory / 'schema-3.json').read_bytes())
    existing = json.loads((directory / 'existing-values-under-v3.json').read_bytes())
    for receipt, revision in [(first, '1'), (third, '3'), (existing, '3')]:
        assert base64.b64decode(receipt['sourceBase64'], validate=True) == (ROOT / ('examples/end-to-end/schema-v' + revision + '.umf.json')).read_bytes()
        assert receipt['upgrade']['source']['umf'] == '0.7.0'
        assert receipt['upgrade']['target']['umf'] == '0.8.0'
        assert receipt['originalValidation']['complete'] is False
        assert all(row['result']['validation']['valid'] is True and
                   row['result']['validation']['complete'] is True for row in receipt['records'])
    assert [len(first['records']), len(third['records']), len(existing['records'])] == [2, 1, 2]
    assert [(r['deliveryId'], r['recordSha256']) for r in first['records']] == [(r['deliveryId'], r['recordSha256']) for r in existing['records']]
    assert all(row['result']['fields'][1]['state'] == 'absent' for row in existing['records'])
    assert result['history_revisions'] == ['1', '3'] and result['live_schema_revisions'] == ['3']
    assert result['complete_interpretations'] == [False, False]
    assert result['events'] == 4 and result['tombstones'] == 1 and result['replay_unchanged'] is True
    assert result['existing_value_checks'] == [{'target_revision': '3', 'existing_records': 2, 'native_migration': False}]
    assert result['published'] is False and result['acknowledged'] is False
print('Actual UMF source revisions and retained existing-value checks pass with exact evolution replay.')
