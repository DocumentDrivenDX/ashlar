"""Focused real-producer integration check; no native acceptance/publication."""
import base64
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
umf_source = Path(sys.argv[1]).resolve()
with tempfile.TemporaryDirectory(prefix='ashlar-local-umf-check-') as temporary:
    receipt = Path(temporary) / 'original-check.json'
    process = subprocess.run([sys.executable, str(ROOT / 'tools/run_local_example.py'),
                              '--umf-source', str(umf_source), '--umf-check-output', str(receipt)],
                             check=True, capture_output=True, text=True)
    value = json.loads(process.stdout)
    original = json.loads(receipt.read_bytes())
    source = (ROOT / 'examples/end-to-end/schema-v3.umf.json').read_bytes()
    assert base64.b64decode(original['sourceBase64'], validate=True) == source
    assert original['sourceSha256'] == hashlib.sha256(source).hexdigest()
    assert original['upgrade']['source'] == json.loads(source)
    assert original['upgrade']['source']['umf'] == '0.7.0'
    assert original['upgrade']['target']['umf'] == '0.8.0'
    assert original['originalValidation']['valid'] is True
    assert original['originalValidation']['complete'] is False
    assert len(original['records']) == value['upstream_record_checks']['records'] == 3
    assert all(record['result']['validation']['valid'] is True and
               record['result']['validation']['complete'] is True for record in original['records'])
    assert (value['events'], value['objects'], value['tombstones']) == (4, 1, 1)
    assert value['replay_unchanged'] is True
    assert value['rows'][0]['props_json'] == '{"23":"updated","24":"雪"}'
    assert value['rows'][0]['retained_json'] == '{"future":18446744073709551615}'
    assert value['complete_interpretation'] is False
    assert value['published'] is False and value['acknowledged'] is False
    request = Path(temporary) / 'invalid-record.json'
    invalid = {'identity': {'module': 'fixture', 'element': 'item'}, 'records': [
        {'deliveryId': 'negative', 'recordSha256': '0' * 64, 'values': []}]}
    request.write_text(json.dumps(invalid))
    command = ['bun', str(ROOT / 'tools/check_umf_record_values.ts'), str(umf_source),
               value['upstream_record_checks']['producer_revision'],
               str(ROOT / 'examples/end-to-end/schema-v3.umf.json'), str(request)]
    refused = subprocess.run(command, capture_output=True)
    assert refused.returncode != 0 and not refused.stdout
    request.write_text('{"identity":{},"identity":{},"records":[]}')
    refused = subprocess.run(command, capture_output=True)
    assert refused.returncode != 0 and not refused.stdout
print('Runnable actual UMF Record checks, source apply/deletion/history and retained replay passed.')
