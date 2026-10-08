"""Actual pinned UMF producer plus original CSV custody, apply and replay check."""
import base64
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from ashlar.csv_source import csv_batches
from run_local_example import fixture_value_entries

with tempfile.TemporaryDirectory(prefix='ashlar-csv-umf-') as temporary:
    receipt=Path(temporary)/'original-check.json'
    process=subprocess.run([sys.executable,str(ROOT/'tools/run_csv_example.py'),
        '--umf-source',str(Path(sys.argv[1]).resolve()),'--umf-check-output',str(receipt)],
        check=True,capture_output=True,text=True,timeout=45)
    output=json.loads(process.stdout)
    result=json.loads(receipt.read_bytes())
    source=(ROOT/'examples/end-to-end/schema-v3.umf.json').read_bytes()
    assert base64.b64decode(result['sourceBase64'],validate=True)==source
    assert result['sourceSha256']==hashlib.sha256(source).hexdigest()
    assert result['upgrade']['source']==json.loads(source)
    assert result['upgrade']['target']['umf']=='0.8.0'
    assert result['originalValidation']['valid'] is True
    assert result['originalValidation']['complete'] is False
    lines=(ROOT/'examples/end-to-end/string-source.csv').read_bytes().splitlines(keepends=True)
    batches=tuple(csv_batches(lines,feed='csv-example',epoch='immutable-example-1',
        source_system='local-example',schema_revision='3',type_id='17',properties={'label':'23','caption':'24'}))
    records=[batch.records[0] for batch in batches if json.loads(batch.records[0].raw)['operation']!='delete']
    assert len(result['records'])==output['upstream_record_checks']['records']==len(records)==3
    for original,checked in zip(records,result['records']):
        assert checked['deliveryId']==original.delivery_id
        assert checked['recordSha256']==hashlib.sha256(original.raw).hexdigest()
        event=json.loads(original.raw)
        assert checked['result']['values']==fixture_value_entries(json.loads(event['props_json']))
        assert checked['result']['validation']['valid'] is True
        assert checked['result']['validation']['complete'] is True
        custody=json.loads(checked['deliveryId'])
        assert base64.b64decode(custody['header_base64'])==lines[0]
        assert base64.b64decode(custody['row_base64'])==lines[int(custody['row_ordinal'])]
    assert (output['events'],output['objects'],output['tombstones'])==(4,1,1)
    assert output['replay_unchanged'] is True
    assert json.loads(output['props_json'])=={'23':'updated, quoted','24':'雪'}
    assert json.loads(output['retained_json'])['unmapped_columns']['future']=='18446744073709551615'
    assert output['complete_interpretation'] is False
    assert output['upstream_record_checks']['native_acceptance'] is False
    assert output['published'] is False and output['acknowledged'] is False
print('Actual UMF CSV checks, original custody, deletion/history and exact replay passed.')
