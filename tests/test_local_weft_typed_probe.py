"""Actual public UMF typed checks; original compiler artifacts from local run."""
import copy
import json
import os
import subprocess
from pathlib import Path
import tempfile
import unittest
from run_local_weft_typed_probe import FIXTURE,FIELDS,_public_records,compile_request,UMF_PIN

UMF=Path('/private/tmp/ashlar-umf-records-c45c72a2')

class TypedPublicModelTests(unittest.TestCase):
    def test_separately_authored_original_source(self):
        model=json.loads((FIXTURE/'model.umf.json').read_bytes())
        self.assertEqual((model['umf'],model['id']),('0.7.0','ashlar-independent-weft-typed-local'))
        elements={e['id']:e for e in model['modules'][0]['elements']}
        self.assertEqual(elements['quantity']['facets'],{'integerWidth':{'bits':64,'signed':True}})
        self.assertEqual(elements['amount']['facets'],{'precision':18,'scale':2})
        self.assertEqual([ref[2] for ref in FIELDS.values()],['integer','decimal','boolean','string'])

    def test_actual_public_upgrade_and_complete_typed_fields(self):
        if not UMF.exists():self.skipTest('Pinned public UMF absent; no public typed qualification executed')
        with tempfile.TemporaryDirectory() as d:
            rows=json.loads((FIXTURE/'rows.json').read_bytes())
            receipt=_public_records(UMF,Path(d),(FIXTURE/'model.umf.json').read_bytes(),rows)
            self.assertEqual(receipt['producerRevision'],UMF_PIN)
            self.assertEqual(receipt['upgrade']['residuals'],[])
            self.assertEqual(sum(len(r['result']['fields']) for r in receipt['records']),8)
            values=receipt['records'][0]['result']['values']
            self.assertEqual([v['value'] for v in values],[{'integerToken':'9007199254740993'},{'decimalToken':'12.50'},{'boolean':True},{'string':'雪'}])

    def test_actual_public_scalar_domains_refuse_corrupt_source(self):
        if not UMF.exists():self.skipTest('Pinned public UMF absent; no public refusal executed')
        rows=json.loads((FIXTURE/'rows.json').read_bytes())
        for replacement in ['{"31":9223372036854775808,"32":12.50,"33":true,"34":"雪"}',
                            '{"31":1,"32":12.501,"33":true,"34":"雪"}',
                            '{"31":1,"32":12.50,"33":"true","34":"雪"}']:
            with self.subTest(replacement=replacement),tempfile.TemporaryDirectory() as d:
                changed=copy.deepcopy(rows);changed[0]['props_json']=replacement
                with self.assertRaises(subprocess.CalledProcessError):_public_records(UMF,Path(d),(FIXTURE/'model.umf.json').read_bytes(),changed)


class OriginalLocalCompilerArtifactsTests(unittest.TestCase):
    def test_actual_compiler_typed_subset_and_engine_refusal(self):
        location=os.environ.get('ASHLAR_LOCAL_WEFT_PROBE')
        if not location:self.skipTest('Set ASHLAR_LOCAL_WEFT_PROBE to original local runner output; no compiler/runtime claim from this skip')
        root=Path(location);report=json.loads((root/'summary.json').read_bytes())
        self.assertEqual((report['public_record_passes'],report['public_field_passes']),(2,8))
        self.assertEqual([p['name'] for p in report['probes']],['select','filter','group-count','join','count'])
        self.assertTrue(report['local_original_props_roundtrip'])
        self.assertFalse(report['published']);self.assertFalse(report['acknowledged'])
        for probe in report['probes']:
            self.assertEqual(probe['status'],'compiled');self.assertFalse(probe['profile_admitted'])
            artifact=json.loads((root/(probe['name']+'-artifact.json')).read_bytes())
            request=json.loads((root/(probe['name']+'-request.json')).read_bytes())
            self.assertEqual(request,compile_request(request['sql'],(FIXTURE/'model.umf.json').read_bytes(),report['table']))
            self.assertEqual(artifact['bindingSha256'],request['target']['bindingSha256'])
            self.assertEqual(artifact['modelPins'],[request['modules'][0]['pin']])
            self.assertTrue(probe['screening'])
            self.assertTrue(all(s['state']=='unsupported-original-sql' for s in probe['screening']))
            self.assertTrue(all('UTF8_BINARY' in s['message'] for s in probe['screening']))
        columns=json.loads((root/'select-artifact.json').read_bytes())['columns']
        self.assertEqual([c['representation']['logicalType']['family'] for c in columns],['integer','decimal','boolean','string'])
        self.assertEqual(columns[0]['representation']['logicalType']['facets'],{'integerWidth':{'bits':64,'signed':True}})
        self.assertEqual(columns[1]['representation']['logicalType']['facets'],{'precision':18,'scale':2})
        self.assertTrue(all(c['representation']['logicalType']['nullable'] is False for c in columns))
