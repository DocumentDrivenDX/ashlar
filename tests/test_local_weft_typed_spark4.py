"""Check actual retained local Spark4 SQL receipts, exact values and controls."""
import hashlib
import json
import os
from pathlib import Path
import sys
import unittest
from decimal import Decimal
from ashlar.weft_decode import decode_exact_scalar
from run_local_weft_typed_probe import FIXTURE,EXTENSION_SHA
from run_local_weft_typed_spark4 import JAR_SHA

class ActualTypedSpark4Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        location=os.environ.get('ASHLAR_LOCAL_WEFT_SPARK4')
        if not location:raise unittest.SkipTest('Set ASHLAR_LOCAL_WEFT_SPARK4 to actual retained local run; no runtime proof from skip')
        wheel=os.environ.get('ASHLAR_WEFT_WHEEL')
        if not wheel:raise ValueError('Set ASHLAR_WEFT_WHEEL to the exact compiler wheel used by the retained run')
        cls.wheel=Path(wheel)
        cls.root=Path(location);cls.report=json.loads((cls.root/'summary.json').read_bytes())

    def test_original_compiler_artifact_reproduction(self):
        wheel=self.wheel
        self.assertEqual(hashlib.sha256((wheel/'weft/weft.abi3.so').read_bytes()).hexdigest(),EXTENSION_SHA)
        sys.path.insert(0,str(wheel));import weft
        for name in ['select','filter','group-count','join','count','refusal-quantity','refusal-amount','refusal-active']:
            request=json.loads((self.root/(name+'-request.json')).read_bytes())
            artifact=json.loads((self.root/(name+'-artifact.json')).read_bytes())
            self.assertEqual(json.loads(weft.compile_json(json.dumps(request))),artifact)
            self.assertEqual(request['modules'][0]['documentJson'],(FIXTURE/'model.umf.json').read_text())

    def test_native_queries_and_all_original_integrity_statements(self):
        report=self.report
        self.assertEqual(report['state'],'passed-local-unchanged-typed-sql')
        self.assertEqual(report['jar_sha256'],JAR_SHA)
        self.assertEqual((report['public_record_passes'],report['public_field_passes']),(2,8))
        probes={p['name']:p for p in report['probes']}
        self.assertEqual(set(probes),{'select','filter','group-count','join','count'})
        self.assertEqual(probes['filter']['rows'],[{'quantity':'9007199254740993','amount':'12.50'}])
        self.assertEqual(probes['group-count']['rows'],[{'active':'false','n':'1'},{'active':'true','n':'1'}])
        self.assertEqual(probes['join']['rows'],[{'left_quantity':'2','right_quantity':'2'},{'left_quantity':'9007199254740993','right_quantity':'9007199254740993'}])
        self.assertEqual(probes['count']['rows'],[{'n':'2'}])
        for name,probe in probes.items():
            artifact=json.loads((self.root/(name+'-artifact.json')).read_bytes())
            expected=[c for o in artifact['obligations'] if o['id']=='ashlar.candidate.scalarIntegrity' for c in o['parameters']['checks']]
            self.assertEqual([c['check'] for c in probe['integrity']],expected)
            self.assertTrue(expected)
            self.assertTrue(all(c['rows']==[{'violations':'0'}] for c in probe['integrity']))
            self.assertFalse(probe['profile_admitted']);self.assertFalse(probe['published'])
        self.assertFalse(report['published']);self.assertFalse(report['acknowledged'])

    def test_native_exact_scalar_carriers(self):
        artifact=json.loads((self.root/'select-artifact.json').read_bytes())
        select=next(p for p in self.report['probes'] if p['name']=='select')
        decoded=[{c['outputName']:decode_exact_scalar(c['representation'],r[c['outputName']]).value for c in artifact['columns']} for r in select['rows']]
        self.assertEqual(decoded,[{'quantity':2,'amount':Decimal('40.00'),'active':False,'label':'second'},
                                  {'quantity':9007199254740993,'amount':Decimal('12.50'),'active':True,'label':'雪'}])

    def test_actual_corrupt_values_stop_before_user_sql(self):
        self.assertTrue(self.report['retained_snapshot_after_refusals'])
        self.assertEqual([c['field'] for c in self.report['refusals']],['quantity','amount','active'])
        for case in self.report['refusals']:
            self.assertFalse(case['user_sql_executed'])
            self.assertEqual(len(case['checks']),4)
            self.assertEqual([c['check']['field']['element'] for c in case['checks'] if c['rows']==[{'violations':'1'}]],[case['field']])
            self.assertTrue(all(c['rows'] in ([{'violations':'0'}],[{'violations':'1'}]) for c in case['checks']))
