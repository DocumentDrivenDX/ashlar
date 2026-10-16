"""Static original fixture/materialization checks; no Spark or native module."""
import copy
import hashlib
import json
from pathlib import Path
import unittest
import run_local_current_query_example as example


class Tests(unittest.TestCase):
    def test_original_public_fixture_and_lossless_local_values(self):
        raw=(Path(__file__).resolve().parents[1]/'examples/current-query-local/mixed-original-uses.request.json').read_bytes()
        self.assertEqual(hashlib.sha256(raw).hexdigest(),example.REQUEST_SHA)
        self.assertEqual(example.ROWS[0][-1],'opaque-λ\x00')
        self.assertEqual(example.ROWS[1][-1],'')
        self.assertEqual(sum(row[0] for row in example.ROWS),2)
        self.assertEqual(json.loads(example.text(example.ROWS))[0][-1],example.ROWS[0][-1])

    def test_binding_preserves_exact_per_use_and_relation_without_authority_flags(self):
        field={'documentId':'d','revision':'r','module':'m','element':'token'}
        packet={'sourceRequestSha256':'a'*64,'designationSha256':'b'*64,'binding':{'sha256':'c'*64},
            'scans':[{'scan':'s0','record':dict(field,element='Resource')}],
            'operations':[{'operands':[{'path':'where/0','input':{'kind':'field','scan':'s0','identity':field,'originalAction':None}}]}],
            'projections':[{'path':'outputs/0','scan':'s0','field':field}]}
        original=copy.deepcopy(packet)
        relation={'catalog':'spark_catalog','schema':'explicit','table':'current_values','uuid':'actual-uuid',
            'version':'0','schemaSha256':'d'*64,'eligibleColumn':'eligible','rowKeyColumn':'private_key'}
        binding=example.original_binding(packet,relation)
        self.assertEqual(packet,original)
        self.assertEqual(binding['scans'][0]['relation'],relation)
        self.assertEqual([v['path'] for v in binding['values']],['where/0','outputs/0'])
        self.assertEqual([v['mode'] for v in binding['values']],['disclosed',None])
        self.assertEqual(binding['values'][1]['domain'],field)
        self.assertNotIn('authority',binding)
        self.assertNotIn('securityReady',binding)
        self.assertEqual(binding['version'],'weft.databricks.current-values/0.1-candidate')
        spark=example.original_binding(packet,relation,engine='spark')
        self.assertEqual(spark['version'],'weft.spark.current-values/0.1-candidate')
        self.assertEqual(spark['values'],binding['values'])
        self.assertEqual(spark['scans'],binding['scans'])
        with self.assertRaisesRegex(ValueError,'development native/shim/source pins'):
            example.main(['--engine','spark','--weft-root','/unused','--request','/unused',
                '--output','/unused','--java-home','/unused','--delta-jar','/unused','--schema','unused'])



if __name__=='__main__':unittest.main()
