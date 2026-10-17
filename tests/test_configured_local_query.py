"""Finite query selection and lossless supported report carriers, no services."""
from dataclasses import replace
from pathlib import Path
from decimal import Decimal
import unittest
from unittest.mock import patch
from ashlar.weft_count_star_distribution import CountStarDistributionPaths
from ashlar.weft_path_decode import PathDecodeConfig,DecodedPaths,PathItem
from ashlar.weft_related_keys_decode import DecodedRelatedKeys
from ashlar.weft_decode import ExactScalar
from ashlar_host.configured_weft_compile import ConfiguredWeftCompileConfig
from ashlar_host.path_capture import PathCaptureConfig
from ashlar_host.configured_local_query import ConfiguredLocalQueryConfig,_report
from ashlar_host.configured_local_runtime import query_configured_local


class Tests(unittest.TestCase):
    def test_explicit_capacities_and_query_cannot_fall_through_to_publish(self):
        config=ConfiguredLocalQueryConfig('SELECT COUNT(*) AS n FROM Item i',
            ConfiguredWeftCompileConfig(CountStarDistributionPaths(Path('/explicit/index'),Path('/explicit/compiler')),1048576),
            PathCaptureConfig(1000,65536,1048576),PathDecodeConfig(65536))
        for fields in ({'sql':''},{'capture':PathCaptureConfig(1001,65536,1048576)},
                       {'capture':PathCaptureConfig(1000,65537,1048576)},
                       {'decoder':PathDecodeConfig(65535)},
                       {'compile':replace(config.compile,maximum_artifact_bytes=1048577)}):
            with self.subTest(fields=fields),self.assertRaises(ValueError):replace(config,**fields)
        with patch('ashlar_host.configured_local_runtime._run')as run:
            for invalid in (None,{},object()):
                with self.assertRaises(ValueError):query_configured_local(object(),ordinal=1,query=invalid)
            run.assert_not_called()

    def test_report_preserves_exact_count_and_unicode_and_refuses_other_values(self):
        original={'decoded':[[ExactScalar(9223372036854775807,'9223372036854775807')],[ExactScalar('雪','雪')]]}
        result=_report(original)
        self.assertEqual(result,{'decoded':[[{'kind':'exact-integer','value':9223372036854775807,'original':'9223372036854775807'}],[{'kind':'exact-string','value':'雪','original':'雪'}]]})
        result['decoded'][1][0]='changed';self.assertEqual(original['decoded'][1][0],ExactScalar('雪','雪'))
        self.assertEqual(_report(ExactScalar(Decimal('-0.00'),'-0.00')),{'kind':'exact-decimal','value':'-0.00','original':'-0.00'})
        self.assertEqual(_report(ExactScalar(False,'false')),{'kind':'exact-boolean','value':False,'original':'false'})
        for unsupported in (ExactScalar(Decimal('NaN'),'NaN'),1.0,object()):
            with self.assertRaises(ValueError):_report(unsupported)

    def test_report_preserves_tagged_keys_path_states_multiplicity_and_original_bytes(self):
        raw=b'{"items":[["same"],["same"]],"truncated":true}'
        keys=DecodedRelatedKeys(raw,(('same',),('same',)),True)
        self.assertEqual(_report(keys),{'kind':'related-keys','original':raw.decode(),
            'items':[['same'],['same']],'truncated':True})
        item=PathItem(('middle',),('雪',),('1','2'))
        path=DecodedPaths(b'{"state":"value"}','value',(item,item),False)
        result=_report(path)
        self.assertEqual(result,{'kind':'related-paths','original':'{"state":"value"}',
            'state':'value','items':[{'intermediate':['middle'],'terminal':['雪'],'edges':['1','2']}]*2,'truncated':False})
        result['items'].clear();self.assertEqual(len(path.items),2)
        absent=DecodedPaths(b'{"state":"absent"}','absent',(),None)
        self.assertEqual(_report(absent)['state'],'absent');self.assertIsNone(_report(absent)['truncated'])


if __name__=='__main__':unittest.main()
