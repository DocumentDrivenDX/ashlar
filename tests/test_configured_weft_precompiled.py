# SPDX-License-Identifier: MIT OR Apache-2.0
"""The .53 precompiled execution seam rejects stale compiler profiles."""
import json
import unittest
from pathlib import Path
from unittest.mock import patch

from ashlar_host.configured_weft_query import query_configured_projection_precompiled
from ashlar_host.count_star_admission import CountStarAdmissionConfig, CountStarSchemaValidation


class PrecompiledWeftTests(unittest.TestCase):
    def compiled(self, profile='count-star-current-053'):
        root=Path(__file__).parent.parent/'docs/helix/04-build/evidence/current-upstream-release-20261024/r02-native-execution-packet-20261024/weft053-schemas'
        schemas={p.name:p.read_bytes() for p in root.glob('*.json')}
        validation=CountStarSchemaValidation(schemas,lambda *_:None,profile=profile)
        admission=CountStarAdmissionConfig(1048576,validation,profile=profile)
        body={'status':'compiled'};raw=json.dumps(body,separators=(',',':')).encode()
        return {'compilerVersion':'0.4.53','admission_config':admission,'request':{},'response':body,
            'recompiled':body,'requestBytes':b'{}\n','responseBytes':raw,'recompiledBytes':raw}

    def test_precompiled_seam_requires_separate_053_profile(self):
        with patch('ashlar_host.configured_weft_query._query',return_value={'held':True}) as query:
            item=self.compiled()
            self.assertEqual(query_configured_projection_precompiled(object(),object(),'SELECT 1',item,capture=object(),decoder=object()),{'held':True})
            self.assertIs(query.call_args.kwargs['precompiled'],item)
        with self.assertRaises(ValueError):
            query_configured_projection_precompiled(object(),object(),'SELECT 1',self.compiled('count-star-current'),capture=object(),decoder=object())

    def test_precompiled_seam_rejects_malformed_bytes(self):
        item=self.compiled();item['responseBytes']=b'not-json'
        with self.assertRaises(ValueError):
            with patch('ashlar_host.configured_weft_query._query'):
                query_configured_projection_precompiled(object(),object(),'SELECT 1',item,capture=object(),decoder=object())


if __name__=='__main__':unittest.main()
