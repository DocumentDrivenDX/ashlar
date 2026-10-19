# SPDX-License-Identifier: MIT OR Apache-2.0
"""Authored fixture continuity; UMF owns schema validation and interpretation."""
import json,unittest
from pathlib import Path

class Tests(unittest.TestCase):
    def test_new_model_retains_original_numeric_record_and_adds_one_relationship(self):
        root=Path(__file__).resolve().parents[1]/'examples/truss-native'
        original=json.loads((root/'typed-query-record.umf.json').read_bytes());related=json.loads((root/'typed-related-record.umf.json').read_bytes())
        self.assertEqual(related['umf'],'0.7.0');self.assertEqual(related['id'],'typed-related-record')
        relationships=related['modules'][0]['relationships'];self.assertEqual(len(relationships),1)
        self.assertEqual(relationships[0],{'id':'related','name':'related','source':[{'module':'m','element':'TypedRecord'}],'target':[{'module':'m','element':'TypedRecord','key':'primary'}],'sourceMultiplicity':{'min':0,'max':2},'targetMultiplicity':{'min':0,'max':2},'targetLifecycle':'independent','directed':True,'inverse':'incoming'})
        related['id']=original['id'];related['modules'][0]['relationships']=[]
        self.assertEqual(related,original)
