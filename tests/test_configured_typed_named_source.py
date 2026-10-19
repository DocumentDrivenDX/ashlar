# SPDX-License-Identifier: MIT OR Apache-2.0
"""Explicit authored revision comparison; no migration or runtime proof."""
import json,unittest
from pathlib import Path
from ashlar.source import jsonl_batches
from ashlar.typed_source_config import load_typed_jsonl_configuration
ROOT=Path(__file__).resolve().parents[1]/'examples/end-to-end'

class Tests(unittest.TestCase):
    def test_named_revision_preserves_original_values_and_declares_new_pins(self):
        original=load_typed_jsonl_configuration(ROOT/'configured-typed-source.json')
        named=load_typed_jsonl_configuration(ROOT/'configured-typed-named-source.json')
        model=json.loads(named.model_source)
        for module in model['modules']:
            for element in module['elements']:self.assertEqual(element.pop('name'),element['id'])
        self.assertEqual(model,json.loads(original.model_source))
        self.assertEqual((original.document_revision,named.document_revision),('1','2'))
        self.assertNotEqual(original.source,named.source);self.assertNotEqual(original.schema,named.schema)
        groups=[tuple(jsonl_batches(c.source_bytes.splitlines(keepends=True),feed=c.feed,epoch=c.epoch))for c in (original,named)]
        self.assertEqual([len(b.records)for b in groups[1]],[2,1,1])
        for old,new in zip(*groups):
            self.assertEqual(old.batch_id,new.batch_id)
            for a,b in zip(old.records,new.records):
                before=json.loads(a.raw);after=json.loads(b.raw)
                self.assertEqual(before.pop('schema_revision'),'1');self.assertEqual(after.pop('schema_revision'),'2')
                self.assertEqual(before,after)
        original.verify();named.verify()

if __name__=='__main__':unittest.main()
