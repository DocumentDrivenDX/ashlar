import json
import unittest
from dataclasses import replace
from pathlib import Path
from run_local_example import fixture_inputs
from ashlar.publication import Descriptor
from ashlar.weft_binding import string_compile_request,WeftBindingError,LAYOUT_SHA256

ROOT=Path(__file__).resolve().parents[1]

class WeftBindingTests(unittest.TestCase):
    def setUp(self):
        self.intake,self.policy,_=fixture_inputs()
        row=json.loads((ROOT/'docs/helix/04-build/evidence/native-evolution-publication-complete-20261008.json').read_text())['summary']['published'][-1]['descriptor']
        self.descriptor=Descriptor(row['publication_id'],row['profile_version'],json.loads(row['table_versions_json']),json.loads(row['schema_revisions_json']),json.loads(row['source_progress_json']),json.loads(row['validation_report_json']),row)
        self.options=dict(schema_alias='fixture-v3',table_uuids={name:'00000000-0000-0000-0000-000000000001' for name in self.descriptor.versions},manifest_uuid='00000000-0000-0000-0000-000000000002',layout_sha256=LAYOUT_SHA256)
    def request(self,**changes):
        return string_compile_request('SELECT i.label FROM Item i',self.intake,self.policy,self.descriptor,**dict(self.options,**changes))
    def test_original_model_and_complete_publication_versions_are_bound(self):
        request=self.request();binding=json.loads(request['target']['bindingJson'])
        self.assertEqual(request['modules'][0]['documentJson'].encode(),self.intake.source)
        self.assertEqual(binding['publication']['id'],self.descriptor.publication_id)
        self.assertEqual({'.'.join(t['name']):t['version'] for t in binding['publication']['tables']},dict(self.descriptor.versions))
        record=binding['records'][0];self.assertEqual(record['schemaRevision'],'3')
        self.assertEqual(record['typeId'],'17');self.assertEqual([p['home'] for p in record['properties']],[{'kind':'props','propertyId':'23'},{'kind':'props','propertyId':'24'}])
        self.assertFalse(request['options']['allowCandidate'])
    def test_layout_schema_and_incomplete_uuid_inventory_refuse(self):
        for changed in [dict(schema_alias='fixture'),dict(layout_sha256='0'*64),dict(manifest_uuid='unknown'),dict(table_uuids={}),dict(dialect='0.3.0')]:
            with self.assertRaises(WeftBindingError):self.request(**changed)
        self.descriptor=replace(self.descriptor,versions={name:True for name in self.descriptor.versions})
        with self.assertRaises(WeftBindingError):self.request()
