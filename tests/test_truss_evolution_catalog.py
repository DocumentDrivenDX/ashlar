# SPDX-License-Identifier: MIT OR Apache-2.0
import sys,types,unittest
from dataclasses import dataclass
from unittest.mock import patch
from ashlar_host.truss_note_evolution import capture_catalog

class Tests(unittest.TestCase):
    def test_exact_original_catalog_is_captured_before_head_advances(self):
        @dataclass
        class Ok:value:object
        @dataclass
        class Historical:catalog_revision:str
        @dataclass
        class Request:selection:object
        @dataclass
        class Available:view:object
        raw=b'{"catalogRevision":"1","unknown":{"retain":true}}'
        class Assembly:
            def catalog_view_request(self):return Request(None)
            def catalog_view_in_transaction(self,tx,request):
                self.request=request
                return Ok(Available(types.SimpleNamespace(original_bytes=raw)))
        sdk=types.ModuleType('truss');sdk.Ok=Ok;sdk.HistoricalCatalogSelection=Historical;sdk.CatalogViewAvailable=Available
        assembly=Assembly()
        with patch.dict(sys.modules,{'truss':sdk}):
            self.assertIs(capture_catalog(assembly,'tx',1),raw)
            self.assertEqual(assembly.request.selection.catalog_revision,'1')
            with self.assertRaisesRegex(RuntimeError,'Exact original evolution catalog revision'):
                capture_catalog(assembly,'tx',2)
