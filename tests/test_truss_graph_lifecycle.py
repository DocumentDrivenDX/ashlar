"""Small selected-target operation/preflight controls, no native SDK execution."""
import json,tempfile,sys,unittest
from pathlib import Path
from dataclasses import dataclass
from types import ModuleType,SimpleNamespace
from unittest.mock import patch
from ashlar_host.truss_graph_lifecycle import run_update_delete_truss_target_command,target_delete_operations

class Tests(unittest.TestCase):
    def test_bad_selected_ids_refuse_before_sdk_or_source_effects(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();config=root/'configuration.json'
            value=dict(connection_uri='private',bun='/not-needed',document_path='/not-needed',document_revision='r1',origin_path='/not-needed',
                object_id=True,edge_id=2,replacement_label='Gamma updated',update_request_key='update',delete_request_key='delete')
            config.write_text(json.dumps(value))
            with self.assertRaisesRegex(ValueError,'signed64 native target identities'):
                run_update_delete_truss_target_command(str(config),str(root/'result.json'))
            self.assertFalse((root/'result.json').exists())
    def test_only_selected_edge_and_target_deleted_in_that_order_with_current_versions(self):
        @dataclass
        class Reference:identity:object
        @dataclass
        class Delete:operation:str;target:Reference;expected_version:str
        module=ModuleType('truss.groups');module.StoredObjectReference=Reference;module.Delete=Delete
        target_identity=SimpleNamespace(id='8',type_id='4');edge_identity=object()
        edge=SimpleNamespace(identity=edge_identity,target=target_identity,version=2)
        target=SimpleNamespace(id=8,type_id=4,version=3)
        with patch.dict(sys.modules,{'truss.groups':module}):
            operations=target_delete_operations(edge,target)
            self.assertEqual([item.operation for item in operations],['delete_edge','delete_object'])
            self.assertEqual([item.expected_version for item in operations],['2','3'])
            self.assertIs(operations[0].target.identity,edge_identity);self.assertIs(operations[1].target.identity,target_identity)
            with self.assertRaisesRegex(ValueError,'object identity differ'):
                target_delete_operations(edge,SimpleNamespace(id=99,type_id=4,version=3))
