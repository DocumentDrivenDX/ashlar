"""Exact fixed physical provenance, compatibility and bounded resource lifetime."""
import hashlib
from importlib.resources import files
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from ashlar_host.local_physical_carriers import original_local_physical_carriers, original_local_materialization_columns, ORIGINAL_LOCAL_PHYSICAL_PROVENANCE
from ashlar_host.original_commerce_local_setup import generated_carriers


class LocalPhysicalCarrierTests(unittest.TestCase):
    def test_generated_columns_match_independent_original_sql_fixture(self):
        from ashlar_host.schema_rows import fixture_columns
        expected = fixture_columns(Path(__file__).resolve().parents[1])
        self.assertEqual(original_local_materialization_columns(), expected)
        self.assertEqual(tuple(original_local_materialization_columns()),
            ('object_current','edge_current','tombstone','whole_source_history'))

    def test_columns_use_fixed_package_without_checkout_reads_and_reject_resource_drift(self):
        with patch('pathlib.Path.read_text', side_effect=AssertionError('Checkout SQL read')):
            columns = original_local_materialization_columns()
        self.assertEqual(tuple(map(len, columns.values())), (17,20,10,6))
        selected, root = self.selected_copy()
        path = root/'runtime-carriers.generated.json'; path.write_bytes(path.read_bytes()+b' ')
        with patch('ashlar_host.local_physical_carriers.files', return_value=selected), self.assertRaises(ValueError):
            original_local_materialization_columns()

    def test_complete_actual_packaged_parity_preserves_original_profiles_and_provenance(self):
        expected=files('ashlar_host').joinpath('resources','original-commerce-local')
        raw=expected.joinpath('runtime-carriers.generated.json').read_bytes()
        self.assertEqual(original_local_physical_carriers(),json.loads(raw))
        self.assertEqual(generated_carriers(),json.loads(raw))
        self.assertEqual(hashlib.sha256(expected.joinpath('provenance.json').read_bytes()).hexdigest(),ORIGINAL_LOCAL_PHYSICAL_PROVENANCE)
        value=original_local_physical_carriers()
        self.assertEqual(value['profile'],'ashlar-generated-original-commerce-local-carriers/0.1')
        self.assertEqual(value['generator']['revision'],'fac1497a5ac5cb39bbaaac34eb0da8dc8b208c69')
        for role,carrier in value['carriers'].items():
            self.assertEqual(carrier['layout']['name'],[role]);self.assertEqual(carrier['layout']['partitionBy'],[])
            self.assertEqual(carrier['layout']['clusterBy'],[]);self.assertEqual(carrier['layout']['properties'],{})
    def selected_copy(self):
        temporary=tempfile.TemporaryDirectory();self.addCleanup(temporary.cleanup)
        selected=Path(temporary.name).resolve();root=selected/'resources/original-commerce-local';root.mkdir(parents=True)
        original=files('ashlar_host').joinpath('resources','original-commerce-local')
        provenance=json.loads(original.joinpath('provenance.json').read_bytes())
        for name in ('provenance.json',*(item['resource']for item in provenance['resources'])):
            root.joinpath(name).write_bytes(original.joinpath(name).read_bytes())
        return selected,root
    def test_actual_resource_mutation_missing_and_generated_payload_refuse(self):
        for name in ('provenance.json','object_current.umf.json','runtime-carriers.generated.json','e-correspondence.json'):
            selected,root=self.selected_copy();path=root/name;path.write_bytes(path.read_bytes()+b' ')
            with patch('ashlar_host.local_physical_carriers.files',return_value=selected),self.assertRaises(ValueError):original_local_physical_carriers()
        selected,root=self.selected_copy();(root/'manifest.umf.json').unlink()
        with patch('ashlar_host.local_physical_carriers.files',return_value=selected),self.assertRaises(FileNotFoundError):original_local_physical_carriers()
    def test_fixed_selection_has_no_callback_path_discovery_or_semantic_override(self):
        callback=lambda:self.fail('Caller callback invoked')
        with self.assertRaises(TypeError):original_local_physical_carriers(callback)
        with self.assertRaises(TypeError):original_local_physical_carriers(root=Path('/foreign'))
        with patch('ashlar_host.local_physical_carriers.files',wraps=files)as selected:
            value=original_local_physical_carriers()
        selected.assert_called_once_with('ashlar_host')
        self.assertNotIn('medical',value['profile']);self.assertNotIn('source_authority',value)
    def test_stream_read_cancel_preserves_primary_while_close_attempted(self):
        cancel=KeyboardInterrupt('bounded resource');events=[]
        class Stream:
            def read(self,count):events.append(('read',count));raise cancel
            def close(self):events.append(('close',));raise OSError('secondary')
        class Resource:
            def joinpath(self,*args):return self
            def open(self,mode):events.append(('open',mode));return Stream()
        with patch('ashlar_host.local_physical_carriers.files',return_value=Resource()),self.assertRaises(KeyboardInterrupt)as error:original_local_physical_carriers()
        self.assertIs(error.exception,cancel);self.assertEqual(events,[('open','rb'),('read',65537),('close',)])
    def test_oversized_resource_refuses_after_bounded_read_and_closes(self):
        events=[]
        class Stream:
            def read(self,count):events.append(count);return b'x'*count
            def close(self):events.append('closed')
        class Resource:
            def joinpath(self,*args):return self
            def open(self,mode):return Stream()
        with patch('ashlar_host.local_physical_carriers.files',return_value=Resource()),self.assertRaises(ValueError):original_local_physical_carriers()
        self.assertEqual(events,[65537,'closed'])


if __name__=='__main__':unittest.main()
