from __future__ import annotations

import ast
import importlib.util
import unittest
from pathlib import Path


HELPER = Path(__file__).with_name('source_048_context_compatible_helper.py')
SPEC = importlib.util.spec_from_file_location('ashlar_context_compatible_helper', HELPER)
assert SPEC is not None and SPEC.loader is not None
helper = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(helper)


class ContextCompatibleHelperTests(unittest.TestCase):
    def test_registrar_view_uses_writable_transaction_and_source_reader_stays_read_only(self):
        self.assertEqual(helper.custody_access_mode(helper.REGISTRAR), 'read_write')
        self.assertEqual(helper.custody_access_mode(helper.READER), 'read_only')
        with self.assertRaises(ValueError):
            helper.custody_access_mode('unexpected-role')

    def test_mode_is_used_for_both_database_and_truss_transaction_adoption(self):
        tree = ast.parse(HELPER.read_text())
        calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)]
        mode_names = {
            node.func.id
            for node in calls
            if isinstance(node.func, ast.Name) and node.func.id == 'custody_access_mode'
        }
        self.assertIn('custody_access_mode', mode_names)
        attributes = {
            node.func.attr
            for node in calls
            if isinstance(node.func, ast.Attribute)
        }
        self.assertIn('catalog_view_in_transaction', attributes)
        adopt_calls = [
            node for node in calls
            if isinstance(node.func, ast.Attribute) and node.func.attr == 'adopt_transaction'
        ]
        self.assertTrue(any(
            keyword.arg == 'access_mode'
            and isinstance(keyword.value, ast.Name)
            and keyword.value.id == 'mode'
            for node in adopt_calls for keyword in node.keywords
        ))
        statements = [
            node.value
            for node in ast.walk(tree)
            if isinstance(node, ast.Constant) and isinstance(node.value, str)
        ]
        self.assertTrue(any('READ WRITE' in value for value in statements))
        self.assertTrue(any('READ ONLY' in value for value in statements))

    def test_view_refusal_still_fails_closed_and_reader_path_is_unchanged(self):
        source = HELPER.read_text()
        self.assertIn("raw_view=view.value.view.original_bytes if isinstance(view.value,CatalogViewAvailable) else b''", source)
        self.assertIn("tx=host.adopt_transaction(access_mode='read_only')", source)
        self.assertIn("conn.execute('SET TRANSACTION ISOLATION LEVEL READ COMMITTED READ ONLY')", source)


if __name__ == '__main__':
    unittest.main()
