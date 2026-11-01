"""Focused effect-free R44 runtime admission regression checks."""
import ast
import importlib.util
import json
from pathlib import Path
import unittest

HERE = Path(__file__).resolve().parent
WORKER = HERE / "r44-recovery-worker.py"
SCOPE = HERE / "r44-host-scope.json"


def load_worker():
    spec = importlib.util.spec_from_file_location("r44_runtime_worker", WORKER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class RuntimeBootstrapTests(unittest.TestCase):
    def test_native_bootstrap_precedes_first_truss_import(self):
        tree = ast.parse(WORKER.read_text())
        native = next(node for node in tree.body
                      if isinstance(node, ast.FunctionDef) and node.name == "native")
        call_line = min(node.lineno for node in ast.walk(native)
                        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                        and node.func.id == "install_selected_runtime")
        truss_import_line = min(node.lineno for node in ast.walk(native)
                                if isinstance(node, ast.ImportFrom) and node.module == "truss")
        self.assertLess(call_line, truss_import_line)

    def test_selected_wheel_and_local_extras_import_before_local_postgres(self):
        worker = load_worker()
        scope = json.loads(SCOPE.read_bytes())
        receipt = worker.install_selected_runtime(scope)
        self.assertEqual(receipt["truss_version"], "0.0.52")
        self.assertEqual(receipt["wheel_sha256"], scope["runtime_candidate"]["wheel_sha256"])
        self.assertTrue(receipt["truss_origin"].startswith(receipt["overlay"] + "/"))
        from truss import LocalPostgres
        self.assertIn("data_directory", __import__("inspect").signature(LocalPostgres).parameters)


if __name__ == "__main__":
    unittest.main()
