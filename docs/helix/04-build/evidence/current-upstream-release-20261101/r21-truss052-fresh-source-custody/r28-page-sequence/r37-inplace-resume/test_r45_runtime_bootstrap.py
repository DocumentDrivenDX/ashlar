"""Focused effect-free R45 runtime and journal-admission checks."""
import ast
import importlib.util
import json
import os
from pathlib import Path
import unittest

HERE = Path(__file__).resolve().parent
WORKER = HERE / "r45-recovery-worker.py"
SCOPE = HERE / "r45-host-scope.json"

def load_worker():
    spec = importlib.util.spec_from_file_location("r45_runtime_worker", WORKER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

class R45BootstrapTests(unittest.TestCase):
    def test_native_environment_precedes_truss_import(self):
        native = next(node for node in ast.parse(WORKER.read_text()).body
                      if isinstance(node, ast.FunctionDef) and node.name == "native")
        lines = {
            name: min(n.lineno for n in ast.walk(native) if isinstance(n, ast.Call)
                      and isinstance(n.func, ast.Name) and n.func.id == name)
            for name in ("install_selected_runtime", "configure_selected_spark_environment")
        }
        truss_import_line = min(n.lineno for n in ast.walk(native)
                                if isinstance(n, ast.ImportFrom) and n.module == "truss")
        self.assertLess(lines["install_selected_runtime"], truss_import_line)
        self.assertLess(lines["configure_selected_spark_environment"], truss_import_line)

    def test_configured_runtime_environment_and_private_runtime_gate(self):
        worker = load_worker()
        scope = json.loads(SCOPE.read_bytes())
        base = worker.load_r42().load_base()
        names = ("JAVA_HOME", "SPARK_LOCAL_IP", "PYSPARK_PYTHON", "PYSPARK_DRIVER_PYTHON")
        previous = {name: os.environ.get(name) for name in names}
        previous_path = list(__import__("sys").path)
        try:
            worker.install_selected_runtime(scope)
            environment = worker.configure_selected_spark_environment(scope, base)
            from ashlar_host.truss_resume_cli import _CurrentTrussRuntime
            from ashlar_host.configured_local_runtime import _runtime
            from pathlib import Path
            runtime = scope["spark_runtime"]
            jars = [Path(item["path"]).resolve() for item in runtime["jars"]]
            config = _CurrentTrussRuntime(jars[0].parent, Path(runtime["java_home"]).resolve(),
                {item["name"]: item["sha256"] for item in runtime["jars"]},
                Path(scope["paths"]["output_root"]).resolve())
            selected = _runtime(config)
            self.assertEqual(selected, jars)
            self.assertEqual(environment["environment"]["JAVA_HOME"], runtime["java_home"])
            self.assertEqual(environment["environment"]["SPARK_LOCAL_IP"], "127.0.0.1")
        finally:
            __import__("sys").path[:] = previous_path
            for name, value in previous.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value

if __name__ == "__main__":
    unittest.main()
