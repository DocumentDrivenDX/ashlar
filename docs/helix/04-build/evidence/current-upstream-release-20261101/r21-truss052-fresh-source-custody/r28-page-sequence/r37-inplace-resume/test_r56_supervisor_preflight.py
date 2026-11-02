import contextlib, importlib.util, io, json, pathlib, unittest
from unittest.mock import patch

HERE = pathlib.Path(__file__).resolve().parent
SUPERVISOR = HERE / "r56_truss052_supervisor.py"
SCOPE = HERE / "r56-host-scope.json"
WORKER = HERE / "r56-recovery-worker.py"

spec = importlib.util.spec_from_file_location("r56_supervisor_test", SUPERVISOR)
module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)

class R56SupervisorPreflightTests(unittest.TestCase):
    def test_exact_r56_scope_and_worker_preflight_without_native_effects(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            with patch("sys.argv", [str(SUPERVISOR), "--scope", str(SCOPE), "--worker", str(WORKER), "--preflight-only"]):
                code = module.main()
        self.assertEqual(code, 0, output.getvalue())
        result = json.loads(output.getvalue())
        self.assertEqual(result["profile"], "ashlar-r56-truss052-committed-page-supervisor/0.1")
        self.assertEqual(result["status"], "preflight_ok")
        self.assertIs(result["native_effects"], False)
        self.assertEqual(result["worker_preflight"]["status"], "passed_effect_free_inplace_preflight")
        self.assertIs(result["worker_preflight"]["source_ack_attempted"], False)

if __name__ == "__main__": unittest.main()
