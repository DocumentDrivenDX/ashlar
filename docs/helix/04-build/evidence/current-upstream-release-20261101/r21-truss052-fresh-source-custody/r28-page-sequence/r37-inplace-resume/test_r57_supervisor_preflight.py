import contextlib, importlib.util, io, json, pathlib, unittest
from unittest.mock import patch

HERE = pathlib.Path(__file__).resolve().parent
SUPERVISOR = HERE / "r57_truss052_supervisor.py"
SCOPE = HERE / "r57-host-scope.json"
WORKER = HERE / "r57-recovery-worker.py"

spec = importlib.util.spec_from_file_location("r57_supervisor_test", SUPERVISOR)
module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)

class R57SupervisorPreflightTests(unittest.TestCase):
    def test_exact_r57_scope_and_worker_preflight_without_native_effects(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            with patch("sys.argv", [str(SUPERVISOR), "--scope", str(SCOPE), "--worker", str(WORKER), "--preflight-only"]):
                code = module.main()
        self.assertEqual(code, 0, output.getvalue())
        result = json.loads(output.getvalue())
        self.assertEqual(result["profile"], "ashlar-r57-truss052-committed-page-supervisor/0.1")
        self.assertEqual(result["status"], "preflight_ok")
        self.assertIs(result["native_effects"], False)
        self.assertEqual(result["worker_preflight"]["status"], "passed_effect_free_inplace_preflight")
        parser = result["worker_preflight"]["real_config_parser"]
        self.assertEqual(parser["status"], "passed_real_resume_config_parser")
        self.assertEqual(parser["intent_selector_keys"], ["path", "sha256"])
        self.assertEqual(parser["request_digest"], "678967ac3deeafa90b514ae8a5a1b05f78b0b32e5f89135f4c67205fede001ea")
        self.assertIs(parser["native_effects"], False)
        self.assertIs(result["worker_preflight"]["source_ack_attempted"], False)

if __name__ == "__main__": unittest.main()
