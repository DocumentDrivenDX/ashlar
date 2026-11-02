import contextlib, importlib.util, io, json, pathlib, unittest
from unittest.mock import patch

HERE = pathlib.Path(__file__).resolve().parent
SUPERVISOR = HERE / "r58_truss052_supervisor.py"
SCOPE = HERE / "r58-host-scope.json"
WORKER = HERE / "r58-recovery-worker.py"

spec = importlib.util.spec_from_file_location("r58_supervisor_test", SUPERVISOR)
module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)

class R58SupervisorPreflightTests(unittest.TestCase):
    def test_exact_r58_scope_and_worker_preflight_without_native_effects(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            with patch("sys.argv", [str(SUPERVISOR), "--scope", str(SCOPE), "--worker", str(WORKER), "--preflight-only"]):
                code = module.main()
        self.assertEqual(code, 0, output.getvalue())
        result = json.loads(output.getvalue())
        self.assertEqual(result["profile"], "ashlar-r58-truss052-committed-page-supervisor/0.1")
        self.assertEqual(result["status"], "preflight_ok")
        self.assertIs(result["native_effects"], False)
        scope = json.loads(SCOPE.read_text())
        binding = scope["current_source_state_binding"]
        self.assertEqual(binding["status"], "stopped_after_R57_and_failed_read_only_source_probe")
        self.assertEqual(binding["checkpoint_lsn"], "0/1F692F8")
        self.assertEqual(binding["next_xid"], "0:858")
        self.assertEqual(result["worker_preflight"]["status"], "passed_effect_free_inplace_preflight")
        helper_inventory = result["worker_preflight"]["native_helpers_preflight"]
        matcher = helper_inventory["page_matcher"]
        self.assertEqual(matcher["status"], "loaded_scope_bound_matcher")
        self.assertEqual(pathlib.Path(matcher["path"]), pathlib.Path(json.loads(SCOPE.read_text())["page_match_helper"]["path"]))
        self.assertEqual(matcher["required_callables"], [
            "verify_publication_selection", "validate_public_page", "validate_seed_property_bindings"])
        self.assertEqual({item["role"] for item in helper_inventory["files"]}, {
            "R37 public page helpers", "R36 base source/catalog helpers",
            "R41 report property projection", "committed R55 page matcher"})
        self.assertEqual({item["module"] for item in helper_inventory["static_imports"]}, {
            "ashlar_r58_committed_page_match", "ashlar_r37_page_helpers",
            "ashlar_r41_report_projection", "ashlar_r36_helpers"})
        custody = result["worker_preflight"]["resume_applied_custody"]
        self.assertEqual(custody["source_plan_count"], 1)
        self.assertEqual(custody["applied_artifact_count"], 1)
        self.assertEqual(custody["publication_id"], scope["predecessor"]["publication_id"])
        self.assertEqual(custody["continued_source_plan_count"], 0)
        self.assertEqual(custody["continued_applied_artifact_count"], 0)
        parser = result["worker_preflight"]["real_config_parser"]
        self.assertEqual(parser["status"], "passed_real_resume_config_parser")
        self.assertEqual(parser["intent_selector_keys"], ["path", "sha256"])
        self.assertEqual(set(parser["native_config_fields_exercised"]), {
            "connection_uri", "catalog_connection_uri", "publication_directory", "jars",
            "publication_intent", "destination_id", "carrier_profile", "runtime_profile",
            "archived_acceptance_files"})
        self.assertEqual(parser["request_digest"], scope["predecessor"]["request_digest"])
        self.assertEqual(parser["resume_intent_path"], scope["predecessor"]["publication_intent"]["path"])
        self.assertEqual(parser["resume_intent_sha256"], scope["predecessor"]["publication_intent"]["sha256"])
        self.assertNotEqual(parser["resume_intent_path"], scope["retained_r55"]["publication_intent_path"])
        self.assertEqual(parser["truss_version"], "0.0.52")
        self.assertEqual(parser["local_extra_versions"], {
            "pgserver-pg18": "18.6.1", "fasteners": "0.20", "platformdirs": "4.12.4", "psutil": "7.2.2"})
        self.assertEqual(parser["truss_wheel_sha256"], "00d8b11abc83d9c84fa131ae6f3eef23ecad9d321d4b04a13641ed31c77e9747")
        self.assertIs(parser["native_effects"], False)
        self.assertIs(result["worker_preflight"]["source_ack_attempted"], False)

if __name__ == "__main__": unittest.main()
