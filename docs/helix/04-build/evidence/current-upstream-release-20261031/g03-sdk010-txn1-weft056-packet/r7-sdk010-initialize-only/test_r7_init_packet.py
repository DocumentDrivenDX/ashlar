"""Inert packet identity and invocation checks; never starts Spark."""
import importlib.util
import builtins
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

PACKET = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("r7_packet", PACKET / "r7_packet.py")
r7 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r7)


class InitPacketTests(unittest.TestCase):
    def test_manifest_and_packet_digest_are_stable(self):
        selected = r7.manifest()
        self.assertEqual(selected["sdk010"]["archiveSha256"],
                         "2fb63dea5ca85b32c6bad823703de526875ac79490234dbfea4a8863c7d5fe6c")
        self.assertEqual(selected["sourceTreeObject"], "6b5fc884380e040f21c1b51b1cc3f49e84d0e9a4")
        self.assertNotIn("currentCheckoutCommit", selected)
        self.assertEqual(selected["limits"]["processGroupRssBytes"], 2 * 1024 ** 3)
        self.assertEqual(selected["producer"]["source"],
                         "/private/tmp/ashlar-g03-pinned-umf-producer-sparse-sdk010")
        self.assertEqual(selected["producer"]["sparseCheckout"],
                         ["native", "package.json", "spec", "src", "tsconfig.json"])
        self.assertEqual(r7.packet_digest(), r7.packet_digest())
        self.assertEqual(r7.REPO, PACKET.parents[6])

    def test_sparse_umf_checkout_has_exact_commit_clean_status_and_dependency_link(self):
        custody = r7.verify_sparse_umf_source(r7.manifest())
        self.assertEqual(custody["commit"], r7.manifest()["producer"]["sourceCommit"])
        self.assertEqual(custody["tree"], r7.manifest()["producer"]["sourceTree"])
        self.assertTrue(custody["clean"])
        self.assertTrue(custody["sparseIndex"])
        self.assertEqual(custody["nodeModulesTarget"],
                         r7.manifest()["producer"]["nodeModulesTarget"])

    def test_current_checkout_after_docs_changes_still_admits_exact_src_tree(self):
        admission = r7.verify_effect_free()
        self.assertEqual(admission["sourceTreeObject"], r7.manifest()["sourceTreeObject"])
        self.assertNotEqual(admission["currentCommit"], r7.manifest()["sourceCommit"])

    def test_source_admission_runs_before_any_repo_import(self):
        events = []
        verify = r7.verify_admitted_source_tree
        original_import = builtins.__import__

        def track_verify(selected):
            events.append("source-tree")
            return verify(selected)

        def track_import(name, *args, **kwargs):
            if name.startswith(("ashlar.", "ashlar_host.")):
                events.append("repo-import")
            return original_import(name, *args, **kwargs)

        with mock.patch.object(r7, "verify_admitted_source_tree", side_effect=track_verify), \
             mock.patch("builtins.__import__", side_effect=track_import):
            r7.verify_effect_free()
        self.assertLess(events.index("source-tree"), events.index("repo-import"))

    def test_group_rss_uses_process_rows_in_kib(self):
        self.assertEqual(r7.group_rss_bytes([{"rssKiB": 512}, {"rssKiB": 1024}]),
                         1536 * 1024)

    def test_cleanup_signals_owned_group_even_when_ps_is_unavailable(self):
        class Child:
            pid = 73124
            def poll(self): return 0

        with mock.patch.object(r7, "process_group_inventory", side_effect=PermissionError), \
             mock.patch.object(r7.os, "killpg") as killpg:
            receipt = r7.stop_and_verify_group(Child())
        self.assertEqual([call.args[1] for call in killpg.call_args_list],
                         [r7.signal.SIGTERM, r7.signal.SIGKILL])
        self.assertFalse(receipt["inventoryVerified"])
        self.assertFalse(receipt["empty"])

    def test_invocation_is_fresh_initialize_only_and_profile_exact(self):
        selected = r7.manifest()
        invocation = r7.invocation(Path("/private/tmp/example-attempt"), selected)
        self.assertEqual(invocation["operation"], "initialize")
        self.assertEqual(invocation["profile"], "ashlar-configured-local/0.3")
        self.assertEqual(invocation["carrier_profile"], "ashlar-delta/0.4-xxhash64")
        self.assertIsNone(invocation["installation_sha256"])
        self.assertEqual(invocation["producer"]["source"],
                         r7.manifest()["producer"]["source"])
        self.assertEqual(invocation["bounds"]["whole_seconds"], 300)
        self.assertEqual(invocation["bounds"]["stream_bytes"], 1048576)

    def test_attempt_storage_accounting_is_recursive_and_read_only(self):
        self.assertGreaterEqual(r7.attempt_size(PACKET), 0)

    def test_worker_imports_packet_modules_in_isolated_child(self):
        child = subprocess.run(
            [r7.manifest()["runtime"]["python"], "-I", "-B",
             str(PACKET / "r7_init_worker.py"), "--help"],
            capture_output=True, text=True, timeout=15)
        self.assertEqual(child.returncode, 0, child.stderr)
        self.assertIn("--prepared-sha256", child.stdout)

    def test_real_generated_invocation_parses_in_isolated_worker_child(self):
        attempt = Path("/private/tmp/ashlar-g03-sdk010-r7-parser-preflight-attempt")
        raw = r7.canonical(r7.invocation(attempt, r7.manifest()))
        with tempfile.TemporaryDirectory() as directory:
            invocation_path = Path(directory) / "initialize-invocation.json"
            invocation_path.write_bytes(raw)
            child = subprocess.run(
                [r7.manifest()["runtime"]["python"], "-I", "-B",
                 str(PACKET / "r7_init_worker.py"), "--preflight-invocation",
                 str(invocation_path)], capture_output=True, text=True, timeout=20)
        self.assertEqual(child.returncode, 0, child.stderr)
        parsed = json.loads(child.stdout)
        self.assertEqual(parsed, {
            "profile": "ashlar-g03-sdk010-r7-invocation-preflight/0.1",
            "operation": "initialize", "carrierProfile": "ashlar-delta/0.4-xxhash64",
            "producerTimeoutSeconds": 30,
        })


if __name__ == "__main__":
    unittest.main()
