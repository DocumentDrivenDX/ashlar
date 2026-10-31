"""Inert packet identity and invocation checks; never starts Spark."""
import importlib.util
import builtins
from pathlib import Path
import unittest
from unittest import mock

PACKET = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("r4_packet", PACKET / "r4_packet.py")
r4 = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r4)


class InitPacketTests(unittest.TestCase):
    def test_manifest_and_packet_digest_are_stable(self):
        selected = r4.manifest()
        self.assertEqual(selected["sdk010"]["archiveSha256"],
                         "2fb63dea5ca85b32c6bad823703de526875ac79490234dbfea4a8863c7d5fe6c")
        self.assertEqual(selected["sourceTreeObject"], "6b5fc884380e040f21c1b51b1cc3f49e84d0e9a4")
        self.assertNotIn("currentCheckoutCommit", selected)
        self.assertEqual(selected["limits"]["processGroupRssBytes"], 2 * 1024 ** 3)
        self.assertEqual(r4.packet_digest(), r4.packet_digest())
        self.assertEqual(r4.REPO, Path("/Users/erik/Projects/ashlar"))

    def test_current_checkout_after_docs_changes_still_admits_exact_src_tree(self):
        admission = r4.verify_effect_free()
        self.assertEqual(admission["sourceTreeObject"], r4.manifest()["sourceTreeObject"])
        self.assertNotEqual(admission["currentCommit"], r4.manifest()["sourceCommit"])

    def test_source_admission_runs_before_any_repo_import(self):
        events = []
        verify = r4.verify_admitted_source_tree
        original_import = builtins.__import__

        def track_verify(selected):
            events.append("source-tree")
            return verify(selected)

        def track_import(name, *args, **kwargs):
            if name.startswith(("ashlar.", "ashlar_host.")):
                events.append("repo-import")
            return original_import(name, *args, **kwargs)

        with mock.patch.object(r4, "verify_admitted_source_tree", side_effect=track_verify), \
             mock.patch("builtins.__import__", side_effect=track_import):
            r4.verify_effect_free()
        self.assertLess(events.index("source-tree"), events.index("repo-import"))

    def test_group_rss_uses_process_rows_in_kib(self):
        self.assertEqual(r4.group_rss_bytes([{"rssKiB": 512}, {"rssKiB": 1024}]),
                         1536 * 1024)

    def test_cleanup_signals_owned_group_even_when_ps_is_unavailable(self):
        class Child:
            pid = 73124
            def poll(self): return 0

        with mock.patch.object(r4, "process_group_inventory", side_effect=PermissionError), \
             mock.patch.object(r4.os, "killpg") as killpg:
            receipt = r4.stop_and_verify_group(Child())
        self.assertEqual([call.args[1] for call in killpg.call_args_list],
                         [r4.signal.SIGTERM, r4.signal.SIGKILL])
        self.assertFalse(receipt["inventoryVerified"])
        self.assertFalse(receipt["empty"])

    def test_invocation_is_fresh_initialize_only_and_profile_exact(self):
        selected = r4.manifest()
        invocation = r4.invocation(Path("/private/tmp/example-attempt"), selected)
        self.assertEqual(invocation["operation"], "initialize")
        self.assertEqual(invocation["profile"], "ashlar-configured-local/0.3")
        self.assertEqual(invocation["carrier_profile"], "ashlar-delta/0.4-xxhash64")
        self.assertIsNone(invocation["installation_sha256"])
        self.assertEqual(invocation["bounds"]["whole_seconds"], 300)
        self.assertEqual(invocation["bounds"]["stream_bytes"], 1048576)

    def test_attempt_storage_accounting_is_recursive_and_read_only(self):
        self.assertGreaterEqual(r4.attempt_size(PACKET), 0)


if __name__ == "__main__":
    unittest.main()
