import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from ashlar_host.pinned_delta_graphframes import ReviewedCanonicalActionDescriptor


ROOT = Path(__file__).resolve().parents[1]
SUPERVISOR_PATH = (ROOT / "docs/helix/04-build/evidence/"
                   "canonical-graphframes-holder-candidate-20261023/"
                   "native-supervisor/bounded_native_supervisor.py")
SPEC = importlib.util.spec_from_file_location("bounded_native_supervisor", SUPERVISOR_PATH)
supervisor = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(supervisor)


PASS_RECEIPT = {
    "status": "bounded-one-hop-passed",
    "canonicalFixtureQualification": "persisted-carrier-and-one-hop-verified",
    "sourceProgress": "absent", "protectedAck": "absent",
    "publication": "absent", "unityCatalog": "absent",
    "productionAuthority": "absent",
}


class BoundedNativeSupervisorTests(unittest.TestCase):
    def test_final_action_anchor_binds_exact_reviewed_inputs_without_authorizing_wrapper(self):
        evidence = (ROOT / "docs/helix/04-build/evidence/"
                    "canonical-graphframes-holder-candidate-20261023")
        packet_path = evidence / "native-supervisor/invocation-packet.json"
        packet = json.loads(packet_path.read_text())
        action = packet["action"]
        descriptor = ReviewedCanonicalActionDescriptor.from_reviewed_bytes(
            Path(action["manifestPath"]).read_bytes(),
            Path(action["modulePath"]).read_bytes(),
            Path(action["sourcePath"]).read_bytes(),
            Path(action["oraclePath"]).read_bytes(),
            review_anchor_path=Path(action["anchorPath"]),
            selected_review_anchor_sha256=Path(action["anchorDigestPath"]).read_text().strip())
        self.assertEqual(descriptor.sha256, action["descriptorSha256"])
        wrapper_path = packet_path.parent / packet["wrapper"]["path"]
        worker_path = packet_path.parent / packet["worker"]["path"]
        self.assertEqual(supervisor.sha256_file(wrapper_path), packet["wrapper"]["sha256"])
        self.assertEqual(supervisor.sha256_file(worker_path), packet["worker"]["sha256"])
        self.assertEqual(packet["reviewStatus"], "pending-independent-wrapper-review")
        self.assertEqual(packet["executionAuthorization"], "not-issued")
        self.assertIsNone(packet["runtime"]["pythonExecutable"])
        self.assertTrue(all(path is None for path in packet["runtime"]["runtimeJars"].values()))

    def test_pending_wrapper_packet_cannot_consume_native_attempt(self):
        evidence = (ROOT / "docs/helix/04-build/evidence/"
                    "canonical-graphframes-holder-candidate-20261023/native-supervisor")
        packet_path = evidence / "invocation-packet.json"
        with tempfile.TemporaryDirectory(prefix="ashlar-supervisor-auth-") as directory:
            auth_path = Path(directory) / "authorization.json"
            auth_path.write_text("{}")
            attempt_parent = Path(directory) / "attempts"
            attempt_parent.mkdir(mode=0o700)
            with self.assertRaisesRegex(supervisor.NativeAttemptRefused,
                                        "[Rr]eviewed wrapper packet"):
                supervisor.run_reviewed_native_attempt(
                    packet_path=packet_path,
                    selected_packet_sha256=supervisor.sha256_file(packet_path),
                    authorization_path=auth_path,
                    selected_authorization_sha256=supervisor.sha256_file(auth_path),
                    attempt_parent=attempt_parent)
            self.assertEqual(list(attempt_parent.iterdir()), [])

    def test_authorized_parent_binding_refuses_an_alternate_fresh_parent(self):
        with tempfile.TemporaryDirectory(prefix="ashlar-parent-binding-") as directory:
            root = Path(directory)
            expected = root / "authorized"
            alternate = root / "alternate"
            expected.mkdir(mode=0o700)
            alternate.mkdir(mode=0o700)
            packet_path, authorization_path = self._authorized_admission_bundle(
                root, expected.resolve())
            selected_packet = supervisor.sha256_file(packet_path)
            selected_authorization = supervisor.sha256_file(authorization_path)
            admitted = supervisor._verify_packet_and_authorization(
                packet_path, selected_packet, authorization_path, selected_authorization)
            self.assertEqual(admitted[4]["attemptParent"], str(expected.resolve()))
            with self.assertRaisesRegex(supervisor.NativeAttemptRefused, "immutable parent"):
                supervisor.run_reviewed_native_attempt(
                    packet_path=packet_path, selected_packet_sha256=selected_packet,
                    authorization_path=authorization_path,
                    selected_authorization_sha256=selected_authorization,
                    attempt_parent=alternate)
            self.assertEqual(list(alternate.iterdir()), [])

    def _authorized_admission_bundle(self, root, attempt_parent):
        root = root.resolve()
        wrapper_path = SUPERVISOR_PATH.resolve()
        worker_path = wrapper_path.parent / "canonical_graphframes_worker.py"
        packet = {
            "reviewStatus": "independently-reviewed",
            "executionStatus": "authorized-for-one-attempt",
            "wrapper": {"path": wrapper_path.name,
                        "sha256": supervisor.sha256_file(wrapper_path)},
            "worker": {"path": worker_path.name,
                       "sha256": supervisor.sha256_file(worker_path)},
            "runtime": {"master": "local[2]"},
            "hardBounds": {
                "attempts": 1, "totalSeconds": supervisor.MAX_ATTEMPT_SECONDS,
                "retainedPlusAttemptBytes": supervisor.MAX_AGGREGATE_BYTES,
                "diagnosticsBytes": supervisor.MAX_DIAGNOSTIC_BYTES,
                "freshRootMode": "0700",
                "processGroup": "one child session; terminate/kill group and reap leader",
                "sparkScratch": "entirely under the fresh accounted attempt root",
            },
        }
        packet_path = root / "admission-packet.json"
        packet_path.write_text(json.dumps(packet, sort_keys=True))
        packet_digest = supervisor.sha256_file(packet_path)
        authorization = {
            "profile": "ashlar-canonical-graphframes-execution-authorization/0.1",
            "status": "authorized-for-one-attempt",
            "invocationPacketSha256": packet_digest,
            "attempts": 1,
            "maximumSeconds": supervisor.MAX_ATTEMPT_SECONDS,
            "maximumAggregateBytes": supervisor.MAX_AGGREGATE_BYTES,
            "maximumDiagnosticsBytes": supervisor.MAX_DIAGNOSTIC_BYTES,
            "master": "local[2]",
            "attemptParent": str(attempt_parent),
        }
        authorization_path = root / "authorization.json"
        authorization_path.write_text(json.dumps(authorization, sort_keys=True))
        return packet_path, authorization_path

    def test_failed_close_invalidates_staged_and_final_success_receipts(self):
        with tempfile.TemporaryDirectory(prefix="ashlar-receipt-invalidation-") as directory:
            root = Path(directory)
            final_receipt = root / "supervisor-receipt.json"
            staged_receipt = root / ".supervisor-receipt.pending"
            final_receipt.write_text('{"status":"bounded-one-hop-passed"}')
            staged_receipt.write_text('{"status":"bounded-one-hop-passed"}')
            supervisor._discard_success_receipts(root)
            self.assertFalse(final_receipt.exists())
            self.assertFalse(staged_receipt.exists())

    def _run(self, code, *, limit=2, output=b""):
        temporary = tempfile.TemporaryDirectory(prefix="ashlar-supervisor-test-")
        root = Path(temporary.name)
        os.chmod(root, 0o700)
        result_path = root / "worker-result.json"
        diagnostic_path = root / "diagnostics.log"
        encoded = json.dumps(PASS_RECEIPT).encode()
        body = (
            "from pathlib import Path; import json, os, sys, time\n"
            f"Path(sys.argv[1]).write_bytes({encoded!r})\n"
            + code
        )
        argv = [sys.executable, "-c", body, str(result_path)]
        with patch.object(supervisor, "MAX_ATTEMPT_SECONDS", max(limit, 1)):
            return (temporary, supervisor._collect_child(
                argv, cwd=root, env=dict(os.environ), root=root,
                retained_bytes=0, timeout_seconds=limit,
                diagnostics_path=diagnostic_path, result_path=result_path))

    def test_one_child_group_returns_pass_only_with_complete_receipt(self):
        temporary, result = self._run("os.write(1, b'worker started\\n')\n")
        try:
            self.assertEqual(result["status"], "bounded-one-hop-passed")
            self.assertEqual(result["receipt"], PASS_RECEIPT)
            self.assertEqual(result["executionAttestation"]["returnCode"], 0)
            self.assertEqual(result["executionAttestation"]["diagnosticsBytes"], 15)
        finally:
            temporary.cleanup()

    def test_timeout_kills_and_reaps_the_child_group(self):
        temporary = tempfile.TemporaryDirectory(prefix="ashlar-supervisor-timeout-")
        root = Path(temporary.name)
        os.chmod(root, 0o700)
        result_path = root / "worker-result.json"
        with patch.object(supervisor, "MAX_ATTEMPT_SECONDS", 2):
            with self.assertRaisesRegex(supervisor.NativeAttemptRefused, "deadline"):
                supervisor._collect_child(
                    [sys.executable, "-c", "import time; time.sleep(5)"],
                    cwd=root, env=dict(os.environ), root=root, retained_bytes=0,
                    timeout_seconds=0.1,
                    diagnostics_path=root / "diagnostics.log", result_path=result_path)
        self.assertFalse(result_path.exists())
        temporary.cleanup()

    def test_diagnostic_overflow_is_capped_then_process_group_is_killed(self):
        temporary = tempfile.TemporaryDirectory(prefix="ashlar-supervisor-diagnostics-")
        root = Path(temporary.name)
        os.chmod(root, 0o700)
        with patch.object(supervisor, "MAX_ATTEMPT_SECONDS", 2), \
             patch.object(supervisor, "MAX_DIAGNOSTIC_BYTES", 1024):
            with self.assertRaisesRegex(supervisor.NativeAttemptRefused, "diagnostics"):
                supervisor._collect_child(
                    [sys.executable, "-c",
                     "import os,time; os.write(2,b'x'*8192); time.sleep(5)"],
                    cwd=root, env=dict(os.environ), root=root, retained_bytes=0,
                    timeout_seconds=2,
                    diagnostics_path=root / "diagnostics.log",
                    result_path=root / "worker-result.json")
        self.assertEqual((root / "diagnostics.log").stat().st_size, 1024)
        temporary.cleanup()

    def test_aggregate_retained_attempt_limit_refuses_before_spawn(self):
        temporary = tempfile.TemporaryDirectory(prefix="ashlar-supervisor-bytes-")
        root = Path(temporary.name)
        os.chmod(root, 0o700)
        with self.assertRaisesRegex(supervisor.NativeAttemptRefused, "1 GiB"):
            supervisor._collect_child(
                [sys.executable, "-c", "raise SystemExit(99)"],
                cwd=root, env=dict(os.environ), root=root,
                retained_bytes=supervisor.MAX_AGGREGATE_BYTES + 1,
                timeout_seconds=2,
                diagnostics_path=root / "diagnostics.log",
                result_path=root / "worker-result.json")
        self.assertFalse((root / "diagnostics.log").exists())
        temporary.cleanup()

    def test_missing_or_wrong_authority_fields_suppress_pass(self):
        temporary = tempfile.TemporaryDirectory(prefix="ashlar-supervisor-authority-")
        root = Path(temporary.name)
        os.chmod(root, 0o700)
        result_path = root / "worker-result.json"
        bad_receipt = dict(PASS_RECEIPT, protectedAck="present")
        body = ("from pathlib import Path; import sys; "
                f"Path(sys.argv[1]).write_text({json.dumps(json.dumps(bad_receipt))!r})")
        with self.assertRaisesRegex(supervisor.NativeAttemptRefused, "absent-authority"):
            supervisor._collect_child(
                [sys.executable, "-c", body, str(result_path)],
                cwd=root, env=dict(os.environ), root=root, retained_bytes=0,
                timeout_seconds=2,
                diagnostics_path=root / "diagnostics.log", result_path=result_path)
        temporary.cleanup()

    def _assert_spawned_child_reaped_after_injected_failure(self, failure_point):
        temporary = tempfile.TemporaryDirectory(prefix="ashlar-supervisor-injected-failure-")
        root = Path(temporary.name)
        os.chmod(root, 0o700)
        pid_path = root / "child.pid"
        code = "import time; time.sleep(10)"
        real_popen = supervisor.subprocess.Popen
        spawned = []

        def tracked_popen(*args, **kwargs):
            process = real_popen(*args, **kwargs)
            spawned.append(process)
            return process
        original_open = Path.open
        diagnostic = root / "diagnostics.log"

        def injected_open(path, *args, **kwargs):
            if failure_point == "diagnostic-open" and path == diagnostic:
                __import__("time").sleep(0.1)
                raise OSError("injected diagnostic open failure")
            return original_open(path, *args, **kwargs)

        selector_patch = patch.object(
            supervisor.selectors.DefaultSelector, "select",
            side_effect=lambda *_args, **_kwargs: (
                __import__("time").sleep(0.1),
                (_ for _ in ()).throw(OSError("injected selector failure")))[1])
        with patch.object(Path, "open", injected_open), \
             patch.object(supervisor.subprocess, "Popen", tracked_popen):
            if failure_point == "selector":
                selector_patch.start()
            try:
                with self.assertRaises(OSError):
                    supervisor._collect_child(
                        [sys.executable, "-c", code], cwd=root, env=dict(os.environ),
                        root=root, retained_bytes=0, timeout_seconds=2,
                        diagnostics_path=diagnostic, result_path=root / "worker-result.json")
            finally:
                if failure_point == "selector":
                    selector_patch.stop()
        self.assertEqual(len(spawned), 1)
        self.assertIsNotNone(spawned[0].poll())
        spawned[0].wait()
        temporary.cleanup()

    def test_diagnostic_open_failure_kills_and_reaps_child(self):
        self._assert_spawned_child_reaped_after_injected_failure("diagnostic-open")

    def test_selector_failure_kills_and_reaps_child(self):
        self._assert_spawned_child_reaped_after_injected_failure("selector")


if __name__ == "__main__":
    unittest.main()
