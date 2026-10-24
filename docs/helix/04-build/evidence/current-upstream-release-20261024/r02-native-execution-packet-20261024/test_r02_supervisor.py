"""Inert supervisor phase tests; all operations are tiny Python no-ops."""
from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
import uuid
from unittest.mock import patch


SCRIPT = Path(__file__).with_name("r02_supervisor.py")
SPEC = importlib.util.spec_from_file_location("r02_supervisor", SCRIPT)
supervisor = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(supervisor)


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


class SupervisorPhaseTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="ashlar-r02-supervisor-test-", dir="/private/tmp")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.private = self.root / "attempts"
        self.private.mkdir(mode=0o700)
        self.original_attempt_parent = supervisor.DEFAULT_ATTEMPT_PARENT
        self.patch_base = patch.object(supervisor, "DEFAULT_ATTEMPT_PARENT", self.private)
        self.patch_pins = patch.object(supervisor, "verify_pins", lambda _manifest: None)
        self.patch_review = patch.object(supervisor, "review_gate", lambda path, _manifest, _review, _phase: sha(path.read_bytes()))
        self.patch_base.start()
        self.patch_pins.start()
        self.patch_review.start()
        self.addCleanup(self.patch_base.stop)
        self.addCleanup(self.patch_pins.stop)
        self.addCleanup(self.patch_review.stop)
        self.source_files = {"fixture": "a" * 64}
        self.vector = {
            "id": "fixture-publication",
            "manifestUuid": "11111111-1111-4111-8111-111111111111",
            "tables": [
                {"name": ["spark_catalog", "fixture", role],
                 "uuid": str(uuid.uuid5(uuid.NAMESPACE_URL, role)), "version": 1}
                for role in sorted(supervisor.REQUIRED_ROLES)
            ],
        }

    def write_manifest(self, name: str, value: dict) -> tuple[Path, Path]:
        attempt = Path(value["attemptRoot"])
        path = attempt / name
        path.write_text(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
        review = attempt / f"{name}.review.json"
        review.write_text("{}\n", encoding="utf-8")
        return path, review

    def manifest_base(self, phase: str, attempt_id: str) -> dict:
        attempt = self.private / f"ashlar-r02-{attempt_id}"
        attempt.mkdir(mode=0o700)
        (attempt / "receipts").mkdir(mode=0o700)
        (attempt / "invocations").mkdir(mode=0o700)
        return {
            "profile": "ashlar-r02-one-shot/0.1", "phase": phase,
            "attemptId": attempt_id, "sourceRevision": "fixture-revision",
            "attemptParent": str(self.private), "attemptRoot": str(attempt),
            "sourceFiles": self.source_files, "runtimeFiles": {},
            "limits": {"wallSeconds": 300, "privateBytes": supervisor.LIMIT_PRIVATE,
                       "receiptBytes": supervisor.LIMIT_RECEIPTS,
                       "operationOutputBytes": supervisor.LIMIT_COMMAND_OUTPUT},
        }

    @staticmethod
    def noop(case: str) -> dict:
        return {"kind": "noop", "case": case, "argv": [sys.executable, "-c", "pass"]}

    def make_prepublication(self) -> tuple[dict, Path]:
        manifest = self.manifest_base("prepublication", uuid.uuid4().hex)
        operations = [("initialize", "initialize"), ("batch-1", "publish"),
                      ("batch-2", "publish"), ("batch-3", "publish"),
                      ("batch-3-replay", "replay")]
        templates = []
        for case, kind in operations:
            path = Path(manifest["attemptRoot"]) / "invocations" / f"{case}.json"
            path.write_text("{}\n", encoding="utf-8")
            templates.append({"case": case, "kind": kind, "path": str(path),
                              "sha256": sha(path.read_bytes())})
        manifest["invocationTemplates"] = templates
        manifest["invocations"] = [item["path"] for item in templates]
        files = {}
        for relative in ("input/configured-typed-graph-source.json", "input/typed-graph-model.umf.json",
                         "input/typed-graph-source.jsonl", "input/typed-graph-binding.json",
                         "jars/delta-spark_4.2_2.13-4.4.1.jar", "jars/delta-storage-4.4.1.jar",
                         "jars/inventory.json"):
            path = Path(manifest["attemptRoot"]) / relative
            path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            path.write_bytes(b"fixture\n")
            files[relative] = sha(path.read_bytes())
        manifest["attemptFiles"] = files
        python = "/private/tmp/ashlar-release-20261021-m3rde5p2/venv/bin/python3.12"
        worker = str(Path(manifest["attemptRoot"]) / "r02_invocation_worker.py")
        manifest["operations"] = []
        for template in templates:
            argv = [python, "-I", "-B", worker, "--attempt", manifest["attemptRoot"],
                    "--case", template["case"], "--template", template["path"],
                    "--python", python, "--java-home", "/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home",
                    "--template-sha256", template["sha256"]]
            manifest["operations"].append({"case": template["case"], "kind": template["kind"],
                                            "templateSha256": template["sha256"], "argv": argv})
        manifest["publicationVectorPath"] = "vector.json"
        path, review = self.write_manifest("prepublication.json", manifest)
        vector_json = json.dumps(self.vector, sort_keys=True, separators=(",", ":"))
        def inert_run(_argv, cwd, _deadline, _output, _private, _receipts):
            (cwd / "vector.json").write_text(vector_json, encoding="utf-8")
        with patch.object(supervisor, "run_one", inert_run):
            supervisor.execute(path, review, "prepublication")
        attempt = self.private / f"ashlar-r02-{manifest['attemptId']}"
        return manifest, attempt / "one-shot-state.json"

    def continuation_manifest(self, prior_manifest: dict, prior_state_path: Path) -> dict:
        prior_raw = prior_state_path.read_bytes()
        prior_state = json.loads(prior_raw)
        vector_raw = json.dumps(self.vector, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
        vector_hash = sha(vector_raw)
        manifest = self.manifest_base("continuation", uuid.uuid4().hex)
        manifest.update({
            "publicationVector": self.vector,
            "publicationVectorSha256": vector_hash,
            "prepublicationState": {
                "path": str(prior_state_path), "sha256": sha(prior_raw),
                "manifestSha256": prior_state["manifestSha256"], "attemptId": prior_state["attemptId"],
            },
            "vectorReviewer": "Sol", "vectorDecision": "execute",
        })
        artifacts = []
        for case in ("backend03-original", "backend03-positive", "backend02-page1", "backend02-continuation"):
            binding_raw = json.dumps({"publication": self.vector}, sort_keys=True, separators=(",", ":"))
            request_raw = json.dumps({"target": {"bindingJson": binding_raw}}, separators=(",", ":")).encode()
            response_raw = json.dumps({"status": "compiled", "bindingSha256": sha(binding_raw.encode())}, separators=(",", ":")).encode()
            attempt = Path(manifest["attemptRoot"])
            request_path, response_path = attempt / f"{case}-request.json", attempt / f"{case}-response.json"
            request_path.write_bytes(request_raw)
            response_path.write_bytes(response_raw)
            artifacts.append({"case": case,
                              "request": {"path": str(request_path), "sha256": sha(request_raw)},
                              "response": {"path": str(response_path), "sha256": sha(response_raw)}})
        manifest["weftArtifacts"] = artifacts
        operations = [("singleton", "backend03-singleton"), ("query", "backend03-original"),
                      ("query", "backend03-positive"), ("query", "backend02-page1"),
                      ("query", "backend02-continuation")]
        manifest["operations"] = [self.noop(case) | {"kind": kind} for kind, case in operations]
        return manifest

    def test_prepublication_reaches_held_state_and_continuation_is_blocked(self):
        prepublication, prior_state = self.make_prepublication()
        held = json.loads(prior_state.read_text(encoding="utf-8"))
        self.assertEqual(held["status"], "complete-held-after-publication")
        continuation = self.continuation_manifest(prepublication, prior_state)
        path, review = self.write_manifest("continuation.json", continuation)
        with self.assertRaisesRegex(PermissionError, "live aliases.*Weft 0.4.53"):
            supervisor.execute(path, review, "continuation")

    def test_prepublication_rejects_missing_or_extra_steps(self):
        for operations in ([("initialize", "initialize")],
                           [("initialize", "initialize"), ("publish", "batch-1"),
                            ("publish", "batch-2"), ("publish", "batch-3"),
                            ("replay", "batch-3-replay"), ("replay", "duplicate")]):
            manifest = self.manifest_base("prepublication", uuid.uuid4().hex)
            manifest["operations"] = [self.noop(case) | {"kind": kind} for kind, case in operations]
            path, review = self.write_manifest(f"invalid-pre-{uuid.uuid4().hex}.json", manifest)
            with self.assertRaises(ValueError):
                supervisor.execute(path, review, "prepublication")

    def test_continuation_rejects_missing_or_extra_steps(self):
        prior_manifest, prior_state = self.make_prepublication()
        base = self.continuation_manifest(prior_manifest, prior_state)
        for operations in (base["operations"][:-1], base["operations"] + [self.noop("duplicate")]):
            manifest = dict(base)
            manifest["attemptId"] = uuid.uuid4().hex
            manifest["operations"] = operations
            path, review = self.write_manifest(f"invalid-cont-{uuid.uuid4().hex}.json", manifest)
            with self.assertRaises(PermissionError):
                supervisor.execute(path, review, "continuation")

    def test_continuation_is_refused_even_with_incomplete_publication_vector(self):
        prior_manifest, prior_state = self.make_prepublication()
        manifest = self.continuation_manifest(prior_manifest, prior_state)
        manifest["publicationVector"]["tables"].pop()
        vector_raw = json.dumps(manifest["publicationVector"], sort_keys=True,
                                separators=(",", ":"), ensure_ascii=False).encode()
        manifest["publicationVectorSha256"] = sha(vector_raw)
        manifest["attemptId"] = uuid.uuid4().hex
        path, review = self.write_manifest("invalid-roles.json", manifest)
        with self.assertRaisesRegex(PermissionError, "live aliases.*Weft 0.4.53"):
            supervisor.execute(path, review, "continuation")

    def test_attempt_parent_is_a_private_subdirectory_of_private_tmp(self):
        self.assertEqual(self.original_attempt_parent, Path("/private/tmp/ashlar-r02-attempts"))
        self.assertEqual(self.original_attempt_parent.parent, Path("/private/tmp"))
        self.assertEqual(self.private.stat().st_uid, __import__("os").getuid())
        self.assertEqual(self.private.stat().st_mode & 0o777, 0o700)


if __name__ == "__main__":
    unittest.main()
