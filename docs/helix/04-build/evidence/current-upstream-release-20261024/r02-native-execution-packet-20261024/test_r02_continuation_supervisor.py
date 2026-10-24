"""Effect-free R02 continuation packet binding checks; no Spark or SQL."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

SCRIPT = Path(__file__).with_name("r02_continuation_supervisor.py")
SPEC = importlib.util.spec_from_file_location("r02_continuation_supervisor", SCRIPT)
supervisor = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(supervisor)
WORKER = Path(__file__).with_name("r02_live_alias_worker.py")
WORKER_SPEC = importlib.util.spec_from_file_location("r02_live_alias_worker", WORKER)
worker = importlib.util.module_from_spec(WORKER_SPEC)
assert WORKER_SPEC.loader is not None
WORKER_SPEC.loader.exec_module(worker)


class ContinuationPacketTests(unittest.TestCase):
    def test_prior_invocation_bytes_are_checked_before_json_use(self):
        with tempfile.TemporaryDirectory(prefix="ashlar-r02-invocation-", dir="/private/tmp") as tmp:
            path = Path(tmp) / "invocation.json"
            raw = b'{"operation":"publish","ordinal":1}\n'
            path.write_bytes(raw)
            self.assertEqual(worker.read_pinned_json(path, supervisor.sha(raw)),
                             {"operation": "publish", "ordinal": 1})
            path.write_bytes(raw + b" ")
            with self.assertRaisesRegex(ValueError, "bytes changed"):
                worker.read_pinned_json(path, supervisor.sha(raw))

    def test_effect_free_prepare_binds_prior_state_vector_runtime_and_review_stages(self):
        with tempfile.TemporaryDirectory(prefix="ashlar-r02-cont-test-", dir="/private/tmp") as tmp:
            path, digest = supervisor.prepare(Path(tmp))
            manifest = supervisor.verify_manifest(path)
            self.assertEqual(supervisor.digest(path), digest)
            self.assertEqual(manifest["sourceRevision"], supervisor.RUNTIME_SOURCE_REVISION)
            self.assertEqual(manifest["priorPublication"]["sourceRevision"],
                             "5298af9dd82c84e4929c0d7a3e286206f5037612")
            self.assertEqual(manifest["priorPublication"]["vectorSha256"],
                             "78e0ca4100faf6340b2beb05a659ba7b0c06504926734bdef089e8e9f34043eb")
            self.assertEqual(manifest["runtime"]["weftIndexSourceSha256"], supervisor.INDEX_SHA256)
            self.assertEqual(supervisor.digest(Path(manifest["runtime"]["weftIndex"])),
                             manifest["runtime"]["weftIndexSha256"])
            self.assertEqual(manifest["runtime"]["weftVersion"], "0.4.53")
            self.assertEqual(manifest["runtime"]["sparkVersion"], "4.2.0")
            self.assertEqual(manifest["runtime"]["deltaVersion"], "4.4.1")
            self.assertEqual(manifest["invocation"]["ordinal"], 1)
            self.assertEqual(manifest["invocation"]["scopeCount"], 1)
            self.assertEqual(manifest["invocation"]["aliasScopeCount"], 1)
            self.assertEqual(manifest["reviewStages"], ["pre-sql", "post-page1"])
            self.assertEqual(sorted(p.name for p in path.parent.iterdir()),
                             [path.name, "weft053-index.json"])

    def test_stale_runtime_or_artifact_hash_refuses_manifest(self):
        with tempfile.TemporaryDirectory(prefix="ashlar-r02-cont-stale-", dir="/private/tmp") as tmp:
            path, _ = supervisor.prepare(Path(tmp))
            value = json.loads(path.read_bytes())
            value["runtime"]["weftIndexSha256"] = "0" * 64
            path.write_bytes(supervisor.canonical(value))
            with self.assertRaisesRegex(ValueError, "stale"):
                supervisor.verify_manifest(path)

    def test_supervisor_review_is_bound_to_exact_manifest_and_stage_contract(self):
        with tempfile.TemporaryDirectory(prefix="ashlar-r02-cont-review-", dir="/private/tmp") as tmp:
            path, _ = supervisor.prepare(Path(tmp))
            manifest = supervisor.verify_manifest(path)
            expected = {"profile": "ashlar-r02-continuation-supervisor-review/0.1", "decision": "execute",
                "reviewer": "Sol", "attemptId": manifest["attemptId"],
                "manifestSha256": supervisor.digest(path), "sourceRevision": manifest["sourceRevision"],
                "priorStateSha256": manifest["priorPublication"]["stateSha256"],
                "publicationVectorSha256": manifest["priorPublication"]["vectorSha256"],
                "runtimeSha256": supervisor.sha(supervisor.canonical(manifest["runtime"])),
                "packetFilesSha256": supervisor.sha(supervisor.canonical(manifest["packetFiles"]))}
            review = path.parent / "supervisor-review.json"
            review.write_bytes(supervisor.canonical(expected))
            supervisor.review_receipt(review, path, manifest)
            expected["publicationVectorSha256"] = "0" * 64
            review.write_bytes(supervisor.canonical(expected))
            with self.assertRaisesRegex(PermissionError, "does not authorize"):
                supervisor.review_receipt(review, path, manifest)


if __name__ == "__main__":
    unittest.main()
