"""Focused effect-free R02 packet generation and vector extraction checks."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
import uuid


PACKET = Path(__file__).parent
SPEC = importlib.util.spec_from_file_location("r02_packet_generator", PACKET / "r02_packet_generator.py")
generator = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(generator)
WORKER_SPEC = importlib.util.spec_from_file_location("r02_invocation_worker", PACKET / "r02_invocation_worker.py")
worker = importlib.util.module_from_spec(WORKER_SPEC)
assert WORKER_SPEC.loader is not None
WORKER_SPEC.loader.exec_module(worker)


class PacketGeneratorTests(unittest.TestCase):
    def test_five_invocations_are_ordered_and_paths_are_attempt_local(self):
        with tempfile.TemporaryDirectory(prefix="r02-generator-test-", dir="/private/tmp") as tmp:
            attempt = Path(tmp) / ("ashlar-r02-" + "a" * 32)
            attempt.mkdir(mode=0o700)
            for name in ("invocations", "input", "jars", "installation", "outputs"):
                (attempt / name).mkdir(mode=0o700)
            umf = attempt / "umf-producer"
            umf.mkdir(mode=0o700)
            (attempt / "input/configured-typed-graph-source.json").write_bytes(
                (generator.ROOT / "examples/end-to-end/configured-typed-graph-source.json").read_bytes())
            for name in ("typed-graph-model.umf.json", "typed-graph-source.jsonl", "typed-graph-binding.json"):
                (attempt / "input" / name).write_bytes((generator.ROOT / "examples/end-to-end" / name).read_bytes())
            attempt_id = attempt.name.removeprefix("ashlar-r02-")
            values = generator.make_invocations(attempt, attempt_id, umf)
            self.assertEqual([(case, kind) for case, kind, _path in values], [
                ("initialize", "initialize"), ("batch-1", "publish"), ("batch-2", "publish"),
                ("batch-3", "publish"), ("batch-3-replay", "replay")])
            for index, (_case, _kind, path) in enumerate(values):
                config = json.loads(path.read_bytes())
                self.assertTrue(path.resolve().is_relative_to(attempt.resolve()))
                self.assertEqual(config["ordinal"], 0 if index == 0 else 1 if index == 1 else 2 if index == 2 else 3)
                self.assertEqual(config["installation_sha256"], None if index == 0 else "@from-initialization@")
                for key in ("installation", "output", "jars", "source_configuration"):
                    self.assertTrue(config[key].startswith(str(attempt.resolve())))
                self.assertTrue(config["producer"]["source"].startswith(str(attempt.resolve())))

    def test_vector_extractor_requires_complete_exact_publication_roles(self):
        with tempfile.TemporaryDirectory(prefix="r02-vector-test-", dir="/private/tmp") as tmp:
            attempt = Path(tmp) / ("ashlar-r02-" + "b" * 32)
            attempt.mkdir(mode=0o700)
            (attempt / "installation").mkdir(mode=0o700)
            ids = {role: str(uuid.uuid4()) for role in worker.ROLES | {"manifest", "attempts"}}
            registry = {"profile": "ashlar-configured-local-installation/0.2",
                        "targets": [{"table": "local.configured." + role,
                                     "path": str(attempt / "installation" / role), "uuid": value}
                                    for role, value in ids.items()]}
            installation = attempt / "installation/installation.json"
            installation.write_text(json.dumps(registry), encoding="utf-8")
            versions = {"local.configured." + role: i for i, role in enumerate(sorted(worker.ROLES))}
            result = {"operation": "replay", "replayed": True, "ordinal": 3, "completed_batches": 3,
                      "manifest": {"profile_version": "ashlar-delta/0.3", "publication_id": "pub-1",
                                   "table_versions_json": json.dumps(versions)}}
            vector = worker.extract_vector(attempt, result, worker.sha256(installation))
            self.assertEqual({tuple(item["name"]) for item in vector["tables"]},
                             {("local", "configured", role) for role in worker.ROLES})
            self.assertEqual(vector["manifestUuid"], ids["manifest"])
            self.assertEqual({item["version"] for item in vector["tables"]}, set(versions.values()))
            result["manifest"]["table_versions_json"] = json.dumps({"local.configured.object_current": 1})
            with self.assertRaisesRegex(ValueError, "four-role"):
                worker.extract_vector(attempt, result, worker.sha256(installation))


if __name__ == "__main__":
    unittest.main()
