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
    def test_live_page_models_meet_reviewed_read_requirements(self):
        from pydantic import BaseModel, create_model
        from umf_access.engine_catalog import EngineRead
        from umf_access.reviewed import ReadBuildError, _fields

        unreviewed = create_model("UnreviewedPageParameters", __base__=BaseModel)
        with self.assertRaisesRegex(ReadBuildError, "strict and forbid extra fields"):
            EngineRead.from_original('{"version":"weft.engine-page/0.1","status":"compiled"}',
                parameters=unreviewed, result=unreviewed)
        parameters, result = worker.make_query_models()
        self.assertEqual((parameters.model_config.get("extra"), parameters.model_config.get("strict")),
                         ("forbid", True))
        self.assertEqual((result.model_config.get("extra"), result.model_config.get("strict")),
                         ("forbid", True))
        self.assertEqual(_fields(parameters), [])
        self.assertEqual([item["name"] for item in _fields(result, parameter=False, allow_related=True)],
                         ["person_key", "person_name"])

    def test_refusal_location_omits_exception_message(self):
        try:
            raise ValueError("private payload must not be logged")
        except ValueError as exc:
            summary = worker.refusal_location(exc)
        self.assertIn("ValueError", summary)
        self.assertRegex(summary, r"test_r02_continuation_supervisor\.py:\d+")
        self.assertLessEqual(summary.count(" <- "), 7)
        self.assertNotIn("private payload", summary)

    def test_selected_weft_page_carriers_match_visible_source_oracle(self):
        """Reproduce page construction without Spark; hidden key is not visible output."""
        import hashlib
        from test_configured_typed_graph_projection_oracle import graph_scope
        from ashlar.weft_053_distribution import Weft053Distribution, Weft053Paths
        from ashlar.weft_binding import LAYOUT_SHA256
        from ashlar_host.configured_typed_graph_sdk_binding import configured_typed_graph_sdk_request
        from ashlar_host.configured_sdk_page_oracle import configured_sdk_page_oracle
        from ashlar_host.configured_sdk_pages import _visible_page_rows
        from ashlar_host.delta_custody import encoded

        index = Path("/private/tmp/ashlar-r02-weft053-index/index2.json")
        raw = index.read_bytes()
        compiler = Weft053Distribution(Weft053Paths(index, hashlib.sha256(raw).hexdigest()))
        with graph_scope(1) as scope:
            aliases = {name: "spark_catalog.ashlar_configured_query_test." + name.split(".")[-1]
                       for name in scope.descriptor.versions}
            request = configured_typed_graph_sdk_request(scope.source.configuration,
                dict(scope.descriptor.raw), scope.vector, scope.manifest_uuid, aliases,
                "SELECT p.person_key,p.person_name FROM Person p", {}, layout_sha256=LAYOUT_SHA256)
            envelope = {"version": "weft.engine-page/0.1", "compiler": "ashlar-spark4-pages",
                        "request": request, "pageSize": 2}
            response = compiler.compile_engine_page_json(encoded(envelope))
            program = json.loads(response)
            carrier = configured_sdk_page_oracle(scope, request, program)({}, None)["rows"][:2]
            visible = _visible_page_rows(program, carrier)
            self.assertEqual(program["page"]["visibleOutputs"], 2)
            self.assertEqual(program["page"]["keyOutputs"], [2])
            self.assertEqual(carrier, [["person-1", "Ada", "person-1"], ["person-2", "Ben", "person-2"]])
            self.assertEqual(visible, [["person-1", "Ada"], ["person-2", "Ben"]])
            oracle = json.loads(Path(__file__).with_name("source-oracle.json").read_bytes())
            expected = oracle["firstPublicationSourceDerivedExpectations"]["objectSingletons"]
            self.assertEqual(visible, [[item["properties"]["21"], item["properties"]["22"]] for item in expected[:2]])

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
