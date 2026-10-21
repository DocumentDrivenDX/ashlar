# SPDX-License-Identifier: MIT OR Apache-2.0
import hashlib
import json
import base64
import unittest

from ashlar_host.managed_current_query import (
    admit_managed_current_source_oracle,
    managed_current_case_bytes,
    managed_current_grouped_paths,
    managed_current_person_page,
    managed_current_query_case,
)


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


def _source_bundle():
    page = json.dumps({"writerXid": "809"}, sort_keys=True, separators=(",", ":")).encode()
    before = json.dumps({"profile": "truss-caller-asserted-feed/0.1", "installationId": "inst",
                         "sourceEpoch": "epoch-current", "consumerId": "consumer",
                         "registrationId": "registration", "nextXid": "0"},
                        sort_keys=True, separators=(",", ":")).encode()
    after = json.dumps({"profile": "truss-caller-asserted-feed/0.1", "installationId": "inst",
                        "sourceEpoch": "epoch-current", "consumerId": "consumer",
                        "registrationId": "registration", "nextXid": "809"},
                       sort_keys=True, separators=(",", ":")).encode()
    value = {
        "profile": "ashlar-managed-current-core-binding/0.1",
        "status": "preinstallation-source-bundle",
        "source": {
            "trussVersion": "0.0.44", "umfVersion": "0.10.0",
            "feed": "feed-current", "epoch": "epoch-current",
            "nativePageBase64": base64.b64encode(page).decode(),
            "cursorBeforeBase64": base64.b64encode(before).decode(),
            "cursorAfterBase64": base64.b64encode(after).decode(),
            "transactionSha256": hashlib.sha256(page).hexdigest(),
            "acceptedDocuments": {"1:typed-graph": {"modelSha256": "d" * 64}},
            "catalogViewSha256": {"1": "c" * 64},
        },
    }
    value["bindingSha256"] = hashlib.sha256(_canonical(value).encode()).hexdigest()
    return value


def _oracle(source_bundle=None):
    from ashlar_host.managed_current_query import _managed_source_identity
    source_bundle = source_bundle or _source_bundle()
    identity = _managed_source_identity(source_bundle, schedule_sha256="a" * 64, schedule_step=7)
    value = {
        "profile": "ashlar-managed-current-source-oracle/0.1",
        "authority": "external-real-truss-holder",
        "trussVersion": identity["trussVersion"],
        "umfVersion": identity["umfVersion"],
        "scheduleSha256": identity["scheduleSha256"],
        "scheduleStep": identity["scheduleStep"],
        "capturedAt": "2026-10-21T14:00:00Z",
        "authorityReceiptSha256": identity["authorityReceiptSha256"],
        "catalogHeadSha256": identity["catalogHeadSha256"],
        "modelSha256": identity["modelSha256"],
        "feed": identity["feed"],
        "epoch": identity["epoch"],
        "cursorSha256": identity["cursorSha256"],
        "persons": [
            {"key": "person-1", "name": "Ada", "quantity": {"state": "value", "value": 9007199254740993}},
            {"key": "person-2", "name": "Bea", "quantity": {"state": "null"}},
            {"key": "person-3", "name": "Lin", "quantity": {"state": "absent"}},
        ],
        "places": [{"key": "place-X", "name": "X"}],
        "edges": [
            {"id": "knows-1", "relationship": "knows", "sourceType": "Person",
             "sourceKey": "person-1", "targetType": "Person", "targetKey": "person-2"},
            {"id": "knows-2", "relationship": "knows", "sourceType": "Person",
             "sourceKey": "person-1", "targetType": "Person", "targetKey": "person-2"},
            {"id": "located-1", "relationship": "located", "sourceType": "Person",
             "sourceKey": "person-2", "targetType": "Place", "targetKey": "place-X"},
        ],
    }
    value["oracleSha256"] = hashlib.sha256(_canonical(value).encode()).hexdigest()
    return admit_managed_current_source_oracle(value,
        source_bundle=source_bundle, schedule_sha256="a" * 64, schedule_step=7)


class ManagedCurrentQueryTests(unittest.TestCase):
  def test_source_oracle_preserves_exact_presence_and_independent_grouped_page_results(self):
    oracle = _oracle()
    self.assertEqual(managed_current_grouped_paths(oracle, threshold=0), [["person-1", "2", "1"]])
    self.assertEqual(managed_current_grouped_paths(oracle, threshold=1), [["person-1", "2", "1"]])
    self.assertEqual(managed_current_grouped_paths(oracle, threshold=2), [])
    first = managed_current_person_page(oracle, page_size=2)
    second = managed_current_person_page(oracle, after=first["nextAfter"], page_size=2)
    self.assertEqual(first, {"rows": [["person-1", "Ada"], ["person-2", "Bea"]],
                             "nextAfter": "person-2", "hasMore": True})
    self.assertEqual(second, {"rows": [["person-3", "Lin"]],
                              "nextAfter": None, "hasMore": False})
    self.assertEqual(oracle.persons[0]["quantity"], {"state": "value", "value": 9007199254740993})
    self.assertEqual(oracle.persons[1]["quantity"], {"state": "null"})
    self.assertEqual(oracle.persons[2]["quantity"], {"state": "absent"})


  def test_case_record_binds_exact_objects_and_opaque_continuation_digest(self):
    oracle = _oracle()
    request = {"interfaceVersion": "weft-compile/0.2.7", "sql": "select ?"}
    artifact = {"status": "compiled", "sql": "SELECT person_key FROM graph", "parameters": [
        {"logicalType": {"family": "string"}, "origin": {"kind": "source"},
         "position": 1, "value": "alice"},
    ]}
    cursor = b"opaque-original-read-owner-cursor"
    class Continuation:
        owner_cursor = cursor
    case = managed_current_query_case(
        case="backend02-page1", source_commit="f" * 40,
        linux_wheel_sha256="1" * 64,
        linux_native_module_sha256="2" * 64,
        linux_record_sha256="3" * 64,
        runtime_identity={"opening": {"sparkVersion": "4.2.0", "runtimeTag": "serverless-test"},
                          "closing": {"sparkVersion": "4.2.0", "runtimeTag": "serverless-test"},
                          "privateDatabricksBuildIdentity": "unknown"},
        publication_receipt={"profile": "ashlar-managed-current-destination-receipt/0.1"},
        request=request, artifact=artifact, oracle=oracle,
        expected_rows=[["person-1", "Ada"], ["person-2", "Bea"]],
        actual_rows=[["person-1", "Ada"], ["person-2", "Bea"]],
        run_id=12345, task_receipt_id="task-run-abc",
        page_size=2, continuation=Continuation(),
    )
    self.assertEqual(case["continuationSha256"], hashlib.sha256(cursor).hexdigest())
    self.assertNotIn("opaque-original-read-owner-cursor", managed_current_case_bytes(case).decode())
    self.assertEqual(json.loads(case["requestJson"]), request)
    self.assertEqual(json.loads(case["artifactJson"]), artifact)
    self.assertEqual(case["expectedRows"], case["actualRows"])
    self.assertEqual(case["expectedRowsSha256"], case["oracle"]["expectedRowsSha256"])
    self.assertEqual(case["sqlSha256"], hashlib.sha256(request["sql"].encode()).hexdigest())
    self.assertEqual(case["compilerSqlSha256"], hashlib.sha256(artifact["sql"].encode()).hexdigest())


  def test_case_recorder_refuses_mismatched_results_or_unverified_source_digest(self):
    oracle = _oracle()
    request = {"sql": "select 1"}
    artifact = {"status": "compiled", "sql": "SELECT '1'", "parameters": []}
    args = dict(
        case="backend03-grouped-having", source_commit="f" * 40,
        linux_wheel_sha256="1" * 64,
        linux_native_module_sha256="2" * 64,
        linux_record_sha256="3" * 64,
        runtime_identity={"opening": {"runtimeTag": "serverless-test"},
                          "closing": {"runtimeTag": "serverless-test"},
                          "privateDatabricksBuildIdentity": "unknown"},
        publication_receipt={"profile": "ashlar-managed-current-destination-receipt/0.1"},
        request=request, artifact=artifact, oracle=oracle,
        expected_rows=[["person-1", "2", "1"]], actual_rows=[],
        run_id=12345, task_receipt_id="task-run-abc",
    )
    with self.assertRaises(ValueError):
        managed_current_query_case(**args)

    bad = {"profile": "ashlar-managed-current-source-oracle/0.1"}
    with self.assertRaises(ValueError):
        admit_managed_current_source_oracle(bad,
            source_bundle=_source_bundle(), schedule_sha256="a" * 64, schedule_step=7)

  def test_source_oracle_refuses_unrelated_feed_cursor_or_schedule_before_read(self):
    source_bundle = _source_bundle()
    _oracle(source_bundle)
    from ashlar_host.managed_current_query import _managed_source_identity
    identity = _managed_source_identity(source_bundle, schedule_sha256="a" * 64, schedule_step=7)
    unrelated = json.loads(_canonical(source_bundle))
    unrelated["source"]["feed"] = "plausible-but-unrelated-feed"
    unrelated.pop("bindingSha256")
    unrelated["bindingSha256"] = hashlib.sha256(_canonical(unrelated).encode()).hexdigest()
    value = {
        "profile": "ashlar-managed-current-source-oracle/0.1",
        "authority": "external-real-truss-holder", "trussVersion": "0.0.44",
        "umfVersion": "0.10.0", "scheduleSha256": "a" * 64, "scheduleStep": 7,
        "capturedAt": "2026-10-21T14:00:00Z", "authorityReceiptSha256": "b" * 64,
        "catalogHeadSha256": "c" * 64, "modelSha256": "d" * 64,
        "feed": "plausible-but-unrelated-feed", "epoch": "epoch-current",
        "cursorSha256": "e" * 64, "persons": [], "places": [], "edges": [],
    }
    value["oracleSha256"] = hashlib.sha256(_canonical(value).encode()).hexdigest()
    with self.assertRaisesRegex(ValueError, "source bundle, manifest progress"):
      admit_managed_current_source_oracle(
          value, source_bundle=unrelated, schedule_sha256="a" * 64, schedule_step=7)
    good = _oracle(source_bundle)
    with self.assertRaises(ValueError):
      _ = admit_managed_current_source_oracle(
          {**json.loads(_canonical(value)), "oracleSha256": value["oracleSha256"]},
          source_bundle=source_bundle, schedule_sha256="f" * 64, schedule_step=7)
    wrong_version = {
        "profile": "ashlar-managed-current-source-oracle/0.1",
        "authority": "external-real-truss-holder", "trussVersion": "0.0.45",
        "umfVersion": "0.10.0", "scheduleSha256": "a" * 64, "scheduleStep": 7,
        "capturedAt": "2026-10-21T14:00:00Z", "authorityReceiptSha256": identity["authorityReceiptSha256"],
        "catalogHeadSha256": identity["catalogHeadSha256"], "modelSha256": identity["modelSha256"],
        "feed": identity["feed"], "epoch": identity["epoch"], "cursorSha256": identity["cursorSha256"],
        "persons": [{"key": "person-1", "name": "Ada", "quantity": {"state": "absent"}}],
        "places": [], "edges": [],
        "catalogEvidenceProfile": "ashlar-truss-current-catalog-evidence/0.2",
        "runtimeProvenance": {"trussWheelSha256": "1" * 64, "runtimeManifestSha256": "2" * 64},
    }
    wrong_version["oracleSha256"] = hashlib.sha256(_canonical(wrong_version).encode()).hexdigest()
    with self.assertRaisesRegex(ValueError, "source bundle, manifest progress"):
      admit_managed_current_source_oracle(
          wrong_version, source_bundle=source_bundle,
          schedule_sha256="a" * 64, schedule_step=7)

  def test_managed_backend_route_refuses_cross_surface_request_artifact_before_execution(self):
    from ashlar.weft_query import MANAGED_CURRENT_BACKEND02, MANAGED_CURRENT_BACKEND03, read_weft

    class Executor:
      calls = 0
      def query(self, sql, parameters):
        self.calls += 1
        raise AssertionError("cross-surface target must be rejected before SQL")

    executor = Executor()
    request = {"target": {"backendId": MANAGED_CURRENT_BACKEND03["backendId"],
                           "backendVersion": MANAGED_CURRENT_BACKEND03["backendVersion"],
                           "targetProfile": MANAGED_CURRENT_BACKEND03["targetProfile"]}}
    artifact = {"backend": MANAGED_CURRENT_BACKEND02}
    with self.assertRaisesRegex(ValueError, "Exact managed current compiler target"):
      read_weft(executor, None, None, None, None, request=request, artifact=artifact,
          context=None, supported_profiles=[], supported_revisions={})
    self.assertEqual(executor.calls, 0)

  def test_original_sdk_two_page_executor_keeps_cursor_private_and_binds_both_pages(self):
    from types import MethodType, SimpleNamespace
    from ashlar_host.managed_current_query import ManagedCurrentReadOwner

    oracle = _oracle()
    cursor = b"opaque-sdk-continuation"
    continuation = SimpleNamespace(owner_cursor=cursor)
    class ResultRow:
      def __init__(self, key, name): self.values = {"person_key": key, "person_name": name}
      def model_dump(self, mode="python"): return dict(self.values)
    class Entry:
      result = SimpleNamespace(model_fields={"person_key": None, "person_name": None})
    class Target:
      def __init__(self, owner): self.owner = owner; self.calls = []
      def page(self, entry, parameters, *, after, timeout):
        self.calls.append(after)
        index = len(self.calls)
        request = {"sql": "SELECT person_key, person_name FROM managed_person"
                   " WHERE person_key > :after ORDER BY person_key"}
        artifact = {"status": "compiled", "sql": "SELECT person_key, person_name FROM managed_person",
                    "parameters": [{"position": 1, "logicalType": {"family": "string"},
                                    "origin": {"kind": "page"}, "value": "" if index == 1 else "bob"}]}
        self.owner.current_request, self.owner.current_artifact = request, artifact
        if index == 1:
          return SimpleNamespace(items=(ResultRow("person-1", "Ada"), ResultRow("person-2", "Bea")),
                                 next_after=continuation)
        if after is not continuation: raise AssertionError("original continuation must be reused")
        return SimpleNamespace(items=(ResultRow("person-3", "Lin"),), next_after=None)

    owner = object.__new__(ManagedCurrentReadOwner)
    owner.runtime_identity = {"opening": {"sparkVersion": "4.2.0", "runtimeTag": "serverless-test"},
                              "closing": {"sparkVersion": "4.2.0", "runtimeTag": "serverless-test"},
                              "privateDatabricksBuildIdentity": "unknown"}
    owner.receipt = {"profile": "ashlar-managed-current-destination-receipt/0.1"}
    owner.current_request = owner.current_artifact = None
    owner.active = False
    owner.maximum_rows = 1000
    owner.expect_rows = MethodType(ManagedCurrentReadOwner.expect_rows, owner)
    target = Target(owner)
    reports = ManagedCurrentReadOwner.run_backend02_two_pages(
        owner, target, Entry(), SimpleNamespace(), oracle=oracle,
        expected_first=[["person-1", "Ada"], ["person-2", "Bea"]],
        expected_second=[["person-3", "Lin"]], source_commit="f" * 40,
        linux_wheel_sha256="1" * 64, linux_native_module_sha256="2" * 64,
        linux_record_sha256="3" * 64, run_id=12345, task_receipt_id="task-run-abc")
    self.assertEqual(len(target.calls), 2)
    self.assertIsNone(target.calls[0])
    self.assertIs(target.calls[1], continuation)
    self.assertEqual(reports["page1"]["continuationSha256"], hashlib.sha256(cursor).hexdigest())
    self.assertEqual(reports["continuation"]["incomingContinuationSha256"],
                     reports["page1"]["continuationSha256"])
    self.assertNotIn("opaque-sdk-continuation", json.dumps(reports))

  def test_read_owner_rebuilds_exact_request_from_source_bundle_and_receipt(self):
    from types import MethodType
    from ashlar_host.managed_current_query import ManagedCurrentReadOwner
    from ashlar_host.managed_current_core import bind_current_core
    from ashlar_host.managed_current_weft_binding import managed_current_count_star_request
    from test_managed_current_weft_binding import _inputs, _managed_config
    from test_managed_current_core import _current_admission

    bundle = bind_current_core(_current_admission(), _managed_config())
    inputs = _inputs(bundle)
    sql = ('SELECT p.person_key, COUNT(*) AS paths, '
           'COUNT_DISTINCT_PATH_TARGETS(x) AS targets FROM Person p '
           'CROSS JOIN EXPAND_PATHS(p.knows, located) AS x GROUP BY p.person_key '
           'HAVING COUNT(*) > 0 ORDER BY p.person_key')
    request = managed_current_count_star_request(bundle, inputs, sql)
    owner = object.__new__(ManagedCurrentReadOwner)
    owner.source_bundle = bundle
    owner.receipt = {"weftBindingInputs": inputs}
    owner.vector = object()
    owner.context = object()
    owner.bind_descriptor = MethodType(lambda self, *args: None, owner)
    owner._renew = MethodType(lambda self: None, owner)
    artifact = {"status": "compiled", "bindingSha256": request["target"]["bindingSha256"]}
    owner.admit_artifact(request, artifact, None, owner.context)
    self.assertEqual(owner.current_request, request)

    altered = json.loads(_canonical(request))
    altered["modules"][0]["documentJson"] += " "
    with self.assertRaisesRegex(ValueError, "exact accepted source and receipt"):
      owner.admit_artifact(altered, artifact, None, owner.context)

  def test_read_owner_renews_all_six_role_metadata_and_refuses_version_drift(self):
    from types import SimpleNamespace
    from unittest.mock import patch
    from ashlar_host.managed_current_query import ManagedCurrentReadOwner

    roles = ("object_current", "edge_current", "tombstone", "whole_source_history",
             "publication_manifest", "publication_attempt_phase")
    def observed(version_delta=0):
      return {role: SimpleNamespace(uuid=f"uuid-{role}", version=10 + version_delta,
                                    schema=(("record_id", "string", False),), protocol={"minReaderVersion": 1})
              for role in roles}

    class Transport:
      def query(self, _sql, parameters):
        return SimpleNamespace(rows=[{"table_owner": "principal-1"}])

    current = observed()
    targets = SimpleNamespace(
        profile=object(), catalog="client_dev", schema="trial_schema",
        names={role: ("client_dev", "trial_schema", role) for role in roles},
        transport=Transport(), observe=lambda: current)
    owner = object.__new__(ManagedCurrentReadOwner)
    owner.active = True
    owner.spark = object()
    owner.targets = targets
    owner.expected_environment = {"principal": "principal-1", "sparkVersion": "4.2.0"}
    owner.opening = observed()

    with patch("ashlar_host.managed_current_publication.observe_managed_current_environment",
               return_value=owner.expected_environment):
      owner._renew()
      current = observed(version_delta=1)
      with self.assertRaises(ValueError):
        owner._renew()

  def test_local_parity_recorder_requires_four_exact_prior_compile_objects(self):
    from pathlib import Path
    from ashlar_host.managed_current_parity import (
        _CASES, _prior_case, build_managed_current_local_parity,
        managed_current_parity_bytes,
    )

    evidence_root = (Path(__file__).resolve().parents[1] / "docs/helix/04-build/evidence"
                     / "current-upstream-release-20261021/local-current")
    compiled = {}
    for case in _CASES:
      prior = _prior_case(case, evidence_root)
      compiled[case] = {"request": prior["request"], "artifact": prior["artifact"]}
    parity = build_managed_current_local_parity(
        evidence_root=evidence_root,
        local_wheel={"sha256": "1" * 64, "nativeModuleSha256": "2" * 64,
                     "recordSha256": "3" * 64},
        source_commit="f" * 40, compiled_cases=compiled)
    self.assertEqual(parity["schema"], "weft-local-program-parity/0.2")
    self.assertEqual(set(parity["cases"]), set(_CASES))
    expected_baseline = {
        "asset": "local-current/current-native-query-and-sdk-pages-20261021.json",
        "sha256": hashlib.sha256((evidence_root / "current-native-query-and-sdk-pages-20261021.json").read_bytes()).hexdigest(),
    }
    self.assertEqual(parity["baselinePacket"], expected_baseline)
    for case_id, case in parity["cases"].items():
      self.assertEqual(set(case["priorEvidence"]), {"asset", "sha256"})
      self.assertEqual(case["priorEvidence"], expected_baseline)
      self.assertEqual(set(case["gzipEvidence"]),
                       {"asset", "compressedSha256", "reportSha256"})
      self.assertEqual(case["gzipEvidence"]["asset"], {
          "backend03-grouped-having": "positive_grouped_graph_query.json.gz",
          "backend03-negative-having": "negative_having_query.json.gz",
          "backend02-page1": "sdk_pages_2_plus_1.json.gz",
          "backend02-continuation": "sdk_pages_2_plus_1.json.gz",
      }[case_id])
    self.assertEqual(json.loads(managed_current_parity_bytes(parity)), parity)
    compiled["backend02-page1"]["artifact"]["sql"] += " "
    with self.assertRaisesRegex(ValueError, "preserve prior SQL"):
      build_managed_current_local_parity(
          evidence_root=evidence_root,
          local_wheel={"sha256": "1" * 64, "nativeModuleSha256": "2" * 64,
                       "recordSha256": "3" * 64},
          source_commit="f" * 40, compiled_cases=compiled)
