from pathlib import Path
import unittest
from unittest.mock import patch

from ashlar_host.fabric_launch_readiness import build_fabric_launch_readiness_report


PLAN = (Path(__file__).resolve().parents[1]
        / "docs/helix/04-build/evidence/commerce-fabric-fresh-functional-plan-20261023/README.md")
PLAN_SHA256 = "ac73bf7dbce41724b7935d908275d213f1a1d43c50d859a55334eb056b5419a1"
RUNNER_REVISION = "0123456789abcdef0123456789abcdef01234567"
RUNNER_SHA256 = "a" * 64
R1 = "70cca0ae-e0db-431c-81e9-6651a43f74e7"
R2 = "4d559fd9-ac44-462b-b208-55826d9ad35a"
R1_NODES = "Tables/dbo/ashlar_commerce_r1_70cca0aee0db431c81e96651a43f74e7_nodes"
R1_EDGES = "Tables/dbo/ashlar_commerce_r1_70cca0aee0db431c81e96651a43f74e7_edges"
R2_NODES = "Tables/dbo/ashlar_commerce_r2_4d559fd9ac44462bb20855826d9ad35a_nodes"
R2_EDGES = "Tables/dbo/ashlar_commerce_r2_4d559fd9ac44462bb20855826d9ad35a_edges"
RECORD_SHA = "b" * 64


def runner_inputs():
    return {
        "runner_revision": RUNNER_REVISION,
        "runner_sha256": RUNNER_SHA256,
        "candidate_ids": {"r1": R1, "r2": R2},
        "candidate_paths": {
            "r1": {"nodes": R1_NODES, "edges": R1_EDGES},
            "r2": {"nodes": R2_NODES, "edges": R2_EDGES},
        },
        "r2_intent_state": "absent",
        "r2_source_state": "preparation",
    }


def independent_anchors():
    return {
        "packet_sha256": PLAN_SHA256,
        "runner_revision": RUNNER_REVISION,
        "runner_sha256": RUNNER_SHA256,
        "selection_record": {
            "artifact_identity": "independently-selected-readiness-anchor-record",
            "artifact_sha256": RECORD_SHA,
            "observed_at": "2026-10-23T12:00:00Z",
            "source_provenance": "offline unit-test fixture; caller assertion only",
        },
    }


def observation(value, name):
    return {
        "value": value,
        "artifact_identity": f"fixture:{name}",
        "artifact_sha256": RECORD_SHA,
        "observed_at": "2026-10-23T12:00:00Z",
        "source_provenance": "offline unit-test fixture; caller assertion only",
    }


def complete_observations():
    values = {
        "r1_fresh_source_readmission": True,
        "r2_accepted_source_transaction": True,
        "r2_complete_publication_vector": True,
        "r2_protected_ack": True,
        "resources_and_capacity_selected": True,
        "principals_and_authorities_verified": True,
        "free_capacity_proves_zero_cost": True,
        "candidate_table_paths_absent": True,
        "candidate_display_names_absent": True,
        "runtime_and_numeric_limits_match": True,
        "cost_bound_usd": 0,
    }
    return {key: observation(value, key) for key, value in values.items()}


class Tests(unittest.TestCase):
    def setUp(self):
        self.packet = PLAN.read_text()
        self.assertEqual(PLAN_SHA256, __import__("hashlib").sha256(self.packet.encode()).hexdigest())

    def report(self, *, packet=None, runner=None, observations=None, anchors=None):
        return build_fabric_launch_readiness_report(
            packet_text=self.packet if packet is None else packet,
            runner_inputs=runner_inputs() if runner is None else runner,
            current_observations={} if observations is None else observations,
            independent_anchors=independent_anchors() if anchors is None else anchors,
        )

    def test_missing_typed_observations_refuse_before_any_effect(self):
        runner, observations = runner_inputs(), {}
        original_runner, original_observations = dict(runner), dict(observations)
        with (patch("ashlar_host.fabric_deployment_http.open_fabric_deployment_http",
                    side_effect=AssertionError("readiness must not open HTTP")) as http,
              patch("ashlar_host.onelake_release_upload.open_onelake_release_upload",
                    side_effect=AssertionError("readiness must not open OneLake")) as upload):
            report = self.report(runner=runner, observations=observations)
        http.assert_not_called()
        upload.assert_not_called()
        self.assertEqual(report["readiness"], "blocked-unverified-checklist")
        self.assertFalse(report["launch_authorized"])
        self.assertFalse(report["automatic_approval"])
        self.assertEqual(report["r2_status"].split(";")[0], "preparation-only")
        self.assertTrue({
            "fresh-r1-source-readmission-missing",
            "r2-accepted-source-transaction-missing",
            "r2-complete-publication-vector-missing",
            "r2-protected-ack-missing",
            "current-workspace-capacity-lakehouse-selection-missing",
            "current-principal-and-owner-authority-missing",
            "free-capacity-zero-cost-proof-missing",
            "read-only-candidate-table-path-absence-missing",
            "read-only-candidate-display-name-absence-missing",
            "current-runtime-and-finite-limits-unverified",
            "fabric-total-cost-bound-is-not-proven-zero",
            "caller-supplied-preflight-records-are-unauthenticated",
        }.issubset(report["blockers"]))
        self.assertEqual(runner, original_runner)
        self.assertEqual(observations, original_observations)

    def test_candidate_path_collision_blocks_report(self):
        runner = runner_inputs()
        runner["candidate_paths"]["r2"]["nodes"] = R1_NODES
        report = self.report(runner=runner, observations=complete_observations())
        self.assertIn("r2-runner-paths-do-not-match-retained-plan", report["blockers"])
        self.assertIn("candidate-path-collision", report["blockers"])
        self.assertFalse(report["launch_authorized"])

    def test_ambiguous_intent_is_preserved_and_never_resumed(self):
        runner = runner_inputs()
        runner["r2_intent_state"] = "unknown"
        report = self.report(runner=runner, observations=complete_observations())
        self.assertEqual(report["r2_resume"], "refused-ambiguous-intent")
        self.assertTrue(report["preserve_existing_intents"])
        self.assertIn("r2-intent-ambiguous-preserve-and-reconcile-original-intent", report["blockers"])

    def test_unknown_r2_intent_state_refuses(self):
        runner = runner_inputs()
        runner["r2_intent_state"] = "invented-state"
        report = self.report(runner=runner, observations=complete_observations())
        self.assertEqual(report["r2_resume"], "refused-unknown-intent-state")
        self.assertTrue(report["preserve_existing_intents"])
        self.assertIn("r2-intent-state-missing-or-unknown-preserve-and-reconcile", report["blockers"])

    def test_runner_cannot_upgrade_authored_r2_preparation_status(self):
        runner = runner_inputs()
        runner["r2_source_state"] = "accepted"
        report = self.report(runner=runner)
        self.assertEqual(report["r2_status"].split(";")[0], "preparation-only")
        self.assertIn("r2-authored-candidate-must-remain-preparation-only", report["blockers"])

    def test_boolean_is_not_a_zero_cost_amount(self):
        observations = complete_observations()
        observations["cost_bound_usd"] = observation(False, "cost_bound_usd")
        report = self.report(observations=observations)
        self.assertIn("fabric-total-cost-bound-is-not-proven-zero", report["blockers"])

    def test_bare_boolean_is_not_a_typed_current_observation(self):
        observations = complete_observations()
        observations["r1_fresh_source_readmission"] = True
        report = self.report(observations=observations)
        self.assertIn("fresh-r1-source-readmission-missing", report["blockers"])
        self.assertEqual(report["observation_status"], "typed-claims-only; unauthenticated; no live service reads performed")

    def test_plan_hash_rejects_modified_numeric_limits(self):
        altered = self.packet.replace("900 seconds total process wall time", "901 seconds total process wall time")
        report = self.report(packet=altered, observations=complete_observations())
        self.assertIn("retained-plan-sha256-does-not-match-independent-anchor", report["blockers"])

    def test_duplicate_candidate_row_refuses_even_with_matching_supplied_anchor(self):
        duplicate = self.packet + "\n" + next(line for line in self.packet.splitlines() if line.startswith("| R1 |"))
        with self.assertRaisesRegex(ValueError, "Exactly one retained R1 and one retained R2"):
            self.report(packet=duplicate, observations=complete_observations())

    def test_matching_typed_claims_remain_blocked_and_unauthenticated(self):
        report = self.report(observations=complete_observations())
        self.assertEqual(report["readiness"], "blocked-unverified-checklist")
        self.assertEqual(report["observation_status"], "typed-claims-only; unauthenticated; no live service reads performed")
        self.assertIn("caller-supplied-preflight-records-are-unauthenticated", report["blockers"])
        self.assertFalse(report["launch_authorized"])

    def test_runner_revision_and_hash_require_selected_anchors(self):
        runner = runner_inputs()
        runner["runner_sha256"] = "c" * 64
        report = self.report(runner=runner)
        self.assertIn("runner-sha256-does-not-match-independent-anchor", report["blockers"])


if __name__ == "__main__":
    unittest.main()
