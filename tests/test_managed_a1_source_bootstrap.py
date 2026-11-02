import json
import unittest

from ashlar_host.managed_a1_source_bootstrap import (
    MODEL_SHA256, candidate_r04_schedule, candidate_seed_group,
    validate_private_bootstrap_config,
)
from ashlar_host.r04_schedule import SLOT_ORDER, STAGES


def bindings():
    return {source: {"feed": f"feed-{source}", "epoch": "epoch-1",
        "installation_id": f"install-{source}", "consumer_id": f"consumer-{source}",
        "registration_id": f"registration-{source}", "ack_scope": f"scope-{source}",
        "ack_registration_id": f"ack-registration-{source}"}
        for source in ("A", "B")}


def private_source_ids():
    return {source: {"feed": f"feed-{source}", "consumer_id": f"consumer-{source}",
        "ack_scope": f"scope-{source}"} for source in ("A", "B")}


class ManagedA1SourceBootstrapTests(unittest.TestCase):
    def test_candidate_schedule_freezes_eight_semantic_slots_and_r28_pins(self):
        schedule = candidate_r04_schedule(bindings())
        document = schedule.document()
        self.assertEqual(tuple(slot["slot"] for slot in document["slots"]), SLOT_ORDER)
        self.assertEqual(tuple(slot["stage"] for slot in document["slots"]), STAGES)
        self.assertEqual(document["slots"][0]["expected_transition"]["operation_count"], 7)
        operations = document["slots"][0]["mutation"]["operations"]
        self.assertEqual(len(operations), 7)
        self.assertEqual(document["slots"][1]["expected_transition"]["operation_count"], 7)
        self.assertEqual(sum(op["operation"] == "create_object" for op in operations), 4)
        self.assertEqual(sum(op["operation"] == "create_edge" for op in operations), 3)
        self.assertEqual({op.get("relationship") for op in operations
                          if op["operation"] == "create_edge"}, {"knows", "located"})
        self.assertEqual(document["accepted_model_pins"]["A"]["modelSha256"], MODEL_SHA256)
        self.assertEqual(len(document["prefix_oracle"]), 9)
        self.assertEqual(len(document["prefix_oracle"][1]["expected_current"]["A"]["objects"]), 4)
        self.assertEqual(len(document["prefix_oracle"][1]["expected_current"]["A"]["edges"]), 3)
        self.assertEqual(len(document["prefix_oracle"][2]["expected_current"]["B"]["objects"]), 4)
        self.assertEqual(document["prefix_oracle"][5]["expected_tombstones"]["A"],
                         ["knows-a-ab", "located-a-a", "person-a-a"])
        self.assertEqual(document["prefix_oracle"][7]["expected_current"]["A"],
                         document["prefix_oracle"][6]["expected_current"]["A"])
        self.assertEqual(document["slots"][0]["mutation"]["operations"][0]["values"]["person_name"],
                         "Ada")
        self.assertEqual(document["prefix_oracle"][1]["expected_history_row_count"]["A"], 7)
        self.assertEqual(document["prefix_oracle"][5]["expected_history_row_count"]["A"], 11)
        self.assertEqual(document["prefix_oracle"][7]["expected_history_row_count"]["A"], 11)
        self.assertEqual(document["prefix_oracle"][8]["expected_history_row_count"]["B"], 11)
        self.assertEqual(document["prefix_oracle"][2]["expected_history_row_count"]["B"], 7)
        self.assertEqual(document["prefix_oracle"][4]["expected_history_row_count"]["B"], 8)
        self.assertEqual(document["prefix_oracle"][6]["expected_history_row_count"]["B"], 11)
        self.assertEqual(candidate_seed_group("A")["records"][0]["values"]["person_key"],
                         "a-ada")
        self.assertEqual(document["prefix_oracle"][8]["expected_current"]["B"],
                         document["prefix_oracle"][7]["expected_current"]["B"])
        self.assertEqual(document["prefix_oracle"][8]["expected_history"]["B"],
                         document["prefix_oracle"][7]["expected_history"]["B"])

    def test_candidate_schedule_rejects_shared_or_missing_source_authority(self):
        selected = bindings()
        selected["B"]["installation_id"] = selected["A"]["installation_id"]
        with self.assertRaises(ValueError):
            candidate_r04_schedule(selected)
        selected = bindings()
        del selected["A"]["ack_scope"]
        with self.assertRaises(ValueError):
            candidate_r04_schedule(selected)

    def test_private_config_is_closed_and_pins_exact_model(self):
        config = {"profile": "ashlar-managed-a1-truss053-source-bootstrap/0.1",
            "root": "/private/tmp/run", "model_path": "/private/tmp/model.json",
            "model_sha256": MODEL_SHA256, "bun_path": "/private/tmp/bun",
            "pg18_initdb": "/private/tmp/pg18/bin/initdb", "source_ids": private_source_ids(),
            "a1_group": {"profile": "ashlar-truss-record-group/0.1"}}
        raw = json.dumps(config, sort_keys=True, separators=(",", ":")).encode()
        self.assertEqual(validate_private_bootstrap_config(raw), config)
        config["unexpected"] = True
        raw = json.dumps(config, sort_keys=True, separators=(",", ":")).encode()
        with self.assertRaises(ValueError):
            validate_private_bootstrap_config(raw)


if __name__ == "__main__":
    unittest.main()
