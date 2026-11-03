import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ashlar_host import managed_a1_source_bootstrap as bootstrap
from ashlar_host.managed_a1_source_bootstrap import (
    MODEL_ID, MODEL_RELATIVE_PATH, MODEL_REVISION, MODEL_SHA256,
    PUBLIC_SETUP_HELPER_RELATIVE_PATH, PUBLIC_SETUP_HELPER_SHA256,
    candidate_r04_schedule, candidate_seed_group, public_group_seed,
    example_private_config, validate_catalog_only_page, validate_private_bootstrap_config,
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
    def _operator_repository(self, root):
        actual = Path(__file__).resolve().parents[1]
        root.mkdir(parents=True)
        model = Path(MODEL_RELATIVE_PATH)
        helper = PUBLIC_SETUP_HELPER_RELATIVE_PATH
        (root / model).parent.mkdir(parents=True)
        (root / helper).parent.mkdir(parents=True)
        shutil.copyfile(actual / model, root / model)
        shutil.copyfile(actual / helper, root / helper)
        return root

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
        with tempfile.TemporaryDirectory(dir="/private/tmp") as temporary:
            repository = self._operator_repository(Path(temporary) / "operator-repo")
            config = {"profile": "ashlar-managed-a1-truss053-source-bootstrap/0.1",
                "root": "/private/tmp/run", "repository_root": str(repository),
                "model_path": str(repository / MODEL_RELATIVE_PATH),
                "setup_helper_path": str(repository / PUBLIC_SETUP_HELPER_RELATIVE_PATH),
                "model_sha256": MODEL_SHA256, "bun_path": "/private/tmp/bun",
                "pg18_initdb": "/private/tmp/pg18/bin/initdb", "source_ids": private_source_ids(),
                "a1_group": {"profile": "ashlar-truss-record-group/0.1"}}
            raw = json.dumps(config, sort_keys=True, separators=(",", ":")).encode()
            self.assertEqual(validate_private_bootstrap_config(raw), config)
            helper_path = Path(config["setup_helper_path"])
            original_helper = helper_path.read_bytes()
            helper_path.write_bytes(original_helper + b"\n# altered\n")
            with self.assertRaisesRegex(ValueError, "setup helper path and bytes"):
                validate_private_bootstrap_config(raw)
            helper_path.write_bytes(original_helper)
            config["unexpected"] = True
            raw = json.dumps(config, sort_keys=True, separators=(",", ":")).encode()
            with self.assertRaises(ValueError):
                validate_private_bootstrap_config(raw)

    def test_config_uses_explicit_operator_checkout_from_installed_package_path(self):
        with tempfile.TemporaryDirectory(dir="/private/tmp") as temporary:
            root = Path(temporary)
            repository = self._operator_repository(root / "operator-repo")
            (root / "pgserver").mkdir()
            with patch.object(bootstrap, "__file__",
                    str(root / "site-packages/ashlar_host/managed_a1_source_bootstrap.py")), \
                 patch("importlib.resources.files", return_value=root / "pgserver"):
                config = example_private_config(root=root / "run", bun_path=root / "bun",
                                                repository_root=repository)
            raw = json.dumps(config, sort_keys=True, separators=(",", ":")).encode()
            self.assertEqual(validate_private_bootstrap_config(raw), config)
            self.assertEqual(Path(config["model_path"]).parents[6], repository / "docs")
            self.assertEqual(Path(config["setup_helper_path"]),
                             repository / PUBLIC_SETUP_HELPER_RELATIVE_PATH)
            self.assertEqual(config["model_sha256"], MODEL_SHA256)
            self.assertEqual(PUBLIC_SETUP_HELPER_SHA256,
                             "d6425948b7b84395429f5b83c624cad27843c2e1eda6af66fce16dcc71d1be89")

    def test_public_group_preflight_receives_exact_typed_scalar_carriers(self):
        from ashlar_host.truss_record_group import preflight
        with tempfile.TemporaryDirectory(dir="/private/tmp") as temporary:
            root = Path(temporary).resolve()
            model_path = Path(__file__).resolve().parents[1] / MODEL_RELATIVE_PATH
            origin_path = root / "origin.json"
            origin_path.write_bytes(b'{"operator":"pure-preflight-test"}')
            config = {"profile": "ashlar-truss-record-group/0.1",
                "connection_uri": "postgresql://unused", "bun": str(Path(__file__).resolve()),
                "document_path": str(model_path), "document_revision": MODEL_REVISION,
                "document_id": MODEL_ID, "source_version": "0.10.0",
                "origin_path": str(origin_path), "request_key": "pure-preflight",
                **public_group_seed(candidate_seed_group("A"))}
            config_path = root / "group.json"
            config_path.write_bytes(json.dumps(config, sort_keys=True,
                separators=(",", ":")).encode())
            selected, _, _, _, _, _ = preflight(config_path, root / "result.json")
            self.assertEqual(len(selected.records), 4)
            self.assertEqual(len(selected.edges), 3)
            self.assertIn(("person_key", "string", "a-ada"), selected.records[0].values)
            self.assertIn(("place_name", "string", "New York"), selected.records[2].values)

    def test_public_catalog_page_is_a_transaction_without_graph_effects(self):
        page = {"interfaceVersion": "truss-native-feed/0.1", "writerXid": "758",
            "revisions": [{"revision": "1"}],
            "catalogPrerequisites": [{}, {}], "changes": [], "sources": [],
            "reservations": []}
        raw = b'{ "writerXid": "758", "interfaceVersion": "truss-native-feed/0.1", "revisions": [{"revision":"1"}], "catalogPrerequisites": [{}, {}], "changes": [], "sources": [], "reservations": [] }'
        self.assertEqual(validate_catalog_only_page(raw, "758"), page)
        page["changes"] = [{"operation": "create"}]
        with self.assertRaises(ValueError):
            validate_catalog_only_page(json.dumps(page).encode(), "758")
        with self.assertRaises(ValueError):
            validate_catalog_only_page(raw.replace(b'"758"', b'"759"', 1), "758")


if __name__ == "__main__":
    unittest.main()
