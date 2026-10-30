# SPDX-License-Identifier: MIT OR Apache-2.0
"""Pure configured-local Delta 0.4 core: no Spark or native effects."""
from pathlib import Path
import json
import unittest
import uuid
from types import MappingProxyType, SimpleNamespace

from ashlar.typed_graph_source_config import load_typed_graph_jsonl_configuration
from ashlar_host.configured_local_delta4_publication import (
    DATA_ROLES, INSTALLATION_ROLES,
    build_delta4_realized_vector, materialize_delta4_expected_rows,
    observe_delta4_lookup_hashes,
    plan_configured_local_delta4_transaction1,
)
from ashlar_host.managed_current_carriers import load_current_managed_carriers
from ashlar_host.delta_custody import LocalDeltaTransport


ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "examples/end-to-end/configured-typed-graph-source.json"


class Tests(unittest.TestCase):
    def plan(self):
        configuration = load_typed_graph_jsonl_configuration(CONFIG.resolve())
        return plan_configured_local_delta4_transaction1(
            configuration, stream="gate3-typed-graph",
            materialized_at_us="1791547200000000")

    def lookup_hashes(self, plan):
        values = {}
        for role, typed in (("object_current", "type_id"),
                            ("edge_current", "rel_type_id")):
            for key in plan.pending_lookup_keys[role]:
                values[(role, key["source_system"], key[typed], key["id"])] = -len(values) - 1
        return values

    def test_transaction1_materializes_exact_six_role_profile_rows(self):
        plan = self.plan()
        self.assertEqual(plan.batch_id, "typed-graph-1")
        self.assertEqual(plan.request["batch_id"], "typed-graph-1")
        self.assertEqual(plan.request["predecessor"], "empty-local-configured-jsonl-stream")
        self.assertEqual({role: len(rows) for role, rows in plan.expected_rows.items()}, {
            "object_current": 4, "edge_current": 4,
            "tombstone": 0, "whole_source_history": 8})
        self.assertTrue(all("lookup_hash" not in row for role in ("object_current", "edge_current")
                            for row in plan.expected_rows[role]))

        rows = materialize_delta4_expected_rows(plan, self.lookup_hashes(plan))
        carriers = load_current_managed_carriers("ashlar-delta/0.4-xxhash64")
        for role in DATA_ROLES:
            fields = {name for name, _kind, _nullable in carriers.ordered_fields(role)}
            self.assertTrue(all(set(row) == fields for row in rows[role]))
        self.assertEqual(sum(row["source_system"] == "typed-graph-source"
                             and row["rel_type_id"] == 31
                             and row["source_id"] == 1 and row["target_id"] == 2
                             for row in rows["edge_current"]), 2)
        self.assertTrue(all(type(row["lookup_hash"]) is int
                            for role in ("object_current", "edge_current")
                            for row in rows[role]))
        self.assertTrue(all(type(row["source_position"]) is int
                            for role in ("object_current", "edge_current")
                            for row in rows[role]))
        person = next(row for row in rows["object_current"] if row["type_id"] == 17 and row["id"] == 1)
        self.assertEqual(person["logical_key_json"], '["person-1"]')

    def test_native_hash_map_must_cover_exact_keys_and_signed_bigints(self):
        plan = self.plan()
        values = self.lookup_hashes(plan)
        materialize_delta4_expected_rows(plan, values)
        with self.assertRaises(ValueError):
            materialize_delta4_expected_rows(plan, {})
        key = next(iter(values))
        with self.assertRaises(ValueError):
            materialize_delta4_expected_rows(plan, {**values, key: True})
        with self.assertRaises(ValueError):
            materialize_delta4_expected_rows(plan, {**values, key: 1 << 63})

    def test_role_qualification_preserves_same_source_type_id_across_entity_kinds(self):
        plan = self.plan()
        # A role-qualified key can represent the overlapping identity without
        # one entity kind overwriting the other's native hash observation.
        overlap = {
            ("object_current", "typed-graph-source", 17, 1): -11,
            ("edge_current", "typed-graph-source", 17, 1): -22,
        }
        self.assertEqual(len(overlap), 2)
        values = self.lookup_hashes(plan)
        with self.assertRaises(ValueError):
            materialize_delta4_expected_rows(plan, {**values, **overlap})

    def native_fixture(self, plan, *, bad_profile=False, omit_result=False, bad_columns=False):
        owner = object.__new__(LocalDeltaTransport)
        targets = {}
        roles = {}
        for role in INSTALLATION_ROLES:
            table = "local.configured." + role
            target = SimpleNamespace(table=table, path=Path("/tmp") / role, uuid=str(uuid.uuid5(uuid.NAMESPACE_URL, role)))
            targets[table] = target
            roles[role] = {"table": table, "path": str(target.path), "uuid": target.uuid}
        owner.targets = MappingProxyType(targets)
        profile = {
            "profile": "ashlar-local-current-installation-profile/0.1",
            "physical_profile": "ashlar-delta/0.4-xxhash64",
            "lookup_hash_profile": "spark-xxhash64-typed-key/0.1",
            "roles": roles,
        }
        if bad_profile:
            profile["physical_profile"] = "ashlar-delta/0.3"
        owner.installation_profile = profile
        calls = []

        def query(sql, parameters):
            calls.append((sql, parameters))
            self.assertIn("xxhash64(CAST(k.source_system AS STRING),CAST(k.type_id AS BIGINT),CAST(k.id AS BIGINT))", sql)
            keys = json.loads(parameters["keys"])
            rows = [{"role": key["role"], "source_system": key["source_system"],
                     "type_id": key["type_id"], "id": key["id"], "lookup_hash": -(index + 1)}
                    for index, key in enumerate(keys)]
            columns = (("role", "STRING"), ("source_system", "STRING"), ("type_id", "STRING"),
                       ("id", "BIGINT"), ("lookup_hash", "BIGINT")) if bad_columns else (
                       ("role", "STRING"), ("source_system", "STRING"), ("type_id", "BIGINT"),
                       ("id", "BIGINT"), ("lookup_hash", "BIGINT"))
            return SimpleNamespace(rows=rows[:-1] if omit_result else rows, columns=columns)

        owner.query = query
        return owner, profile, calls

    def test_native_hash_observation_is_one_bounded_read_for_exact_six_role_installation(self):
        plan = self.plan()
        owner, profile, calls = self.native_fixture(plan)
        hashes = observe_delta4_lookup_hashes(plan, owner, profile)
        self.assertEqual(len(calls), 1)
        self.assertEqual(len(hashes), 8)
        self.assertEqual({key[0] for key in hashes}, {"object_current", "edge_current"})

    def test_native_hash_observation_refuses_wrong_profile_or_incomplete_result(self):
        plan = self.plan()
        owner, profile, calls = self.native_fixture(plan, bad_profile=True)
        with self.assertRaises(ValueError):
            observe_delta4_lookup_hashes(plan, owner, profile)
        self.assertEqual(calls, [])
        owner, profile, calls = self.native_fixture(plan)
        owner.installation_profile = None
        with self.assertRaises(ValueError):
            observe_delta4_lookup_hashes(plan, owner, profile)
        self.assertEqual(calls, [])
        owner, profile, calls = self.native_fixture(plan, omit_result=True)
        with self.assertRaises(ValueError):
            observe_delta4_lookup_hashes(plan, owner, profile)
        self.assertEqual(len(calls), 1)
        owner, profile, calls = self.native_fixture(plan, bad_columns=True)
        with self.assertRaises(ValueError):
            observe_delta4_lookup_hashes(plan, owner, profile)
        self.assertEqual(len(calls), 1)

    def test_realized_vector_binds_separately_supplied_retained_manifest(self):
        versions = {"local.configured." + role: index for index, role in enumerate(DATA_ROLES, 1)}
        # The helper receives a retained row; the pure `.4` core exposes no
        # manifest-minting function and makes no claim that this JSON is true.
        manifest = {
            "publication_id": "retained-native-publication-1",
            "profile_version": "ashlar-delta/0.4-xxhash64",
            "table_versions_json": __import__("json").dumps(versions, sort_keys=True, separators=(",", ":")),
            "source_progress_json": '{"gate3-typed-graph":{}}',
            "schema_revisions_json": '{"typed-graph":"1"}',
            "validation_report_json": '{"complete":true}',
            "recorded_at": "1791547200000001",
        }
        installation = {}
        for index, role in enumerate(INSTALLATION_ROLES, 1):
            installation[role] = {
                "name": "local.configured." + role,
                "uuid": str(uuid.uuid5(uuid.NAMESPACE_URL, role)),
                "version": versions["local.configured." + role] if role in DATA_ROLES else 0,
                "schema_sha256": "c" * 64,
                "protocol_sha256": "d" * 64,
            }
        vector = build_delta4_realized_vector(manifest, installation)
        self.assertEqual(vector["carrierProfile"], "ashlar-delta/0.4-xxhash64")
        self.assertEqual(set(vector["dataRoles"]), set(DATA_ROLES))
        self.assertEqual(set(vector["installationInventory"]), set(INSTALLATION_ROLES))
        self.assertEqual(len(vector["canonicalSha256"]), 64)
        changed = dict(manifest, profile_version="ashlar-delta/0.3")
        with self.assertRaises(ValueError):
            build_delta4_realized_vector(changed, installation)


if __name__ == "__main__":
    unittest.main()
