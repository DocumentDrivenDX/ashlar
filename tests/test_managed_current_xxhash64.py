# SPDX-License-Identifier: MIT OR Apache-2.0
import json
import unittest
from types import SimpleNamespace

from ashlar_host.graph_sql import TABLES, graph_sql_plan
from ashlar.apply import Change, EntityKey, EntityState, empty_state
from ashlar_host.managed_current_carriers import load_current_managed_carriers
from ashlar_host.managed_current_incremental import ManagedCurrentAffectedReader


class Tests(unittest.TestCase):
    def test_new_profile_changes_only_lookup_representation_and_preserves_legacy(self):
        legacy = load_current_managed_carriers("ashlar-delta/0.3")
        current = load_current_managed_carriers("ashlar-delta/0.4-xxhash64")
        self.assertEqual(legacy.ordered_fields("object_current"),
                         load_current_managed_carriers().ordered_fields("object_current"))
        for role in ("object_current", "edge_current"):
            old = dict((name, (kind, nullable)) for name, kind, nullable in legacy.ordered_fields(role))
            new = dict((name, (kind, nullable)) for name, kind, nullable in current.ordered_fields(role))
            self.assertEqual(set(old), set(new))
            self.assertEqual(old["lookup_hash"], ("STRING", False))
            self.assertEqual(new["lookup_hash"], ("BIGINT", False))
            self.assertTrue(current.create_sql(role, ("cat", "schema", role)).find("BIGINT NOT NULL") >= 0)
        self.assertEqual(current.physical_profile, "ashlar-delta/0.4-xxhash64")
        self.assertEqual(current.lookup_hash_profile, "spark-xxhash64-typed-key/0.1")

    def test_native_writer_uses_typed_spark_hash_and_does_not_python_hash_rows(self):
        from tests.test_truss_graph_sql import Tests as GraphTests
        batch, _raw = GraphTests().batch()
        _after, steps = graph_sql_plan(empty_state(), batch,
            {role: "cat.schema." + role for role in TABLES},
            materialized_at="2026-10-12T00:00:00+00:00", schema_policy=lambda _change: None,
            lookup_hash_profile="spark-xxhash64-typed-key/0.1")
        object_insert = next(step for step in steps if step["statement"].startswith("INSERT INTO `cat`.`schema`.`object_current`"))
        self.assertIn("xxhash64(CAST(r.source_system AS STRING),CAST(r.type_id AS BIGINT),CAST(r.id AS BIGINT)) AS lookup_hash",
                      object_insert["statement"])
        row = json.loads(object_insert["parameters"]["rows"])[0]
        self.assertNotIn("lookup_hash", row)

    def test_collision_probe_filters_wrong_key_and_rejects_duplicate_identity(self):
        class Transport:
            def __init__(self, duplicate=False):
                self.calls = []
                self.duplicate = duplicate
                self.candidates = (
                    {"source_system": "feed", "type_id": 5, "id": 9, "lookup_hash": 42},
                    # Deliberate collision: same hash, different exact key.
                    {"source_system": "feed", "type_id": 8, "id": 90, "lookup_hash": 42},
                )
            def query(self, sql, parameters):
                self.calls.append((sql, parameters))
                if "object_current" in sql:
                    key = (parameters["obj_0_source"], int(parameters["obj_0_type"]),
                           int(parameters["obj_0_id"]))
                    # Simulate SQL predicate evaluation: both candidates have
                    # the requested hash, then full-key terms filter out the
                    # colliding identity. Optionally return a physical duplicate.
                    rows = tuple(row for row in self.candidates
                                 if row["lookup_hash"] == 42
                                 and (row["source_system"], row["type_id"], row["id"]) == key)
                    if self.duplicate and rows:
                        rows += (dict(rows[0]),)
                    return SimpleNamespace(rows=rows)
                return SimpleNamespace(rows=())

        tables = {role: "cat.schema." + role for role in
                  ("object_current", "edge_current", "tombstone", "whole_source_history")}
        transport = Transport()
        reader = ManagedCurrentAffectedReader(transport, tables,
            lookup_hash_profile="spark-xxhash64-typed-key/0.1")
        change = Change("feed", "epoch", "delivery", "a" * 64, "create",
            EntityState(EntityKey("feed", "object", 5, 9), 1, "rev", "{}", "{}"))
        read = reader.read((change,))
        self.assertEqual([(row["source_system"], row["type_id"], row["id"])
                          for row in read.current["object_current"]], [("feed", 5, 9)])
        sql, params = transport.calls[0]
        self.assertIn("xxhash64(CAST(:obj_0_source AS STRING),CAST(:obj_0_type AS BIGINT),CAST(:obj_0_id AS BIGINT))", sql)
        self.assertIn("t.source_system=:obj_0_source", sql)
        self.assertIn("t.type_id=CAST(:obj_0_type AS BIGINT)", sql)
        self.assertIn("t.id=CAST(:obj_0_id AS BIGINT)", sql)
        self.assertEqual(params, {"obj_0_source": "feed", "obj_0_type": "5", "obj_0_id": "9"})

        duplicate_reader = ManagedCurrentAffectedReader(Transport(duplicate=True), tables,
            lookup_hash_profile="spark-xxhash64-typed-key/0.1")
        with self.assertRaisesRegex(ValueError, "duplicate or unrequested identities"):
            duplicate_reader.read((change,))

    def test_legacy_full_snapshot_path_refuses_xxhash64_carrier(self):
        from ashlar_host.managed_current_publication import ManagedCurrentGraphPublicationDriver
        driver = object.__new__(ManagedCurrentGraphPublicationDriver)
        driver.carriers = load_current_managed_carriers("ashlar-delta/0.4-xxhash64")
        with self.assertRaisesRegex(ValueError, "affected-key incremental publisher"):
            driver._prior_state_from_history()

    def test_native_hash_result_accepts_signed_negative_bigint(self):
        class Transport:
            def query(self, _sql, _parameters):
                return SimpleNamespace(rows=({"source_system": "feed", "type_id": 5, "id": 9,
                                              "lookup_hash": -(1 << 63)},))

        tables = {role: "cat.schema." + role for role in
                  ("object_current", "edge_current", "tombstone", "whole_source_history")}
        reader = ManagedCurrentAffectedReader(Transport(), tables,
            lookup_hash_profile="spark-xxhash64-typed-key/0.1")
        result = reader.native_hashes("type_id", [{"source_system": "feed", "type_id": 5, "id": 9}])
        self.assertEqual(result[("feed", 5, 9)], -(1 << 63))


if __name__ == "__main__":
    unittest.main()
