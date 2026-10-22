# SPDX-License-Identifier: MIT OR Apache-2.0
import hashlib
import json
import unittest
from types import SimpleNamespace

from ashlar_host.graph_sql import TABLES, graph_sql_plan
from ashlar.apply import Change, EntityKey, EntityState, empty_state
from ashlar.source import jsonl_batches, records_digest
from ashlar_host.managed_current_carriers import load_current_managed_carriers
from ashlar_host.managed_current_incremental import ManagedCurrentAffectedReader


class Tests(unittest.TestCase):
    @staticmethod
    def whole_batch(batch_id, events):
        encode = lambda value: json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()
        event_bytes = [encode({"kind": "event", **event}) + b"\n" for event in events]
        commit = {"kind": "commit", "batch_id": batch_id,
                  "record_count": len(event_bytes),
                  "records_sha256": records_digest(event_bytes)}
        lines = [encode({"kind": "begin", "batch_id": batch_id}) + b"\n",
                 *event_bytes, encode(commit) + b"\n"]
        return next(jsonl_batches(lines, feed="feed-opaque", epoch="epoch-opaque"))

    @staticmethod
    def whole_event(delivery_id, *, kind, type_id, identity, version, operation,
                    props_json="{}", retained_json="{}", endpoints=None):
        event = {"source_profile": "ashlar-whole-entity/0.1",
                 "source_system": "source-雪", "schema_revision": "schema-r1",
                 "retained_json": retained_json, "delivery_id": delivery_id,
                 "entity_kind": kind, "type_id": str(type_id), "id": str(identity),
                 "entity_version": str(version), "operation": operation,
                 "props_json": props_json}
        if endpoints is not None:
            event["endpoints"] = [{"type_id": str(t), "id": str(i)} for t, i in endpoints]
        return event
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

    def test_xxhash64_staging_uses_exact_bigints_and_preserves_opaque_strings(self):
        low, high = -(1 << 63), (1 << 63) - 1
        object_low_create = self.whole_event(
            "delivery:create:雪", kind="object", type_id=low, identity=low,
            version=1, operation="create", props_json='{ "exact": 9007199254740993 }',
            retained_json='{"cursor":{"opaque":"001"}}')
        object_low_replace = self.whole_event(
            "delivery:replace:1:雪", kind="object", type_id=low, identity=low,
            version=2, operation="replace", props_json='{"exact":9007199254740994}',
            retained_json='{"cursor":{"opaque":"001"}}')
        object_low_replace_again = self.whole_event(
            "delivery:replace:2:雪", kind="object", type_id=low, identity=low,
            version=3, operation="replace", props_json='{"exact":9007199254740995}',
            retained_json='{"cursor":{"opaque":"001"}}')
        object_high = self.whole_event(
            "delivery:high", kind="object", type_id=high, identity=high,
            version=high, operation="create", props_json='{"boundary":"max"}')
        edge = self.whole_event(
            "delivery:edge", kind="edge", type_id=low, identity=9007199254740993,
            version=1, operation="create", endpoints=((low, low), (high, high)),
            props_json='{}', retained_json='{"edge":"parallel-safe"}')
        ephemeral_edge = self.whole_event(
            "delivery:edge-ephemeral-create", kind="edge", type_id=low, identity=-9007199254740993,
            version=1, operation="create", endpoints=((low, low), (high, high)),
            props_json='{}', retained_json='{"edge":"create-delete"}')
        ephemeral_edge_delete = self.whole_event(
            "delivery:edge-ephemeral-delete", kind="edge", type_id=low, identity=-9007199254740993,
            version=2, operation="delete", endpoints=((low, low), (high, high)),
            props_json='{}', retained_json='{"edge":"create-delete"}')
        create_batch = self.whole_batch("create-update-edge", [
            object_low_create, object_low_replace, object_low_replace_again,
            object_high, edge, ephemeral_edge, ephemeral_edge_delete])
        tables = {role: "cat.schema." + role for role in TABLES}
        after, steps = graph_sql_plan(empty_state(), create_batch, tables,
            materialized_at="2026-10-12T00:00:00+00:00", schema_policy=lambda _change: None,
            lookup_hash_profile="spark-xxhash64-typed-key/0.1")

        node_insert = next(step for step in steps if step["statement"].startswith(
            "INSERT INTO `cat`.`schema`.`object_current`"))
        edge_insert = next(step for step in steps if step["statement"].startswith(
            "INSERT INTO `cat`.`schema`.`edge_current`"))
        node_sql, edge_sql = node_insert["statement"], edge_insert["statement"]
        self.assertIn("ARRAY<STRUCT<source_system:STRING,type_id:BIGINT,id:BIGINT", node_sql)
        self.assertIn("entity_version:BIGINT", node_sql)
        self.assertIn("r.type_id AS type_id,r.id AS id", node_sql)
        self.assertNotIn("cast(r.id AS BIGINT)", node_sql)
        self.assertIn("ARRAY<STRUCT<source_system:STRING,rel_type_id:BIGINT,id:BIGINT", edge_sql)
        self.assertIn("source_id:BIGINT,target_type:BIGINT,target_id:BIGINT", edge_sql)
        self.assertIn("xxhash64(CAST(r.source_system AS STRING),CAST(r.type_id AS BIGINT),CAST(r.id AS BIGINT)) AS lookup_hash", node_sql)
        self.assertIn("xxhash64(CAST(r.source_system AS STRING),CAST(r.rel_type_id AS BIGINT),CAST(r.id AS BIGINT)) AS lookup_hash", edge_sql)

        node_rows = json.loads(node_insert["parameters"]["rows"])
        low_row = next(row for row in node_rows if row["id"] == low)
        high_row = next(row for row in node_rows if row["id"] == high)
        self.assertIs(type(low_row["type_id"]), int)
        self.assertEqual((low_row["type_id"], low_row["id"], low_row["entity_version"]), (low, low, 3))
        self.assertEqual((high_row["type_id"], high_row["id"], high_row["entity_version"]), (high, high, high))
        self.assertEqual(low_row["source_delivery_id"], "delivery:replace:2:雪")
        self.assertEqual(json.loads(low_row["source_cursor_json"]),
                         {"profile": create_batch.profile, "offset": create_batch.cursor_after})
        self.assertEqual(low_row["props_json"], '{"exact":9007199254740995}')
        self.assertEqual(low_row["retained_json"], '{"cursor":{"opaque":"001"}}')
        edge_rows = json.loads(edge_insert["parameters"]["rows"])
        self.assertEqual(len(edge_rows), 1)  # create->delete has no final current afterimage
        edge_row = edge_rows[0]
        self.assertEqual((edge_row["rel_type_id"], edge_row["id"], edge_row["entity_version"],
                          edge_row["source_type"], edge_row["source_id"], edge_row["target_type"], edge_row["target_id"]),
                         (low, 9007199254740993, 1, low, low, high, high))
        self.assertEqual(edge_row["source_delivery_id"], "delivery:edge")
        self.assertEqual(edge_row["retained_json"], '{"edge":"parallel-safe"}')
        self.assertEqual(len(after.current), 3)
        ephemeral_key = EntityKey("source-雪", "edge", low, -9007199254740993)
        self.assertNotIn(ephemeral_key, after.current)
        self.assertEqual(after.tombstones[ephemeral_key].state.version, 2)
        history = next(step for step in steps if step["statement"].startswith(
            "INSERT INTO `cat`.`schema`.`whole_source_history`"))
        history_rows = json.loads(history["parameters"]["rows"])
        self.assertEqual(len(history_rows), 7)
        delivery_ids = [json.loads(row["change_json"])["delivery_id"] for row in history_rows]
        self.assertIn("delivery:edge-ephemeral-create", delivery_ids)
        self.assertIn("delivery:edge-ephemeral-delete", delivery_ids)
        create_delete_tombstone = next(step for step in steps if step["statement"].startswith(
            "INSERT INTO `cat`.`schema`.`tombstone`"))
        tombstone_row = json.loads(create_delete_tombstone["parameters"]["rows"])[0]
        self.assertEqual((tombstone_row["id"], tombstone_row["entity_version"],
                          tombstone_row["source_delivery_id"]),
                         (-9007199254740993, 2, "delivery:edge-ephemeral-delete"))

        delete_batch = self.whole_batch("delete-edge", [self.whole_event(
            "delivery:delete-edge", kind="edge", type_id=low, identity=9007199254740993,
            version=2, operation="delete", endpoints=((low, low), (high, high)),
            retained_json='{"edge":"parallel-safe"}')])
        deleted, delete_steps = graph_sql_plan(after, delete_batch, tables,
            materialized_at="2026-10-12T00:01:00+00:00", schema_policy=lambda _change: None,
            lookup_hash_profile="spark-xxhash64-typed-key/0.1")
        edge_merge = next(step for step in delete_steps if step["statement"].startswith(
            "MERGE INTO `cat`.`schema`.`edge_current`"))
        self.assertIn("ARRAY<STRUCT<source_system:STRING,type_id:BIGINT,id:BIGINT>>", edge_merge["statement"])
        merge_keys = json.loads(edge_merge["parameters"]["keys"])
        self.assertEqual(merge_keys, [{"source_system": "source-雪", "type_id": low, "id": 9007199254740993}])
        tombstone_insert = next(step for step in delete_steps if step["statement"].startswith(
            "INSERT INTO `cat`.`schema`.`tombstone`"))
        tombstone_row = json.loads(tombstone_insert["parameters"]["rows"])[0]
        self.assertEqual((tombstone_row["type_id"], tombstone_row["id"], tombstone_row["entity_version"]),
                         (low, 9007199254740993, 2))
        self.assertIs(type(tombstone_row["source_delivery_id"]), str)
        replay, replay_steps = graph_sql_plan(deleted, delete_batch, tables,
            materialized_at="2026-10-12T00:02:00+00:00", schema_policy=lambda _change: None,
            lookup_hash_profile="spark-xxhash64-typed-key/0.1")
        self.assertEqual(replay, deleted)
        self.assertEqual(replay_steps, [])
        replay_create, replay_create_steps = graph_sql_plan(after, create_batch, tables,
            materialized_at="2026-10-12T00:03:00+00:00", schema_policy=lambda _change: None,
            lookup_hash_profile="spark-xxhash64-typed-key/0.1")
        self.assertEqual(replay_create, after)
        self.assertEqual(replay_create_steps, [])

    def test_latest_change_index_uses_linear_key_lookups(self):
        from unittest.mock import patch

        events = [self.whole_event(
            f"delivery-{identity}", kind="object", type_id=41, identity=identity,
            version=1, operation="create", props_json="{}")
            for identity in range(128)]
        batch = self.whole_batch("many-distinct-identities", events)
        comparisons = [0]
        original_eq = EntityKey.__eq__

        def counted_eq(left, right):
            comparisons[0] += 1
            return original_eq(left, right)

        with patch.object(EntityKey, "__eq__", counted_eq):
            _after, steps = graph_sql_plan(empty_state(), batch,
                {role: "cat.schema." + role for role in TABLES},
                materialized_at="2026-10-12T00:00:00+00:00", schema_policy=lambda _change: None,
                lookup_hash_profile="spark-xxhash64-typed-key/0.1")
        self.assertEqual(len([step for step in steps if step["statement"].startswith(
            "INSERT INTO `cat`.`schema`.`object_current`")]), 1)
        # Distinct identity hashes should use the latest-change dict rather
        # than scanning the whole fresh page once for every touched key.
        self.assertLessEqual(comparisons[0], 4 * len(events))

    def test_legacy_sql_staging_bytes_remain_unchanged(self):
        from tests.test_truss_graph_sql import Tests as GraphTests
        batch, _raw = GraphTests().batch()
        _after, steps = graph_sql_plan(empty_state(), batch,
            {role: "cat.schema." + role for role in TABLES},
            materialized_at="2026-10-12T00:00:00+00:00", schema_policy=lambda _change: None,
            lookup_hash_profile="sha256-json-v1")
        merge = next(step["statement"] for step in steps if step["statement"].startswith(
            "MERGE INTO `cat`.`schema`.`object_current`"))
        insert = next(step["statement"] for step in steps if step["statement"].startswith(
            "INSERT INTO `cat`.`schema`.`object_current`"))
        self.assertEqual(hashlib.sha256(merge.encode()).hexdigest(),
                         "4d4fa204cd188c41e82f2229656abe8bf342a9b0261b1ae7a53cb80b4ac635c9")
        self.assertEqual(hashlib.sha256(insert.encode()).hexdigest(),
                         "11a94cad1472c864f15cbcff2d90a8b8b6b7a90882cfb840fd82d4caff5f47c9")
        key_rows = json.loads(next(step["parameters"]["keys"] for step in steps if step["statement"] == merge))
        self.assertEqual(key_rows, [{"source_system": "installation", "type_id": "3", "id": "7"}])
        staged = json.loads(next(step["parameters"]["rows"] for step in steps if step["statement"] == insert))[0]
        self.assertEqual((staged["type_id"], staged["id"], staged["entity_version"]), ("3", "7", "1"))

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
