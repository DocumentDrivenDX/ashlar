# SPDX-License-Identifier: MIT OR Apache-2.0
import base64
import hashlib
import json
import unittest
from types import SimpleNamespace

from ashlar.apply import Change, EntityKey, EntityState
from ashlar_host.managed_current_incremental import (
    AffectedRead, ManagedCurrentAffectedReader, canonical_intent, reduce_affected_page,
)


class Frame:
    def __init__(self, rows):
        self.rows = rows
        self.limited = None
        self.schema = SimpleNamespace(fields=[SimpleNamespace(name="value", dataType=SimpleNamespace(simpleString=lambda: "string"))])

    def limit(self, count):
        self.limited = count
        return self

    def collect(self):
        return self.rows


class Spark:
    def __init__(self, frame):
        self.frame = frame

    def sql(self, sql, args=None):
        return self.frame


class Tests(unittest.TestCase):
    def test_transport_limits_on_server_before_collect(self):
        from ashlar_host.managed_current_publication import ManagedCurrentSparkTransport
        frame = Frame([SimpleNamespace(asDict=lambda recursive=True: {"value": "x"})])
        result = ManagedCurrentSparkTransport(Spark(frame), maximum_rows=2).query("SELECT 1", {})
        self.assertEqual(frame.limited, 3)
        self.assertEqual(result.rows, ({"value": "x"},))

    def test_affected_reducer_accepts_exact_duplicate_marker_without_new_state(self):
        raw = b'{"original":"event"}'
        digest = hashlib.sha256(raw).hexdigest()
        change = Change("feed", "epoch", "delivery-1", digest, "create",
                        EntityState(EntityKey("feed", "object", 5, 9), 1, "rev",
                                    '{"name":null}', "{}"))
        marker = {"feed": "feed", "epoch": "epoch", "delivery_id": "delivery-1",
                  "digest": change.raw_digest,
                  "change_json": json.dumps(__import__("dataclasses").asdict(change), separators=(",", ":")),
                  "raw_base64": base64.b64encode(raw).decode()}
        read = AffectedRead({"object_current": (), "edge_current": ()}, (), (marker,), {}, 0,
                            hashlib.sha256(raw).hexdigest())
        calls = []
        _prior, result = reduce_affected_page((change,), read, schema_policy=lambda item: calls.append(item))
        self.assertEqual(dict(result.current), {})
        self.assertEqual(calls, [change])

    def test_native_reader_rejects_unrequested_returned_identity(self):
        class Transport:
            def query(self, sql, parameters):
                if "object_current" in sql:
                    return SimpleNamespace(rows=({"source_system": "other", "type_id": 5, "id": 9},))
                return SimpleNamespace(rows=())
        key = EntityKey("feed", "object", 5, 9)
        change = Change("feed", "epoch", "delivery-1", "a" * 64, "create",
                        EntityState(key, 1, "rev", "{}", "{}"))
        tables = {role: "client_dev.trial." + role for role in
                  ("object_current", "edge_current", "tombstone", "whole_source_history")}
        with self.assertRaisesRegex(ValueError, "unrequested identities"):
            ManagedCurrentAffectedReader(Transport(), tables).read((change,))

    def test_conflicting_reused_delivery_refuses(self):
        raw = b'{"original":"event"}'
        original = Change("feed", "epoch", "delivery-1", hashlib.sha256(raw).hexdigest(), "create",
                          EntityState(EntityKey("feed", "object", 5, 9), 1, "rev", "{}", "{}"))
        conflicting = Change("feed", "epoch", "delivery-1", "b" * 64, "create",
                             EntityState(EntityKey("feed", "object", 5, 9), 1, "rev", '{"x":1}', "{}"))
        marker = {"feed": "feed", "epoch": "epoch", "delivery_id": "delivery-1",
                  "digest": original.raw_digest,
                  "change_json": json.dumps(__import__("dataclasses").asdict(original), separators=(",", ":")),
                  "raw_base64": base64.b64encode(raw).decode()}
        read = AffectedRead({"object_current": (), "edge_current": ()}, (), (marker,), {}, 0, "0" * 64)
        with self.assertRaisesRegex(ValueError, "conflicts"):
            reduce_affected_page((conflicting,), read, schema_policy=lambda _item: None)

    def test_intent_is_compact_and_versioned(self):
        roles = ("object_current", "edge_current", "tombstone", "whole_source_history",
                 "publication_manifest", "publication_attempt_phase")
        body, encoded = canonical_intent(request={"digest": "x"}, predecessor_manifest={"id": "p"},
            four_role_heads={"c.s." + role: 0 for role in roles[:4]},
            six_role_inventory={role: {"name": "c.s." + role, "uuid": role, "version": 0,
                "schema": [], "protocol": {}} for role in roles},
            touched_keys=[["feed", "object", 1, 2]], afterimages={"object_current": []},
            raw_delivery_digests=["a" * 64], effects=[], source_progress={"feed": "next"})
        self.assertEqual(body["profile"], "ashlar-managed-current-incremental/0.1")
        self.assertNotIn("baselineDataRows", encoded)
        self.assertNotIn("expectedRows", encoded)

    def test_page_reduction_preserves_null_and_requires_final_endpoints(self):
        old = EntityState(EntityKey("feed", "object", 5, 9), 1, "rev", '{"field":"old"}', "{}")
        replacement = Change("feed", "epoch", "delivery-2", "c" * 64, "replace",
            EntityState(old.key, 2, "rev", '{"field":null}', "{}"))
        row = {"source_system": "feed", "type_id": 5, "id": 9, "entity_version": 1,
            "schema_revision": "rev", "props_json": old.props_json, "retained_json": "{}"}
        read = AffectedRead({"object_current": (row,), "edge_current": ()}, (), (), {}, 0, "0" * 64)
        _prior, after = reduce_affected_page((replacement,), read, schema_policy=lambda _change: None)
        self.assertEqual(after.current[old.key].props_json, '{"field":null}')

        missing_endpoint = Change("feed", "epoch", "edge-1", "d" * 64, "create",
            EntityState(EntityKey("feed", "edge", 8, 2), 1, "rev", "{}", "{}",
                (old.key, EntityKey("feed", "object", 6, 11))))
        with self.assertRaisesRegex(ValueError, "Dangling typed endpoint"):
            reduce_affected_page((missing_endpoint,), read, schema_policy=lambda _change: None)

    def test_node_delete_uses_scalar_survivor_witness(self):
        first = EntityKey("feed", "object", 5, 9)
        second = EntityKey("feed", "object", 5, 10)
        edge_key = EntityKey("feed", "edge", 8, 3)
        object_state = EntityState(first, 1, "rev", "{}", "{}")
        edge_state = EntityState(edge_key, 1, "rev", "{}", "{}", (first, second))
        delete_node = Change("feed", "epoch", "node-del", "e" * 64, "delete",
                             EntityState(first, 2, "rev", "{}", "{}"))
        object_row = {"source_system": "feed", "type_id": 5, "id": 9, "entity_version": 1,
            "schema_revision": "rev", "props_json": "{}", "retained_json": "{}"}
        read = AffectedRead({"object_current": (object_row,), "edge_current": ()}, (), (), {}, 1, "0" * 64)
        with self.assertRaisesRegex(ValueError, "surviving predecessor edge"):
            reduce_affected_page((delete_node,), read, schema_policy=lambda _change: None)

        remove_edge = Change("feed", "epoch", "edge-del", "f" * 64, "delete",
                             EntityState(edge_key, 2, "rev", "{}", "{}", (first, second)))
        edge_row = {"source_system": "feed", "rel_type_id": 8, "id": 3, "entity_version": 1,
            "schema_revision": "rev", "props_json": "{}", "retained_json": "{}",
            "source_type": 5, "source_id": 9, "target_type": 5, "target_id": 10}
        second_row = {**object_row, "id": 10}
        read = AffectedRead({"object_current": (object_row, second_row), "edge_current": (edge_row,)},
                            (), (), {}, 0, "0" * 64)
        prior, after = reduce_affected_page((remove_edge, delete_node), read,
                                            schema_policy=lambda _change: None)
        self.assertIn(edge_key, prior.current)
        self.assertNotIn(edge_key, after.current)

    def test_native_reader_filters_affected_keys_and_uses_scalar_delete_gate(self):
        class Transport:
            def __init__(self): self.calls = []
            def query(self, sql, parameters):
                self.calls.append((sql, parameters))
                rows = ({"remaining": 1},) if "COUNT(1) AS remaining" in sql else ()
                return SimpleNamespace(rows=rows)
        first = EntityKey("feed", "object", 5, 9)
        remove = Change("feed", "epoch", "node-del", "e" * 64, "delete",
                        EntityState(first, 2, "rev", "{}", "{}"))
        transport = Transport()
        tables = {role: "client_dev.trial." + role for role in
                  ("object_current", "edge_current", "tombstone", "whole_source_history")}
        read = ManagedCurrentAffectedReader(transport, tables).read((remove,))
        selects = [sql for sql, _ in transport.calls if sql.startswith("SELECT")]
        self.assertTrue(all(" WHERE " in sql for sql in selects))
        self.assertTrue(any("COUNT(1) AS remaining" in sql for sql in selects))
        self.assertEqual(read.surviving_incident_edge_count, 1)
        self.assertFalse(any("SELECT * FROM" in sql for sql in selects))


if __name__ == "__main__":
    unittest.main()
