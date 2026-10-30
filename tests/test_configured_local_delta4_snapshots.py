# SPDX-License-Identifier: MIT OR Apache-2.0
"""Inert six-role `.4` fresh-head and post-effect snapshot admission checks."""
from pathlib import Path
import sqlite3
import hashlib
import json
import re
from types import MappingProxyType, SimpleNamespace
import unittest
import uuid
from unittest.mock import patch

from ashlar.typed_graph_source_config import load_typed_graph_jsonl_configuration
from ashlar_host.configured_local_delta4_publication import (
    DATA_ROLES, INSTALLATION_ROLES, admit_delta4_fresh_empty_installation,
    materialize_delta4_expected_rows, plan_configured_local_delta4_transaction1,
    rehydrate_delta4_fresh_head_admission, validate_delta4_transaction1_snapshots,
)
from ashlar_host.delta_custody import LocalDeltaTransport
from ashlar_host.managed_current_carriers import load_current_managed_carriers


ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "examples/end-to-end/configured-typed-graph-source.json"
PROTOCOL_PROFILE = "ashlar-configured-local-delta/0.1"


class FakeTransport:
    def __init__(self, plan, *, changed_uuid=None, changed_version=None, row_fault=None,
                 path_detail_mismatch=None, table_location_mismatch=None):
        owner = object.__new__(LocalDeltaTransport)
        self.owner = owner
        carriers = load_current_managed_carriers("ashlar-delta/0.4-xxhash64")
        targets = {}
        self.profile = {
            "profile": "ashlar-local-current-installation-profile/0.1",
            "physical_profile": "ashlar-delta/0.4-xxhash64",
            "lookup_hash_profile": "spark-xxhash64-typed-key/0.1",
            "roles": {},
        }
        self.versions = {role: 1 for role in INSTALLATION_ROLES}
        self.histories = {role: [1, 0] for role in INSTALLATION_ROLES}
        self.counts = {role: 0 for role in INSTALLATION_ROLES}
        self.row_fault = row_fault
        self.path_detail_mismatch = path_detail_mismatch
        self.table_location_mismatch = table_location_mismatch
        self.expected = None
        self.baseline_rows = {role: [] for role in INSTALLATION_ROLES}
        self.plan = plan
        self.lookup_hashes = {}
        self.native_metadata = {role: {} for role in INSTALLATION_ROLES}
        self.native_operations = {role: {0: "CREATE TABLE", 1: "SET TBLPROPERTIES"}
                                 for role in INSTALLATION_ROLES}
        for role in ("object_current", "edge_current"):
            column = "type_id" if role == "object_current" else "rel_type_id"
            for key in plan.pending_lookup_keys[role]:
                identity = (role, key["source_system"], key[column], key["id"])
                self.lookup_hashes[identity] = -(len(self.lookup_hashes) + 1)
        for role in INSTALLATION_ROLES:
            table = "local.configured." + role
            path = Path("/tmp") / role
            target_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, role))
            target = SimpleNamespace(table=table, path=path, uuid=target_uuid)
            targets[table] = target
            kind_fields = carriers.ordered_fields(role)
            self.profile["roles"][role] = {
                "table": table, "path": str(path), "uuid": target_uuid,
                "fields": [list(item) for item in kind_fields],
                "protocol": {"uuid": target_uuid, "profile": PROTOCOL_PROFILE,
                             "reader_version": 1, "writer_version": 2, "features": []},
            }
        owner.targets = MappingProxyType(targets)
        owner.installation_profile = self.profile
        owner.original_history = self.original_history
        owner._snapshot = self.snapshot
        owner._detail = self.path_detail
        owner.query = self.query
        owner.spark = self.spark_port()
        owner.db = sqlite3.connect(":memory:")
        for table in ("local_operation", "local_plan", "local_publication_artifact"):
            owner.db.execute("CREATE TABLE " + table + " (id INTEGER)")
        owner.db.execute("CREATE TABLE local_source_plan (request_digest TEXT PRIMARY KEY,original TEXT NOT NULL)")
        if changed_uuid:
            self.profile["roles"][changed_uuid]["uuid"] = str(uuid.uuid5(uuid.NAMESPACE_URL, "wrong-uuid"))
        if changed_version:
            self.histories[changed_version] = [3, 2, 1, 0]
        self.owner = owner

    @property
    def targets(self):
        return self.owner.targets

    def original_history(self, target):
        role = target.table.rsplit(".", 1)[1]
        return [{"version": value,
                 "operation": self.native_operations[role].get(value),
                 **({"userMetadata": self.native_metadata[role][value]}
                    if value in self.native_metadata[role] else {})}
                for value in self.histories[role]]

    def snapshot(self, target, version):
        role = target.table.rsplit(".", 1)[1]
        carriers = load_current_managed_carriers("ashlar-delta/0.4-xxhash64")
        fields = carriers.ordered_fields(role)
        source_rows = (self.baseline_rows[role] if version == 1 else
                       self.expected[role] if self.expected is not None else [])
        rows = [{name: str(value) if type(value) is int else value for name, value in row.items()}
                for row in source_rows]
        rows.sort(key=lambda row: json.dumps(row, sort_keys=True, separators=(",", ":"),
                                             ensure_ascii=False, allow_nan=False))
        schema = [[name, {"STRING": "string", "BIGINT": "bigint", "TIMESTAMP": "timestamp"}[kind]]
                  for name, kind, _nullable in fields]
        payload = json.dumps(rows, sort_keys=True, separators=(",", ":"),
                             ensure_ascii=False, allow_nan=False)
        return {"schema": schema, "rows": rows,
                "row_sha256": hashlib.sha256(payload.encode()).hexdigest()}

    def path_detail(self, target, *, full=False):
        role = target.table.rsplit(".", 1)[1]
        identity = "wrong-path-uuid" if role == self.path_detail_mismatch else target.uuid
        return {"id": identity, "location": str(target.path)}

    def spark_port(self):
        class Reader:
            def __init__(self, outer): self.outer = outer
            def format(self, _value): return self
            def option(self, _key, _value): return self
            def load(self, path):
                role = Path(path).name
                fields = self.outer.profile["roles"][role]["fields"]
                names = {"STRING": "string", "BIGINT": "long", "TIMESTAMP": "timestamp"}
                schema = SimpleNamespace(fields=[SimpleNamespace(
                    name=name, dataType=SimpleNamespace(typeName=lambda kind=names[kind]: kind),
                    nullable=nullable) for name, kind, nullable in fields])
                return SimpleNamespace(schema=schema)
        return SimpleNamespace(read=Reader(self))

    def query(self, sql, _parameters):
        if "xxhash64(" in sql:
            keys = json.loads(_parameters["keys"])
            rows = []
            for item in keys:
                role = item["role"]
                identity = (role, item["source_system"], item["type_id"], item["id"])
                rows.append({**item, "lookup_hash": self.lookup_hashes[identity]})
            return SimpleNamespace(rows=rows, columns=(
                ("role", "STRING"), ("source_system", "STRING"),
                ("type_id", "BIGINT"), ("id", "BIGINT"),
                ("lookup_hash", "BIGINT")))
        role = sql.replace("`", "").split("local.configured.")[-1].split()[0].split(".")[0]
        if sql.startswith("DESCRIBE DETAIL"):
            selected = self.profile["roles"][role]
            location = str(selected["path"])
            if role == self.table_location_mismatch:
                location = "file:/tmp/cloned-delta-log/" + role
            return SimpleNamespace(rows=[{"id": selected["uuid"], "format": "delta",
                "location": location, "minReaderVersion": 1, "minWriterVersion": 2,
                "tableFeatures": []}], columns=())
        if "SELECT count(*) AS row_count" in sql:
            return SimpleNamespace(rows=[{"row_count": self.counts[role]}],
                                   columns=(("row_count", "BIGINT"),))
        carriers = load_current_managed_carriers("ashlar-delta/0.4-xxhash64")
        names = [name for name, _kind, _nullable in carriers.ordered_fields(role)]
        columns = tuple((name, kind) for name, kind, _nullable in carriers.ordered_fields(role))
        if sql.endswith("LIMIT 0"):
            return SimpleNamespace(rows=[], columns=columns)
        if "VERSION AS OF" in sql:
            version = int(re.search(r"VERSION AS OF (\d+)", sql).group(1))
            rows = (self.baseline_rows[role] if version == 1 else self.expected[role])
            rendered = [{name: str(value) if type(value) is int else value for name, value in row.items()}
                        for row in rows]
            if self.row_fault == (role, "duplicate") and rendered:
                rendered.append(dict(rendered[0]))
            elif self.row_fault == (role, "missing") and rendered:
                rendered.pop()
            return SimpleNamespace(rows=rendered, columns=tuple((name, "STRING") for name in names))
        raise AssertionError("Unexpected fake native query: " + sql)

    def transport(self):
        return self.owner


class Tests(unittest.TestCase):
    def setUp(self):
        configuration = load_typed_graph_jsonl_configuration(CONFIG.resolve())
        self.plan = plan_configured_local_delta4_transaction1(
            configuration, stream="gate3-typed-graph", materialized_at_us="1791547200000000")
        self.hashes = {}
        for role, type_column in (("object_current", "type_id"), ("edge_current", "rel_type_id")):
            for key in self.plan.pending_lookup_keys[role]:
                self.hashes[(role, key["source_system"], key[type_column], key["id"])] = -(len(self.hashes) + 1)
        self.expected = materialize_delta4_expected_rows(self.plan, self.hashes)

    def install_expected(self, fake):
        fake.expected = self.expected

    def fresh(self, fake):
        return admit_delta4_fresh_empty_installation(fake.transport(), fake.profile)

    def mark_effects(self, fake):
        for role in ("object_current", "edge_current", "whole_source_history"):
            fake.histories[role] = [2, 1, 0]

    def test_clean_fresh_and_post_effect_native_bag_path(self):
        fake = FakeTransport(self.plan)
        self.install_expected(fake)
        fresh = self.fresh(fake)
        self.mark_effects(fake)
        effect_versions = {role: 2 for role in DATA_ROLES}
        effect_versions["tombstone"] = 1
        with patch("ashlar_host.local_current_carrier_profile.verify_local_xxhash64_snapshot",
                   side_effect=lambda _spark, _path, _version, role: len(self.expected[role])):
            receipt = validate_delta4_transaction1_snapshots(
                self.plan, fake.transport(), fake.profile, fresh,
                lookup_hashes=self.hashes, effect_versions=effect_versions)
        self.assertEqual(set(receipt["dataRoles"]), set(DATA_ROLES))
        self.assertEqual(set(receipt["installationInventory"]), set(INSTALLATION_ROLES))
        self.assertIn("future retention not established", receipt["snapshotReadability"])

    def test_changed_uuid_version_or_missing_role_refuses(self):
        fake = FakeTransport(self.plan, changed_uuid="object_current")
        with self.assertRaises(ValueError):
            self.fresh(fake)
        fake = FakeTransport(self.plan, changed_version="object_current")
        with self.assertRaises(ValueError):
            self.fresh(fake)
        fake = FakeTransport(self.plan, path_detail_mismatch="object_current")
        with self.assertRaises(ValueError):
            self.fresh(fake)
        fake = FakeTransport(self.plan, table_location_mismatch="object_current")
        with self.assertRaises(ValueError):
            self.fresh(fake)
        for history in ([1, 0, 0], [2, 0]):
            fake = FakeTransport(self.plan)
            fake.histories["object_current"] = history
            with self.assertRaises(ValueError):
                self.fresh(fake)
        fake = FakeTransport(self.plan)
        targets = dict(fake.targets)
        targets.pop("local.configured.tombstone")
        fake.owner.targets = MappingProxyType(targets)
        with self.assertRaises(ValueError):
            self.fresh(fake)

    def test_post_effect_changed_version_and_incomplete_or_duplicate_bags_refuse(self):
        fake = FakeTransport(self.plan)
        self.install_expected(fake)
        fresh = self.fresh(fake)
        self.mark_effects(fake)
        fake.histories["object_current"] = [3, 2, 1, 0]
        effect_versions = {role: 2 for role in DATA_ROLES}
        effect_versions["tombstone"] = 1
        with patch("ashlar_host.local_current_carrier_profile.verify_local_xxhash64_snapshot",
                   side_effect=lambda _spark, _path, _version, role: len(self.expected[role])):
            with self.assertRaises(ValueError):
                validate_delta4_transaction1_snapshots(
                    self.plan, fake.transport(), fake.profile, fresh,
                    lookup_hashes=self.hashes, effect_versions=effect_versions)
        for fault in (("object_current", "duplicate"), ("edge_current", "missing")):
            fake = FakeTransport(self.plan, row_fault=fault)
            self.install_expected(fake)
            fresh = self.fresh(fake)
            self.mark_effects(fake)
            effect_versions = {role: 2 for role in DATA_ROLES}
            effect_versions["tombstone"] = 1
            with patch("ashlar_host.local_current_carrier_profile.verify_local_xxhash64_snapshot",
                       side_effect=lambda _spark, _path, _version, role: len(self.expected[role])):
                with self.assertRaises(ValueError):
                    validate_delta4_transaction1_snapshots(
                        self.plan, fake.transport(), fake.profile, fresh,
                        lookup_hashes=self.hashes, effect_versions=effect_versions)

    def test_post_effect_metadata_roles_must_remain_fresh(self):
        fake = FakeTransport(self.plan)
        self.install_expected(fake)
        fresh = self.fresh(fake)
        self.mark_effects(fake)
        fake.histories["publication_manifest"] = [2, 1, 0]
        effect_versions = {role: 2 for role in DATA_ROLES}
        effect_versions["tombstone"] = 1
        with patch("ashlar_host.local_current_carrier_profile.verify_local_xxhash64_snapshot",
                   side_effect=lambda _spark, _path, _version, role: len(self.expected[role])):
            with self.assertRaises(ValueError):
                validate_delta4_transaction1_snapshots(
                    self.plan, fake.transport(), fake.profile, fresh,
                    lookup_hashes=self.hashes, effect_versions=effect_versions)


if __name__ == "__main__":
    unittest.main()
