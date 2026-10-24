# SPDX-License-Identifier: MIT OR Apache-2.0
"""Synthetic SDK Statement Execution responses; no credentials or cloud calls."""
import hashlib
import json
from pathlib import Path
import re
from types import SimpleNamespace
import unittest

from ashlar.native import SQLResult
from ashlar.protocol import ReaderProtocolProfile
from ashlar_host.managed_current_carriers import load_current_managed_carriers
from ashlar_host.managed_current_core import CARRIER_PROFILE_XXHASH64, ROLES
from ashlar_host.managed_current_receipt_read import (
    ManagedCurrentReceiptConfig, ManagedCurrentSQLReceiptReader,
)


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


class FakeSQLReader:
    def __init__(self, manifest, uuids=None, carrier_profile="ashlar-delta/0.3"):
        self.config = SimpleNamespace(warehouse_id="a" * 16, catalog="client_dev",
                                      schema="trial_schema", statement_seconds=30,
                                      maximum_statements=128, maximum_rows=512)
        self.channel = SimpleNamespace(name=SimpleNamespace(value="CHANNEL_NAME_CURRENT"),
                                       dbsql_version="2026.39")
        warehouse = SimpleNamespace(id=self.config.warehouse_id,
            enable_serverless_compute=True, channel=self.channel)
        self.workspace = SimpleNamespace(warehouses=SimpleNamespace(get=lambda _: warehouse))
        self.manifest = manifest
        self.uuids = uuids or {role: f"00000000-0000-0000-0000-{index:012d}"
                               for index, role in enumerate(ROLES, 1)}
        self.carriers = load_current_managed_carriers(carrier_profile)
        self.calls = []
        self.active = True

    def renew(self):
        if not self.active:
            raise AssertionError("reader custody closed")

    def query(self, sql, parameters):
        self.calls.append((sql, dict(parameters)))
        if "current_user() AS principal" in sql:
            return SQLResult([{"principal": "owner@example.test", "catalog": "client_dev",
                "schema": "trial_schema", "current_version_json":
                '{"dbr_version":null,"dbsql_version":"2026.39",'
                '"u_build_hash":"u-build","r_build_hash":"r-build"}'}])
        if "FROM system.information_schema.tables" in sql:
            selected = parameters.get("name")
            roles = (selected,) if selected else ROLES
            return SQLResult([{"table_name": role, "table_type": "MANAGED",
                "table_owner": "owner@example.test"} for role in roles])
        if "FROM system.information_schema.columns" in sql:
            rows = []
            for role in ROLES:
                for ordinal, (name, kind, nullable) in enumerate(self.carriers.ordered_fields(role), 1):
                    rows.append({"table_name": role, "column_name": name, "data_type": kind,
                        "is_nullable": "YES" if nullable else "NO", "ordinal_position": str(ordinal)})
            return SQLResult(rows)
        if sql.startswith("DESCRIBE DETAIL "):
            role = sql.rsplit(".", 1)[1].strip("`")
            return SQLResult([{"id": self.uuids[role], "format": "delta",
                "minReaderVersion": "1", "minWriterVersion": "2", "tableFeatures": "[]"}])
        if sql.startswith("DESCRIBE HISTORY "):
            return SQLResult([{"version": "0"}])
        if "FROM `client_dev`.`trial_schema`.`publication_manifest` WHERE" in sql:
            return SQLResult([self.manifest])
        if sql.startswith("SELECT * FROM ") and " VERSION AS OF " in sql:
            role = re.findall(r"`([^`]*)`", sql)[-1]
            cols = tuple((name, kind) for name, kind, _ in self.carriers.ordered_fields(role))
            return SQLResult([], cols)
        raise AssertionError("Unexpected SQL: " + sql)


def _fixture(carrier_profile="ashlar-delta/0.3", source_truss_version="0.0.44"):
    from test_managed_current_core import _current_admission
    from ashlar_host.publication_planning import truss_request_for
    from ashlar.source_checkpoint import truss_checkpoint

    admission = _current_admission()
    revisions = {"typed-graph-example": "1"}
    request = truss_request_for(admission.batch.feed, admission.batch,
                                "managed-current-genesis/0.1", revisions)
    checkpoint = json.loads(truss_checkpoint(admission.batch))
    names = {role: ".".join(("client_dev", "trial_schema", role))
             for role in ("object_current", "edge_current", "tombstone", "whole_source_history")}
    table_versions = {table: 0 for table in names.values()}
    manifest = {"publication_id": "publication-one", "profile_version": carrier_profile,
        "table_versions_json": _canonical(table_versions),
        "source_progress_json": _canonical({checkpoint["feed"]: checkpoint}),
        "schema_revisions_json": _canonical(revisions),
        "validation_report_json": _canonical({"complete": True,
            "request_digest": request["request_digest"]}), "recorded_at": "1"}
    uuids = {role: f"00000000-0000-0000-0000-{index:012d}"
             for index, role in enumerate(ROLES, 1)}
    owner = "owner@example.test"
    installation = {}
    carriers = load_current_managed_carriers(carrier_profile)
    for role in ROLES:
        installation[role] = {"name": ".".join(("client_dev", "trial_schema", role)),
            "uuid": uuids[role], "version": 0,
            "schema": [[name, kind.lower().replace("bigint", "long"), nullable]
                       for name, kind, nullable in carriers.ordered_fields(role)],
            "protocol": {"uuid": uuids[role], "profile": "trial-reader/0.1",
                "reader_version": 1, "writer_version": 2, "features": []}}
    receipt = {"profile": "ashlar-managed-current-destination-receipt/0.1",
        "publicationId": manifest["publication_id"], "requestDigest": request["request_digest"],
        "manifest": manifest, "manifestSha256": hashlib.sha256(
            _canonical(manifest).encode()).hexdigest(),
        "dataRoleVector": {table: {"uuid": uuids[role], "version": 0}
            for table, role in ((names[role], role) for role in names)},
        "weftBindingInputs": {"manifest": manifest,
            "manifestUuid": uuids["publication_manifest"],
            "dataRoles": {role: {"name": names[role], "uuid": uuids[role], "version": 0}
                          for role in ("object_current", "edge_current", "tombstone",
                                       "whole_source_history")}},
        "installationInventory": installation,
        "sourceAuthority": f"development Truss {source_truss_version} page on managed Unity Catalog; external source ACK required"}
    config = ManagedCurrentReceiptConfig("client_dev", "trial_schema", "a" * 16,
        owner, owner, source_truss_version, ReaderProtocolProfile("trial-reader/0.1", (1,), (2,), ()),
        carrier_profile=carrier_profile)
    return request, manifest, receipt, uuids, config


class ManagedCurrentReceiptReadTests(unittest.TestCase):
    def test_048_source_version_is_preserved_through_receipt_reopen(self):
        request, manifest, receipt, uuids, config = _fixture(source_truss_version="0.0.48")
        sql = FakeSQLReader(manifest, uuids)
        reader = ManagedCurrentSQLReceiptReader(sql, config)
        observed = reader.reopen(request_bytes=_canonical(request).encode(),
            manifest_bytes=_canonical(manifest).encode(), expected_receipt=receipt)
        self.assertEqual(observed.profile, "ashlar-managed-current-external-receipt-read/0.1")
        self.assertEqual(config.source_truss_version, "0.0.48")
        self.assertEqual(receipt["sourceAuthority"],
            "development Truss 0.0.48 page on managed Unity Catalog; external source ACK required")

    def test_049_source_version_is_preserved_through_receipt_reopen(self):
        request, manifest, receipt, uuids, config = _fixture(source_truss_version="0.0.49")
        sql = FakeSQLReader(manifest, uuids)
        reader = ManagedCurrentSQLReceiptReader(sql, config)
        observed = reader.reopen(request_bytes=_canonical(request).encode(),
            manifest_bytes=_canonical(manifest).encode(), expected_receipt=receipt)
        self.assertEqual(config.source_truss_version, "0.0.49")
        self.assertEqual(receipt["sourceAuthority"],
            "development Truss 0.0.49 page on managed Unity Catalog; external source ACK required")

    def test_050_source_version_is_admitted_by_receipt_and_spark_reopen_configs(self):
        from test_managed_current_spark_receipt_read import _spark_config
        _, _, _, _, config = _fixture(source_truss_version="0.0.50")
        self.assertEqual(config.source_truss_version, "0.0.50")
        spark_config = _spark_config(config)
        self.assertEqual(spark_config.source_truss_version, "0.0.50")

    def test_xxhash64_receipt_uses_exact_physical_profile_for_reopen_and_ack_ports(self):
        request, manifest, receipt, uuids, config = _fixture(CARRIER_PROFILE_XXHASH64)
        sql = FakeSQLReader(manifest, uuids, CARRIER_PROFILE_XXHASH64)
        reader = ManagedCurrentSQLReceiptReader(sql, config)
        observation = reader.reopen(request_bytes=_canonical(request).encode(),
            manifest_bytes=_canonical(manifest).encode(), expected_receipt=receipt)
        self.assertEqual(observation.supported_profiles, (CARRIER_PROFILE_XXHASH64,))
        self.assertEqual(observation.source_acknowledgement_ports()["supported_profiles"],
                         (CARRIER_PROFILE_XXHASH64,))
        self.assertIn(("lookup_hash", "bigint", False),
                      tuple((name, kind.lower(), nullable)
                            for name, kind, nullable in sql.carriers.ordered_fields("object_current")))
        reader.close()

    def test_xxhash64_reader_refuses_legacy_manifest_profile(self):
        from dataclasses import replace
        request, manifest, receipt, uuids, config = _fixture()
        config = replace(config, carrier_profile=CARRIER_PROFILE_XXHASH64)
        reader = ManagedCurrentSQLReceiptReader(
            FakeSQLReader(manifest, uuids, CARRIER_PROFILE_XXHASH64), config)
        with self.assertRaises(ValueError):
            reader.reopen(request_bytes=_canonical(request).encode(),
                manifest_bytes=_canonical(manifest).encode(), expected_receipt=receipt)

    def test_reopens_exact_four_role_vector_and_six_role_native_inventory(self):
        request, manifest, receipt, uuids, config = _fixture()
        sql = FakeSQLReader(manifest, uuids)
        reader = ManagedCurrentSQLReceiptReader(sql, config)
        observation = reader.reopen(request_bytes=_canonical(request).encode(),
            manifest_bytes=_canonical(manifest).encode(), expected_receipt=receipt)
        # TrussFeedSessions resolves once before its ACK transaction and once
        # after the native call before commit. Keep both renewals in the hard
        # statement budget together with open and close observations.
        from ashlar.publication import resolve_publication
        required = {table: target[0] for table, target in observation.vector.targets.items()}
        revisions = dict(observation.supported_revisions)
        for _ in range(2):
            resolve_publication(observation.backend, observation.publication_id, required,
                context=reader.context, supported_profiles=observation.supported_profiles,
                supported_revisions=revisions)
            observation.backend.validate_manifest(manifest, reader.context)
        reader.close()
        self.assertLessEqual(len(sql.calls), 128)
        self.assertEqual(observation.profile, "ashlar-managed-current-external-receipt-read/0.1")
        self.assertEqual(observation.publication_id, manifest["publication_id"])
        self.assertEqual(set(observation.installation_inventory), set(ROLES))
        self.assertEqual(set(observation.vector.targets), set(receipt["dataRoleVector"]))
        self.assertEqual(observation.runtime_identity["opening"], observation.runtime_identity["closing"])
        self.assertEqual(observation.installation_inventory, observation.closing_installation_inventory)
        self.assertEqual(observation.native_backend, observation.backend)
        self.assertEqual(observation.source_acknowledgement_ports()["vector"], observation.vector)
        self.assertEqual(hashlib.sha256(observation.source_receipt_bytes).hexdigest(),
                         observation.source_receipt_sha256)
        self.assertTrue(all("system.information_schema" in sql_text
            or any(f"`{role}`" in sql_text for role in ROLES)
            or "current_user()" in sql_text for sql_text, _ in sql.calls))
        self.assertFalse(any("SELECT *" in sql_text and "LIMIT 0" not in sql_text
                             for sql_text, _ in sql.calls))

    def test_wrong_role_or_table_uuid_in_task_receipt_refuses(self):
        request, manifest, receipt, uuids, config = _fixture()
        wrong_role = json.loads(json.dumps(receipt))
        wrong_role["dataRoleVector"]["client_dev.trial_schema.not_object_current"] = \
            wrong_role["dataRoleVector"].pop("client_dev.trial_schema.object_current")
        with self.assertRaisesRegex(ValueError, "four-role vector"):
            ManagedCurrentSQLReceiptReader(FakeSQLReader(manifest, uuids), config).reopen(
                request_bytes=_canonical(request).encode(), manifest_bytes=_canonical(manifest).encode(),
                expected_receipt=wrong_role)
        wrong_uuid = json.loads(json.dumps(receipt))
        wrong_uuid["dataRoleVector"]["client_dev.trial_schema.object_current"]["uuid"] = \
            "ffffffff-ffff-ffff-ffff-ffffffffffff"
        with self.assertRaisesRegex(ValueError, "four-role vector"):
            ManagedCurrentSQLReceiptReader(FakeSQLReader(manifest, uuids), config).reopen(
                request_bytes=_canonical(request).encode(), manifest_bytes=_canonical(manifest).encode(),
                expected_receipt=wrong_uuid)

    def test_wrong_ambient_namespace_refuses_before_any_sql(self):
        request, manifest, receipt, uuids, config = _fixture()
        sql = FakeSQLReader(manifest, uuids)
        sql.config.catalog = "main"
        with self.assertRaisesRegex(ValueError, "explicit catalog and schema"):
            ManagedCurrentSQLReceiptReader(sql, config)
        self.assertEqual(sql.calls, [])

    def test_changed_coordination_role_refuses_at_pre_ack_resolver_admission(self):
        request, manifest, receipt, uuids, config = _fixture()
        sql = FakeSQLReader(manifest, uuids)
        reader = ManagedCurrentSQLReceiptReader(sql, config)
        observation = reader.reopen(request_bytes=_canonical(request).encode(),
            manifest_bytes=_canonical(manifest).encode(), expected_receipt=receipt)
        # The ACK port's resolver admission is the point where six-role custody
        # must be renewed, before a caller can use it to acknowledge the source.
        sql.uuids["publication_attempt_phase"] = "ffffffff-ffff-ffff-ffff-ffffffffffff"
        with self.assertRaisesRegex(ValueError, "six-role Unity Catalog identity changed"):
            observation.backend.authorize(reader.context, observation.publication_id,
                                          tuple(observation.vector.targets))
        self.assertIsNone(observation.runtime_identity["closing"])

    def test_manifest_digest_and_original_source_checkpoint_are_mandatory(self):
        request, manifest, receipt, uuids, config = _fixture()
        wrong_digest = json.loads(json.dumps(receipt))
        wrong_digest["manifestSha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "manifest bytes"):
            ManagedCurrentSQLReceiptReader(FakeSQLReader(manifest, uuids), config).reopen(
                request_bytes=_canonical(request).encode(), manifest_bytes=_canonical(manifest).encode(),
                expected_receipt=wrong_digest)
        wrong_manifest = dict(manifest)
        progress = json.loads(wrong_manifest["source_progress_json"])
        progress[next(iter(progress))]["position"] = "forged-cursor"
        wrong_manifest["source_progress_json"] = _canonical(progress)
        wrong_receipt = dict(receipt, manifest=wrong_manifest,
            manifestSha256=hashlib.sha256(_canonical(wrong_manifest).encode()).hexdigest())
        wrong_receipt["weftBindingInputs"] = dict(receipt["weftBindingInputs"], manifest=wrong_manifest)
        with self.assertRaisesRegex(ValueError, "progress differs"):
            ManagedCurrentSQLReceiptReader(FakeSQLReader(wrong_manifest, uuids), config).reopen(
                request_bytes=_canonical(request).encode(), manifest_bytes=_canonical(wrong_manifest).encode(),
                expected_receipt=wrong_receipt)

    def test_closing_reopen_refuses_native_identity_drift(self):
        request, manifest, receipt, uuids, config = _fixture()
        sql = FakeSQLReader(manifest, uuids)
        reader = ManagedCurrentSQLReceiptReader(sql, config)
        reader.reopen(request_bytes=_canonical(request).encode(),
            manifest_bytes=_canonical(manifest).encode(), expected_receipt=receipt)
        sql.uuids["publication_attempt_phase"] = "ffffffff-ffff-ffff-ffff-ffffffffffff"
        with self.assertRaisesRegex(ValueError, "runtime or six-role Unity Catalog identity changed"):
            reader.close()


if __name__ == "__main__":
    unittest.main()
