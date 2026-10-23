# SPDX-License-Identifier: MIT OR Apache-2.0
"""Offline receipt-verifier tests; the fake Spark session performs no I/O."""
import copy
import json
import unittest
from datetime import datetime, timezone
import time
from unittest.mock import patch
from types import SimpleNamespace

from ashlar.native import SQLResult
from ashlar.protocol import ReaderProtocolProfile
from ashlar_host.managed_current_carriers import load_current_managed_carriers
from ashlar_host.managed_current_core import ROLES
from ashlar_host.managed_current_spark_receipt_read import (
    ManagedCurrentSparkReceiptConfig, ManagedCurrentSparkReceiptReader, _native_json,
)
from test_managed_current_receipt_read import _fixture, _canonical


class _Row:
    def __init__(self, value): self.value = value
    def asDict(self, recursive=True): return dict(self.value)


class _Frame:
    def __init__(self, rows, columns):
        self.rows, self.schema = rows, SimpleNamespace(fields=[
            SimpleNamespace(name=n, dataType=SimpleNamespace(
                simpleString=lambda t=("bigint" if t == "long" else t): t))
            for n, t in columns])
    def limit(self, count): return self
    def collect(self): return [_Row(r) for r in self.rows]


class _Spark:
    def __init__(self, manifest, receipt, uuids, *, owner="owner@example.test"):
        self.manifest, self.receipt, self.uuids, self.owner = manifest, receipt, uuids, owner
        self.version = "15.4.x-scala2.12"
        self.sparkContext = _SparkContext()
        self.conf = SimpleNamespace(get=lambda key: {
            "spark.databricks.clusterUsageTags.clusterId": "cluster-1",
            "spark.databricks.clusterUsageTags.sparkVersion": "15.4.x-scala2.12"}[key])
        self.carriers = load_current_managed_carriers("ashlar-delta/0.3")
        self.inventory = receipt["installationInventory"]
        self.calls = []

    def sql(self, sql, args=None):
        args = args or {}; self.calls.append(sql)
        if "current_user() AS principal" in sql:
            rows, cols = [{"principal": self.owner, "catalog": "client_dev", "schema": "trial_schema"}], \
                (("principal", "string"), ("catalog", "string"), ("schema", "string"))
        elif "version() AS sparkBuildText" in sql:
            rows, cols = [{"sparkBuildText": "Spark build"}], (("sparkBuildText", "string"),)
        elif "FROM system.information_schema.tables" in sql:
            rows = [{"table_name": r, "table_type": "MANAGED", "table_owner": self.owner} for r in ROLES]
            cols = (("table_name", "string"), ("table_type", "string"), ("table_owner", "string"))
        elif "FROM system.information_schema.columns" in sql:
            rows = []
            for role in ROLES:
                for pos, (name, kind, nullable) in enumerate(self.carriers.ordered_fields(role), 1):
                    rows.append({"table_name": role, "column_name": name, "data_type": kind,
                        "is_nullable": "YES" if nullable else "NO", "ordinal_position": pos})
            cols = tuple((name, typ) for name, typ in (("table_name", "string"), ("column_name", "string"),
                ("data_type", "string"), ("is_nullable", "string"), ("ordinal_position", "long")))
        elif sql.startswith("DESCRIBE DETAIL"):
            role = sql.rsplit(".", 1)[-1].strip("`")
            item = self.inventory[role]
            proto = item["protocol"]
            rows = [{"id": self.uuids[role], "format": "delta",
                "minReaderVersion": proto["reader_version"], "minWriterVersion": proto["writer_version"],
                "tableFeatures": proto["features"]}]
            cols = (("id", "string"), ("format", "string"), ("minReaderVersion", "long"),
                    ("minWriterVersion", "long"), ("tableFeatures", "array<string>"))
        elif sql.startswith("DESCRIBE HISTORY"):
            rows, cols = [{"version": 0}], (("version", "long"),)
        elif "publication_manifest` WHERE publication_id" in sql:
            rows, cols = [self.manifest], tuple((k, "string") for k in self.manifest)
        elif sql.startswith("SELECT * FROM") and "VERSION AS OF" in sql:
            role = sql.split("VERSION AS OF", 1)[0].rsplit(".", 1)[-1].strip("` ")
            cols = tuple((name, kind.lower().replace("bigint", "long"))
                         for name, kind, _ in self.carriers.ordered_fields(role))
            rows = []
        else:
            raise AssertionError("Unexpected Spark SQL: " + sql)
        return _Frame(rows, cols)


class _SparkContext:
    applicationId = "app-1"
    def __init__(self): self.properties = {}
    def getLocalProperty(self, key): return self.properties.get(key)
    def setLocalProperty(self, key, value): self.properties[key] = value
    def setJobGroup(self, group, description, interruptOnCancel=True):
        self.properties["spark.jobGroup.id"] = group
    def cancelJobGroup(self, group): pass


def _spark_config(config):
    runtime = {"runtimeName": "databricks-jobs-cluster", "sparkVersion": "15.4.x-scala2.12",
        "sparkBuildText": "Spark build", "runtimeTag": "15.4.x-scala2.12",
        "sparkApplicationId": "app-1", "clusterId": "cluster-1",
        "principal": config.expected_principal, "catalog": config.catalog,
        "schema": config.schema, "protocol": config.protocol_profile.identity}
    return ManagedCurrentSparkReceiptConfig(config.catalog, config.schema,
        config.expected_principal, config.expected_table_owner, config.source_truss_version,
        config.protocol_profile, runtime)


class ManagedCurrentSparkReceiptReadTests(unittest.TestCase):
    def setUp(self):
        self.request, self.manifest, self.receipt, self.uuids, sql_config = _fixture()
        self.config = _spark_config(sql_config)
        self.spark = _Spark(self.manifest, self.receipt, self.uuids)
        self.reader = ManagedCurrentSparkReceiptReader(self.spark, self.config)

    def reopen(self, receipt=None):
        return self.reader.reopen(request_bytes=_canonical(self.request).encode(),
            manifest_bytes=_canonical(self.manifest).encode(), expected_receipt=receipt or self.receipt)

    def test_reopens_spark_native_inventory_and_closes_with_same_identity(self):
        observation = self.reopen()
        self.reader.close()
        self.assertEqual(observation.profile, "ashlar-managed-current-spark-receipt-reopen/0.1")
        self.assertEqual(observation.vector.authority, "managed-current-spark:owner@example.test")
        self.assertEqual(observation.runtime_identity["opening"], observation.runtime_identity["closing"])
        self.assertEqual(observation.installation_inventory, observation.closing_installation_inventory)
        self.assertEqual(observation.source_receipt_bytes[:1], b"{")
        self.assertFalse(hasattr(observation, "source_acknowledgement_ports"))
        self.assertFalse(hasattr(observation, "native_backend"))
        self.assertFalse(hasattr(observation, "resolver_backend"))
        self.assertFalse(any("warehouse" in sql.lower() for sql in self.spark.calls))

    def test_rejects_bool_or_float_as_native_version(self):
        for value in (True, 1.0, -1, 2**63):
            with self.subTest(value=value), self.assertRaises(ValueError):
                ManagedCurrentSparkReceiptReader._integer(value)
        self.assertEqual(ManagedCurrentSparkReceiptReader._integer(0), 0)

    def test_spark_datetime_normalization_keeps_microseconds_and_zone(self):
        value = datetime(2026, 10, 23, 12, 34, 56, 123456, tzinfo=timezone.utc)
        self.assertEqual(_native_json({"at": value}),
                         '{"at":"2026-10-23T12:34:56.123456+00:00"}')

    def test_read_owner_rejects_write_or_unbounded_query_shapes(self):
        injected = "DELETE FROM x /* FROM system.information_schema.tables */"
        multi = ("SELECT current_user() AS principal,current_catalog() AS catalog,"
                 "current_schema() AS schema; DELETE FROM x")
        for sql in ("DELETE FROM x", "SELECT * FROM arbitrary_table", "INSERT INTO x SELECT 1",
                    injected, multi):
            with self.subTest(sql=sql), self.assertRaises(ValueError):
                self.reader.executor.query(sql, {})

    def test_deadline_checked_after_collect(self):
        original = self.spark.sql
        def delayed(sql, args=None):
            frame = original(sql, args)
            self.reader.started = time.monotonic() - self.config.maximum_elapsed_seconds - 1
            return frame
        self.spark.sql = delayed
        with self.assertRaisesRegex(ValueError, "cumulative deadline"):
            self.reader.executor.query("SELECT version() AS sparkBuildText", {})

    def test_close_timeout_withholds_observation(self):
        class ImmediateTimer:
            def __init__(self, _seconds, callback): self.callback = callback
            def start(self): self.callback()
            def cancel(self): pass
        with patch("ashlar_host.managed_current_spark_receipt_read.threading.Timer", ImmediateTimer):
            with self.assertRaisesRegex(ValueError, "closing deadline"):
                self.reader.close()

    def test_rejects_inventory_uuid_owner_and_source_vector_drift(self):
        self.spark.uuids["publication_attempt_phase"] = "ffffffff-ffff-ffff-ffff-ffffffffffff"
        with self.assertRaisesRegex(ValueError, "six-role UC identity changed"):
            self.reader.close()

        request, manifest, receipt, uuids, sql_config = _fixture()
        bad = copy.deepcopy(receipt)
        bad["dataRoleVector"]["client_dev.trial_schema.object_current"]["uuid"] = uuids["edge_current"]
        reader = ManagedCurrentSparkReceiptReader(_Spark(manifest, bad, uuids), _spark_config(sql_config))
        with self.assertRaisesRegex(ValueError, "four-role vector"):
            reader.reopen(request_bytes=_canonical(request).encode(),
                manifest_bytes=_canonical(manifest).encode(), expected_receipt=bad)

    def test_rejects_runtime_principal_and_manifest_change(self):
        self.spark.owner = "changed@example.test"
        with self.assertRaisesRegex(ValueError, "runtime/principal/namespace"):
            self.reader.close()

        request, manifest, receipt, uuids, sql_config = _fixture()
        bad = copy.deepcopy(receipt); bad["manifestSha256"] = "0" * 64
        reader = ManagedCurrentSparkReceiptReader(_Spark(manifest, receipt, uuids), _spark_config(sql_config))
        with self.assertRaisesRegex(ValueError, "Exact task receipt"):
            reader.reopen(request_bytes=_canonical(request).encode(),
                manifest_bytes=_canonical(manifest).encode(), expected_receipt=bad)


if __name__ == "__main__":
    unittest.main()
