# SPDX-License-Identifier: MIT OR Apache-2.0
"""Offline receipt-verifier tests; the fake Spark session performs no I/O."""
import copy
from contextlib import contextmanager
from dataclasses import replace
import hashlib
import json
import unittest
from datetime import datetime, timezone
import time
import threading
from unittest.mock import patch
from types import SimpleNamespace

from ashlar.native import SQLResult
from ashlar.pins import PostgresPins
from ashlar.apply import EntityKey
from ashlar.protocol import ReaderProtocolProfile
from ashlar_host.managed_current_carriers import load_current_managed_carriers
from ashlar_host.managed_current_core import CARRIER_PROFILE_XXHASH64, ROLES
from ashlar_host.managed_current_a1_trial_plan import ManagedCurrentA1TrialPlan
from ashlar_host.managed_current_spark_receipt_read import (
    ManagedCurrentSparkReceiptConfig, ManagedCurrentSparkServerlessReceiptConfig,
    ManagedCurrentSparkReceiptReader, ManagedCurrentSparkServerlessReceiptReader,
    _native_json,
)
from ashlar_host.managed_current_spark_source_oracle import (
    ManagedCurrentSourcePrefixOracle, build_managed_current_prefix_oracle,
    compare_complete_managed_current_bags, derive_managed_current_prefix_lookup_keys,
    _source_prefix_lookup_tokens,
)
from ashlar_host.managed_operational_retention_interval import (
    ManagedOperationalRetentionSelection, OperationalRetentionInterval,
    OperationalRetentionIntervalError, SOURCE_CUSTODY_FIELDS,
)
from ashlar_host.managed_operational_read_port import (
    ManagedOperationalReadPort, ManagedOperationalReadPortError,
    ManagedOperationalReadPinPort,
)
from ashlar.manifest import manifest_pin_vector
from ashlar_host.truss_note_publication import graph_expected_rows
from test_managed_current_receipt_read import _fixture, _canonical


class _Row:
    def __init__(self, value): self.value = value
    def asDict(self, recursive=True): return dict(self.value)


class _Frame:
    def __init__(self, rows, columns, on_limit=None):
        self.rows, self.on_limit, self.schema = rows, on_limit, SimpleNamespace(fields=[
            SimpleNamespace(name=n, dataType=SimpleNamespace(
                simpleString=lambda t=("bigint" if t == "long" else t): t))
            for n, t in columns])
    def limit(self, count):
        if self.on_limit is not None: self.on_limit(count)
        self.rows = self.rows[:count]; return self
    def collect(self): return [_Row(r) for r in self.rows]


class _Spark:
    def __init__(self, manifest, receipt, uuids, *, owner="owner@example.test", request=None):
        self.manifest, self.receipt, self.uuids, self.owner = manifest, receipt, uuids, owner
        self.version = "15.4.x-scala2.12"
        self.sparkContext = _SparkContext()
        self.conf = SimpleNamespace(get=lambda key: {
            "spark.databricks.clusterUsageTags.clusterId": "cluster-1",
            "spark.databricks.clusterUsageTags.sparkVersion": "15.4.x-scala2.12"}[key])
        self.carriers = load_current_managed_carriers(manifest["profile_version"])
        self.inventory = receipt["installationInventory"]
        self.request = request or _fixture_request_for_attempt(manifest, receipt)
        self.attempt_rows = _attempt_rows(self.request, manifest)
        self.calls = []
        self.parameters = []
        self.limits = []
        self.role_rows = {role: [] for role in ("object_current", "edge_current", "tombstone",
                                                "whole_source_history")}
        self.timezone = "UTC"
        self.ordinal_base = 1
        self.ordinal_position_overrides = {}
        self.role_identity_drift = None
        self.hash_values = {}
        self.hash_rows_override = None
        self.catalog = "client_dev"
        self.schema = "trial_schema"

    def sql(self, sql, args=None):
        args = args or {}; self.calls.append(sql); self.parameters.append(dict(args))
        if "current_user() AS principal" in sql:
            rows, cols = [{"principal": self.owner, "catalog": self.catalog, "schema": self.schema}], \
                (("principal", "string"), ("catalog", "string"), ("schema", "string"))
        elif "version() AS sparkBuildText" in sql:
            rows, cols = [{"sparkBuildText": "Spark build"}], (("sparkBuildText", "string"),)
        elif "current_timezone() AS session_timezone" in sql:
            rows, cols = [{"session_timezone": self.timezone}], (("session_timezone", "string"),)
        elif "FROM system.information_schema.tables" in sql:
            rows = [{"table_name": r, "table_type": "MANAGED", "table_owner": self.owner} for r in ROLES]
            cols = (("table_name", "string"), ("table_type", "string"), ("table_owner", "string"))
        elif "FROM system.information_schema.columns" in sql:
            rows = []
            for role in ROLES:
                for pos, (name, kind, nullable) in enumerate(self.carriers.ordered_fields(role),
                                                             self.ordinal_base):
                    pos = self.ordinal_position_overrides.get(role, {}).get(name, pos)
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
        elif "publication_attempt_phase` VERSION AS OF " in sql:
            rows = list(self.attempt_rows)
            cols = (("phase", "string"), ("request_digest", "string"),
                    ("payload_json", "string"), ("payload_digest", "string"))
        elif " VERSION AS OF " in sql and " LIMIT 1025" in sql:
            role = sql.split(" VERSION AS OF ", 1)[0].rsplit(".", 1)[-1].strip("` ")
            fields = self.carriers.ordered_fields(role)
            cols = tuple((name, {"STRING": "string", "BIGINT": "string",
                                 "BOOLEAN": "boolean", "TIMESTAMP": "string"}[kind])
                         for name, kind, _ in fields)
            rows = list(self.role_rows[role])
            if self.role_identity_drift == role:
                self.uuids[role] = "00000000-0000-4000-8000-000000000001"
        elif sql == ("SELECT xxhash64(CAST(:source AS STRING),CAST(:type AS BIGINT),"
                     "CAST(:id AS BIGINT)) AS lookup_hash"):
            args_key = (args["source"], args["type"], args["id"])
            rows = (list(self.hash_rows_override) if self.hash_rows_override is not None else
                    [{"lookup_hash": self.hash_values.get(args_key, -1)}])
            cols = (("lookup_hash", "long"),)
        elif sql.startswith("SELECT * FROM") and "VERSION AS OF" in sql:
            role = sql.split("VERSION AS OF", 1)[0].rsplit(".", 1)[-1].strip("` ")
            cols = tuple((name, kind.lower().replace("bigint", "long"))
                         for name, kind, _ in self.carriers.ordered_fields(role))
            rows = []
        else:
            raise AssertionError("Unexpected Spark SQL: " + sql)
        return _Frame(rows, cols, self.limits.append)


class _SparkContext:
    applicationId = "app-1"
    def __init__(self): self.properties = {}
    def getLocalProperty(self, key): return self.properties.get(key)
    def setLocalProperty(self, key, value): self.properties[key] = value
    def setJobGroup(self, group, description, interruptOnCancel=True):
        self.properties["spark.jobGroup.id"] = group
    def cancelJobGroup(self, group): pass


class _SparkConnect(_Spark):
    @property
    def sparkContext(self):
        raise AssertionError("Spark Connect test must never access sparkContext")
    @sparkContext.setter
    def sparkContext(self, _):
        self._forbidden_context = True

    def __init__(self, manifest, receipt, uuids, *, interrupt_result=None):
        self.initial_tags = {"caller-owned-tag"}
        self._thread_tags = threading.local()
        self.interrupt_result = ["operation-123"] if interrupt_result is None else interrupt_result
        self.interrupt_calls = []
        self.interrupt_saw_active_operation = []
        self.active_tagged_operations = set()
        self.query_tag_snapshots = []
        super().__init__(manifest, receipt, uuids)
        self.version = "4.2.0"
        self.conf = SimpleNamespace(get=lambda key: {
            "spark.databricks.clusterUsageTags.sparkVersion": "serverless-4.2.0"}[key])

    def _tags(self):
        if not hasattr(self._thread_tags, "value"):
            self._thread_tags.value = set(self.initial_tags)
        return self._thread_tags.value
    def addTag(self, tag): self._tags().add(tag)
    def getTags(self): return set(self._tags())
    def removeTag(self, tag): self._tags().discard(tag)
    def sql(self, sql, args=None):
        self.query_tag_snapshots.append(self.getTags())
        return super().sql(sql, args=args)
    def interruptTag(self, tag):
        self.interrupt_calls.append(tag)
        self.interrupt_saw_active_operation.append(tag in self.active_tagged_operations)
        return list(self.interrupt_result)


def _spark_config(config):
    runtime = {"runtimeName": "databricks-jobs-cluster", "sparkVersion": "15.4.x-scala2.12",
        "sparkBuildText": "Spark build", "runtimeTag": "15.4.x-scala2.12",
        "sparkApplicationId": "app-1", "clusterId": "cluster-1",
        "principal": config.expected_principal, "catalog": config.catalog,
        "schema": config.schema, "protocol": config.protocol_profile.identity,
        "sessionTimeZone": "UTC"}
    return ManagedCurrentSparkReceiptConfig(config.catalog, config.schema,
        config.expected_principal, config.expected_table_owner, config.source_truss_version,
        config.protocol_profile, runtime, carrier_profile=config.carrier_profile)


def _fixture_request_for_attempt(manifest, receipt):
    request, _, _, _, _ = _fixture()
    return request


def _empty_current_source_prefix():
    from test_managed_current_core import _config, _current_admission
    from ashlar_host.managed_current_core import bind_current_core
    from ashlar_host.publication_planning import truss_request_for

    config, admission = _config(), _current_admission()
    revisions = {revision: hashlib.sha256(selected["report"]).hexdigest()
                 for revision, selected in admission.revisions.items()}
    request = truss_request_for(admission.batch.feed, admission.batch,
        "managed-current-genesis/0.1", revisions)
    bundle = bind_current_core(admission, config)
    checkpoint = json.loads(request["source_checkpoint_json"])
    prefix = {"profile": "ashlar-managed-current-source-prefix/0.1",
        "carrierProfile": config.carrier_profile,
        "destination": {"catalog": config.catalog, "schema": config.schema},
        "scheduleSha256": "a" * 64, "scheduleStep": 1,
        "entries": [{"bundleJson": _canonical(bundle), "requestJson": _canonical(request),
                     "materializationClockUs": "1792756800123456"}],
        "progress": {checkpoint["feed"]: checkpoint}}
    data = _canonical(prefix).encode()
    return config, prefix, data


def _xxhash_current_source_prefix():
    from test_managed_current_core import _current_admission
    from ashlar_host.managed_current_core import (
        CARRIER_PROFILE_XXHASH64, ManagedCurrentCoreConfig, bind_current_core,
    )
    from ashlar_host.publication_planning import truss_request_for
    from ashlar.source_checkpoint import truss_checkpoint

    config = ManagedCurrentCoreConfig("client_dev", "trial_schema", None, None, 0, 100,
                                      carrier_profile=CARRIER_PROFILE_XXHASH64)
    admission = _current_admission()
    revisions = {revision: hashlib.sha256(selected["report"]).hexdigest()
                 for revision, selected in admission.revisions.items()}
    request = truss_request_for(admission.batch.feed, admission.batch,
        "managed-current-genesis/0.1", revisions)
    bundle = bind_current_core(admission, config)
    checkpoint = json.loads(truss_checkpoint(admission.batch))
    prefix = {"profile": "ashlar-managed-current-source-prefix/0.1",
        "carrierProfile": CARRIER_PROFILE_XXHASH64,
        "destination": {"catalog": config.catalog, "schema": config.schema},
        "scheduleSha256": "b" * 64, "scheduleStep": 1,
        "entries": [{"bundleJson": _canonical(bundle), "requestJson": _canonical(request),
                     "materializationClockUs": "1792756800123456"}],
        "progress": {checkpoint["feed"]: checkpoint}}
    raw = _canonical(prefix).encode()
    return config, prefix, raw, request


def _xxhash_spark_case():
    from ashlar_host.managed_current_core import CARRIER_PROFILE_XXHASH64

    prefix_config, prefix, prefix_bytes, request = _xxhash_current_source_prefix()
    _old_request, manifest, receipt, uuids, sql_config = _fixture(CARRIER_PROFILE_XXHASH64)
    manifest = copy.deepcopy(manifest)
    manifest["source_progress_json"] = _canonical(prefix["progress"])
    manifest["schema_revisions_json"] = request["schema_revisions_json"]
    validation = json.loads(manifest["validation_report_json"])
    validation["request_digest"] = request["request_digest"]
    manifest["validation_report_json"] = _canonical(validation)
    receipt = copy.deepcopy(receipt)
    receipt["requestDigest"] = request["request_digest"]
    receipt["manifest"] = manifest
    receipt["manifestSha256"] = hashlib.sha256(_canonical(manifest).encode()).hexdigest()
    receipt["weftBindingInputs"]["manifest"] = manifest
    spark = _Spark(manifest, receipt, uuids, request=request)
    return prefix_config, prefix, prefix_bytes, request, manifest, receipt, uuids, _spark_config(sql_config), spark


def _authored_adapter_oracle(case, expected_rows):
    # This constructor-created oracle is deliberately untrusted adapter input;
    # it tests capture mechanics only, not source-prefix custody or acceptance.
    roles = ("object_current", "edge_current", "tombstone", "whole_source_history")
    return ManagedCurrentSourcePrefixOracle(
        "ashlar-managed-current-source-prefix/0.1", case.config.carrier_profile,
        hashlib.sha256(b"authored fake-Spark prefix").hexdigest(), "a" * 64, 1,
        _canonical({"catalog": case.config.catalog, "schema": case.config.schema}),
        _canonical(json.loads(case.manifest["source_progress_json"])),
        _canonical(case.request), "[]", _canonical({role: expected_rows.get(role, []) for role in roles}))


def _native_test_row(carriers, role, marker):
    row = {}
    for name, kind, nullable in carriers.ordered_fields(role):
        if nullable:
            row[name] = None
        elif kind == "STRING":
            row[name] = marker
        elif kind == "BIGINT":
            row[name] = "9223372036854775000"
        elif kind == "BOOLEAN":
            row[name] = marker == "true"
        elif kind == "TIMESTAMP":
            row[name] = "1792756800123456"
    return row


def _attempt_rows(request, manifest):
    phases = ("prepared", "applying", "applied", "committing", "committed")
    result = _canonical({"effects": {"profile": "opaque-retained-effects/0.1"},
                         "manifest": manifest})
    descriptor = _canonical(manifest)
    rows = []
    for phase in phases:
        payload = {"request": request, "result_json": result if phase in phases[2:] else None,
                   "descriptor_json": descriptor if phase == "committed" else None,
                   "plan_json": '{"profile":"opaque-retained-plan/0.1"}'}
        text = _canonical(payload)
        rows.append({"phase": phase, "request_digest": request["request_digest"],
            "payload_json": text, "payload_digest": hashlib.sha256(text.encode()).hexdigest()})
    return rows


def _replace_phase_payload(rows, phase, update):
    row = next(item for item in rows if item["phase"] == phase)
    payload = json.loads(row["payload_json"])
    update(payload)
    row["payload_json"] = _canonical(payload)
    row["payload_digest"] = hashlib.sha256(row["payload_json"].encode()).hexdigest()
    row["request_digest"] = payload["request"]["request_digest"]


def _change_attempt_value(rows, key, value, *, phase):
    _replace_phase_payload(rows, phase, lambda payload: payload.__setitem__(key, value))


def _change_attempt_request(rows, *, phase):
    def update(payload):
        request = payload["request"]
        request["predecessor"] = "changed-predecessor"
        body = {key: value for key, value in request.items() if key != "request_digest"}
        request["request_digest"] = hashlib.sha256(_canonical(body).encode()).hexdigest()
    _replace_phase_payload(rows, phase, update)


def _change_attempt_descriptor(rows):
    _replace_phase_payload(rows, "committed", lambda payload: payload.__setitem__(
        "descriptor_json", _canonical({"publication_id": "different"})))


class ManagedCurrentSparkReceiptReadTests(unittest.TestCase):
    def setUp(self):
        self.request, self.manifest, self.receipt, self.uuids, sql_config = _fixture()
        self.config = _spark_config(sql_config)
        self.spark = _Spark(self.manifest, self.receipt, self.uuids)
        self.reader = ManagedCurrentSparkReceiptReader(self.spark, self.config)

    def reopen(self, receipt=None):
        return self.reader.reopen(request_bytes=_canonical(self.request).encode(),
            manifest_bytes=_canonical(self.manifest).encode(), expected_receipt=receipt or self.receipt)

    def _serverless_reader(self, *, interrupt_result=None,
                           operational_observation_stage=False):
        runtime = {"runtimeName": "databricks-serverless", "sparkVersion": "4.2.0",
            "sparkBuildText": "Spark build", "runtimeTag": "serverless-4.2.0",
            "principal": self.config.expected_principal, "catalog": self.config.catalog,
            "schema": self.config.schema, "protocol": self.config.protocol_profile.identity}
        selected = ManagedCurrentSparkServerlessReceiptConfig(
            self.config.catalog, self.config.schema, self.config.expected_principal,
            self.config.expected_table_owner, self.config.source_truss_version,
            self.config.protocol_profile, runtime,
            carrier_profile=self.config.carrier_profile,
            operational_observation_stage=operational_observation_stage)
        spark = _SparkConnect(self.manifest, self.receipt, self.uuids,
            interrupt_result=interrupt_result)
        return spark, ManagedCurrentSparkServerlessReceiptReader(spark, selected)

    def test_serverless_connect_reader_reopens_without_sparkcontext_and_restores_tags(self):
        spark, reader = self._serverless_reader()
        before = set(reader._serverless_tag.preexisting_tags)
        observation = reader.reopen(request_bytes=_canonical(self.request).encode(),
            manifest_bytes=_canonical(self.manifest).encode(), expected_receipt=self.receipt)
        self.assertEqual(observation.runtime_identity["opening"]["runtimeName"],
                         "databricks-serverless")
        self.assertEqual({key: observation.runtime_identity["opening"][key]
                          for key in reader.config.expected_runtime}, reader.config.expected_runtime)
        self.assertEqual(observation.runtime_identity["opening"]["sessionTimeZone"], "UTC")
        self.assertNotIn("sparkApplicationId", observation.runtime_identity["opening"])
        self.assertNotIn("clusterId", observation.runtime_identity["opening"])
        self.assertEqual(spark.getTags(), before | {reader._serverless_tag.tag})
        reader.close()
        self.assertEqual(spark.getTags(), before)
        self.assertEqual(spark.interrupt_calls, [])

    def test_serverless_information_schema_accepts_only_contiguous_zero_or_one_based_ordinals(self):
        spark, reader = self._serverless_reader()
        spark.ordinal_base = 0
        self.assertEqual(set(reader._observe_inventory()), set(ROLES))
        role = ROLES[0]
        fields = reader.carriers.ordered_fields(role)
        spark.ordinal_position_overrides[role] = {fields[1][0]: 0}
        with self.assertRaisesRegex(ValueError, "Ordered native columns"):
            reader._observe_inventory()
        spark.ordinal_position_overrides[role] = {fields[0][0]: 2}
        with self.assertRaisesRegex(ValueError, "Ordered native columns"):
            reader._observe_inventory()

    def test_generated_trial_config_routes_through_parent_bundle_to_connect_reader(self):
        import shutil
        import sys
        import tempfile
        from pathlib import Path
        from types import SimpleNamespace
        from unittest.mock import patch
        from tools.create_managed_a1_trial_config import build_config
        from tools.run_managed_a1_serverless import _retention_bundle
        from ashlar_host.configured_local_runtime import PROFILE

        runtime_result = {"runtimeName": "databricks-serverless", "sparkVersion": "4.2.0",
            "sparkBuildText": "Spark build", "runtimeTag": "serverless-4.2.0",
            "principal": "owner@example.test", "catalog": "client_dev", "schema": "trial_schema",
            "protocol": PROFILE.identity,
            "serverlessApiProbe": {"profile": "ashlar-spark-connect-tag-api-probe/0.2",
                "providedJobSessionBorrowed": True, "tagAddedAndObserved": True,
                "taggedQueryRowCount": 1, "taggedQueryValue": 1,
                "interruptTagResponseType": "list", "interruptTagResponseCount": 0,
                "tagRemoved": True, "preexistingTagsRestored": True,
                "operationTerminationClaimed": False}, "sideEffects": []}
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as temporary:
            receipt_path = Path(temporary) / "local-qualification.json"
            receipt_path.write_bytes(b"qualified local release")
            config = build_config(runtime_result=runtime_result,
                trial_id="a1-serverless-chain", notebook_root="/Workspace/Shared/ashlar",
                environment_version=7, dependencies=[{"path": "/Workspace/Shared/ashlar.whl",
                    "sha256": "a" * 64}], local_receipt=receipt_path,
                evidence_dir="docs/helix/04-build/evidence/test",
                bun_path=Path(shutil.which("bun")), python=Path(sys.executable))
        plan = SimpleNamespace(predecessor=SimpleNamespace(manifest={"publication_id": "prior"}),
            selection=SimpleNamespace(schedule_sha256="b" * 64,
                                      schema_revisions={"test": "revision-1"}))
        from ashlar_host.configured_local_runtime import MANAGED_SERVERLESS_PROFILE
        protocol_wire = {"identity": MANAGED_SERVERLESS_PROFILE.identity,
            "reader_versions": sorted(MANAGED_SERVERLESS_PROFILE.reader_versions),
            "writer_versions": sorted(MANAGED_SERVERLESS_PROFILE.writer_versions),
            "features": sorted(MANAGED_SERVERLESS_PROFILE.features)}
        captured = {}
        def compose(**kwargs):
            captured.update(kwargs)
            return SimpleNamespace(selection=SimpleNamespace(
                reader_config=kwargs["reader_config"]), vector=object())
        with patch("ashlar_host.managed_current_a1_retention_builder.build_managed_current_a1_retention_bundle",
                   side_effect=compose), patch(
                "ashlar_host.truss_current_catalog.installed_fresh_truss_runtime_pins",
                return_value={"version": "0.0.55"}):
            bundle = _retention_bundle(trial=config["trial"],
                protocol_profile=protocol_wire, plan=plan, request_bytes=b"{}", manifest_bytes=b"{}",
                destination_receipt=b"{}", task_receipt={}, prefix_bytes=b"prefix",
                prefix_sha="c" * 64, selected_prefix={}, source_snapshot=b"snapshot",
                custody_digests={})
        self.assertEqual(type(bundle.selection.reader_config), ManagedCurrentSparkServerlessReceiptConfig)
        self.assertTrue(bundle.selection.reader_config.operational_observation_stage)
        _, managed_manifest, managed_receipt, managed_uuids, _ = _fixture(
            carrier_profile=CARRIER_PROFILE_XXHASH64)
        spark = _SparkConnect(managed_manifest, managed_receipt, managed_uuids)
        spark.owner = config["expected_principal"]
        spark.schema = config["schema"]
        spark.catalog = config["catalog"]
        from ashlar_host.managed_a1_serverless_tasks import _environment
        from ashlar_host.managed_current_spark_receipt_read import BorrowedServerlessTaskSession
        environment_session = BorrowedServerlessTaskSession(spark, stage="environment",
            task_session_scope_id="config-test-task", deadline_monotonic=time.monotonic() + 10)
        environment = _environment(spark, MANAGED_SERVERLESS_PROFILE,
                                   config["expected_runtime"],
                                   session=environment_session)
        environment_session.close()
        self.assertEqual(environment, config["expected_runtime"])
        reader = ManagedCurrentSparkServerlessReceiptReader(
            spark, bundle.selection.reader_config)
        try:
            self.assertEqual({key: reader.opening_runtime[key]
                              for key in config["expected_runtime"]}, config["expected_runtime"])
            self.assertTrue(any("current_timezone()" in sql for sql in spark.calls))
            self.assertEqual(reader.opening_runtime["sessionTimeZone"], "UTC")
            self.assertIs(captured["reader_config"], bundle.selection.reader_config)
        finally:
            reader._serverless_tag.close()

    def test_classic_trial_routes_parent_bundle_to_borrowed_classic_reader(self):
        import shutil
        import sys
        import tempfile
        from pathlib import Path
        from types import SimpleNamespace
        from unittest.mock import patch
        from tools.create_managed_a1_trial_config import build_config
        from tools.run_managed_a1_serverless import _retention_bundle
        from ashlar_host.configured_local_runtime import MANAGED_CLASSIC_PROFILE
        from ashlar_host.managed_current_spark_receipt_read import (
            ManagedCurrentSparkClassicTaskReceiptConfig,
        )

        owner = "owner@example.test"
        trial_id = "a1-classic-retention"
        cluster = {"cluster_id": "0712-123456-abc123",
            "cluster_name": "ashlar-release-classic-test", "state": "RUNNING",
            "single_user_name": owner, "node_type_id": "Standard_D4ds_v5",
            "driver_node_type_id": "Standard_D4ds_v5", "spark_version": "19.x-scala2.13",
            "spark_conf": {"spark.databricks.cluster.profile": "singleNode",
                "spark.master": "local[*]", "spark.sql.session.timeZone": "UTC"},
            "num_workers": 0, "data_security_mode": "SINGLE_USER",
            "runtime_engine": "STANDARD", "autotermination_minutes": 10,
            "policy_id": "001654FA4AA08D1A", "custom_tags": {
                "ashlar_owner": "ashlar-release", "ashlar_trial": trial_id.replace("-", "_"),
                "ashlar_nonce": "a" * 32}}
        with tempfile.TemporaryDirectory(dir=Path.cwd()) as temporary:
            receipt_path = Path(temporary) / "local-qualification.json"
            receipt_path.write_bytes(b"qualified local release")
            config = build_config(runtime_result={"principal": owner},
                trial_id=trial_id, notebook_root="/Workspace/Users/owner@example.test/ashlar",
                environment_version=None,
                dependencies=[{"path": "/Workspace/Shared/ashlar.whl", "sha256": "a" * 64}],
                local_receipt=receipt_path,
                evidence_dir="docs/helix/04-build/evidence/test",
                bun_path=Path(shutil.which("bun")), python=Path(sys.executable),
                execution_profile="classic_existing_cluster",
                classic_cluster_readback=cluster, remaining_wall_seconds=840)
        wire = {"identity": MANAGED_CLASSIC_PROFILE.identity,
            "reader_versions": sorted(MANAGED_CLASSIC_PROFILE.reader_versions),
            "writer_versions": sorted(MANAGED_CLASSIC_PROFILE.writer_versions),
            "features": sorted(MANAGED_CLASSIC_PROFILE.features)}
        plan = SimpleNamespace(predecessor=SimpleNamespace(manifest={"publication_id": "prior"}),
            selection=SimpleNamespace(schedule_sha256="b" * 64,
                schema_revisions={"test": "revision-1"}))
        captured = {}
        def compose(**kwargs):
            captured.update(kwargs)
            return SimpleNamespace(selection=SimpleNamespace(
                reader_config=kwargs["reader_config"]), vector=object())
        with patch("ashlar_host.managed_current_a1_retention_builder.build_managed_current_a1_retention_bundle",
                   side_effect=compose), patch(
                "ashlar_host.truss_current_catalog.installed_fresh_truss_runtime_pins",
                return_value={"version": "0.0.55"}):
            bundle = _retention_bundle(trial=config["trial"], protocol_profile=wire,
                plan=plan, request_bytes=b"{}", manifest_bytes=b"{}",
                destination_receipt=b"{}", task_receipt={}, prefix_bytes=b"prefix",
                prefix_sha="c" * 64, selected_prefix={}, source_snapshot=b"snapshot",
                custody_digests={})
        self.assertIs(type(bundle.selection.reader_config), ManagedCurrentSparkClassicTaskReceiptConfig)
        self.assertTrue(bundle.selection.reader_config.operational_observation_stage)
        self.assertEqual(captured["reader_config"].expected_runtime["clusterId"], cluster["cluster_id"])

    def test_serverless_open_deadline_interrupts_a_blocked_connect_request(self):
        import threading
        import time

        entered = threading.Event()
        release = threading.Event()
        spark = _SparkConnect(self.manifest, self.receipt, self.uuids)
        original_sql = spark.sql
        def blocked_sql(sql, args=None):
            tags = spark.getTags()
            spark.query_tag_snapshots.append(tags)
            owned = next(tag for tag in tags if tag.startswith(
                "ashlar-managed-current-close-"))
            spark.active_tagged_operations.add(owned)
            entered.set()
            try:
                release.wait(3)
                return original_sql(sql, args=args)
            finally:
                spark.active_tagged_operations.discard(owned)
        spark.sql = blocked_sql
        runtime = {"runtimeName": "databricks-serverless", "sparkVersion": "4.2.0",
            "sparkBuildText": "Spark build", "runtimeTag": "serverless-4.2.0",
            "principal": self.config.expected_principal, "catalog": self.config.catalog,
            "schema": self.config.schema, "protocol": self.config.protocol_profile.identity}
        selected = ManagedCurrentSparkServerlessReceiptConfig(
            self.config.catalog, self.config.schema, self.config.expected_principal,
            self.config.expected_table_owner, self.config.source_truss_version,
            self.config.protocol_profile, runtime, maximum_elapsed_seconds=1,
            carrier_profile=self.config.carrier_profile)
        started = time.monotonic()
        try:
            with self.assertRaisesRegex(ValueError, "operation completion remains unknown"):
                ManagedCurrentSparkServerlessReceiptReader(spark, selected)
            self.assertTrue(entered.is_set())
            self.assertLess(time.monotonic() - started, 2.5)
            deadline = time.monotonic() + 1
            while not spark.interrupt_calls and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertEqual(len(spark.interrupt_calls), 1)
            self.assertEqual(spark.interrupt_saw_active_operation, [True])
            self.assertTrue(any(spark.interrupt_calls[0] in tags
                                for tags in spark.query_tag_snapshots))
        finally:
            release.set()

    def test_serverless_close_timeout_uses_tag_interrupt_without_claiming_termination(self):
        class ImmediateTimer:
            def __init__(self, _seconds, callback):
                self.callback = callback
                self.alive = False
            def start(self):
                self.alive = True
                self.callback()
                self.alive = False
            def cancel(self): pass
            def join(self, _timeout): pass
            def is_alive(self): return self.alive

        spark, reader = self._serverless_reader(interrupt_result=["operation-123"])
        before = set(reader._serverless_tag.preexisting_tags)
        reader.reopen(request_bytes=_canonical(self.request).encode(),
            manifest_bytes=_canonical(self.manifest).encode(), expected_receipt=self.receipt)
        with patch("ashlar_host.managed_current_spark_receipt_read.threading.Timer", ImmediateTimer):
            with self.assertRaisesRegex(ValueError, "termination was not established"):
                reader.close()
        self.assertEqual(len(spark.interrupt_calls), 1)
        self.assertEqual(spark.getTags(), before)
        self.assertIsNone(reader._observation.runtime_identity.get("closing"))


    def test_fresh_053_is_admitted_as_a_versioned_managed_reader_profile(self):
        fresh = replace(self.config, source_truss_version="0.0.53")
        self.assertEqual(fresh.source_truss_version, "0.0.53")
        with self.assertRaisesRegex(ValueError, "runtime, UC namespace"):
            replace(self.config, source_truss_version="0.0.46")

    def test_fresh_054_is_admitted_as_a_versioned_managed_reader_profile(self):
        fresh = replace(self.config, source_truss_version="0.0.54")
        self.assertEqual(fresh.source_truss_version, "0.0.54")
        with self.assertRaisesRegex(ValueError, "runtime, UC namespace"):
            replace(self.config, source_truss_version="0.0.56")

    def test_fresh_055_is_admitted_as_a_versioned_managed_reader_profile(self):
        fresh = replace(self.config, source_truss_version="0.0.55")
        self.assertEqual(fresh.source_truss_version, "0.0.55")
        with self.assertRaisesRegex(ValueError, "runtime, UC namespace"):
            replace(self.config, source_truss_version="0.0.56")

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
        self.assertEqual(observation.attempt_custody.table_version, 0)
        self.assertRegex(observation.attempt_custody.committed_payload_sha256, r"^[0-9a-f]{64}$")
        self.assertRegex(observation.attempt_custody.plan_sha256, r"^[0-9a-f]{64}$")
        self.assertFalse(any("warehouse" in sql.lower() for sql in self.spark.calls))

    def test_attempt_read_captures_161_sentinel_independent_of_general_row_budget(self):
        self.reader.config = __import__("dataclasses").replace(self.config, maximum_rows=100,
                                                                maximum_result_bytes=4194304)
        self.spark.attempt_rows = self.spark.attempt_rows * 32 + self.spark.attempt_rows[:1]
        with self.assertRaisesRegex(ValueError, "source-qualified phase inventory"):
            self.reopen()
        self.assertTrue(any("LIMIT 161" in sql for sql in self.spark.calls))
        self.assertIn(161, self.spark.limits)

    def test_attempt_read_refuses_gaps_duplicates_changed_plan_and_manifest_mismatch(self):
        for label, mutate in (
            ("gap", lambda rows: rows.__delitem__(2)),
            ("duplicate", lambda rows: rows.append(dict(rows[-1]))),
            ("plan", lambda rows: _change_attempt_value(rows, "plan_json", '{"changed":true}', phase="committed")),
            ("result", lambda rows: _change_attempt_value(rows, "result_json", '{"effects":{},"manifest":{}}', phase="committed")),
            ("request", lambda rows: _change_attempt_request(rows, phase="committed")),
            ("manifest", lambda rows: _change_attempt_descriptor(rows)),
        ):
            with self.subTest(label=label):
                request, manifest, receipt, uuids, sql_config = _fixture()
                spark = _Spark(manifest, receipt, uuids)
                mutate(spark.attempt_rows)
                reader = ManagedCurrentSparkReceiptReader(spark, _spark_config(sql_config))
                with self.assertRaises(ValueError):
                    reader.reopen(request_bytes=_canonical(request).encode(),
                        manifest_bytes=_canonical(manifest).encode(), expected_receipt=receipt)

    def test_attempt_read_refuses_attempt_uuid_drift_between_bookends(self):
        original = self.spark.sql
        def drift(sql, args=None):
            frame = original(sql, args)
            if "publication_attempt_phase` VERSION AS OF " in sql:
                self.spark.uuids["publication_attempt_phase"] = "ffffffff-ffff-ffff-ffff-ffffffffffff"
            return frame
        self.spark.sql = drift
        with self.assertRaisesRegex(ValueError, "identity changed during committed read"):
            self.reopen()

    def test_attempt_read_applies_whole_result_byte_budget(self):
        self.reader.config = __import__("dataclasses").replace(self.config,
                                                                maximum_result_bytes=4096)
        with self.assertRaisesRegex(ValueError, "native result exceeds byte budget"):
            self.reopen()

    def test_attempt_query_requires_exact_template_and_bound_scope(self):
        base = "`client_dev`.`trial_schema`.`publication_attempt_phase` VERSION AS OF 0"
        prefix = ("SELECT phase,request_digest,payload_json,payload_digest FROM " + base
                  + " WHERE stream=:stream AND batch_id=:batch ORDER BY CASE phase "
                  + "WHEN 'prepared' THEN 0 WHEN 'applying' THEN 1 WHEN 'applied' THEN 2 "
                  + "WHEN 'committing' THEN 3 WHEN 'committed' THEN 4 END LIMIT 161")
        for sql, params in ((prefix + " /* comment */", {"stream": "s", "batch": "b"}),
                            (prefix.replace("VERSION AS OF 0", "VERSION AS OF 1"),
                             {"stream": "s", "batch": "b"}),
                            (prefix, {"stream": "s"})):
            with self.subTest(sql=sql, params=params), self.assertRaises(ValueError):
                self.reader.executor.query(sql, params)

    def test_current_empty_source_prefix_restores_and_freezes_all_four_bags(self):
        config, prefix, data = _empty_current_source_prefix()
        oracle = build_managed_current_prefix_oracle(data,
            prefix_sha256=hashlib.sha256(data).hexdigest(), config=config)
        self.assertEqual(oracle.prefix_sha256, hashlib.sha256(data).hexdigest())
        self.assertEqual(oracle.final_request, json.loads(prefix["entries"][0]["requestJson"]))
        self.assertEqual(oracle.progress, prefix["progress"])
        self.assertEqual({role: rows for role, rows in oracle.expected_rows.items()},
                         {role: [] for role in ("object_current", "edge_current", "tombstone",
                                                "whole_source_history")})
        changed = copy.deepcopy(prefix)
        changed["progress"] = {}
        forged = _canonical(changed).encode()
        with self.assertRaisesRegex(ValueError, "encountered-feed progress"):
            build_managed_current_prefix_oracle(forged,
                prefix_sha256=hashlib.sha256(forged).hexdigest(), config=config)
        with self.assertRaisesRegex(ValueError, "pinned bounded source-prefix bytes"):
            build_managed_current_prefix_oracle(data,
                prefix_sha256="0" * 64, config=config)
        changed_schedule = copy.deepcopy(prefix)
        changed_schedule["scheduleStep"] = 2
        raw = _canonical(changed_schedule).encode()
        with self.assertRaises(ValueError):
            build_managed_current_prefix_oracle(raw,
                prefix_sha256=hashlib.sha256(raw).hexdigest(), config=config)
        from ashlar.attempt_store import verify_request_digest
        forged = copy.deepcopy(prefix)
        request = json.loads(forged["entries"][0]["requestJson"])
        request["schema_revisions_json"] = _canonical({"forged": "0" * 64})
        unsigned = dict(request); unsigned.pop("request_digest")
        request["request_digest"] = hashlib.sha256(_canonical(unsigned).encode()).hexdigest()
        verify_request_digest(request)
        forged["entries"][0]["requestJson"] = _canonical(request)
        raw = _canonical(forged).encode()
        with self.assertRaisesRegex(ValueError, "schema revisions must bind"):
            build_managed_current_prefix_oracle(raw,
                prefix_sha256=hashlib.sha256(raw).hexdigest(), config=config)

    def test_source_prefix_rejects_duplicate_keys_bad_clock_unknown_entry_and_forged_bundle(self):
        config, prefix, data = _empty_current_source_prefix()
        for label, change in (
            ("clock", lambda item: item["entries"][0].__setitem__("materializationClockUs", "01")),
            ("entry key", lambda item: item["entries"][0].__setitem__("unknown", "value")),
            ("bundle", lambda item: item["entries"][0].__setitem__("bundleJson", "{}")),
        ):
            with self.subTest(label=label):
                altered = copy.deepcopy(prefix); change(altered)
                raw = _canonical(altered).encode()
                with self.assertRaises(ValueError):
                    build_managed_current_prefix_oracle(raw,
                        prefix_sha256=hashlib.sha256(raw).hexdigest(), config=config)
        duplicate = data.replace(b'"profile":"ashlar-managed-current-source-prefix/0.1"',
                                 b'"profile":"ashlar-managed-current-source-prefix/0.1",'
                                 b'"profile":"ashlar-managed-current-source-prefix/0.1"')
        with self.assertRaisesRegex(ValueError, "Invalid retained JSON"):
            build_managed_current_prefix_oracle(duplicate,
                prefix_sha256=hashlib.sha256(duplicate).hexdigest(), config=config)

    def test_authored_historical_page_projects_nonempty_bags_offline_only(self):
        from ashlar.apply import empty_state, plan_apply
        from ashlar_host.resources import RESOURCE_ROOT
        from ashlar_host.schema_rows import fixture_columns
        from ashlar_host.truss_graph_admission import TrussGraphAdmission
        from test_truss_graph_admission import Tests as AuthoredFixture

        batch, catalog_views = AuthoredFixture().original()
        admission = TrussGraphAdmission(batch, catalog_views)
        state = plan_apply(empty_state(), admission.changes, schema_policy=admission.admit,
                           schema_transition_policy=admission.admit_transition)
        columns = fixture_columns(RESOURCE_ROOT)
        bags = graph_expected_rows({role: [] for role in columns}, state, admission.changes,
            batch, columns, "2026-10-23T12:00:00.000000+00:00", admission.parsed.event_bytes,
            admission.parsed.edge_order_keys)
        # This deliberately historical authored fixture tests the shared source
        # projection only; it is not admitted as a current Truss source bundle.
        self.assertEqual((len(bags["object_current"]), len(bags["edge_current"]),
                          len(bags["whole_source_history"])), (2, 1, 3))
        from ashlar_host.managed_current_core import CARRIER_PROFILE_LEGACY
        self.assertRegex(compare_complete_managed_current_bags(
            bags, bags, carrier_profile=CARRIER_PROFILE_LEGACY), r"^[0-9a-f]{64}$")
        changed = copy.deepcopy(bags)
        changed["object_current"].append(copy.deepcopy(changed["object_current"][0]))
        with self.assertRaisesRegex(ValueError, "bag differs"):
            compare_complete_managed_current_bags(
                bags, changed, carrier_profile=CARRIER_PROFILE_LEGACY)
        oversized = {role: [] for role in bags}
        oversized["object_current"] = bags["object_current"] * 1025
        with self.assertRaisesRegex(ValueError, "exceeds bounded inputs"):
            compare_complete_managed_current_bags(
                oversized, bags, carrier_profile=CARRIER_PROFILE_LEGACY)

    def test_optional_source_oracle_captures_exact_versioned_rows_and_utc(self):
        oracle = _authored_adapter_oracle(self, {})
        observation = self.reader.reopen(request_bytes=_canonical(self.request).encode(),
            manifest_bytes=_canonical(self.manifest).encode(), expected_receipt=self.receipt,
            source_oracle=oracle)
        self.assertRegex(observation.bag_comparison_sha256, r"^[0-9a-f]{64}$")
        self.assertEqual(observation.source_prefix_sha256, oracle.prefix_sha256)
        self.assertEqual(self.reader.opening_runtime["sessionTimeZone"], "UTC")
        data_sql = [sql for sql in self.spark.calls if "LIMIT 1025" in sql]
        self.assertEqual(len(data_sql), 4)
        self.assertTrue(all("VERSION AS OF" in sql for sql in data_sql))
        self.assertTrue(any("unix_micros" in sql for sql in data_sql))
        self.assertIn(1025, self.spark.limits)
        self.reader.close()
        closing_data_sql = [sql for sql in self.spark.calls if "LIMIT 1025" in sql]
        self.assertEqual(len(closing_data_sql), 8)
        self.assertEqual(observation.runtime_identity["closing"]["sessionTimeZone"], "UTC")
        self.assertEqual(self.reader._approved_data_queries, set())

    def test_operational_reader_stage_does_one_full_bag_and_fresh_resolver_close(self):
        oracle = _authored_adapter_oracle(self, {})
        spark, reader = self._serverless_reader(operational_observation_stage=True)
        try:
            constructor_end = len(spark.calls)
            observation = reader.reopen(request_bytes=_canonical(self.request).encode(),
                manifest_bytes=_canonical(self.manifest).encode(), expected_receipt=self.receipt,
                source_oracle=oracle)
            reopen_end = len(spark.calls)
            self.assertRegex(observation.bag_comparison_sha256, r"^[0-9a-f]{64}$")
            self.assertEqual(sum("LIMIT 1025" in sql for sql in spark.calls), 4)
            reopen_sql = spark.calls[constructor_end:reopen_end]
            self.assertEqual(sum(sql.startswith("DESCRIBE DETAIL") for sql in reopen_sql), 18)
            self.assertFalse(any(sql.startswith("DESCRIBE HISTORY") or
                                 "system.information_schema" in sql or
                                 "current_timezone()" in sql for sql in reopen_sql))
            self.assertIsNone(reader._operational_reopen_witness)
            reader.close()
            self.assertEqual(sum("LIMIT 1025" in sql for sql in spark.calls), 4)
            close_sql = spark.calls[reopen_end:]
            self.assertEqual(sum(sql.startswith("DESCRIBE DETAIL") for sql in close_sql), 14)
            self.assertEqual(sum(sql.startswith("DESCRIBE HISTORY") for sql in close_sql), 6)
            manifest_reads = [sql for sql in spark.calls
                if "publication_manifest` WHERE publication_id" in sql]
            self.assertGreaterEqual(len(manifest_reads), 2)
            self.assertTrue(any("VERSION AS OF" in sql for sql in spark.calls
                                if "publication_attempt_phase` VERSION AS OF" in sql))
        finally:
            if not reader.closed:
                reader._serverless_tag.close()

    def test_operational_reopen_witness_is_cleared_when_resolver_fails(self):
        spark, reader = self._serverless_reader(operational_observation_stage=True)
        try:
            with patch("ashlar_host.managed_current_spark_receipt_read.resolve_publication",
                       side_effect=RuntimeError("resolver stopped")):
                with self.assertRaisesRegex(RuntimeError, "resolver stopped"):
                    reader.reopen(request_bytes=_canonical(self.request).encode(),
                        manifest_bytes=_canonical(self.manifest).encode(),
                        expected_receipt=self.receipt,
                        source_oracle=_authored_adapter_oracle(self, {}))
            self.assertIsNone(reader._operational_reopen_witness)
        finally:
            if not reader.closed:
                reader._serverless_tag.close()

    def test_exact_version_bag_digest_is_stable_under_native_row_reordering(self):
        role = "object_current"
        first_row = _native_test_row(self.reader.carriers, role, "first")
        second_row = _native_test_row(self.reader.carriers, role, "second")
        expected_rows = {name: [] for name in ("object_current", "edge_current",
                                                "tombstone", "whole_source_history")}
        expected_rows[role] = [first_row, second_row]
        oracle = _authored_adapter_oracle(self, expected_rows)
        first_spark, first_reader = self._serverless_reader(
            operational_observation_stage=True)
        first_spark.role_rows[role] = [first_row, second_row]
        first = first_reader.reopen(request_bytes=_canonical(self.request).encode(),
            manifest_bytes=_canonical(self.manifest).encode(), expected_receipt=self.receipt,
            source_oracle=oracle)
        first_reader.close()

        second_spark, second_reader = self._serverless_reader(
            operational_observation_stage=True)
        second_spark.role_rows[role] = [second_row, first_row]
        second = second_reader.reopen(request_bytes=_canonical(self.request).encode(),
            manifest_bytes=_canonical(self.manifest).encode(), expected_receipt=self.receipt,
            source_oracle=oracle)
        second_reader.close()
        self.assertEqual(first.bag_comparison_sha256, second.bag_comparison_sha256)
        self.assertEqual(first.bag_comparison_summary["roles"][role]["comparison_sha256"],
                         second.bag_comparison_summary["roles"][role]["comparison_sha256"])

    def test_close_repeats_source_derived_witness_and_refuses_data_drift(self):
        oracle = _authored_adapter_oracle(self, {})
        self.reader.reopen(request_bytes=_canonical(self.request).encode(),
            manifest_bytes=_canonical(self.manifest).encode(), expected_receipt=self.receipt,
            source_oracle=oracle)
        row = _native_test_row(self.reader.carriers, "object_current", "changed-at-close")
        self.spark.role_rows["object_current"] = [row]
        with self.assertRaisesRegex(ValueError, "bag differs"):
            self.reader.close()
        self.assertEqual(self.reader._approved_data_queries, set())
        self.assertEqual(sum("LIMIT 1025" in sql for sql in self.spark.calls), 8)

    def test_source_oracle_anchor_mismatch_refuses_before_versioned_reads(self):
        oracle = _authored_adapter_oracle(self, {})
        oracle = __import__("dataclasses").replace(oracle, final_request_json="{}")
        with self.assertRaisesRegex(ValueError, "Frozen prefix comparison anchors"):
            self.reader.reopen(request_bytes=_canonical(self.request).encode(),
                manifest_bytes=_canonical(self.manifest).encode(), expected_receipt=self.receipt,
                source_oracle=oracle)
        self.assertFalse(any("LIMIT 1025" in sql for sql in self.spark.calls))

    def test_source_expected_bag_bounds_refuse_before_reads(self):
        oracle = _authored_adapter_oracle(self, {})
        oversized = {name: [] for name in ("object_current", "edge_current", "tombstone",
                                           "whole_source_history")}
        oversized["object_current"] = [{} for _ in range(1025)]
        oracle = __import__("dataclasses").replace(oracle,
            expected_rows_json=_canonical(oversized))
        with self.assertRaisesRegex(ValueError, "Frozen source comparison bags exceed"):
            self.reader.reopen(request_bytes=_canonical(self.request).encode(),
                manifest_bytes=_canonical(self.manifest).encode(), expected_receipt=self.receipt,
                source_oracle=oracle)
        self.assertFalse(any("LIMIT 1025" in sql for sql in self.spark.calls))

    def test_manifest_extra_unchanged_feed_refuses_reduced_oracle(self):
        oracle = _authored_adapter_oracle(self, {})
        manifest = copy.deepcopy(self.manifest)
        progress = json.loads(manifest["source_progress_json"])
        progress["second-feed"] = {"feed": "second-feed", "epoch": "epoch-b",
                                    "position": "checkpoint-b"}
        manifest["source_progress_json"] = _canonical(progress)
        versions = json.loads(manifest["table_versions_json"])
        role_tables = {".".join((self.config.catalog, self.config.schema, role)): role
                       for role in ("object_current", "edge_current", "tombstone",
                                    "whole_source_history")}
        with self.assertRaisesRegex(ValueError, "Frozen prefix comparison anchors"):
            self.reader._read_and_compare_source_bags(oracle, self.request, manifest,
                                                       versions, role_tables)
        self.assertFalse(any("LIMIT 1025" in sql for sql in self.spark.calls))

    def test_versioned_capture_refuses_uuid_drift_at_bookend_and_clears_allowlist(self):
        self.spark.role_identity_drift = "object_current"
        with self.assertRaisesRegex(ValueError, "identity changed during complete bag capture"):
            self.reader.reopen(request_bytes=_canonical(self.request).encode(),
                manifest_bytes=_canonical(self.manifest).encode(), expected_receipt=self.receipt,
                source_oracle=_authored_adapter_oracle(self, {}))
        self.assertEqual(self.reader._approved_data_queries, set())

    def test_native_capture_is_order_insensitive_and_keeps_large_integer_tokens(self):
        role = "object_current"
        first = _native_test_row(self.reader.carriers, role, "first")
        second = _native_test_row(self.reader.carriers, role, "second")
        expected = {name: [] for name in ("object_current", "edge_current", "tombstone",
                                          "whole_source_history")}
        expected[role] = [first, second]
        self.spark.role_rows[role] = [second, first]
        oracle = _authored_adapter_oracle(self, expected)
        observation = self.reader.reopen(request_bytes=_canonical(self.request).encode(),
            manifest_bytes=_canonical(self.manifest).encode(), expected_receipt=self.receipt,
            source_oracle=oracle)
        self.assertRegex(observation.bag_comparison_sha256, r"^[0-9a-f]{64}$")

    def test_native_capture_rejects_changed_opaque_payload_token(self):
        role = "object_current"
        expected_row = _native_test_row(self.reader.carriers, role, "original")
        self.spark.role_rows[role] = [dict(expected_row)]
        expected = {name: [] for name in ("object_current", "edge_current", "tombstone",
                                          "whole_source_history")}
        expected[role] = [expected_row]
        oracle = _authored_adapter_oracle(self, expected)
        payload_column = next(name for name, kind, _ in self.reader.carriers.ordered_fields(role)
                              if kind == "STRING" and name == "props_json")
        self.spark.role_rows[role][0][payload_column] = '{"changed":true}'
        with self.assertRaisesRegex(ValueError, "bag differs"):
            self.reader.reopen(request_bytes=_canonical(self.request).encode(),
                manifest_bytes=_canonical(self.manifest).encode(), expected_receipt=self.receipt,
                source_oracle=oracle)

    def test_capture_enforces_cumulative_actual_row_limit(self):
        roles = ("object_current", "edge_current", "tombstone", "whole_source_history")
        for role in roles:
            row = _native_test_row(self.reader.carriers, role, "fixture")
            self.spark.role_rows[role] = [dict(row) for _ in range(257)]
        with self.assertRaisesRegex(ValueError, "cumulative row or byte bound"):
            self.reader.reopen(request_bytes=_canonical(self.request).encode(),
                manifest_bytes=_canonical(self.manifest).encode(), expected_receipt=self.receipt,
                source_oracle=_authored_adapter_oracle(self, {}))

    def test_capture_rejects_bad_bigint_timezone_and_role_sentinel(self):
        role = "object_current"
        row = _native_test_row(self.reader.carriers, role, "fixture")
        bigint = next(name for name, kind, nullable in self.reader.carriers.ordered_fields(role)
                      if kind == "BIGINT" and not nullable)
        oracle = _authored_adapter_oracle(self, {})
        self.spark.role_rows[role] = [dict(row, **{bigint: "01"})]
        with self.assertRaisesRegex(ValueError, "canonical text"):
            self.reader.reopen(request_bytes=_canonical(self.request).encode(),
                manifest_bytes=_canonical(self.manifest).encode(), expected_receipt=self.receipt,
                source_oracle=oracle)
        self.reader, self.spark = ManagedCurrentSparkReceiptReader(
            _Spark(self.manifest, self.receipt, self.uuids), self.config), None
        self.spark = self.reader.spark
        self.spark.role_rows[role] = [dict(row) for _ in range(1025)]
        self.reader.config = __import__("dataclasses").replace(self.config, maximum_rows=16)
        with self.assertRaisesRegex(ValueError, "Complete bounded Spark result"):
            self.reader.reopen(request_bytes=_canonical(self.request).encode(),
                manifest_bytes=_canonical(self.manifest).encode(), expected_receipt=self.receipt,
                source_oracle=_authored_adapter_oracle(self, {}))
        self.assertIn(1025, self.spark.limits)

    def test_non_utc_session_timezone_refuses_opening(self):
        original_sql = self.spark.sql
        def altered_sql(sql, args=None):
            if "current_timezone()" in sql:
                return _Frame([{"session_timezone": "America/New_York"}],
                              (("session_timezone", "string"),))
            return original_sql(sql, args)
        self.spark.sql = altered_sql
        with self.assertRaisesRegex(ValueError, "UTC Spark session timezone"):
            ManagedCurrentSparkReceiptReader(self.spark, self.config)

    def test_xxhash_empty_prefix_is_rebuilt_after_zero_native_probes(self):
        (core_config, _prefix, prefix_bytes, request, manifest, receipt,
         uuids, spark_config, spark) = _xxhash_spark_case()
        keys = derive_managed_current_prefix_lookup_keys(prefix_bytes,
            prefix_sha256=hashlib.sha256(prefix_bytes).hexdigest(), config=core_config,
            expected_source_truss_version=spark_config.source_truss_version)
        self.assertEqual(keys, ())
        reader = ManagedCurrentSparkReceiptReader(spark, spark_config)
        observation = reader.reopen(request_bytes=_canonical(request).encode(),
            manifest_bytes=_canonical(manifest).encode(), expected_receipt=receipt,
            source_prefix_bytes=prefix_bytes,
            source_prefix_sha256=hashlib.sha256(prefix_bytes).hexdigest(),
            source_core_config=core_config)
        self.assertRegex(observation.bag_comparison_sha256, r"^[0-9a-f]{64}$")
        self.assertEqual(observation.source_prefix_sha256,
                         hashlib.sha256(prefix_bytes).hexdigest())
        self.assertFalse(any("xxhash64" in sql for sql in spark.calls))
        self.assertEqual(len([sql for sql in spark.calls if "LIMIT 1025" in sql]), 4)

    def test_xxhash_source_prefix_requires_the_reader_truss_version(self):
        (core_config, _prefix, prefix_bytes, _request, _manifest, _receipt,
         _uuids, spark_config, spark) = _xxhash_spark_case()
        with self.assertRaisesRegex(ValueError, "selected Truss source version"):
            derive_managed_current_prefix_lookup_keys(prefix_bytes,
                prefix_sha256=hashlib.sha256(prefix_bytes).hexdigest(), config=core_config,
                expected_source_truss_version=("0.0.44" if spark_config.source_truss_version != "0.0.44"
                                               else "0.0.49"))
        self.assertFalse(any("xxhash64" in sql for sql in spark.calls))

    def test_xxhash_probe_uses_exact_typed_parameters_and_preserves_full_keys(self):
        (_core, _prefix, _bytes, _request, manifest, receipt,
         uuids, spark_config, spark) = _xxhash_spark_case()
        reader = ManagedCurrentSparkReceiptReader(spark, spark_config)
        args = ("sourcé", str(-(1 << 63)), str((1 << 63) - 1))
        spark.hash_values[args] = -(1 << 63)
        keys = (("object", "sourcé", -(1 << 63), (1 << 63) - 1),
                ("edge", "sourcé", -(1 << 63), (1 << 63) - 1),
                ("object", "other-source", 1, 2))
        spark.hash_values[("other-source", "1", "2")] = -(1 << 63)
        observed = reader._probe_source_lookup_hashes(keys)
        self.assertEqual(observed, {key: -(1 << 63) for key in keys})
        probe_indexes = [i for i, sql in enumerate(spark.calls) if "xxhash64" in sql]
        self.assertEqual(len(probe_indexes), 2)
        self.assertEqual(spark.calls[-1],
            "SELECT xxhash64(CAST(:source AS STRING),CAST(:type AS BIGINT),"
            "CAST(:id AS BIGINT)) AS lookup_hash")
        selected = next(spark.parameters[index] for index in probe_indexes
                        if spark.parameters[index]["source"] == "sourcé")
        self.assertEqual(selected,
                         {"source": "sourcé", "type": str(-(1 << 63)),
                          "id": str((1 << 63) - 1)})
        self.assertEqual(reader._approved_hash_parameters, set())

    def test_xxhash_probe_refuses_bool_hash_invalid_identity_and_budget_exhaustion(self):
        (_core, _prefix, _bytes, _request, manifest, receipt,
         uuids, spark_config, spark) = _xxhash_spark_case()
        reader = ManagedCurrentSparkReceiptReader(spark, spark_config)
        key = (("object", "feed", 1, 2),)
        spark.hash_values[("feed", "1", "2")] = True
        with self.assertRaisesRegex(ValueError, "signed64 and non-Boolean"):
            reader._probe_source_lookup_hashes(key)
        self.assertEqual(reader._approved_hash_parameters, set())
        calls_before = len(spark.calls)
        with self.assertRaisesRegex(ValueError, "signed64 source identity"):
            reader._probe_source_lookup_hashes((("object", "feed", True, 2),))
        self.assertEqual(len(spark.calls), calls_before)
        too_many = tuple(("object", "feed", index, index + 1) for index in range(33))
        with self.assertRaisesRegex(ValueError, "at most 32 unique keys"):
            reader._probe_source_lookup_hashes(too_many)
        self.assertEqual(len(spark.calls), calls_before)
        spark.hash_rows_override = []
        with self.assertRaisesRegex(ValueError, "One exact native signed BIGINT hash"):
            reader._probe_source_lookup_hashes(key)
        self.assertEqual(reader._approved_hash_parameters, set())
        calls_before_budget = len(spark.calls)
        reader.statements = reader.config.maximum_statements - 24 - 12
        with self.assertRaisesRegex(ValueError, "Insufficient statement budget"):
            reader._probe_source_lookup_hashes(key)
        self.assertEqual(len(spark.calls), calls_before_budget)

    def test_xxhash_probe_inventory_keeps_intermediate_create_deleted_by_later_page(self):
        object_key = EntityKey("feed", "object", 7, 11)
        edge_key = EntityKey("feed", "edge", 9, 13)
        created = SimpleNamespace(current={object_key: object(), edge_key: object()})
        deleted = SimpleNamespace(current={})
        create_changes = SimpleNamespace(changes=[])
        delete_changes = SimpleNamespace(changes=[
            SimpleNamespace(state=SimpleNamespace(key=object_key), operation="delete"),
            SimpleNamespace(state=SimpleNamespace(key=edge_key), operation="delete"),
        ])
        tokens = _source_prefix_lookup_tokens((
            (SimpleNamespace(), created, create_changes, "clock-1"),
            (created, deleted, delete_changes, "clock-2"),
        ))
        self.assertEqual(tokens, {
            ("object", "feed", 7, 11), ("edge", "feed", 9, 13),
        })

    def test_xxhash_probe_refuses_oversized_source_utf8_token_before_query(self):
        (_core, _prefix, _bytes, _request, _manifest, _receipt,
         _uuids, _spark_config, spark) = _xxhash_spark_case()
        reader = ManagedCurrentSparkReceiptReader(spark, _spark_config)
        before = len(spark.calls)
        key = (("object", "é" * 32769, 1, 2),)
        with self.assertRaisesRegex(ValueError, "65536 UTF-8 bytes"):
            reader._probe_source_lookup_hashes(key)
        self.assertEqual(len(spark.calls), before)

    def test_xxhash_prebuilt_oracle_cannot_claim_native_probe(self):
        (_core, _prefix, _bytes, request, manifest, receipt,
         uuids, spark_config, spark) = _xxhash_spark_case()
        reader = ManagedCurrentSparkReceiptReader(spark, spark_config)
        oracle = ManagedCurrentSourcePrefixOracle("ashlar-managed-current-source-prefix/0.1",
            CARRIER_PROFILE_XXHASH64, "a" * 64, "b" * 64, 1,
            _canonical({"catalog": spark_config.catalog, "schema": spark_config.schema}),
            manifest["source_progress_json"], _canonical(request), "[]",
            _canonical({role: [] for role in ("object_current", "edge_current", "tombstone",
                                               "whole_source_history")}))
        with self.assertRaisesRegex(ValueError, "Prebuilt .4 source oracles"):
            reader.reopen(request_bytes=_canonical(request).encode(),
                manifest_bytes=_canonical(manifest).encode(), expected_receipt=receipt,
                source_oracle=oracle)
        self.assertFalse(any("xxhash64" in sql for sql in spark.calls))

    def test_timezone_change_at_closing_withholds_closing_inventory(self):
        observation = self.reopen()
        self.spark.timezone = "America/New_York"
        with self.assertRaisesRegex(ValueError, "UTC Spark session timezone"):
            self.reader.close()
        self.assertEqual(observation.runtime_identity["closing"], None)

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

    def test_general_query_refuses_its_over_limit_sentinel(self):
        self.reader.config = __import__("dataclasses").replace(self.config, maximum_rows=16)
        self.spark.sql = lambda sql, args=None: _Frame(
            [{"sparkBuildText": "build"} for _ in range(17)], (("sparkBuildText", "string"),))
        with self.assertRaisesRegex(ValueError, "Complete bounded Spark result"):
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


class ManagedOperationalRetentionIntervalTests(unittest.TestCase):
    def setUp(self):
        (self.core_config, self.prefix, self.prefix_bytes, self.request,
         self.manifest, self.receipt, self.uuids, self.config, _spark) = _xxhash_spark_case()
        self.events = []
        self.sessions = []
        self.custody = {key: hashlib.sha256(key.encode()).hexdigest()
                        for key in SOURCE_CUSTODY_FIELDS}
        self.selection = ManagedOperationalRetentionSelection(
            reader_config=self.config,
            request_bytes=_canonical(self.request).encode(),
            manifest_bytes=_canonical(self.manifest).encode(),
            receipt_bytes=_canonical(self.receipt).encode(),
            expected_receipt=self.receipt,
            vector=manifest_pin_vector(self.manifest, {
                f"{self.config.catalog}.{self.config.schema}.{role}": self.uuids[role]
                for role in ("object_current", "edge_current", "tombstone", "whole_source_history")
            }, authority=f"managed-current-spark:{self.config.expected_principal}"),
            expected_source_custody=self.custody,
            source_prefix_bytes=self.prefix_bytes,
            source_prefix_sha256=hashlib.sha256(self.prefix_bytes).hexdigest(),
            source_core_config=self.core_config,
        )
        test_case = self

        class Session:
            def __init__(self, stage, session_id):
                self.stage, self.session_id = stage, session_id
                self.spark = _Spark(test_case.manifest, test_case.receipt, test_case.uuids,
                                    request=test_case.request)
                self.closed = False

            def close(self):
                self.closed = True
                test_case.events.append(("session-close", self.stage))

        class SessionFactory:
            def open(self, *, stage, deadline_monotonic):
                test_case.events.append(("session-open", stage))
                session = Session(stage, f"session-{len(test_case.sessions) + 1}")
                test_case.sessions.append(session)
                return session

        class SourceObserver:
            def observe(self, *, stage, selection, vector):
                test_case.events.append(("source", stage))
                return dict(test_case.custody)

        class PolicyObserver:
            def observe(self, *, stage, session, vector):
                reads = sum("LIMIT 1025" in sql for sql in session.spark.calls)
                test_case.events.append(("policy", stage, reads))
                return test_case._policy()

        self.session_factory = SessionFactory()
        self.source_observer = SourceObserver()
        self.policy_observer = PolicyObserver()

    def test_a1_selected_prefix_anchors_are_snapshotted_as_original_bytes(self):
        selected = {"sources": {"A": {"identity": {"feed": "feed-a"}}}}
        plan = ManagedCurrentA1TrialPlan(
            SimpleNamespace(schedule_bytes=b"original schedule"), None,
            self.request, {}, None)
        observed = []

        def verify(_plan, prefix_bytes, *, prefix_sha256, selected, schedule_bytes, pages):
            observed.append(selected)
            return object()

        with patch.object(ManagedCurrentA1TrialPlan, "verify_delivery", verify):
            selection = ManagedOperationalRetentionSelection(
                reader_config=replace(self.config, source_truss_version="0.0.53"),
                request_bytes=_canonical(self.request).encode(),
                manifest_bytes=_canonical(self.manifest).encode(),
                receipt_bytes=_canonical(self.receipt).encode(),
                expected_receipt=self.receipt, vector=self.selection.vector,
                expected_source_custody=self.custody,
                source_prefix_bytes=self.prefix_bytes,
                source_prefix_sha256=hashlib.sha256(self.prefix_bytes).hexdigest(),
                source_core_config=self.core_config,
                a1_plan=plan,
                a1_prefix_selected=selected)

        selected["sources"]["A"]["identity"]["feed"] = "changed-after-admission"
        self.assertEqual(selection.selected_a1_prefix()["sources"]["A"]["identity"]["feed"],
                         "feed-a")
        stage_copy = selection.selected_a1_prefix()
        stage_copy["sources"]["A"]["identity"]["feed"] = "changed-stage-copy"
        self.assertEqual(selection.selected_a1_prefix()["sources"]["A"]["identity"]["feed"],
                         "feed-a")
        self.assertEqual(observed[0]["sources"]["A"]["identity"]["feed"], "feed-a")

    @staticmethod
    def _policy():
        return {
            "retention_settings": {
                role: {"data_retention_seconds": 604800, "log_retention_seconds": 604800}
                for role in ("object_current", "edge_current", "tombstone", "whole_source_history")
            },
            "predictive_optimization": "ENABLE",
            "maintenance_inventory_sha256": "b" * 64,
        }

    def test_open_pre_ack_close_each_own_fresh_session_and_read_all_four_roles(self):
        interval = OperationalRetentionInterval.open(
            self.selection, self.session_factory, self.source_observer, self.policy_observer)
        interval.renew_pre_ack()
        receipt = interval.close()

        self.assertEqual(receipt.outcome, "observed_available_at_checks")
        self.assertEqual(receipt.authority, "read_observation_only_no_source_ack")
        self.assertEqual([item.stage for item in receipt.observations],
                         ["open", "pre_ack", "close"])
        self.assertEqual([item.stage for item in self.sessions],
                         ["open", "pre_ack", "close"])
        self.assertTrue(all(item.closed for item in self.sessions))
        self.assertTrue(all(sum("LIMIT 1025" in sql for sql in item.spark.calls) == 8
                            for item in self.sessions))
        self.assertEqual([event for event in self.events if event[0] == "policy"],
                         [("policy", stage, 4) for stage in ("open", "pre_ack", "close")])
        for stage in ("open", "pre_ack", "close"):
            self.assertEqual(
                [event[0] for event in self.events if len(event) > 1 and event[1] == stage],
                ["session-open", "policy", "session-close", "source"],
            )

    def test_post_ack_close_only_is_one_fresh_four_role_observation(self):
        receipt = OperationalRetentionInterval.observe_close_only(
            self.selection, self.session_factory, self.source_observer,
            self.policy_observer)
        self.assertEqual(receipt.outcome, "observed_available_at_close")
        self.assertEqual(receipt.authority, "read_observation_only_no_source_ack")
        self.assertEqual([item.stage for item in receipt.observations], ["close"])
        self.assertEqual([item.stage for item in self.sessions], ["close"])
        self.assertTrue(self.sessions[0].closed)
        self.assertEqual([event for event in self.events if event[0] == "policy"],
                         [("policy", "close", 4)])
        self.assertEqual([event for event in self.events if event[0] == "source"],
                         [("source", "close")])

    def test_parent_interval_uuid_spans_nonterminal_pre_ack_and_post_ack_close(self):
        context = object()
        interval_id = "3c9ca5c5-a1e8-4990-b7dd-f7b405f014c8"
        port = ManagedOperationalReadPort(self.selection, self.session_factory,
            self.source_observer, self.policy_observer, context=context)
        pre_ack = port.observe_pre_ack(self.selection.receipt_bytes,
            self.selection.vector, context, interval_id=interval_id)
        self.assertEqual(pre_ack.profile,
            "ashlar-managed-operational-retention-pre-ack/0.1")
        self.assertEqual(pre_ack.outcome, "pre_ack_checks_complete_close_pending")
        self.assertEqual(pre_ack.interval_id, interval_id)
        self.assertEqual([item.stage for item in pre_ack.observations], ["open", "pre_ack"])
        self.assertEqual([item.stage for item in self.sessions], ["open", "pre_ack"])
        self.assertTrue(all(item.closed for item in self.sessions))
        with self.assertRaisesRegex(ManagedOperationalReadPortError, "Exact selected"):
            port.observe_pre_ack(self.selection.receipt_bytes,
                self.selection.vector, context, interval_id=interval_id)

        close = OperationalRetentionInterval.observe_close_only(self.selection,
            self.session_factory, self.source_observer, self.policy_observer,
            interval_id=interval_id)
        self.assertEqual(close.profile, "ashlar-managed-operational-retention/0.1")
        self.assertEqual(close.outcome, "observed_available_at_close")
        self.assertEqual(close.interval_id, interval_id)
        self.assertEqual([item.stage for item in close.observations], ["close"])
        self.assertEqual([item.stage for item in self.sessions], ["open", "pre_ack", "close"])

    def test_noncanonical_parent_interval_uuid_refuses_before_any_observation(self):
        with self.assertRaisesRegex(OperationalRetentionIntervalError,
                                    "Canonical parent-owned operational interval UUID"):
            OperationalRetentionInterval.observe_pre_ack(self.selection,
                self.session_factory, self.source_observer, self.policy_observer,
                interval_id="not-a-uuid")
        self.assertEqual(self.sessions, [])

    def test_serverless_interval_allows_only_same_task_borrowed_session_wrappers(self):
        from ashlar_host.managed_a1_serverless_tasks import _ServerlessSessionFactory
        runtime = {"runtimeName": "databricks-serverless", "sparkVersion": "4.2.0",
            "sparkBuildText": "Spark build", "runtimeTag": "serverless-4.2.0",
            "principal": self.config.expected_principal, "catalog": self.config.catalog,
            "schema": self.config.schema, "protocol": self.config.protocol_profile.identity}
        config = ManagedCurrentSparkServerlessReceiptConfig(
            self.config.catalog, self.config.schema, self.config.expected_principal,
            self.config.expected_table_owner, self.config.source_truss_version,
            self.config.protocol_profile, runtime,
            carrier_profile=self.config.carrier_profile)
        selection = ManagedOperationalRetentionSelection(
            reader_config=config, request_bytes=self.selection.request_bytes,
            manifest_bytes=self.selection.manifest_bytes, receipt_bytes=self.selection.receipt_bytes,
            expected_receipt=self.selection.expected_receipt, vector=self.selection.vector,
            expected_source_custody=dict(self.selection.expected_source_custody),
            source_prefix_bytes=self.selection.source_prefix_bytes,
            source_prefix_sha256=self.selection.source_prefix_sha256,
            source_core_config=self.selection.source_core_config)
        spark = _SparkConnect(self.manifest, self.receipt, self.uuids)
        spark.request = self.request
        spark.attempt_rows = _attempt_rows(self.request, self.manifest)
        spark.owner = self.config.expected_principal
        spark.catalog, spark.schema = self.config.catalog, self.config.schema
        class PinObserver:
            def prepare(self, **_): pass
        factory = _ServerlessSessionFactory(spark, PinObserver(), selection,
            selection.vector, task_scope_id="one-task", deadline_monotonic=time.monotonic() + 30)
        interval = OperationalRetentionInterval.open(
            selection, factory, self.source_observer, self.policy_observer)
        interval.renew_pre_ack()
        result = interval.close()
        self.assertEqual([item.session_ownership_profile for item in result.observations],
            ["databricks-serverless-provided-task-session/0.1"] * 3)
        self.assertEqual({item.task_session_scope_id for item in result.observations},
                         {"one-task"})
        self.assertEqual(len({item.session_wrapper_id for item in result.observations}), 3)

        classic_factory = self.session_factory
        rejected = OperationalRetentionInterval(
            selection, classic_factory, self.source_observer, self.policy_observer)
        with self.assertRaisesRegex(ValueError, "borrowed wrapper"):
            rejected._capture("open")

        from ashlar_host.managed_current_spark_receipt_read import BorrowedServerlessTaskSession
        class DriftingScopeFactory:
            def open(self, *, stage, deadline_monotonic):
                return BorrowedServerlessTaskSession(spark, stage=stage,
                    task_session_scope_id=stage,
                    deadline_monotonic=min(deadline_monotonic, time.monotonic() + 20))
        drifting = OperationalRetentionInterval(
            selection, DriftingScopeFactory(), self.source_observer, self.policy_observer)
        drifting._capture("open")
        with self.assertRaisesRegex(ValueError, "task-scoped serverless borrowed wrapper"):
            drifting.renew_pre_ack()
        self.assertFalse(hasattr(interval, "acknowledge"))

    def test_changed_policy_at_pre_ack_invalidates_interval(self):
        with self.assertRaisesRegex(OperationalRetentionIntervalError, "matching frozen prefix bytes"):
            ManagedOperationalRetentionSelection(
                reader_config=self.config,
                request_bytes=_canonical(self.request).encode(),
                manifest_bytes=_canonical(self.manifest).encode(),
                receipt_bytes=_canonical(self.receipt).encode(),
                expected_receipt=self.receipt, vector=self.selection.vector,
                expected_source_custody=dict(self.custody))

        class DriftingPolicy:
            def observe(inner_self, *, stage, session, vector):
                value = self._policy()
                if stage == "pre_ack":
                    value["maintenance_inventory_sha256"] = "c" * 64
                return value

        interval = OperationalRetentionInterval.open(
            self.selection, self.session_factory, self.source_observer, DriftingPolicy())
        with self.assertRaisesRegex(OperationalRetentionIntervalError, "inventory changed"):
            interval.renew_pre_ack()
        self.assertTrue(all(item.closed for item in self.sessions))
        with self.assertRaisesRegex(OperationalRetentionIntervalError, "incomplete"):
            interval.close()
        self.assertFalse(hasattr(interval, "acknowledge"))

    def test_port_binds_exact_receipt_vector_and_context_across_three_stages(self):
        context = object()
        port = ManagedOperationalReadPort(
            self.selection, self.session_factory, self.source_observer,
            self.policy_observer, context=context)
        handle = port.open(self.selection.receipt_bytes, self.selection.vector, context)
        self.assertEqual([item.stage for item in self.sessions], ["open"])
        self.assertGreater(handle.remaining_lifetime_seconds(), 0)
        handle.renew()
        receipt = handle.close()
        self.assertEqual(receipt.outcome, "observed_available_at_checks")
        self.assertEqual(receipt.authority, "read_observation_only_no_source_ack")
        self.assertEqual([item.stage for item in self.sessions], ["open", "pre_ack", "close"])
        self.assertTrue(all(item.closed for item in self.sessions))
        self.assertEqual([sum("LIMIT 1025" in sql for sql in item.spark.calls)
                          for item in self.sessions], [8, 8, 8])

    def test_pin_composition_holds_exact_vector_across_open_renew_and_close(self):
        context = object()
        events = self.events
        vector = self.selection.vector

        class Executor:
            @contextmanager
            def transaction(inner_self, supplied):
                self.assertIs(supplied, context)
                events.append(("pin-transaction", "open"))
                try:
                    yield inner_self
                finally:
                    events.append(("pin-transaction", "close"))

            def query(inner_self, sql, params):
                from ashlar.pins import LIVE_PIN_TIMEOUT_SQL,LIVE_PIN_LIFETIME_SQL
                if sql == LIVE_PIN_TIMEOUT_SQL:
                    events.append(("pin-idle-timeout",))
                    return SQLResult([{"applied":params["timeout"]}])
                if sql == LIVE_PIN_LIFETIME_SQL:
                    events.append(("pin-lifetime",))
                    return SQLResult([{"idle_timeout_seconds":900.0,
                        "transaction_timeout_seconds":900.0,
                        "transaction_age_seconds":0.0}])
                if "assert_uuid_available(" in sql or "assert_active(" in sql:
                    return SQLResult([])
                if sql.startswith("SELECT table_name"):
                    return SQLResult([
                        {"table_name": table, "table_uuid": uuid,
                         "version": str(version), "digest": vector.custody_digest,
                         "released": False}
                        for table, (uuid, version) in vector.targets.items()
                    ])
                raise AssertionError("Unexpected native pin query")

        class PinPolicy:
            def authorize_read(inner_self, actual, supplied):
                self.assertIs(actual, vector)
                self.assertIs(supplied, context)
                events.append(("pin-read-authority",))

            def authorize_source_bound_read(inner_self, actual, selection, supplied):
                self.assertIs(actual, vector)
                self.assertIs(selection, self.selection)
                self.assertIs(supplied, context)
                events.append(("pin-source-authority",))

        read_port = ManagedOperationalReadPort(
            self.selection, self.session_factory, self.source_observer,
            self.policy_observer, context=context)
        port = ManagedOperationalReadPinPort(
            read_port, PostgresPins(Executor(), PinPolicy()), context=context,
            native_pin_transaction_timeout_seconds=900)
        handle = port.open(self.selection.receipt_bytes, vector, context)
        self.assertLess(events.index(("pin-transaction", "open")),
                        events.index(("session-open", "open")))
        self.assertGreater(handle.native_pin_remaining_lifetime_seconds(),30)
        handle.renew()
        self.assertGreater(handle.native_pin_remaining_lifetime_seconds(),30)
        self.assertEqual([item.stage for item in self.sessions], ["open", "pre_ack"])
        receipt = handle.close()
        self.assertEqual(receipt.outcome, "observed_available_at_checks")
        self.assertEqual(receipt.authority, "read_observation_only_no_source_ack")
        self.assertEqual([item.stage for item in self.sessions], ["open", "pre_ack", "close"])
        self.assertLess(events.index(("session-close", "close")),
                        len(events) - 1 - events[::-1].index(("pin-source-authority",)))
        self.assertEqual(events[-1], ("pin-transaction", "close"))
        self.assertFalse(hasattr(handle, "acknowledge"))

    def test_pin_composition_requires_source_bound_policy_before_open(self):
        context = object()
        read_port = ManagedOperationalReadPort(
            self.selection, self.session_factory, self.source_observer,
            self.policy_observer, context=context)

        class PolicyWithoutSourceAuthority:
            def authorize_read(self, vector, supplied):
                return None

        with self.assertRaisesRegex(ManagedOperationalReadPortError,
                                    "source-bound policy"):
            ManagedOperationalReadPinPort(
                read_port, PostgresPins(object(), PolicyWithoutSourceAuthority()),
                context=context)
        self.assertEqual(self.sessions, [])

    def test_port_refuses_changed_receipt_vector_context_and_reuse_before_sessions(self):
        context = object()
        port = ManagedOperationalReadPort(
            self.selection, self.session_factory, self.source_observer,
            self.policy_observer, context=context)
        with self.assertRaisesRegex(ManagedOperationalReadPortError, "Exact selected"):
            port.open(self.selection.receipt_bytes + b" ", self.selection.vector, context)
        with self.assertRaisesRegex(ManagedOperationalReadPortError, "Exact selected"):
            port.open(self.selection.receipt_bytes, self.selection.vector, object())
        self.assertEqual(self.sessions, [])
        handle = port.open(self.selection.receipt_bytes, self.selection.vector, context)
        with self.assertRaisesRegex(ManagedOperationalReadPortError, "Exact selected"):
            port.open(self.selection.receipt_bytes, self.selection.vector, context)
        self.assertEqual([item.stage for item in self.sessions], ["open"])
        handle.renew()
        handle.close()

    def test_port_refuses_a_second_pre_ack_renewal_and_attempts_close_observation(self):
        context = object()
        port = ManagedOperationalReadPort(
            self.selection, self.session_factory, self.source_observer,
            self.policy_observer, context=context)
        handle = port.open(self.selection.receipt_bytes, self.selection.vector, context)
        handle.renew()
        with self.assertRaisesRegex(ManagedOperationalReadPortError, "pre-ACK observation refused"):
            handle.renew()
        self.assertEqual([item.stage for item in self.sessions], ["open", "pre_ack"])
        with self.assertRaisesRegex(ManagedOperationalReadPortError, "closing observation incomplete"):
            handle.close()
        self.assertEqual([item.stage for item in self.sessions], ["open", "pre_ack", "close"])
        self.assertTrue(all(item.closed for item in self.sessions))

    def test_failed_pre_ack_source_observation_still_attempts_fresh_closing_read(self):
        class DriftAtPreAck:
            def observe(inner_self, *, stage, selection, vector):
                value = dict(self.custody)
                if stage == "pre_ack":
                    value["checkpoint"] = "c" * 64
                self.events.append(("source", stage))
                return value

        context = object()
        port = ManagedOperationalReadPort(
            self.selection, self.session_factory, DriftAtPreAck(),
            self.policy_observer, context=context)
        handle = port.open(self.selection.receipt_bytes, self.selection.vector, context)
        with self.assertRaisesRegex(ManagedOperationalReadPortError, "pre-ACK observation refused"):
            handle.renew()
        with self.assertRaisesRegex(ManagedOperationalReadPortError, "closing observation incomplete"):
            handle.close()
        self.assertEqual([item.stage for item in self.sessions], ["open", "pre_ack", "close"])
        self.assertTrue(all(item.closed for item in self.sessions))
        self.assertEqual([event for event in self.events if event[0] == "source"],
                         [("source", "open"), ("source", "pre_ack"), ("source", "close")])

    def test_expired_interval_attempts_closing_read_but_never_reports_success(self):
        now = [0.0]
        interval = OperationalRetentionInterval.open(
            self.selection, self.session_factory, self.source_observer,
            self.policy_observer, clock=lambda: now[0])
        interval.renew_pre_ack()
        now[0] = 901.0
        with self.assertRaisesRegex(OperationalRetentionIntervalError, "incomplete"):
            interval.close()
        self.assertEqual([item.stage for item in self.sessions], ["open", "pre_ack", "close"])
        self.assertTrue(all(item.closed for item in self.sessions))
        self.assertEqual(len(interval.observations), 3)
        self.assertGreater(interval.observations[-1].elapsed_seconds, 900)


class ManagedA1ServerlessSessionTests(unittest.TestCase):
    def test_serverless_borrowed_factory_reuses_injected_session_without_stopping_it(self):
        import time
        from ashlar_host.managed_a1_serverless_tasks import _ServerlessSessionFactory
        from ashlar_host.managed_current_spark_receipt_read import BorrowedServerlessTaskSession

        class Frame:
            def collect(self): return [{"ok": 1}]
        class ConnectParent:
            def __init__(self):
                self.tags = threading.local()
                self.query_tags = []
                self.stop_called = False
            def _tags(self):
                if not hasattr(self.tags, "value"): self.tags.value = {"caller"}
                return self.tags.value
            def addTag(self, tag): self._tags().add(tag)
            def getTags(self): return set(self._tags())
            def removeTag(self, tag): self._tags().discard(tag)
            def interruptTag(self, tag): return []
            def sql(self, text):
                self.query_tags.append(set(self._tags()))
                return Frame()

        parent = ConnectParent()
        factory = _ServerlessSessionFactory(parent, object(), object(), object(),
            task_scope_id="scope", deadline_monotonic=time.monotonic() + 5)
        owned = factory.open(stage="close", deadline_monotonic=time.monotonic() + 5)
        self.assertIs(type(owned), BorrowedServerlessTaskSession)
        self.assertIs(owned.spark, parent)
        self.assertEqual(owned.query("SELECT 1"), [{"ok": 1}])
        self.assertTrue(any(any(tag.startswith("ashlar-managed-current-close-")
                                for tag in tags) for tags in parent.query_tags))
        owned.close()
        self.assertFalse(parent.stop_called)
        self.assertEqual(parent.getTags(), {"caller"})


if __name__ == "__main__":
    unittest.main()
