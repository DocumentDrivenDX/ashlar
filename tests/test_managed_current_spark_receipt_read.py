# SPDX-License-Identifier: MIT OR Apache-2.0
"""Offline receipt-verifier tests; the fake Spark session performs no I/O."""
import copy
import hashlib
import json
import unittest
from datetime import datetime, timezone
import time
from unittest.mock import patch
from types import SimpleNamespace

from ashlar.native import SQLResult
from ashlar.apply import EntityKey
from ashlar.protocol import ReaderProtocolProfile
from ashlar_host.managed_current_carriers import load_current_managed_carriers
from ashlar_host.managed_current_core import CARRIER_PROFILE_XXHASH64, ROLES
from ashlar_host.managed_current_spark_receipt_read import (
    ManagedCurrentSparkReceiptConfig, ManagedCurrentSparkReceiptReader, _native_json,
)
from ashlar_host.managed_current_spark_source_oracle import (
    ManagedCurrentSourcePrefixOracle, build_managed_current_prefix_oracle,
    compare_complete_managed_current_bags, derive_managed_current_prefix_lookup_keys,
    _source_prefix_lookup_tokens,
)
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
        self.role_identity_drift = None
        self.hash_values = {}
        self.hash_rows_override = None

    def sql(self, sql, args=None):
        args = args or {}; self.calls.append(sql); self.parameters.append(dict(args))
        if "current_user() AS principal" in sql:
            rows, cols = [{"principal": self.owner, "catalog": "client_dev", "schema": "trial_schema"}], \
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
        self.assertEqual(observation.runtime_identity["closing"]["sessionTimeZone"], "UTC")
        self.assertEqual(self.reader._approved_data_queries, set())

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


if __name__ == "__main__":
    unittest.main()
