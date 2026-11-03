# SPDX-License-Identifier: MIT OR Apache-2.0
"""Task-side implementation for the bounded three-run A1 trial.

Notebook entrypoints are deliberately thin. This module consumes their one
injected Spark session and digest-bound Volume artifacts; it does not start
jobs, connect to Truss, or ACK a source page.
"""
from __future__ import annotations

import base64
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import time
import uuid


def _need(value, message):
    if not value:
        raise ValueError(message)


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def _field(value, name):
    if type(value) is dict and name in value:
        return value[name]
    if hasattr(value, name):
        return getattr(value, name)
    raise ValueError("Detached A/B genesis evidence omitted " + name)


def _model_content_bytes(value):
    content = None
    for name in ("content", "bytes", "content_base64"):
        if type(value) is dict and name in value:
            content = value[name]
            break
        if hasattr(value, name):
            content = getattr(value, name)
            break
    _need(content is not None, "Detached selected model content omitted")
    if type(content) is str:
        content = base64.b64decode(content, validate=True)
    _need(type(content) is bytes and content,
          "Original selected model content bytes required")
    return content


def _validate_trial(trial):
    _need(type(trial) is dict and trial.get("catalog") == "client_dev"
          and type(trial.get("schema")) is str
          and re.fullmatch(r"ashlar_a1_[A-Za-z0-9_]{1,120}", trial["schema"])
          and type(trial.get("expected_runtime")) is dict
          and trial["expected_runtime"].get("catalog") == "client_dev"
          and trial["expected_runtime"].get("schema") == trial["schema"]
          and trial["expected_runtime"].get("runtimeName") == "databricks-serverless"
          and trial["expected_runtime"].get("principal"),
          "Exact bounded serverless trial namespace and run-as identity required")
    for key in ("stream", "stage_id", "materialized_at"):
        _need(type(trial.get(key)) is str and trial[key] and "\0" not in trial[key],
              "Exact managed A1 " + key + " required")
    _need(re.fullmatch(r"0|[1-9][0-9]{0,18}", trial["materialized_at"])
          and int(trial["materialized_at"]) < 2**63,
          "Canonical bounded A1 materialization clock required")


def _load_volume(uri, digest, maximum=4 * 1024 * 1024):
    _need(type(uri) is str and uri.startswith("/Volumes/client_dev/ashlar_a1_")
          and not any(part in (".", "..") for part in uri.split("/"))
          and type(digest) is str and re.fullmatch(r"[0-9a-f]{64}", digest),
          "Exact private owned-volume artifact path and SHA-256 required")
    path = Path(uri)
    raw = path.read_bytes()
    _need(0 < len(raw) <= maximum and hashlib.sha256(raw).hexdigest() == digest,
          "Exact bounded Volume artifact bytes differ from their supplied digest")
    return raw


def _protocol_profile(value):
    from ashlar.protocol import ReaderProtocolProfile
    _need(type(value) is dict and set(value) == {
        "identity", "reader_versions", "writer_versions", "features"},
        "Exact admitted reader protocol profile required")
    return ReaderProtocolProfile(value["identity"], tuple(value["reader_versions"]),
        tuple(value["writer_versions"]), tuple(value["features"]))


def _environment(spark, protocol, expected, *, namespace=True, session=None):
    from .managed_current_publication import observe_managed_current_environment
    runner = None if session is None else session.run_bounded
    value = observe_managed_current_environment(spark, protocol, operation_runner=runner)
    timezone_sql = lambda: spark.sql("SELECT current_timezone() AS session_timezone").collect()
    timezone_rows = timezone_sql() if runner is None else runner(timezone_sql)
    timezone_value = (timezone_rows[0].asDict(recursive=True)
                      if len(timezone_rows) == 1 else None)
    _need(len(timezone_rows) == 1
          and type(timezone_value) is dict
          and timezone_value.get("session_timezone") in ("UTC", "Etc/UTC"),
          "Unique UTC Spark session timezone readback required")
    keys = ["runtimeName", "sparkVersion", "principal", "protocol"]
    if namespace:
        keys.extend(("catalog", "schema"))
    for key in keys:
        _need(value.get(key) == expected.get(key),
              "Observed serverless environment differs from the locked " + key)
    if expected.get("sparkBuildText") is not None:
        _need(value.get("sparkBuildText") == expected["sparkBuildText"],
              "Observed serverless Spark build differs from its lock")
    if expected.get("runtimeTag") is not None:
        _need(value.get("runtimeTag") == expected["runtimeTag"],
              "Observed serverless runtime tag differs from its lock")
    _need(value.get("runtimeName") == "databricks-serverless",
          "Dedicated serverless runtime required")
    return value


def _query_rows(spark, sql, *, action="collect", session=None):
    operation = lambda: getattr(spark.sql(sql), action)()
    return operation() if session is None else session.run_bounded(operation)


def _po_status(spark, table, *, session=None):
    rows = _query_rows(spark, "DESCRIBE TABLE EXTENDED " + table, session=session)
    found = []
    for row in rows:
        values = row.asDict(recursive=True)
        label = str(values.get("col_name", values.get("colName", ""))).strip().casefold()
        if label == "predictive optimization":
            found.append(str(values.get("data_type", values.get("dataType", ""))).strip().upper())
    _need(len(found) == 1 and found[0] in ("ENABLE", "ENABLED"),
          "Unity Catalog predictive optimization must be observed enabled")
    return "ENABLE"


def _retention_seconds(spark, table, *, session=None):
    from ashlar.retention import interval_microseconds
    values = {row["key"]: row["value"] for row in
              _query_rows(spark, "SHOW TBLPROPERTIES " + table, session=session)}
    data = values.get("delta.deletedFileRetentionDuration", "interval 7 days")
    log = values.get("delta.logRetentionDuration", "interval 30 days")
    return {"data_retention_seconds": interval_microseconds(data) // 1_000_000,
            "log_retention_seconds": interval_microseconds(log) // 1_000_000}


def _maintenance_digest(spark, catalog, schema, roles, *, session=None):
    rows = []
    for role in sorted(roles):
        table = f"`{catalog}`.`{schema}`.`{role}`"
        detail = _query_rows(spark, "DESCRIBE DETAIL " + table,
                             action="first", session=session)
        history = _query_rows(spark, "DESCRIBE HISTORY " + table + " LIMIT 8",
                              session=session)
        _need(detail is not None and len(history) <= 8,
              "Bounded native maintenance inventory required")
        rows.append({"role": role, "detail": detail.asDict(recursive=True),
                     "history": [item.asDict(recursive=True) for item in history]})
    return hashlib.sha256(_canonical(rows)).hexdigest()


class _GenesisOnlyManifestPolicy:
    """Narrow writer gate for exactly one fresh empty A/B genesis row."""
    def __init__(self, *, table, table_uuid, context, schedule_sha256,
                 progress_json, revisions, target, schema, principal):
        self.table, self.table_uuid, self.context = table, table_uuid, context
        self.schedule_sha256, self.progress_json = schedule_sha256, progress_json
        self.revisions, self.target = revisions, target
        self.schema, self.principal = schema, principal
        self.writer_active = False
        self.admitted_digest = None
        self.committed = False

    @contextmanager
    def writer(self, table, table_uuid, context):
        _need(not self.writer_active and not self.committed
              and (table, table_uuid, context) == (self.table, self.table_uuid, self.context),
              "Genesis manifest writer must be fresh, exact and one-use")
        self.writer_active = True
        try:
            yield None
        except BaseException:
            raise
        else:
            _need(self.admitted_digest is not None,
                  "Genesis writer closed without the exact admitted intent")
            self.committed = True
        finally:
            self.writer_active = False

    def admit(self, row, context):
        from ashlar.manifest import validate_manifest_row
        _need(self.writer_active and not self.committed and context is self.context,
              "Genesis manifest admission outside its one-use writer")
        validate_manifest_row(row)
        report = json.loads(row["validation_report_json"])
        progress = json.loads(row["source_progress_json"])
        revisions = json.loads(row["schema_revisions_json"])
        versions = json.loads(row["table_versions_json"])
        _need(row["profile_version"] == "ashlar-delta/0.4-xxhash64"
              and report.get("publication_plan_profile") ==
                  "ashlar-managed-current-genesis-manifest/0.1"
              and report.get("complete") is True
              and report.get("schedule_sha256") == self.schedule_sha256
              and report.get("source_page_accepted") is False
              and report.get("source_acknowledged") is False
              and progress == self.progress_json and revisions == self.revisions
              and set(versions) == {self.target.tables[role] for role in
                  ("object_current", "edge_current", "tombstone", "whole_source_history")}
              and all(version == 0 for version in versions.values()),
              "Manifest writer admits only the exact empty A/B genesis intent")
        digest = hashlib.sha256(_canonical(row)).hexdigest()
        _need(self.admitted_digest in (None, digest),
              "Genesis manifest bytes changed during the one-use writer scope")
        self.admitted_digest = digest
        return None


def initialize_genesis_task(spark, parameters):
    """Create the empty six-role target and durable no-page/no-ACK genesis."""
    from ashlar.manifest import DeltaManifestStore
    from .managed_current_core import CARRIER_PROFILE_XXHASH64, ROLES
    from .managed_current_genesis import ManagedCurrentGenesisManifestOwner
    from .managed_current_publication import ManagedCurrentTargetOwner
    from .r04_schedule import R04Schedule
    from .truss_detached_a1_genesis_codec import decode_detached_a1_genesis_snapshot
    from .managed_a1_detached_admission import admit_progress

    _need(type(parameters) is dict and set(parameters) == {
        "trial", "genesis_volume_uri", "genesis_sha256", "protocol_profile"},
        "Closed A1 initializer task parameters required")
    trial = parameters["trial"]
    _validate_trial(trial)
    protocol = _protocol_profile(parameters["protocol_profile"])
    expected = trial["expected_runtime"]
    opening = _environment(spark, protocol, expected, namespace=False)
    raw = _load_volume(parameters["genesis_volume_uri"], parameters["genesis_sha256"])
    detached = decode_detached_a1_genesis_snapshot(raw)
    schedule = R04Schedule(detached.schedule_bytes)
    progress = {source: admit_progress(source, detached.source_evidence(source))
                for source in ("A", "B")}

    spark.sql("USE CATALOG `client_dev`").collect()
    spark.sql("USE SCHEMA `" + trial["schema"] + "`").collect()
    opening = _environment(spark, protocol, expected)
    target = ManagedCurrentTargetOwner(spark, catalog="client_dev", schema=trial["schema"],
        protocol_profile=protocol, carrier_profile=CARRIER_PROFILE_XXHASH64)
    _need(not target._existing(), "Initializer requires a fresh empty owned schema")
    inventory = target.initialize()
    manifest_role = "publication_manifest"
    manifest_table = target.tables[manifest_role]
    manifest_uuid = inventory[manifest_role].uuid
    context = object()
    progress_json = {item.feed: item.document() for item in progress.values()}
    policy = _GenesisOnlyManifestPolicy(table=manifest_table, table_uuid=manifest_uuid,
        context=context, schedule_sha256=detached.schedule_sha256,
        progress_json=progress_json, revisions=detached.schema_revisions,
        target=target, schema=trial["schema"], principal=expected["principal"])
    store = DeltaManifestStore(target.transport, policy, manifest_table, manifest_uuid)
    owner = ManagedCurrentGenesisManifestOwner(target, store)
    recorded_at = str(time.time_ns() // 1000)
    args = {"schedule_bytes": detached.schedule_bytes,
        "source_progress": progress,
        "selected_model_evidence": {source: {
            "pin": _field(_field(detached.source_evidence(source), "model"), "pin"),
            "bytes": _model_content_bytes(
                _field(detached.source_evidence(source), "model"))}
            for source in ("A", "B")},
        "catalog_acceptance_revisions": {source: {
            "installation_id": progress[source].installation_id,
            "revision": _field(detached.source_evidence(source), "catalog_revision"),
            "report_bytes": base64.b64decode(_field(detached.source_evidence(source),
                "report_bytes_base64"), validate=True),
            "catalog_bytes": base64.b64decode(_field(detached.source_evidence(source),
                "view_bytes_base64"), validate=True)}
            for source in ("A", "B")},
        "schema_revisions": detached.schema_revisions,
        "recorded_at": recorded_at, "context": context}
    row = owner.initialize(**args)
    _need(policy.admitted_digest == hashlib.sha256(_canonical(row)).hexdigest(),
          "Genesis manifest did not pass its exact one-use writer gate")
    closing = _environment(spark, protocol, expected)
    _need(closing == opening, "Runtime or catalog context changed during initializer")
    owner_rows = spark.sql("SELECT table_name,table_owner FROM system.information_schema.tables "
        "WHERE table_catalog=:catalog AND table_schema=:schema",
        args={"catalog": "client_dev", "schema": trial["schema"]}).collect()
    ownership = {item["table_name"]: item["table_owner"] for item in owner_rows}
    _need(set(ownership) == set(ROLES)
          and all(owner_name == expected["principal"] for owner_name in ownership.values()),
          "Complete six-role native UC installation owner check required")
    po = {_po_status(spark, f"`client_dev`.`{trial['schema']}`.`{role}`")
          for role in ROLES}
    _need(po == {"ENABLE"}, "Predictive optimization must remain enabled on all six roles")
    exact_manifest = _canonical(row)
    result = {"profile": "ashlar-managed-a1-initializer-result/0.1",
        "runtimeIdentity": {"opening": opening, "closing": closing},
        "predictiveOptimization": "ENABLE",
        "genesisManifestBytesBase64": base64.b64encode(exact_manifest).decode("ascii"),
        "manifestSha256": hashlib.sha256(exact_manifest).hexdigest(),
        "installationInventory": {role: {"name": inventory[role].name,
            "uuid": inventory[role].uuid, "owner": ownership[role],
            "version": inventory[role].version} for role in ROLES}}
    return result


class VolumeSourceCustodyObserver:
    """Bounded task-to-parent RPC for same-session source-custody renewal."""
    def __init__(self, rpc_directory, *, timeout_seconds=90):
        self.directory = Path(rpc_directory)
        self.timeout_seconds = timeout_seconds

    def _rpc(self, *, kind, stage, selection, vector):
        from ashlar_host.managed_operational_retention_interval import SOURCE_CUSTODY_FIELDS
        _need(kind in ("source", "pin") and stage in ("open", "pre_ack", "close"),
              "Closed source/pin RPC stage required")
        _need(kind != "pin" or stage in ("open", "pre_ack"),
              "Pin RPC is only valid before opening and pre-ACK reads")
        _need(self.directory.is_dir() and self.directory.as_posix().startswith(
            "/Volumes/client_dev/ashlar_a1_"), "Private UC Volume RPC directory required")
        nonce = uuid.uuid4().hex
        request = {"profile": "ashlar-managed-a1-source-observer-rpc/0.1",
            "kind": kind, "stage": stage, "nonce": nonce,
            "expected": dict(selection.expected_source_custody),
            "vectorSha256": hashlib.sha256(_canonical({
                "authority": vector.authority, "scope_kind": vector.scope_kind,
                "scope_id": vector.scope_id, "custody_digest": vector.custody_digest,
                "targets": {k: list(v) for k, v in vector.targets.items()}})).hexdigest()}
        request_path = self.directory / ("request-" + nonce + ".json")
        response_path = self.directory / ("response-" + nonce + ".json")
        temp_path = self.directory / (".request-" + nonce + ".tmp")
        temp_path.write_bytes(_canonical(request))
        os.replace(temp_path, request_path)
        deadline = time.monotonic() + self.timeout_seconds
        while time.monotonic() < deadline:
            if response_path.exists():
                raw = response_path.read_bytes()
                response_path.unlink()
                value = json.loads(raw)
                _need(type(value) is dict and set(value) == {
                    "profile", "kind", "nonce", "stage", "custodyDigests"}
                    and value["profile"] == "ashlar-managed-a1-source-observer-rpc/0.1"
                    and value["kind"] == kind and value["nonce"] == nonce
                    and value["stage"] == stage
                    and type(value["custodyDigests"]) is dict
                    and set(value["custodyDigests"]) == set(SOURCE_CUSTODY_FIELDS)
                    and value["custodyDigests"] == request["expected"],
                    "Parent source/pin response differs from held exact custody")
                return value["custodyDigests"]
            time.sleep(0.2)
        raise TimeoutError("Source-holder custody RPC response timed out")

    def observe(self, *, stage, selection, vector):
        return self._rpc(kind="source", stage=stage, selection=selection, vector=vector)


class VolumeNativePinStageObserver(VolumeSourceCustodyObserver):
    """Open/renew the parent-owned PG pin before corresponding UC read stages."""
    def prepare(self, *, stage, selection, vector):
        self._rpc(kind="pin", stage=stage, selection=selection, vector=vector)


class _ServerlessSessionFactory:
    def __init__(self, spark, pin_observer, selection, vector, *, task_scope_id,
                 deadline_monotonic):
        self.spark, self.pin_observer = spark, pin_observer
        self.selection, self.vector = selection, vector
        self.task_scope_id, self.deadline_monotonic = task_scope_id, deadline_monotonic
    def open(self, *, stage, deadline_monotonic):
        _need(time.monotonic() < deadline_monotonic,
              "Managed read interval deadline expired before the task Spark wrapper")
        from .managed_current_spark_receipt_read import BorrowedServerlessTaskSession
        if stage in ("open", "pre_ack"):
            self.pin_observer.prepare(stage=stage, selection=self.selection, vector=self.vector)
        return BorrowedServerlessTaskSession(self.spark, stage=stage,
            task_session_scope_id=self.task_scope_id,
            deadline_monotonic=min(deadline_monotonic, self.deadline_monotonic))


class _ServerlessPolicyObserver:
    def __init__(self, catalog, schema, roles):
        self.catalog, self.schema, self.roles = catalog, schema, tuple(roles)
    def observe(self, *, stage, session, vector):
        table = lambda role: f"`{self.catalog}`.`{self.schema}`.`{role}`"
        settings = {role: _retention_seconds(session.spark, table(role), session=session)
                    for role in ("object_current", "edge_current", "tombstone",
                                 "whole_source_history")}
        _need(all(value["data_retention_seconds"] >= 604800
                  and value["log_retention_seconds"] >= 604800
                  for value in settings.values()),
              "Four data-role retention windows must each cover seven days")
        po = {_po_status(session.spark, table(role), session=session) for role in settings}
        _need(po == {"ENABLE"}, "Predictive optimization must remain enabled at every stage")
        return {"retention_settings": settings, "predictive_optimization": "ENABLE",
            "maintenance_inventory_sha256": _maintenance_digest(
                session.spark, self.catalog, self.schema, self.roles, session=session)}


def publisher_task(spark, parameters):
    """Execute one pre-admitted first-A plan and return its exact receipt bytes."""
    from ashlar.manifest import manifest_pin_vector
    from ashlar_host.truss_feed_sessions import encode_truss_destination_receipt
    from .managed_current_core import CARRIER_PROFILE_XXHASH64
    from .managed_current_trial import ManagedCurrentTrialConfig, run_managed_current_trial_page
    from .managed_a1_detached_admission import admit_detached_a1_plan

    required = {"trial", "protocol_profile", "phase", "genesis_volume_uri", "genesis_sha256",
        "genesis_manifest_volume_uri", "genesis_manifest_sha256",
        "a1_snapshot_volume_uri", "a1_snapshot_sha256"}
    _need(type(parameters) is dict and set(parameters) == required,
          "Closed managed A1 publisher inputs required")
    trial = parameters["trial"]
    _validate_trial(trial)
    phase = parameters["phase"]
    _need(phase in ("a0", "a1"), "Explicit A0 metadata or A1 semantic phase required")
    protocol = _protocol_profile(parameters["protocol_profile"])
    spark.sql("USE CATALOG `" + trial["catalog"] + "`").collect()
    spark.sql("USE SCHEMA `" + trial["schema"] + "`").collect()
    opening = _environment(spark, protocol, trial["expected_runtime"])
    genesis = _load_volume(parameters["genesis_volume_uri"], parameters["genesis_sha256"])
    initial_raw = _load_volume(parameters["genesis_manifest_volume_uri"],
                               parameters["genesis_manifest_sha256"])
    _need(len(initial_raw) <= 4 * 1024 * 1024, "Bounded exact genesis manifest required")
    initial_manifest = json.loads(initial_raw)
    context = object()
    plan = admit_detached_a1_plan(genesis_snapshot_bytes=genesis,
        genesis_manifest=initial_manifest, stream=trial["stream"],
        materialized_at=trial["materialized_at"], context=context,
        continuation=phase == "a1")
    _need(len(plan.selection.admission.changes) == (0 if phase == "a0" else 7),
          "A0 must be metadata-only and A1 must contain the exact seven admitted graph changes")
    snapshot = _load_volume(parameters["a1_snapshot_volume_uri"],
                            parameters["a1_snapshot_sha256"])
    _need(len(snapshot) <= 4 * 1024 * 1024,
          "Exact detached A1 page and source-custody snapshot exceeds its durable bound")
    from .truss_detached_source_snapshot_codec import decode_detached_source_snapshot
    detached_a = decode_detached_source_snapshot(snapshot)
    held_page = detached_a.source_page
    _need(held_page.source == "A"
          and held_page.schedule_sha256 == plan.selection.schedule_sha256
          and held_page.request_bytes == _canonical(plan.request)
          and held_page.page_content == plan.delivery_page.page_bytes
          and held_page.prior_bytes == plan.delivery_page.prior_cursor_bytes
          and held_page.next_bytes == plan.delivery_page.next_cursor_bytes
          and held_page.publication_id == plan.request["publication_id"],
          "Opened A source-holder custody does not match the exact frozen first-A plan")
    revisions = json.dumps(plan.selection.schema_revisions, ensure_ascii=False,
                           sort_keys=True, separators=(",", ":"))
    cost = trial["cost"]
    config = ManagedCurrentTrialConfig(
        catalog=trial["catalog"], schema=trial["schema"], stream=trial["stream"],
        predecessor=plan.predecessor.manifest["publication_id"],
        schedule_sha256=plan.selection.schedule_sha256,
        schedule_step=1, stage_id=trial["stage_id"], schema_revisions_json=revisions,
        materialized_at=trial["materialized_at"],
        expected_environment=trial["expected_runtime"],
        maximum_task_seconds=trial["publisher_timeout_seconds"],
        estimated_total_dbu=cost["estimated_total_dbu"],
        authorized_total_dbu=cost["authorized_total_dbu"],
        estimated_total_cost_usd=cost["estimated_total_usd"],
        reserve_dbu_for_cleanup=cost["cleanup_reserve_dbu"],
        reserve_usd_for_cleanup=cost["cleanup_reserve_usd"],
        cost_estimate_includes_startup_driver_and_cleanup=True,
        create_schema=False,
        preferred_budget_exception=cost["preferred_budget_exception"],
        retries=0, max_concurrent_runs=1, dedicated_serverless=True,
        predictive_optimization_disabled=False,
        carrier_profile=CARRIER_PROFILE_XXHASH64)
    result = run_managed_current_trial_page(spark, config=config,
        protocol_profile=protocol, a1_plan=plan)
    task = result["task"]
    receipt = task["receipt"]
    _need(receipt.get("profile") == "ashlar-managed-current-destination-receipt/0.1",
          "Existing managed-current publication task returned no exact destination receipt")
    table_roles = ("object_current", "edge_current", "tombstone", "whole_source_history")
    inventory = receipt["installationInventory"]
    uuids = {inventory[role]["name"]: inventory[role]["uuid"] for role in table_roles}
    vector = manifest_pin_vector(receipt["manifest"], uuids,
                                 authority="managed-current-spark:" +
                                    trial["expected_runtime"]["principal"])
    destination_receipt = encode_truss_destination_receipt(
        _canonical(plan.request), _canonical(receipt["manifest"]), vector)
    _need(_environment(spark, protocol, trial["expected_runtime"]) ==
          task["runtimeIdentity"]["closing"],
          "Publisher output runtime identity differs from fresh task closing observation")
    po = {_po_status(spark, f"`{trial['catalog']}`.`{trial['schema']}`.`{role}`")
          for role in ("object_current", "edge_current", "tombstone", "whole_source_history",
                       "publication_manifest", "publication_attempt_phase")}
    _need(po == {"ENABLE"}, "Predictive optimization must remain enabled for all six roles")
    return {"profile": "ashlar-managed-a1-publisher-result/0.1", "phase": phase,
        "runtimeIdentity": task["runtimeIdentity"],
        "predictiveOptimization": "ENABLE",
        "requestBytesBase64": base64.b64encode(_canonical(plan.request)).decode("ascii"),
        "manifestBytesBase64": base64.b64encode(_canonical(receipt["manifest"])).decode("ascii"),
        "destinationReceiptBytesBase64": base64.b64encode(destination_receipt).decode("ascii"),
        "receipt": receipt}


def verifier_task(spark, parameters):
    """Independently reopen the publication through three fresh UC sessions."""
    from .managed_current_core import CARRIER_PROFILE_XXHASH64, ROLES
    from .managed_current_trial import ManagedCurrentTrialConfig
    from .managed_current_spark_receipt_read import ManagedCurrentSparkServerlessReceiptConfig
    from .managed_current_spark_receipt_read import BorrowedServerlessTaskSession
    from .managed_operational_read_port import ManagedOperationalReadPort
    from .managed_current_a1_trial_plan import ManagedCurrentA1TrialPlan
    from .managed_a1_detached_admission import admit_detached_a1_plan

    required = {"trial", "protocol_profile", "phase", "post_ack_close", "genesis_volume_uri", "genesis_sha256",
        "genesis_manifest_volume_uri", "genesis_manifest_sha256",
        "a1_snapshot_volume_uri", "a1_snapshot_sha256", "publisher_result_volume_uri",
        "publisher_result_sha256", "prefix_volume_uri", "prefix_sha256",
        "selected_prefix_volume_uri", "selected_prefix_sha256", "source_rpc_directory"}
    _need(type(parameters) is dict and set(parameters) == required,
          "Closed managed A1 verifier inputs required")
    trial = parameters["trial"]
    _validate_trial(trial)
    phase = parameters["phase"]
    _need(phase in ("a0", "a1"), "Explicit A0 metadata or A1 semantic phase required")
    _need(type(parameters["post_ack_close"]) is bool,
          "Explicit verifier pre-ACK or post-ACK close-only mode required")
    protocol = _protocol_profile(parameters["protocol_profile"])
    task_scope_id = uuid.uuid4().hex
    task_deadline = time.monotonic() + trial["verifier_timeout_seconds"] - 5
    if task_deadline <= time.monotonic():
        raise ValueError("Verifier task has no bounded SQL observation budget")
    opening_session = BorrowedServerlessTaskSession(spark, stage="environment_open",
        task_session_scope_id=task_scope_id, deadline_monotonic=task_deadline)
    try:
        opening_session.query("USE CATALOG `" + trial["catalog"] + "`")
        opening_session.query("USE SCHEMA `" + trial["schema"] + "`")
        opening = _environment(spark, protocol, trial["expected_runtime"],
                               session=opening_session)
    finally:
        opening_session.close()
    genesis = _load_volume(parameters["genesis_volume_uri"], parameters["genesis_sha256"])
    initial_raw = _load_volume(parameters["genesis_manifest_volume_uri"],
                               parameters["genesis_manifest_sha256"])
    genesis_manifest = json.loads(initial_raw)
    context = object()
    plan = admit_detached_a1_plan(genesis_snapshot_bytes=genesis,
        genesis_manifest=genesis_manifest, stream=trial["stream"],
        materialized_at=trial["materialized_at"], context=context,
        continuation=phase == "a1")
    _need(len(plan.selection.admission.changes) == (0 if phase == "a0" else 7),
          "Verifier phase must match the actual metadata-only A0 or seven-change A1 page")
    _need(type(plan) is ManagedCurrentA1TrialPlan,
          "Exact reconstructed A1 plan required by independent verifier")
    publisher = json.loads(_load_volume(parameters["publisher_result_volume_uri"],
                                         parameters["publisher_result_sha256"]))
    _need(type(publisher) is dict and publisher.get("profile") ==
          "ashlar-managed-a1-publisher-result/0.1" and publisher.get("phase") == phase,
          "Exact closed publisher receipt result required")
    try:
        request_bytes = base64.b64decode(publisher["requestBytesBase64"], validate=True)
        manifest_bytes = base64.b64decode(publisher["manifestBytesBase64"], validate=True)
        destination_receipt = base64.b64decode(
            publisher["destinationReceiptBytesBase64"], validate=True)
    except (KeyError, ValueError, TypeError) as error:
        raise ValueError("Exact publisher request, manifest and destination receipt bytes required") from error
    _need(request_bytes == _canonical(plan.request)
          and json.loads(manifest_bytes) == publisher["receipt"]["manifest"],
          "Publisher bytes differ from independent frozen A1 plan and receipt")
    prefix = _load_volume(parameters["prefix_volume_uri"], parameters["prefix_sha256"])
    snapshot = _load_volume(parameters["a1_snapshot_volume_uri"],
                            parameters["a1_snapshot_sha256"])
    selected_prefix = json.loads(_load_volume(parameters["selected_prefix_volume_uri"],
                                               parameters["selected_prefix_sha256"]))
    if type(selected_prefix) is not dict:
        raise ValueError("Exact separately selected prefix anchors required")
    from .truss_current_catalog import installed_fresh_truss_runtime_pins
    reader_config = ManagedCurrentSparkServerlessReceiptConfig(
        catalog=trial["catalog"], schema=trial["schema"],
        expected_principal=trial["expected_runtime"]["principal"],
        expected_table_owner=trial["expected_runtime"]["principal"],
        source_truss_version=installed_fresh_truss_runtime_pins()["version"],
        protocol_profile=protocol,
        expected_runtime=trial["expected_runtime"],
        maximum_elapsed_seconds=300, maximum_statements=128,
        maximum_rows=1024, maximum_result_bytes=4 * 1024 * 1024,
        carrier_profile=CARRIER_PROFILE_XXHASH64)
    core_config = ManagedCurrentTrialConfig(
        catalog=trial["catalog"], schema=trial["schema"], stream=trial["stream"],
        predecessor=plan.predecessor.manifest["publication_id"],
        schedule_sha256=plan.selection.schedule_sha256, schedule_step=1,
        stage_id=trial["stage_id"],
        schema_revisions_json=json.dumps(plan.selection.schema_revisions,
            sort_keys=True, separators=(",", ":")),
        materialized_at=trial["materialized_at"],
        expected_environment=trial["expected_runtime"],
        maximum_task_seconds=trial["verifier_timeout_seconds"],
        estimated_total_dbu=trial["cost"]["estimated_total_dbu"],
        authorized_total_dbu=trial["cost"]["authorized_total_dbu"],
        estimated_total_cost_usd=trial["cost"]["estimated_total_usd"],
        reserve_dbu_for_cleanup=trial["cost"]["cleanup_reserve_dbu"],
        reserve_usd_for_cleanup=trial["cost"]["cleanup_reserve_usd"],
        cost_estimate_includes_startup_driver_and_cleanup=True,
        create_schema=False,
        preferred_budget_exception=trial["cost"]["preferred_budget_exception"],
        carrier_profile=CARRIER_PROFILE_XXHASH64).core_config()
    from .managed_current_a1_retention_builder import build_managed_current_a1_retention_bundle
    bundle = build_managed_current_a1_retention_bundle(
        plan=plan, reader_config=reader_config, request_bytes=request_bytes,
        manifest_bytes=manifest_bytes, task_receipt=publisher["receipt"],
        destination_receipt_bytes=destination_receipt,
        source_prefix_bytes=prefix,
        source_prefix_sha256=parameters["prefix_sha256"],
        selected_prefix=selected_prefix, source_snapshot_bytes=snapshot,
        source_custody_digests=trial["source_custody_digests"],
        source_core_config=core_config)
    selection = bundle.selection
    source_observer = VolumeSourceCustodyObserver(parameters["source_rpc_directory"])
    pin_observer = VolumeNativePinStageObserver(parameters["source_rpc_directory"])
    policy_observer = _ServerlessPolicyObserver(trial["catalog"], trial["schema"], ROLES)
    read_port = ManagedOperationalReadPort(selection,
        _ServerlessSessionFactory(spark, pin_observer, selection, bundle.vector,
            task_scope_id=task_scope_id, deadline_monotonic=task_deadline),
        source_observer, policy_observer,
        context=context)
    handle = read_port.open(bundle.destination_receipt_bytes, bundle.vector, context)
    handle.renew()
    interval = handle.close()
    closing_session = BorrowedServerlessTaskSession(spark, stage="environment_close",
        task_session_scope_id=task_scope_id, deadline_monotonic=task_deadline)
    try:
        closing = _environment(spark, protocol, trial["expected_runtime"],
                               session=closing_session)
        publication_po = _po_status(spark,
            f"`{trial['catalog']}`.`{trial['schema']}`.`publication_manifest`",
            session=closing_session)
    finally:
        closing_session.close()
    _need(closing == publisher["runtimeIdentity"]["closing"],
          "Independent verifier runtime differs from the publisher runtime tuple")
    _need(publication_po == "ENABLE",
        "Predictive optimization changed or is not enabled")
    observations = [{
        "stage": item.stage,
        "observed_at_utc": item.observed_at_utc,
        "elapsed_seconds": item.elapsed_seconds,
        "vector_targets": item.vector_targets,
        "runtime_identity": item.runtime_identity,
        "source_custody_digests": item.source_custody_digests,
        "session_ownership_profile": item.session_ownership_profile,
        "task_session_scope_id": item.task_session_scope_id,
        "session_wrapper_id": item.session_wrapper_id,
        "retention_settings": item.retention_settings,
        "predictive_optimization": item.predictive_optimization,
        "maintenance_inventory_sha256": item.maintenance_inventory_sha256,
        "row_count": item.row_count,
        "normalized_bytes": item.normalized_bytes,
        "role_comparisons": item.role_comparisons,
        "comparison_sha256": item.comparison_sha256,
    } for item in interval.observations]
    observation_value = {"profile": interval.profile, "interval_id": interval.interval_id,
        "outcome": interval.outcome, "authority": interval.authority,
        "maximum_elapsed_seconds": interval.maximum_elapsed_seconds,
        "observations": observations}
    observed_bytes = _canonical(observation_value)
    return {"profile": "ashlar-managed-a1-verifier-result/0.1", "phase": phase,
        "observationPhase": "post_ack_close" if parameters["post_ack_close"] else "pre_ack",
        "runtimeIdentity": {"opening": opening, "closing": closing},
        "predictiveOptimization": "ENABLE",
        "managedObservation": observation_value,
        "managedObservationSha256": hashlib.sha256(observed_bytes).hexdigest(),
        "requestSha256": hashlib.sha256(request_bytes).hexdigest(),
        "manifestSha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "destinationReceiptSha256": hashlib.sha256(destination_receipt).hexdigest(),
        "vector": {"authority": bundle.vector.authority,
            "scope_kind": bundle.vector.scope_kind,
            "scope_id": bundle.vector.scope_id,
            "custody_digest": bundle.vector.custody_digest,
            "targets": {key: list(value) for key, value in bundle.vector.targets.items()}}}
