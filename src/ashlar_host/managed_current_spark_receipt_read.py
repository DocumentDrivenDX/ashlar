# SPDX-License-Identifier: MIT OR Apache-2.0
"""Offline, read-only Spark reopen of one managed-current task receipt.

This owner is deliberately separate from the SQL warehouse receipt profile.
It does not acquire a Spark session, provision compute, publish, or ACK.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
import queue
import re
import threading
import time
import uuid
from datetime import date, datetime

from ashlar.manifest import manifest_pin_vector
from ashlar.native import SQLResult, quote_table_identifier
from ashlar.publication import Snapshot, resolve_publication
from ashlar.protocol import ReaderProtocolProfile, validate_protocol_detail
from ashlar.schema import decode_retained_json
from .managed_current_carriers import load_current_managed_carriers
from .managed_current_core import (
    CARRIER_PROFILE_LEGACY, CARRIER_PROFILE_XXHASH64, ManagedCurrentCoreConfig, ROLES,
)
from .managed_current_spark_source_oracle import (
    ManagedCurrentSourcePrefixOracle, build_managed_current_prefix_oracle,
    compare_complete_managed_current_bags, derive_managed_current_prefix_lookup_keys,
)

PROFILE = "ashlar-managed-current-spark-receipt-reopen/0.1"
DATA_ROLES = ("object_current", "edge_current", "tombstone", "whole_source_history")
_IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}\Z")
_XXHASH_SQL = ("SELECT xxhash64(CAST(:source AS STRING),CAST(:type AS BIGINT),"
               "CAST(:id AS BIGINT)) AS lookup_hash")
_INFORMATION_SCHEMA_COLUMNS_SQL = (
    "SELECT table_name,column_name,UPPER(full_data_type) AS data_type,is_nullable,ordinal_position "
    "FROM system.information_schema.columns WHERE table_catalog=:catalog "
    "AND table_schema=:schema ORDER BY table_name,ordinal_position")
# Probe-only source token cap. Source identifiers have no smaller limit in the
# managed-current contract; 64 KiB is conservative and keeps each bound native
# parameter well below the 4 MiB frozen-prefix envelope.
_MAX_PROBE_SOURCE_BYTES = 65536
SERVERLESS_TASK_SESSION_OWNERSHIP = "databricks-serverless-provided-task-session/0.1"
CLASSIC_TASK_SESSION_OWNERSHIP = "databricks-classic-provided-task-session/0.1"


def _require(value, reason="Managed Spark receipt reopen refused"):
    if not value:
        raise ValueError(reason)


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


def _native_json(value):
    """Losslessly frame public native metadata values; reject implicit coercion."""
    def normalize(item):
        if item is None or type(item) in (str, bool, int, float):
            return item
        if isinstance(item, datetime):
            return item.isoformat(timespec="microseconds")
        if isinstance(item, date):
            return item.isoformat()
        if type(item) in (list, tuple):
            return [normalize(v) for v in item]
        if type(item) is dict:
            _require(all(type(k) is str for k in item), "Native metadata map keys must be text")
            return {k: normalize(v) for k, v in item.items()}
        raise ValueError("Unsupported native metadata representation")
    return _canonical(normalize(value))


def _spark_type(value):
    # Spark LongType.simpleString() is "bigint"; the resolver's public schema
    # vocabulary calls the same exact native type "long".
    return {"bigint": "long"}.get(value, value)


@dataclass(frozen=True)
class ManagedCurrentSparkReceiptConfig:
    catalog: str
    schema: str
    expected_principal: str
    expected_table_owner: str
    source_truss_version: str
    protocol_profile: ReaderProtocolProfile = field(repr=False, compare=False)
    expected_runtime: dict = field(repr=False, compare=False)
    maximum_elapsed_seconds: int = 300
    maximum_statements: int = 128
    maximum_rows: int = 1024
    maximum_result_bytes: int = 1048576
    carrier_profile: str = CARRIER_PROFILE_LEGACY
    execution_profile: str = "classic_jobs_cluster"
    operational_observation_stage: bool = False

    def __post_init__(self):
        _require(type(self.catalog) is str and _IDENT.fullmatch(self.catalog)
                 and type(self.schema) is str and _IDENT.fullmatch(self.schema)
                 and all(type(v) is str and v and "\0" not in v and len(v.encode()) <= 1024
                         for v in (self.expected_principal, self.expected_table_owner))
                 and self.source_truss_version in ("0.0.44", "0.0.45", "0.0.48", "0.0.49", "0.0.50", "0.0.52", "0.0.53", "0.0.54", "0.0.55")
                 and type(self.protocol_profile) is ReaderProtocolProfile
                 and type(self.expected_runtime) is dict
                 and self.expected_runtime.get("principal") == self.expected_principal
                 and self.expected_runtime.get("catalog") == self.catalog
                 and self.expected_runtime.get("schema") == self.schema
                 and self.expected_runtime.get("protocol") == self.protocol_profile.identity
                 and self.execution_profile in ("classic_jobs_cluster", "serverless_connect")
                 and type(self.operational_observation_stage) is bool
                 and (not self.operational_observation_stage
                      or self.execution_profile == "serverless_connect"
                      or type(self) is globals().get(
                          "ManagedCurrentSparkClassicTaskReceiptConfig"))
                 and self.expected_runtime.get("runtimeName") == (
                     "databricks-serverless" if self.execution_profile == "serverless_connect"
                     else "databricks-jobs-cluster")
                 and type(self.maximum_elapsed_seconds) is int
                 and 1 <= self.maximum_elapsed_seconds <= 600
                 and type(self.maximum_statements) is int and 16 <= self.maximum_statements <= 128
                 and type(self.maximum_rows) is int and 16 <= self.maximum_rows <= 4096
                 and type(self.maximum_result_bytes) is int
                 and 4096 <= self.maximum_result_bytes <= 4194304
                 and self.carrier_profile in (CARRIER_PROFILE_LEGACY, CARRIER_PROFILE_XXHASH64),
                 "Explicit Spark runtime, UC namespace, owner, source, protocol, and finite limits required")


@dataclass(frozen=True)
class ManagedCurrentSparkServerlessReceiptConfig(ManagedCurrentSparkReceiptConfig):
    """Explicit Spark Connect/serverless profile; never requires SparkContext IDs."""
    execution_profile: str = field(default="serverless_connect", init=False)


@dataclass(frozen=True)
class ManagedCurrentSparkClassicTaskReceiptConfig(ManagedCurrentSparkReceiptConfig):
    """Explicit borrowed Spark task wrapper on a locked classic Jobs cluster."""
    operational_observation_stage: bool = True

    def __post_init__(self):
        super().__post_init__()
        _require(self.execution_profile == "classic_jobs_cluster"
                 and self.expected_runtime.get("runtimeName") == "databricks-jobs-cluster"
                 and type(self.expected_runtime.get("clusterId")) is str
                 and self.expected_runtime["clusterId"]
                 and self.expected_runtime.get("sessionTimeZone") in ("UTC", "Etc/UTC")
                 and "sparkApplicationId" not in self.expected_runtime,
                 "Exact borrowed classic Jobs runtime/cluster selection required")


class _ServerlessSparkTagCancellation:
    """Own one exact SparkSession tag and its bounded interrupt callback."""

    def __init__(self, spark):
        self.spark = spark
        required = ("addTag", "getTags", "removeTag", "interruptTag")
        if any(not callable(getattr(spark, name, None)) for name in required):
            raise ValueError("Spark Connect tag, removal and interrupt APIs are required")
        existing = spark.getTags()
        if type(existing) is not set or any(type(tag) is not str for tag in existing):
            raise ValueError("Spark Connect getTags must return an exact set of text tags")
        self.preexisting_tags = frozenset(existing)
        self.tag = "ashlar-managed-current-close-" + uuid.uuid4().hex
        if self.tag in self.preexisting_tags:
            raise ValueError("Unique Spark Connect close tag required")
        self.interrupt_called = False
        self.interrupt_ids = None
        self.interrupt_error = None
        self.closed = False
        spark.addTag(self.tag)
        current = spark.getTags()
        if type(current) is not set or self.tag not in current:
            try:
                spark.removeTag(self.tag)
            finally:
                raise ValueError("Spark Connect did not retain the exact operation tag")

    def interrupt_tagged_operations(self):
        self.interrupt_called = True
        try:
            result = self.spark.interruptTag(self.tag)
            if type(result) is not list or any(type(item) is not str or not item for item in result):
                raise ValueError("Spark Connect interruptTag must return a list of operation IDs")
            self.interrupt_ids = tuple(result)
        except BaseException as error:
            self.interrupt_error = error

    def run_tagged_thread(self, operation):
        """Bind the owned tag to a worker thread (Spark tags are thread-local)."""
        before = self.spark.getTags()
        if type(before) is not set or any(type(tag) is not str for tag in before):
            raise ValueError("Spark Connect worker tags must be an exact text set")
        if self.tag in before:
            raise ValueError("Owned Spark Connect tag unexpectedly exists on worker thread")
        self.spark.addTag(self.tag)
        primary = None
        try:
            current = self.spark.getTags()
            if type(current) is not set or current != before | {self.tag}:
                raise ValueError("Spark Connect worker did not bind the exact owned operation tag")
            return operation()
        except BaseException as error:
            primary = error
            raise
        finally:
            try:
                self.spark.removeTag(self.tag)
                restored = self.spark.getTags()
                if type(restored) is not set or restored != before:
                    raise ValueError("Spark Connect worker tag cleanup did not restore caller tags")
            except BaseException as cleanup_error:
                if primary is None:
                    raise
                if callable(getattr(primary, "add_note", None)):
                    primary.add_note("Worker tag cleanup failed: " + type(cleanup_error).__name__)

    def close(self):
        if self.closed:
            raise ValueError("Spark Connect operation tag already closed")
        primary = None
        try:
            self.spark.removeTag(self.tag)
            remaining = self.spark.getTags()
            if (type(remaining) is not set or self.tag in remaining
                    or frozenset(remaining) != self.preexisting_tags):
                raise ValueError("Spark Connect close tag did not restore the original tag set")
        except BaseException as error:
            primary = error
        self.closed = True
        if primary is not None:
            raise primary


class SparkConnectOperationTimeout(ValueError):
    """A tagged Spark Connect operation timed out; completion remains unknown."""


def run_spark_connect_tagged_operation(spark, operation, *, deadline_monotonic):
    """Run one Spark Connect action on a worker with its own thread-local tag.

    On timeout, request ``interruptTag`` asynchronously and refuse to claim the
    server operation terminated. The task/job deadline remains the outer bound.
    """
    if (spark is None or not callable(operation)
            or type(deadline_monotonic) not in (int, float)
            or deadline_monotonic <= time.monotonic()):
        raise ValueError("Live bounded Spark Connect operation and deadline required")
    tag_owner = _ServerlessSparkTagCancellation(spark)
    result = queue.Queue(maxsize=1)

    def invoke():
        try:
            result.put((True, tag_owner.run_tagged_thread(operation)))
        except BaseException as error:
            result.put((False, error))

    thread = threading.Thread(target=invoke, name="ashlar-spark-connect-owned-query", daemon=True)
    primary = None
    value = None
    try:
        thread.start()
        try:
            succeeded, value = result.get(timeout=max(0, deadline_monotonic - time.monotonic()))
            if not succeeded:
                primary = value
        except queue.Empty:
            primary = SparkConnectOperationTimeout(
                "Spark Connect query exceeded its task deadline; tagged interrupt requested, "
                "operation completion remains unknown")
            interrupt = threading.Thread(target=tag_owner.interrupt_tagged_operations,
                name="ashlar-spark-connect-owned-interrupt", daemon=True)
            interrupt.start()
    except BaseException as error:
        primary = error
    try:
        tag_owner.close()
    except BaseException as cleanup_error:
        if primary is None:
            primary = cleanup_error
        elif callable(getattr(primary, "add_note", None)):
            primary.add_note("Spark Connect owned-tag cleanup also failed: "
                             + type(cleanup_error).__name__)
    if primary is not None:
        raise primary
    return value


class BorrowedServerlessTaskSession:
    """A per-observation wrapper around the task-injected parent SparkSession.

    ``close`` releases this wrapper only. The Databricks notebook task owns the
    supplied Spark session; this adapter never calls ``newSession`` or ``stop``.
    The opaque task-scope value is a local correlation ID, not a provider session
    identifier.
    """

    ownership_profile = SERVERLESS_TASK_SESSION_OWNERSHIP

    def __init__(self, spark, *, stage, task_session_scope_id, deadline_monotonic):
        if (spark is None or type(stage) is not str or not stage
                or type(task_session_scope_id) is not str or not task_session_scope_id
                or type(deadline_monotonic) not in (int, float)
                or deadline_monotonic <= time.monotonic()):
            raise ValueError("Exact borrowed serverless task Spark session and deadline required")
        required = ("addTag", "getTags", "removeTag", "interruptTag", "sql")
        if any(not callable(getattr(spark, name, None)) for name in required):
            raise ValueError("Provided serverless Spark session lacks the required tag/query APIs")
        self.spark = spark
        self.stage = stage
        self.session_id = "ashlar-observation-wrapper-" + uuid.uuid4().hex
        self.task_session_scope_id = task_session_scope_id
        self.deadline_monotonic = deadline_monotonic
        self.closed = False
        self.completion_unknown = False

    def run_bounded(self, operation):
        if self.closed or self.completion_unknown:
            raise ValueError("Borrowed serverless task session is closed or has unknown query completion")
        try:
            return run_spark_connect_tagged_operation(
                self.spark, operation, deadline_monotonic=self.deadline_monotonic)
        except SparkConnectOperationTimeout:
            self.completion_unknown = True
            raise

    def query(self, sql, *, action="collect"):
        if type(sql) is not str or not sql or action not in ("collect", "first"):
            raise ValueError("Bounded serverless SQL text and supported result action required")
        return self.run_bounded(lambda: getattr(self.spark.sql(sql), action)())

    def close(self):
        if self.closed:
            raise ValueError("Borrowed serverless task-session wrapper already closed")
        # Do not stop or replace the task-injected SparkSession. A timeout
        # remains an unknown remote operation even after local wrapper cleanup.
        self.closed = True


class BorrowedClassicTaskSession:
    """Task-local bounded wrapper around the injected classic Jobs Spark session.

    It uses a thread-local Spark job group for each operation, restores the
    worker's prior local properties, and never stops the cluster-owned context.
    """
    ownership_profile = CLASSIC_TASK_SESSION_OWNERSHIP

    def __init__(self, spark, *, stage, task_session_scope_id, deadline_monotonic):
        sc = getattr(spark, "sparkContext", None)
        required = ("getLocalProperty", "setLocalProperty", "setJobGroup", "cancelJobGroup")
        if (sc is None or any(not callable(getattr(sc, name, None)) for name in required)
                or type(stage) is not str or not stage
                or type(task_session_scope_id) is not str or not task_session_scope_id
                or type(deadline_monotonic) not in (int, float)
                or deadline_monotonic <= time.monotonic()):
            raise ValueError("Exact borrowed classic Jobs Spark session and deadline required")
        self.spark, self.spark_context = spark, sc
        self.stage = stage
        self.session_id = "ashlar-classic-observation-wrapper-" + uuid.uuid4().hex
        self.task_session_scope_id = task_session_scope_id
        self.deadline_monotonic = float(deadline_monotonic)
        self.closed = False
        self.completion_unknown = False

    def run_bounded(self, operation):
        if self.closed or self.completion_unknown:
            raise ValueError("Borrowed classic task session is closed or query completion is unknown")
        remaining = self.deadline_monotonic - time.monotonic()
        if remaining <= 0:
            raise ValueError("Classic task operation deadline expired")
        group = "ashlar-classic-task-" + uuid.uuid4().hex
        keys = ("spark.jobGroup.id", "spark.job.description", "spark.job.interruptOnCancel")
        result = queue.Queue(maxsize=1)

        def invoke():
            previous = {}
            outcome = None
            try:
                for key in keys:
                    previous[key] = self.spark_context.getLocalProperty(key)
                self.spark_context.setJobGroup(group, "bounded Ashlar classic task SQL",
                                               interruptOnCancel=True)
                try:
                    outcome = (True, operation())
                except BaseException as error:
                    outcome = (False, error)
            except BaseException as error:
                outcome = (False, error)
            finally:
                restore_errors = []
                for key, value in previous.items():
                    try:
                        self.spark_context.setLocalProperty(key, value)
                    except BaseException as error:
                        restore_errors.append(error)
                if restore_errors:
                    outcome = (False, ValueError(
                        "Classic task Spark job-group restoration failed; task state is unknown"))
                if outcome is None:
                    outcome = (False, ValueError("Classic task SQL operation did not complete"))
                result.put(outcome)

        thread = threading.Thread(target=invoke, name="ashlar-classic-task-sql", daemon=True)
        thread.start()
        try:
            succeeded, value = result.get(timeout=remaining)
        except queue.Empty as error:
            self.completion_unknown = True
            interrupter = threading.Thread(target=self.spark_context.cancelJobGroup,
                args=(group,), name="ashlar-classic-task-cancel", daemon=True)
            interrupter.start()
            raise ValueError("Classic Spark job group cancellation requested; completion remains unknown") from error
        if not succeeded:
            raise value
        return value

    def query(self, sql, *, action="collect"):
        if type(sql) is not str or not sql or action not in ("collect", "first"):
            raise ValueError("Bounded classic SQL and supported result action required")
        return self.run_bounded(lambda: getattr(self.spark.sql(sql), action)())

    def close(self):
        if self.closed:
            raise ValueError("Borrowed classic task-session wrapper already closed")
        self.closed = True


@dataclass(frozen=True)
class ManagedCurrentSparkReceiptReopenObservation:
    """Offline reopen result. It intentionally has no source-ACK port."""
    profile: str
    publication_id: str
    manifest: dict = field(repr=False)
    manifest_sha256: str
    source_receipt_bytes: bytes = field(repr=False, compare=False)
    source_receipt_sha256: str
    vector: object = field(repr=False, compare=False)
    installation_inventory: dict = field(repr=False)
    closing_installation_inventory: dict = field(default_factory=dict, repr=False)
    runtime_identity: dict = field(default_factory=dict, repr=False)
    attempt_custody: object = field(default=None, repr=False, compare=False)
    source_prefix_sha256: str | None = None
    bag_comparison_sha256: str | None = None
    bag_comparison_summary: dict | None = field(default=None, repr=False)


@dataclass(frozen=True)
class ManagedCurrentSparkAttemptCustody:
    """Opaque retained text digests; this is not a native effect proof."""
    table_version: int
    committed_payload_sha256: str
    plan_sha256: str | None
    result_sha256: str
    descriptor_sha256: str


class _SparkReceiptExecutor:
    def __init__(self, owner):
        self.owner = owner

    def query(self, sql, parameters):
        owner = self.owner
        owner._check_deadline()
        _require(type(sql) is str)
        c = owner.config
        base = "`" + c.catalog + "`.`" + c.schema + "`."
        exact_selects = {
            "SELECT current_user() AS principal,current_catalog() AS catalog,current_schema() AS schema",
            "SELECT version() AS sparkBuildText",
            "SELECT current_timezone() AS session_timezone",
            "SELECT table_name,table_type,table_owner FROM system.information_schema.tables "
            "WHERE table_catalog=:catalog AND table_schema=:schema",
            _INFORMATION_SCHEMA_COLUMNS_SQL,
            "SELECT publication_id,profile_version,table_versions_json,source_progress_json,"
            "schema_revisions_json,validation_report_json,"
            "CAST(unix_micros(recorded_at) AS STRING) AS recorded_at FROM " + base
            + "`publication_manifest` WHERE publication_id=:publication_id",
        }
        attempt_query = None
        data_queries = owner._approved_data_queries
        hash_parameters = owner._approved_hash_parameters
        if owner.opening is not None:
            version = owner.opening["publication_attempt_phase"]["version"]
            attempt_query = ("SELECT phase,request_digest,payload_json,payload_digest FROM "
                + base + "`publication_attempt_phase` VERSION AS OF " + str(version)
                + " WHERE stream=:stream AND batch_id=:batch ORDER BY CASE phase "
                + "WHEN 'prepared' THEN 0 WHEN 'applying' THEN 1 WHEN 'applied' THEN 2 "
                + "WHEN 'committing' THEN 3 WHEN 'committed' THEN 4 END LIMIT 161")
        if sql.startswith("SELECT * FROM ") and " VERSION AS OF " in sql:
            match = re.fullmatch(r"SELECT \* FROM (.+) VERSION AS OF ([0-9]{1,19}) LIMIT 0", sql)
            if match:
                selected = {base + "`" + role + "`": role for role in DATA_ROLES}
                role = selected.get(match.group(1))
                if role:
                    version = int(match.group(2))
                    exact_selects.add(sql) if version <= owner.opening[role]["version"] else None
        exact_describes = {"DESCRIBE DETAIL " + base + "`" + role + "`" for role in ROLES}
        exact_describes.update("DESCRIBE HISTORY " + base + "`" + role + "` LIMIT 1" for role in ROLES)
        is_attempt_query = sql == attempt_query
        is_data_query = sql in data_queries
        is_hash_query = sql == _XXHASH_SQL
        _require((sql in exact_selects or sql in exact_describes or is_attempt_query
                  or is_data_query or is_hash_query)
                 and type(parameters) is dict
                 and all(type(k) is str and type(v) is str for k, v in parameters.items()),
                 "Bound Spark SQL with string parameters required")
        if is_attempt_query:
            _require(set(parameters) == {"stream", "batch"},
                     "Exact source-qualified committed-attempt keys required")
        if is_data_query:
            _require(parameters == {}, "Pinned versioned role reads take no parameters")
        if is_hash_query:
            _require(set(parameters) == {"source", "type", "id"}
                     and tuple(parameters[key] for key in ("source", "type", "id"))
                     in hash_parameters,
                     "Only preapproved typed source-key hash probes are admitted")
            hash_parameters.remove(tuple(parameters[key] for key in ("source", "type", "id")))
        owner.statements += 1
        statement_limit = (owner.config.maximum_statements if owner.closing
                           else owner.config.maximum_statements - 24)
        _require(owner.statements <= statement_limit,
                 "Spark receipt statement budget exhausted")
        def execute():
            frame = owner.spark.sql(sql, args=dict(parameters))
            fields = tuple((f.name, _spark_type(f.dataType.simpleString()))
                           for f in frame.schema.fields)
            if is_attempt_query:
                _require(fields == (("phase", "string"), ("request_digest", "string"),
                                    ("payload_json", "string"), ("payload_digest", "string")),
                         "Exact native committed-attempt carrier required")
            if is_hash_query:
                _require(fields == (("lookup_hash", "long"),),
                         "Exact native signed BIGINT hash projection required")
            capture_cap = (161 if is_attempt_query else 1025 if is_data_query
                           else owner.config.maximum_rows + 1)
            return fields, frame.limit(capture_cap).collect()

        if owner.config.execution_profile == "serverless_connect":
            fields, rows = owner._run_serverless_bounded(execute)
        elif (type(owner.config) is ManagedCurrentSparkClassicTaskReceiptConfig
              and owner._operation_runner is not None):
            runner_owner = getattr(owner._operation_runner, "__self__", None)
            previous_deadline = getattr(runner_owner, "deadline_monotonic", None)
            close_deadline = owner._classic_close_deadline if owner.closing else None
            if close_deadline is not None and previous_deadline is not None:
                runner_owner.deadline_monotonic = min(previous_deadline, close_deadline)
            try:
                fields, rows = owner._operation_runner(execute)
            finally:
                if close_deadline is not None and previous_deadline is not None:
                    runner_owner.deadline_monotonic = previous_deadline
        else:
            fields, rows = execute()
        owner._check_deadline()
        accepted_cap = 161 if is_attempt_query else 1024 if is_data_query else owner.config.maximum_rows
        _require(len(rows) <= accepted_cap, "Complete bounded Spark result required")
        mapped = [r.asDict(recursive=True) for r in rows]
        _require(all(type(r) is dict and set(r) == {n for n, _ in fields} for r in mapped))
        _require(len(_native_json(mapped).encode("utf-8")) <= owner.config.maximum_result_bytes,
                 "Spark native result exceeds byte budget")
        return SQLResult(mapped, fields)


class ManagedCurrentSparkReceiptReader:
    """Reopen a receipt from a separately injected SparkSession."""

    def __init__(self, spark, config: ManagedCurrentSparkReceiptConfig, *,
                 operation_deadline_monotonic=None, operation_runner=None):
        _require(spark is not None and type(config) in (
            ManagedCurrentSparkReceiptConfig, ManagedCurrentSparkServerlessReceiptConfig,
            ManagedCurrentSparkClassicTaskReceiptConfig))
        if (operation_deadline_monotonic is not None
                and (type(operation_deadline_monotonic) not in (int, float)
                     or operation_deadline_monotonic <= time.monotonic())):
            raise ValueError("Future serverless task operation deadline required")
        _require(operation_deadline_monotonic is None
                 or config.execution_profile == "serverless_connect"
                 or type(config) is ManagedCurrentSparkClassicTaskReceiptConfig,
                 "A task deadline override requires an explicit borrowed task profile")
        _require(operation_runner is None or type(config) is ManagedCurrentSparkClassicTaskReceiptConfig
                 and callable(operation_runner),
                 "An operation runner requires the exact borrowed classic task profile")
        self.spark, self.config = spark, config
        self._operation_runner = operation_runner
        self.carriers = load_current_managed_carriers(config.carrier_profile)
        self.started, self.statements, self.closed = time.monotonic(), 0, False
        self.closing = False
        self._closing_bookend_complete = False
        self.context = object()
        self.opening = None
        self._approved_data_queries = set()
        self._approved_hash_parameters = set()
        self.executor = _SparkReceiptExecutor(self)
        self._serverless_tag = None
        self._serverless_operation_unknown = False
        self._serverless_close_deadline = None
        self._serverless_task_deadline = operation_deadline_monotonic
        self._classic_close_deadline = None
        if config.execution_profile == "serverless_connect":
            self._serverless_tag = _ServerlessSparkTagCancellation(self.spark)
        else:
            sc = getattr(self.spark, "sparkContext", None)
            _require(sc is not None and all(hasattr(sc, name) for name in
                ("getLocalProperty", "setLocalProperty", "setJobGroup", "cancelJobGroup")),
                "Spark job-group cancellation is required for bounded closing")
        self._observe_environment = self._observe_spark_environment
        try:
            self.opening_runtime = self._observe_environment()
            if type(config) is ManagedCurrentSparkClassicTaskReceiptConfig:
                runtime_match = (type(self.opening_runtime) is dict
                    and {key: self.opening_runtime.get(key) for key in config.expected_runtime}
                        == config.expected_runtime
                    and set(self.opening_runtime) == set(config.expected_runtime)
                        | {"sparkApplicationId"})
            elif config.execution_profile == "serverless_connect":
                runtime_match = (type(self.opening_runtime) is dict
                    and {key: self.opening_runtime.get(key) for key in config.expected_runtime}
                        == config.expected_runtime
                    and set(self.opening_runtime) == set(config.expected_runtime) | {"sessionTimeZone"})
            else:
                runtime_match = self.opening_runtime == config.expected_runtime
            _require(runtime_match,
                     "Spark opening runtime/principal/namespace differs from explicit selection")
            self.opening = self._observe_inventory()
            self._manifest = self._request = self._publication_id = self._observation = None
            # Retain the original byte inputs so close can independently repeat
            # the resolver and source-derived data witness under the same session.
            self._reopen_inputs = None
            self._closing_witness = None
            self._operational_reopen_witness = None
        except BaseException as error:
            if self._serverless_tag is not None:
                try:
                    self._serverless_tag.close()
                except BaseException as close_error:
                    if callable(getattr(error, "add_note", None)):
                        error.add_note("Spark Connect tag cleanup failed: "
                                       + type(close_error).__name__)
            raise

    def _observe_spark_environment(self):
        """Observe Spark Jobs identity without borrowing the SQL/serverless profile."""
        c = self.config
        if c.execution_profile == "serverless_connect":
            from .managed_current_publication import observe_managed_current_environment
            observed = self._run_serverless_bounded(lambda:
                observe_managed_current_environment(self.spark, c.protocol_profile))
            timezone_rows = self.executor.query(
                "SELECT current_timezone() AS session_timezone", {}).rows
            _require(len(timezone_rows) == 1
                     and timezone_rows[0].get("session_timezone") in ("UTC", "Etc/UTC"),
                     "Exact UTC Spark Connect session timezone required")
            observed["sessionTimeZone"] = timezone_rows[0]["session_timezone"]
            _require(observed.get("runtimeName") == "databricks-serverless"
                     and observed.get("protocol") == c.protocol_profile.identity,
                     "Exact serverless runtime and protocol observations required")
            return observed
        rows = self.executor.query("SELECT current_user() AS principal,"
            "current_catalog() AS catalog,current_schema() AS schema", {}).rows
        _require(len(rows) == 1)
        timezone_rows = self.executor.query(
            "SELECT current_timezone() AS session_timezone", {}).rows
        _require(len(timezone_rows) == 1
                 and timezone_rows[0].get("session_timezone") in ("UTC", "Etc/UTC"),
                 "Exact UTC Spark session timezone required")
        try:
            build_rows = self.executor.query("SELECT version() AS sparkBuildText", {}).rows
            build_text = build_rows[0].get("sparkBuildText") if len(build_rows) == 1 else None
        except Exception:
            build_text = None
        try:
            cluster_id = self.spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
        except Exception:
            cluster_id = None
        try:
            runtime_tag = self.spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")
        except Exception:
            runtime_tag = None
        app_id = getattr(getattr(self.spark, "sparkContext", None), "applicationId", None)
        observed = {"runtimeName": "databricks-jobs-cluster", "sparkVersion": self.spark.version,
            "sparkBuildText": build_text, "runtimeTag": runtime_tag,
            "sparkApplicationId": app_id, "clusterId": cluster_id,
            "principal": rows[0].get("principal"), "catalog": rows[0].get("catalog"),
            "schema": rows[0].get("schema"), "protocol": c.protocol_profile.identity,
            "sessionTimeZone": timezone_rows[0]["session_timezone"]}
        _require(all(type(observed[k]) is str and observed[k] for k in (
            "runtimeName", "sparkVersion", "sparkApplicationId", "clusterId", "principal",
            "catalog", "schema", "protocol")),
            "Spark Jobs runtime, application, cluster, principal, namespace and protocol required")
        _require(all(observed[k] is None or type(observed[k]) is str
                     for k in ("sparkBuildText", "runtimeTag")))
        return observed

    def _check_deadline(self):
        _require(not self._serverless_operation_unknown,
                 "Prior Spark Connect operation timed out; completion remains unknown")
        _require(not self.closed and (self.closing or
                 time.monotonic() - self.started < self.config.maximum_elapsed_seconds),
                 "Spark receipt cumulative deadline expired")
        if self.closing and self._classic_close_deadline is not None:
            _require(time.monotonic() < self._classic_close_deadline,
                     "Classic Spark close deadline expired")

    def _run_serverless_bounded(self, operation):
        """Bound one Connect request by the reader/close deadline, then interrupt its tag."""
        if self.closing:
            deadline = self._serverless_close_deadline
        else:
            deadline = self.started + self.config.maximum_elapsed_seconds
            if self._serverless_task_deadline is not None:
                deadline = min(deadline, self._serverless_task_deadline)
        remaining = deadline - time.monotonic() if deadline is not None else 0
        _require(remaining > 0, "Spark Connect operation deadline expired")
        result = queue.Queue(maxsize=1)

        def invoke():
            try:
                result.put((True, self._serverless_tag.run_tagged_thread(operation)))
            except BaseException as error:
                result.put((False, error))

        thread = threading.Thread(target=invoke, name="ashlar-spark-connect-query", daemon=True)
        thread.start()
        try:
            succeeded, value = result.get(timeout=remaining)
        except queue.Empty as error:
            self._serverless_operation_unknown = True
            # interruptTag itself may block; do not extend the caller's deadline
            # or report termination when its response arrives.
            interrupt = threading.Thread(target=self._serverless_tag.interrupt_tagged_operations,
                name="ashlar-spark-connect-interrupt", daemon=True)
            interrupt.start()
            raise ValueError(
                "Spark Connect request exceeded its bounded deadline; tagged interrupt requested, "
                "operation completion remains unknown") from error
        if not succeeded:
            raise value
        return value

    def run_observer_operation(self, operation):
        """Run bounded verifier policy SQL under this reader's exact owned tag."""
        _require(self.config.execution_profile == "serverless_connect"
                 and self._serverless_tag is not None and not self.closed and not self.closing,
                 "Live Spark Connect receipt reader required for bounded observer SQL")
        return self._run_serverless_bounded(operation)

    @staticmethod
    def _unique_pairs(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON member")
            result[key] = value
        return result

    @staticmethod
    def _integer(value):
        # Spark returns native integral numbers while SQL returns text. Reject bool,
        # floats, decimal coercion and overflow at this normalization boundary.
        if type(value) is int:
            _require(0 <= value < 2**63, "Native nonnegative signed64 value required")
            return value
        _require(type(value) is str and re.fullmatch(r"0|[1-9][0-9]{0,18}", value)
                 and int(value) < 2**63, "Canonical native nonnegative signed64 value required")
        return int(value)

    def _observe_inventory(self):
        c = self.config
        tables = self.executor.query(
            "SELECT table_name,table_type,table_owner FROM system.information_schema.tables "
            "WHERE table_catalog=:catalog AND table_schema=:schema",
            {"catalog": c.catalog, "schema": c.schema}).rows
        _require(len(tables) == len(ROLES)
                 and {r.get("table_name") for r in tables} == set(ROLES)
                 and all(r.get("table_type") == "MANAGED"
                         and r.get("table_owner") == c.expected_table_owner for r in tables),
                 "Exactly six selected managed roles and owners required")
        cols = self.executor.query(
            _INFORMATION_SCHEMA_COLUMNS_SQL,
            {"catalog": c.catalog, "schema": c.schema}).rows
        by_role = {role: [] for role in ROLES}
        for row in cols:
            role = row.get("table_name")
            _require(role in by_role and row.get("is_nullable") in ("YES", "NO"))
            by_role[role].append((self._integer(row.get("ordinal_position")), row.get("column_name"),
                                  row.get("data_type"), row.get("is_nullable") == "YES"))
        inventory = {}
        for role in ROLES:
            expected = tuple((n, t, nullable) for n, t, nullable in self.carriers.ordered_fields(role))
            actual = by_role[role]
            positions = [x[0] for x in actual]
            ordinal_base = positions[0] if positions else None
            _require(actual and ordinal_base in (0, 1)
                     and positions == list(range(ordinal_base, ordinal_base + len(actual)))
                     and tuple((n, t, nullable) for _, n, t, nullable in actual) == expected,
                     "Ordered native columns differ from the admitted carrier")
            detail_rows = self.executor.query("DESCRIBE DETAIL " + quote_table_identifier(
                ".".join((c.catalog, c.schema, role))), {}).rows
            _require(len(detail_rows) == 1)
            detail = dict(detail_rows[0])
            _require(detail.get("format") == "delta")
            table_uuid = detail.get("id")
            _require(type(table_uuid) is str and str(uuid.UUID(table_uuid)) == table_uuid,
                     "Canonical native Delta UUID required")
            features = detail.get("tableFeatures")
            _require(type(features) in (list, tuple), "Spark native table features must remain an array")
            proto_detail = dict(detail)
            proto_detail["minReaderVersion"] = str(self._integer(detail.get("minReaderVersion")))
            proto_detail["minWriterVersion"] = str(self._integer(detail.get("minWriterVersion")))
            proto_detail["tableFeatures"] = _canonical(list(features))
            protocol = validate_protocol_detail(proto_detail, uuid=table_uuid, profile=c.protocol_profile)
            history = self.executor.query("DESCRIBE HISTORY " + quote_table_identifier(
                ".".join((c.catalog, c.schema, role))) + " LIMIT 1", {}).rows
            _require(len(history) == 1)
            version = self._integer(history[0].get("version"))
            # Persist the same simpleString spelling emitted by the publisher's
            # installed Spark StructType. Resolver schema JSON separately uses
            # Spark's ``long`` spelling for signed 64-bit values.
            schema = [[name, kind.lower(), nullable] for _, name, kind, nullable in actual]
            digest = hashlib.sha256(_native_json(detail).encode()).hexdigest()
            inventory[role] = {"name": [c.catalog, c.schema, role], "uuid": table_uuid,
                "version": version, "schema": schema, "protocol": protocol,
                "detailSha256": digest, "owner": c.expected_table_owner, "tableType": "MANAGED"}
        _require(len({i["uuid"] for i in inventory.values()}) == len(ROLES),
                 "Six distinct native Delta table identities required")
        return inventory

    def _native_identity(self, role):
        c = self.config
        rows = self.executor.query("DESCRIBE DETAIL " + quote_table_identifier(
            ".".join((c.catalog, c.schema, role))), {}).rows
        _require(len(rows) == 1)
        detail = dict(rows[0])
        table_uuid = detail.get("id")
        _require(detail.get("format") == "delta" and type(table_uuid) is str
                 and str(uuid.UUID(table_uuid)) == table_uuid)
        features = detail.get("tableFeatures")
        _require(type(features) in (list, tuple), "Spark table features must remain a native array")
        protocol_detail = dict(detail)
        protocol_detail["minReaderVersion"] = str(self._integer(detail.get("minReaderVersion")))
        protocol_detail["minWriterVersion"] = str(self._integer(detail.get("minWriterVersion")))
        protocol_detail["tableFeatures"] = _canonical(list(features))
        protocol = validate_protocol_detail(protocol_detail, uuid=table_uuid,
                                            profile=c.protocol_profile)
        return table_uuid, protocol

    def _admit_committed_attempt(self, request, manifest):
        """Reopen bounded source-qualified phase custody at the admitted table head."""
        from ashlar.attempt_store import validate_source_qualified_phase_inventory

        role = "publication_attempt_phase"
        admitted = self.opening[role]
        identity = (admitted["uuid"], admitted["protocol"])
        _require(self._native_identity(role) == identity,
                 "Native attempt table identity changed before committed read")
        table = quote_table_identifier(".".join((self.config.catalog, self.config.schema, role)))
        sql = ("SELECT phase,request_digest,payload_json,payload_digest FROM " + table
               + " VERSION AS OF " + str(admitted["version"])
               + " WHERE stream=:stream AND batch_id=:batch ORDER BY CASE phase "
               + "WHEN 'prepared' THEN 0 WHEN 'applying' THEN 1 WHEN 'applied' THEN 2 "
               + "WHEN 'committing' THEN 3 WHEN 'committed' THEN 4 END LIMIT 161")
        rows = self.executor.query(sql, {"stream": request["stream"],
                                         "batch": request["batch_id"]}).rows
        _require(self._native_identity(role) == identity,
                 "Native attempt table identity changed during committed read")
        records = validate_source_qualified_phase_inventory(
            request["stream"], request["batch_id"], rows, request, maximum_rows=160)
        _require(len(records) == 5 and
                 tuple(record.phase for record in records) ==
                 ("prepared", "applying", "applied", "committing", "committed"),
                 "Exact complete committed source-qualified attempt required")
        payload_text = records[-1].payload_json
        payload = decode_retained_json(payload_text.encode("utf-8"))
        _require(type(payload) is dict and set(payload) in (
                 {"request", "result_json", "descriptor_json"},
                 {"request", "result_json", "descriptor_json", "plan_json"})
                 and payload.get("request") == request,
                 "Exact retained committed request strings required")
        result_text, descriptor_text = payload.get("result_json"), payload.get("descriptor_json")
        _require(type(result_text) is str and type(descriptor_text) is str,
                 "Exact committed result and descriptor strings required")
        result = decode_retained_json(result_text.encode("utf-8"))
        descriptor = decode_retained_json(descriptor_text.encode("utf-8"))
        _require(type(result) is dict and set(result) == {"effects", "manifest"}
                 and type(result.get("effects")) is dict
                 and result.get("manifest") == manifest
                 and descriptor == manifest,
                 "Committed result/descriptor must equal the reopened manifest")
        plan_text = payload.get("plan_json")
        _require(plan_text is None or type(plan_text) is str,
                 "Retained committed plan must remain exact text")
        return ManagedCurrentSparkAttemptCustody(
            admitted["version"], hashlib.sha256(payload_text.encode("utf-8")).hexdigest(),
            hashlib.sha256(plan_text.encode("utf-8")).hexdigest() if plan_text is not None else None,
            hashlib.sha256(result_text.encode("utf-8")).hexdigest(),
            hashlib.sha256(descriptor_text.encode("utf-8")).hexdigest())

    def _role_read_sql(self, role, version):
        _require(role in DATA_ROLES and type(version) is int and 0 <= version < 2**63,
                 "Exact admitted role and literal version required")
        table = quote_table_identifier(".".join((self.config.catalog, self.config.schema, role)))
        projection = []
        for name, kind, _nullable in self.carriers.ordered_fields(role):
            column = "`" + name + "`"
            if kind == "BIGINT":
                projection.append("CAST(" + column + " AS STRING) AS " + column)
            elif kind == "TIMESTAMP":
                projection.append("CAST(unix_micros(" + column + ") AS STRING) AS " + column)
            else:
                projection.append(column)
        return ("SELECT " + ",".join(projection) + " FROM " + table
                + " VERSION AS OF " + str(version) + " LIMIT 1025")

    def _read_and_compare_source_bags(self, oracle, request, manifest, versions, role_tables):
        _require(type(oracle) is ManagedCurrentSourcePrefixOracle
                 and oracle.profile in ("ashlar-managed-current-source-prefix/0.1",
                                        "ashlar-managed-current-a1-source-oracle/0.1")
                 and oracle.carrier_profile == self.config.carrier_profile
                 and oracle.destination == {"catalog": self.config.catalog, "schema": self.config.schema}
                 and oracle.final_request == request
                 and oracle.progress == json.loads(manifest["source_progress_json"],
                                                    object_pairs_hook=self._unique_pairs)
                 and type(oracle.prefix_sha256) is str
                 and re.fullmatch(r"[0-9a-f]{64}", oracle.prefix_sha256),
                 "Frozen prefix comparison anchors differ from reopened request/manifest")
        expected = oracle.expected_rows
        _require(type(expected) is dict and set(expected) == set(DATA_ROLES)
                 and all(type(expected[role]) is list for role in DATA_ROLES),
                 "Exact source-derived four-role comparison bags required")
        expected_rows = sum(len(expected[role]) for role in DATA_ROLES)
        expected_bytes = sum(len(_canonical(expected[role]).encode("utf-8")) for role in DATA_ROLES)
        _require(expected_rows <= 1024 and expected_bytes <= 4 * 1024 * 1024,
                 "Frozen source comparison bags exceed row or byte bounds")
        signed64 = re.compile(r"0|-?[1-9][0-9]{0,18}\Z")
        for role in DATA_ROLES:
            fields = self.carriers.ordered_fields(role)
            names = {name for name, _kind, _nullable in fields}
            for row in expected[role]:
                _require(type(row) is dict and set(row) == names,
                         "Frozen source role row has an unexpected carrier shape")
                for name, kind, nullable in fields:
                    value = row[name]
                    _require(value is not None or nullable,
                             "Frozen source non-null carrier became null")
                    if value is None:
                        continue
                    if kind == "STRING":
                        _require(type(value) is str, "Frozen source STRING must remain exact text")
                    elif kind in ("BIGINT", "TIMESTAMP"):
                        _require(type(value) is str and signed64.fullmatch(value)
                                 and -(1 << 63) <= int(value) < (1 << 63),
                                 "Frozen source signed64 token must be canonical text")
                    elif kind == "BOOLEAN":
                        _require(type(value) is bool, "Frozen source BOOLEAN must remain Boolean")
        queries = {}
        for table, role in role_tables.items():
            version = versions[table]
            _require(type(version) is int and 0 <= version <= self.opening[role]["version"])
            queries[role] = self._role_read_sql(role, version)
        self._approved_data_queries = set(queries.values())
        try:
            return self._capture_source_role_bags(expected, queries)
        finally:
            self._approved_data_queries.clear()

    def _probe_source_lookup_hashes(self, source_keys):
        """Probe bounded .4 source keys using only native typed Spark SQL."""
        _require(self.config.carrier_profile == CARRIER_PROFILE_XXHASH64
                 and type(source_keys) is tuple and len(source_keys) <= 32
                 and len(set(source_keys)) == len(source_keys),
                 "Bounded .4 source-key probe set of at most 32 unique keys required")
        by_arguments = {}
        for item in source_keys:
            _require(type(item) is tuple and len(item) == 4,
                     "Exact kind/source/type/id source key required")
            kind, source, type_id, entity_id = item
            _require(kind in ("object", "edge") and type(source) is str and source
                     and type(type_id) is int and type(entity_id) is int
                     and -(1 << 63) <= type_id < (1 << 63)
                     and -(1 << 63) <= entity_id < (1 << 63),
                     "Non-Boolean signed64 source identity tokens required")
            _require(len(source.encode("utf-8")) <= _MAX_PROBE_SOURCE_BYTES,
                     "Native source-key probe source token exceeds 65536 UTF-8 bytes")
            args = (source, str(type_id), str(entity_id))
            by_arguments.setdefault(args, set()).add(item)
        _require(len(by_arguments) <= 32,
                 "Distinct native source-key probe limit exceeded")
        _require(not self._approved_hash_parameters
                 and self.statements + len(by_arguments) + 12
                 <= self.config.maximum_statements - 24,
                 "Insufficient statement budget for probes, row captures, bookends and closing")
        self._approved_hash_parameters = set(by_arguments)
        by_native_key = {}
        try:
            for args, identities in sorted(by_arguments.items()):
                result = self.executor.query(_XXHASH_SQL,
                    {"source": args[0], "type": args[1], "id": args[2]})
                _require(len(result.rows) == 1 and result.columns == (("lookup_hash", "long"),),
                         "One exact native signed BIGINT hash per source key required")
                value = result.rows[0].get("lookup_hash")
                _require(type(value) is int and -(1 << 63) <= value < (1 << 63),
                         "Native xxhash64 result must be signed64 and non-Boolean")
                by_native_key[args] = value
            _require(not self._approved_hash_parameters,
                     "Every preapproved source-key hash probe must be consumed exactly once")
        finally:
            self._approved_hash_parameters.clear()
        result = {}
        for args, identities in by_arguments.items():
            for kind, source, type_id, entity_id in identities:
                result[(kind, source, type_id, entity_id)] = by_native_key[args]
        _require(set(result) == set(source_keys), "Native source-key hash map differs from exact oracle keys")
        return result

    def _frozen_prefix_anchors(self, prefix_bytes, prefix_sha256, source_core_config,
                               request, manifest):
        _require(type(prefix_bytes) is bytes and 0 < len(prefix_bytes) <= 4 * 1024 * 1024
                 and type(prefix_sha256) is str
                 and re.fullmatch(r"[0-9a-f]{64}", prefix_sha256)
                 and hashlib.sha256(prefix_bytes).hexdigest() == prefix_sha256
                 and type(source_core_config) is ManagedCurrentCoreConfig
                 and source_core_config.carrier_profile == CARRIER_PROFILE_XXHASH64
                 and source_core_config.catalog == self.config.catalog
                 and source_core_config.schema == self.config.schema
                 and self.config.carrier_profile == CARRIER_PROFILE_XXHASH64,
                 "Separately frozen .4 prefix bytes/hash and matching source config required")
        prefix = decode_retained_json(prefix_bytes)
        _require(type(prefix) is dict and prefix.get("profile") ==
                 "ashlar-managed-current-source-prefix/0.1"
                 and prefix.get("carrierProfile") == CARRIER_PROFILE_XXHASH64
                 and prefix.get("destination") == {"catalog": self.config.catalog,
                                                    "schema": self.config.schema}
                 and type(prefix.get("entries")) is list and prefix["entries"]
                 and type(prefix["progress"]) is dict
                 and prefix["progress"] == json.loads(manifest["source_progress_json"],
                                                       object_pairs_hook=self._unique_pairs),
                 "Frozen .4 prefix config/progress differs from reopened manifest")
        last_request = decode_retained_json(prefix["entries"][-1]["requestJson"].encode("utf-8"))
        _require(last_request == request,
                 "Frozen .4 final request differs from independently reopened request")
        return prefix

    def _capture_source_role_bags(self, expected, queries):
        actual = {}
        total_rows = total_bytes = 0
        projected_types = {"STRING": "string", "BIGINT": "string",
                           "BOOLEAN": "boolean", "TIMESTAMP": "string"}
        signed64 = re.compile(r"0|-?[1-9][0-9]{0,18}\Z")
        for role in DATA_ROLES:
            identity = (self.opening[role]["uuid"], self.opening[role]["protocol"])
            _require(self._native_identity(role) == identity,
                     "Pinned native role identity changed before complete bag capture")
            result = self.executor.query(queries[role], {})
            fields = self.carriers.ordered_fields(role)
            _require(tuple(result.columns) == tuple((name, projected_types[kind])
                                                     for name, kind, _ in fields),
                     "Exact ordered native role projection types required")
            _require(len(result.rows) <= 1024, "Per-role complete capture sentinel exceeded")
            _require(self._native_identity(role) == identity,
                     "Pinned native role identity changed during complete bag capture")
            normalized = []
            for row in result.rows:
                _require(type(row) is dict and set(row) == {name for name, _, _ in fields},
                         "Exact projected native row fields required")
                item = {}
                for name, kind, nullable in fields:
                    value = row[name]
                    _require(value is not None or nullable, "Native non-null carrier became null")
                    if value is None:
                        item[name] = None
                    elif kind == "STRING":
                        _require(type(value) is str, "Native STRING must remain exact text")
                        item[name] = value
                    elif kind in ("BIGINT", "TIMESTAMP"):
                        _require(type(value) is str and signed64.fullmatch(value)
                                 and -(1 << 63) <= int(value) < (1 << 63),
                                 "Projected signed64 token must be canonical text")
                        item[name] = value
                    elif kind == "BOOLEAN":
                        _require(type(value) is bool, "Native BOOLEAN token must remain Boolean")
                        item[name] = value
                    else:
                        raise ValueError("Unsupported managed-current projection type")
                normalized.append(item)
            # The native query is deliberately unordered. Keep multiplicity,
            # but canonicalize the bag before hashing so repeated exact-version
            # observations do not depend on Spark's row-return order.
            normalized.sort(key=_canonical)
            actual[role] = normalized
            total_rows += len(normalized)
            total_bytes += len(_canonical(normalized).encode("utf-8"))
            _require(total_rows <= 1024 and total_bytes <= 4 * 1024 * 1024,
                     "Native four-role capture exceeds cumulative row or byte bound")
        _require(sum(len(actual[r]) for r in DATA_ROLES) <= 1024
                 and sum(len(_canonical(actual[r]).encode("utf-8")) for r in DATA_ROLES)
                 <= 4 * 1024 * 1024,
                 "Native four-role capture exceeds complete bounds")
        comparison_sha256 = compare_complete_managed_current_bags(
            expected, actual, carrier_profile=self.config.carrier_profile)
        summary = {
            "roles": {
                role: {
                    "row_count": len(actual[role]),
                    "normalized_bytes": len(_canonical(actual[role]).encode("utf-8")),
                    "comparison_sha256": hashlib.sha256(
                        _canonical(actual[role]).encode("utf-8")).hexdigest(),
                }
                for role in DATA_ROLES
            },
            "row_count": sum(len(actual[role]) for role in DATA_ROLES),
            "normalized_bytes": sum(len(_canonical(actual[role]).encode("utf-8"))
                                    for role in DATA_ROLES),
            "comparison_sha256": comparison_sha256,
        }
        return comparison_sha256, summary

    def _renew(self):
        self._check_deadline()
        witness = self._operational_reopen_witness
        if witness is not None:
            _require(self.config.execution_profile == "serverless_connect"
                     and self.config.operational_observation_stage
                     and not self.closing and not self.closed
                     and not self._serverless_operation_unknown
                     and type(witness) is tuple and len(witness) == 4
                     and witness[0] is self.context
                     and witness[1] == self._publication_id
                     and witness[2] is self._request
                     and witness[3] is self._manifest,
                     "Operational opening inventory witness is stale or out of scope")
            # Within one read-only resolver invocation, repeated authorization
            # callbacks bind the same exact request and manifest to this fresh
            # opening inventory. Exact VERSION AS OF reads and role identity
            # bookends remain live; reader close still performs a fresh full
            # inventory/runtime check. Never carry this witness across reopen,
            # close, task, or observation stage.
            return self.opening_runtime, self.opening
        if self.closing and self._closing_bookend_complete:
            return self.opening_runtime, self.opening
        runtime = self._observe_environment()
        _require(runtime == self.opening_runtime,
                 "Spark runtime/principal/namespace identity changed")
        inventory = self._observe_inventory()
        _require(inventory == self.opening,
                 "Spark runtime or six-role UC identity changed")
        return runtime, inventory

    def reopen(self, *, request_bytes, manifest_bytes, expected_receipt, source_oracle=None,
               source_prefix_bytes=None, source_prefix_sha256=None, source_core_config=None,
               a1_plan=None, a1_prefix_selected=None):
        prefix_supplied = any(value is not None for value in (
            source_prefix_bytes, source_prefix_sha256, source_core_config))
        a1_args_supplied = any(value is not None for value in (
            a1_plan, a1_prefix_selected))
        is_a1_reader = self.config.source_truss_version in ("0.0.52", "0.0.53", "0.0.54", "0.0.55")
        if is_a1_reader:
            _require(self.config.carrier_profile == CARRIER_PROFILE_XXHASH64
                     and source_oracle is None
                     and prefix_supplied
                     and all(value is not None for value in (
                         source_prefix_bytes, source_prefix_sha256, source_core_config))
                     and a1_plan is not None and type(a1_prefix_selected) is dict,
                     "Truss .52/.53/.54 A1 reads require the exact `/0.2` plan, selected page and complete `.4` prefix pins")
            from .managed_current_a1_trial_plan import ManagedCurrentA1TrialPlan
            from .managed_current_truss052_genesis_owner import (
                ManagedCurrentTruss052GenesisSelection,
            )
            from .managed_current_truss053_genesis_owner import (
                ManagedCurrentTruss053GenesisSelection,
            )
            from .truss_graph_admission import TrussGraphAdmission
            expected_selection = {
                "0.0.52": ManagedCurrentTruss052GenesisSelection,
                "0.0.53": ManagedCurrentTruss053GenesisSelection,
                "0.0.54": ManagedCurrentTruss053GenesisSelection,
                "0.0.55": ManagedCurrentTruss053GenesisSelection,
            }[self.config.source_truss_version]
            _require(type(a1_plan) is ManagedCurrentA1TrialPlan
                     and type(a1_plan.selection) is expected_selection
                     and type(a1_plan.selection.admission) is TrussGraphAdmission
                     and a1_plan.selection.admission.current_truss_version
                         == self.config.source_truss_version,
                     "A1 plan selection and graph admission must match the configured Truss version")
        else:
            _require(not a1_args_supplied,
                     "A1 `/0.2` plan inputs require the exact Truss .52/.53/.54 reader profile")
        if self.config.carrier_profile == CARRIER_PROFILE_XXHASH64:
            _require(source_oracle is None,
                     "Prebuilt .4 source oracles cannot stand in for native-probed hashes")
            _require(not prefix_supplied or all(value is not None for value in (
                source_prefix_bytes, source_prefix_sha256, source_core_config)),
                "Complete frozen .4 prefix bytes/hash/config input required")
        else:
            _require(not prefix_supplied,
                     "Frozen source-prefix probing is reserved for the .4 carrier")
        _require(type(request_bytes) is bytes and 0 < len(request_bytes) <= 4194304
                 and type(manifest_bytes) is bytes and 0 < len(manifest_bytes) <= 4194304,
                 "Bounded original request and manifest bytes required")
        request, manifest = decode_retained_json(request_bytes), decode_retained_json(manifest_bytes)
        _require(type(request) is dict and type(manifest) is dict and type(expected_receipt) is dict
                 and expected_receipt.get("profile") == "ashlar-managed-current-destination-receipt/0.1"
                 and expected_receipt.get("publicationId") == manifest.get("publication_id")
                 and expected_receipt.get("manifest") == manifest
                 and expected_receipt.get("manifestSha256") == hashlib.sha256(
                     _canonical(manifest).encode()).hexdigest(), "Exact task receipt and manifest required")
        self._manifest, self._request = manifest, request
        self._publication_id = manifest.get("publication_id")
        _require(type(self._publication_id) is str and self._publication_id)
        quoted_manifest = quote_table_identifier(".".join((self.config.catalog, self.config.schema,
                                                              "publication_manifest")))
        observed = self.executor.query(
            "SELECT publication_id,profile_version,table_versions_json,source_progress_json,"
            "schema_revisions_json,validation_report_json,"
            "CAST(unix_micros(recorded_at) AS STRING) AS recorded_at FROM " + quoted_manifest
            + " WHERE publication_id=:publication_id", {"publication_id": self._publication_id}).rows
        _require(len(observed) == 1 and dict(observed[0]) == manifest,
                 "Exact task manifest was not independently reopened")
        frozen_prefix = source_keys = None
        if prefix_supplied:
            if a1_plan is None:
                frozen_prefix = self._frozen_prefix_anchors(source_prefix_bytes,
                    source_prefix_sha256, source_core_config, request, manifest)
                source_keys = derive_managed_current_prefix_lookup_keys(source_prefix_bytes,
                    prefix_sha256=source_prefix_sha256, config=source_core_config,
                    expected_source_truss_version=self.config.source_truss_version)
            else:
                _require(self.config.carrier_profile == CARRIER_PROFILE_XXHASH64
                         and self.config.source_truss_version in ("0.0.52", "0.0.53", "0.0.54", "0.0.55")
                         and type(a1_prefix_selected) is dict,
                         "A1 `/0.2` source selection requires exact fresh Truss runtime and `.4` pins")
                from .managed_current_spark_source_oracle import managed_current_a1_lookup_keys
                frozen_prefix = a1_plan.verify_delivery(
                    source_prefix_bytes, prefix_sha256=source_prefix_sha256,
                    selected=a1_prefix_selected,
                    schedule_bytes=a1_plan.selection.schedule_bytes,
                    pages=(a1_plan.delivery_page,))
                source_keys = managed_current_a1_lookup_keys(a1_plan,
                    config=source_core_config)
        from ashlar.publication import descriptor_from_manifest_row
        from ashlar.attempt_store import verify_request_digest
        descriptor = descriptor_from_manifest_row(dict(observed[0]))
        verify_request_digest(request)
        _require(request.get("request_digest") == expected_receipt.get("requestDigest"),
                 "Receipt request digest differs from original source intent")
        versions = json.loads(manifest["table_versions_json"], object_pairs_hook=self._unique_pairs)
        role_tables = {".".join((self.config.catalog, self.config.schema, role)): role for role in DATA_ROLES}
        _require(type(versions) is dict and set(versions) == set(role_tables),
                 "Manifest must pin exactly four distinct data roles")
        uuids = {table: self.opening[role]["uuid"] for table, role in role_tables.items()}
        authority = "managed-current-spark:" + self.config.expected_principal
        vector = manifest_pin_vector(manifest, uuids, authority=authority)
        expected_vector = {table: {"uuid": uuids[table], "version": versions[table]} for table in role_tables}
        _require(expected_receipt.get("dataRoleVector") == expected_vector,
                 "Task receipt four-role vector differs from native tables")
        inventory = expected_receipt.get("installationInventory")
        _require(type(inventory) is dict and set(inventory) == set(ROLES))
        for role in ROLES:
            expected, actual = inventory[role], self.opening[role]
            _require(type(expected) is dict and set(expected) == {"name", "uuid", "version", "schema", "protocol"}
                     and expected == {"name": ".".join(actual["name"]), "uuid": actual["uuid"],
                                     "version": actual["version"], "schema": actual["schema"],
                                     "protocol": actual["protocol"]},
                     "Task receipt six-role inventory differs from live UC")
        attempt_custody = self._admit_committed_attempt(request, manifest)
        binding = expected_receipt.get("weftBindingInputs")
        _require(type(binding) is dict and set(binding) == {"manifest", "manifestUuid", "dataRoles"}
                 and binding["manifest"] == manifest
                 and binding["manifestUuid"] == self.opening["publication_manifest"]["uuid"]
                 and type(binding["dataRoles"]) is dict and set(binding["dataRoles"]) == set(DATA_ROLES),
                 "Exact compiler binding inputs required")
        roles = {}
        for role in DATA_ROLES:
            table = ".".join((self.config.catalog, self.config.schema, role))
            item, actual = binding["dataRoles"][role], self.opening[role]
            _require(type(item) is dict and item == {"name": table, "uuid": actual["uuid"],
                                                    "version": actual["version"]})
            roles[role] = {"name": list(actual["name"]), "uuid": actual["uuid"], "version": actual["version"]}
        _require(expected_receipt.get("sourceAuthority") ==
                 f"development Truss {self.config.source_truss_version} page on managed Unity Catalog; external source ACK required",
                 "Receipt source authority differs from the selected source holder")
        revisions = json.loads(request["schema_revisions_json"], object_pairs_hook=self._unique_pairs)
        _require(type(revisions) is dict and revisions
                 and all(type(k) is str and type(v) is str for k, v in revisions.items()))
        native_types = {"STRING": "string", "BIGINT": "long", "BOOLEAN": "boolean",
                        "TIMESTAMP": "timestamp"}
        schemas = {table: tuple((n, native_types[t]) for n, t, _ in
                                self.carriers.ordered_fields(role)) for table, role in role_tables.items()}
        policy = _SparkReceiptPolicy(self, descriptor)
        backend = _SparkReceiptBackend(self, policy, schemas)
        if self.config.execution_profile == "serverless_connect" \
                and self.config.operational_observation_stage:
            _require(self._operational_reopen_witness is None and self._observation is None,
                     "Operational stage permits one exact resolver reopen")
            self._operational_reopen_witness = (
                self.context, self._publication_id, request, manifest)
        try:
            resolved = resolve_publication(backend, self._publication_id, uuids,
                context=self.context, supported_profiles=(self.config.carrier_profile,),
                supported_revisions={k: (v,) for k, v in revisions.items()})
        finally:
            self._operational_reopen_witness = None
        _require(dict(resolved.descriptor.raw) == manifest
                 and all(s.uuid == uuids[t] and s.version == versions[t]
                         for t, s in resolved.snapshots.items()),
                 "Independent native resolver differs from exact four-role publication")
        bag_comparison_sha256 = bag_comparison_summary = None
        if frozen_prefix is not None:
            lookup_hashes = self._probe_source_lookup_hashes(source_keys)
            if a1_plan is None:
                source_oracle = build_managed_current_prefix_oracle(source_prefix_bytes,
                    prefix_sha256=source_prefix_sha256, config=source_core_config,
                    lookup_hashes=lookup_hashes,
                    expected_source_truss_version=self.config.source_truss_version)
            else:
                from .managed_current_spark_source_oracle import (
                    build_managed_current_a1_source_oracle,
                )
                source_oracle = build_managed_current_a1_source_oracle(
                    a1_plan, config=source_core_config,
                    prefix_bytes=source_prefix_bytes,
                    prefix_sha256=source_prefix_sha256,
                    selected=a1_prefix_selected, manifest=manifest,
                    lookup_hashes=lookup_hashes)
            _require(source_oracle.final_request == request
                     and source_oracle.progress == json.loads(manifest["source_progress_json"],
                                                                object_pairs_hook=self._unique_pairs)
                     and source_oracle.destination == {"catalog": self.config.catalog,
                                                        "schema": self.config.schema},
                     "Rebuilt native-hash oracle differs from reopened source/manifest anchors")
        if source_oracle is not None:
            bag_comparison_sha256, bag_comparison_summary = self._read_and_compare_source_bags(
                source_oracle, request, manifest, versions, role_tables)
        from .truss_feed_sessions import encode_truss_destination_receipt
        source_receipt = encode_truss_destination_receipt(request_bytes, manifest_bytes, vector)
        installation = {role: {k: v for k, v in item.items() if k != "detailSha256"}
                        for role, item in self.opening.items()}
        observation = ManagedCurrentSparkReceiptReopenObservation(PROFILE, self._publication_id,
            manifest, expected_receipt["manifestSha256"], source_receipt,
            hashlib.sha256(source_receipt).hexdigest(), vector, installation, {},
            {"opening": self.opening_runtime, "closing": None}, attempt_custody,
            source_oracle.prefix_sha256 if source_oracle is not None else None,
            bag_comparison_sha256, bag_comparison_summary)
        self._observation = observation
        self._reopen_inputs = {
            "request_bytes": request_bytes,
            "manifest_bytes": manifest_bytes,
            "expected_receipt": expected_receipt,
            "source_oracle": source_oracle,
            "source_prefix_bytes": source_prefix_bytes,
            "source_prefix_sha256": source_prefix_sha256,
            "source_core_config": source_core_config,
            "a1_plan": a1_plan,
            "a1_prefix_selected": a1_prefix_selected,
        }
        self._closing_witness = {
            "request": request,
            "manifest": manifest,
            "expected_receipt": expected_receipt,
            "source_oracle": source_oracle,
            "versions": versions,
            "role_tables": role_tables,
            "uuids": uuids,
        }
        return observation

    def _close_resolver_and_data_witness(self):
        """Reopen the exact manifest/vector and source-derived pinned bags."""
        selected = self._closing_witness
        _require(type(selected) is dict,
                 "Exact original receipt reopen required before closing")
        request, manifest = selected["request"], selected["manifest"]
        quoted_manifest = quote_table_identifier(".".join((self.config.catalog,
            self.config.schema, "publication_manifest")))
        rows = self.executor.query(
            "SELECT publication_id,profile_version,table_versions_json,source_progress_json,"
            "schema_revisions_json,validation_report_json,"
            "CAST(unix_micros(recorded_at) AS STRING) AS recorded_at FROM " + quoted_manifest
            + " WHERE publication_id=:publication_id",
            {"publication_id": manifest["publication_id"]}).rows
        _require(len(rows) == 1 and dict(rows[0]) == manifest,
                 "Exact task manifest changed at closing")
        versions, role_tables, uuids = (selected["versions"], selected["role_tables"],
                                        selected["uuids"])
        from ashlar.publication import descriptor_from_manifest_row
        descriptor = descriptor_from_manifest_row(dict(rows[0]))
        vector = manifest_pin_vector(manifest, uuids,
            authority="managed-current-spark:" + self.config.expected_principal)
        from .truss_feed_sessions import encode_truss_destination_receipt
        receipt_bytes = encode_truss_destination_receipt(
            self._reopen_inputs["request_bytes"], self._reopen_inputs["manifest_bytes"], vector)
        _require(vector == self._observation.vector
                 and receipt_bytes == self._observation.source_receipt_bytes
                 and selected["expected_receipt"].get("dataRoleVector") == {
                     table: {"uuid": uuids[table], "version": versions[table]}
                     for table in role_tables},
                 "Exact four-role publication vector changed at closing")
        revisions = json.loads(request["schema_revisions_json"],
                               object_pairs_hook=self._unique_pairs)
        native_types = {"STRING": "string", "BIGINT": "long", "BOOLEAN": "boolean",
                        "TIMESTAMP": "timestamp"}
        schemas = {table: tuple((name, native_types[kind])
                                for name, kind, _nullable in self.carriers.ordered_fields(role))
                   for table, role in role_tables.items()}
        backend = _SparkReceiptBackend(self, _SparkReceiptPolicy(self, descriptor), schemas)
        resolved = resolve_publication(backend, manifest["publication_id"], uuids,
            context=self.context, supported_profiles=(self.config.carrier_profile,),
            supported_revisions={key: (value,) for key, value in revisions.items()})
        _require(dict(resolved.descriptor.raw) == manifest
                 and all(snapshot.uuid == uuids[table] and snapshot.version == versions[table]
                         for table, snapshot in resolved.snapshots.items()),
                 "Closing native resolver differs from exact four-role publication")
        oracle = selected["source_oracle"]
        if oracle is not None:
            if self.config.operational_observation_stage:
                _require(self._observation.bag_comparison_sha256 is not None
                         and type(self._observation.bag_comparison_summary) is dict,
                         "Exact source-derived bag comparison required before the operational close bookend")
            else:
                comparison, summary = self._read_and_compare_source_bags(
                    oracle, request, manifest, versions, role_tables)
                _require(comparison == self._observation.bag_comparison_sha256
                         and summary == self._observation.bag_comparison_summary,
                         "Closing source-derived data witness changed")

    def _close_serverless(self):
        _require(self._serverless_tag is not None and not self._serverless_tag.closed,
                 "Exact live Spark Connect close tag required")
        self.closing = True
        timeout_fired = threading.Event()
        cancellation_thread_error = []
        close_started = time.monotonic()
        close_timeout_seconds = 60 if self.config.operational_observation_stage else 20
        self._serverless_close_deadline = min(
            close_started + close_timeout_seconds,
            self.started + self.config.maximum_elapsed_seconds,
            self._serverless_task_deadline if self._serverless_task_deadline is not None
                else close_started + close_timeout_seconds)
        close_budget = self._serverless_close_deadline - close_started
        _require(close_budget > 0,
                 "Spark Connect close deadline expired before tagged cleanup")

        def cancel_close():
            timeout_fired.set()
            try:
                self._serverless_tag.interrupt_tagged_operations()
            except BaseException as error:
                cancellation_thread_error.append(error)

        timer = threading.Timer(close_budget, cancel_close)
        timer.daemon = True
        runtime = inventory = None
        primary = None
        try:
            timer.start()
            original = self._observation
            if original is None or self._reopen_inputs is None:
                self._renew()
                _require(not timeout_fired.is_set()
                         and time.monotonic() < self._serverless_close_deadline,
                         "Spark Connect close deadline expired; tagged interrupt requested and completion unknown")
                _require(False, "Exact original receipt reopen required before closing")
            self._closing_bookend_complete = True
            self._close_resolver_and_data_witness()
            self._closing_bookend_complete = False
            _require(not timeout_fired.is_set()
                     and time.monotonic() < self._serverless_close_deadline,
                     "Spark Connect close deadline expired; tagged interrupt requested and completion unknown")
            runtime, inventory = self._renew()
            _require(not timeout_fired.is_set()
                     and time.monotonic() < self._serverless_close_deadline,
                     "Spark Connect close deadline expired; tagged interrupt requested and completion unknown")
        except BaseException as error:
            primary = error
        finally:
            self._closing_bookend_complete = False
            timer.cancel()
            timer.join(min(2, max(0, self._serverless_close_deadline - time.monotonic())))
            if timer.is_alive() and primary is None:
                primary = ValueError(
                    "Spark Connect tagged interrupt call did not return; cancellation outcome unknown")
            try:
                self._serverless_tag.close()
            except BaseException as error:
                if primary is None:
                    primary = error
                elif callable(getattr(primary, "add_note", None)):
                    primary.add_note("Spark Connect operation-tag cleanup also failed: "
                                     + type(error).__name__)
        if timeout_fired.is_set():
            operation_ids = self._serverless_tag.interrupt_ids
            detail = ("response IDs observed" if operation_ids is not None
                      else "response unavailable")
            error = ValueError(
                "Spark Connect close timed out; interruptTag was requested (" + detail
                + "), but operation termination was not established")
            if primary is not None and callable(getattr(primary, "add_note", None)):
                error.add_note("Close operation also failed: " + type(primary).__name__)
            primary = error
        elif cancellation_thread_error and primary is None:
            primary = ValueError("Spark Connect tagged interrupt returned no qualified response")
            if callable(getattr(primary, "add_note", None)):
                primary.add_note("Interrupt API error: " + type(cancellation_thread_error[0]).__name__)
        if primary is not None:
            raise primary
        if self._observation is not None:
            self._observation.runtime_identity["closing"] = runtime
            self._observation.closing_installation_inventory.clear()
            self._observation.closing_installation_inventory.update({
                role: {k: v for k, v in item.items() if k != "detailSha256"}
                for role, item in inventory.items()})
        self.closed = True

    def close(self):
        _require(not self.closed, "Spark receipt reader already closed")
        if self.config.execution_profile == "serverless_connect":
            return self._close_serverless()
        if (type(self.config) is ManagedCurrentSparkClassicTaskReceiptConfig
                and self._operation_runner is not None):
            return self._close_classic_task()
        self.closing = True
        sc = getattr(self.spark, "sparkContext", None)
        group = "managed-current-receipt-close-" + uuid.uuid4().hex
        keys = ("spark.jobGroup.id", "spark.job.description", "spark.job.interruptOnCancel")
        previous = {key: sc.getLocalProperty(key) for key in keys} if sc is not None else {}
        timeout_fired = threading.Event()
        close_started = time.monotonic()
        def cancel_close():
            timeout_fired.set()
            sc.cancelJobGroup(group)
        timer = threading.Timer(30, cancel_close)
        try:
            sc.setJobGroup(group, "bounded managed current receipt close", interruptOnCancel=True)
            timer.daemon = True
            timer.start()
            original = self._observation
            if original is None or self._reopen_inputs is None:
                self._renew()
                _require(not timeout_fired.is_set() and time.monotonic() - close_started < 30,
                         "Spark closing deadline expired")
                _require(False, "Exact original receipt reopen required before closing")
            self._closing_bookend_complete = True
            self._close_resolver_and_data_witness()
            self._closing_bookend_complete = False
            runtime, inventory = self._renew()
            _require(not timeout_fired.is_set() and time.monotonic() - close_started < 30,
                     "Spark closing deadline expired")
        finally:
            self._closing_bookend_complete = False
            timer.cancel()
            for key, value in previous.items():
                sc.setLocalProperty(key, value)
        if self._observation is not None:
            self._observation.runtime_identity["closing"] = runtime
            self._observation.closing_installation_inventory.clear()
            self._observation.closing_installation_inventory.update({
                role: {k: v for k, v in item.items() if k != "detailSha256"}
                for role, item in inventory.items()})
        self.closed = True

    def _close_classic_task(self):
        """Close through the actual operation wrapper with a matching 30s bound."""
        self.closing = True
        close_deadline = min(time.monotonic() + 30,
            self.started + self.config.maximum_elapsed_seconds,
            self._serverless_task_deadline if self._serverless_task_deadline is not None
            else float("inf"))
        _require(close_deadline > time.monotonic(),
                 "Classic receipt task deadline expired before closing")
        self._classic_close_deadline = close_deadline
        close_started = time.monotonic()
        try:
            original = self._observation
            if original is None or self._reopen_inputs is None:
                self._renew()
                _require(time.monotonic() < close_deadline,
                         "Classic Spark close deadline expired")
                _require(False, "Exact original receipt reopen required before closing")
            self._closing_bookend_complete = True
            self._close_resolver_and_data_witness()
            self._closing_bookend_complete = False
            runtime, inventory = self._renew()
            _require(time.monotonic() < close_deadline
                     and time.monotonic() - close_started < 30,
                     "Classic Spark close deadline expired")
        finally:
            self._closing_bookend_complete = False
            self._classic_close_deadline = None
        if self._observation is not None:
            self._observation.runtime_identity["closing"] = runtime
            self._observation.closing_installation_inventory.clear()
            self._observation.closing_installation_inventory.update({
                role: {k: v for k, v in item.items() if k != "detailSha256"}
                for role, item in inventory.items()})
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        from .lifecycle import finish
        finish(exc, [self.close])
        return False


class ManagedCurrentSparkServerlessReceiptReader(ManagedCurrentSparkReceiptReader):
    """Explicit Spark Connect reader using per-session tags, never SparkContext."""

    def __init__(self, spark, config: ManagedCurrentSparkServerlessReceiptConfig, *,
                 operation_deadline_monotonic=None):
        if type(config) is not ManagedCurrentSparkServerlessReceiptConfig:
            raise ValueError("Explicit serverless Spark Connect receipt config required")
        super().__init__(spark, config,
                         operation_deadline_monotonic=operation_deadline_monotonic)


class _SparkReceiptPolicy:
    def __init__(self, owner, descriptor):
        self.owner, self.descriptor = owner, descriptor

    def authorize(self, context, publication_id, tables):
        _require(context is self.owner.context and publication_id == self.owner._publication_id
                 and set(tables) == {".".join((self.owner.config.catalog,
                     self.owner.config.schema, role)) for role in DATA_ROLES})
        self.owner._renew()

    def validate_descriptor(self, descriptor, context):
        from ashlar.source_checkpoint import bind_source_descriptor
        _require(context is self.owner.context and dict(descriptor.raw) == self.owner._manifest
                 and descriptor.publication_id == self.owner._publication_id)
        bind_source_descriptor(self.owner._request, descriptor,
                               expected_publication_id=self.owner._publication_id)

    def validate_snapshot(self, table, table_uuid, version, columns):
        role = table.rsplit(".", 1)[-1]
        expected = self.owner.opening[role]
        native = {"STRING": "string", "BIGINT": "long", "BOOLEAN": "boolean",
                  "TIMESTAMP": "timestamp"}
        _require(expected["uuid"] == table_uuid and type(version) is int
                 and 0 <= version <= expected["version"]
                 and tuple(columns) == tuple((n, native[t]) for n, t, _ in
                     self.owner.carriers.ordered_fields(role)))


class _SparkReceiptBackend:
    def __init__(self, owner, policy, schemas):
        self.owner, self.policy = owner, policy
        self.manifest_table = ".".join((owner.config.catalog, owner.config.schema, "publication_manifest"))
        self.manifest_uuid = owner.opening["publication_manifest"]["uuid"]
        self.schemas = schemas

    def authorize(self, context, publication_id, tables):
        return self.policy.authorize(context, publication_id, tables)

    def descriptors(self, publication_id):
        self.owner._renew()
        return self.owner.executor.query("SELECT publication_id,profile_version,table_versions_json,"
            "source_progress_json,schema_revisions_json,validation_report_json,"
            "CAST(unix_micros(recorded_at) AS STRING) AS recorded_at FROM "
            + quote_table_identifier(self.manifest_table)
            + " WHERE publication_id=:publication_id", {"publication_id": publication_id}).rows

    def validate_descriptor(self, descriptor, context):
        return self.policy.validate_descriptor(descriptor, context)

    def inspect_snapshot(self, table, version):
        role = table.rsplit(".", 1)[-1]
        expected = self.owner.opening.get(role)
        _require(expected is not None and table == ".".join(expected["name"])
                 and type(version) is int and 0 <= version <= expected["version"]
                 and table in self.schemas)
        before = self.owner._native_identity(role)
        _require(before == (expected["uuid"], expected["protocol"]),
                 "Pinned native role identity changed before snapshot inspection")
        result = self.owner.executor.query("SELECT * FROM " + quote_table_identifier(table)
            + " VERSION AS OF " + str(version) + " LIMIT 0", {})
        _require(not result.rows and tuple(result.columns) == self.schemas[table])
        self.policy.validate_snapshot(table, expected["uuid"], version, result.columns)
        after = self.owner._native_identity(role)
        _require(after == before, "Pinned native role identity changed during snapshot inspection")
        return Snapshot(table, expected["uuid"], version)
