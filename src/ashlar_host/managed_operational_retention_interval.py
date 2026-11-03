# SPDX-License-Identifier: MIT OR Apache-2.0
"""Read-only, three-observation UC managed publication interval.

The owner uses a caller-owned session factory and independent source/policy
observers. It has no publication, pin-registration, protected ACK, or source
cursor mutation capability. The injected adapters remain authority boundaries;
protocol fakes do not qualify them.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import copy
import hashlib
import json
import re
import time
import uuid
from types import MappingProxyType
from typing import Protocol

from ashlar_host.managed_current_core import (
    CARRIER_PROFILE_XXHASH64, ManagedCurrentCoreConfig,
)
from ashlar_host.managed_current_spark_receipt_read import (
    ManagedCurrentSparkReceiptConfig,
    ManagedCurrentSparkReceiptReader,
    ManagedCurrentSparkServerlessReceiptConfig,
    ManagedCurrentSparkServerlessReceiptReader,
    ManagedCurrentSparkReceiptReopenObservation,
    BorrowedServerlessTaskSession,
    SERVERLESS_TASK_SESSION_OWNERSHIP,
)
from ashlar.pins import PinVector


PROFILE = "ashlar-managed-operational-retention/0.1"
DATA_ROLES = ("object_current", "edge_current", "tombstone", "whole_source_history")
SOURCE_CUSTODY_FIELDS = (
    "source_registration", "consumer_registration", "cursor",
    "destination_registration", "ack_scope", "checkpoint",
    "current_page", "retained_page",
)
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MAX_INTERVAL_SECONDS = 900
_MIN_RETENTION_SECONDS = 7 * 24 * 60 * 60


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


class OperationalRetentionIntervalError(ValueError):
    """A bounded managed-read observation refused or did not complete."""


class OwnedSparkSession(Protocol):
    """One fresh host-owned Spark session; close releases this session."""

    spark: object
    session_id: str

    def close(self) -> object: ...


class OwnedSparkSessionFactory(Protocol):
    def open(self, *, stage: str, deadline_monotonic: float) -> OwnedSparkSession: ...


class OperationalSourceCustodyObserver(Protocol):
    def observe(self, *, stage: str, selection: "ManagedOperationalRetentionSelection",
                vector: PinVector) -> dict: ...


class OperationalPolicyObserver(Protocol):
    def observe(self, *, stage: str, session: OwnedSparkSession,
                vector: PinVector) -> dict: ...


@dataclass(frozen=True)
class ManagedOperationalRetentionSelection:
    """Original inputs independently selected by the source/receipt owner."""

    reader_config: ManagedCurrentSparkReceiptConfig = field(repr=False, compare=False)
    request_bytes: bytes = field(repr=False)
    manifest_bytes: bytes = field(repr=False)
    receipt_bytes: bytes = field(repr=False)
    expected_receipt: dict = field(repr=False, compare=False)
    vector: PinVector = field(repr=False, compare=False)
    expected_source_custody: dict = field(repr=False, compare=False)
    source_oracle: object | None = field(default=None, repr=False, compare=False)
    source_prefix_bytes: bytes | None = field(default=None, repr=False)
    source_prefix_sha256: str | None = None
    source_core_config: object | None = field(default=None, repr=False, compare=False)
    a1_plan: object | None = field(default=None, repr=False, compare=False)
    a1_prefix_selected: dict | None = field(default=None, repr=False, compare=False)
    _expected_receipt_bytes: bytes = field(init=False, repr=False, compare=False)
    _a1_prefix_selected_bytes: bytes | None = field(init=False, repr=False, compare=False)

    def __post_init__(self):
        if type(self.reader_config) not in (ManagedCurrentSparkReceiptConfig,
                                            ManagedCurrentSparkServerlessReceiptConfig):
            raise OperationalRetentionIntervalError("Explicit managed Spark receipt configuration required")
        if (type(self.request_bytes) is not bytes or not 0 < len(self.request_bytes) <= 4 * 1024 * 1024
                or type(self.manifest_bytes) is not bytes
                or not 0 < len(self.manifest_bytes) <= 4 * 1024 * 1024
                or type(self.receipt_bytes) is not bytes
                or not 0 < len(self.receipt_bytes) <= 4 * 1024 * 1024
                or type(self.expected_receipt) is not dict
                or type(self.vector) is not PinVector):
            raise OperationalRetentionIntervalError("Frozen original request, manifest, exact receipt bytes and vector required")
        expected_tables = {
            f"{self.reader_config.catalog}.{self.reader_config.schema}.{role}"
            for role in DATA_ROLES
        }
        if set(self.vector.targets) != expected_tables:
            raise OperationalRetentionIntervalError("Complete original four-role vector required")
        if (type(self.expected_source_custody) is not dict
                or set(self.expected_source_custody) != set(SOURCE_CUSTODY_FIELDS)
                or any(type(v) is not str or not _SHA256.fullmatch(v)
                       for v in self.expected_source_custody.values())):
            raise OperationalRetentionIntervalError("Exact selected source-custody digests required")
        object.__setattr__(self, "expected_source_custody",
                           MappingProxyType(dict(self.expected_source_custody)))
        try:
            receipt_bytes = json.dumps(self.expected_receipt, ensure_ascii=False,
                sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
        except (TypeError, ValueError) as error:
            raise OperationalRetentionIntervalError("Closed finite task receipt required") from error
        object.__setattr__(self, "_expected_receipt_bytes", receipt_bytes)
        object.__setattr__(self, "_a1_prefix_selected_bytes", None)
        def unique_members(items):
            parsed = {}
            for key, value in items:
                if key in parsed:
                    raise ValueError("duplicate receipt member")
                parsed[key] = value
            return parsed
        try:
            receipt_text = self.receipt_bytes.decode("utf-8")
            if json.loads(receipt_text, object_pairs_hook=unique_members) != self.expected_receipt:
                raise ValueError()
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
            raise OperationalRetentionIntervalError(
                "Exact selected receipt bytes must decode to the retained receipt") from error
        if (self.reader_config.carrier_profile != CARRIER_PROFILE_XXHASH64
                or self.source_oracle is not None
                or type(self.source_prefix_bytes) is not bytes
                or not 0 < len(self.source_prefix_bytes) <= 4 * 1024 * 1024
                or type(self.source_prefix_sha256) is not str
                or not _SHA256.fullmatch(self.source_prefix_sha256)
                or hashlib.sha256(self.source_prefix_bytes).hexdigest() != self.source_prefix_sha256
                or type(self.source_core_config) is not ManagedCurrentCoreConfig
                or self.source_core_config.carrier_profile != CARRIER_PROFILE_XXHASH64
                or (self.source_core_config.catalog, self.source_core_config.schema)
                   != (self.reader_config.catalog, self.reader_config.schema)):
            raise OperationalRetentionIntervalError(
                "The .4 interval requires matching frozen prefix bytes, digest and native-probe core config")
        if self.a1_plan is not None:
            from .managed_current_a1_trial_plan import ManagedCurrentA1TrialPlan
            if (type(self.a1_plan) is not ManagedCurrentA1TrialPlan
                    or self.reader_config.source_truss_version not in (
                        "0.0.52", "0.0.53", "0.0.54", "0.0.55")
                    or type(self.a1_prefix_selected) is not dict):
                raise OperationalRetentionIntervalError(
                    "A1 operational read requires an admitted `.52`-`.55` plan and selected `/0.2` custody")
            try:
                from ashlar.schema import decode_retained_json
                request = decode_retained_json(self.request_bytes)
                manifest = decode_retained_json(self.manifest_bytes)
                selected_bytes = _canonical(self.a1_prefix_selected).encode("utf-8")
                if not 0 < len(selected_bytes) <= 4 * 1024 * 1024:
                    raise ValueError("A1 selected custody exceeds the bounded carrier")
                object.__setattr__(self, "_a1_prefix_selected_bytes", selected_bytes)
                if (request != self.a1_plan.request
                        or _canonical(request).encode("utf-8") != self.request_bytes
                        or manifest != self.expected_receipt.get("manifest")
                        or _canonical(manifest).encode("utf-8") != self.manifest_bytes
                        or request.get("request_digest") != self.expected_receipt.get("requestDigest")
                        or manifest.get("publication_id") != self.expected_receipt.get("publicationId")):
                    raise ValueError("A1 receipt anchors differ")
                self.a1_plan.verify_delivery(
                    self.source_prefix_bytes,
                    prefix_sha256=self.source_prefix_sha256,
                    selected=decode_retained_json(selected_bytes),
                    schedule_bytes=self.a1_plan.selection.schedule_bytes,
                    pages=(self.a1_plan.delivery_page,))
            except (AttributeError, TypeError, ValueError) as error:
                raise OperationalRetentionIntervalError(
                    "Exact A1 `/0.2` page, request and destination receipt correspondence required") from error
        elif self.a1_prefix_selected is not None:
            raise OperationalRetentionIntervalError(
                "A1 `/0.2` anchors require the exact prepared A1 plan")

    def selected_a1_prefix(self):
        """Decode a fresh copy of the A1 anchors retained at admission time."""
        if self._a1_prefix_selected_bytes is None:
            return None
        from ashlar.schema import decode_retained_json
        return decode_retained_json(self._a1_prefix_selected_bytes)


@dataclass(frozen=True)
class ManagedOperationalObservation:
    profile: str
    interval_id: str
    stage: str
    observed_at_utc: str
    elapsed_seconds: float
    vector_targets: dict
    runtime_identity: dict = field(repr=False)
    source_custody_digests: dict
    retention_settings: dict
    predictive_optimization: str
    maintenance_inventory_sha256: str
    row_count: int
    normalized_bytes: int
    role_comparisons: dict
    comparison_sha256: str
    session_ownership_profile: str = "host_owned_fresh_spark_session/0.1"
    task_session_scope_id: str = ""
    session_wrapper_id: str = ""


@dataclass(frozen=True)
class ManagedOperationalIntervalReceipt:
    profile: str
    interval_id: str
    outcome: str
    authority: str
    maximum_elapsed_seconds: int
    observations: tuple[ManagedOperationalObservation, ...]


def _sha(value, label):
    if type(value) is not str or not _SHA256.fullmatch(value):
        raise OperationalRetentionIntervalError(f"Canonical {label} SHA-256 required")
    return value


def _policy_observation(value):
    if (type(value) is not dict
            or set(value) != {"retention_settings", "predictive_optimization",
                              "maintenance_inventory_sha256"}
            or type(value["retention_settings"]) is not dict
            or set(value["retention_settings"]) != set(DATA_ROLES)
            or value["predictive_optimization"] != "ENABLE"):
        raise OperationalRetentionIntervalError("Complete managed retention and PO observation required")
    settings = {}
    for role, item in value["retention_settings"].items():
        if (type(item) is not dict or set(item) != {"data_retention_seconds", "log_retention_seconds"}
                or any(type(seconds) is not int or seconds < _MIN_RETENTION_SECONDS
                       for seconds in item.values())):
            raise OperationalRetentionIntervalError(
                f"At least seven days of data and log retention required for {role}")
        settings[role] = dict(item)
    return {
        "retention_settings": settings,
        "predictive_optimization": "ENABLE",
        "maintenance_inventory_sha256": _sha(
            value["maintenance_inventory_sha256"], "maintenance inventory"),
    }


def _runtime_identity(observation):
    if type(observation.runtime_identity) is not dict:
        raise OperationalRetentionIntervalError("Native Spark runtime identity record required")
    value = observation.runtime_identity.get("opening")
    if type(value) is not dict:
        raise OperationalRetentionIntervalError("Native Spark opening runtime identity required")
    if value.get("sessionTimeZone") not in ("UTC", "Etc/UTC"):
        raise OperationalRetentionIntervalError("Explicit UTC Spark session timezone required")
    return copy.deepcopy(value)


def _stable_runtime(value):
    return {key: item for key, item in value.items() if key != "sparkApplicationId"}


class OperationalRetentionInterval:
    """Read exact four-role bags at open, pre-ACK and close; never ACKs."""

    def __init__(self, selection, session_factory, source_observer, policy_observer,
                 *, clock=time.monotonic):
        if (type(selection) is not ManagedOperationalRetentionSelection
                or not callable(getattr(session_factory, "open", None))
                or not callable(getattr(source_observer, "observe", None))
                or not callable(getattr(policy_observer, "observe", None))):
            raise OperationalRetentionIntervalError("Owned Spark, source and policy observers required")
        self.selection = selection
        self.session_factory = session_factory
        self.source_observer = source_observer
        self.policy_observer = policy_observer
        self.clock = clock
        self.started = clock()
        self.deadline = self.started + _MAX_INTERVAL_SECONDS
        self.interval_id = str(uuid.uuid4())
        self.observations = []
        self._session_ids = set()
        self._spark_sessions = []
        self._session_wrappers = []
        self._last_session_ownership_profile = None
        self._last_task_session_scope_id = None
        self._last_session_wrapper_id = None
        self._policy_baseline = None
        self._runtime_baseline = None
        self._invalid = False
        self._closed = False

    @classmethod
    def open(cls, selection, session_factory, source_observer, policy_observer,
             *, clock=time.monotonic):
        interval = cls(selection, session_factory, source_observer, policy_observer, clock=clock)
        interval._capture("open")
        return interval

    @classmethod
    def observe_close_only(cls, selection, session_factory, source_observer,
                           policy_observer, *, clock=time.monotonic):
        """Make one fresh post-ACK close observation, without claiming an interval.

        This path is for the terminal close after a separate three-point
        pre-ACK interval and matched source ACK. It deliberately returns one
        ``close`` observation, never an opening/pre-ACK/close interval receipt.
        """
        interval = cls(selection, session_factory, source_observer, policy_observer,
                       clock=clock)
        observation = interval._capture("close", closing=True)
        interval._closed = True
        return ManagedOperationalIntervalReceipt(
            profile=PROFILE,
            interval_id=interval.interval_id,
            outcome="observed_available_at_close",
            authority="read_observation_only_no_source_ack",
            maximum_elapsed_seconds=_MAX_INTERVAL_SECONDS,
            observations=(observation,),
        )

    def _require_active(self, *, closing=False):
        if self._closed or (self._invalid and not closing):
            raise OperationalRetentionIntervalError("Operational interval is closed or invalidated")
        if self.clock() >= self.deadline and not closing:
            self._invalid = True
            raise OperationalRetentionIntervalError("Operational interval exceeded 900 seconds")

    def _source_custody(self, stage):
        observed = self.source_observer.observe(
            stage=stage, selection=self.selection, vector=self.selection.vector)
        if (type(observed) is not dict or set(observed) != set(SOURCE_CUSTODY_FIELDS)
                or any(type(v) is not str or not _SHA256.fullmatch(v)
                       for v in observed.values())
                or dict(observed) != dict(self.selection.expected_source_custody)):
            raise OperationalRetentionIntervalError(
                "Original source registration, cursor, page, checkpoint or ACK-scope custody changed")
        return dict(observed)

    def _reader_observation(self, stage, *, closing=False):
        session = self.session_factory.open(stage=stage, deadline_monotonic=self.deadline)
        reader = None
        primary = None
        primary_tb = None
        result = None
        policy_value = None
        close_error = None
        try:
            if self.clock() >= self.deadline and not closing:
                raise OperationalRetentionIntervalError("Operational interval exceeded 900 seconds")
            serverless_borrowed = (
                type(self.selection.reader_config) is ManagedCurrentSparkServerlessReceiptConfig
                and type(session) is BorrowedServerlessTaskSession)
            if (session is None or type(getattr(session, "session_id", None)) is not str
                    or not session.session_id or session.session_id in self._session_ids
                    or getattr(session, "spark", None) is None
                    or (type(self.selection.reader_config) is ManagedCurrentSparkServerlessReceiptConfig
                        and not serverless_borrowed)
                    or (type(self.selection.reader_config) is ManagedCurrentSparkReceiptConfig
                        and type(session) is BorrowedServerlessTaskSession)
                    or (serverless_borrowed and self._session_wrappers
                        and any(type(previous) is not BorrowedServerlessTaskSession
                                or previous.task_session_scope_id != session.task_session_scope_id
                                or previous.spark is not session.spark
                                for previous in self._session_wrappers))
                    or (any(session.spark is prior for prior in self._spark_sessions)
                        and (not serverless_borrowed
                             or any(type(previous) is not BorrowedServerlessTaskSession
                                    or previous.task_session_scope_id != session.task_session_scope_id
                                    for previous in self._session_wrappers)))
                    or not callable(getattr(session, "close", None))):
                raise OperationalRetentionIntervalError(
                    "Fresh owned session or exact task-scoped serverless borrowed wrapper required")
            self._session_ids.add(session.session_id)
            self._spark_sessions.append(session.spark)
            if not hasattr(self, "_session_wrappers"):
                self._session_wrappers = []
            self._session_wrappers.append(session)
            self._last_session_ownership_profile = (
                SERVERLESS_TASK_SESSION_OWNERSHIP if serverless_borrowed
                else "host_owned_fresh_spark_session/0.1")
            self._last_task_session_scope_id = (
                session.task_session_scope_id if serverless_borrowed else session.session_id)
            self._last_session_wrapper_id = session.session_id
            if type(self.selection.reader_config) is ManagedCurrentSparkServerlessReceiptConfig:
                reader = ManagedCurrentSparkServerlessReceiptReader(
                    session.spark, self.selection.reader_config,
                    operation_deadline_monotonic=(session.deadline_monotonic
                        if serverless_borrowed else None))
            else:
                reader = ManagedCurrentSparkReceiptReader(session.spark, self.selection.reader_config)
            kwargs = {
                "request_bytes": self.selection.request_bytes,
                "manifest_bytes": self.selection.manifest_bytes,
                "expected_receipt": json.loads(self.selection._expected_receipt_bytes),
            }
            if self.selection.reader_config.carrier_profile == CARRIER_PROFILE_XXHASH64:
                kwargs.update(source_prefix_bytes=self.selection.source_prefix_bytes,
                              source_prefix_sha256=self.selection.source_prefix_sha256,
                              source_core_config=self.selection.source_core_config)
                if self.selection.a1_plan is not None:
                    kwargs.update(a1_plan=self.selection.a1_plan,
                                  a1_prefix_selected=self.selection.selected_a1_prefix())
            result = reader.reopen(**kwargs)
            self._validate_reader_result(result)
            if self.clock() >= self.deadline and not closing:
                raise OperationalRetentionIntervalError("Operational interval exceeded 900 seconds")
            policy_value = self.policy_observer.observe(
                stage=stage, session=session, vector=self.selection.vector)
        except BaseException as error:
            primary, primary_tb = error, error.__traceback__
        finally:
            for closer in (
                (reader.close if reader is not None else None),
                (getattr(session, "close", None) if session is not None else None),
            ):
                if not callable(closer):
                    continue
                try:
                    closer()
                except BaseException as error:
                    if close_error is None:
                        close_error = error
        if primary is not None:
            if close_error is not None and callable(getattr(primary, "add_note", None)):
                primary.add_note("Operational interval session close also failed: "
                                 + type(close_error).__name__)
            raise primary.with_traceback(primary_tb)
        if close_error is not None:
            raise OperationalRetentionIntervalError(
                "Managed receipt reader or owned Spark session failed to close") from close_error
        return result, policy_value

    def _validate_reader_result(self, result):
        if (type(result) is not ManagedCurrentSparkReceiptReopenObservation
                or result.bag_comparison_sha256 is None
                or type(result.bag_comparison_summary) is not dict
                or result.vector != self.selection.vector
                or result.manifest_sha256 != json.loads(
                    self.selection._expected_receipt_bytes).get("manifestSha256")):
            raise OperationalRetentionIntervalError(
                "Fresh complete source-derived four-role comparison and exact vector required")
        if (self.selection.source_prefix_sha256 is not None
                and result.source_prefix_sha256 != self.selection.source_prefix_sha256):
            raise OperationalRetentionIntervalError("Frozen source-prefix digest changed")
        summary = result.bag_comparison_summary
        if (set(summary) != {"roles", "row_count", "normalized_bytes", "comparison_sha256"}
                or type(summary["roles"]) is not dict
                or set(summary["roles"]) != set(DATA_ROLES)
                or type(summary["row_count"]) is not int
                or not 0 <= summary["row_count"] <= 1024
                or type(summary["normalized_bytes"]) is not int
                or not 0 <= summary["normalized_bytes"] <= 4 * 1024 * 1024
                or summary["comparison_sha256"] != result.bag_comparison_sha256
                or type(summary["comparison_sha256"]) is not str
                or not _SHA256.fullmatch(summary["comparison_sha256"])):
            raise OperationalRetentionIntervalError("Complete bounded four-role comparison summary required")
        row_count = normalized_bytes = 0
        for role, role_summary in summary["roles"].items():
            if (type(role_summary) is not dict
                    or set(role_summary) != {"row_count", "normalized_bytes", "comparison_sha256"}
                    or type(role_summary["row_count"]) is not int
                    or not 0 <= role_summary["row_count"] <= 1024
                    or type(role_summary["normalized_bytes"]) is not int
                    or not 0 <= role_summary["normalized_bytes"] <= 4 * 1024 * 1024
                    or type(role_summary["comparison_sha256"]) is not str
                    or not _SHA256.fullmatch(role_summary["comparison_sha256"])):
                raise OperationalRetentionIntervalError(
                    f"Complete bounded {role} comparison summary required")
            row_count += role_summary["row_count"]
            normalized_bytes += role_summary["normalized_bytes"]
        if (row_count != summary["row_count"]
                or normalized_bytes != summary["normalized_bytes"]):
            raise OperationalRetentionIntervalError("Role comparison summaries do not match aggregate bounds")

    def _capture(self, stage, *, closing=False):
        self._require_active(closing=closing)
        try:
            result, policy_value = self._reader_observation(stage, closing=closing)
            policy = _policy_observation(policy_value)
            source = self._source_custody(stage)
            runtime = _runtime_identity(result)
            if self._runtime_baseline is None:
                self._runtime_baseline = _stable_runtime(runtime)
            elif _stable_runtime(runtime) != self._runtime_baseline:
                raise OperationalRetentionIntervalError("Managed runtime or principal identity drifted")
            if self._policy_baseline is None:
                self._policy_baseline = policy
            elif policy != self._policy_baseline:
                raise OperationalRetentionIntervalError(
                    "Retention, predictive-optimization or maintenance inventory changed")
            summary = result.bag_comparison_summary
            observation = ManagedOperationalObservation(
                profile=PROFILE,
                interval_id=self.interval_id,
                stage=stage,
                observed_at_utc=datetime.now(timezone.utc).isoformat(timespec="microseconds"),
                elapsed_seconds=self.clock() - self.started,
                vector_targets={
                    table: {"uuid": target[0], "version": target[1]}
                    for table, target in sorted(self.selection.vector.targets.items())
                },
                runtime_identity=runtime,
                source_custody_digests=source,
                retention_settings=copy.deepcopy(policy["retention_settings"]),
                predictive_optimization=policy["predictive_optimization"],
                maintenance_inventory_sha256=policy["maintenance_inventory_sha256"],
                row_count=summary["row_count"],
                normalized_bytes=summary["normalized_bytes"],
                role_comparisons=copy.deepcopy(summary["roles"]),
                comparison_sha256=summary["comparison_sha256"],
                session_ownership_profile=self._last_session_ownership_profile,
                task_session_scope_id=self._last_task_session_scope_id,
                session_wrapper_id=self._last_session_wrapper_id,
            )
            if self.clock() >= self.deadline and not closing:
                raise OperationalRetentionIntervalError("Operational interval exceeded 900 seconds")
            self.observations.append(observation)
            return observation
        except BaseException:
            self._invalid = True
            raise

    def renew_pre_ack(self):
        self._require_active()
        if len(self.observations) != 1:
            self._invalid = True
            raise OperationalRetentionIntervalError("Exactly one opening observation required")
        return self._capture("pre_ack")

    def close(self):
        if self._closed:
            raise OperationalRetentionIntervalError("Operational interval already closed")
        if self.clock() >= self.deadline:
            self._invalid = True
        was_invalid = self._invalid
        close_error = None
        try:
            # Always attempt an independent closing observation, including after
            # refusal or deadline. A successful read cannot repair an invalid
            # interval; it only supplies the required closing evidence.
            self._capture("close", closing=True)
        except BaseException as error:
            close_error = error
        self._closed = True
        if was_invalid or close_error is not None or len(self.observations) != 3:
            self._invalid = True
            if close_error is not None:
                raise OperationalRetentionIntervalError(
                    "Operational interval did not complete all three observations") from close_error
            raise OperationalRetentionIntervalError("Operational interval is incomplete")
        return ManagedOperationalIntervalReceipt(
            profile=PROFILE,
            interval_id=self.interval_id,
            outcome="observed_available_at_checks",
            authority="read_observation_only_no_source_ack",
            maximum_elapsed_seconds=_MAX_INTERVAL_SECONDS,
            observations=tuple(self.observations),
        )
