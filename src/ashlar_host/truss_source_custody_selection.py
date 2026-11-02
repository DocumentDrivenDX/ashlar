# SPDX-License-Identifier: MIT OR Apache-2.0
"""Source-owned bindings for the candidate Truss source-custody API.

This module checks correspondence only. The frozen R04 schedule, selected
destination, and actual public Truss page must already have been admitted by
their owning source/schedule policies. The generated registration/scope bytes
are Ashlar's exact canonical selection for that interval; they do not claim
that Truss or an external destination registry authenticated their meaning.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import importlib.metadata
import json
import datetime
import os
from pathlib import Path
import re
import weakref
from decimal import Decimal
from types import MappingProxyType

from ashlar.pins import PinVector
from ashlar.schema import decode_retained_json
from ashlar.source_checkpoint import truss_checkpoint
from ashlar.staging import batch_row
from ashlar.truss_native_feed import native_feed_batch
from .r04_schedule import R04Schedule, canonical


_IDENTITY_FIELDS = (
    "feed", "consumer_id", "registration_id", "ack_scope",
    "ack_registration_id", "installation_id", "epoch",
)
_MAX_BYTES = 4 * 1024 * 1024
_RELEASE_ID = "truss-catalog-runtime/0.75-candidate"
_RELEASE_SHA256 = "5ecb599f65d64e6656bf6c83c30cea6e2aefd488cb3fe821b749458d5e9b3522"
_ANCHORS = weakref.WeakValueDictionary()
_OPEN_SELECTIONS = weakref.WeakValueDictionary()
_SELECTIONS = weakref.WeakValueDictionary()
_EXPECTED_POST_STATES = weakref.WeakValueDictionary()
_LOCAL_NATIVE_OBSERVATIONS = weakref.WeakValueDictionary()

LOCAL_ROW_BAG_CANONICALIZATION = "ashlar-local-native-row-bag/0.1"
LOCAL_EXPECTED_POST_STATE_PROFILE = "ashlar-local-native-expected-post-state/0.1"
LOCAL_NATIVE_OBSERVATION_PROFILE = "ashlar-local-native-delta-publication-observation/0.1"
_LOCAL_DATA_ROLES = ("object_current", "edge_current", "tombstone", "whole_source_history")
# LocalDeltaTransport is the complete installed six-role Delta registry. Source
# and publication vectors cover the four data roles only; attempts and manifest
# remain registered installation roles and must still be present exactly.
_LOCAL_INSTALLATION_ROLES = (*_LOCAL_DATA_ROLES, "attempts", "manifest")
_MAX_NATIVE_ROWS = 100_000
_MAX_NATIVE_CELL_BYTES = 1 * 1024 * 1024
_MAX_NATIVE_ROW_BAG_BYTES = 64 * 1024 * 1024
_MAX_NATIVE_FILES = 4096
_MAX_NATIVE_LOG_VERSIONS = 100_000
_MAX_NATIVE_WALK_ENTRIES = 100_000


class TrussSourceSelectionError(ValueError):
    """Selected Truss source, destination, or publication correspondence refused."""


def _issued(registry, value):
    return registry.get(id(value)) is value


def _issue(registry, value):
    registry[id(value)] = value
    return value


def _need(value, message):
    if not value:
        raise TrussSourceSelectionError(message)


def _bounded_text(value, label, maximum=4096):
    _need(type(value) is str and value and "\0" not in value
          and len(value.encode("utf-8")) <= maximum,
          f"Explicit bounded {label} required")
    return value


def _canonical_bytes(value):
    return canonical(value)


def local_native_row_bag_summary(rows):
    """Digest an unordered native row bag without losing duplicates or nulls.

    Rows are canonical JSON objects sorted by encoded bytes. Each row is
    length-prefixed with an unsigned 8-byte big-endian size before hashing.
    This binds multiplicity while making native collection order irrelevant.
    """
    if type(rows) not in (list, tuple):
        raise TrussSourceSelectionError("Exact native row list required")
    encoded_rows = []
    for row in rows:
        if type(row) is not dict:
            raise TrussSourceSelectionError("Native row must be an exact field mapping")
        encoded_rows.append(_canonical_bytes(row))
    encoded_rows.sort()
    digest = hashlib.sha256()
    for raw in encoded_rows:
        digest.update(len(raw).to_bytes(8, "big"))
        digest.update(raw)
    return {"canonicalization": LOCAL_ROW_BAG_CANONICALIZATION,
            "row_count": len(encoded_rows), "row_bag_sha256": digest.hexdigest()}


@dataclass(frozen=True, eq=False)
class ExpectedNativePostState:
    profile: str
    publication_id: str
    request_digest: str
    carrier_profile: str
    roles: object = field(repr=False, compare=False)

    def receipt(self):
        return {"profile": self.profile, "publication_id": self.publication_id,
                "request_digest": self.request_digest,
                "carrier_profile": self.carrier_profile,
                "roles": {role: dict(value) for role, value in self.roles.items()}}


def _mint_expected_native_post_state(*, publication_id, request_digest, following,
                                    carrier_profile="ashlar-delta/0.4-xxhash64"):
    """Mint exact planned role bags from the publisher's computed post-state."""
    publication_id = _bounded_text(publication_id, "publication ID", 4096)
    _need(type(request_digest) is str and re.fullmatch(r"[0-9a-f]{64}", request_digest),
          "Exact publication request digest required")
    _need(type(following) is dict and set(following) == set(_LOCAL_DATA_ROLES),
          "Complete exact planned four-role post-state required")
    _need(carrier_profile == "ashlar-delta/0.4-xxhash64",
          "Local native post-state requires the pinned Delta carrier profile")
    roles = {role: MappingProxyType(local_native_row_bag_summary(following[role]))
             for role in _LOCAL_DATA_ROLES}
    value = ExpectedNativePostState(
        profile=LOCAL_EXPECTED_POST_STATE_PROFILE, publication_id=publication_id,
        request_digest=request_digest, carrier_profile="ashlar-delta/0.4-xxhash64",
        roles=MappingProxyType(roles))
    return _issue(_EXPECTED_POST_STATES, value)


@dataclass(frozen=True)
class LocalNativeDeltaRoleObservation:
    role: str
    table: str
    uuid: str
    version: int
    head_version: int
    head_json_sha256: str
    log_history: tuple
    active_file_paths: tuple
    physical_file_paths: tuple
    active_files: int
    physical_parquet_files: int
    row_bag_sha256: str
    row_count: int


@dataclass(frozen=True, eq=False)
class LocalNativeDeltaPublicationObservation:
    profile: str
    publication_id: str
    manifest_bytes: bytes = field(repr=False)
    manifest_sha256: str
    vector: PinVector = field(repr=False, compare=False)
    receipt_bytes: bytes = field(repr=False)
    receipt_sha256: str
    roles: object = field(repr=False, compare=False)


def _strict_local_json(raw, label):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise TrussSourceSelectionError(f"Duplicate {label} member")
            result[key] = value
        return result
    try:
        return json.loads(raw, object_pairs_hook=pairs,
                          parse_constant=lambda _value: (_ for _ in ()).throw(
                              TrussSourceSelectionError(f"Nonfinite {label} value")))
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError) as error:
        raise TrussSourceSelectionError(f"Malformed {label}") from error


def _local_safe_relative_path(value, label):
    _need(type(value) is str and 0 < len(value.encode("utf-8")) <= 8192
          and "\\" not in value and "\0" not in value,
          f"Bounded local {label} required")
    path = Path(value)
    _need(not path.is_absolute() and all(part not in ("", ".", "..") for part in path.parts),
          f"Safe relative local {label} required")
    _need(path.as_posix() == value, f"Canonical relative local {label} required")
    return value


def _local_delta_log_inventory(root, version, expected_uuid):
    log_dir = root / "_delta_log"
    _need(log_dir.is_dir() and not log_dir.is_symlink(), "Exact local Delta log directory required")
    _need(type(version) is int and 0 <= version <= _MAX_NATIVE_LOG_VERSIONS,
          "Bounded selected local Delta version required")
    log_entries = []
    with os.scandir(log_dir) as entries:
        for entry in entries:
            _need(len(log_entries) < _MAX_NATIVE_WALK_ENTRIES,
                  "Bounded local Delta log directory inventory required")
            log_entries.append((entry.name, entry.is_file(follow_symlinks=False),
                                entry.is_symlink()))
    disk_versions = []
    for name, is_file, is_symlink in log_entries:
        if name.endswith(".json"):
            match = re.fullmatch(r"([0-9]{20})\.json", name)
            _need(match is not None and is_file and not is_symlink,
                  "Canonical local Delta JSON log filename required")
            disk_versions.append(int(match.group(1)))
    _need(sorted(disk_versions) == list(range(version + 1)),
          "Selected local Delta version must be the current contiguous log head")
    history = []
    active = set()
    found_uuid = None
    for item_version in range(version + 1):
        path = log_dir / f"{item_version:020d}.json"
        _need(path.is_file() and not path.is_symlink(),
              "Contiguous local Delta JSON log history required")
        _need(path.stat().st_size <= _MAX_BYTES,
              "Bounded local Delta JSON action file required")
        raw = path.read_bytes()
        _need(0 < len(raw) <= _MAX_BYTES, "Bounded local Delta JSON action file required")
        history.append((item_version, hashlib.sha256(raw).hexdigest()))
        lines = raw.splitlines()
        _need(0 < len(lines) <= 100_000, "Bounded local Delta action inventory required")
        for line in lines:
            action = _strict_local_json(line, "Delta action")
            _need(type(action) is dict and len(action) == 1,
                  "Closed single-action local Delta JSON line required")
            if "metaData" in action:
                metadata = action["metaData"]
                _need(type(metadata) is dict and type(metadata.get("id")) is str,
                      "Local Delta metadata UUID required")
                if found_uuid is None:
                    found_uuid = metadata["id"]
                else:
                    _need(found_uuid == metadata["id"], "Local Delta metadata UUID drift")
            if "add" in action:
                add = action["add"]
                _need(type(add) is dict, "Local Delta add action required")
                rel = _local_safe_relative_path(add.get("path"), "active data path")
                active.add(rel)
                _need(len(active) <= _MAX_NATIVE_FILES, "Bounded active Delta file set required")
            if "remove" in action:
                remove = action["remove"]
                _need(type(remove) is dict, "Local Delta remove action required")
                active.discard(_local_safe_relative_path(remove.get("path"), "removed data path"))
        _need(len(active) <= _MAX_NATIVE_FILES, "Bounded active Delta file set required")
    _need(found_uuid == expected_uuid, "Delta-log UUID differs from selected table UUID")

    physical = set()
    seen = 0
    pending = [root]
    while pending:
        directory = pending.pop()
        with os.scandir(directory) as entries:
            for entry in entries:
                seen += 1
                _need(seen <= _MAX_NATIVE_WALK_ENTRIES,
                      "Bounded local Delta file walk required")
                _need(not entry.is_symlink(), "Symlink in local Delta target refused")
                full = Path(entry.path)
                if entry.is_dir(follow_symlinks=False):
                    pending.append(full)
                elif entry.is_file(follow_symlinks=False) and entry.name.endswith(".parquet"):
                    rel = full.relative_to(root).as_posix()
                    physical.add(_local_safe_relative_path(rel, "physical Parquet path"))
                    _need(len(physical) <= _MAX_NATIVE_FILES,
                          "Bounded physical local Parquet file set required")
    _need(active <= physical, "Active local Delta files are not all physically present")
    return tuple(history), tuple(sorted(active)), tuple(sorted(physical))


def _native_cell(value, kind):
    if value is None:
        return None
    if kind == "timestamp":
        _need(isinstance(value, datetime.datetime), "Native timestamp cell required")
        if value.tzinfo is None:
            value = value.replace(tzinfo=datetime.timezone.utc)
        delta = value.astimezone(datetime.timezone.utc) - datetime.datetime(1970, 1, 1,
                                                                         tzinfo=datetime.timezone.utc)
        return str((delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds)
    if kind in ("tinyint", "smallint", "int", "bigint") or kind.startswith("decimal("):
        _need(type(value) in (int, str) or isinstance(value, Decimal),
              "Native integral/decimal cell required")
        return str(value)
    if kind == "string":
        _need(type(value) is str, "Native string cell required")
        return value
    if kind == "boolean":
        _need(type(value) is bool, "Native boolean cell required")
        return value
    raise TrussSourceSelectionError("Unsupported local native Delta value type")


def _local_native_rows(spark, path, version):
    frame = spark.read.format("delta").option("versionAsOf", version).load(str(path))
    fields = frame.schema.fields
    _need(type(fields) in (list, tuple) and 1 <= len(fields) <= 256,
          "Bounded exact local native schema required")
    schema = []
    for item in fields:
        name = getattr(item, "name", None)
        kind = item.dataType.simpleString()
        _need(type(name) is str and name and name not in {entry[0] for entry in schema},
              "Unique native Delta field names required")
        if kind == "timestamp":
            # PySpark materializes Spark TIMESTAMP as a naive Python datetime.
            # Its conversion follows the host timezone, so attaching UTC after
            # collect can shift the instant. Convert in Spark before crossing
            # that boundary; unix_micros is independent of Python's local TZ.
            schema.append((name, "bigint"))
        else:
            schema.append((name, kind))
    if any(item.dataType.simpleString() == "timestamp" for item in fields):
        from pyspark.sql import functions as F
        projection = []
        for item in fields:
            name = item.name
            if item.dataType.simpleString() == "timestamp":
                projection.append(F.unix_micros(F.col(name)).alias(name))
            else:
                projection.append(F.col(name))
        frame = frame.select(*projection)
    rows = frame.limit(_MAX_NATIVE_ROWS + 1).collect()
    _need(type(rows) is list and len(rows) <= _MAX_NATIVE_ROWS,
          "Bounded complete local native row bag required")
    mapped = []
    total_bytes = 0
    for row in rows:
        values = row.asDict(recursive=True)
        _need(type(values) is dict and set(values) == {name for name, _ in schema},
              "Exact native Delta row fields required")
        native = {name: _native_cell(values[name], kind) for name, kind in schema}
        _need(all(value is None or len(_canonical_bytes(value)) <= _MAX_NATIVE_CELL_BYTES
                  for value in native.values()),
              "Bounded local native cell required")
        row_bytes = len(_canonical_bytes(native))
        _need(row_bytes <= _MAX_NATIVE_CELL_BYTES * max(1, len(schema)),
              "Bounded local native row required")
        total_bytes += row_bytes
        _need(total_bytes <= _MAX_NATIVE_ROW_BAG_BYTES,
              "Bounded complete local native row bytes required")
        mapped.append(native)
    return mapped


def read_local_native_delta_publication_observation(*, anchor, spark, destination_transport,
        manifest_bytes, vector, receipt_bytes, expected_post_state):
    """Read and issue exact local Delta evidence; callers cannot supply observations.

    The factory opens each selected snapshot with ``versionAsOf``, reads its
    complete bounded row bag, checks Delta metadata and contiguous JSON actions,
    and inventories active and physical files before issuing the observation.
    """
    _need(_issued(_ANCHORS, anchor) and anchor.authority_profile == "local-development",
          "Validated local-development source anchor required")
    from .delta_custody import DeltaTarget, LocalDeltaTransport
    _need(type(destination_transport) is LocalDeltaTransport
          and destination_transport is anchor.destination_transport
          and getattr(destination_transport, "spark", None) is spark,
          "Exact preselected local Delta transport instance required")
    _need(type(manifest_bytes) is bytes and 0 < len(manifest_bytes) <= _MAX_BYTES
          and type(vector) is PinVector and type(receipt_bytes) is bytes
          and 0 < len(receipt_bytes) <= _MAX_BYTES,
          "Exact local publication manifest, vector and receipt required")
    _need(type(expected_post_state) is ExpectedNativePostState
          and _issued(_EXPECTED_POST_STATES, expected_post_state),
          "Exact publisher-issued expected native post-state required")
    manifest = decode_retained_json(manifest_bytes)
    publication_id = _bounded_text(manifest.get("publication_id"), "publication ID", 4096)
    _need(_canonical_bytes(manifest) == manifest_bytes
          and manifest.get("profile_version") == "ashlar-delta/0.4-xxhash64"
          and vector.authority == "private-local-process"
          and vector.scope_kind == "manifest" and vector.scope_id == publication_id
          and vector.custody_digest == hashlib.sha256(manifest_bytes).hexdigest(),
          "Exact canonical local manifest and vector binding required")
    _need(publication_id == anchor.preselected_publication_id
          and expected_post_state.publication_id == publication_id,
          "Publisher post-state differs from selected publication")
    request = decode_retained_json(anchor.request_bytes)
    from ashlar.attempt_store import verify_request_digest
    inner_digest = verify_request_digest(request)
    _need(hashlib.sha256(anchor.request_bytes).hexdigest() == anchor.request_sha256
          and expected_post_state.request_digest == inner_digest
          and publication_id == "truss-graph-" + inner_digest,
          "Selected request and publisher state differ")
    from .truss_feed_sessions import encode_truss_destination_receipt
    expected_receipt = encode_truss_destination_receipt(anchor.request_bytes, manifest_bytes, vector)
    _need(receipt_bytes == expected_receipt,
          "Destination receipt differs from exact request, manifest and vector")
    versions = decode_retained_json(manifest["table_versions_json"].encode("utf-8"))
    targets = destination_transport.targets
    installation_tables = {
        "local.trussgraph." + role for role in _LOCAL_INSTALLATION_ROLES}
    data_tables = {"local.trussgraph." + role for role in _LOCAL_DATA_ROLES}
    _need(type(versions) is dict and set(versions) == set(vector.targets)
          and type(targets) is MappingProxyType and set(targets) == installation_tables
          and set(vector.targets) == data_tables
          and type(anchor.destination_target_bindings) is MappingProxyType
          and set(anchor.destination_target_bindings) == data_tables,
          "Preselected destination transport, vector and manifest versions required")
    _need(type(anchor.destination_installation_bindings) is MappingProxyType
          and set(anchor.destination_installation_bindings) == installation_tables,
          "Exact selected six-role installation binding required")
    for table in installation_tables:
        target = targets[table]
        _need(type(target) is DeltaTarget and target.table == table
              and anchor.destination_installation_bindings[table]
                  == (str(target.path), target.uuid),
              "Selected local Delta installation registry differs from source selection")
    role_entries = {}
    for table, target in vector.targets.items():
        role = table.rsplit(".", 1)[-1]
        _need(table == "local.trussgraph." + role and role in _LOCAL_DATA_ROLES
              and role not in role_entries,
              "Exact local.trussgraph role table required")
        _need(type(target) is tuple and len(target) == 2
              and type(target[0]) is str and target[0]
              and type(target[1]) is int and target[1] == versions[table],
              "Exact selected Delta UUID/version required")
        selected_target = targets[table]
        _need(type(selected_target) is DeltaTarget and selected_target.table == table
              and selected_target.uuid == target[0]
              and anchor.destination_target_bindings[table]
                  == (str(selected_target.path), selected_target.uuid),
              "Selected local Delta transport target differs from publication vector")
        path = Path(selected_target.path)
        _need(path.is_absolute() and path.exists() and path.is_dir()
              and all(not ancestor.is_symlink() for ancestor in (path, *path.parents)),
              "Canonical absolute local Delta target without symlink ancestors required")
        root = path.resolve(strict=True)
        _need((root / "_delta_log").is_dir(),
              "Canonical local Delta target with log required")
        rows = _local_native_rows(spark, root, target[1])
        summary = local_native_row_bag_summary(rows)
        expected = expected_post_state.roles.get(role)
        _need(expected is not None
              and expected["canonicalization"] == LOCAL_ROW_BAG_CANONICALIZATION
              and summary == dict(expected),
              "Native Delta row bag differs from exact publisher post-state")
        detail_rows = spark.sql("DESCRIBE DETAIL delta.`" + str(root).replace("`", "``") + "`").limit(2).collect()
        _need(type(detail_rows) is list and len(detail_rows) == 1,
              "Unique exact local Delta table detail required")
        detail = detail_rows[0].asDict(recursive=True)
        _need(detail.get("id") == target[0], "Spark Delta UUID differs from selected vector")
        history, active, physical = _local_delta_log_inventory(root, target[1], target[0])
        role_entries[role] = LocalNativeDeltaRoleObservation(
            role=role, table=table, uuid=target[0], version=target[1],
            head_version=history[-1][0], head_json_sha256=history[-1][1],
            log_history=history, active_file_paths=active, physical_file_paths=physical,
            active_files=len(active), physical_parquet_files=len(physical),
            row_bag_sha256=summary["row_bag_sha256"], row_count=summary["row_count"])
    _need(set(role_entries) == set(_LOCAL_DATA_ROLES)
          and set(expected_post_state.roles) == set(_LOCAL_DATA_ROLES),
          "Complete exact four-role native publication required")
    value = LocalNativeDeltaPublicationObservation(
        profile=LOCAL_NATIVE_OBSERVATION_PROFILE, publication_id=publication_id,
        manifest_bytes=manifest_bytes, manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
        vector=vector, receipt_bytes=receipt_bytes,
        receipt_sha256=hashlib.sha256(receipt_bytes).hexdigest(),
        roles=MappingProxyType(role_entries))
    return _issue(_LOCAL_NATIVE_OBSERVATIONS, value)


@dataclass(frozen=True)
class TrussSourceCustodyAnchor:
    """Bindings frozen before publication from schedule plus actual source page."""

    schedule_sha256: str
    source: str
    identity: dict = field(repr=False, compare=False)
    page: object = field(repr=False, compare=False)
    destination_id: str
    request_bytes: bytes = field(repr=False)
    request_sha256: str
    destination_registration_bytes: bytes = field(repr=False)
    ack_scope_bytes: bytes = field(repr=False)
    maximum_facts: int = 4096
    maximum_bytes: int = 8 * 1024 * 1024
    authority_profile: str = "r04-scheduled"
    preselected_publication_id: str | None = None
    destination_transport: object = field(default=None, repr=False, compare=False)
    destination_target_bindings: object = field(default=None, repr=False, compare=False)
    destination_installation_bindings: object = field(default=None, repr=False, compare=False)

    def open_selection(self):
        """Issue the pre-publication source selection from this exact A/B anchor."""
        _need(_issued(_ANCHORS, self),
              "Source open selection must come from the validated A/B anchor")
        _need(self.authority_profile == "r04-scheduled" and self.source in ("A", "B")
              and self.preselected_publication_id is not None,
              "Pre-publication source custody requires a frozen publication ID")
        return _issue(_OPEN_SELECTIONS, TrussSourceCustodyOpenSelection(
            page=self.page, maximum_facts=self.maximum_facts,
            maximum_bytes=self.maximum_bytes, request_bytes=self.request_bytes,
            destination_registration_bytes=self.destination_registration_bytes,
            ack_scope_bytes=self.ack_scope_bytes, destination_id=self.destination_id,
            request_sha256=self.request_sha256,
            publication_id=self.preselected_publication_id,
            schedule_sha256=self.schedule_sha256, source=self.source,
            identity=MappingProxyType(dict(self.identity)), anchor=self))

    def bind_publication(self, *, manifest_bytes: bytes, vector: PinVector,
                         receipt_bytes: bytes, observation: object
                         ) -> "TrussSourceCustodySelection":
        """Bind to the exact managed-reader observation and original publication."""
        _need(_issued(_ANCHORS, self),
              "Source custody anchor must come from the validated source selector")
        _need(self.authority_profile == "r04-scheduled" and self.source in ("A", "B"),
              "Managed observation binder is limited to the scheduled R04 source")
        if (type(manifest_bytes) is not bytes or not 0 < len(manifest_bytes) <= _MAX_BYTES
                or type(vector) is not PinVector
                or type(receipt_bytes) is not bytes or not 0 < len(receipt_bytes) <= _MAX_BYTES):
            raise TrussSourceSelectionError(
                "Exact original manifest, complete pin vector and destination receipt required")
        manifest = decode_retained_json(manifest_bytes)
        publication_id = _bounded_text(manifest.get("publication_id"), "publication ID", 4096)
        if (self.preselected_publication_id is not None
                and publication_id != self.preselected_publication_id):
            raise TrussSourceSelectionError("Manifest differs from the preselected publication ID")
        from .truss_feed_sessions import encode_truss_destination_receipt
        expected_receipt = encode_truss_destination_receipt(
            self.request_bytes, manifest_bytes, vector)
        _need(receipt_bytes == expected_receipt,
              "Destination receipt differs from the exact request, manifest and full vector")
        names = set(vector.targets)
        _need(len(names) == 4 and all(type(name) is str and len(name.split(".")) == 3
              for name in names)
              and len({tuple(name.split(".")[:2]) for name in names}) == 1
              and {name.split(".")[-1] for name in names} == {
                  "object_current", "edge_current", "tombstone", "whole_source_history"},
              "Complete exact four-role managed data vector required")
        from .managed_current_spark_receipt_read import (
            PROFILE as MANAGED_READ_PROFILE, ManagedCurrentSparkReceiptReopenObservation,
        )
        _need(type(observation) is ManagedCurrentSparkReceiptReopenObservation
              and observation.profile == MANAGED_READ_PROFILE
              and observation.publication_id == publication_id
              and observation.manifest == manifest
              and observation.manifest_sha256 == hashlib.sha256(manifest_bytes).hexdigest()
              and observation.vector == vector
              and observation.source_receipt_bytes == receipt_bytes
              and observation.source_receipt_sha256 == hashlib.sha256(receipt_bytes).hexdigest(),
              "Managed reader observation must bind this exact manifest, vector and receipt")
        return _issue(_SELECTIONS, TrussSourceCustodySelection(
            page=self.page, maximum_facts=self.maximum_facts,
            maximum_bytes=self.maximum_bytes, request_bytes=self.request_bytes,
            destination_registration_bytes=self.destination_registration_bytes,
            ack_scope_bytes=self.ack_scope_bytes, destination_id=self.destination_id,
            request_sha256=self.request_sha256, publication_id=publication_id,
            manifest_bytes=manifest_bytes, vector=vector, receipt_bytes=receipt_bytes,
            schedule_sha256=self.schedule_sha256, source=self.source,
            identity=MappingProxyType(dict(self.identity)),
            authority_profile=self.authority_profile))

    def bind_local_native_publication(self, *, manifest_bytes: bytes, vector: PinVector,
                                      receipt_bytes: bytes, expected_post_state: object,
                                      observation: "LocalNativeDeltaPublicationObservation"
                                      ) -> "TrussSourceCustodySelection":
        """Bind local-dev source custody to planned rows and independent Delta readback.

        This deliberately grants no managed production authority. R04 remains
        bound only by ``bind_publication`` and its managed reader observation.
        """
        _need(_issued(_ANCHORS, self),
              "Source custody anchor must come from the validated source selector")
        _need(self.authority_profile == "local-development"
              and self.source == "local-development",
              "Local native binder requires the distinct local-development anchor")
        if (type(manifest_bytes) is not bytes or not 0 < len(manifest_bytes) <= _MAX_BYTES
                or type(vector) is not PinVector
                or type(receipt_bytes) is not bytes or not 0 < len(receipt_bytes) <= _MAX_BYTES
                or type(expected_post_state) is not ExpectedNativePostState
                or not _issued(_EXPECTED_POST_STATES, expected_post_state)
                or type(observation) is not LocalNativeDeltaPublicationObservation
                or not _issued(_LOCAL_NATIVE_OBSERVATIONS, observation)):
            raise TrussSourceSelectionError(
                "Exact local manifest, vector, publisher post-state and native readback required")
        manifest = decode_retained_json(manifest_bytes)
        publication_id = _bounded_text(manifest.get("publication_id"), "publication ID", 4096)
        _need(manifest.get("profile_version") == "ashlar-delta/0.4-xxhash64"
              and _canonical_bytes(manifest) == manifest_bytes
              and vector.authority == "private-local-process"
              and vector.scope_kind == "manifest" and vector.scope_id == publication_id
              and vector.custody_digest == hashlib.sha256(manifest_bytes).hexdigest(),
              "Exact canonical local `.4` manifest and pin digest required")
        _need(publication_id == self.preselected_publication_id
              and expected_post_state.profile == LOCAL_EXPECTED_POST_STATE_PROFILE
              and expected_post_state.carrier_profile == "ashlar-delta/0.4-xxhash64"
              and expected_post_state.publication_id == publication_id,
              "Publisher post-state differs from frozen local request/publication")
        request = decode_retained_json(self.request_bytes)
        from ashlar.attempt_store import verify_request_digest
        inner_digest = verify_request_digest(request)
        _need(hashlib.sha256(self.request_bytes).hexdigest() == self.request_sha256
              and expected_post_state.request_digest == inner_digest
              and publication_id == "truss-graph-" + inner_digest,
              "Frozen local request, publisher digest and publication ID differ")
        from .truss_feed_sessions import encode_truss_destination_receipt
        expected_receipt = encode_truss_destination_receipt(
            self.request_bytes, manifest_bytes, vector)
        _need(receipt_bytes == expected_receipt,
              "Destination receipt differs from exact local request, manifest and vector")
        _need(type(observation.profile) is str
              and observation.profile == LOCAL_NATIVE_OBSERVATION_PROFILE
              and observation.publication_id == publication_id
              and observation.manifest_bytes == manifest_bytes
              and observation.manifest_sha256 == hashlib.sha256(manifest_bytes).hexdigest()
              and observation.vector == vector
              and observation.receipt_bytes == receipt_bytes
              and observation.receipt_sha256 == hashlib.sha256(receipt_bytes).hexdigest(),
              "Native Delta observation differs from exact local manifest/vector/receipt")
        versions_raw = manifest.get("table_versions_json")
        _need(type(versions_raw) is str, "Exact local manifest data-role versions required")
        versions = decode_retained_json(versions_raw.encode("utf-8"))
        _need(type(versions) is dict and set(versions) == set(vector.targets),
              "Manifest and complete four-role vector target sets differ")
        role_rows = observation.roles
        _need(type(role_rows) in (dict, MappingProxyType)
              and set(role_rows) == set(_LOCAL_DATA_ROLES)
              and set(expected_post_state.roles) == set(_LOCAL_DATA_ROLES),
              "Exact four-role native and expected post-state maps required")
        target_roles = {}
        for table, target in vector.targets.items():
            role = table.rsplit(".", 1)[-1]
            _need(table == "local.trussgraph." + role
                  and role in _LOCAL_DATA_ROLES and role not in target_roles,
                  "Exact local.trussgraph four-role vector required")
            target_roles[role] = (table, target)
        _need(set(target_roles) == set(_LOCAL_DATA_ROLES),
              "All native graph data roles must be selected")
        for role in _LOCAL_DATA_ROLES:
            native = role_rows[role]
            _need(type(native) is LocalNativeDeltaRoleObservation
                  and native.role == role,
                  "Exact typed local native role observation required")
            table, (uuid, version) = target_roles[role]
            _need(native.table == table and native.uuid == uuid
                  and type(native.version) is int and native.version == version
                  and versions[table] == version
                  and type(native.head_version) is int and native.head_version == version,
                  "Native Delta role UUID/version differs from selected publication vector")
            _need(type(native.log_history) is tuple and 1 <= len(native.log_history) <= 100001
                  and len(native.log_history) == version + 1,
                  "Complete bounded contiguous local Delta log history required")
            previous = -1
            last_digest = None
            for entry in native.log_history:
                _need(type(entry) is tuple and len(entry) == 2
                      and type(entry[0]) is int and entry[0] == previous + 1
                      and type(entry[1]) is str
                      and re.fullmatch(r"[0-9a-f]{64}", entry[1]) is not None,
                      "Contiguous canonical Delta log version/digest sequence required")
                previous, last_digest = entry
            _need(native.head_json_sha256 == last_digest
                  and type(native.active_files) is int and 0 <= native.active_files <= 1000000
                  and type(native.physical_parquet_files) is int
                  and native.active_files <= native.physical_parquet_files <= 1000000
                  and type(native.active_file_paths) is tuple
                  and type(native.physical_file_paths) is tuple
                  and len(native.active_file_paths) == native.active_files
                  and len(native.physical_file_paths) == native.physical_parquet_files
                  and tuple(sorted(set(native.active_file_paths))) == native.active_file_paths
                  and tuple(sorted(set(native.physical_file_paths))) == native.physical_file_paths
                  and set(native.active_file_paths) <= set(native.physical_file_paths)
                  and all(_local_safe_relative_path(item, "active data path") == item
                          for item in native.active_file_paths)
                  and all(_local_safe_relative_path(item, "physical Parquet path") == item
                          for item in native.physical_file_paths)
                  and type(native.row_count) is int and 0 <= native.row_count <= 1000000
                  and type(native.row_bag_sha256) is str
                  and re.fullmatch(r"[0-9a-f]{64}", native.row_bag_sha256) is not None,
                  "Bounded actual native file and row-bag observation required")
            expected = expected_post_state.roles[role]
            _need(type(expected) in (dict, MappingProxyType)
                  and set(expected) == {"canonicalization", "row_count", "row_bag_sha256"}
                  and expected["canonicalization"] == LOCAL_ROW_BAG_CANONICALIZATION
                  and type(expected["row_count"]) is int
                  and expected["row_count"] == native.row_count
                  and expected["row_bag_sha256"] == native.row_bag_sha256,
                  "Native Delta row bag differs from exact publisher post-state")
        return _issue(_SELECTIONS, TrussSourceCustodySelection(
            page=self.page, maximum_facts=self.maximum_facts,
            maximum_bytes=self.maximum_bytes, request_bytes=self.request_bytes,
            destination_registration_bytes=self.destination_registration_bytes,
            ack_scope_bytes=self.ack_scope_bytes, destination_id=self.destination_id,
            request_sha256=self.request_sha256, publication_id=publication_id,
            manifest_bytes=manifest_bytes, vector=vector, receipt_bytes=receipt_bytes,
            schedule_sha256=self.schedule_sha256, source=self.source,
            identity=MappingProxyType(dict(self.identity)),
            authority_profile="local-development"))


@dataclass(frozen=True, eq=False)
class TrussSourceCustodyOpenSelection:
    """Issuer-bound pre-publication source open selection, with no receipt authority."""

    page: object = field(repr=False, compare=False)
    maximum_facts: int
    maximum_bytes: int
    request_bytes: bytes = field(repr=False)
    destination_registration_bytes: bytes = field(repr=False)
    ack_scope_bytes: bytes = field(repr=False)
    destination_id: str
    request_sha256: str
    publication_id: str
    schedule_sha256: str
    source: str
    identity: dict = field(repr=False, compare=False)
    anchor: TrussSourceCustodyAnchor = field(repr=False, compare=False)

    def to_candidate_sdk(self, *, runtime_status=None):
        _need(_issued(_OPEN_SELECTIONS, self)
              and _issued(_ANCHORS, self.anchor),
              "Pre-publication Truss selection must come from its live A/B anchor")
        _need(self.anchor.preselected_publication_id == self.publication_id,
              "Preselected Truss publication ID changed")
        try:
            from truss import CatalogRuntimeStatus
            from truss.contracts import ProfilePin
            from truss.feed import FeedSourceSelection
            from .truss_current_catalog import (
                CURRENT_052_CUSTODY_PROFILE, installed_truss_052_source_custody_profile)
            package_version = importlib.metadata.version("umf-truss")
            if package_version != "0.0.52":
                raise ValueError("Exact Truss 0.0.52 candidate package required")
            installed_profile = installed_truss_052_source_custody_profile()
        except (ImportError, LookupError, ValueError) as error:
            raise TrussSourceSelectionError(
                "Exact Truss source-custody candidate runtime unavailable") from error
        _need(type(runtime_status) is CatalogRuntimeStatus
              and runtime_status.installed is True
              and runtime_status.installation_id == self.identity["installation_id"]
              and runtime_status.release_id == _RELEASE_ID
              and runtime_status.release_sha256 == _RELEASE_SHA256
              and type(runtime_status.layout_profile) is ProfilePin
              and installed_profile == CURRENT_052_CUSTODY_PROFILE,
              "Truss source custody is gated to the reviewed candidate manifest")
        return FeedSourceSelection(
            page=self.page, maximum_facts=self.maximum_facts,
            maximum_bytes=self.maximum_bytes, request_bytes=self.request_bytes,
            destination_registration_bytes=self.destination_registration_bytes,
            ack_scope_bytes=self.ack_scope_bytes, destination_id=self.destination_id,
            request_sha256=self.request_sha256, publication_id=self.publication_id)


@dataclass(frozen=True)
class TrussSourceCustodySelection:
    """Exact, publication-bound candidate input; still not ACK authority."""

    page: object = field(repr=False, compare=False)
    maximum_facts: int
    maximum_bytes: int
    request_bytes: bytes = field(repr=False)
    destination_registration_bytes: bytes = field(repr=False)
    ack_scope_bytes: bytes = field(repr=False)
    destination_id: str
    request_sha256: str
    publication_id: str
    manifest_bytes: bytes = field(repr=False)
    vector: PinVector = field(repr=False, compare=False)
    receipt_bytes: bytes = field(repr=False)
    schedule_sha256: str
    source: str
    identity: dict = field(repr=False, compare=False)
    authority_profile: str = "r04-scheduled"

    def to_candidate_sdk(self, *, runtime_status=None):
        """Materialize the public selection only for the exact candidate release.

        A caller-supplied ``CatalogRuntimeStatus`` has no provenance by type
        alone; the caller must obtain it through public
        ``status_in_transaction`` on the selected installation. This gate
        establishes correspondence only and grants no ACK authority.
        """
        _need(_issued(_SELECTIONS, self),
              "Publication selection must come from the validated binder")
        try:
            from truss import CatalogRuntimeStatus
            from truss.contracts import ProfilePin
            from truss.feed import FeedSourceSelection
            from .truss_current_catalog import (
                CURRENT_052_CUSTODY_PROFILE, CURRENT_053_FRESH_CUSTODY_PROFILE,
                installed_truss_052_source_custody_profile,
                installed_truss_053_fresh_source_custody_profile)
            package_version = importlib.metadata.version("umf-truss")
            if package_version == "0.0.52":
                installed_profile = installed_truss_052_source_custody_profile()
                expected_profile = CURRENT_052_CUSTODY_PROFILE
            elif package_version == "0.0.53":
                installed_profile = installed_truss_053_fresh_source_custody_profile()
                expected_profile = CURRENT_053_FRESH_CUSTODY_PROFILE
            else:
                raise ValueError("Exact Truss 0.0.52 custody or 0.0.53 fresh-baseline package required")
        except (ImportError, LookupError, ValueError) as error:
            raise TrussSourceSelectionError(
                "Exact Truss source-custody candidate runtime unavailable") from error
        _need(type(runtime_status) is CatalogRuntimeStatus
              and runtime_status.installed is True
              and type(runtime_status.installation_id) is str
              and bool(runtime_status.installation_id)
              and runtime_status.installation_id == self.identity["installation_id"]
              and runtime_status.release_id == _RELEASE_ID
              and runtime_status.release_sha256 == _RELEASE_SHA256
              and type(runtime_status.layout_profile) is ProfilePin
              and package_version in ("0.0.52", "0.0.53")
              and installed_profile == expected_profile
              and getattr(FeedSourceSelection, "__name__", None) == "FeedSourceSelection",
              "Truss source custody gated by the exact selected release manifest")
        return FeedSourceSelection(
            page=self.page, maximum_facts=self.maximum_facts,
            maximum_bytes=self.maximum_bytes, request_bytes=self.request_bytes,
            destination_registration_bytes=self.destination_registration_bytes,
            ack_scope_bytes=self.ack_scope_bytes, destination_id=self.destination_id,
            request_sha256=self.request_sha256, publication_id=self.publication_id)


def select_truss_source_custody_anchor(*, schedule: R04Schedule, source: str,
                                       page: object, request_bytes: bytes,
                                       destination_id: str,
                                       preselected_publication_id: str) -> TrussSourceCustodyAnchor:
    """Freeze Ashlar-owned canonical registration/scope bytes before publication.

    ``schedule`` and ``page`` are correspondence inputs. Their independent
    admission remains the R04 schedule policy and public Truss source owner.
    """
    _need(type(schedule) is R04Schedule and source in ("A", "B"),
          "Exact frozen R04 schedule and source slot required")
    _need(type(request_bytes) is bytes and 0 < len(request_bytes) <= _MAX_BYTES,
          "Exact bounded original request bytes required")
    destination_id = _bounded_text(destination_id, "selected destination ID", 1024)
    preselected_publication_id = _bounded_text(
        preselected_publication_id, "preselected publication ID", 4096)
    identity = schedule.document()["source_bindings"][source]
    _need(type(identity) is dict and set(_IDENTITY_FIELDS).issubset(identity)
          and all(type(identity[key]) is str and identity[key]
                  and "\0" not in identity[key] for key in _IDENTITY_FIELDS),
          "Complete separately selected R04 source/consumer/ACK identity required")
    try:
        fields = ("content", "sha256", "prior", "next", "consumer_id",
                  "registration_id", "outcome")
        page_values = {key: getattr(page, key) for key in fields}
        _need(type(page_values["content"]) is bytes
              and type(page_values["sha256"]) is str
              and type(page_values["prior"].content) is bytes
              and type(page_values["next"].content) is bytes,
              "Actual complete Truss page and opaque cursors required")
        batch = native_feed_batch(page_values["content"], sha256=page_values["sha256"],
            prior=page_values["prior"].content, next=page_values["next"].content,
            feed=identity["feed"])
        prior = decode_retained_json(page_values["prior"].content)
        following = decode_retained_json(page_values["next"].content)
        request = decode_retained_json(request_bytes)
    except (AttributeError, TypeError, ValueError, KeyError) as error:
        raise TrussSourceSelectionError(
            "Original public Truss page, cursor and request correspondence required") from error
    _need(page_values["outcome"] == "transaction"
          and page_values["consumer_id"] == identity["consumer_id"]
          and page_values["registration_id"] == identity["registration_id"]
          and batch.feed == identity["feed"] and batch.epoch == identity["epoch"]
          and all(cursor.get(key) == expected for cursor in (prior, following)
              for key, expected in (("consumerId", identity["consumer_id"]),
                  ("registrationId", identity["registration_id"]),
                  ("installationId", identity["installation_id"]),
                  ("sourceEpoch", identity["epoch"]))),
          "Actual Truss page/cursor differs from independently selected source identity")
    row = batch_row(batch)
    _need(request.get("source_batch_json") == row["batch_json"]
          and request.get("source_batch_digest") == row["batch_digest"]
          and request.get("source_checkpoint_json") == truss_checkpoint(batch),
          "Original request does not bind the actual selected Truss page")
    # Recheck the outer/inner request digest using the owner implementation.
    from ashlar.attempt_store import verify_request_digest
    digest = verify_request_digest(request)
    expected_publication_id = "ashlar-current:" + hashlib.sha256(
        digest.encode("ascii")).hexdigest()
    _need(preselected_publication_id == expected_publication_id,
          "Preselected publication ID must derive from the frozen managed request")
    request_sha = hashlib.sha256(request_bytes).hexdigest()
    selected = {
        "profile": "ashlar-truss-destination-registration/0.1",
        "scheduleSha256": schedule.sha256, "source": source,
        "destinationId": destination_id,
        "ackRegistrationId": identity["ack_registration_id"],
        "ackScope": identity["ack_scope"], "feed": identity["feed"],
        "epoch": identity["epoch"], "installationId": identity["installation_id"],
        "consumerId": identity["consumer_id"],
        "consumerRegistrationId": identity["registration_id"],
        "requestSha256": request_sha,
    }
    scope = {
        "profile": "ashlar-truss-ack-scope/0.1",
        "scheduleSha256": schedule.sha256, "source": source,
        "scopeId": identity["ack_scope"],
        "ackRegistrationId": identity["ack_registration_id"],
        "destinationId": destination_id, "feed": identity["feed"],
        "epoch": identity["epoch"], "installationId": identity["installation_id"],
        "consumerId": identity["consumer_id"],
        "consumerRegistrationId": identity["registration_id"],
        "priorCursorSha256": hashlib.sha256(page_values["prior"].content).hexdigest(),
        "nextCursorSha256": hashlib.sha256(page_values["next"].content).hexdigest(),
        "requestSha256": request_sha,
    }
    registration_bytes = _canonical_bytes(selected)
    scope_bytes = _canonical_bytes(scope)
    _need(len(registration_bytes) <= 1048576 and len(scope_bytes) <= 1048576,
          "Canonical source registration/scope selection exceeds Truss API bounds")
    return _issue(_ANCHORS, TrussSourceCustodyAnchor(
        schedule_sha256=schedule.sha256, source=source, identity=MappingProxyType(dict(identity)),
        page=page, destination_id=destination_id, request_bytes=request_bytes,
        request_sha256=request_sha, destination_registration_bytes=registration_bytes,
        ack_scope_bytes=scope_bytes,
        preselected_publication_id=preselected_publication_id))


def select_truss_local_dev_source_custody_anchor(
        *, registration: object, consumer: object, runtime_status: object,
        accepted_document: object, page: object, feed: str,
        source_holder_id: str, document_id: str,
        destination_id: str,
        ack_registration_id: str, ack_scope_id: str, publication_id: str,
        request_bytes: bytes, predecessor_id: str,
        destination_transport: object) -> TrussSourceCustodyAnchor:
    """Freeze one new local-development source interval from public Truss results.

    This separate selector is deliberately not an R04 schedule path. The
    acceptance report, genesis consumer and feed page must be exact public
    Truss result types; their bytes establish correspondence, while the
    ``local-development`` label grants no production authority.
    """
    try:
        from truss import CatalogRuntimeStatus
        from truss.catalog_results import CatalogRegistration
        from truss.feed import FeedConsumer, FeedPage
        from truss.reader_catalog import AcceptedDocument
    except ImportError as error:
        raise TrussSourceSelectionError("Public Truss .52 source result types required") from error
    if (type(registration) is not CatalogRegistration
            or type(consumer) is not FeedConsumer
            or type(runtime_status) is not CatalogRuntimeStatus
            or type(accepted_document) is not AcceptedDocument
            or type(page) is not FeedPage):
        raise TrussSourceSelectionError(
            "Exact public Truss acceptance, accepted document, consumer, status and page required")
    from .delta_custody import DeltaTarget, LocalDeltaTransport
    _need(type(destination_transport) is LocalDeltaTransport
          and type(destination_transport.targets) is MappingProxyType
          and set(destination_transport.targets) == {
              "local.trussgraph." + role for role in _LOCAL_INSTALLATION_ROLES},
          "Exact selected six-role local Delta transport required")
    for table, target in destination_transport.targets.items():
        _need(type(target) is DeltaTarget and target.table == table
              and table == "local.trussgraph." + table.rsplit(".", 1)[-1]
              and table.rsplit(".", 1)[-1] in _LOCAL_INSTALLATION_ROLES
              and type(target.uuid) is str and target.uuid
              and target.path.is_absolute() and target.path.is_dir()
              and all(not ancestor.is_symlink() for ancestor in
                      (target.path, *target.path.parents)),
              "Exact existing local Delta transport target required")
    destination_target_bindings = MappingProxyType({
        table: (str(target.path), target.uuid)
        for table, target in destination_transport.targets.items()
        if table.rsplit(".", 1)[-1] in _LOCAL_DATA_ROLES})
    destination_installation_bindings = MappingProxyType({
        table: (str(target.path), target.uuid)
        for table, target in destination_transport.targets.items()})
    for value, label, maximum in (
            (feed, "feed", 1024), (source_holder_id, "source-holder ID", 1024),
            (document_id, "document ID", 4096),
            (destination_id, "destination ID", 1024),
            (ack_registration_id, "ACK registration ID", 1024),
            (ack_scope_id, "ACK scope ID", 1024),
            (publication_id, "publication ID", 4096),
            (predecessor_id, "predecessor ID", 4096)):
        _bounded_text(value, label, maximum)
    if (type(request_bytes) is not bytes or not 0 < len(request_bytes) <= _MAX_BYTES
            or type(registration.report_bytes) is not bytes
            or not 0 < len(registration.report_bytes) <= 64 * 1024 * 1024
            or type(registration.registration_id) is not str or not registration.registration_id
            or type(registration.revision) is not int or registration.revision < 1
            or runtime_status.installed is not True
            or type(runtime_status.installation_id) is not str or not runtime_status.installation_id):
        raise TrussSourceSelectionError("Bounded public Truss registration, runtime and request required")
    try:
        report = decode_retained_json(registration.report_bytes)
        accepted = report["acceptedInput"]
        documents = accepted["documents"]
        # The public report encodes catalog revision as canonical decimal
        # text; CatalogRegistration carries the same revision as an int.
        # Compare their documented representations without changing either
        # original value or accepting lossy/noncanonical numbers.
        report_revision = report.get("revision")
        registration_revision = registration.revision
        if (report.get("registrationId") != registration.registration_id
                or type(report_revision) is not str
                or not re.fullmatch(r"[1-9][0-9]{0,18}", report_revision)
                or type(registration_revision) is not int
                or str(registration_revision) != report_revision
                or type(documents) is not list):
            raise ValueError("Registration report identity differs")
        matches = [item for item in documents
                   if type(item) is dict and item.get("documentId") == document_id]
        if len(matches) != 1:
            raise ValueError("Source-holder document absent from accepted report")
        source_doc = matches[0]
        artifact = source_doc.get("artifact")
        document_revision = source_doc.get("documentRevision")
        model_sha256 = artifact.get("sha256") if type(artifact) is dict else None
        if (type(document_revision) is not str or not document_revision
                or type(model_sha256) is not str
                or re.fullmatch(r"[0-9a-f]{64}", model_sha256) is None
                or type(artifact.get("identity")) is not str or not artifact.get("identity")):
            raise ValueError("Source-holder report document identity, revision and digest required")
        document_bytes = accepted_document.content
        if (type(document_bytes) is not bytes
                or hashlib.sha256(document_bytes).hexdigest() != model_sha256
                or accepted_document.document_id != document_id
                or accepted_document.document_revision != document_revision
                or accepted_document.artifact_identity != artifact["identity"]
                or accepted_document.sha256 != model_sha256
                or accepted_document.registration_id != registration.registration_id
                or type(accepted_document.catalog_revision) is not int
                or accepted_document.catalog_revision != registration.revision
                or type(accepted_document.source_catalog_revision) is not int
                or accepted_document.source_catalog_revision < 1
                or accepted_document.source_catalog_revision > accepted_document.catalog_revision):
            raise ValueError("Public accepted document differs from acceptance report or registration")
        identity = {
            "feed": feed,
            "consumer_id": consumer.consumer_id,
            "registration_id": consumer.registration_id,
            "ack_scope": ack_scope_id,
            "ack_registration_id": ack_registration_id,
            "installation_id": runtime_status.installation_id,
            "epoch": None,
            "source_holder_id": source_holder_id,
            "document_id": document_id,
            "artifact_identity": artifact["identity"],
            "document_revision": document_revision,
            "model_sha256": model_sha256,
            "acceptance_registration_id": registration.registration_id,
            "acceptance_report_sha256": hashlib.sha256(registration.report_bytes).hexdigest(),
            "catalog_revision": str(registration.revision),
        }
        if (consumer.consumer_id != page.consumer_id
                or consumer.registration_id != page.registration_id
                or consumer.checkpoint.content != page.prior.content):
            raise ValueError("Public genesis consumer differs from the actual source page")
        batch = native_feed_batch(page.content, sha256=page.sha256,
            prior=page.prior.content, next=page.next.content, feed=feed)
        prior = decode_retained_json(page.prior.content)
        following = decode_retained_json(page.next.content)
        identity["epoch"] = batch.epoch
        if (page.outcome != "transaction"
                or batch.epoch != prior.get("sourceEpoch")
                or prior.get("sourceEpoch") != following.get("sourceEpoch")
                or any(cursor.get(key) != expected for cursor in (prior, following)
                    for key, expected in (("installationId", runtime_status.installation_id),
                        ("consumerId", consumer.consumer_id),
                        ("registrationId", consumer.registration_id)))
                or page.consumer_id != consumer.consumer_id
                or page.registration_id != consumer.registration_id):
            raise ValueError("Feed page/cursors differ from accepted installation and genesis consumer")
        request = decode_retained_json(request_bytes)
        row = batch_row(batch)
        if (request.get("predecessor") != predecessor_id
                or request.get("source_batch_json") != row["batch_json"]
                or request.get("source_batch_digest") != row["batch_digest"]
                or request.get("source_checkpoint_json") != truss_checkpoint(batch)):
            raise ValueError("Canonical request differs from exact local-development source page")
        from ashlar.attempt_store import verify_request_digest
        inner_digest = verify_request_digest(request)
        if publication_id != "truss-graph-" + inner_digest:
            raise ValueError("Local graph publication ID must be derived from the exact request")
    except (AttributeError, KeyError, TypeError, ValueError) as error:
        raise TrussSourceSelectionError(
            "Exact public local-development source registration/page/request correspondence required") from error

    request_sha = hashlib.sha256(request_bytes).hexdigest()
    prior_sha = hashlib.sha256(page.prior.content).hexdigest()
    next_sha = hashlib.sha256(page.next.content).hexdigest()
    registration_value = {
        "profile": "ashlar-local-development-destination-registration/0.1",
        "authority": "local-development",
        "sourceHolderId": source_holder_id,
        "documentId": document_id,
        "artifactIdentity": identity["artifact_identity"],
        "acceptanceRegistrationId": registration.registration_id,
        "catalogRevision": registration.revision,
        "documentRevision": document_revision,
        "modelSha256": model_sha256,
        "acceptanceReportSha256": identity["acceptance_report_sha256"],
        "ackRegistrationId": ack_registration_id,
        "ackScopeId": ack_scope_id,
        "destinationId": destination_id,
        "feed": feed, "epoch": batch.epoch,
        "installationId": runtime_status.installation_id,
        "consumerId": consumer.consumer_id,
        "consumerRegistrationId": consumer.registration_id,
        "requestSha256": request_sha,
    }
    scope_value = {
        "profile": "ashlar-local-development-ack-scope/0.1",
        "authority": "local-development",
        "sourceHolderId": source_holder_id,
        "documentId": document_id,
        "artifactIdentity": identity["artifact_identity"],
        "acceptanceReportSha256": identity["acceptance_report_sha256"],
        "scopeId": ack_scope_id,
        "ackRegistrationId": ack_registration_id,
        "destinationId": destination_id,
        "feed": feed, "epoch": batch.epoch,
        "installationId": runtime_status.installation_id,
        "consumerId": consumer.consumer_id,
        "consumerRegistrationId": consumer.registration_id,
        "priorCursorSha256": prior_sha,
        "nextCursorSha256": next_sha,
        "pageSha256": page.sha256,
        "requestSha256": request_sha,
        "publicationId": publication_id,
        "maximumPages": 1,
    }
    registration_bytes = _canonical_bytes(registration_value)
    scope_bytes = _canonical_bytes(scope_value)
    if len(registration_bytes) > 1048576 or len(scope_bytes) > 1048576:
        raise TrussSourceSelectionError("Canonical local-development source bindings exceed Truss API limits")
    # The caller retains this exact anchor and later attaches the immutable
    # publication/vector/read observation. This path is intentionally distinct
    # from the schedule-bound A/B source selector above.
    return _issue(_ANCHORS, TrussSourceCustodyAnchor(
        schedule_sha256="", source="local-development", identity=MappingProxyType(dict(identity)),
        page=page, destination_id=destination_id, request_bytes=request_bytes,
        request_sha256=request_sha, destination_registration_bytes=registration_bytes,
        ack_scope_bytes=scope_bytes, authority_profile="local-development",
        preselected_publication_id=publication_id,
        destination_transport=destination_transport,
        destination_target_bindings=destination_target_bindings,
        destination_installation_bindings=destination_installation_bindings))
