# SPDX-License-Identifier: MIT OR Apache-2.0
"""Independent bounded SQL-warehouse reopen of a managed-current receipt.

This is an external read owner for the source holder. It never creates tables,
starts compute, mutates a Truss cursor, or substitutes Spark for SQL warehouse
observations. A caller still supplies its own trusted pin holder to Truss ACK.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
import hashlib
import json
import re
import time
import uuid

from ashlar.manifest import manifest_pin_vector
from ashlar.native import quote_table_identifier
from ashlar.publication import Snapshot
from ashlar.publication import resolve_publication
from ashlar.protocol import ReaderProtocolProfile, validate_protocol_detail
from ashlar.schema import decode_retained_json

from .managed_current_carriers import load_current_managed_carriers
from .managed_current_core import CARRIER_PROFILE_LEGACY, CARRIER_PROFILE_XXHASH64, ROLES

PROFILE = "ashlar-managed-current-external-receipt-read/0.1"
DATA_ROLES = ("object_current", "edge_current", "tombstone", "whole_source_history")
_IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}\Z")
_SHA = re.compile(r"[0-9a-f]{64}\Z")


def _require(value, reason="Managed current receipt reopen refused"):
    if not value:
        raise ValueError(reason)


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


@dataclass(frozen=True)
class ManagedCurrentReceiptConfig:
    catalog: str
    schema: str
    warehouse_id: str
    expected_principal: str
    expected_table_owner: str
    source_truss_version: str
    protocol_profile: ReaderProtocolProfile = field(repr=False, compare=False)
    maximum_elapsed_seconds: int = 300
    carrier_profile: str = CARRIER_PROFILE_LEGACY

    def __post_init__(self):
        _require(type(self.catalog) is str and _IDENT.fullmatch(self.catalog)
                 and type(self.schema) is str and _IDENT.fullmatch(self.schema)
                 and type(self.warehouse_id) is str and re.fullmatch(r"[0-9a-f]{16}", self.warehouse_id)
                 and all(type(value) is str and value and "\0" not in value and len(value.encode()) <= 1024
                         for value in (self.expected_principal, self.expected_table_owner))
                 and self.source_truss_version in ("0.0.44", "0.0.45", "0.0.48", "0.0.49")
                 and type(self.protocol_profile) is ReaderProtocolProfile
                 and self.carrier_profile in (CARRIER_PROFILE_LEGACY, CARRIER_PROFILE_XXHASH64)
                 and type(self.maximum_elapsed_seconds) is int
                 and 1 <= self.maximum_elapsed_seconds <= 600,
                 "Exact catalog/schema, dedicated warehouse, owner, Truss version, and protocol profile required")


@dataclass(frozen=True)
class ManagedCurrentReceiptObservation:
    """Private ACK inputs and independently observed native installation."""
    profile: str
    publication_id: str
    manifest: dict = field(repr=False)
    manifest_sha256: str
    source_receipt_bytes: bytes = field(repr=False, compare=False)
    source_receipt_sha256: str
    vector: object = field(repr=False, compare=False)
    backend: object = field(repr=False, compare=False)
    installation_inventory: dict = field(repr=False)
    closing_installation_inventory: dict = field(repr=False)
    weft_binding_inputs: dict = field(repr=False)
    runtime_identity: dict = field(repr=False)
    supported_profiles: tuple = ("ashlar-delta/0.3",)
    supported_revisions: dict = field(default_factory=dict, repr=False)

    @property
    def native_backend(self):
        return self.backend

    def source_acknowledgement_ports(self):
        """Ports for TrussFeedSessions; its real pin owner remains caller-supplied."""
        return {"vector": self.vector, "publication_backend": self.backend,
                "supported_profiles": self.supported_profiles,
                "supported_revisions": dict(self.supported_revisions)}


class ManagedCurrentSQLReceiptReader:
    """Reopen one exact task receipt through the SDK Statement Execution API."""

    def __init__(self, sql_reader, config: ManagedCurrentReceiptConfig):
        _require(type(config) is ManagedCurrentReceiptConfig)
        _require(sql_reader is not None and hasattr(sql_reader, "query")
                 and hasattr(sql_reader, "renew") and hasattr(sql_reader, "workspace")
                 and getattr(getattr(sql_reader, "config", None), "warehouse_id", None) == config.warehouse_id,
                 "Existing dedicated Databricks SDK SQL reader required")
        sql_config = sql_reader.config
        _require(getattr(sql_config, "catalog", None) == config.catalog
                 and getattr(sql_config, "schema", None) == config.schema,
                 "SQL Statement Execution must pin the same explicit catalog and schema")
        _require(type(getattr(sql_config, "statement_seconds", None)) is int
                 and 1 <= sql_config.statement_seconds <= 30
                 and type(getattr(sql_config, "maximum_statements", None)) is int
                 and 120 <= sql_config.maximum_statements <= 128
                 and type(getattr(sql_config, "maximum_rows", None)) is int
                 and 128 <= sql_config.maximum_rows <= 1024,
                 "Receipt reopen needs bounded 30-second statements, rows, and ACK headroom")
        self.sql = sql_reader
        self.config = config
        self.started = time.monotonic()
        self.closed = False
        self.executor = _ReceiptSQLExecutor(self)
        self.carriers = load_current_managed_carriers(config.carrier_profile)
        self.context = object()
        self.opening_runtime = self._observe_runtime()
        self.opening = self._observe_inventory()
        self._manifest = None
        self._request = None
        self._publication_id = None
        self._backend = None
        self._vector = None
        self._observation = None

    def _query(self, sql, parameters=None, *, maximum=64):
        result = self.executor.query(sql, parameters or {})
        _require(len(result.rows) <= maximum, "Bounded complete SQL metadata result required")
        return result

    def _check_deadline(self):
        _require(not self.closed, "Managed current receipt reader already closed")
        _require(time.monotonic() - self.started < self.config.maximum_elapsed_seconds,
                 "Receipt reader cumulative deadline expired")

    def _native_query(self, sql, parameters):
        self._check_deadline()
        self.sql.renew()
        result = self.sql.query(sql, parameters)
        self._check_deadline()
        return result

    def _observe_runtime(self):
        c = self.config
        self.sql.renew()
        warehouse = self.sql.workspace.warehouses.get(c.warehouse_id)
        channel = getattr(warehouse, "channel", None)
        channel_name = getattr(getattr(channel, "name", None), "value", None)
        channel_version = getattr(channel, "dbsql_version", None)
        _require(warehouse.id == c.warehouse_id and warehouse.enable_serverless_compute is True
                 and channel_name == "CHANNEL_NAME_CURRENT"
                 and type(channel_version) is str and channel_version,
                 "Selected current-channel serverless SQL warehouse required")
        rows = self.executor.query(
            "SELECT current_user() AS principal,current_catalog() AS catalog,"
            "current_schema() AS schema,to_json(current_version(),"
            "map('ignoreNullFields','false')) AS current_version_json", {}) .rows
        _require(len(rows) == 1 and set(rows[0]) == {
            "principal", "catalog", "schema", "current_version_json"},
            "One complete public SQL runtime observation required")
        row = dict(rows[0])
        _require(row["principal"] == c.expected_principal and row["catalog"] == c.catalog
                 and row["schema"] == c.schema and type(row["current_version_json"]) is str,
                 "SQL warehouse principal or selected namespace differs")
        try:
            version = json.loads(row["current_version_json"], object_pairs_hook=self._unique_pairs)
        except (ValueError, TypeError, json.JSONDecodeError):
            _require(False, "Public current_version() result must be complete JSON")
        _require(type(version) is dict and version.get("dbsql_version") == channel_version,
                 "Public current_version() and selected SQL warehouse channel differ")
        return {"warehouseId": warehouse.id, "channel": channel_name,
                "channelVersion": channel_version, "principal": row["principal"],
                "catalog": row["catalog"], "schema": row["schema"],
                "currentVersionJson": row["current_version_json"]}

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
        _require(type(value) is str and re.fullmatch(r"0|[1-9][0-9]{0,18}", value)
                 and int(value) < 2**63, "Canonical native nonnegative signed64 version required")
        return int(value)

    def _table_infos(self):
        c = self.config
        table_rows = self._query(
            "SELECT table_name,table_type,table_owner FROM system.information_schema.tables "
            "WHERE table_catalog=:catalog AND table_schema=:schema",
            {"catalog": c.catalog, "schema": c.schema}, maximum=len(ROLES) + 1).rows
        _require(len(table_rows) == len(ROLES)
                 and {row.get("table_name") for row in table_rows} == set(ROLES)
                 and all(row.get("table_type") == "MANAGED"
                         and row.get("table_owner") == c.expected_table_owner for row in table_rows),
                 "Only the six expected managed roles with the selected owner may be observed")
        column_rows = self._query(
            "SELECT table_name,column_name,data_type,is_nullable,ordinal_position "
            "FROM system.information_schema.columns WHERE table_catalog=:catalog AND table_schema=:schema "
            "ORDER BY table_name,ordinal_position",
            {"catalog": c.catalog, "schema": c.schema}, maximum=512).rows
        by_role = {role: [] for role in ROLES}
        for row in column_rows:
            role = row.get("table_name")
            _require(role in by_role and row.get("is_nullable") in ("YES", "NO"),
                     "Exact native role columns required")
            by_role[role].append((self._integer(row.get("ordinal_position")), row.get("column_name"),
                                  row.get("data_type"), row.get("is_nullable") == "YES"))
        expected = {role: tuple((name, kind, nullable)
                                for name, kind, nullable in self.carriers.ordered_fields(role))
                    for role in ROLES}
        for role in ROLES:
            actual = by_role[role]
            _require(actual and [item[0] for item in actual] == list(range(1, len(actual) + 1))
                     and tuple((name, kind, nullable) for _, name, kind, nullable in actual)
                     == expected[role], "Observed native columns differ from the UMF 0.10 carrier")
        return by_role

    def _detail(self, role):
        table = ".".join((self.config.catalog, self.config.schema, role))
        quoted = quote_table_identifier(table)
        rows = self._query("DESCRIBE DETAIL " + quoted, {}).rows
        _require(len(rows) == 1, "Unique native Delta table detail required")
        detail = dict(rows[0])
        _require(detail.get("format") == "delta", "Native Delta table required")
        raw_features = detail.get("tableFeatures")
        if type(raw_features) is str:
            try:
                features = json.loads(raw_features, object_pairs_hook=self._unique_pairs)
            except (ValueError, TypeError, json.JSONDecodeError):
                _require(False, "Complete native tableFeatures JSON required")
        else:
            features = raw_features
        _require(type(features) in (list, tuple), "Typed native tableFeatures required")
        protocol_detail = dict(detail)
        protocol_detail["minReaderVersion"] = str(detail.get("minReaderVersion"))
        protocol_detail["minWriterVersion"] = str(detail.get("minWriterVersion"))
        protocol_detail["tableFeatures"] = _canonical(list(features))
        table_uuid = detail.get("id")
        _require(type(table_uuid) is str and str(uuid.UUID(table_uuid)) == table_uuid,
                 "Canonical actual native Delta UUID required")
        protocol = validate_protocol_detail(protocol_detail, uuid=table_uuid,
                                            profile=self.config.protocol_profile)
        digest = hashlib.sha256(_canonical(detail).encode("utf-8")).hexdigest()
        return table_uuid, protocol, digest

    def _observe_inventory(self):
        columns = self._table_infos()
        inventory = {}
        for role in ROLES:
            table = quote_table_identifier(".".join((self.config.catalog, self.config.schema, role)))
            table_uuid, protocol, detail_sha = self._detail(role)
            history = self._query("DESCRIBE HISTORY " + table + " LIMIT 1", {}).rows
            _require(len(history) == 1, "One current Delta history head required per role")
            version = self._integer(history[0].get("version"))
            schema = [[name, {"STRING": "string", "BIGINT": "long",
                              "BOOLEAN": "boolean", "TIMESTAMP": "timestamp"}[kind], nullable]
                      for _, name, kind, nullable in columns[role]]
            inventory[role] = {"name": [self.config.catalog, self.config.schema, role],
                "uuid": table_uuid, "version": version, "schema": schema,
                "protocol": protocol, "detailSha256": detail_sha,
                "owner": self.config.expected_table_owner, "tableType": "MANAGED"}
        _require(len({row["uuid"] for row in inventory.values()}) == len(ROLES),
                 "Six distinct native Delta table identities required")
        return inventory

    def _renew(self):
        runtime = self._observe_runtime()
        inventory = self._observe_inventory()
        _require(runtime == self.opening_runtime and inventory == self.opening,
                 "Warehouse runtime or six-role Unity Catalog identity changed")
        return runtime, inventory

    def close(self):
        """Require an independent full six-role/runtime closing observation."""
        _require(not self.closed, "Managed current receipt reader already closed")
        runtime, inventory = self._renew()
        if self._observation is not None:
            self._observation.runtime_identity["closing"] = runtime
            self._observation.closing_installation_inventory.clear()
            self._observation.closing_installation_inventory.update({
                role: {key: value for key, value in item.items() if key != "detailSha256"}
                for role, item in inventory.items()})
        self.closed = True

    def reopen(self, *, request_bytes, manifest_bytes, expected_receipt):
        """Independently resolve a task receipt before the source holder ACKs.

        `expected_receipt` is the private task result; original request and
        manifest bytes remain caller-owned and are never logged or returned.
        """
        _require(type(request_bytes) is bytes and 0 < len(request_bytes) <= 4194304
                 and type(manifest_bytes) is bytes and 0 < len(manifest_bytes) <= 4194304,
                 "Bounded original request and manifest UTF-8 bytes required")
        request = decode_retained_json(request_bytes)
        expected_manifest = decode_retained_json(manifest_bytes)
        _require(type(request) is dict and type(expected_manifest) is dict
                 and type(expected_receipt) is dict
                 and expected_receipt.get("profile") == "ashlar-managed-current-destination-receipt/0.1"
                 and expected_receipt.get("publicationId") == expected_manifest.get("publication_id")
                 and expected_receipt.get("manifest") == expected_manifest
                 and expected_receipt.get("manifestSha256") == hashlib.sha256(
                     _canonical(expected_manifest).encode("utf-8")).hexdigest(),
                 "Exact original task receipt and manifest bytes required")
        publication_id = expected_manifest.get("publication_id")
        _require(type(publication_id) is str and publication_id and len(publication_id.encode()) <= 1024)
        self._request = request
        self._manifest = expected_manifest
        self._publication_id = publication_id
        observed = self.executor.query(
            "SELECT publication_id,profile_version,table_versions_json,source_progress_json,"
            "schema_revisions_json,validation_report_json,"
            "CAST(unix_micros(recorded_at) AS STRING) AS recorded_at FROM "
            + quote_table_identifier(".".join((self.config.catalog, self.config.schema,
                                                 "publication_manifest")))
            + " WHERE publication_id=:publication_id", {"publication_id": publication_id}).rows
        _require(len(observed) == 1 and dict(observed[0]) == expected_manifest,
                 "Exact task manifest was not independently reopened from Unity Catalog")
        from ashlar.publication import descriptor_from_manifest_row
        descriptor = descriptor_from_manifest_row(dict(observed[0]))
        from ashlar.attempt_store import verify_request_digest
        verify_request_digest(request)
        _require(request.get("request_digest") == expected_receipt.get("requestDigest"),
                 "Receipt request digest differs from the original source intent")
        versions = json.loads(expected_manifest["table_versions_json"], object_pairs_hook=self._unique_pairs)
        role_tables = {".".join((self.config.catalog, self.config.schema, role)): role
                       for role in DATA_ROLES}
        _require(type(versions) is dict and set(versions) == set(role_tables),
                 "Manifest must pin exactly the four distinct graph data roles")
        uuids = {table: self.opening[role]["uuid"] for table, role in role_tables.items()}
        authority = "managed-current-sql:" + self.config.warehouse_id + ":" + self.config.expected_principal
        vector = manifest_pin_vector(expected_manifest, uuids, authority=authority)
        _require(expected_receipt.get("dataRoleVector") == {
            table: {"uuid": uuids[table], "version": versions[table]}
            for table in role_tables}, "Task receipt full four-role vector differs from observed native tables")
        expected_inventory = expected_receipt.get("installationInventory")
        _require(type(expected_inventory) is dict and set(expected_inventory) == set(ROLES),
                 "Task receipt must retain the separate six-role inventory")
        for role in ROLES:
            expected_item = expected_inventory[role]
            actual_item = self.opening[role]
            _require(type(expected_item) is dict and set(expected_item) == {
                "name", "uuid", "version", "schema", "protocol"}
                and expected_item["name"] == ".".join(actual_item["name"])
                and expected_item["uuid"] == actual_item["uuid"]
                and expected_item["version"] == actual_item["version"]
                and expected_item["schema"] == actual_item["schema"]
                and expected_item["protocol"] == actual_item["protocol"],
                "Task receipt six-role name, UUID, head, schema, or protocol differs")
        expected_binding = expected_receipt.get("weftBindingInputs")
        _require(type(expected_binding) is dict and set(expected_binding) == {
            "manifest", "manifestUuid", "dataRoles"}
            and expected_binding["manifest"] == expected_manifest
            and expected_binding["manifestUuid"] == self.opening["publication_manifest"]["uuid"]
            and type(expected_binding["dataRoles"]) is dict
            and set(expected_binding["dataRoles"]) == set(DATA_ROLES),
            "Task receipt independent four-role compiler binding inputs required")
        normalized_binding_roles = {}
        for role in DATA_ROLES:
            table = ".".join((self.config.catalog, self.config.schema, role))
            expected_role = expected_binding["dataRoles"][role]
            observed_role = self.opening[role]
            _require(type(expected_role) is dict and set(expected_role) == {"name", "uuid", "version"}
                     and expected_role["name"] == table
                     and expected_role["uuid"] == observed_role["uuid"]
                     and expected_role["version"] == observed_role["version"],
                     "Task receipt compiler binding role identity differs from live UC")
            normalized_binding_roles[role] = {"name": list(observed_role["name"]),
                "uuid": observed_role["uuid"], "version": observed_role["version"]}
        _require(expected_receipt.get("sourceAuthority") ==
                 f"development Truss {self.config.source_truss_version} page on managed Unity Catalog; external source ACK required",
                 "Task receipt source version differs from the selected external Truss holder")
        revisions = json.loads(request["schema_revisions_json"], object_pairs_hook=self._unique_pairs)
        _require(type(revisions) is dict and revisions
                 and all(type(key) is str and type(value) is str for key, value in revisions.items()),
                 "Exact original accepted UMF revision map required")
        schemas = {table: tuple((name, kind) for name, kind, _nullable in
                               self.carriers.ordered_fields(role))
                   for table, role in role_tables.items()}
        policy = _ManagedReceiptPolicy(self, descriptor)
        backend = _ManagedReceiptBackend(self, policy, schemas)
        resolved = resolve_publication(backend, publication_id, uuids, context=self.context,
            supported_profiles=(self.config.carrier_profile,),
            supported_revisions={key: (value,) for key, value in revisions.items()})
        _require(dict(resolved.descriptor.raw) == expected_manifest
                 and all(snapshot.uuid == uuids[table]
                         and snapshot.version == versions[table]
                         for table, snapshot in resolved.snapshots.items()),
                 "Independent native resolver differs from the exact four-role publication")
        from .truss_feed_sessions import encode_truss_destination_receipt
        source_receipt_bytes = encode_truss_destination_receipt(request_bytes, manifest_bytes, vector)
        installation = {role: {key: value for key, value in item.items()
                               if key != "detailSha256"}
                        for role, item in self.opening.items()}
        observation = ManagedCurrentReceiptObservation(PROFILE, publication_id,
            expected_manifest, expected_receipt["manifestSha256"], source_receipt_bytes,
            hashlib.sha256(source_receipt_bytes).hexdigest(), vector, backend,
            installation, {},
            {"manifest": expected_manifest,
             "manifestUuid": self.opening["publication_manifest"]["uuid"],
             "dataRoles": normalized_binding_roles},
            {"opening": self.opening_runtime, "closing": None},
            supported_profiles=(self.config.carrier_profile,),
            supported_revisions={key: (value,) for key, value in revisions.items()})
        self._observation = observation
        return observation


class _ManagedReceiptPolicy:
    def __init__(self, owner, descriptor):
        self.owner = owner
        self.descriptor = descriptor

    def authorize(self, context, publication_id, tables):
        _require(context is self.owner.context and publication_id == self.owner._publication_id
                 and set(tables) == {".".join((self.owner.config.catalog,
                     self.owner.config.schema, role)) for role in DATA_ROLES})
        # This is the resolver/ACK admission barrier: all six roles, including
        # publication_attempt_phase, must still be the opening installation.
        self.owner._renew()
        return None

    def validate_descriptor(self, descriptor, context):
        from ashlar.source_checkpoint import bind_source_descriptor
        _require(context is self.owner.context
                 and dict(descriptor.raw) == self.owner._manifest
                 and descriptor.publication_id == self.owner._publication_id)
        bind_source_descriptor(self.owner._request, descriptor,
                               expected_publication_id=self.owner._publication_id)
        _require(self.owner._manifest is not None
                 and self.owner._manifest == dict(descriptor.raw),
                 "Source-bound descriptor changed during admission")
        return None

    def validate_snapshot(self, table, table_uuid, version, columns):
        role = table.rsplit(".", 1)[-1]
        expected = self.owner.opening.get(role)
        _require(expected is not None and expected["uuid"] == table_uuid
                 and type(version) is int and 0 <= version <= expected["version"]
                 and tuple(columns) == tuple((name, kind) for name, kind, _nullable
                                             in self.owner.carriers.ordered_fields(role)),
                 "Pinned native snapshot UUID, version, or UMF column types differ")
        return None


class _ManagedReceiptBackend:
    """Bound resolver port with one full six-role renewal per admission.

    NativeBackend performs repeated per-role owner/detail scans that would
    exceed the deliberately small source-ACK statement budget. Here the full
    inventory renewal is the authority barrier, while each consumed snapshot
    is still bracketed by actual table identity and a real versioned LIMIT 0.
    """
    def __init__(self, owner, policy, schemas):
        self.owner = owner
        self.policy = policy
        self.manifest_table = ".".join((owner.config.catalog, owner.config.schema,
                                        "publication_manifest"))
        self.manifest_uuid = owner.opening["publication_manifest"]["uuid"]
        self.schemas = schemas

    def authorize(self, context, publication_id, tables):
        return self.policy.authorize(context, publication_id, tables)

    def descriptors(self, publication_id):
        before, protocol, _ = self.owner._detail("publication_manifest")
        if before != self.manifest_uuid or protocol != self.owner.opening[
                "publication_manifest"]["protocol"]:
            raise ValueError("Publication manifest replaced during admission")
        rows = self.owner.executor.query(
            "SELECT publication_id,profile_version,table_versions_json,source_progress_json,"
            "schema_revisions_json,validation_report_json,"
            "CAST(unix_micros(recorded_at) AS STRING) AS recorded_at FROM "
            + quote_table_identifier(self.manifest_table)
            + " WHERE publication_id=:publication_id", {"publication_id": publication_id}).rows
        after, after_protocol, _ = self.owner._detail("publication_manifest")
        if (after, after_protocol) != (before, protocol):
            raise ValueError("Publication manifest changed during descriptor read")
        return rows

    def validate_descriptor(self, descriptor, context):
        return self.policy.validate_descriptor(descriptor, context)

    def validate_manifest(self, manifest, context):
        # TrussFeedSessions calls this after each resolution as an explicit
        # effects-admission hook. The immediately preceding resolve already
        # crossed `authorize`, which renewed all six role identities.
        _require(context is self.owner.context and type(manifest) is dict
                 and manifest == self.owner._manifest,
                 "Only the exact independently reopened manifest is admitted")
        return None

    def inspect_snapshot(self, table, version):
        role = table.rsplit(".", 1)[-1]
        expected = self.owner.opening.get(role)
        if (expected is None or table != ".".join(expected["name"])
                or type(version) is not int or not 0 <= version <= expected["version"]
                or table not in self.schemas):
            raise ValueError("Snapshot is outside the renewed six-role installation")
        before, protocol, _ = self.owner._detail(role)
        if before != expected["uuid"] or protocol != expected["protocol"]:
            raise ValueError("Pinned graph table changed before snapshot inspection")
        result = self.owner.executor.query("SELECT * FROM " + quote_table_identifier(table)
            + " VERSION AS OF " + str(version) + " LIMIT 0", {})
        if result.rows or tuple(result.columns) != tuple(self.schemas[table]):
            raise ValueError("Pinned native snapshot schema mismatch")
        self.policy.validate_snapshot(table, before, version, result.columns)
        after, after_protocol, _ = self.owner._detail(role)
        if (after, after_protocol) != (before, protocol):
            raise ValueError("Pinned graph table changed during snapshot inspection")
        return Snapshot(table, before, version)


class _ReceiptSQLExecutor:
    """One deadline and SQL port for inventory and NativeBackend statements."""
    def __init__(self, owner):
        self.owner = owner

    def query(self, sql, parameters):
        return self.owner._native_query(sql, parameters)


@contextmanager
def open_managed_current_receipt_read(sql_config, *, context, config):
    """Acquire only the existing managed SQL read transport; never create a warehouse."""
    from .managed_sql_read import open_managed_sql_read
    from .lifecycle import finish
    with open_managed_sql_read(sql_config, context=context) as sql_reader:
        owner = ManagedCurrentSQLReceiptReader(sql_reader, config)
        primary = None
        try:
            yield owner
        except BaseException as error:
            primary = error
        finish(primary, [owner.close])
