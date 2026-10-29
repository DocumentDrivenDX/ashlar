# SPDX-License-Identifier: MIT OR Apache-2.0
"""Person-scoped managed Backend02 page owner over an existing publication.

The caller supplies an authenticated Spark session and a mandatory current
authorization policy.  This module opens no compute, publishes nothing, and
cannot turn the campaign source oracle into ordinary query authority.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import contextmanager
import hashlib
import json

from ashlar.native import SQLResult, quote_table_identifier
from .lifecycle import finish
from .managed_current_query import ManagedCurrentReadOwner, _canonical, _require


class ManagedCurrentPersonPolicy(ABC):
    """Trusted host port for effective row/column rights of one current person.

    Every method must fail closed.  The policy digest must change when effective
    rights change; the native Spark session itself must run as this person so UC
    applies its current row filters and column masks to every SQL statement.
    """

    @abstractmethod
    def open(self, *, principal, publication, vector):
        """Authorize opening and return a lowercase SHA-256 policy revision."""

    @abstractmethod
    def renew(self, *, principal, publication, vector):
        """Recheck effective rights and return the same revision or refuse."""

    @abstractmethod
    def authorize_page(self, *, principal, request, artifact, publication):
        """Authorize exact source fields, predicates, ordering and output."""

    @abstractmethod
    def authorize_rows(self, *, principal, rows, descriptor, artifact):
        """Authorize complete decoded rows before typed release."""

    @abstractmethod
    def close(self, *, principal, publication, vector):
        """Recheck and close custody after the final native read."""


class ManagedCurrentPersonPageOwner(ManagedCurrentReadOwner):
    """Closed `.4-xxhash64` page owner; no oracle or fixture expected rows.

    Only the inherited Backend02 ``backend02_target`` constructor is exposed.
    Original source/receipt bindings are rechecked by ``admit_artifact``.  The
    source bundle is development authority until a production source owner is
    separately qualified.
    """

    def __init__(self, spark, *, target_owner, receipt, source_bundle,
                 expected_environment, person_policy, maximum_rows=1000):
        from .managed_current_core import CARRIER_PROFILE_XXHASH64, LOOKUP_HASH_PROFILE_XXHASH64
        from .managed_current_publication import (ManagedCurrentTargetOwner,
                                                  observe_managed_current_environment)
        from .managed_current_weft_binding import _validated_inputs
        _require(spark is not None and type(target_owner) is ManagedCurrentTargetOwner
                 and target_owner.spark is spark and type(receipt) is dict
                 and type(source_bundle) is dict and type(expected_environment) is dict
                 and type(person_policy) is not ManagedCurrentPersonPolicy
                 and isinstance(person_policy, ManagedCurrentPersonPolicy)
                 and type(maximum_rows) is int and 1 <= maximum_rows <= 1000,
                 "Authenticated managed person, policy and finite row cap required")
        _require(target_owner.carrier_profile == CARRIER_PROFILE_XXHASH64
                 and target_owner.carriers.lookup_hash_profile == LOOKUP_HASH_PROFILE_XXHASH64
                 and source_bundle.get("destination", {}).get("physicalProfile") == CARRIER_PROFILE_XXHASH64,
                 "Exact managed xxhash64 carrier profile required")
        observed = observe_managed_current_environment(spark, target_owner.profile)
        _require(observed == expected_environment
                 and observed["runtimeName"] == "databricks-serverless"
                 and observed["catalog"] == target_owner.catalog
                 and observed["schema"] == target_owner.schema,
                 "Person session, selected managed runtime and namespace differ")
        self.spark, self.targets = spark, target_owner
        self.receipt, self.source_bundle = json.loads(_canonical(receipt)), json.loads(_canonical(source_bundle))
        self.expected_environment = dict(expected_environment)
        self.principal, self.person_policy = observed["principal"], person_policy
        self.maximum_rows, self.carrier_profile = maximum_rows, CARRIER_PROFILE_XXHASH64
        self.context, self.active, self.page_token = object(), False, None
        self.current_request, self.current_artifact = None, None
        self.opening = self.targets.open_existing()
        manifest, manifest_uuid, names, data_roles, *_ = _validated_inputs(
            self.source_bundle, self.receipt.get("weftBindingInputs"))
        _require(self.source_bundle["source"]["trussVersion"] == "0.0.50",
                 "Selected current Truss 0.0.50 source pin required")
        self._validate_person_receipt(manifest, manifest_uuid, names, data_roles)
        self.vector = self._pin_vector()
        revision = self.person_policy.open(principal=self.principal,
            publication=self.receipt["manifest"], vector=self.vector)
        _require(type(revision) is str and len(revision) == 64
                 and all(c in "0123456789abcdef" for c in revision),
                 "Current effective policy revision required")
        self.policy_revision = revision
        self.backend = self._native_backend()
        self.active = True
        try:
            finish(None, [self._renew, lambda: _require(
                self.person_policy.close(principal=self.principal,
                    publication=self.receipt["manifest"], vector=self.vector) is None,
                "Person policy close must complete")])
        finally:
            self.active = False

    def _validate_person_receipt(self, manifest, manifest_uuid, names, data_roles):
        from .managed_current_core import ROLES
        from .managed_current_core import ManagedCurrentCoreConfig, restore_managed_current_admission
        from .managed_current_weft_binding import _target_for
        from ashlar.source_checkpoint import truss_checkpoint
        receipt = self.receipt
        _require(set(receipt) == {"profile", "publicationId", "requestDigest", "manifest",
                 "manifestSha256", "dataRoleVector", "weftBindingInputs",
                 "installationInventory", "sourceAuthority"}
                 and receipt["profile"] == "ashlar-managed-current-destination-receipt/0.1"
                 and receipt["manifest"] == manifest
                 and receipt["publicationId"] == manifest["publication_id"]
                 and receipt["manifestSha256"] == hashlib.sha256(_canonical(manifest).encode()).hexdigest()
                 and receipt["sourceAuthority"] ==
                    "development Truss 0.0.50 page on managed Unity Catalog; external source ACK required",
                 "Exact retained managed publication receipt required")
        report = json.loads(manifest["validation_report_json"])
        _require(type(report) is dict and report.get("complete") is True
                 and report.get("request_digest") == receipt["requestDigest"],
                 "Retained request digest differs from held publication")
        inventory = receipt["installationInventory"]
        _require(type(inventory) is dict and set(inventory) == set(ROLES),
                 "Complete managed installation inventory required")
        for role in ROLES:
            observed, item = self.opening[role], inventory[role]
            _require(type(item) is dict and item.get("uuid") == observed.uuid
                     and tuple(item.get("name", ())) == self.targets.names[role]
                     and item.get("version") == observed.version
                     and tuple(tuple(x) for x in item.get("schema", ())) == observed.schema
                     and item.get("protocol") == observed.protocol,
                     "Managed installation differs from retained publication receipt")
        _require(inventory["publication_manifest"]["uuid"] == manifest_uuid)
        role_names = {".".join(names[role]): role for role in data_roles}
        _require(type(receipt["dataRoleVector"]) is dict
                 and set(receipt["dataRoleVector"]) == set(role_names))
        for table, role in role_names.items():
            observed = self.opening[role]
            _require(receipt["dataRoleVector"][table] ==
                     {"uuid": observed.uuid, "version": observed.version}
                     and data_roles[role]["uuid"] == observed.uuid
                     and data_roles[role]["version"] == observed.version)
        binding = receipt["weftBindingInputs"]
        _require(binding["manifest"] == manifest and binding["manifestUuid"] == manifest_uuid)
        from .managed_current_core import CARRIER_PROFILE_XXHASH64
        source = self.source_bundle
        targets = {surface: _target_for(source, surface, CARRIER_PROFILE_XXHASH64)
                   for surface in ("Backend03", "Backend02")}
        cfg = ManagedCurrentCoreConfig(self.targets.catalog, self.targets.schema,
            targets["Backend03"], targets["Backend02"], 0, 0,
            carrier_profile=CARRIER_PROFILE_XXHASH64)
        admission = restore_managed_current_admission(source, cfg)
        progress = json.loads(manifest["source_progress_json"])
        _require(progress.get(admission.batch.feed) == truss_checkpoint(admission.batch),
                 "Held publication progress differs from the accepted source page")
        actual = self.targets.transport.query(
            "SELECT publication_id,profile_version,table_versions_json,source_progress_json,"
            "schema_revisions_json,validation_report_json,"
            "cast(unix_micros(recorded_at) AS STRING) AS recorded_at FROM "
            + quote_table_identifier(".".join(self.targets.names["publication_manifest"]))
            + " WHERE publication_id=:id", {"id": receipt["publicationId"]}).rows
        _require(len(actual) == 1 and dict(actual[0]) == manifest,
                 "Exact held manifest must reopen under the current person")

    def _renew(self):
        from .managed_current_publication import observe_managed_current_environment
        _require(self.active and observe_managed_current_environment(self.spark, self.targets.profile)
                 == self.expected_environment,
                 "Current person or managed runtime changed during read")
        current = self.targets.observe()
        _require(set(current) == set(self.opening))
        for role, before in self.opening.items():
            after = current[role]
            _require((before.uuid, before.version, before.schema, before.protocol)
                     == (after.uuid, after.version, after.schema, after.protocol),
                     "Held managed table identity or head changed during read")
        _require(self.person_policy.renew(principal=self.principal,
            publication=self.receipt["manifest"], vector=self.vector) == self.policy_revision,
            "Current effective policy changed during read")

    @contextmanager
    def hold(self, vector, *, context):
        _require(context is self.context and vector == self.vector and not self.active)
        revision = self.person_policy.open(principal=self.principal,
            publication=self.receipt["manifest"], vector=self.vector)
        if revision != self.policy_revision:
            finish(ValueError("Effective person policy changed before page"),
                [lambda: self.person_policy.close(principal=self.principal,
                    publication=self.receipt["manifest"], vector=self.vector)])
        self.active = True
        primary = None
        try:
            self._renew()
            try:
                yield vector
            except BaseException as error:
                primary = error
        except BaseException as error:
            primary = error
        try:
            finish(primary, [self._renew,
                lambda: _require(self.person_policy.close(principal=self.principal,
                    publication=self.receipt["manifest"], vector=self.vector) is None,
                    "Person policy close must complete")])
        finally:
            self.active = False

    hold_page = hold

    def admit_artifact(self, request, artifact, descriptor, context):
        from .managed_current_weft_binding import XXHASH64_BACKEND02_VERSION
        target = request.get("target") if type(request) is dict else None
        _require(type(target) is dict and target.get("backendVersion") == XXHASH64_BACKEND02_VERSION,
                 "Only selected managed Backend02 pages are admitted")
        self._renew()
        _require(self.person_policy.authorize_page(principal=self.principal, request=request,
            artifact=artifact, publication=self.receipt["manifest"]) is None,
            "Current policy must complete page authorization")
        return super().admit_artifact(request, artifact, descriptor, context)

    def page_context(self, descriptor, vector, context):
        self.bind_descriptor(descriptor, vector, context)
        if self.page_token is None:
            self.page_token = _canonical({"publicationId": descriptor.publication_id,
                "vector": {table: list(pair) for table, pair in vector.targets.items()},
                "principal": self.principal, "policyRevision": self.policy_revision,
                "runtime": self.expected_environment}).encode()
        return bytes(self.page_token)

    def authorize_result(self, rows, descriptor, context):
        self.bind_descriptor(descriptor, self.vector, context)
        _require(self.current_artifact is not None)
        _require(self.person_policy.authorize_rows(principal=self.principal, rows=rows,
            descriptor=descriptor, artifact=self.current_artifact) is None,
            "Current policy must complete result authorization")
        self._renew()

    def query(self, sql, parameters):
        self._renew()
        _require(self.current_artifact is not None and type(sql) is str
                 and type(parameters) is dict)
        artifact = self.current_artifact
        slots = {"p" + str(slot["position"]): slot["value"]
                 for slot in artifact.get("parameters", [])}
        allowed = [artifact.get("sql")]
        for obligation in artifact.get("obligations", []):
            allowed.extend(check.get("sql") for check in
                obligation.get("parameters", {}).get("checks", []) if type(check) is dict)
        _require(sql in allowed and parameters == slots,
                 "Only exact compiler SQL and parameter carriers may execute")
        frame = self.spark.sql(sql, args=parameters)
        rows = frame.limit(self.maximum_rows + 1).collect()
        _require(len(rows) <= self.maximum_rows, "Person page row cap exceeded")
        type_names = {"string": "STRING", "long": "BIGINT", "integer": "INT",
                      "boolean": "BOOLEAN", "timestamp": "TIMESTAMP",
                      "double": "DOUBLE", "decimal": "DECIMAL"}
        columns = tuple((field.name, type_names.get(field.dataType.simpleString(),
                       field.dataType.simpleString().upper())) for field in frame.schema.fields)
        self._renew()
        return SQLResult([row.asDict(recursive=True) for row in rows], columns)

    def run_backend03(self, *args, **kwargs):
        raise ValueError("Person page owner does not admit Backend03 grouped queries")

    def run_backend03_case(self, *args, **kwargs):
        raise ValueError("Person page owner cannot produce campaign case receipts")

    def run_backend02_page(self, *args, **kwargs):
        raise ValueError("Person page owner cannot use fixture expected rows")

    def run_backend02_two_pages(self, *args, **kwargs):
        raise ValueError("Person page owner cannot produce campaign case receipts")
