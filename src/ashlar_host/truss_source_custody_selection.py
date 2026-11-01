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
import re
import weakref
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
_SELECTIONS = weakref.WeakValueDictionary()
_EXPECTED_POST_STATES = weakref.WeakValueDictionary()
_LOCAL_NATIVE_OBSERVATIONS = weakref.WeakValueDictionary()

LOCAL_ROW_BAG_CANONICALIZATION = "ashlar-local-native-row-bag/0.1"
LOCAL_EXPECTED_POST_STATE_PROFILE = "ashlar-local-native-expected-post-state/0.1"
LOCAL_NATIVE_OBSERVATION_PROFILE = "ashlar-local-native-delta-publication-observation/0.1"
_LOCAL_DATA_ROLES = ("object_current", "edge_current", "tombstone", "whole_source_history")


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


def _issue_local_native_delta_publication_observation(*, publication_id,
        manifest_bytes, manifest_sha256, vector, receipt_bytes, receipt_sha256, roles):
    """Issue the closed observation after the caller's independent native reads."""
    value = LocalNativeDeltaPublicationObservation(
        profile=LOCAL_NATIVE_OBSERVATION_PROFILE, publication_id=publication_id,
        manifest_bytes=manifest_bytes, manifest_sha256=manifest_sha256,
        vector=vector, receipt_bytes=receipt_bytes, receipt_sha256=receipt_sha256,
        roles=MappingProxyType(dict(roles)))
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
                CURRENT_052_CUSTODY_PROFILE, installed_truss_052_source_custody_profile)
            package_version = importlib.metadata.version("umf-truss")
            installed_profile = installed_truss_052_source_custody_profile()
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
              and package_version == "0.0.52"
              and installed_profile == CURRENT_052_CUSTODY_PROFILE
              and getattr(FeedSourceSelection, "__name__", None) == "FeedSourceSelection",
              "Truss source custody is gated to the reviewed .75 candidate manifest")
        return FeedSourceSelection(
            page=self.page, maximum_facts=self.maximum_facts,
            maximum_bytes=self.maximum_bytes, request_bytes=self.request_bytes,
            destination_registration_bytes=self.destination_registration_bytes,
            ack_scope_bytes=self.ack_scope_bytes, destination_id=self.destination_id,
            request_sha256=self.request_sha256, publication_id=self.publication_id)


def select_truss_source_custody_anchor(*, schedule: R04Schedule, source: str,
                                       page: object, request_bytes: bytes,
                                       destination_id: str) -> TrussSourceCustodyAnchor:
    """Freeze Ashlar-owned canonical registration/scope bytes before publication.

    ``schedule`` and ``page`` are correspondence inputs. Their independent
    admission remains the R04 schedule policy and public Truss source owner.
    """
    _need(type(schedule) is R04Schedule and source in ("A", "B"),
          "Exact frozen R04 schedule and source slot required")
    _need(type(request_bytes) is bytes and 0 < len(request_bytes) <= _MAX_BYTES,
          "Exact bounded original request bytes required")
    destination_id = _bounded_text(destination_id, "selected destination ID", 1024)
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
    verify_request_digest(request)
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
        ack_scope_bytes=scope_bytes))


def select_truss_local_dev_source_custody_anchor(
        *, registration: object, consumer: object, runtime_status: object,
        page: object, feed: str, source_holder_id: str, document_id: str,
        destination_id: str,
        ack_registration_id: str, ack_scope_id: str, publication_id: str,
        request_bytes: bytes, predecessor_id: str) -> TrussSourceCustodyAnchor:
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
    except ImportError as error:
        raise TrussSourceSelectionError("Public Truss .52 source result types required") from error
    if (type(registration) is not CatalogRegistration
            or type(consumer) is not FeedConsumer
            or type(runtime_status) is not CatalogRuntimeStatus
            or type(page) is not FeedPage):
        raise TrussSourceSelectionError("Exact public Truss acceptance, consumer, status and page required")
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
        if (report["registrationId"] != registration.registration_id
                or report["revision"] != registration.revision
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
                or type(artifact.get("identity")) is not str or not artifact.get("identity")
                or type(artifact.get("bytesBase64")) is not str):
            raise ValueError("Source-holder document identity, revision and digest required")
        import base64
        document_bytes = base64.b64decode(artifact["bytesBase64"], validate=True)
        if hashlib.sha256(document_bytes).hexdigest() != model_sha256:
            raise ValueError("Accepted source-holder document bytes differ from report digest")
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
        preselected_publication_id=publication_id))
