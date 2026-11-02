# SPDX-License-Identifier: MIT OR Apache-2.0
"""Closed IPC codec for exact Truss .53 A1 page and accepted-source evidence.

The decoded snapshot contains plain evidence only. It cannot issue Truss
selection, custody, publication, or ACK authority.
"""
from __future__ import annotations

import base64
from dataclasses import dataclass
import hashlib
import json
from types import MappingProxyType, SimpleNamespace

from .r04_schedule import canonical
from .truss_detached_source_codec import (
    DetachedTrussSourcePage, decode_detached_source_page,
    encode_detached_source_page,
)


PROFILE = "ashlar-truss-detached-a1-source-snapshot/0.1"
MAX_REPORT_BYTES = 4 * 1024 * 1024
MAX_VIEW_BYTES = 8 * 1024 * 1024
MAX_DOCUMENT_BYTES = 16 * 1024 * 1024
MAX_SNAPSHOT_BYTES = 48 * 1024 * 1024
_TOP_KEYS = {"profile", "source_page_bytes_base64", "catalog_revision",
    "catalog_registration_id", "catalog_registration_bytes_base64",
    "report_bytes_base64", "view_bytes_base64", "progress", "documents",
    "binding_sha256"}
_PROGRESS_KEYS = {"feed", "epoch", "position", "position_sha256",
    "catalog_registration_sha256", "consumer_id", "consumer_registration_id",
    "installation_id", "cursor_bytes_base64"}
_DOCUMENT_KEYS = {"document_id", "document_revision", "artifact_identity",
    "registration_id", "catalog_revision", "sha256", "content_base64"}


class DetachedSourceSnapshotError(ValueError):
    """A detached exact Truss source snapshot is malformed or inconsistent."""


def _b64(raw):
    return base64.b64encode(raw).decode("ascii")


def _decode64(value, label, maximum):
    if type(value) is not str or len(value) > 4 * ((maximum + 2) // 3):
        raise DetachedSourceSnapshotError(f"Bounded {label} bytes required")
    try:
        raw = base64.b64decode(value, validate=True)
    except (ValueError, TypeError) as error:
        raise DetachedSourceSnapshotError(f"Exact {label} base64 required") from error
    if len(raw) > maximum or _b64(raw) != value:
        raise DetachedSourceSnapshotError(f"Canonical bounded {label} bytes required")
    return raw


def _json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate member")
            result[key] = value
        return result
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs,
                           parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    except (UnicodeError, ValueError, json.JSONDecodeError) as error:
        raise DetachedSourceSnapshotError("Closed exact source snapshot JSON required") from error
    if type(value) is not dict:
        raise DetachedSourceSnapshotError("Source snapshot object required")
    return value


def _need(condition, message):
    if not condition:
        raise DetachedSourceSnapshotError(message)


def _page_fields(page):
    return (page.content, page.sha256, page.prior.content, page.next.content,
            page.consumer_id, page.registration_id, page.writer_xid,
            page.safe_watermark_xid, page.outcome)


@dataclass(frozen=True)
class DetachedAcceptedSourceDocument:
    document_id: str
    document_revision: str
    artifact_identity: str
    registration_id: str
    catalog_revision: str
    sha256: str
    content: bytes


@dataclass(frozen=True)
class DetachedTrussSourceSnapshot:
    """Detached page, selected catalog, accepted report/view and source bytes."""

    source_page: DetachedTrussSourcePage
    catalog_revision: str
    catalog_registration_id: str
    catalog_registration_bytes: bytes
    report_bytes: bytes
    view_bytes: bytes
    progress: object
    documents: tuple[DetachedAcceptedSourceDocument, ...]

    def current_catalog_evidence(self):
        """Recreate plain .53 evidence; this does not create source authority."""
        from .truss_current_catalog import (
            CURRENT_TRUSS_053_FRESH_VERSION, CURRENT_TRUSS_053_FRESH_WHEEL_SHA256,
            CURRENT_TRUSS_053_FRESH_RUNTIME_MANIFEST_SHA256, CurrentCatalogEvidence,
        )
        sources = {}
        for item in self.documents:
            sources[(self.catalog_revision, item.document_id)] = SimpleNamespace(
                document_id=item.document_id,
                document_revision=item.document_revision,
                artifact_identity=item.artifact_identity,
                registration_id=item.registration_id,
                catalog_revision=int(item.catalog_revision),
                sha256=item.sha256,
                content=item.content)
        return CurrentCatalogEvidence(
            CURRENT_TRUSS_053_FRESH_VERSION,
            {self.catalog_revision: self.report_bytes},
            {self.catalog_revision: self.view_bytes}, sources,
            CURRENT_TRUSS_053_FRESH_WHEEL_SHA256,
            CURRENT_TRUSS_053_FRESH_RUNTIME_MANIFEST_SHA256)


def _extract(anchor, observation):
    from .managed_current_truss053_genesis_owner import (
        ManagedCurrentTruss053SourceObservation, _is_issued_source_observation,
    )
    from .managed_current_genesis import TrussSourceGenesisProgress
    from .truss_current_catalog import (
        CURRENT_TRUSS_053_FRESH_VERSION, CURRENT_TRUSS_053_FRESH_WHEEL_SHA256,
        CURRENT_TRUSS_053_FRESH_RUNTIME_MANIFEST_SHA256, CurrentCatalogEvidence,
        parse_current_catalog_json,
    )
    from truss.reader_catalog import AcceptedDocument
    from .truss_source_custody_selection import TrussSourceCustodyAnchor, _ANCHORS, _issued

    _need(type(anchor) is TrussSourceCustodyAnchor and _issued(_ANCHORS, anchor)
          and anchor.authority_profile == "r04-scheduled",
          "Issued prepublication source anchor required")
    _need(type(observation) is ManagedCurrentTruss053SourceObservation
          and _is_issued_source_observation(observation),
          "Issued public Truss .53 source observation required")
    _need(observation.source == anchor.source,
          "Detached source observation differs from issued A/B anchor")
    _need(type(observation.catalog_evidence) is CurrentCatalogEvidence
          and observation.catalog_evidence.truss_version == CURRENT_TRUSS_053_FRESH_VERSION
          and (observation.catalog_evidence.runtime_wheel_sha256,
               observation.catalog_evidence.runtime_manifest_sha256)
             == (CURRENT_TRUSS_053_FRESH_WHEEL_SHA256,
                 CURRENT_TRUSS_053_FRESH_RUNTIME_MANIFEST_SHA256),
          "Exact fresh Truss .53 catalog evidence required")
    progress = observation.progress
    _need(type(progress) is TrussSourceGenesisProgress,
          "Exact admitted Truss .53 genesis progress required")
    _need((progress.feed, progress.epoch, progress.installation_id,
           progress.consumer_id, progress.consumer_registration_id)
          == (anchor.identity.get("feed"), anchor.identity.get("epoch"),
              anchor.identity.get("installation_id"), anchor.identity.get("consumer_id"),
              anchor.identity.get("registration_id")),
          "Truss page and source progress identities differ from selected anchor")
    _need(_page_fields(observation.page) == _page_fields(anchor.page)
          and progress.cursor_bytes == anchor.page.prior.content
          and progress.position == _b64(progress.cursor_bytes)
          and hashlib.sha256(progress.cursor_bytes).hexdigest() == progress.position_sha256,
          "Truss page, cursor, and accepted source evidence are not one selection")

    evidence = observation.catalog_evidence
    _need(type(evidence.reports) is dict and len(evidence.reports) == 1
          and type(evidence.views) is dict and set(evidence.views) == set(evidence.reports),
          "One exact accepted catalog revision required")
    revision, report_bytes = next(iter(evidence.reports.items()))
    view_bytes = evidence.views[revision]
    _need(observation.report_bytes == report_bytes
          and observation.catalog_view_bytes == view_bytes,
          "Observation bytes differ from the admitted .53 catalog evidence")
    _need(type(revision) is str and revision.isascii() and revision.isdecimal()
          and str(int(revision)) == revision and int(revision) > 0
          and type(report_bytes) is bytes and 0 < len(report_bytes) <= MAX_REPORT_BYTES
          and type(view_bytes) is bytes and 0 < len(view_bytes) <= MAX_VIEW_BYTES,
          "Bounded exact report and complete-view bytes required")
    report = parse_current_catalog_json(report_bytes)
    view = parse_current_catalog_json(view_bytes)
    _need(report.get("registrationId") == progress.catalog_registration_id
          and str(report.get("revision")) == revision
          and view.get("catalogRevision") == revision,
          "Selected catalog registration and revision differ from report/view")
    _need(view.get("interfaceVersion") == "truss-catalog-view/0.2.0"
          and view.get("scope") == {"kind": "complete"},
          "Complete public Truss catalog view required")
    registration_bytes = progress.catalog_registration_bytes
    _need(type(registration_bytes) is bytes and registration_bytes
          and hashlib.sha256(registration_bytes).hexdigest()
             == progress.catalog_registration_sha256,
          "Exact selected catalog registration bytes required")

    accepted = report.get("acceptedInput", {}).get("documents")
    _need(type(accepted) is list and bool(accepted),
          "Exact accepted document inventory required")
    document_ids = [item.get("documentId") for item in accepted if type(item) is dict]
    _need(len(document_ids) == len(accepted)
          and all(type(item) is str and item for item in document_ids)
          and len(set(document_ids)) == len(document_ids),
          "Distinct exact accepted document inventory required")
    _need(type(observation.accepted_documents) is dict
          and set(observation.accepted_documents) == set(document_ids),
          "Public accepted documents differ from selected report inventory")
    _need(set(evidence.sources) == {(revision, item["documentId"]) for item in accepted},
          "Catalog evidence source bytes differ from selected report inventory")
    documents = []
    for item in accepted:
        document_id = item["documentId"]
        artifact = item["artifact"]
        _need(type(item.get("documentRevision")) is str and item["documentRevision"]
              and type(artifact) is dict
              and type(artifact.get("identity")) is str and artifact["identity"]
              and type(artifact.get("sha256")) is str
              and len(artifact["sha256"]) == 64,
              "Exact accepted document revision and artifact pin required")
        document = observation.accepted_documents[document_id]
        source = evidence.sources[(revision, document_id)]
        expected = (document_id, item["documentRevision"], artifact["identity"],
                    progress.catalog_registration_id, revision, artifact["sha256"])
        actual = (document.document_id, document.document_revision,
                  document.artifact_identity, document.registration_id,
                  str(document.catalog_revision), document.sha256)
        _need(type(document) is AcceptedDocument and actual == expected
              and type(document.content) is bytes
              and 0 < len(document.content) <= MAX_DOCUMENT_BYTES
              and hashlib.sha256(document.content).hexdigest() == document.sha256,
              "Public accepted-document registration, revision, identity, or content differs")
        _need((source.document_id, source.document_revision, source.artifact_identity,
               source.registration_id, str(source.catalog_revision), source.sha256,
               source.content) == (*expected, document.content),
              "Catalog source bytes differ from exact public accepted document")
        documents.append(DetachedAcceptedSourceDocument(*expected, document.content))
    return revision, registration_bytes, report_bytes, view_bytes, progress, tuple(documents)


def encode_detached_source_snapshot(anchor, observation) -> bytes:
    """Encode issued page anchor plus its exact public .53 source observation."""
    revision, registration_bytes, report_bytes, view_bytes, progress, documents = (
        _extract(anchor, observation))
    page_bytes = encode_detached_source_page(anchor)
    body = {
        "profile": PROFILE,
        "source_page_bytes_base64": _b64(page_bytes),
        "catalog_revision": revision,
        "catalog_registration_id": progress.catalog_registration_id,
        "catalog_registration_bytes_base64": _b64(registration_bytes),
        "report_bytes_base64": _b64(report_bytes),
        "view_bytes_base64": _b64(view_bytes),
        "progress": {
            "feed": progress.feed, "epoch": progress.epoch,
            "position": progress.position, "position_sha256": progress.position_sha256,
            "catalog_registration_sha256": progress.catalog_registration_sha256,
            "consumer_id": progress.consumer_id,
            "consumer_registration_id": progress.consumer_registration_id,
            "installation_id": progress.installation_id,
            "cursor_bytes_base64": _b64(progress.cursor_bytes),
        },
        "documents": [{
            "document_id": item.document_id,
            "document_revision": item.document_revision,
            "artifact_identity": item.artifact_identity,
            "registration_id": item.registration_id,
            "catalog_revision": item.catalog_revision,
            "sha256": item.sha256,
            "content_base64": _b64(item.content),
        } for item in documents],
    }
    _need(len(documents) > 0 and sum(len(item.content) for item in documents)
          <= MAX_SNAPSHOT_BYTES, "Bounded accepted source document inventory required")
    envelope = {**body, "binding_sha256": hashlib.sha256(canonical(body)).hexdigest()}
    raw = canonical(envelope)
    _need(len(raw) <= MAX_SNAPSHOT_BYTES, "Detached source snapshot exceeds its bound")
    return raw


def decode_detached_source_snapshot(raw: bytes) -> DetachedTrussSourceSnapshot:
    """Decode closed exact bytes into plain evidence without Truss authority."""
    _need(type(raw) is bytes and 0 < len(raw) <= MAX_SNAPSHOT_BYTES,
          "Bounded exact detached source snapshot bytes required")
    value = _json(raw)
    _need(set(value) == _TOP_KEYS and canonical(value) == raw
          and value["profile"] == PROFILE,
          "Closed canonical detached source snapshot required")
    binding = value.pop("binding_sha256")
    _need(type(binding) is str and len(binding) == 64
          and hashlib.sha256(canonical(value)).hexdigest() == binding,
          "Detached source snapshot binding digest differs")
    page_raw = _decode64(value["source_page_bytes_base64"],
                         "source page envelope", 20 * 1024 * 1024)
    page = decode_detached_source_page(page_raw)
    revision = value["catalog_revision"]
    registration_id = value["catalog_registration_id"]
    registration_bytes = _decode64(value["catalog_registration_bytes_base64"],
                                    "catalog registration", 1024 * 1024)
    report_bytes = _decode64(value["report_bytes_base64"], "accepted report", MAX_REPORT_BYTES)
    view_bytes = _decode64(value["view_bytes_base64"], "catalog view", MAX_VIEW_BYTES)
    progress = value["progress"]
    _need(type(progress) is dict and set(progress) == _PROGRESS_KEYS
          and all(type(progress[key]) is str and progress[key]
                  for key in _PROGRESS_KEYS - {"cursor_bytes_base64"}),
          "Closed exact source progress identity required")
    cursor = _decode64(progress["cursor_bytes_base64"], "progress cursor", 1024 * 1024)
    _need(type(revision) is str and revision.isascii() and revision.isdecimal()
          and str(int(revision)) == revision and int(revision) > 0
          and type(registration_id) is str and registration_id
          and hashlib.sha256(registration_bytes).hexdigest()
             == progress["catalog_registration_sha256"],
          "Selected catalog revision or registration digest differs")
    report = _json(report_bytes)
    view = _json(view_bytes)
    _need(report.get("registrationId") == registration_id
          and str(report.get("revision")) == revision
          and view.get("catalogRevision") == revision
          and view.get("interfaceVersion") == "truss-catalog-view/0.2.0"
          and view.get("scope") == {"kind": "complete"},
          "Detached report and complete catalog view identity differs")
    _need((page.identity.get("feed"), page.identity.get("epoch"),
           page.identity.get("installation_id"), page.identity.get("consumer_id"),
           page.identity.get("registration_id"))
          == (progress["feed"], progress["epoch"], progress["installation_id"],
              progress["consumer_id"], progress["consumer_registration_id"])
          and page.prior_bytes == cursor
          and page.page_fields["consumer_id"] == progress["consumer_id"]
          and page.page_fields["registration_id"] == progress["consumer_registration_id"],
          "Detached source page and progress identities differ")
    accepted = report.get("acceptedInput", {}).get("documents")
    records = value["documents"]
    _need(type(accepted) is list and type(records) is list and records
          and len(accepted) == len(records),
          "Detached accepted document inventory required")
    documents = []
    seen = set()
    for source, item in zip(accepted, records):
        _need(type(source) is dict and type(item) is dict
              and set(item) == _DOCUMENT_KEYS,
              "Closed accepted source document record required")
        artifact = source.get("artifact")
        expected = (source.get("documentId"), source.get("documentRevision"),
                    artifact.get("identity") if type(artifact) is dict else None,
                    registration_id, revision,
                    artifact.get("sha256") if type(artifact) is dict else None)
        actual = (item["document_id"], item["document_revision"], item["artifact_identity"],
                  item["registration_id"], item["catalog_revision"], item["sha256"])
        content = _decode64(item["content_base64"], "accepted source document", MAX_DOCUMENT_BYTES)
        _need(actual == expected and actual[0] not in seen
              and type(actual[0]) is str and actual[0]
              and type(actual[1]) is str and actual[1]
              and type(actual[2]) is str and actual[2]
              and type(actual[5]) is str and hashlib.sha256(content).hexdigest() == actual[5],
              "Detached accepted document registration/revision/digest differs from report")
        seen.add(actual[0])
        documents.append(DetachedAcceptedSourceDocument(*actual, content))
    _need(type(progress["position_sha256"]) is str
          and hashlib.sha256(cursor).hexdigest() == progress["position_sha256"]
          and progress["position"] == _b64(cursor),
          "Detached source cursor digest differs")
    from .managed_current_genesis import TrussSourceGenesisProgress
    progress_object = TrussSourceGenesisProgress(
        feed=progress["feed"], epoch=progress["epoch"],
        position=progress["position"], position_sha256=progress["position_sha256"],
        catalog_registration_id=registration_id,
        catalog_registration_sha256=progress["catalog_registration_sha256"],
        consumer_id=progress["consumer_id"],
        consumer_registration_id=progress["consumer_registration_id"],
        installation_id=progress["installation_id"],
        catalog_registration_bytes=registration_bytes, cursor_bytes=cursor)
    return DetachedTrussSourceSnapshot(page, revision, registration_id,
        registration_bytes, report_bytes, view_bytes, progress_object,
        tuple(documents))
