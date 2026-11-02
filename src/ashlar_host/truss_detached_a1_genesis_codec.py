# SPDX-License-Identifier: MIT OR Apache-2.0
"""Closed plain-evidence transport for a fresh Truss .53 A/B A1 genesis read."""
from __future__ import annotations

import base64
from dataclasses import dataclass
import hashlib
import json

from .r04_schedule import canonical


PROFILE = "ashlar-truss-detached-a1-genesis/0.1"
MAX_BYTES = 64 * 1024 * 1024
MAX_DOCUMENT_BYTES = 16 * 1024 * 1024


class DetachedA1GenesisError(ValueError):
    """Detached A/B genesis evidence is malformed or inconsistent."""


def _b64(raw):
    return base64.b64encode(raw).decode("ascii")


def _read64(value, label, maximum):
    if type(value) is not str or len(value) > 4 * ((maximum + 2) // 3):
        raise DetachedA1GenesisError(f"Bounded {label} bytes required")
    try:
        raw = base64.b64decode(value, validate=True)
    except (ValueError, TypeError) as error:
        raise DetachedA1GenesisError(f"Canonical {label} base64 required") from error
    if len(raw) > maximum or _b64(raw) != value:
        raise DetachedA1GenesisError(f"Bounded canonical {label} bytes required")
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
        raise DetachedA1GenesisError("Closed canonical A/B genesis JSON required") from error
    if type(value) is not dict:
        raise DetachedA1GenesisError("A/B genesis envelope object required")
    return value


def _need(value, message):
    if not value:
        raise DetachedA1GenesisError(message)


def _source_payload(selection, source):
    from .managed_current_truss053_genesis_owner import (
        ManagedCurrentTruss053SourceObservation, _is_issued_source_observation,
    )
    from .managed_current_genesis import TrussSourceGenesisProgress
    from .truss_current_catalog import (
        CURRENT_TRUSS_054_FRESH_VERSION, CURRENT_TRUSS_054_FRESH_WHEEL_SHA256,
        CURRENT_TRUSS_054_FRESH_RUNTIME_MANIFEST_SHA256, CurrentCatalogEvidence,
    )
    from truss.feed import FeedPage
    from truss.reader_catalog import AcceptedDocument

    observation = selection.source_observations[source]
    progress = observation.progress
    _need(type(observation) is ManagedCurrentTruss053SourceObservation
          and _is_issued_source_observation(observation)
          and observation.source == source and type(progress) is TrussSourceGenesisProgress
          and type(observation.page) is FeedPage,
          "Original issued public Truss .53 A/B observation required")
    evidence = observation.catalog_evidence
    _need(type(evidence) is CurrentCatalogEvidence
          and evidence.truss_version == CURRENT_TRUSS_054_FRESH_VERSION
          and (evidence.runtime_wheel_sha256, evidence.runtime_manifest_sha256)
             == (CURRENT_TRUSS_054_FRESH_WHEEL_SHA256,
                 CURRENT_TRUSS_054_FRESH_RUNTIME_MANIFEST_SHA256),
          "Exact fresh .54 catalog evidence required for both sources")
    revision = next(iter(evidence.reports), "")
    _need(revision in evidence.reports and set(evidence.views) == {revision}
          and observation.report_bytes == evidence.reports[revision]
          and observation.catalog_view_bytes == evidence.views[revision],
          "Original accepted .53 report and complete view bytes required")
    report = json.loads(observation.report_bytes)
    view = json.loads(observation.catalog_view_bytes)
    _need(type(report) is dict and type(view) is dict
          and report.get("registrationId") == progress.catalog_registration_id
          and str(report.get("revision")) == revision
          and view.get("catalogRevision") == revision
          and view.get("scope") == {"kind": "complete"},
          "Selected original report/view registration and revision must agree")
    accepted = report.get("acceptedInput", {}).get("documents")
    _need(type(accepted) is list and bool(accepted)
          and type(observation.accepted_documents) is dict
          and set(observation.accepted_documents) == {
              item.get("documentId") for item in accepted if type(item) is dict},
          "Complete exact accepted-document inventory required")
    docs = []
    for item in accepted:
        doc_id = item["documentId"]
        artifact = item["artifact"]
        document = observation.accepted_documents[doc_id]
        source_doc = evidence.sources.get((revision, doc_id))
        expected = (doc_id, item["documentRevision"], artifact["identity"],
                    progress.catalog_registration_id, revision, artifact["sha256"])
        actual = (document.document_id, document.document_revision,
                  document.artifact_identity, document.registration_id,
                  str(document.catalog_revision), document.sha256)
        _need(type(document) is AcceptedDocument and actual == expected
              and type(document.content) is bytes
              and 0 < len(document.content) <= MAX_DOCUMENT_BYTES
              and hashlib.sha256(document.content).hexdigest() == document.sha256
              and source_doc is not None and source_doc.content == document.content,
              "Public accepted-document identity, registration, revision, or bytes changed")
        docs.append({"document_id": doc_id,
            "document_revision": document.document_revision,
            "artifact_identity": document.artifact_identity,
            "registration_id": document.registration_id,
            "catalog_revision": revision, "sha256": document.sha256,
            "content_base64": _b64(document.content)})
    page = observation.page
    _need(type(page.content) is bytes and hashlib.sha256(page.content).hexdigest() == page.sha256
          and type(page.prior.content) is bytes and type(page.next.content) is bytes
          and page.outcome == ("transaction" if source == "A" else page.outcome),
          "Exact original public page and cursor bytes required")
    _need(source != "B" or page.outcome in ("transaction", "empty_interval"),
          "Public B genesis page outcome required")
    model = selection.selected_model_evidence[source]
    _need(type(model) is dict and set(model) == {"pin", "bytes"}
          and type(model["bytes"]) is bytes
          and hashlib.sha256(model["bytes"]).hexdigest() == model["pin"].get("modelSha256"),
          "Exact selected R04 model bytes required")
    return {
        "source": source,
        "identity": {"feed": progress.feed, "epoch": progress.epoch,
            "installation_id": progress.installation_id,
            "consumer_id": progress.consumer_id,
            "consumer_registration_id": progress.consumer_registration_id,
            "catalog_registration_id": progress.catalog_registration_id,
            "catalog_revision": revision},
        "progress": {"feed": progress.feed, "epoch": progress.epoch,
            "position": progress.position, "position_sha256": progress.position_sha256,
            "catalog_registration_sha256": progress.catalog_registration_sha256,
            "catalog_registration_id": progress.catalog_registration_id,
            "catalog_registration_bytes_base64": _b64(progress.catalog_registration_bytes),
            "consumer_id": progress.consumer_id,
            "consumer_registration_id": progress.consumer_registration_id,
            "installation_id": progress.installation_id,
            "cursor_bytes_base64": _b64(progress.cursor_bytes)},
        "catalog_revision": revision,
        "report_bytes_base64": _b64(observation.report_bytes),
        "view_bytes_base64": _b64(observation.catalog_view_bytes),
        "page": {"consumer_id": page.consumer_id, "registration_id": page.registration_id,
            "prior_bytes_base64": _b64(page.prior.content),
            "next_bytes_base64": _b64(page.next.content),
            "content_base64": _b64(page.content), "sha256": page.sha256,
            "writer_xid": page.writer_xid, "safe_watermark_xid": page.safe_watermark_xid,
            "outcome": page.outcome},
        "documents": docs,
        "model": {"pin": model["pin"], "content_base64": _b64(model["bytes"])},
    }


def encode_detached_a1_genesis_snapshot(selection) -> bytes:
    """Encode exact two-source genesis evidence; carries no issuer or ACK object."""
    from .managed_current_truss053_genesis_owner import (
        ManagedCurrentTruss053GenesisSelection,
    )
    from .r04_schedule import R04Schedule
    _need(type(selection) is ManagedCurrentTruss053GenesisSelection
          and type(selection.source_observations) is dict
          and set(selection.source_observations) == {"A", "B"}
          and type(selection.schedule_bytes) is bytes
          and selection.schedule_sha256 == hashlib.sha256(selection.schedule_bytes).hexdigest(),
          "Exact public `.53` two-source genesis selection required")
    schedule = R04Schedule(selection.schedule_bytes)
    sources = [_source_payload(selection, source) for source in ("A", "B")]
    body = {"profile": PROFILE,
        "schedule_bytes_base64": _b64(selection.schedule_bytes),
        "schedule_sha256": selection.schedule_sha256,
        "schema_revisions": selection.schema_revisions,
        "sources": sources}
    raw = canonical({**body, "binding_sha256": hashlib.sha256(canonical(body)).hexdigest()})
    _need(len(raw) <= MAX_BYTES, "Bounded exact A/B genesis transport required")
    return raw


@dataclass(frozen=True)
class DetachedA1GenesisSnapshot:
    schedule_bytes: bytes
    schedule_sha256: str
    schema_revisions: dict
    sources: dict

    def source_evidence(self, source: str):
        """Return plain A/B evidence; no source selection or ACK authority."""
        if source not in ("A", "B"):
            raise DetachedA1GenesisError("A/B source slot required")
        return self.sources[source]


def decode_detached_a1_genesis_snapshot(raw: bytes, *, expected_a_prior_cursor="0") -> DetachedA1GenesisSnapshot:
    _need(type(raw) is bytes and 0 < len(raw) <= MAX_BYTES,
          "Bounded exact A/B genesis bytes required")
    value = _json(raw)
    _need(set(value) == {"profile", "schedule_bytes_base64", "schedule_sha256",
                        "schema_revisions", "sources", "binding_sha256"}
          and value["profile"] == PROFILE and canonical(value) == raw,
          "Closed canonical A/B genesis envelope required")
    binding = value.pop("binding_sha256")
    _need(type(binding) is str and hashlib.sha256(canonical(value)).hexdigest() == binding,
          "A/B genesis binding digest differs")
    schedule_bytes = _read64(value["schedule_bytes_base64"], "schedule", 1024 * 1024)
    from .r04_schedule import R04Schedule
    schedule = R04Schedule(schedule_bytes)
    _need(value["schedule_sha256"] == hashlib.sha256(schedule_bytes).hexdigest()
          and type(value["schema_revisions"]) is dict
          and type(value["sources"]) is list and len(value["sources"]) == 2,
          "Exact frozen schedule and two source slots required")
    sources = {}
    for item, source in zip(value["sources"], ("A", "B")):
        _need(type(item) is dict and set(item) == {"source", "identity", "progress",
            "catalog_revision", "report_bytes_base64", "view_bytes_base64",
            "page", "documents", "model"} and item.get("source") == source
              and type(item.get("identity")) is dict and type(item.get("progress")) is dict
              and type(item.get("page")) is dict and type(item.get("documents")) is list
              and bool(item["documents"]), "Closed exact A/B source snapshot required")
        progress = item["progress"]
        _need(set(progress) == {"feed", "epoch", "position", "position_sha256",
            "catalog_registration_sha256", "catalog_registration_id",
            "catalog_registration_bytes_base64", "consumer_id",
            "consumer_registration_id", "installation_id", "cursor_bytes_base64"},
            "Closed source progress fields required")
        cursor = _read64(progress.get("cursor_bytes_base64"), "cursor", 1024 * 1024)
        registration = _read64(progress.get("catalog_registration_bytes_base64"),
                                "catalog registration", 4 * 1024 * 1024)
        _need(hashlib.sha256(cursor).hexdigest() == progress.get("position_sha256")
              and progress.get("position") == _b64(cursor)
              and hashlib.sha256(registration).hexdigest()
                  == progress.get("catalog_registration_sha256"),
              "A/B source progress cursor and registration digests differ")
        page = item["page"]
        _need(set(page) == {"consumer_id", "registration_id", "prior_bytes_base64",
            "next_bytes_base64", "content_base64", "sha256", "writer_xid",
            "safe_watermark_xid", "outcome"}, "Closed public page fields required")
        content = _read64(page.get("content_base64"), "page", 8 * 1024 * 1024)
        prior = _read64(page.get("prior_bytes_base64"), "prior cursor", 1024 * 1024)
        following = _read64(page.get("next_bytes_base64"), "next cursor", 1024 * 1024)
        _need(hashlib.sha256(content).hexdigest() == page.get("sha256")
              and (page.get("consumer_id"), page.get("registration_id"))
                  == (progress.get("consumer_id"), progress.get("consumer_registration_id"))
              and prior == cursor
              and page.get("outcome") in (("transaction",) if source == "A"
                                           else ("transaction", "empty_interval")),
              "A/B public page and genesis progress identities differ")
        binding = schedule.document()["source_bindings"][source]
        _need(all(binding.get(key) == expected for key, expected in (
            ("feed", progress.get("feed")), ("epoch", progress.get("epoch")),
            ("installation_id", progress.get("installation_id")),
            ("consumer_id", progress.get("consumer_id")),
            ("registration_id", progress.get("consumer_registration_id"))))
            and type(item["identity"]) is dict
            and all(item["identity"].get(key) == expected for key, expected in (
                ("feed", progress.get("feed")), ("epoch", progress.get("epoch")),
                ("installation_id", progress.get("installation_id")),
                ("consumer_id", progress.get("consumer_id")),
                ("consumer_registration_id", progress.get("consumer_registration_id")),
                ("catalog_registration_id", progress.get("catalog_registration_id")),
                ("catalog_revision", item.get("catalog_revision")))),
            "A/B source identity differs from frozen R04 schedule or progress")
        try:
            before_cursor = _json(prior)
            after_cursor = _json(following)
        except DetachedA1GenesisError:
            raise
        for cursor_doc in (before_cursor, after_cursor):
            _need(cursor_doc.get("installationId") == progress.get("installation_id")
                  and cursor_doc.get("sourceEpoch") == progress.get("epoch")
                  and cursor_doc.get("consumerId") == progress.get("consumer_id")
                  and cursor_doc.get("registrationId") == progress.get("consumer_registration_id"),
                  "A/B cursor differs from independently selected genesis identity")
        expected_position = expected_a_prior_cursor if source == "A" else "0"
        _need(type(expected_position) is str and expected_position.isdecimal()
              and before_cursor.get("nextXid") == expected_position,
              "A/B source cursor differs from exact phase predecessor")
        report = _read64(item.get("report_bytes_base64"), "report", 4 * 1024 * 1024)
        view = _read64(item.get("view_bytes_base64"), "complete view", 8 * 1024 * 1024)
        report_obj, view_obj = _json(report), _json(view)
        revision = item.get("catalog_revision")
        _need(type(revision) is str and revision.isdecimal()
              and str(int(revision)) == revision and int(revision) > 0
              and report_obj.get("registrationId") == progress.get("catalog_registration_id")
              and str(report_obj.get("revision")) == revision
              and view_obj.get("catalogRevision") == revision
              and view_obj.get("scope") == {"kind": "complete"},
              "A/B accepted report and complete view do not bind selected revision")
        accepted = report_obj.get("acceptedInput", {}).get("documents")
        _need(type(accepted) is list and len(accepted) == len(item["documents"])
              and {entry.get("documentId") for entry in accepted if type(entry) is dict}
                  == {entry.get("document_id") for entry in item["documents"]},
              "Detached documents differ from the complete accepted report inventory")
        docs = []
        for doc in item["documents"]:
            _need(type(doc) is dict and set(doc) == {"document_id", "document_revision",
                "artifact_identity", "registration_id", "catalog_revision", "sha256",
                "content_base64"}, "Closed exact accepted-document item required")
            content_doc = _read64(doc["content_base64"], "accepted document", MAX_DOCUMENT_BYTES)
            _need(doc["catalog_revision"] == revision
                  and doc["registration_id"] == progress.get("catalog_registration_id")
                  and hashlib.sha256(content_doc).hexdigest() == doc["sha256"],
                  "Accepted-document registration/revision/content binding differs")
            source_item = next(entry for entry in accepted
                               if entry.get("documentId") == doc["document_id"])
            artifact = source_item.get("artifact")
            _need(source_item.get("documentRevision") == doc["document_revision"]
                  and type(artifact) is dict
                  and artifact.get("identity") == doc["artifact_identity"]
                  and artifact.get("sha256") == doc["sha256"],
                  "Detached accepted document differs from original report artifact pin")
            docs.append({**doc, "content": content_doc})
        model = item.get("model")
        _need(type(model) is dict and set(model) == {"pin", "content_base64"}
              and type(model["pin"]) is dict,
              "Exact selected model pin required")
        model_content = _read64(model["content_base64"], "selected model", MAX_DOCUMENT_BYTES)
        _need(hashlib.sha256(model_content).hexdigest() == model["pin"].get("modelSha256"),
              "Selected model bytes differ from original R04 pin")
        _need(model["pin"] == schedule.document()["accepted_model_pins"][source],
              "Selected model pin differs from frozen R04 schedule")
        sources[source] = {**item, "report_bytes": report, "view_bytes": view,
            "progress": {**progress, "cursor_bytes": cursor,
                "catalog_registration_bytes": registration},
            "page": {**page, "content": content, "prior_bytes": prior,
                "next_bytes": following}, "documents": tuple(docs),
            "model": {"pin": model["pin"], "content": model_content}}
    return DetachedA1GenesisSnapshot(schedule_bytes, value["schedule_sha256"],
                                     value["schema_revisions"], sources)
