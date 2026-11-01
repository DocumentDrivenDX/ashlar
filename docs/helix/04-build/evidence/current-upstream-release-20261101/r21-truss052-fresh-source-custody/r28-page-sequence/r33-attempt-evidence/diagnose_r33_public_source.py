#!/usr/bin/env python3
"""Read R24 public Truss source facts on a disposable, hash-verified cold copy.

This diagnostic is intentionally limited to public status, report, accepted
document, and feed reads. It never accepts a document, enrolls a consumer,
applies a group, acknowledges a page, starts Spark, or writes Delta data.
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import shutil
import stat
from pathlib import Path

SOURCE_ROOT = Path("/private/tmp/ashlar-r24-truss052-fresh-source-20261101")
SOURCE_PGDATA = SOURCE_ROOT / "pgdata"
R33_ROOT = Path("/private/tmp/ashlar-r33-truss052-page-sequence-20261101")
DIAGNOSTIC_PGDATA = R33_ROOT / "pgdata"
OUT = Path(__file__).with_name("r33-public-source-reconciliation-v3.json")
REPORT_PROJECTION = Path(__file__).with_name("r24-public-report-document-projection-v3.json")
SCOPE_PATH = Path(__file__).parents[1] / "r33-host-scope.json"
WORKER_PATH = Path(__file__).parents[1] / "r28-local-worker.py"
MAX_TREE_BYTES = 512 * 1024 * 1024


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON member {key!r}")
        result[key] = value
    return result


def tree_digest(root: Path) -> dict:
    root = root.resolve(strict=True)
    if (not root.is_dir() or root.is_symlink() or root.stat().st_uid != os.getuid()
            or root.stat().st_mode & 0o077):
        raise RuntimeError("PGDATA must be an owner-private directory")
    result = hashlib.sha256()
    files = total = 0
    entries = sorted(root.rglob("*"), key=lambda p: p.relative_to(root).as_posix().encode())
    if len(entries) > 10000:
        raise RuntimeError("PGDATA entry bound exceeded")
    for path in entries:
        st = path.lstat()
        if st.st_uid != os.getuid() or stat.S_ISLNK(st.st_mode):
            raise RuntimeError("PGDATA contains a foreign-owned or linked entry")
        relative = path.relative_to(root).as_posix().encode()
        if stat.S_ISDIR(st.st_mode):
            if st.st_mode & 0o077:
                raise RuntimeError("PGDATA directory is not private")
            kind = b"D"
        elif stat.S_ISREG(st.st_mode):
            if st.st_mode & 0o022:
                raise RuntimeError("PGDATA file is group/world writable")
            kind = b"F"
            files += 1
            total += st.st_size
            if total > MAX_TREE_BYTES:
                raise RuntimeError("PGDATA size bound exceeded")
        else:
            raise RuntimeError("PGDATA contains a non-regular entry")
        result.update(kind + len(relative).to_bytes(4, "big") + relative
                      + (st.st_mode & 0o777).to_bytes(4, "big")
                      + st.st_size.to_bytes(8, "big"))
        if kind == b"F":
            with path.open("rb") as stream:
                while chunk := stream.read(1024 * 1024):
                    result.update(chunk)
    return {"sha256": result.hexdigest(), "files": files, "bytes": total}


def unwrap(result, label, Ok):
    if not isinstance(result, Ok):
        raise RuntimeError(f"public API returned {type(result).__name__} for {label}")
    return result.value


def public_read(connection, callback, *, Ok, CatalogRuntimeConfiguration,
                create_reference_assembly, adopt_psycopg_host):
    with connection.transaction():
        connection.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED READ ONLY")
        connection.execute("SET LOCAL statement_timeout = '10000ms'")
        host = adopt_psycopg_host(connection, statement_timeout_ms=10000,
                                  operation_timeout_ms=30000)
        assembly = None
        primary = None
        try:
            tx = unwrap(host.adopt_transaction(access_mode="read_only"),
                        "public transaction adoption", Ok)
            assembly = unwrap(create_reference_assembly(
                CatalogRuntimeConfiguration(), host), "reference assembly", Ok)
            return callback(assembly, tx)
        except BaseException as error:
            primary = error
            raise
        finally:
            failures = []
            if assembly is not None:
                try:
                    assembly.dispose()
                except BaseException as error:
                    failures.append(error)
            try:
                if not host.detach():
                    raise RuntimeError("public Truss host detach was not confirmed")
            except BaseException as error:
                failures.append(error)
            if failures and primary is None:
                raise failures[0]


def main() -> None:
    scope = json.loads(SCOPE_PATH.read_bytes(), object_pairs_hook=unique_pairs)
    source_receipt = json.loads((R33_ROOT / "receipts/resume-source-copy.json").read_bytes(),
                                object_pairs_hook=unique_pairs)
    acceptance = json.loads((SOURCE_ROOT / "receipts/acceptance.json").read_bytes(),
                            object_pairs_hook=unique_pairs)
    consumer_receipt = json.loads((SOURCE_ROOT / "receipts/consumer-registration.json").read_bytes(),
                                  object_pairs_hook=unique_pairs)
    page_receipt = json.loads((R33_ROOT / "receipts/genesis-page-selection.json").read_bytes(),
                              object_pairs_hook=unique_pairs)
    source_page_paths = list((R33_ROOT / "delta").glob("source-page-*.json"))
    if len(source_page_paths) != 1:
        raise RuntimeError("R33 must retain exactly one attempted source page")
    source_page_raw = source_page_paths[0].read_bytes()
    source_page = json.loads(source_page_raw, object_pairs_hook=unique_pairs)
    page_content = base64.b64decode(source_page["content"], validate=True)
    if (digest(page_content) != page_receipt["page_sha256"]
            or digest(page_content) != source_page["sha256"]):
        raise RuntimeError("R33 retained public page bytes do not match the page receipt")

    source_tree = tree_digest(SOURCE_PGDATA)
    copied_tree_expected = source_receipt["source_tree"]
    if source_tree != copied_tree_expected:
        raise RuntimeError("Immutable R24 PGDATA no longer matches R33 cold-copy source receipt")
    # R33 already contains the complete cold copy proven equal to R24 before
    # its public-only read phase. Reuse that stopped attempt clone in place to
    # avoid another 48 MiB copy; never start or modify R24 itself.
    diagnostic_tree_before = tree_digest(DIAGNOSTIC_PGDATA)

    from truss import (CatalogRuntimeConfiguration, LocalPostgres, Ok,
                       adopt_psycopg_host, create_reference_assembly)
    from truss.feed import FeedPage
    from truss.reader_catalog import AcceptedDocument
    import psycopg

    runtime = LocalPostgres(DIAGNOSTIC_PGDATA)
    runtime.__enter__()
    report_raw = None
    accepted_document = None
    page = None
    try:
        info = runtime.info
        if Path(info.data_directory) != DIAGNOSTIC_PGDATA:
            raise RuntimeError("Diagnostic Truss runtime is not bound to copied PGDATA")
        params = psycopg.conninfo.conninfo_to_dict(info.connection_uri)
        names = scope["source"]
        registrar = dict(params, user=names["registrar_login_role"])
        with psycopg.connect(**registrar, autocommit=False, connect_timeout=5) as conn:
            status, report = public_read(conn, lambda assembly, tx: (
                unwrap(assembly.status_in_transaction(tx), "catalog status", Ok),
                unwrap(assembly.report_in_transaction(tx, acceptance["revision"]), "catalog report", Ok)),
                Ok=Ok, CatalogRuntimeConfiguration=CatalogRuntimeConfiguration,
                create_reference_assembly=create_reference_assembly,
                adopt_psycopg_host=adopt_psycopg_host)
        report_raw = report.original_bytes
        if type(report_raw) is not bytes or digest(report_raw) != acceptance["report_sha256"]:
            raise RuntimeError("Public report bytes do not match immutable R24 acceptance hash")

        reader = dict(params, user=names["ordinary_reader_role"])
        with psycopg.connect(**reader, autocommit=False, connect_timeout=5) as conn:
            accepted_document, page = public_read(conn, lambda assembly, tx: (
                unwrap(assembly.accepted_document_in_transaction(tx,
                    document_id=scope["model"]["document_id"], maximum_bytes=1048576),
                    "accepted document", Ok),
                unwrap(assembly.read_feed_in_transaction(tx,
                    consumer_id=names["consumer_id"], maximum_facts=4096,
                    maximum_bytes=scope["publication"]["maximum_page_bytes"]),
                    "feed page", Ok)), Ok=Ok,
                CatalogRuntimeConfiguration=CatalogRuntimeConfiguration,
                create_reference_assembly=create_reference_assembly,
                adopt_psycopg_host=adopt_psycopg_host)
    finally:
        runtime.__exit__(None, None, None)

    report_value = json.loads(report_raw, object_pairs_hook=unique_pairs)
    accepted_input = report_value.get("acceptedInput")
    documents = accepted_input.get("documents") if type(accepted_input) is dict else None
    if type(documents) is not list:
        raise RuntimeError("Public acceptance report lacks a document list")
    matching = [item for item in documents if type(item) is dict
                and item.get("documentId") == scope["model"]["document_id"]]
    if len(matching) != 1:
        raise RuntimeError("Public acceptance report must contain exactly one selected document")
    doc = matching[0]
    artifact = doc.get("artifact")
    artifact = artifact if type(artifact) is dict else None
    fields = {key: type(value).__name__ for key, value in doc.items()}
    artifact_fields = ({key: type(value).__name__ for key, value in artifact.items()}
                       if artifact is not None else {})
    accepted_content = accepted_document.content
    selected_report_item = {
        "fields": fields,
        "artifact_fields": artifact_fields,
        "document_id": doc.get("documentId"),
        "document_revision": doc.get("documentRevision"),
        "document_revision_alias": doc.get("revision"),
        "artifact_identity": artifact.get("identity") if artifact else None,
        "artifact_sha256": artifact.get("sha256") if artifact else None,
        "artifact_bytes_base64_present": bool(artifact and type(artifact.get("bytesBase64")) is str),
        "accepted_document": {
            "type": type(accepted_document).__name__,
            "document_id": accepted_document.document_id,
            "document_revision": accepted_document.document_revision,
            "artifact_identity": accepted_document.artifact_identity,
            "sha256": accepted_document.sha256,
            "registration_id": accepted_document.registration_id,
            "catalog_revision": accepted_document.catalog_revision,
            "source_catalog_revision": accepted_document.source_catalog_revision,
            "content_bytes": len(accepted_content),
            "content_sha256": digest(accepted_content),
            "validation_bytes_sha256": (digest(accepted_document.validation_bytes)
                if accepted_document.validation_bytes is not None else None),
            "ddd_profile_bytes_sha256": (digest(accepted_document.ddd_profile_bytes)
                if accepted_document.ddd_profile_bytes is not None else None),
            "ddd_validation_bytes_sha256": (digest(accepted_document.ddd_validation_bytes)
                if accepted_document.ddd_validation_bytes is not None else None),
        },
        "report_embeds_accepted_document_bytes": (
            artifact is not None and type(artifact.get("bytesBase64")) is str
            and base64.b64decode(artifact["bytesBase64"], validate=True) == accepted_content),
        "accepted_document_correspondence": {
            "document_id": accepted_document.document_id == doc.get("documentId"),
            "document_revision": accepted_document.document_revision == doc.get("documentRevision"),
            "artifact_identity": accepted_document.artifact_identity == artifact.get("identity"),
            "artifact_sha256": accepted_document.sha256 == artifact.get("sha256"),
            "content_sha256": digest(accepted_content) == accepted_document.sha256,
            "registration_id": accepted_document.registration_id == report_value.get("registrationId"),
            "catalog_revision": accepted_document.catalog_revision == int(report_value["revision"]),
            "registration_revision": accepted_document.catalog_revision == report.revision,
        },
    }
    page_fields = {name: getattr(page, name, None) for name in (
        "consumer_id", "registration_id", "writer_xid", "outcome", "sha256")}
    page_fields.update({"content_sha256": digest(page.content),
        "content_bytes": len(page.content), "prior_sha256": digest(page.prior.content),
        "next_sha256": digest(page.next.content),
        "prior_cursor": json.loads(page.prior.content, object_pairs_hook=unique_pairs),
        "next_cursor": json.loads(page.next.content, object_pairs_hook=unique_pairs)})
    page_correspondence = {
        "matches_r33_page_sha256": page.sha256 == page_receipt["page_sha256"],
        "matches_r33_page_content": page.content == page_content,
        "matches_r33_prior_cursor": page.prior.content == base64.b64decode(source_page["prior"], validate=True),
        "matches_r33_next_cursor": page.next.content == base64.b64decode(source_page["next"], validate=True),
        "matches_r33_consumer": page.consumer_id == names["consumer_id"],
        "matches_r33_consumer_registration": page.registration_id == consumer_receipt["registration_id"],
        "matches_installation_cursor": all(json.loads(cursor.content).get("installationId") == status.installation_id
            for cursor in (page.prior, page.next)),
        "matches_consumer_cursor": all(json.loads(cursor.content).get("consumerId") == page.consumer_id
            for cursor in (page.prior, page.next)),
        "matches_registration_cursor": all(json.loads(cursor.content).get("registrationId") == page.registration_id
            for cursor in (page.prior, page.next)),
    }
    if not all(page_correspondence.values()):
        raise RuntimeError(f"Public feed page drifted from R33 retained evidence: {page_correspondence}")

    # Keep a compact live-shaped projection, while the full public report
    # remains preserved in the retained feed-page evidence by its exact hash.
    projection = {"profile": "ashlar-r24-public-report-document-projection/0.1",
        "source_report_sha256": digest(report_raw),
        "registration_id": report_value["registrationId"],
        "revision": report_value["revision"], "document": doc}
    projection_raw = json.dumps(projection, sort_keys=True, indent=2,
        ensure_ascii=False).encode() + b"\n"
    fd = os.open(REPORT_PROJECTION, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(projection_raw)
        stream.flush()
        os.fsync(stream.fileno())

    from ashlar.truss_native_feed import native_feed_batch
    from ashlar_host.publication_planning import truss_request_for
    batch = native_feed_batch(page.content, sha256=page.sha256,
        prior=page.prior.content, next=page.next.content, feed=names["feed_selection"])
    page_payload = json.loads(page.content, object_pairs_hook=unique_pairs)
    report_revisions = []
    for prerequisite in page_payload["catalogPrerequisites"]:
        encoded_report = prerequisite.get("reportBytesBase64")
        if encoded_report is None:
            continue
        embedded = base64.b64decode(encoded_report, validate=True)
        embedded_report = json.loads(embedded, object_pairs_hook=unique_pairs)
        if digest(embedded) != prerequisite.get("reportSha256"):
            raise RuntimeError("Public page prerequisite report digest differs")
        report_revisions.append(int(embedded_report["revision"]))
    schema_revision = str(max(report_revisions)) if report_revisions else "0"
    request = truss_request_for(scope["publication"]["feed"], batch,
        scope["publication"]["predecessor"], {status.installation_id: schema_revision})
    request_raw = json.dumps(request, sort_keys=True, separators=(",", ":"),
        ensure_ascii=False, allow_nan=False).encode()
    intent_paths = list((R33_ROOT / "delta").glob("publication-intent-*.json"))
    if len(intent_paths) != 1:
        raise RuntimeError("R33 must retain exactly one publication intent")
    intent = json.loads(intent_paths[0].read_bytes(), object_pairs_hook=unique_pairs)
    request_correspondence = {
        "request_digest": request["request_digest"],
        "request_sha256": digest(request_raw),
        "publication_id": "truss-graph-" + request["request_digest"],
        "predecessor": request["predecessor"],
        "schema_revisions_json": request["schema_revisions_json"],
        "request_digest_matches_intent": request["request_digest"] == intent["request_digest"],
        "publication_id_matches_intent": "truss-graph-" + request["request_digest"] == intent["publication_id"],
        "source_page_sha256_matches_intent": digest(source_page_raw) == intent["source_page_sha256"],
        "source_page_content_sha256": digest(page.content),
        "source_page_wrapper_sha256": digest(source_page_raw),
    }
    if not all(request_correspondence[key] for key in (
            "request_digest_matches_intent", "publication_id_matches_intent",
            "source_page_sha256_matches_intent")):
        raise RuntimeError(f"Reconstructed public page request differs from R33 intent: {request_correspondence}")

    accepted_correspondence = selected_report_item["accepted_document_correspondence"]
    # Invoke the corrected complete selector using the actual typed results from
    # the public calls above. The destination registry is an inert six-role
    # LocalDeltaTransport with private empty directories: no Spark is started
    # and no Delta target is read or written by this selector-only check.
    from types import MappingProxyType
    from truss.feed import FeedConsumer
    from truss.catalog_results import CatalogRegistration
    from ashlar_host.delta_custody import DeltaTarget, LocalDeltaTransport
    from ashlar_host.truss_source_custody_selection import select_truss_local_dev_source_custody_anchor
    with __import__("tempfile").TemporaryDirectory(prefix="ashlar-r33-selector-targets-") as temporary:
        temporary = Path(temporary)
        roles = ("object_current", "edge_current", "tombstone", "whole_source_history",
                 "attempts", "manifest")
        targets = {}
        for role in roles:
            table = "local.trussgraph." + role
            target_path = temporary / role
            target_path.mkdir(mode=0o700)
            targets[table] = DeltaTarget(table, target_path, "diagnostic-uuid-" + role)
        destination_transport = LocalDeltaTransport.__new__(LocalDeltaTransport)
        destination_transport.targets = MappingProxyType(targets)
        registration = CatalogRegistration(revision=int(acceptance["revision"]),
            registration_id=report_value["registrationId"], report_bytes=report_raw, replayed=False)
        consumer = FeedConsumer(consumer_id=page.consumer_id,
            registration_id=page.registration_id, checkpoint=page.prior)
        anchor = select_truss_local_dev_source_custody_anchor(
            registration=registration, accepted_document=accepted_document, consumer=consumer,
            runtime_status=status, page=page, feed=scope["publication"]["feed"],
            source_holder_id=names["source_holder_id"], document_id=scope["model"]["document_id"],
            destination_id=scope["publication"]["destination_id"],
            ack_registration_id=names["ack_registration_id"], ack_scope_id=names["ack_scope_id"],
            publication_id=intent["publication_id"], request_bytes=request_raw,
            predecessor_id=request["predecessor"], destination_transport=destination_transport)
        selector_preflight = {
            "outcome": "accepted",
            "input_types": {"report_readback": type(report).__name__,
                "registration_argument": type(registration).__name__,
                "accepted_document": type(accepted_document).__name__,
                "consumer": type(consumer).__name__, "runtime_status": type(status).__name__,
                "page": type(page).__name__, "transport": type(destination_transport).__name__},
            "consumer_origin": "typed FeedConsumer built from retained public enrollment identity and exact public page prior checkpoint; Truss 0.0.52 has no read-only consumer lookup",
            "accepted_document_correspondence": accepted_correspondence,
            "page_correspondence": page_correspondence,
            "request_correspondence": request_correspondence,
            "destination_roles": sorted(targets),
            "bound_data_target_roles": sorted(anchor.destination_target_bindings),
            "destination_registration_sha256": digest(anchor.destination_registration_bytes),
            "ack_scope_sha256": digest(anchor.ack_scope_bytes),
            "authority_profile": anchor.authority_profile,
            "authority": "local-development",
        }
    if not all(accepted_correspondence.values()):
        raise RuntimeError(f"AcceptedDocument/report correspondence differs: {accepted_correspondence}")
    result = {
        "profile": "ashlar-r33-public-source-readonly-diagnostic/0.3",
        "authority": "local-development only",
        "source_root_untouched": True,
        "source_pgdata": {"path": str(SOURCE_PGDATA), **source_tree},
        "r33_cold_copy_provenance": {"source_tree_sha256": source_receipt["source_tree"]["sha256"],
            "source_tree_matches_r24": source_receipt["source_tree"] == source_tree,
            "pgdata_before_readonly_api": diagnostic_tree_before,
            "pgdata_after_readonly_api": tree_digest(DIAGNOSTIC_PGDATA)},
        "public_runtime_status": {"installed": status.installed,
            "installation_id": status.installation_id, "release_id": status.release_id},
        "public_report": {"bytes": len(report_raw),
            "sha256": digest(report_raw), "registration_id": report_value.get("registrationId"),
            "revision": report_value.get("revision"),
            "revision_type": type(report_value.get("revision")).__name__,
            "accepted_input_keys": sorted(accepted_input),
            "document_count": len(documents), "selected_document": selected_report_item,
            "compact_projection_fixture": str(REPORT_PROJECTION),
            "compact_projection_sha256": digest(projection_raw)},
        "selector_expected_report_shape": {
            "documentId": "string",
            "documentRevision": "string",
            "artifact.identity": "string",
            "artifact.sha256": "64-hex string",
            "artifact.bytesBase64": "string",
            "actual_document_fields": fields,
            "actual_artifact_fields": artifact_fields,
        },
        "public_page": page_fields,
        "page_correspondence": page_correspondence,
        "request_correspondence": request_correspondence,
        "complete_selector_preflight": selector_preflight,
        "r33_attempt": {"page_sha256": page_receipt["page_sha256"],
            "acknowledgement": page_receipt["acknowledgement"],
            "publication_id": intent["publication_id"],
            "request_digest": intent["request_digest"],
            "source_ack_attempted": False},
        "limits": ["one read-only copy", "no Spark", "no Delta data write", "no source ACK"],
    }
    raw = json.dumps(result, sort_keys=True, indent=2, ensure_ascii=False).encode() + b"\n"
    fd = os.open(OUT, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    print(json.dumps({"receipt": str(OUT), "receipt_sha256": digest(raw),
                      "report_projection": str(REPORT_PROJECTION),
                      "report_sha256": digest(report_raw),
                      "document_fields": fields, "artifact_fields": artifact_fields,
                      "page_correspondence": page_correspondence}, sort_keys=True))


if __name__ == "__main__":
    main()
