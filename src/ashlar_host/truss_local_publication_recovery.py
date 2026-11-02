# SPDX-License-Identifier: MIT OR Apache-2.0
"""No-replay recovery for a committed local Truss publication.

The recovery boundary is deliberately narrow: it restores the applied artifact
from the original journal, independently reads the exact Delta vector, durably
records that correspondence, and only then permits the existing protected
Truss source-custody ACK lifecycle.
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
import stat
from pathlib import Path

from ashlar.attempt_store import verify_request_digest
from ashlar.manifest import manifest_pin_vector
from .lifecycle import owned_context
from .truss_recovery import restore_applied_truss_publication
from .truss_source_custody_selection import (
    TrussSourceCustodyAnchor,
    TrussSourceCustodySelection,
    _mint_expected_native_post_state,
    read_local_native_delta_publication_observation,
)
from .truss052_source_custody import Truss052SourceCustodySession


class TrussPublicationRecoveryError(RuntimeError):
    """The retained publication could not be independently recovered."""


def _unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise TrussPublicationRecoveryError("Duplicate retained JSON key")
        result[key] = value
    return result


def _json(raw: bytes):
    try:
        value = json.loads(raw, object_pairs_hook=_unique_pairs)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise TrussPublicationRecoveryError("Canonical retained JSON required") from error
    if type(value) is not dict:
        raise TrussPublicationRecoveryError("Retained object required")
    return value


def _encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def _write_private_exclusive(path: Path, raw: bytes):
    path = Path(path)
    if (not path.is_absolute() or path.is_symlink() or not path.parent.is_dir()
            or path.exists() or len(raw) > 1024 * 1024):
        raise TrussPublicationRecoveryError("Fresh bounded completion marker path required")
    parent = path.parent.stat()
    if parent.st_uid != os.getuid() or parent.st_mode & 0o077:
        raise TrussPublicationRecoveryError("Owner-private recovery receipt directory required")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(fd)
        if info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise TrussPublicationRecoveryError("Private recovery marker ownership required")
        offset = 0
        while offset < len(raw):
            offset += os.write(fd, raw[offset:])
        os.fsync(fd)
    finally:
        os.close(fd)
    dfd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(dfd)
    finally:
        os.close(dfd)
    return hashlib.sha256(raw).hexdigest()


def _read_private(path: Path, *, maximum_bytes: int):
    path = Path(path)
    if (not path.is_absolute() or path.resolve(strict=True) != path
            or any(parent.is_symlink() for parent in path.parents)):
        raise TrussPublicationRecoveryError("Canonical retained carrier path required")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        info = os.fstat(fd)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or info.st_mode & 0o077 or not 0 <= info.st_size <= maximum_bytes):
            raise TrussPublicationRecoveryError("Owner-private bounded retained carrier required")
        result = bytearray()
        while len(result) <= info.st_size:
            chunk = os.read(fd, min(65536, info.st_size + 1 - len(result)))
            if not chunk:
                break
            result.extend(chunk)
        if len(result) != info.st_size:
            raise TrussPublicationRecoveryError("Retained private carrier changed during read")
        return bytes(result)
    finally:
        os.close(fd)


def recover_committed_truss_publication_and_ack(
        driver, request_digest: str, pending_receipt_bytes: bytes, anchor, *,
        spark, custody_session_factory, completion_path: Path,
        ack_attempt_path: Path, ack_reconciliation_path: Path,
        publication_intent_path: Path, publication_intent_sha256: str):
    """Restore, independently verify, durably mark, then protected-ACK once.

    ``anchor`` must be freshly selected from the actual public source page and
    source registration before this call. The passed driver must already be
    reopened on the exact existing six-role journal/targets. This function
    never calls a publisher or continuation API.
    """
    if (type(request_digest) is not str or len(request_digest) != 64
            or type(pending_receipt_bytes) is not bytes
            or not 0 < len(pending_receipt_bytes) <= 8 * 1024 * 1024
            or type(anchor) is not TrussSourceCustodyAnchor
            or not callable(custody_session_factory)
            or type(spark) is type(None)):
        raise TrussPublicationRecoveryError("Exact bounded original recovery inputs required")
    pending = _json(pending_receipt_bytes)
    pending_sha = hashlib.sha256(pending_receipt_bytes).hexdigest()
    marker_paths = tuple(Path(item) for item in
                         (completion_path, ack_attempt_path, ack_reconciliation_path))
    if (len(set(marker_paths)) != 3 or any(not item.is_absolute() or item.exists()
                                           or item.is_symlink() for item in marker_paths)):
        raise TrussPublicationRecoveryError("Three distinct fresh recovery marker paths required")
    if (pending.get("status") != "publication_returned_native_readback_pending"
            or pending.get("acknowledgement") != "not_attempted"
            or pending.get("request_digest") != request_digest
            or pending.get("publication_id") != "truss-graph-" + request_digest):
        raise TrussPublicationRecoveryError("Original unacknowledged publication receipt required")

    def retained_bytes(key):
        encoded = pending.get(key)
        if type(encoded) is not str:
            raise TrussPublicationRecoveryError("Exact retained publication bytes required")
        try:
            raw = base64.b64decode(encoded, validate=True)
        except (ValueError, TypeError) as error:
            raise TrussPublicationRecoveryError("Valid retained publication bytes required") from error
        if base64.b64encode(raw).decode("ascii") != encoded:
            raise TrussPublicationRecoveryError("Canonical retained base64 required")
        return raw

    request_raw = retained_bytes("request_bytes_base64")
    manifest_raw = retained_bytes("manifest_bytes_base64")
    receipt_raw = retained_bytes("receipt_bytes_base64")
    request = _json(request_raw)
    manifest = _json(manifest_raw)
    receipt_sha = hashlib.sha256(receipt_raw).hexdigest()
    if (hashlib.sha256(request_raw).hexdigest() != pending.get("request_sha256")
            or verify_request_digest(request) != request_digest
            or request.get("request_digest") != request_digest
            or hashlib.sha256(manifest_raw).hexdigest() != pending.get("manifest_sha256")
            or manifest != pending.get("manifest")
            or manifest.get("publication_id") != pending.get("publication_id")
            or hashlib.sha256(receipt_raw).hexdigest() != pending.get("receipt_sha256")
            or pending.get("feed") != request.get("stream")):
        raise TrussPublicationRecoveryError("Retained request, manifest, receipt or digest binding differs")

    page = anchor.page
    source = pending.get("source_page")
    archive_path = Path(source.get("path", "")) if type(source) is dict else Path("")
    intent_path = Path(publication_intent_path)
    if (not archive_path.is_absolute() or archive_path.resolve() != archive_path
            or not intent_path.is_absolute() or intent_path.resolve() != intent_path
            or archive_path.parent != intent_path.parent
            or intent_path.name != f"publication-intent-{request_digest}.json"
            or type(publication_intent_sha256) is not str or len(publication_intent_sha256) != 64):
        raise TrussPublicationRecoveryError("Exact colocated R42 page and intent paths required")
    archive_raw = _read_private(archive_path, maximum_bytes=3 * 1024 * 1024)
    intent_raw = _read_private(intent_path, maximum_bytes=65536)
    if (hashlib.sha256(archive_raw).hexdigest() != source.get("archive_sha256")
            or hashlib.sha256(intent_raw).hexdigest() != publication_intent_sha256):
        raise TrussPublicationRecoveryError("Original R42 page archive or publication intent digest differs")
    intent = _json(intent_raw)
    if (set(intent) != {"feed", "publication_id", "request_digest", "source_page_path", "source_page_sha256"}
            or intent.get("feed") != pending.get("feed")
            or intent.get("publication_id") != pending.get("publication_id")
            or intent.get("request_digest") != request_digest
            or intent.get("source_page_path") != str(archive_path)
            or intent.get("source_page_sha256") != source.get("archive_sha256")):
        raise TrussPublicationRecoveryError("Original R42 publication intent does not bind archived page")
    from .truss_page_archive import decode_truss_page
    archived_page = decode_truss_page(archive_raw, source["archive_sha256"], pending["feed"])
    page_fields = ("content", "sha256", "prior", "next", "consumer_id", "registration_id", "writer_xid", "outcome")
    if any(getattr(archived_page, name, None) != getattr(page, name, None) for name in page_fields):
        raise TrussPublicationRecoveryError("Fresh public page differs from original archived Truss page")
    checkpoint = _json(request.get("source_checkpoint_json", "").encode("utf-8"))
    try:
        checkpoint_position = base64.b64decode(checkpoint["position"], validate=True)
        checkpoint_previous = base64.b64decode(checkpoint["previous"], validate=True)
    except (KeyError, TypeError, ValueError) as error:
        raise TrussPublicationRecoveryError("Original request checkpoint bytes required") from error
    if (type(source) is not dict or page.consumer_id != pending.get("consumer_id")
            or page.sha256 != source.get("page_sha256")
            or hashlib.sha256(page.content).hexdigest() != source.get("content_sha256")
            or hashlib.sha256(page.prior.content).hexdigest() != source.get("prior_sha256")
            or hashlib.sha256(page.next.content).hexdigest() != source.get("next_sha256")
            or page.registration_id != source.get("registration_id")
            or type(page.writer_xid) is not str
            or not page.writer_xid.isascii()
            or not page.writer_xid.isdigit()
            or page.writer_xid.startswith("0")
            or type(source.get("writer_xid")) is not str
            or not source["writer_xid"].isascii()
            or not source["writer_xid"].isdigit()
            or source["writer_xid"].startswith("0")
            or page.writer_xid != source["writer_xid"]
            or checkpoint.get("feed") != pending.get("feed")
            or checkpoint.get("batch_id") != page.writer_xid
            or checkpoint.get("payload_digest") != page.sha256
            or checkpoint_position != page.next.content
            or checkpoint_previous != page.prior.content
            or anchor.request_bytes != request_raw
            or anchor.destination_id != pending.get("destination_id")
            or anchor.preselected_publication_id != pending.get("publication_id")):
        raise TrussPublicationRecoveryError("Fresh public source selection differs from retained page/receipt")
    if request.get("batch_id") != page.writer_xid:
        raise TrussPublicationRecoveryError("Original request does not identify selected source page")

    from ashlar.pins import PinVector
    raw_vector = pending.get("publication_vector")
    if (type(raw_vector) is not dict or set(raw_vector) != {
            "authority", "scope_kind", "scope_id", "custody_digest", "targets"}
            or type(raw_vector.get("targets")) is not dict):
        raise TrussPublicationRecoveryError("Exact retained publication vector required")
    vector = PinVector(raw_vector["authority"], raw_vector["scope_kind"],
                       raw_vector["scope_id"], raw_vector["custody_digest"],
                       {key: tuple(value) for key, value in raw_vector["targets"].items()})

    # Restoration is read/validation-only: it admits the original SQLite plan
    # and committed artifact; it does not issue Delta writes or source ACKs.
    with owned_context(driver.writer(driver.source_sessions.batch().feed, driver.context)):
        descriptor = restore_applied_truss_publication(driver, request_digest)
    active = driver.policy.active
    if (type(active) is not dict or active.get("request") != request
            or active.get("manifest") != manifest
            or _encoded(active.get("manifest")) != manifest_raw
            or descriptor is None or dict(descriptor.raw) != manifest):
        raise TrussPublicationRecoveryError("Restored applied artifact differs from retained publication")
    selected_vector = manifest_pin_vector(
        manifest,
        {table: driver.transport.targets[table].uuid
         for table in json.loads(manifest["table_versions_json"])},
        authority="private-local-process")
    if selected_vector != vector:
        raise TrussPublicationRecoveryError("Retained pin vector differs from original journal targets")
    from .truss_feed_sessions import encode_truss_destination_receipt
    if encode_truss_destination_receipt(request_raw, manifest_raw, vector) != receipt_raw:
        raise TrussPublicationRecoveryError("Retained destination receipt is not the exact original binding")
    expected = _mint_expected_native_post_state(
        publication_id=manifest["publication_id"], request_digest=request_digest,
        following=active["expected"], carrier_profile="ashlar-delta/0.4-xxhash64")
    observation = read_local_native_delta_publication_observation(
        anchor=anchor, spark=spark, destination_transport=driver.transport,
        manifest_bytes=manifest_raw, vector=vector, receipt_bytes=receipt_raw,
        expected_post_state=expected)
    selection = anchor.bind_local_native_publication(
        manifest_bytes=manifest_raw, vector=vector, receipt_bytes=receipt_raw,
        expected_post_state=expected, observation=observation)
    custody = custody_session_factory(selection)
    if (type(selection) is not TrussSourceCustodySelection
            or selection.authority_profile != "local-development"
            or selection.request_bytes != request_raw
            or selection.receipt_bytes != receipt_raw
            or selection.publication_id != observation.publication_id
            or type(custody) is not Truss052SourceCustodySession
            or not callable(custody.runtime_status_provider)):
        raise TrussPublicationRecoveryError(
            "Exact local-development selection and fresh-status protected ACK session required")

    role_receipts = {
        role: {"table": role_value.table, "uuid": role_value.uuid,
               "version": role_value.version, "head_version": role_value.head_version,
               "head_json_sha256": role_value.head_json_sha256,
               "row_bag_sha256": role_value.row_bag_sha256,
               "row_count": role_value.row_count}
        for role, role_value in observation.roles.items()}
    completion = {
        "profile": "ashlar-truss-local-readback-complete/0.1",
        "status": "native_readback_verified_ack_not_attempted",
        "request_digest": request_digest,
        "pending_receipt_sha256": pending_sha,
        "source_page_sha256": page.sha256,
        "prior_cursor_sha256": hashlib.sha256(page.prior.content).hexdigest(),
        "next_cursor_sha256": hashlib.sha256(page.next.content).hexdigest(),
        "publication_id": observation.publication_id,
        "manifest_sha256": observation.manifest_sha256,
        "receipt_sha256": observation.receipt_sha256,
        "vector": {key: list(value) for key, value in observation.vector.targets.items()},
        "roles": role_receipts,
        "expected_post_state": expected.receipt(),
        "acknowledgement": "not_attempted_at_marker_creation",
        "publication_replayed": False,
    }
    completion_raw = _encoded(completion) + b"\n"
    completion_sha = _write_private_exclusive(Path(completion_path), completion_raw)

    # A separate durable intent conservatively records that an ACK may begin.
    # It follows native verification and the completion marker; no publisher is
    # reachable from this code path.
    ack_attempt = {
        "profile": "ashlar-truss-local-protected-ack-attempt/0.1",
        "status": "ack_may_be_attempted",
        "completion_marker_sha256": completion_sha,
        "pending_receipt_sha256": pending_sha,
        "publication_id": observation.publication_id,
        "page_sha256": page.sha256,
        "ack_may_have_been_attempted": True,
    }
    try:
        custody.open()
        observed_page = custody.observe_selected_page()
        if (observed_page.sha256 != page.sha256
                or observed_page.prior.content != page.prior.content
                or observed_page.next.content != page.next.content):
            raise TrussPublicationRecoveryError("Held Truss source page differs before protected ACK")
        ack_attempt_sha = _write_private_exclusive(Path(ack_attempt_path),
                                                    _encoded(ack_attempt) + b"\n")
        ack_error = None
        try:
            ack = custody.acknowledge()
        except Exception as error:
            # The durable attempt marker already exists. Never resubmit; ask
            # the same live session for its one permitted read-only outcome.
            ack = None
            ack_error = error
        reconciled = custody.reconcile()
        checkpoint = getattr(reconciled, "checkpoint", None)
        checkpoint_bytes = getattr(checkpoint, "content", None)
        outcome = getattr(reconciled, "outcome", None)
        if (type(outcome) is not str or outcome not in ("matched", "missing", "conflicting")
                or type(checkpoint_bytes) is not bytes):
            raise TrussPublicationRecoveryError(
                "Exact public Truss ACK reconciliation outcome and checkpoint required")
        matched = (outcome == "matched" and checkpoint_bytes == page.next.content)
        ack_receipt = {
            "profile": "ashlar-truss-local-protected-ack-reconciled/0.1",
            "status": ("matched_local_development_only" if matched
                       else "unmatched_local_development_only"),
            "completion_marker_sha256": completion_sha,
            "ack_attempt_marker_sha256": ack_attempt_sha,
            "pending_receipt_sha256": pending_sha,
            "publication_id": observation.publication_id,
            "page_sha256": page.sha256,
            "checkpoint_sha256": hashlib.sha256(checkpoint_bytes).hexdigest(),
            "outcome": outcome,
            "ack_call_outcome": ("returned" if ack_error is None else "raised_then_reconciled"),
            "ack_call_error_type": type(ack_error).__name__ if ack_error is not None else None,
            "acknowledgement": "matched" if matched else "not_confirmed",
            "publication_replayed": False,
            "production_authority": False,
        }
        ack_raw = _encoded(ack_receipt) + b"\n"
        ack_sha = _write_private_exclusive(Path(ack_reconciliation_path), ack_raw)
        if not matched:
            raise TrussPublicationRecoveryError(
                "Protected Truss ACK reconciliation did not match the selected next checkpoint")
        custody.close()
    finally:
        custody.dispose()
    return {"completion_marker_sha256": completion_sha,
            "ack_attempt_marker_sha256": ack_attempt_sha,
            "ack_reconciliation_sha256": ack_sha,
            "observation": observation, "expected_post_state": expected,
            "selection": selection, "ack": ack, "reconciliation": reconciled}
