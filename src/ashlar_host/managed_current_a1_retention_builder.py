# SPDX-License-Identifier: MIT OR Apache-2.0
"""Compose exact publisher/verifier bytes into the existing managed A1 read/pin owner."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib


class ManagedA1RetentionBuildError(ValueError):
    """Publisher, verifier, source plan or managed pin identities disagree."""


@dataclass(frozen=True)
class ManagedA1RetentionBundle:
    """Pure exact inputs for ManagedOperationalReadPinPort and source bind."""
    selection: object
    vector: object
    task_receipt_bytes: bytes
    destination_receipt_bytes: bytes


def build_managed_current_a1_retention_bundle(*, plan, reader_config,
        request_bytes: bytes, manifest_bytes: bytes, task_receipt: dict,
        destination_receipt_bytes: bytes, source_prefix_bytes: bytes,
        source_prefix_sha256: str, selected_prefix: dict,
        source_snapshot_bytes: bytes, source_custody_digests: dict,
        source_core_config):
    """Build an admitted current-fresh retention selection from exact task outputs.

    This performs no cloud/native read. The caller must still provide the real
    `ManagedOperationalReadPinPort` and its live session/source/policy owners.
    """
    import json
    from .r04_schedule import canonical
    from .managed_current_a1_trial_plan import ManagedCurrentA1TrialPlan
    from .managed_current_spark_receipt_read import (
        CARRIER_PROFILE_XXHASH64, ManagedCurrentSparkReceiptConfig,
    )
    from .truss_current_catalog import installed_fresh_truss_runtime_pins
    from .managed_operational_retention_interval import (
        ManagedOperationalRetentionSelection, SOURCE_CUSTODY_FIELDS,
    )
    from .truss_feed_sessions import encode_truss_destination_receipt
    from ashlar.manifest import manifest_pin_vector
    from ashlar.pins import PinVector

    runtime_pins = installed_fresh_truss_runtime_pins()
    if (type(plan) is not ManagedCurrentA1TrialPlan
            or type(reader_config) is not ManagedCurrentSparkReceiptConfig
            or reader_config.source_truss_version != runtime_pins["version"]
            or reader_config.carrier_profile != CARRIER_PROFILE_XXHASH64
            or type(task_receipt) is not dict):
        raise ManagedA1RetentionBuildError(
            "Exact installed fresh Truss A1 plan and Spark receipt profile required")
    if (type(request_bytes) is not bytes or request_bytes != canonical(plan.request)
            or type(manifest_bytes) is not bytes or not manifest_bytes
            or type(destination_receipt_bytes) is not bytes
            or type(source_prefix_bytes) is not bytes or not source_prefix_bytes
            or type(source_prefix_sha256) is not str
            or hashlib.sha256(source_prefix_bytes).hexdigest() != source_prefix_sha256
            or type(selected_prefix) is not dict):
        raise ManagedA1RetentionBuildError("Exact canonical A1 request, manifest, receipt and prefix required")
    try:
        manifest = json.loads(manifest_bytes.decode("utf-8"))
        if canonical(manifest) != manifest_bytes:
            raise ValueError("noncanonical manifest")
        inventory = task_receipt["installationInventory"]
        versions = json.loads(manifest["table_versions_json"])
        table_names = {f"{reader_config.catalog}.{reader_config.schema}.{role}"
                       for role in ("object_current", "edge_current", "tombstone",
                                    "whole_source_history")}
        uuids = {f"{reader_config.catalog}.{reader_config.schema}.{role}":
                 inventory[role]["uuid"] for role in (
                     "object_current", "edge_current", "tombstone", "whole_source_history")}
        vector = manifest_pin_vector(manifest, uuids,
            authority=f"managed-current-spark:{reader_config.expected_principal}")
        expected_vector = {table: {"uuid": uuid, "version": versions[table]}
                           for table, uuid in uuids.items()}
        if (type(inventory) is not dict
                or not {"publicationId", "manifestSha256", "requestDigest",
                        "dataRoleVector", "sourceAuthority"}.issubset(task_receipt)
                or task_receipt["publicationId"] != manifest["publication_id"]
                or task_receipt["manifestSha256"] != hashlib.sha256(manifest_bytes).hexdigest()
                or task_receipt["requestDigest"] != plan.request["request_digest"]
                or task_receipt["dataRoleVector"] != expected_vector
                or set(versions) != table_names or set(vector.targets) != table_names
                or any(vector.targets[table] != (uuids[table], versions[table])
                       for table in table_names)
                or task_receipt["sourceAuthority"] !=
                    f"development Truss {runtime_pins['version']} page on managed Unity Catalog; external source ACK required"):
            raise ValueError("publisher/verifier vector, manifest or source identity differs")
        task_receipt_bytes = canonical(task_receipt)
        if json.loads(task_receipt_bytes.decode("utf-8")) != task_receipt:
            raise ValueError("task receipt is not canonical finite JSON")
        from .truss_detached_source_snapshot_codec import decode_detached_source_snapshot
        if type(source_snapshot_bytes) is not bytes or not source_snapshot_bytes:
            raise ValueError("Exact child-held A source snapshot bytes required")
        source_receipt = encode_truss_destination_receipt(
            request_bytes, manifest_bytes, vector)
        if source_receipt != destination_receipt_bytes:
            raise ValueError("destination receipt differs from exact request/manifest/vector")
        detached = decode_detached_source_snapshot(source_snapshot_bytes)
        from .truss053_source_custody import detached_source_custody_digests
        computed_custody = detached_source_custody_digests(detached)
        if computed_custody != source_custody_digests:
            raise ValueError("Source custody digest map differs from the detached held A page")
        if (type(source_custody_digests) is not dict
                or set(source_custody_digests) != set(SOURCE_CUSTODY_FIELDS)
                or any(type(value) is not str or len(value) != 64
                       or any(c not in "0123456789abcdef" for c in value)
                       for value in source_custody_digests.values())):
            raise ValueError("Exact eight-field live source custody digests required")
        selection = ManagedOperationalRetentionSelection(
            reader_config=reader_config, request_bytes=request_bytes,
            manifest_bytes=manifest_bytes, receipt_bytes=task_receipt_bytes,
            expected_receipt=task_receipt, vector=vector,
            expected_source_custody=source_custody_digests,
            source_prefix_bytes=source_prefix_bytes,
            source_prefix_sha256=source_prefix_sha256,
            source_core_config=source_core_config, a1_plan=plan,
            a1_prefix_selected=selected_prefix)
    except (KeyError, TypeError, ValueError) as error:
        raise ManagedA1RetentionBuildError(
            "Exact publisher/verifier/source-owner A1 admission could not be composed") from error
    if type(vector) is not PinVector:
        raise ManagedA1RetentionBuildError("Native four-role PinVector required")
    return ManagedA1RetentionBundle(selection, vector, task_receipt_bytes,
                                    destination_receipt_bytes)
