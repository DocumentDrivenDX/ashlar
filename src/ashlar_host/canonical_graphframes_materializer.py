# SPDX-License-Identifier: MIT OR Apache-2.0
"""Narrow local materializer for the frozen R05 GraphFrames discriminator.

This is fixture preparation only. It deliberately accepts an existing Spark
session and never starts one or exposes paths until both tables pass admission.
If a process fails mid-write, partial tables may remain in the private root.
Cleanup requires a future explicit owner path that reacquires this stable lock
and verifies owned paths and Delta UUIDs. This module does not guess cleanup
ownership or remove partial outputs.
"""
from __future__ import annotations

import hashlib
import fcntl
import importlib.util
import json
import os
from pathlib import Path
import re
import stat
import tempfile

from .local_current_delta4_umf import LocalUmfDelta4Bridge, _pinned_resources
from .managed_current_xxhash64_carriers import load_current_managed_xxhash64_carriers
from .pinned_delta_graphframes import _quote_path

EVIDENCE = Path(__file__).resolve().parents[2] / "docs/helix/04-build/evidence/canonical-graphframes-holder-candidate-20261023/action-discriminator-r1"
ROLES = ("object_current", "edge_current")
EXPECTED_PROTOCOL = {
    "minReaderVersion": 1,
    "minWriterVersion": 7,
    "writerFeatures": ["appendOnly", "clustering", "domainMetadata", "invariants"],
}


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _frozen_source():
    manifest_raw = (EVIDENCE / "manifest.json").read_bytes()
    if len(manifest_raw) > 65536:
        raise PermissionError("Frozen discriminator manifest exceeds its bound")
    manifest = json.loads(manifest_raw)
    generator_path = EVIDENCE / "generator.py"
    source_path = EVIDENCE / "source-rows.json"
    oracle_path = EVIDENCE / "independent-oracle.json"
    inventory_path = EVIDENCE / "artifacts.sha256"
    inventory = {}
    for line in inventory_path.read_text(encoding="utf-8").splitlines():
        parts = line.split("  ", 1)
        if (len(parts) != 2 or re.fullmatch(r"[0-9a-f]{64}", parts[0]) is None
                or parts[1] in inventory):
            raise PermissionError("Frozen discriminator artifact inventory is malformed")
        digest, relative = parts
        inventory[relative] = digest
    source_raw = source_path.read_bytes()
    generator_raw = generator_path.read_bytes()
    oracle_raw = oracle_path.read_bytes()
    if len(source_raw) > 1_048_576:
        raise PermissionError("Frozen discriminator source exceeds its bound")
    for name, relative, raw, canonical in (
            ("generatorSha256", "generator.py", generator_raw, generator_raw),
            ("sourceSha256", "source-rows.json", source_raw, source_raw[:-1]),
            ("oracleSha256", "independent-oracle.json", oracle_raw, oracle_raw)):
        expected = manifest.get(name)
        if (type(expected) is not str or _sha(canonical) != expected
                or inventory.get(relative) != _sha(raw)):
            raise PermissionError(f"Frozen canonical discriminator {name} mismatch")
    if not source_raw.endswith(b"\n") or source_raw.endswith(b"\n\n"):
        raise PermissionError("Frozen source artifact must have exactly one trailing LF")
    if manifest.get("rowCounts") != [5, 4]:
        raise PermissionError("Frozen five-node/four-edge discriminator required")
    spec = importlib.util.spec_from_file_location("_frozen_r05_discriminator", generator_path)
    if spec is None or spec.loader is None:
        raise PermissionError("Frozen source generator cannot be loaded")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    document = module.source_document()
    if _sha(module.source_bytes()) != manifest["sourceSha256"]:
        raise PermissionError("Frozen source rows differ from the independently retained source bytes")
    expected_doc = json.loads(source_path.read_bytes())
    if document != expected_doc:
        raise PermissionError("Generator output differs from frozen source rows")
    return document


def _private_root(path):
    supplied = Path(path)
    if not supplied.is_absolute():
        raise PermissionError("Absolute canonical fixture root required")
    supplied_info = supplied.lstat()
    if stat.S_ISLNK(supplied_info.st_mode):
        raise PermissionError("Fixture root cannot be a symlink")
    root = supplied.resolve(strict=True)
    info = root.lstat()
    temporary_root = Path(tempfile.gettempdir()).resolve(strict=True)
    try:
        root.relative_to(temporary_root)
        under_temporary_root = True
    except ValueError:
        under_temporary_root = False
    if (stat.S_ISLNK(info.st_mode) or not root.is_dir()
            or not under_temporary_root or info.st_mode & 0o077
            or (hasattr(os, "getuid") and info.st_uid != os.getuid())):
        raise PermissionError("A canonical, nonsymlink private mode-0700 fixture root is required")
    return root


def _acquire_lock(root):
    lock = root / ".ashlar-graphframes-action.lock"
    fd = os.open(lock, os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0), 0o600)
    info = os.fstat(fd)
    current = os.stat(lock, follow_symlinks=False)
    if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_mode & 0o077
            or (info.st_dev, info.st_ino) != (current.st_dev, current.st_ino)):
        os.close(fd)
        raise PermissionError("Stable private fixture lock inode required")
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        os.close(fd)
        raise PermissionError("Canonical fixture root is already in use") from exc
    return fd


def _protocol(detail):
    features = detail.get("tableFeatures")
    if (detail.get("minReaderVersion") != EXPECTED_PROTOCOL["minReaderVersion"]
            or detail.get("minWriterVersion") != EXPECTED_PROTOCOL["minWriterVersion"]
            or type(features) is not list
            or not all(type(item) is str for item in features)
            or len(features) != len(set(features))
            or set(features) != set(EXPECTED_PROTOCOL["writerFeatures"])):
        raise PermissionError("Created Delta table does not have the exact writer-7 protocol")


def _spark_type_names(fields):
    """Validate UMF carrier kinds and return their native Spark type names."""
    allowed = {"STRING", "BIGINT", "TIMESTAMP"}
    if (type(fields) is not tuple or not fields
            or any(type(item) is not tuple or len(item) != 3
                   or item[1] not in allowed for item in fields)):
        raise ValueError("Exact UMF carrier field types required")
    return tuple((name, kind, nullable) for name, kind, nullable in fields)


def _verify_detail_layout(detail, layout):
    """Require observable clustering and statistics metadata from checked UMF."""
    if (type(detail) is not dict or type(layout) is not dict
            or type(layout.get("clusterBy")) is not list
            or type(layout.get("properties")) is not dict):
        raise PermissionError("Complete checked UMF local layout and Delta detail required")
    properties = detail.get("properties")
    if (type(properties) is not dict
            or properties.get("delta.dataSkippingStatsColumns")
            != layout["properties"].get("delta.dataSkippingStatsColumns")
            or detail.get("clusteringColumns") != layout["clusterBy"]):
        raise PermissionError("Persisted Delta layout differs from selected UMF local layout")


def materialize_canonical_discriminator(spark, fixture_root):
    """Create the frozen 5/4 Delta paths with checked UMF Delta-4 DDL.

    Returns the two paths only after schema, protocol and row counts pass.
    Caller owns the private root lifecycle and must keep it exclusive.
    """
    if spark is None:
        raise ValueError("An explicitly supplied Spark session is required")
    if getattr(spark, "version", None) != "4.2.0":
        raise PermissionError("Exact Spark 4.2.0 local runtime required before DDL")
    root = _private_root(fixture_root)
    for role in ROLES:
        _quote_path(root / role)
    lock_fd = _acquire_lock(root)
    try:
        allowed = {".ashlar-graphframes-action.lock"}
        if {entry.name for entry in root.iterdir()} != allowed:
            raise FileExistsError("Fresh fixture root containing only the stable lock is required")
        source = _frozen_source()
        carriers = load_current_managed_xxhash64_carriers()
        generated = json.loads(carriers.generated_bytes)
        _manifest, _bundle, sources, semantics = _pinned_resources()
        if (generated.get("profile") != "ashlar-managed-current-carriers/0.4-xxhash64"
                or generated.get("generator", {}).get("revision") != source["umfRevision"]):
            raise PermissionError("Selected UMF `.4` generated carrier is not admitted")
        quoted_paths = {role: _quote_path(root / role) for role in ROLES}
        with LocalUmfDelta4Bridge.open() as bridge:
            proposals = {}
            for role in ROLES:
                path = str(root / role)
                umf_name = role + ".umf.json"
                proposals[role] = bridge.realize(sources[umf_name], path, role)["proposal"]
            for proposal in proposals.values():
                for setting in proposal["writerSettings"]:
                    spark.conf.set(setting["key"], setting["value"])
                spark.sql(proposal["sql"])

        from pyspark.sql import functions as F
        from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType
        types = {"STRING": StringType, "BIGINT": LongType, "TIMESTAMP": TimestampType}
        rows_by_role = source["tables"]
        for role in ROLES:
            fields = _spark_type_names(carriers.ordered_fields(role))
            schema = StructType([StructField(name, types[kind](), nullable)
                                 for name, kind, nullable in fields])
            identity = "type_id" if role == "object_current" else "rel_type_id"
            values = []
            from datetime import datetime
            for row in rows_by_role[role]:
                values.append(tuple(datetime.fromisoformat(row[name].replace("Z", "+00:00"))
                                    if name == "published_at" else row.get(name)
                                    for name, _kind, _nullable in fields if name != "lookup_hash"))
            input_schema = StructType([field for field in schema.fields if field.name != "lookup_hash"])
            frame = spark.createDataFrame(values, schema=input_schema).withColumn(
                "lookup_hash", F.xxhash64(F.col("source_system").cast("string"),
                                         F.col(identity).cast("long"), F.col("id").cast("long")))
            ordered = frame.select(*(name for name, _kind, _nullable in fields))
            ordered.write.format("delta").mode("append").save(str(root / role))

        for role in ROLES:
            path = str(root / role)
            detail_rows = spark.sql(f"DESCRIBE DETAIL {quoted_paths[role]}").collect()
            if len(detail_rows) != 1:
                raise PermissionError("Exactly one Delta table detail row required")
            detail = detail_rows[0].asDict(recursive=True)
            _protocol(detail)
            _verify_detail_layout(detail, semantics[role]["layout"])
            if detail.get("numFiles", 0) < 1:
                raise PermissionError("Materialized Delta table has no data files")
            expected = 5 if role == "object_current" else 4
            table = spark.read.format("delta").load(path)
            expected_fields = carriers.ordered_fields(role)
            got = tuple((field.name, field.dataType.simpleString().upper(), field.nullable)
                        for field in table.schema.fields)
            expected_signature = _spark_type_names(expected_fields)
            if got != expected_signature:
                raise PermissionError(f"Materialized {role} schema differs from checked UMF columns")
            if table.count() != expected:
                raise PermissionError(f"Materialized {role} row count mismatch")
        return {role: str(root / role) for role in ROLES}
    finally:
        os.close(lock_fd)
