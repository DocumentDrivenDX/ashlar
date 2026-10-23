# SPDX-License-Identifier: MIT OR Apache-2.0
"""Internal synthetic pinned-Delta GraphFrames candidate (no publication authority)."""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
import hashlib
import importlib.resources
import fcntl
import json
import os
from pathlib import Path
import re
import stat
import time
from itertools import islice
from types import MappingProxyType

PROFILE = "ashlar-synthetic-pinned-delta-graphframes/0.1"
GENERATOR_REVISION = "ashlar-graphframes-struct-smoke-fixture/0.1"
FIXTURE_ID = "graphframes-struct-ids-parallel-edge-smoke/0.1"
SOURCE_SYSTEM = "ashlar-graphframes-smoke"
# Delta 4.4.1 DESCRIBE DETAIL reports the implicitly supported legacy writer
# features for protocol (1, 2), even though they are absent from the protocol
# action's optional readerFeatures/writerFeatures fields.
PROTOCOL = {"minReaderVersion": 1, "minWriterVersion": 2,
            "tableFeatures": ["appendOnly", "invariants"]}
MAX_ONE_HOP_ROWS = 16
MAX_LEASE_SECONDS = 300
FIXTURE_INPUT_SHA256 = "00fea3fb2372d99add7a03fe576d0833eae16afb9e2e8139da06cd289b6a4e68"

NODE_SCHEMA = (
    # The actual Delta 4.4.1 path writer persists these Spark-created columns
    # as nullable. The exact fixture row-bag check separately proves every
    # observed identity component has its declared non-null fixture value.
    ("source_system", "string", True),
    ("type_id", "bigint", True),
    ("id", "bigint", True),
)
EDGE_SCHEMA = (
    ("source_system", "string", True),
    ("rel_type_id", "bigint", True),
    ("id", "bigint", True),
    ("source_type", "bigint", True),
    ("source_id", "bigint", True),
    ("target_type", "bigint", True),
    ("target_id", "bigint", True),
)

# This closed input is intentionally tiny. The expected one-hop bag is frozen
# separately below; it is not computed from these input rows at execution time.
_NODES = (
    (SOURCE_SYSTEM, 1, 101),
    (SOURCE_SYSTEM, 1, 202),
    (SOURCE_SYSTEM, 1, 303),  # isolated vertex
)
_EDGES = (
    (SOURCE_SYSTEM, 7, 9001, 1, 101, 1, 202),
    (SOURCE_SYSTEM, 7, 9002, 1, 101, 1, 202),  # parallel, independent identity
)
SEED = (SOURCE_SYSTEM, 1, 101)
EXPECTED_ONE_HOP = (
    (SOURCE_SYSTEM, 1, 101, SOURCE_SYSTEM, 7, 9001, SOURCE_SYSTEM, 1, 202),
    (SOURCE_SYSTEM, 1, 101, SOURCE_SYSTEM, 7, 9002, SOURCE_SYSTEM, 1, 202),
)


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


def fixture_input_digest():
    """Digest the frozen generator revision, parameters, and exact source rows."""
    payload = {
        "fixtureId": FIXTURE_ID,
        "generatorRevision": GENERATOR_REVISION,
        "parameters": {"parallelEdges": 2, "selectedSeed": list(SEED),
                       "isolatedNode": [SOURCE_SYSTEM, 1, 303]},
        "tables": {"object_current": [list(row) for row in _NODES],
                   "edge_current": [list(row) for row in _EDGES]},
    }
    digest = hashlib.sha256(_canonical(payload)).hexdigest()
    if digest != FIXTURE_INPUT_SHA256:
        raise ValueError("Frozen synthetic GraphFrames generator input changed")
    return digest


@dataclass(frozen=True)
class SyntheticGraphFixture:
    fixture_id: str = FIXTURE_ID
    generator_revision: str = GENERATOR_REVISION
    input_sha256: str = ""
    node_rows: tuple = _NODES
    edge_rows: tuple = _EDGES
    selected_seed: tuple = SEED
    expected_one_hop: tuple = EXPECTED_ONE_HOP

    def __post_init__(self):
        if (self.fixture_id != FIXTURE_ID or self.generator_revision != GENERATOR_REVISION
                or self.node_rows != _NODES or self.edge_rows != _EDGES
                or self.selected_seed != SEED or self.expected_one_hop != EXPECTED_ONE_HOP):
            raise ValueError("Frozen synthetic GraphFrames fixture required")
        if self.input_sha256 not in ("", FIXTURE_INPUT_SHA256):
            raise ValueError("Frozen synthetic GraphFrames input digest required")
        object.__setattr__(self, "input_sha256", fixture_input_digest())


def _quote_path(path):
    text = str(path)
    if (not text.startswith("/") or "\x00" in text or "`" in text
            or "\n" in text or "\r" in text or len(text.encode()) > 4096):
        raise ValueError("Canonical bounded local Delta path required")
    return "delta.`" + text + "`"


def _row_dict(row):
    if hasattr(row, "asDict"):
        return row.asDict(recursive=True)
    if type(row) is dict:
        return row
    raise ValueError("Native bounded Spark metadata row required")


def _actual_schema_signature(schema):
    try:
        fields = tuple((field.name, field.dataType.simpleString(), bool(field.nullable))
                       for field in schema.fields)
        raw = schema.json().encode("utf-8")
    except Exception as error:
        raise ValueError("Native Delta StructType required") from error
    return fields, hashlib.sha256(raw).hexdigest()


@dataclass(frozen=True)
class SyntheticDeltaTablePin:
    role: str
    table_uuid: str
    version: int
    schema_sha256: str
    protocol: dict

    def __post_init__(self):
        if (self.role not in ("object_current", "edge_current")
                or type(self.table_uuid) is not str
                or re.fullmatch(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", self.table_uuid) is None
                or type(self.version) is not int or self.version != 0
                or type(self.schema_sha256) is not str
                or re.fullmatch(r"[0-9a-f]{64}", self.schema_sha256) is None
                or self.protocol != PROTOCOL):
            raise ValueError("Exact synthetic current-table Delta identity required")


class PinnedSyntheticDeltaFixture:
    """Actual Spark/Delta observer for exactly two immutable fixture tables."""

    def __init__(self, spark, fixture, table_paths, *, maximum_lease_seconds=120):
        if type(fixture) is not SyntheticGraphFixture:
            raise ValueError("Closed frozen synthetic fixture required")
        if type(table_paths) is not dict or set(table_paths) != {"object_current", "edge_current"}:
            raise ValueError("Exactly the two current graph fixture tables required")
        if type(maximum_lease_seconds) is not int or not 0 < maximum_lease_seconds <= MAX_LEASE_SECONDS:
            raise ValueError("Finite synthetic fixture lease required")
        self._spark = spark
        self.fixture = fixture
        paths = {}
        for role, path in table_paths.items():
            text = str(path)
            _quote_path(text)
            paths[role] = text
        if len(set(paths.values())) != 2:
            raise ValueError("Distinct current-table Delta paths required")
        self._paths = paths
        self._maximum_lease_seconds = maximum_lease_seconds

    def _observe_table(self, role):
        path = self._paths[role]
        name = _quote_path(path)
        details = self._spark.sql("DESCRIBE DETAIL " + name).limit(2).collect()
        if len(details) != 1:
            raise ValueError("One actual Delta table detail row required")
        detail = _row_dict(details[0])
        if detail.get("format") != "delta":
            raise ValueError("Actual Delta current table required")
        history = self._spark.sql("DESCRIBE HISTORY " + name + " LIMIT 1").limit(2).collect()
        if len(history) != 1:
            raise ValueError("One actual Delta head version required")
        version = _row_dict(history[0]).get("version")
        if type(version) is not int:
            raise ValueError("Native Delta table version required")
        protocol = {
            "minReaderVersion": detail.get("minReaderVersion"),
            "minWriterVersion": detail.get("minWriterVersion"),
            "tableFeatures": sorted(detail.get("tableFeatures") or []),
        }
        if protocol != PROTOCOL:
            observed_features = detail.get("tableFeatures")
            if (type(observed_features) is list and len(observed_features) <= 16
                    and all(type(item) is str and len(item) <= 96
                            and re.fullmatch(r"[A-Za-z0-9_-]+", item) is not None
                            for item in observed_features)):
                features_summary = sorted(observed_features)
            else:
                features_summary = {"kind": type(observed_features).__name__,
                                    "count": len(observed_features)
                                    if isinstance(observed_features, (list, tuple)) else None}
            raise ValueError(
                "Observed Delta protocol differs from exact writer-2 legacy feature set: "
                + json.dumps({"minReaderVersion": detail.get("minReaderVersion"),
                              "minWriterVersion": detail.get("minWriterVersion"),
                              "tableFeatures": features_summary},
                             ensure_ascii=True, sort_keys=True, separators=(",", ":")))
        reader = self._spark.read.format("delta")
        schema = reader.load(path).schema
        signature, schema_sha = _actual_schema_signature(schema)
        if signature != (NODE_SCHEMA if role == "object_current" else EDGE_SCHEMA):
            raise ValueError("Exact frozen current-table fixture schema required")
        pin = SyntheticDeltaTablePin(role, detail.get("id"), version, schema_sha, protocol)
        return pin

    def open(self):
        # Two complete observations make table UUID/head/schema/protocol changes
        # during lease acquisition a refusal, rather than a mixed starting pin.
        opening = {role: self._observe_table(role) for role in ("object_current", "edge_current")}
        if {role: self._observe_table(role) for role in ("object_current", "edge_current")} != opening:
            raise PermissionError("Synthetic Delta fixture changed while pinning")
        return SyntheticDeltaFixtureLease(self, opening, time.monotonic() + self._maximum_lease_seconds)


class SyntheticDeltaFixtureLease:
    def __init__(self, owner, pins, deadline):
        self._owner = owner
        self.pins = dict(pins)
        self._deadline = deadline
        self._closed = False

    def renew(self):
        if self._closed or time.monotonic() >= self._deadline:
            raise PermissionError("Synthetic Delta fixture lease expired")
        try:
            current = {role: self._owner._observe_table(role)
                       for role in ("object_current", "edge_current")}
        except (ValueError, TypeError) as error:
            raise PermissionError("Synthetic Delta fixture identity/head/schema/protocol changed") from error
        if current != self.pins:
            raise PermissionError("Synthetic Delta fixture identity/head/schema/protocol changed")
        return None

    def current_frames(self):
        self.renew()
        frames = {}
        expected_rows = {
            "object_current": self._owner.fixture.node_rows,
            "edge_current": self._owner.fixture.edge_rows,
        }
        columns = {
            "object_current": ("source_system", "type_id", "id"),
            "edge_current": ("source_system", "rel_type_id", "id", "source_type",
                             "source_id", "target_type", "target_id"),
        }
        for role in ("object_current", "edge_current"):
            pin = self.pins[role]
            path = self._owner._paths[role]
            frame = (self._owner._spark.read.format("delta")
                     .option("versionAsOf", pin.version).load(path))
            signature, digest = _actual_schema_signature(frame.schema)
            if (signature != (NODE_SCHEMA if role == "object_current" else EDGE_SCHEMA)
                    or digest != pin.schema_sha256):
                raise PermissionError("Pinned Delta snapshot schema differs")
            # This fixture is deliberately tiny. Read only its exact declared
            # columns and at most expected-count+1 rows to establish the whole
            # table bag before GraphFrames sees it; this is never used on a
            # production or large graph table.
            observed = frame.select(*columns[role]).limit(len(expected_rows[role]) + 1).collect()
            actual = tuple(sorted(tuple(_row_dict(row)[column] for column in columns[role])
                                  for row in observed))
            if actual != tuple(sorted(expected_rows[role])):
                raise PermissionError("Pinned Delta tiny fixture row bag differs from frozen input")
            frames[role] = frame
        return frames

    def evidence(self):
        self.renew()
        return {
            "fixtureId": self._owner.fixture.fixture_id,
            "generatorRevision": self._owner.fixture.generator_revision,
            "inputSha256": self._owner.fixture.input_sha256,
            "sourceProgress": "absent",
            "protectedAck": "absent",
            "tables": {role: {
                "uuid": pin.table_uuid,
                "version": pin.version,
                "schemaSha256": pin.schema_sha256,
                "protocol": pin.protocol,
            } for role, pin in self.pins.items()},
        }

    def close(self):
        if self._closed:
            return None
        self.renew()
        self._closed = True
        return None


@contextmanager
def hold_synthetic_delta_fixture(owner):
    if type(owner) is not PinnedSyntheticDeltaFixture:
        raise ValueError("Actual pinned synthetic Delta fixture owner required")
    lease = owner.open()
    primary = None
    try:
        lease.renew()
        yield lease
    except BaseException as error:
        primary = error
        raise
    finally:
        try:
            lease.close()
        except BaseException:
            if primary is None:
                raise


def create_synthetic_delta_fixture(spark, table_paths, fixture=None):
    """Write the frozen tiny fixture once; caller must supply fresh Delta paths."""
    from pyspark.sql.types import StructType, StructField, StringType, LongType
    selected = fixture if fixture is not None else SyntheticGraphFixture()
    if type(selected) is not SyntheticGraphFixture:
        raise ValueError("Closed frozen synthetic fixture required")
    if type(table_paths) is not dict or set(table_paths) != {"object_current", "edge_current"}:
        raise ValueError("Exactly two fresh current-table Delta paths required")
    signatures = {"object_current": NODE_SCHEMA, "edge_current": EDGE_SCHEMA}
    values = {"object_current": selected.node_rows, "edge_current": selected.edge_rows}
    for role in ("object_current", "edge_current"):
        path = str(table_paths[role])
        _quote_path(path)
        fields = signatures[role]
        schema = StructType([StructField(name,
            StringType() if kind == "string" else LongType(), nullable)
            for name, kind, nullable in fields])
        (spark.createDataFrame(list(values[role]), schema)
         .write.format("delta").mode("errorifexists").save(path))
    return PinnedSyntheticDeltaFixture(spark, selected, table_paths)


def _graphframes_views(nodes, edges, functions):
    """Map canonical .4 carrier frames to transient GraphFrames views.

    All carrier payload columns survive. The scalar node ``id`` is retained as
    ``ashlar_entity_id`` while GraphFrames receives a source-qualified struct
    in its required ``id`` column. Edge identity remains an independent struct;
    no row collection, hashing, deduplication, or persisted copy occurs here.
    """
    required_nodes = {"source_system", "type_id", "id"}
    required_edges = {"source_system", "rel_type_id", "id", "source_type",
                      "source_id", "target_type", "target_id"}
    if not required_nodes.issubset(set(nodes.columns)):
        raise ValueError("Canonical .4 object_current columns required")
    if not required_edges.issubset(set(edges.columns)):
        raise ValueError("Canonical .4 edge_current columns required")
    if "ashlar_entity_id" in nodes.columns:
        raise ValueError("Reserved transient node value column already exists")
    if {"src", "dst", "edge_id"}.intersection(edges.columns):
        raise ValueError("Reserved transient edge columns already exist")

    def typed_struct(fields):
        return functions.struct(*(functions.col(column).cast(kind).alias(alias)
                                  for column, kind, alias in fields))

    node_identity = typed_struct((
        ("source_system", "string", "source_system"),
        ("type_id", "long", "type_id"),
        ("id", "long", "id"),
    ))
    vertex_columns = [functions.col(name) for name in nodes.columns if name != "id"]
    vertex_columns.extend((functions.col("id").alias("ashlar_entity_id"),
                           node_identity.alias("id")))
    vertices = nodes.select(*vertex_columns)

    src = typed_struct((
        ("source_system", "string", "source_system"),
        ("source_type", "long", "type_id"),
        ("source_id", "long", "id"),
    ))
    dst = typed_struct((
        ("source_system", "string", "source_system"),
        ("target_type", "long", "type_id"),
        ("target_id", "long", "id"),
    ))
    edge_identity = typed_struct((
        ("source_system", "string", "source_system"),
        ("rel_type_id", "long", "rel_type_id"),
        ("id", "long", "id"),
    ))
    graph_edges = edges.select(*(tuple(functions.col(name) for name in edges.columns)
                                 + (src.alias("src"), dst.alias("dst"),
                                    edge_identity.alias("edge_id"))))
    return vertices, graph_edges


CANONICAL_SYNTHETIC_PROFILE = "ashlar-canonical-synthetic-pinned-delta/0.1"
CANONICAL_CARRIER_PROFILE = "ashlar-delta/0.4-xxhash64"
CANONICAL_UMF_REVISION = "2ede4c879bea5db13794a6fa30e67c821306c17f"
CANONICAL_RUNTIME_SELECTION = (("spark", "4.2.0"), ("delta", "4.4.1"),
                               ("graphframes", "0.12.3"), ("jdk", "21.0.13"))

# Hashes are carried by the passing 2026-10-21 compatibility receipt.  The
# class-location check below proves which JVM artifacts are actually loaded;
# Python distribution metadata and spark.jars configuration are not used.
CANONICAL_RUNTIME_ARTIFACTS = {
    "delta-spark_4.2_2.13-4.4.1.jar": "2b1d2cd411bf0bbced64b96fe621f8ffce7bb6e394c8064ab783fc6edce449be",
    "delta-storage-4.4.1.jar": "f585818d646cb3625b305270ec24f3e6f0b6ea63ed9bac1ae8f340f8faaeae8f",
    "graphframes-spark4_2.13-0.12.3.jar": "a78958f7ea3ac89ed1ea9dbbd1546f403d94151e969bb0de96e33697a5f49f50",
}
MAX_SNAPSHOT_FILES = 100_000
MAX_SNAPSHOT_METADATA_BYTES = 16 * 1024 * 1024

# Small canonical `.4` action fixture. The value rows are deliberately source
# records, before the generated Spark-native pruning hash is added. The
# independent one-hop occurrence bag lives in action-oracle.json and is never
# derived by this module.
CANONICAL_FIXTURE_GENERATOR_REVISION = "canonical-graphframes-action-fixture/0.1"
CANONICAL_FIXTURE_SOURCE_SYSTEM = "ashlar-canonical-graphframes"
CANONICAL_FIXTURE_SCHEMA_REVISION = "canonical-action-r1"
CANONICAL_FIXTURE_SOURCE_FEED = "synthetic-canonical-action"
CANONICAL_FIXTURE_EPOCH = "1"
CANONICAL_FIXTURE_PUBLISHED_AT = "2026-10-23T00:00:00.123456Z"
CANONICAL_FIXTURE_SEED = (CANONICAL_FIXTURE_SOURCE_SYSTEM, 1, 101)
CANONICAL_FIXTURE_NODE_ROWS = (
    {"source_system": CANONICAL_FIXTURE_SOURCE_SYSTEM, "type_id": 1, "id": 101,
     "logical_key_json": "[]", "schema_revision": CANONICAL_FIXTURE_SCHEMA_REVISION,
     "entity_version": 1, "props_json": '{"name":"seed"}', "retained_json": "{}",
     "root_id": None, "source_feed": CANONICAL_FIXTURE_SOURCE_FEED,
     "source_epoch": CANONICAL_FIXTURE_EPOCH, "source_position": 1,
     "published_at": CANONICAL_FIXTURE_PUBLISHED_AT, "apply_batch_id": "fixture-nodes-r1",
     "source_cursor_json": "{}", "source_delivery_id": "node-delivery-101"},
    {"source_system": CANONICAL_FIXTURE_SOURCE_SYSTEM, "type_id": 1, "id": 202,
     "logical_key_json": "[]", "schema_revision": CANONICAL_FIXTURE_SCHEMA_REVISION,
     "entity_version": 1, "props_json": '{"name":"target"}', "retained_json": "{}",
     "root_id": None, "source_feed": CANONICAL_FIXTURE_SOURCE_FEED,
     "source_epoch": CANONICAL_FIXTURE_EPOCH, "source_position": 2,
     "published_at": CANONICAL_FIXTURE_PUBLISHED_AT, "apply_batch_id": "fixture-nodes-r1",
     "source_cursor_json": "{}", "source_delivery_id": "node-delivery-202"},
    {"source_system": CANONICAL_FIXTURE_SOURCE_SYSTEM, "type_id": 1, "id": 303,
     "logical_key_json": "[]", "schema_revision": CANONICAL_FIXTURE_SCHEMA_REVISION,
     "entity_version": 1, "props_json": '{"name":"isolated"}', "retained_json": "{}",
     "root_id": None, "source_feed": CANONICAL_FIXTURE_SOURCE_FEED,
     "source_epoch": CANONICAL_FIXTURE_EPOCH, "source_position": 3,
     "published_at": CANONICAL_FIXTURE_PUBLISHED_AT, "apply_batch_id": "fixture-nodes-r1",
     "source_cursor_json": "{}", "source_delivery_id": "node-delivery-303"},
)
CANONICAL_FIXTURE_EDGE_ROWS = (
    {"source_system": CANONICAL_FIXTURE_SOURCE_SYSTEM, "rel_type_id": 7, "id": 9001,
     "source_type": 1, "source_id": 101, "target_type": 1, "target_id": 202,
     "schema_revision": CANONICAL_FIXTURE_SCHEMA_REVISION, "entity_version": 1,
     "props_json": '{"ordinal":1}', "retained_json": "{}", "order_key": None,
     "source_feed": CANONICAL_FIXTURE_SOURCE_FEED, "source_epoch": CANONICAL_FIXTURE_EPOCH,
     "source_position": 1, "published_at": CANONICAL_FIXTURE_PUBLISHED_AT,
     "apply_batch_id": "fixture-edges-r1", "source_cursor_json": "{}",
     "source_delivery_id": "edge-delivery-9001"},
    {"source_system": CANONICAL_FIXTURE_SOURCE_SYSTEM, "rel_type_id": 7, "id": 9002,
     "source_type": 1, "source_id": 101, "target_type": 1, "target_id": 202,
     "schema_revision": CANONICAL_FIXTURE_SCHEMA_REVISION, "entity_version": 1,
     "props_json": '{"ordinal":2}', "retained_json": "{}", "order_key": None,
     "source_feed": CANONICAL_FIXTURE_SOURCE_FEED, "source_epoch": CANONICAL_FIXTURE_EPOCH,
     "source_position": 2, "published_at": CANONICAL_FIXTURE_PUBLISHED_AT,
     "apply_batch_id": "fixture-edges-r1", "source_cursor_json": "{}",
     "source_delivery_id": "edge-delivery-9002"},
)


def canonical_fixture_source_bytes():
    """Return exact source rows and formulas frozen by the external manifest."""
    return _canonical({
        "profile": CANONICAL_SYNTHETIC_PROFILE,
        "generatorRevision": CANONICAL_FIXTURE_GENERATOR_REVISION,
        "carrierProfile": CANONICAL_CARRIER_PROFILE,
        "carrierDefinitionSha256": _canonical_carrier_definition_sha256(),
        "umfRevision": CANONICAL_UMF_REVISION,
        "runtime": dict(CANONICAL_RUNTIME_SELECTION),
        "runtimeArtifacts": dict(CANONICAL_RUNTIME_ARTIFACTS),
        "rowCounts": {"object_current": 3, "edge_current": 2},
        "formulas": {
            "nodeIdentity": "(source_system,type_id,id) unique; ids 101,202,303",
            "edgeIdentity": "(source_system,rel_type_id,id) unique; ids 9001,9002",
            "endpoints": "both edges: (1,101)->(1,202); node 303 is isolated",
            "multiplicity": "two independently identified parallel edge occurrences",
            "lookup_hash": "Spark SQL xxhash64(CAST(source_system AS STRING), CAST(type_id_or_rel_type_id AS BIGINT), CAST(id AS BIGINT)); seed 42",
            "nulls": {"object_current.root_id": None, "edge_current.order_key": None},
            "payload": "fixed UTF-8 JSON strings; source positions 1..count; published_at " + CANONICAL_FIXTURE_PUBLISHED_AT,
            "signed64": "all integer identity and position values are within signed BIGINT",
            "proofs": {
                "signed64": "each integer literal is explicitly represented as a generated BIGINT carrier value",
                "nodeIdentityUnique": "three distinct node ids 101,202,303 under fixed source/type",
                "edgeIdentityUnique": "two distinct edge ids 9001,9002 under fixed source/relation type",
                "endpointsInNodeDomain": "both endpoint tuples are explicitly present in the three-row node source",
                "parallelEdgeMultiplicity": "both source rows share endpoints but have independent edge ids and delivery ids",
                "nullAndPayloadRules": "only root_id and order_key are intentionally null; every other generated field is present per row",
            },
        },
        "selectedSeed": list(CANONICAL_FIXTURE_SEED),
        "action": {
            "cap": 2,
            "outputSchema": ["src_source", "src_type", "src_id", "edge_source",
                             "edge_type", "edge_id", "dst_source", "dst_type", "dst_id"],
            "selectedSeedDegree": 2,
        },
        "tables": {
            "object_current": [dict(row) for row in CANONICAL_FIXTURE_NODE_ROWS],
            "edge_current": [dict(row) for row in CANONICAL_FIXTURE_EDGE_ROWS],
        },
    })


def _acquire_canonical_fixture_lock(root):
    """Acquire the same stable cooperative lock used by action custody."""
    canonical_root = Path(root).resolve(strict=True)
    info = canonical_root.stat()
    if (not canonical_root.is_dir() or info.st_mode & 0o077
            or (hasattr(os, "getuid") and info.st_uid != os.getuid())):
        raise PermissionError("Private canonical fixture root with mode 0700 or tighter required")
    lock_path = canonical_root / ".ashlar-graphframes-action.lock"
    flags = os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(lock_path, flags, 0o600)
    try:
        held = os.fstat(fd)
        current = os.stat(lock_path, follow_symlinks=False)
        if (not stat.S_ISREG(held.st_mode) or held.st_nlink != 1
                or held.st_mode & 0o077 or (held.st_dev, held.st_ino) !=
                (current.st_dev, current.st_ino)):
            raise PermissionError("Stable private canonical fixture lock inode required")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise PermissionError("Canonical synthetic fixture is already in use") from error
        return fd
    except BaseException:
        os.close(fd)
        raise


def _canonical_fixture_generator_sha256():
    return hashlib.sha256(Path(__file__).resolve(strict=True).read_bytes()).hexdigest()


def _canonical_fixture_source_sha256():
    return hashlib.sha256(canonical_fixture_source_bytes() + b"\n").hexdigest()


def _fsync_directory(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _canonical_delta_uuid(table_path):
    log_path = Path(table_path) / "_delta_log" / "00000000000000000000.json"
    if log_path.is_symlink() or not log_path.is_file():
        raise PermissionError("Owned canonical Delta version-zero log required")
    metadata = []
    with log_path.open("r", encoding="utf-8") as stream:
        for line in stream:
            if len(line.encode("utf-8")) > 1_048_576:
                raise PermissionError("Canonical Delta metadata line exceeds bound")
            value = _json_without_duplicate_members(line.encode("utf-8"))
            if type(value) is dict and type(value.get("metaData")) is dict:
                metadata.append(value["metaData"])
    if len(metadata) != 1:
        raise PermissionError("One canonical Delta table UUID action required")
    identity = metadata[0].get("id")
    if (type(identity) is not str or re.fullmatch(
            r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
            identity) is None):
        raise PermissionError("Canonical Delta table UUID required")
    return identity


def _owned_fixture_marker_bytes(root, state, tables=None):
    root_stat = root.stat()
    return _canonical({
        "profile": "ashlar-canonical-graphframes-owned-fixture/0.1",
        "state": state,
        "root": {"path": str(root), "device": root_stat.st_dev,
                 "inode": root_stat.st_ino},
        "generator": {"revision": CANONICAL_FIXTURE_GENERATOR_REVISION,
                      "sha256": _canonical_fixture_generator_sha256()},
        "sourceSha256": _canonical_fixture_source_sha256(),
        "carrierDefinitionSha256": _canonical_carrier_definition_sha256(),
        "tables": tables or {},
    })


def write_canonical_graphframes_fixture(spark, fixture_root):
    """Write the three-node/two-edge fixture under action's cooperative lock.

    This is an explicit mutating preparation helper. It refuses an occupied
    root and never starts Spark itself. Hash columns are evaluated by Spark's
    native xxhash64 expression after the exact generated carrier schema is
    materialized.
    """
    root = _canonical_local_path(fixture_root)
    lock_fd = _acquire_canonical_fixture_lock(root)
    marker_path = root / ".ashlar-graphframes-action-owner.json"
    marker_fd = None
    try:
        paths = {role: root / role for role in ("object_current", "edge_current")}
        allowed = {".ashlar-graphframes-action.lock"}
        if ({entry.name for entry in root.iterdir()} != allowed
                or marker_path.exists() or marker_path.is_symlink()
                or any(path.exists() or path.is_symlink() for path in paths.values())):
            raise FileExistsError("A wholly fresh private canonical fixture root is required")
        marker_fd = os.open(marker_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL
                            | getattr(os, "O_NOFOLLOW", 0), 0o600)
        os.write(marker_fd, _owned_fixture_marker_bytes(root, "writing"))
        os.fsync(marker_fd)
        _fsync_directory(root)
        from datetime import datetime
        from pyspark.sql import functions as F
        from pyspark.sql.types import (LongType, StringType, StructField,
                                       StructType, TimestampType)
        kinds = {"bigint": LongType, "string": StringType, "timestamp": TimestampType}
        table_rows = {"object_current": CANONICAL_FIXTURE_NODE_ROWS,
                      "edge_current": CANONICAL_FIXTURE_EDGE_ROWS}
        for role, rows in table_rows.items():
            signature = _canonical_carrier_signature(role)
            schema = StructType([StructField(name, kinds[kind](), nullable)
                                 for name, kind, nullable in signature])
            values = []
            for item in rows:
                record = dict(item)
                record["lookup_hash"] = 0
                record["published_at"] = datetime.fromisoformat(
                    record["published_at"].replace("Z", "+00:00"))
                values.append(tuple(record[field.name] for field in schema.fields))
            identity_type = "type_id" if role == "object_current" else "rel_type_id"
            frame = spark.createDataFrame(values, schema=schema).withColumn(
                "lookup_hash", F.xxhash64(
                    F.col("source_system").cast("string"),
                    F.col(identity_type).cast("long"), F.col("id").cast("long")))
            frame.write.format("delta").mode("errorifexists").save(str(paths[role]))
        table_ownership = {}
        for role, path in paths.items():
            if path.is_symlink() or not path.is_dir():
                raise PermissionError("Created canonical Delta path must be a real directory")
            info = path.stat()
            table_ownership[role] = {
                "path": str(path), "device": info.st_dev, "inode": info.st_ino,
                "deltaUuid": _canonical_delta_uuid(path),
            }
        complete = _owned_fixture_marker_bytes(root, "complete", table_ownership)
        os.lseek(marker_fd, 0, os.SEEK_SET)
        os.ftruncate(marker_fd, 0)
        os.write(marker_fd, complete)
        os.fsync(marker_fd)
        _fsync_directory(root)
        return {role: str(path) for role, path in paths.items()}
    finally:
        if marker_fd is not None:
            os.close(marker_fd)
        os.close(lock_fd)


def cleanup_canonical_graphframes_fixture(fixture_root):
    """Remove only the two owned tables while holding action's stable lock."""
    import shutil
    root = _canonical_local_path(fixture_root)
    lock_fd = _acquire_canonical_fixture_lock(root)
    try:
        marker_path = root / ".ashlar-graphframes-action-owner.json"
        if marker_path.is_symlink() or not marker_path.is_file():
            raise PermissionError("Complete owned canonical fixture receipt required")
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        marker_fd = os.open(marker_path, flags)
        try:
            before = os.fstat(marker_fd)
            if (not stat.S_ISREG(before.st_mode) or before.st_mode & 0o077
                    or (hasattr(os, "getuid") and before.st_uid != os.getuid())
                    or before.st_size > 64 * 1024):
                raise PermissionError("Private bounded owned fixture receipt required")
            with os.fdopen(marker_fd, "rb", closefd=False) as stream:
                marker_raw = stream.read(64 * 1024 + 1)
            after = os.fstat(marker_fd)
            current = os.stat(marker_path, follow_symlinks=False)
            identity_before = (before.st_dev, before.st_ino, before.st_size,
                               before.st_mtime_ns)
            identity_after = (after.st_dev, after.st_ino, after.st_size,
                              after.st_mtime_ns)
            identity_path = (current.st_dev, current.st_ino, current.st_size,
                             current.st_mtime_ns)
            if (len(marker_raw) != before.st_size or identity_before != identity_after
                    or identity_after != identity_path):
                raise PermissionError("Owned fixture receipt changed while reading")
        finally:
            os.close(marker_fd)
        marker = _json_without_duplicate_members(marker_raw)
        root_info = root.stat()
        expected_header = {
            "profile": "ashlar-canonical-graphframes-owned-fixture/0.1",
            "state": "complete",
            "root": {"path": str(root), "device": root_info.st_dev,
                     "inode": root_info.st_ino},
            "generator": {"revision": CANONICAL_FIXTURE_GENERATOR_REVISION,
                          "sha256": _canonical_fixture_generator_sha256()},
            "sourceSha256": _canonical_fixture_source_sha256(),
            "carrierDefinitionSha256": _canonical_carrier_definition_sha256(),
        }
        if (type(marker) is not dict or any(marker.get(key) != value
                                             for key, value in expected_header.items())):
            raise PermissionError("Owned fixture receipt does not match current root and bytes")
        tables = marker.get("tables")
        if type(tables) is not dict or set(tables) != {"object_current", "edge_current"}:
            raise PermissionError("Complete owned fixture table inventory required")
        verified_paths = []
        for role in ("object_current", "edge_current"):
            path = root / role
            record = tables[role]
            if (type(record) is not dict or set(record) !=
                    {"path", "device", "inode", "deltaUuid"}
                    or path.is_symlink() or not path.is_dir()
                    or path.resolve(strict=True) != path):
                raise PermissionError("Exact owned canonical table path required")
            info = path.stat()
            if ((record["path"] != str(path)
                    or (record["device"], record["inode"]) != (info.st_dev, info.st_ino)
                    or record["deltaUuid"] != _canonical_delta_uuid(path))):
                raise PermissionError("Owned canonical table path, inode, or UUID changed")
            verified_paths.append(path)
        for path in verified_paths:
            shutil.rmtree(path)
        marker_path.unlink()
    finally:
        os.close(lock_fd)


@dataclass(frozen=True)
class CanonicalSyntheticFixtureDescriptor:
    """Frozen synthetic generator/oracle declarations; not an audited oracle."""
    generator_revision: str
    generator_sha256: str
    source_sha256: str
    row_counts: tuple
    formula: str
    selected_seed: tuple
    oracle_rows: tuple
    carrier_definition_sha256: str = field(init=False)
    _sha256: str = field(init=False, repr=False)

    def __post_init__(self):
        if (type(self.generator_revision) is not str or not self.generator_revision
                or len(self.generator_revision) > 128
                or any(type(value) is not str or re.fullmatch(r"[0-9a-f]{64}", value) is None
                       for value in (self.generator_sha256, self.source_sha256))
                or type(self.row_counts) is not tuple or len(self.row_counts) != 2
                or any(type(value) is not int or value <= 0 for value in self.row_counts)
                or type(self.formula) is not str or not self.formula or len(self.formula) > 4096
                or type(self.selected_seed) is not tuple or len(self.selected_seed) != 3
                or type(self.selected_seed[0]) is not str
                or any(type(value) is not int or not -(1 << 63) <= value < (1 << 63)
                       for value in self.selected_seed[1:])
                or type(self.oracle_rows) is not tuple or len(self.oracle_rows) > MAX_ONE_HOP_ROWS
                or any(type(row) is not tuple or len(row) != 9
                       or any(type(row[index]) is not str or not row[index]
                              for index in (0, 3, 6))
                       or any(type(row[index]) is not int
                              or not -(1 << 63) <= row[index] < (1 << 63)
                              for index in (1, 2, 4, 5, 7, 8))
                       or tuple(row[:3]) != self.selected_seed
                       for row in self.oracle_rows)):
            raise ValueError("Complete frozen canonical synthetic fixture descriptor required")
        carrier_sha = _canonical_carrier_definition_sha256()
        object.__setattr__(self, "carrier_definition_sha256", carrier_sha)
        object.__setattr__(self, "_sha256", hashlib.sha256(_canonical({
            "profile": CANONICAL_SYNTHETIC_PROFILE,
            "carrierProfile": CANONICAL_CARRIER_PROFILE,
            "carrierDefinitionSha256": carrier_sha,
            "umfRevision": CANONICAL_UMF_REVISION,
            "runtimeSelection": dict(CANONICAL_RUNTIME_SELECTION),
            "generatorRevision": self.generator_revision,
            "generatorSha256": self.generator_sha256,
            "sourceSha256": self.source_sha256,
            "rowCounts": list(self.row_counts),
            "formula": self.formula,
            "selectedSeed": list(self.selected_seed),
            "oracleRows": [list(row) for row in self.oracle_rows],
        })).hexdigest())

    @property
    def sha256(self):
        return self._sha256


def _json_without_duplicate_members(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("Duplicate JSON member in reviewed action descriptor")
            result[key] = value
        return result

    try:
        return json.loads(raw.decode("utf-8"), object_pairs_hook=pairs)
    except (UnicodeError, json.JSONDecodeError) as error:
        raise ValueError("UTF-8 JSON action descriptor required") from error


@dataclass(frozen=True)
class ReviewedCanonicalActionDescriptor:
    """Byte-verified inputs for one reviewed canonical synthetic action.

    The old declaration-only descriptor intentionally cannot enter the action
    gate. This factory requires the reviewed manifest and every referenced byte
    artifact, and computes their digests itself. Review status remains a human
    evidence assertion; the factory does not manufacture review authority.
    """
    manifest_bytes: bytes
    generator_bytes: bytes
    source_bytes: bytes
    oracle_bytes: bytes
    manifest_sha256: str
    review_anchor_path: str
    review_anchor_sha256: str
    generator_sha256: str
    source_sha256: str
    oracle_sha256: str
    fixture: CanonicalSyntheticFixtureDescriptor
    cap: int
    output_schema: tuple
    _sha256: str

    @classmethod
    def from_reviewed_bytes(cls, manifest_bytes, generator_bytes, source_bytes,
                            oracle_bytes, *, review_anchor_path,
                            selected_review_anchor_sha256):
        artifacts = (manifest_bytes, generator_bytes, source_bytes, oracle_bytes)
        if any(type(item) is not bytes or not item or len(item) > MAX_SNAPSHOT_METADATA_BYTES
               for item in artifacts):
            raise ValueError("Bounded nonempty reviewed action evidence bytes required")
        manifest = _json_without_duplicate_members(manifest_bytes)
        required = {"profile", "reviewStatus", "reviewId", "generatorRevision",
                    "generatorSha256", "sourceSha256", "carrierDefinitionSha256",
                    "umfRevision", "runtime", "rowCounts", "formula",
                    "formulaProofs", "selectedSeed", "cap", "outputSchema",
                    "oracleSha256"}
        if type(manifest) is not dict or set(manifest) != required:
            raise ValueError("Closed reviewed canonical action manifest required")
        if (manifest["profile"] != CANONICAL_SYNTHETIC_PROFILE
                or manifest["reviewStatus"] != "independently-reviewed"
                or type(manifest["reviewId"]) is not str or not manifest["reviewId"]
                or manifest["umfRevision"] != CANONICAL_UMF_REVISION
                or manifest["runtime"] != dict(CANONICAL_RUNTIME_SELECTION)):
            raise ValueError("Reviewed canonical profile, reviewer record, UMF and runtime required")
        generator_sha = hashlib.sha256(generator_bytes).hexdigest()
        source_sha = hashlib.sha256(source_bytes).hexdigest()
        oracle_sha = hashlib.sha256(oracle_bytes).hexdigest()
        carrier_sha = _canonical_carrier_definition_sha256()
        if (manifest["generatorSha256"] != generator_sha
                or manifest["sourceSha256"] != source_sha
                or manifest["oracleSha256"] != oracle_sha
                or manifest["carrierDefinitionSha256"] != carrier_sha):
            raise ValueError("Reviewed canonical action input bytes or carrier definition changed")
        counts = manifest["rowCounts"]
        seed = manifest["selectedSeed"]
        cap = manifest["cap"]
        formula = manifest["formula"]
        schema = manifest["outputSchema"]
        proofs = manifest["formulaProofs"]
        proof_keys = {"signed64", "nodeIdentityUnique", "edgeIdentityUnique",
                      "endpointsInNodeDomain", "parallelEdgeMultiplicity", "nullAndPayloadRules"}
        if (type(counts) is not list or len(counts) != 2
                or any(type(value) is not int or value <= 0 for value in counts)
                or type(seed) is not list or len(seed) != 3
                or type(seed[0]) is not str or not seed[0]
                or any(type(value) is not int or not -(1 << 63) <= value < (1 << 63)
                       for value in seed[1:])
                or type(cap) is not int or not 0 < cap <= MAX_ONE_HOP_ROWS
                or type(formula) is not dict or not formula
                or type(proofs) is not dict or set(proofs) != proof_keys
                or any(type(value) is not str or not value for value in proofs.values())):
            raise ValueError("Complete frozen formula, proof declarations, seed and cap required")
        expected_schema = ["src_source", "src_type", "src_id", "edge_source",
                           "edge_type", "edge_id", "dst_source", "dst_type", "dst_id"]
        if schema != expected_schema:
            raise ValueError("Exact edge-distinct nine-field output schema required")
        oracle = _json_without_duplicate_members(oracle_bytes)
        if (type(oracle) is not dict or set(oracle) != {"profile", "seed", "rows"}
                or oracle["profile"] != "ashlar-canonical-one-hop-independent-oracle/0.1"
                or oracle["seed"] != seed or type(oracle["rows"]) is not list
                or len(oracle["rows"]) > cap):
            raise ValueError("Independent frozen seed occurrence bag within cap required")
        rows = tuple(tuple(row) if type(row) is list else row for row in oracle["rows"])
        if any(type(row) is not tuple or len(row) != 9 or tuple(row[:3]) != tuple(seed)
               for row in rows):
            raise ValueError("Oracle must contain only complete selected-seed edge occurrences")
        fixture = CanonicalSyntheticFixtureDescriptor(
            manifest["generatorRevision"], generator_sha, source_sha,
            tuple(counts), json.dumps(formula, ensure_ascii=False, sort_keys=True,
                                      separators=(",", ":")),
            tuple(seed), rows)
        if fixture.carrier_definition_sha256 != manifest["carrierDefinitionSha256"]:
            raise ValueError("Generated `.4` carrier bytes changed after descriptor freeze")
        manifest_sha = hashlib.sha256(manifest_bytes).hexdigest()
        if (type(selected_review_anchor_sha256) is not str
                or re.fullmatch(r"[0-9a-f]{64}", selected_review_anchor_sha256) is None):
            raise ValueError("Separately selected external review-anchor SHA-256 required")
        raw_anchor_path = Path(review_anchor_path)
        if raw_anchor_path.is_symlink():
            raise PermissionError("Review anchor must not be selected through a symlink")
        anchor_path = raw_anchor_path.resolve(strict=True)
        anchor_digest = _verify_canonical_review_anchor(
            anchor_path, selected_review_anchor_sha256, manifest_sha,
            generator_sha, source_sha, oracle_sha, carrier_sha,
            manifest["reviewId"])
        digest = hashlib.sha256(_canonical({
            "manifestSha256": manifest_sha, "generatorSha256": generator_sha,
            "sourceSha256": source_sha, "oracleSha256": oracle_sha,
            "carrierDefinitionSha256": carrier_sha,
            "reviewAnchorSha256": anchor_digest,
        })).hexdigest()
        return cls(manifest_bytes, generator_bytes, source_bytes, oracle_bytes,
                   manifest_sha, str(anchor_path), anchor_digest, generator_sha,
                   source_sha, oracle_sha, fixture, cap, tuple(schema), digest)

    def verify_review_anchor(self):
        return _verify_canonical_review_anchor(
            Path(self.review_anchor_path), self.review_anchor_sha256,
            self.manifest_sha256, self.generator_sha256, self.source_sha256,
            self.oracle_sha256, self.fixture.carrier_definition_sha256,
            json.loads(self.manifest_bytes.decode("utf-8"))["reviewId"])

    @property
    def sha256(self):
        return self._sha256


def _verify_canonical_review_anchor(path, selected_sha256, manifest_sha256,
                                    generator_sha256, source_sha256, oracle_sha256,
                                    carrier_sha256, review_id):
    path = Path(path)
    if path.is_symlink():
        raise PermissionError("Review anchor must be a canonical regular file")
    resolved = path.resolve(strict=True)
    if resolved != path or not resolved.is_file():
        raise PermissionError("Bounded canonical external review-anchor file required")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(resolved, flags)
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or before.st_size > 64 * 1024:
            raise PermissionError("Bounded canonical external review-anchor file required")
        with os.fdopen(fd, "rb", closefd=False) as stream:
            raw = stream.read(64 * 1024 + 1)
        after = os.fstat(fd)
        path_stat = os.stat(resolved, follow_symlinks=False)
        if (len(raw) != before.st_size
                or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
                != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
                or (after.st_dev, after.st_ino) != (path_stat.st_dev, path_stat.st_ino)):
            raise PermissionError("External review-anchor file changed while read")
    finally:
        os.close(fd)
    digest = hashlib.sha256(raw).hexdigest()
    if digest != selected_sha256:
        raise PermissionError("External review-anchor digest changed")
    anchor = _json_without_duplicate_members(raw)
    expected = {
        "profile": "ashlar-canonical-action-review-anchor/0.1",
        "reviewStatus": "independently-reviewed", "reviewId": review_id,
        "manifestSha256": manifest_sha256, "generatorSha256": generator_sha256,
        "sourceSha256": source_sha256, "oracleSha256": oracle_sha256,
        "carrierDefinitionSha256": carrier_sha256,
    }
    if type(anchor) is not dict or anchor != expected:
        raise PermissionError("External review anchor does not bind the selected action artifacts")
    return digest


@dataclass(frozen=True)
class CanonicalSyntheticTablePin:
    role: str
    table_uuid: str
    version: int
    schema_sha256: str
    protocol: tuple
    active_file_count: int

    def __post_init__(self):
        if (self.role not in ("object_current", "edge_current")
                or type(self.table_uuid) is not str
                or re.fullmatch(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", self.table_uuid) is None
                or type(self.version) is not int or self.version < 0
                or type(self.active_file_count) is not int
                or not 0 <= self.active_file_count <= MAX_SNAPSHOT_FILES
                or type(self.schema_sha256) is not str
                or re.fullmatch(r"[0-9a-f]{64}", self.schema_sha256) is None
                or self.protocol != (PROTOCOL["minReaderVersion"],
                                     PROTOCOL["minWriterVersion"],
                                     tuple(PROTOCOL["tableFeatures"]))):
            raise ValueError("Exact canonical synthetic current-table identity required")


def _canonical_carrier_signature(role):
    if role not in ("object_current", "edge_current"):
        raise ValueError("Canonical current carrier role required")
    resource = (importlib.resources.files("ashlar_host")
                .joinpath("resources/managed-current-core-010-xxhash64/runtime-carriers.generated.json"))
    artifact = json.loads(resource.read_text(encoding="utf-8"))
    if (artifact.get("physicalProfile") != CANONICAL_CARRIER_PROFILE
            or artifact.get("generator", {}).get("revision") != CANONICAL_UMF_REVISION):
        raise ValueError("Generated .4 carrier profile differs")
    spark_types = {"long": "bigint", "integer": "int", "boolean": "boolean",
                   "string": "string", "timestamp": "timestamp"}
    return tuple((item["name"], spark_types.get(item["deltaType"], item["deltaType"]),
                  item["nullable"])
                 for item in artifact["carriers"][role]["columns"])


def _canonical_carrier_definition_sha256():
    resource = (importlib.resources.files("ashlar_host")
                .joinpath("resources/managed-current-core-010-xxhash64/runtime-carriers.generated.json"))
    return hashlib.sha256(resource.read_bytes()).hexdigest()


class PinnedCanonicalSyntheticDeltaFixture:
    """Read-only two-table `.4` synthetic holder; no publication/ACK authority."""

    def __init__(self, spark, fixture, table_paths, *, maximum_lease_seconds=120):
        if type(fixture) is not CanonicalSyntheticFixtureDescriptor:
            raise ValueError("Frozen canonical synthetic fixture descriptor required")
        if (type(table_paths) is not dict
                or set(table_paths) != {"object_current", "edge_current"}
                or len(set(map(str, table_paths.values()))) != 2):
            raise ValueError("Exactly two distinct canonical current-table paths required")
        if (type(maximum_lease_seconds) is not int
                or not 0 < maximum_lease_seconds <= MAX_LEASE_SECONDS):
            raise ValueError("Finite canonical fixture lease required")
        if getattr(spark, "version", None) != dict(CANONICAL_RUNTIME_SELECTION)["spark"]:
            raise PermissionError("Selected Spark 4.2.0 runtime required")
        self._spark = spark
        self.fixture = fixture
        if _canonical_carrier_definition_sha256() != fixture.carrier_definition_sha256:
            raise ValueError("Canonical generated carrier definition changed after freeze")
        paths = {}
        for role in ("object_current", "edge_current"):
            text = str(table_paths[role])
            _quote_path(text)
            paths[role] = text
        self._paths = paths
        self._maximum_lease_seconds = maximum_lease_seconds

    def _observe_table(self, role):
        path = self._paths[role]
        name = _quote_path(path)
        details = self._spark.sql("DESCRIBE DETAIL " + name).limit(2).collect()
        if len(details) != 1:
            raise ValueError("One actual canonical Delta table detail row required")
        detail = _row_dict(details[0])
        if detail.get("format") != "delta":
            raise ValueError("Actual canonical Delta current table required")
        history = self._spark.sql("DESCRIBE HISTORY " + name + " LIMIT 1").limit(2).collect()
        if len(history) != 1 or type(_row_dict(history[0]).get("version")) is not int:
            raise ValueError("One actual canonical Delta table head required")
        version = _row_dict(history[0])["version"]
        protocol = {"minReaderVersion": detail.get("minReaderVersion"),
                    "minWriterVersion": detail.get("minWriterVersion"),
                    "tableFeatures": sorted(detail.get("tableFeatures") or [])}
        protocol_signature = (protocol["minReaderVersion"], protocol["minWriterVersion"],
                              tuple(protocol["tableFeatures"]))
        active_file_count = detail.get("numFiles")
        if (type(active_file_count) is not int
                or not 0 <= active_file_count <= MAX_SNAPSHOT_FILES):
            raise ValueError("Finite native Delta numFiles witness within inventory bound required")
        if protocol_signature != (PROTOCOL["minReaderVersion"],
                                  PROTOCOL["minWriterVersion"],
                                  tuple(PROTOCOL["tableFeatures"])):
            raise ValueError("Canonical synthetic Delta protocol differs from selected runtime")
        if _canonical_carrier_definition_sha256() != self.fixture.carrier_definition_sha256:
            raise ValueError("Frozen generated carrier definition changed")
        schema = self._spark.read.format("delta").load(path).schema
        signature, schema_sha = _actual_schema_signature(schema)
        if signature != _canonical_carrier_signature(role):
            raise ValueError("Exact generated `.4` carrier schema required")
        return CanonicalSyntheticTablePin(role, detail.get("id"), version,
                                          schema_sha, protocol_signature,
                                          active_file_count)

    def open(self):
        observed = {role: self._observe_table(role)
                    for role in ("object_current", "edge_current")}
        if len({pin.table_uuid for pin in observed.values()}) != 2:
            raise ValueError("Distinct native Delta UUIDs required for current roles")
        repeated = {role: self._observe_table(role)
                    for role in ("object_current", "edge_current")}
        if repeated != observed:
            raise PermissionError("Canonical synthetic Delta tables changed while pinning")
        return CanonicalSyntheticDeltaLease(
            self, observed, time.monotonic() + self._maximum_lease_seconds)


class CanonicalSyntheticDeltaLease:
    def __init__(self, owner, pins, deadline):
        self._owner = owner
        self._pins = MappingProxyType(dict(pins))
        self._deadline = deadline
        self._closed = False

    def renew(self):
        if self._closed or time.monotonic() >= self._deadline:
            raise PermissionError("Canonical synthetic fixture lease expired")
        current = {role: self._owner._observe_table(role)
                   for role in ("object_current", "edge_current")}
        if current != self._pins:
            raise PermissionError("Canonical synthetic table UUID/head/schema/protocol changed")

    def current_frames(self):
        """Return versionAsOf handles, not qualified snapshot execution authority.

        This candidate does not prove retained data-file availability or hold
        exclusive use. Callers must not treat lazy frames as qualified snapshots.
        """
        self.renew()
        result = {}
        for role in ("object_current", "edge_current"):
            pin = self._pins[role]
            frame = (self._owner._spark.read.format("delta")
                     .option("versionAsOf", pin.version).load(self._owner._paths[role]))
            signature, digest = _actual_schema_signature(frame.schema)
            if (_canonical_carrier_definition_sha256()
                    != self._owner.fixture.carrier_definition_sha256
                    or signature != _canonical_carrier_signature(role)
                    or digest != pin.schema_sha256):
                raise PermissionError("Pinned canonical `.4` Delta schema changed")
            result[role] = frame
        return result

    def evidence(self):
        self.renew()
        return {"profile": CANONICAL_SYNTHETIC_PROFILE,
                "fixtureSha256": self._owner.fixture.sha256,
                "carrierProfile": CANONICAL_CARRIER_PROFILE,
                "runtimeSelection": dict(CANONICAL_RUNTIME_SELECTION),
                "observedRuntime": {"spark": self._owner._spark.version},
                "descriptorAuthority": "declared-and-digested-only; generator/source/oracle not independently audited",
                "retainedFileCustody": "not-established",
                "publication": "absent", "sourceProgress": "absent",
                "protectedAck": "absent", "unityCatalog": "absent",
                "tables": {role: {"uuid": pin.table_uuid, "version": pin.version,
                    "schemaSha256": pin.schema_sha256,
                    "protocol": {"minReaderVersion": pin.protocol[0],
                                 "minWriterVersion": pin.protocol[1],
                                 "tableFeatures": list(pin.protocol[2])},
                    "activeFileCount": pin.active_file_count}
                    for role, pin in self._pins.items()}}

    def close(self):
        if self._closed:
            return None
        self.renew()
        self._closed = True
        return None


@contextmanager
def hold_canonical_synthetic_delta_fixture(owner):
    if type(owner) is not PinnedCanonicalSyntheticDeltaFixture:
        raise ValueError("Canonical synthetic Delta fixture owner required")
    lease = owner.open()
    primary = None
    try:
        lease.renew()
        yield lease
    except BaseException as error:
        primary = error
        raise
    finally:
        try:
            lease.close()
        except BaseException:
            if primary is None:
                raise


def _canonical_local_path(path):
    text = os.fspath(path)
    if type(text) is not str or not text or "\x00" in text:
        raise ValueError("Canonical local fixture path required")
    resolved = Path(text).resolve(strict=True)
    if not resolved.is_absolute():
        raise ValueError("Absolute canonical fixture path required")
    return resolved


def _snapshot_file_inventory(frame, fixture_root, table_path, expected_file_count):
    if (type(expected_file_count) is not int
            or not 0 <= expected_file_count <= MAX_SNAPSHOT_FILES):
        raise PermissionError("Pin-bound native Delta file-count witness required")
    # Delta DESCRIBE DETAIL supplies this exact pin-bound numFiles scalar before
    # Spark's inputFiles API can materialize its array. The admitted count bounds
    # the subsequent array to MAX_SNAPSHOT_FILES; disagreement refuses.
    files = frame.inputFiles()
    if not hasattr(files, "__iter__"):
        raise PermissionError("Native bounded Delta active-file inventory required")
    try:
        file_count = len(files)
    except TypeError:
        file_count = None
    if file_count is not None and file_count > MAX_SNAPSHOT_FILES:
        raise PermissionError("Delta active-file inventory exceeds configured bound")
    if file_count is not None and file_count != expected_file_count:
        raise PermissionError("Active-file inventory differs from pin-bound Delta numFiles")
    if file_count is None:
        raise PermissionError("Exact bounded active-file array length required")
    records = []
    metadata_bytes = 0
    root = Path(fixture_root)
    for index, raw_path in enumerate(islice(iter(files), MAX_SNAPSHOT_FILES + 1)):
        if index >= MAX_SNAPSHOT_FILES:
            raise PermissionError("Delta active-file inventory exceeds configured bound")
        if type(raw_path) is not str or not raw_path.startswith("file:"):
            raise PermissionError("Canonical local retained Delta files are required")
        from urllib.parse import unquote, urlparse
        uri = urlparse(raw_path)
        if uri.scheme != "file" or uri.netloc not in ("", "localhost"):
            raise PermissionError("Unsupported retained-file URI")
        raw_candidate = Path(unquote(uri.path))
        if raw_candidate.is_symlink():
            raise PermissionError("Symlinked active Delta data files are unsupported")
        try:
            candidate = raw_candidate.resolve(strict=True)
        except OSError as error:
            raise PermissionError("Pinned active Delta data file is unavailable") from error
        if len(str(candidate).encode("utf-8")) > 4096:
            raise PermissionError("Retained Delta path exceeds configured bound")
        try:
            candidate.relative_to(root)
        except ValueError as error:
            raise PermissionError("Active Delta file is outside private fixture root") from error
        info = candidate.stat()
        if not stat.S_ISREG(info.st_mode):
            raise PermissionError("Active Delta data file must be a regular file")
        # On local filesystems the device/inode pair is the stronger immutable
        # object identity requested by the packet; size and nanosecond mtime
        # additionally detect ordinary in-place replacement and mutation.
        record = {"kind": "activeData", "path": str(candidate), "size": info.st_size,
                  "mtimeNs": info.st_mtime_ns, "device": info.st_dev,
                  "inode": info.st_ino}
        metadata_bytes += len(_canonical(record)) + 1
        if metadata_bytes > MAX_SNAPSHOT_METADATA_BYTES:
            raise PermissionError("Serialized active-file inventory exceeds configured bound")
        records.append(record)

    # The active data-file inventory alone does not retain the version metadata
    # needed to resolve the pin. Conservatively inventory the full bounded
    # Delta log directory as well, catching vacuum/log cleanup and new commits.
    log_root = Path(table_path) / "_delta_log"
    if log_root.is_symlink() or not log_root.is_dir():
        raise PermissionError("Pinned Delta log directory is unavailable")
    try:
        log_root.resolve(strict=True).relative_to(root)
    except ValueError as error:
        raise PermissionError("Delta log directory is outside private fixture root") from error
    enumerated = 0
    for directory, subdirs, names in os.walk(log_root, followlinks=False):
        if any(Path(directory, name).is_symlink() for name in subdirs):
            raise PermissionError("Symlinked Delta log directories are unsupported")
        for name in names:
            enumerated += 1
            if enumerated > MAX_SNAPSHOT_FILES:
                raise PermissionError("Delta log inventory exceeds configured bound")
            raw_candidate = Path(directory, name)
            if raw_candidate.is_symlink():
                raise PermissionError("Symlinked Delta log files are unsupported")
            try:
                candidate = raw_candidate.resolve(strict=True)
            except OSError as error:
                raise PermissionError("Pinned Delta log file is unavailable") from error
            try:
                candidate.relative_to(root)
            except ValueError as error:
                raise PermissionError("Delta log file is outside private fixture root") from error
            info = candidate.stat()
            if not stat.S_ISREG(info.st_mode):
                raise PermissionError("Delta log inventory entries must be regular files")
            record = {"kind": "deltaLog", "path": str(candidate), "size": info.st_size,
                      "mtimeNs": info.st_mtime_ns, "device": info.st_dev,
                      "inode": info.st_ino}
            metadata_bytes += len(_canonical(record)) + 1
            if metadata_bytes > MAX_SNAPSHOT_METADATA_BYTES:
                raise PermissionError("Serialized retained-file inventory exceeds configured bound")
            records.append(record)
    records.sort(key=lambda item: (item["kind"], item["path"]))
    encoded = _canonical(records)
    if len(encoded) > MAX_SNAPSHOT_METADATA_BYTES:
        raise PermissionError("Serialized active-file inventory exceeds configured bound")
    return tuple(tuple(sorted(item.items())) for item in records), hashlib.sha256(encoded).hexdigest()


class PinnedCanonicalGraphFramesActionFixture:
    """Exclusive retained-file and native-runtime owner for one synthetic action."""
    def __init__(self, spark, descriptor, fixture_root, table_paths, *,
                 maximum_lease_seconds=120):
        if type(descriptor) is not ReviewedCanonicalActionDescriptor:
            raise ValueError("Independently reviewed canonical action descriptor required")
        root = _canonical_local_path(fixture_root)
        root_info = root.stat()
        if (not root.is_dir() or root_info.st_mode & 0o077
                or (hasattr(os, "getuid") and root_info.st_uid != os.getuid())):
            raise PermissionError("Private canonical fixture root with mode 0700 or tighter required")
        if type(table_paths) is not dict or set(table_paths) != {"object_current", "edge_current"}:
            raise ValueError("Exactly two canonical current-table paths required")
        canonical_paths = {}
        for role, path in table_paths.items():
            resolved = _canonical_local_path(path)
            try:
                resolved.relative_to(root)
            except ValueError as error:
                raise PermissionError("Canonical Delta tables must remain under private fixture root") from error
            canonical_paths[role] = str(resolved)
        if len(set(canonical_paths.values())) != 2:
            raise ValueError("Distinct canonical current-table paths required")
        if type(maximum_lease_seconds) is not int or not 0 < maximum_lease_seconds <= MAX_LEASE_SECONDS:
            raise ValueError("Finite canonical action lease required")
        self._spark = spark
        self.descriptor = descriptor
        self.root = root
        self._root_identity = (root_info.st_dev, root_info.st_ino)
        self.table_paths = MappingProxyType(dict(canonical_paths))
        self.lock_path = root / ".ashlar-graphframes-action.lock"
        self.maximum_lease_seconds = maximum_lease_seconds
        self._selected_state_sha256 = self._selected_state_digest()

    def _selected_state_digest(self):
        try:
            payload = {
                "descriptorSha256": self.descriptor.sha256,
                "reviewAnchorPath": str(self.descriptor.review_anchor_path),
                "reviewAnchorSha256": self.descriptor.review_anchor_sha256,
                "root": str(self.root),
                "rootIdentity": list(self._root_identity),
                "tablePaths": dict(self.table_paths),
                "lockPath": str(self.lock_path),
            }
            return hashlib.sha256(_canonical(payload)).hexdigest()
        except (AttributeError, TypeError, ValueError) as error:
            raise PermissionError("Selected canonical action inputs changed") from error

    def _verify_selected_state(self):
        if self._selected_state_digest() != self._selected_state_sha256:
            raise PermissionError("Selected canonical descriptor/root/table/lock identity changed")
        try:
            self.descriptor.verify_review_anchor()
        except (OSError, ValueError, PermissionError) as error:
            raise PermissionError("External canonical review anchor changed") from error

    def _acquire_lock(self):
        self._verify_selected_state()
        if (self.root.resolve(strict=True) != self.root
                or (self.root.stat().st_dev, self.root.stat().st_ino) != self._root_identity):
            raise PermissionError("Canonical private fixture root identity changed")
        flags = os.O_CREAT | os.O_RDWR
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        fd = os.open(self.lock_path, flags, 0o600)
        try:
            before = os.fstat(fd)
            if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1
                    or before.st_mode & 0o077
                    or (hasattr(os, "getuid") and before.st_uid != os.getuid())):
                raise PermissionError("Stable private regular lock inode required")
            if Path(self.lock_path).resolve(strict=True) != self.lock_path:
                raise PermissionError("Canonical stable lock path required")
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise PermissionError("Canonical synthetic fixture is already in use") from error
            after = os.fstat(fd)
            path_after = os.stat(self.lock_path, follow_symlinks=False)
            if ((before.st_dev, before.st_ino) != (after.st_dev, after.st_ino)
                    or (after.st_dev, after.st_ino) != (path_after.st_dev, path_after.st_ino)
                    or (self.root.stat().st_dev, self.root.stat().st_ino) != self._root_identity):
                raise PermissionError("Canonical fixture lock identity changed")
            return fd, {"path": str(self.lock_path), "device": after.st_dev,
                        "inode": after.st_ino,
                        "rootDevice": self._root_identity[0],
                        "rootInode": self._root_identity[1]}
        except BaseException:
            os.close(fd)
            raise

    def open(self):
        lock_fd, lock_identity = self._acquire_lock()
        lease = None
        try:
            self._verify_selected_state()
            base = PinnedCanonicalSyntheticDeltaFixture(
                self._spark, self.descriptor.fixture, dict(self.table_paths),
                maximum_lease_seconds=self.maximum_lease_seconds)
            lease = base.open()
            frames = lease.current_frames()
            inventories = {role: _snapshot_file_inventory(
                frame, self.root, self.table_paths[role],
                lease._pins[role].active_file_count)
                          for role, frame in frames.items()}
            self._verify_selected_state()
            return CanonicalGraphFramesActionLease(
                self, lease, lock_fd, lock_identity, inventories,
                time.monotonic() + self.maximum_lease_seconds)
        except BaseException:
            if lease is not None:
                try:
                    lease.close()
                except BaseException:
                    pass
            os.close(lock_fd)
            raise


class CanonicalGraphFramesActionLease:
    def __init__(self, owner, delta_lease, lock_fd, lock_identity, inventories, deadline):
        self._owner = owner
        self._delta_lease = delta_lease
        self._lock_fd = lock_fd
        self._lock_identity = lock_identity
        self._inventories = MappingProxyType(dict(inventories))
        self._deadline = deadline
        self._closed = False
        self._opened_monotonic_ns = time.monotonic_ns()
        self._closed_monotonic_ns = None
        self._runtime_identity = None

    def record_runtime_identity(self, identity):
        if self._runtime_identity is not None:
            raise PermissionError("Canonical runtime identity already recorded")
        self._runtime_identity = identity

    def _check_lock_identity(self):
        try:
            root_stat = self._owner.root.stat()
            held = os.fstat(self._lock_fd)
            current = os.stat(self._owner.lock_path, follow_symlinks=False)
            canonical_root = self._owner.root.resolve(strict=True)
        except OSError as error:
            raise PermissionError("Canonical fixture root or lock path disappeared") from error
        if (canonical_root != self._owner.root
                or (root_stat.st_dev, root_stat.st_ino) != self._owner._root_identity
                or (held.st_dev, held.st_ino) != (self._lock_identity["device"], self._lock_identity["inode"])
                or (current.st_dev, current.st_ino) != (held.st_dev, held.st_ino)):
            raise PermissionError("Canonical fixture lock inode changed")

    def renew(self):
        if self._closed or time.monotonic() >= self._deadline:
            raise PermissionError("Canonical GraphFrames action lease expired")
        self._owner._verify_selected_state()
        self._check_lock_identity()
        self._delta_lease.renew()
        frames = self._delta_lease.current_frames()
        current = {role: _snapshot_file_inventory(
            frame, self._owner.root, self._owner.table_paths[role],
            self._delta_lease._pins[role].active_file_count)
                   for role, frame in frames.items()}
        if current != dict(self._inventories):
            raise PermissionError("Retained Delta active-file inventory changed")
        self._owner._verify_selected_state()

    def current_frames(self):
        self.renew()
        return self._delta_lease.current_frames()

    def evidence(self):
        self.renew()
        base = self._delta_lease.evidence()
        return {
            "profile": CANONICAL_SYNTHETIC_PROFILE,
            "descriptorSha256": self._owner.descriptor.sha256,
            "lock": dict(self._lock_identity),
            "lockWindow": {"openedMonotonicNs": self._opened_monotonic_ns,
                           "leaseDeadlineMonotonicNs": int(self._deadline * 1_000_000_000)},
            "retainedFileCustody": "cooperative-exclusive-private-root",
            "snapshotInventorySha256": {
                role: value[1] for role, value in self._inventories.items()},
            "fixture": base,
            "sourceProgress": "absent", "protectedAck": "absent",
            "publication": "absent", "unityCatalog": "absent",
            "productionAuthority": "absent",
        }

    def close(self):
        if self._closed:
            return None
        try:
            self.renew()
            if self._runtime_identity is not None:
                if _admit_canonical_graphframes_runtime(self._owner._spark) != self._runtime_identity:
                    raise PermissionError("Loaded runtime or JAR identity changed before release")
            self._delta_lease.close()
            self._check_lock_identity()
            self._owner._verify_selected_state()
        finally:
            self._closed = True
            self._closed_monotonic_ns = time.monotonic_ns()
            os.close(self._lock_fd)


@contextmanager
def hold_canonical_graphframes_action_fixture(owner):
    if type(owner) is not PinnedCanonicalGraphFramesActionFixture:
        raise ValueError("Reviewed canonical GraphFrames action owner required")
    lease = owner.open()
    primary = None
    try:
        lease.renew()
        yield lease
    except BaseException as error:
        primary = error
        raise
    finally:
        try:
            lease.close()
        except BaseException:
            if primary is None:
                raise


def _jvm_class_jar(spark, class_name):
    jvm = spark._jvm
    loader = jvm.java.lang.Thread.currentThread().getContextClassLoader()
    if loader is None:
        loader = jvm.org.apache.spark.util.Utils.getContextOrSparkClassLoader()
    if loader is None:
        raise PermissionError("Spark context classloader is unavailable")
    cls = loader.loadClass(class_name)
    location = (cls.getProtectionDomain().getCodeSource().getLocation()
                .toURI().getPath())
    path = Path(location).resolve(strict=True)
    if not path.is_file() or path.suffix != ".jar":
        raise PermissionError("Loaded runtime class must resolve to an actual JAR")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    return path, digest


def _admit_canonical_graphframes_runtime(spark):
    if getattr(spark, "version", None) != "4.2.0":
        raise PermissionError("Actual Spark 4.2.0 runtime required")
    try:
        java_version = spark._jvm.java.lang.System.getProperty("java.version")
        delta_main, delta_hash = _jvm_class_jar(
            spark, "org.apache.spark.sql.delta.DeltaLog")
        delta_storage, storage_hash = _jvm_class_jar(spark, "io.delta.storage.LogStore")
        graphframes_jar, graphframes_hash = _jvm_class_jar(
            spark, "org.graphframes.GraphFrame")
    except Exception as error:
        raise PermissionError("Actual loaded Delta, GraphFrames and JDK runtime admission failed") from error
    if java_version != "21.0.13":
        raise PermissionError("Actual JDK 21.0.13 runtime required")
    observed = {
        "spark": spark.version, "delta": "4.4.1",
        "graphframes": "0.12.3", "jdk": java_version,
        "jars": {
            delta_main.name: {"path": str(delta_main), "sha256": delta_hash},
            delta_storage.name: {"path": str(delta_storage), "sha256": storage_hash},
            graphframes_jar.name: {"path": str(graphframes_jar), "sha256": graphframes_hash},
        },
    }
    if ({delta_main.name: delta_hash, delta_storage.name: storage_hash,
         graphframes_jar.name: graphframes_hash} != CANONICAL_RUNTIME_ARTIFACTS):
        raise PermissionError("Loaded Delta or GraphFrames JAR bytes differ from reviewed runtime")
    return observed


def _verify_canonical_persisted_fixture(spark, lease, descriptor):
    """Prove both tiny persisted carrier bags before GraphFrame construction.

    Only scalar aggregates are returned to Python. Carrier rows stay in Spark;
    the separate GraphFrames action remains the only collected result bag.
    """
    from datetime import datetime
    from pyspark.sql import functions as F
    from pyspark.sql.types import (LongType, StringType, StructField,
                                   StructType, TimestampType)

    if type(descriptor) is not ReviewedCanonicalActionDescriptor:
        raise PermissionError("Reviewed canonical action descriptor required for persisted gates")
    source = _json_without_duplicate_members(descriptor.source_bytes)
    if type(source) is not dict or set(source.get("tables", {})) != {
            "object_current", "edge_current"}:
        raise PermissionError("Exact reviewed canonical source tables required")
    if spark.conf.get("spark.sql.session.timeZone") != "UTC":
        raise PermissionError("UTC Spark session timezone required for timestamp gate")

    frames = lease.current_frames()
    pins = lease._delta_lease._pins
    expected_counts = dict(zip(("object_current", "edge_current"),
                               descriptor.fixture.row_counts))
    type_map = {"string": StringType, "bigint": LongType,
                "timestamp": TimestampType}
    checks = {}

    def scalar(frame):
        row = frame.limit(1).first()
        if row is None:
            raise PermissionError("One bounded native fixture aggregate required")
        return row[0]

    for role in ("object_current", "edge_current"):
        frame = frames[role]
        expected_schema = _canonical_carrier_signature(role)
        observed_schema, _ = _actual_schema_signature(frame.schema)
        if observed_schema != expected_schema:
            raise PermissionError(
                f"Persisted {role} schema, field order, type, or nullability differs")

        pin = pins[role]
        expected_protocol = (PROTOCOL["minReaderVersion"],
                             PROTOCOL["minWriterVersion"],
                             tuple(PROTOCOL["tableFeatures"]))
        if pin.protocol != expected_protocol:
            raise PermissionError(f"Persisted {role} Delta protocol differs")

        row_count = scalar(frame.agg(F.count(F.lit(1)).alias("row_count")))
        if row_count != expected_counts[role]:
            raise PermissionError(f"Persisted {role} row count differs from reviewed source")

        rows = source["tables"][role]
        schema = StructType([StructField(name, type_map[kind](), nullable)
                             for name, kind, nullable in expected_schema])
        identity_type = "type_id" if role == "object_current" else "rel_type_id"
        values = []
        for source_row in rows:
            record = dict(source_row)
            record["lookup_hash"] = 0
            record["published_at"] = datetime.fromisoformat(
                record["published_at"].replace("Z", "+00:00"))
            values.append(tuple(record[field.name] for field in schema.fields))
        expected = spark.createDataFrame(values, schema=schema).withColumn(
            "lookup_hash", F.xxhash64(F.col("source_system").cast("string"),
                                      F.col(identity_type).cast("long"),
                                      F.col("id").cast("long")))

        native_hash = F.xxhash64(F.col("source_system").cast("string"),
                                 F.col(identity_type).cast("long"),
                                 F.col("id").cast("long"))
        bad_hash_count = scalar(frame.where(
            F.col("lookup_hash").isNull()
            | (F.col("lookup_hash") != native_hash)
        ).agg(F.count(F.lit(1)).alias("bad_hashes")))
        if bad_hash_count != 0:
            raise PermissionError(f"Persisted {role} Spark xxhash64 values differ")

        # Bidirectional exceptAll preserves multiplicity and compares all
        # authored values, including explicit nulls and timestamp microseconds.
        columns = [field[0] for field in expected_schema]
        extra_count = scalar(frame.select(*columns).exceptAll(
            expected.select(*columns)).agg(F.count(F.lit(1)).alias("extra_rows")))
        missing_count = scalar(expected.select(*columns).exceptAll(
            frame.select(*columns)).agg(F.count(F.lit(1)).alias("missing_rows")))
        if extra_count != 0 or missing_count != 0:
            raise PermissionError(
                f"Persisted {role} source values or timestamp microseconds differ")

        checks[role] = {
            "rowCount": row_count,
            "schemaSha256": _actual_schema_signature(frame.schema)[1],
            "schemaNullability": "matched",
            "protocol": {"minReaderVersion": pin.protocol[0],
                         "minWriterVersion": pin.protocol[1],
                         "tableFeatures": list(pin.protocol[2])},
            "sourceMultiset": "matched",
            "publishedAt": "UTC exact microsecond equality",
            "lookupHash": "Spark xxhash64 recomputation matched",
        }

    return {"status": "verified", "tables": checks,
            "carrierRowsCollected": 0}


def _run_bounded_graphframes_one_hop(spark, lease, profile, fixture_sha256,
                                    selected_seed, expected_rows):
    """Run the shared bounded action over already-held version-pinned frames."""
    if type(lease) is not SyntheticDeltaFixtureLease or profile != PROFILE:
        raise PermissionError("Only the reviewed tiny synthetic smoke action is enabled")
    from graphframes import GraphFrame
    from pyspark.sql import functions as F

    pins = lease.evidence()
    frames = lease.current_frames()
    vertices, graph_edges = _graphframes_views(
        frames["object_current"], frames["edge_current"], F)
    graph = GraphFrame(vertices, graph_edges)
    seed = F.struct(
        F.lit(selected_seed[0]).cast("string").alias("source_system"),
        F.lit(selected_seed[1]).cast("long").alias("type_id"),
        F.lit(selected_seed[2]).cast("long").alias("id"),
    )
    rows = (graph.find("(a)-[e]->(b)")
        .where(F.col("a.id") == seed)
        .select(
            F.col("a.id.source_system").alias("src_source"),
            F.col("a.id.type_id").alias("src_type"),
            F.col("a.id.id").alias("src_id"),
            F.col("e.edge_id.source_system").alias("edge_source"),
            F.col("e.edge_id.rel_type_id").alias("edge_type"),
            F.col("e.edge_id.id").alias("edge_id"),
            F.col("b.id.source_system").alias("dst_source"),
            F.col("b.id.type_id").alias("dst_type"),
            F.col("b.id.id").alias("dst_id"),
        ).limit(MAX_ONE_HOP_ROWS + 1).collect())
    if len(rows) > MAX_ONE_HOP_ROWS:
        raise ValueError("Bounded one-hop GraphFrames result exceeded")
    actual = tuple(sorted(tuple(row[name] for name in (
        "src_source", "src_type", "src_id", "edge_source", "edge_type", "edge_id",
        "dst_source", "dst_type", "dst_id")) for row in rows))
    if actual != tuple(sorted(expected_rows)):
        raise ValueError("GraphFrames one-hop edge occurrence bag differs from frozen oracle")
    lease.renew()
    return {
        "profile": profile,
        "status": "passed",
        "runtime": {"spark": spark.version, "delta": "4.4.1", "graphframes": "0.12.3"},
        "fixtureSha256": fixture_sha256,
        "fixture": pins,
        "result": [list(row) for row in actual],
        "resultRows": len(actual),
        "graphX": "not-used",
        "wholeFrameCollect": "not-used",
    }


def run_canonical_graphframes_one_hop(spark, fixture_owner, *,
                                      native_fixture_verifier=None):
    """Run the separately gated canonical `.4` seed action under retained custody.

    This entrypoint is intentionally unavailable to the older metadata-only
    descriptor and holder. It performs one capped edge-occurrence collection;
    neither input frame is collected or persisted.
    """
    if type(fixture_owner) is not PinnedCanonicalGraphFramesActionFixture:
        raise ValueError("Reviewed canonical GraphFrames action owner required")
    if fixture_owner._spark is not spark:
        raise PermissionError("Action Spark session must be the admitted holder session")
    receipt = None
    with hold_canonical_graphframes_action_fixture(fixture_owner) as lease:
        runtime_open = _admit_canonical_graphframes_runtime(spark)
        lease.record_runtime_identity(runtime_open)
        evidence_open = lease.evidence()
        if native_fixture_verifier is not _verify_canonical_persisted_fixture:
            raise PermissionError(
                "Native persisted-fixture verification hook required before GraphFrame")
        persisted_fixture = native_fixture_verifier(
            spark, lease, fixture_owner.descriptor)
        if (type(persisted_fixture) is not dict
                or persisted_fixture.get("status") != "verified"
                or persisted_fixture.get("carrierRowsCollected") != 0):
            raise PermissionError("Complete bounded persisted-fixture verification required")
        lease.renew()
        frames = lease.current_frames()
        vertices, graph_edges = _graphframes_views(
            frames["object_current"], frames["edge_current"],
            __import__("pyspark.sql.functions", fromlist=["functions"]))
        from graphframes import GraphFrame
        graph = GraphFrame(vertices, graph_edges)
        seed_values = fixture_owner.descriptor.fixture.selected_seed
        from pyspark.sql import functions as F
        seed = F.struct(
            F.lit(seed_values[0]).cast("string").alias("source_system"),
            F.lit(seed_values[1]).cast("long").alias("type_id"),
            F.lit(seed_values[2]).cast("long").alias("id"),
        )
        started = time.monotonic()
        rows = (graph.find("(a)-[e]->(b)")
            .where(F.col("a.id") == seed)
            .select(
                F.col("a.id.source_system").alias("src_source"),
                F.col("a.id.type_id").alias("src_type"),
                F.col("a.id.id").alias("src_id"),
                F.col("e.edge_id.source_system").alias("edge_source"),
                F.col("e.edge_id.rel_type_id").alias("edge_type"),
                F.col("e.edge_id.id").alias("edge_id"),
                F.col("b.id.source_system").alias("dst_source"),
                F.col("b.id.type_id").alias("dst_type"),
                F.col("b.id.id").alias("dst_id"),
            ).limit(fixture_owner.descriptor.cap + 1).collect())
        elapsed = time.monotonic() - started
        if len(rows) > fixture_owner.descriptor.cap:
            raise ValueError("Canonical one-hop action exceeded cap and is incomplete")
        names = fixture_owner.descriptor.output_schema
        actual = tuple(sorted(tuple(row[name] for name in names) for row in rows))
        expected = tuple(sorted(fixture_owner.descriptor.fixture.oracle_rows))
        if actual != expected:
            raise ValueError("Canonical one-hop edge occurrence bag differs from independent oracle")
        lease.renew()
        runtime_close = _admit_canonical_graphframes_runtime(spark)
        if runtime_close != runtime_open:
            raise PermissionError("Loaded runtime or JAR identity changed during action")
        evidence_close = lease.evidence()
        if evidence_close != evidence_open:
            raise PermissionError("Canonical action custody evidence changed during action")
        receipt = {
            "profile": CANONICAL_SYNTHETIC_PROFILE,
            "status": "bounded-one-hop-passed",
            "canonicalFixtureQualification": "persisted-carrier-and-one-hop-verified",
            "expectedRowCounts": list(fixture_owner.descriptor.fixture.row_counts),
            "persistedFixtureEvidence": persisted_fixture,
            "descriptorSha256": fixture_owner.descriptor.sha256,
            "inputDigests": {
                "manifest": fixture_owner.descriptor.manifest_sha256,
                "reviewAnchor": fixture_owner.descriptor.review_anchor_sha256,
                "generator": fixture_owner.descriptor.generator_sha256,
                "source": fixture_owner.descriptor.source_sha256,
                "oracle": fixture_owner.descriptor.oracle_sha256,
            },
            "lock": evidence_open["lock"],
            "lockWindow": evidence_open["lockWindow"],
            "tables": evidence_open["fixture"]["tables"],
            "nativeInventoryBound": {
                role: {"numFiles": evidence_open["fixture"]["tables"][role]["activeFileCount"],
                      "maximum": MAX_SNAPSHOT_FILES}
                for role in ("object_current", "edge_current")},
            "snapshotInventorySha256": evidence_open["snapshotInventorySha256"],
            "runtime": runtime_close,
            "selectedSeed": list(seed_values),
            "cap": fixture_owner.descriptor.cap,
            "outputSchema": list(fixture_owner.descriptor.output_schema),
            "actionElapsedSeconds": elapsed,
            "result": [list(row) for row in actual],
            "resultRows": len(actual),
            "renewals": ["opening", "post-action", "closing"],
            "wholeFrameCollect": "not-used",
            "graphX": "not-used",
            "sourceProgress": "absent", "protectedAck": "absent",
            "publication": "absent", "unityCatalog": "absent",
            "productionAuthority": "absent",
        }
    receipt["lockWindow"]["closedMonotonicNs"] = lease._closed_monotonic_ns
    return receipt


def run_synthetic_graphframes_one_hop(spark, fixture_owner):
    """Run the existing tiny actual GraphFrames smoke; no full-frame collect."""
    if type(fixture_owner) is not PinnedSyntheticDeltaFixture:
        raise ValueError("Actual pinned synthetic Delta fixture owner required")
    if spark.version != "4.2.0":
        raise PermissionError("Selected Spark 4.2.0 smoke runtime required")
    with hold_synthetic_delta_fixture(fixture_owner) as lease:
        return _run_bounded_graphframes_one_hop(
            spark, lease, PROFILE, fixture_owner.fixture.input_sha256,
            fixture_owner.fixture.selected_seed,
            fixture_owner.fixture.expected_one_hop)
