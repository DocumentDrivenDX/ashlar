# SPDX-License-Identifier: MIT OR Apache-2.0
"""Internal synthetic pinned-Delta GraphFrames candidate (no publication authority)."""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import json
import re
import time

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


def run_synthetic_graphframes_one_hop(spark, fixture_owner):
    """Run one actual bounded GraphFrames motif; no full-frame collection/copy."""
    from graphframes import GraphFrame
    from pyspark.sql import functions as F

    if type(fixture_owner) is not PinnedSyntheticDeltaFixture:
        raise ValueError("Actual pinned synthetic Delta fixture owner required")
    if spark.version != "4.2.0":
        raise PermissionError("Selected Spark 4.2.0 smoke runtime required")
    with hold_synthetic_delta_fixture(fixture_owner) as lease:
        pins = lease.evidence()
        frames = lease.current_frames()
        nodes = frames["object_current"]
        edges = frames["edge_current"]
        node_id = F.struct(
            F.col("source_system").cast("string").alias("source_system"),
            F.col("type_id").cast("long").alias("type_id"),
            F.col("id").cast("long").alias("id"),
        )
        vertices = nodes.select(node_id.alias("id"))
        src = F.struct(
            F.col("source_system").cast("string").alias("source_system"),
            F.col("source_type").cast("long").alias("type_id"),
            F.col("source_id").cast("long").alias("id"),
        )
        dst = F.struct(
            F.col("source_system").cast("string").alias("source_system"),
            F.col("target_type").cast("long").alias("type_id"),
            F.col("target_id").cast("long").alias("id"),
        )
        edge_id = F.struct(
            F.col("source_system").cast("string").alias("source_system"),
            F.col("rel_type_id").cast("long").alias("rel_type_id"),
            F.col("id").cast("long").alias("id"),
        )
        graph_edges = edges.select(src.alias("src"), dst.alias("dst"), edge_id.alias("edge_id"))
        graph = GraphFrame(vertices, graph_edges)
        seed = F.struct(
            F.lit(SEED[0]).cast("string").alias("source_system"),
            F.lit(SEED[1]).cast("long").alias("type_id"),
            F.lit(SEED[2]).cast("long").alias("id"),
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
        expected = tuple(sorted(fixture_owner.fixture.expected_one_hop))
        if actual != expected:
            raise ValueError("GraphFrames one-hop edge occurrence bag differs from frozen oracle")
        lease.renew()
        return {
            "profile": PROFILE,
            "status": "passed",
            "runtime": {"spark": spark.version, "delta": "4.4.1", "graphframes": "0.12.3"},
            "fixture": pins,
            "result": [list(row) for row in actual],
            "resultRows": len(actual),
            "graphX": "not-used",
            "wholeFrameCollect": "not-used",
        }
