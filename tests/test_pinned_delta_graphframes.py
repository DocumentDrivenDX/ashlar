import unittest
import hashlib
import json
import os
import tempfile
import sys
import types
from pathlib import Path
from unittest.mock import patch

import ashlar_host.pinned_delta_graphframes as graphframes_module

_REVIEW_ANCHOR_TEMP_DIRS = []

from ashlar_host.pinned_delta_graphframes import (
    EDGE_SCHEMA, FIXTURE_INPUT_SHA256, NODE_SCHEMA, PROTOCOL,
    CANONICAL_RUNTIME_SELECTION, CanonicalSyntheticFixtureDescriptor,
    PinnedCanonicalSyntheticDeltaFixture, _canonical_carrier_signature,
    PinnedSyntheticDeltaFixture, SyntheticGraphFixture,
    create_synthetic_delta_fixture, fixture_input_digest, _graphframes_views,
    hold_synthetic_delta_fixture, hold_canonical_synthetic_delta_fixture,
)


class FakeField:
    def __init__(self, name, dtype, nullable):
        self.name = name
        self.dataType = type("DataType", (), {"simpleString": lambda self: dtype})()
        self.nullable = nullable


class FakeSchema:
    def __init__(self, signature):
        self.fields = [FakeField(*item) for item in signature]

    def json(self):
        return "schema:" + repr(tuple((f.name, f.dataType.simpleString(), f.nullable)
                                       for f in self.fields))


class FakeResult:
    def __init__(self, rows):
        self.rows = rows
        self.limit_value = None

    def limit(self, value):
        self.limit_value = value
        return self

    def collect(self):
        return self.rows[:self.limit_value]


class FakeReader:
    def __init__(self, spark):
        self.spark = spark
        self.options = {}

    def format(self, value):
        if value != "delta":
            raise AssertionError("delta format expected")
        return self

    def option(self, name, value):
        self.options[name] = value
        return self

    def load(self, path):
        self.spark.reads.append((path, dict(self.options)))
        frame = FakeFrame(FakeSchema(self.spark.schema_by_path[path]),
                           self.spark.content_by_path[path])
        frame._input_files = list(getattr(self.spark, "input_files_by_path", {}).get(path, []))
        frame._spark = self.spark
        self.spark.loaded_frames.append(frame)
        return frame


class FakeFrame:
    def __init__(self, schema, rows):
        self.schema = schema
        self.rows = rows
        self.columns = tuple(field.name for field in schema.fields)
        self.maximum = None

    def select(self, *columns):
        return FakeFrame(self.schema,
                         [{name: row[name] for name in columns} for row in self.rows])

    def limit(self, maximum):
        selected = FakeFrame(self.schema, self.rows[:maximum])
        selected.maximum = maximum
        return selected

    def collect(self):
        self.collect_calls = getattr(self, "collect_calls", 0) + 1
        return self.rows if self.maximum is None else self.rows[:self.maximum]

    def inputFiles(self):
        if hasattr(self, "_spark"):
            self._spark.input_file_calls += 1
        return list(getattr(self, "_input_files", []))


class FakeSpark:
    def __init__(self, paths):
        self.paths = list(paths.values())
        self.schema_by_path = {paths["object_current"]: NODE_SCHEMA,
                               paths["edge_current"]: EDGE_SCHEMA}
        self.uuid_by_path = {
            paths["object_current"]: "00000000-0000-0000-0000-000000000001",
            paths["edge_current"]: "00000000-0000-0000-0000-000000000002",
        }
        self.version_by_path = {path: 0 for path in self.paths}
        self.protocol_by_path = {path: dict(PROTOCOL) for path in self.paths}
        self.content_by_path = {
            paths["object_current"]: [
                dict(zip(("source_system", "type_id", "id"), row))
                for row in (("ashlar-graphframes-smoke", 1, 101),
                            ("ashlar-graphframes-smoke", 1, 202),
                            ("ashlar-graphframes-smoke", 1, 303))],
            paths["edge_current"]: [
                dict(zip(("source_system", "rel_type_id", "id", "source_type", "source_id",
                          "target_type", "target_id"), row))
                for row in (("ashlar-graphframes-smoke", 7, 9001, 1, 101, 1, 202),
                            ("ashlar-graphframes-smoke", 7, 9002, 1, 101, 1, 202))],
        }
        self.reads = []
        self.loaded_frames = []
        self.input_files_by_path = {}
        self.input_file_calls = 0

    @property
    def read(self):
        return FakeReader(self)

    def sql(self, query):
        path = next((path for path in self.paths if "`" + path + "`" in query), None)
        if path is None:
            raise AssertionError("unexpected native query: %r paths=%r" % (query, self.paths))
        if query.startswith("DESCRIBE DETAIL"):
            protocol = self.protocol_by_path[path]
            return FakeResult([{
                "format": "delta", "id": self.uuid_by_path[path],
                "minReaderVersion": protocol["minReaderVersion"],
                "minWriterVersion": protocol["minWriterVersion"],
                "tableFeatures": protocol["tableFeatures"],
                "numFiles": getattr(self, "native_num_files_by_path", {}).get(
                    path, len(getattr(self, "input_files_by_path", {}).get(path, []))),
            }])
        if query.startswith("DESCRIBE HISTORY"):
            return FakeResult([{"version": self.version_by_path[path]}])
        raise AssertionError("unexpected native query")


class CanonicalFakeSpark(FakeSpark):
    version = dict(CANONICAL_RUNTIME_SELECTION)["spark"]

    def __init__(self, paths):
        super().__init__(paths)
        self.schema_by_path = {paths[role]: _canonical_carrier_signature(role)
                               for role in ("object_current", "edge_current")}
        self.content_by_path = {path: [] for path in self.paths}


def reviewed_action_descriptor(anchor_path=None):
    generator = b"independently reviewed deterministic generator bytes v1"
    source = b"immutable synthetic source artifact bytes v1"
    seed = ["ashlar-scale-fixture", 1, 0]
    row = ["ashlar-scale-fixture", 1, 0, "ashlar-scale-fixture", 7, 0,
           "ashlar-scale-fixture", 1, 1]
    oracle = json.dumps({"profile": "ashlar-canonical-one-hop-independent-oracle/0.1",
                         "seed": seed, "rows": [row]}, separators=(",", ":")).encode()
    if anchor_path is None:
        anchor_temp = tempfile.TemporaryDirectory(prefix="ashlar-gf-anchor-")
        _REVIEW_ANCHOR_TEMP_DIRS.append(anchor_temp)
        anchor_path = Path(anchor_temp.name) / "review-anchor.json"
    else:
        anchor_path = Path(anchor_path)
    manifest = {
        "profile": graphframes_module.CANONICAL_SYNTHETIC_PROFILE,
        "reviewStatus": "independently-reviewed", "reviewId": "review-2026-10-23-01",
        "generatorRevision": "synthetic-generator-test/1",
        "generatorSha256": hashlib.sha256(generator).hexdigest(),
        "sourceSha256": hashlib.sha256(source).hexdigest(),
        "carrierDefinitionSha256": graphframes_module._canonical_carrier_definition_sha256(),
        "umfRevision": graphframes_module.CANONICAL_UMF_REVISION,
        "runtime": dict(graphframes_module.CANONICAL_RUNTIME_SELECTION),
        "rowCounts": [1_000_000, 5_000_000],
        "formula": {"nodes": "sequential signed64 identity", "edges": "bounded endpoint formula"},
        "formulaProofs": {key: "reviewed finite arithmetic proof" for key in (
            "signed64", "nodeIdentityUnique", "edgeIdentityUnique",
            "endpointsInNodeDomain", "parallelEdgeMultiplicity", "nullAndPayloadRules")},
        "selectedSeed": seed, "cap": 1,
        "outputSchema": ["src_source", "src_type", "src_id", "edge_source",
                         "edge_type", "edge_id", "dst_source", "dst_type", "dst_id"],
        "oracleSha256": hashlib.sha256(oracle).hexdigest(),
    }
    manifest_bytes = json.dumps(manifest, separators=(",", ":")).encode()
    anchor = {
        "profile": "ashlar-canonical-action-review-anchor/0.1",
        "reviewStatus": "independently-reviewed", "reviewId": manifest["reviewId"],
        "manifestSha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "generatorSha256": manifest["generatorSha256"],
        "sourceSha256": manifest["sourceSha256"],
        "oracleSha256": manifest["oracleSha256"],
        "carrierDefinitionSha256": manifest["carrierDefinitionSha256"],
    }
    anchor_bytes = json.dumps(anchor, separators=(",", ":")).encode()
    anchor_path.write_bytes(anchor_bytes)
    descriptor = graphframes_module.ReviewedCanonicalActionDescriptor.from_reviewed_bytes(
        manifest_bytes, generator, source, oracle,
        review_anchor_path=anchor_path,
        selected_review_anchor_sha256=hashlib.sha256(anchor_bytes).hexdigest())
    return descriptor, row


class FakeActionQuery:
    def __init__(self, rows): self.rows = rows; self.limit_value = None
    def where(self, _predicate): return self
    def select(self, *_columns): return self
    def limit(self, value): self.limit_value = value; return self
    def collect(self): return self.rows[:self.limit_value]


class FakeActionGraphFrame:
    rows = []
    last = None
    def __init__(self, _vertices, _edges): pass
    def find(self, _pattern):
        type(self).last = FakeActionQuery(type(self).rows)
        return type(self).last


class ActionExpr:
    def __init__(self, value): self.value = value
    def cast(self, kind): return ActionExpr(("cast", self.value, kind))
    def alias(self, name): return ActionExpr(("alias", self.value, name))
    def __eq__(self, other): return ActionExpr(("eq", self.value, getattr(other, "value", other)))


class ActionFunctions:
    @staticmethod
    def col(name): return ActionExpr(("column", name))
    @staticmethod
    def lit(value): return ActionExpr(("literal", value))
    @staticmethod
    def struct(*items): return ActionExpr(("struct", tuple(item.value for item in items)))


def canonical_fixture_descriptor(**changes):
    values = dict(
        generator_revision="canonical-carrier-benchmark/0.1",
        generator_sha256="a" * 64,
        source_sha256="b" * 64,
        row_counts=(1_000_000_000, 5_000_000_000),
        formula="frozen deterministic typed carrier formula; oracle independently specified",
        selected_seed=("ashlar-scale-fixture", 1, 0),
        oracle_rows=(("ashlar-scale-fixture", 1, 0,
                      "ashlar-scale-fixture", 7, 0,
                      "ashlar-scale-fixture", 1, 1),),
    )
    values.update(changes)
    return CanonicalSyntheticFixtureDescriptor(**values)


class Expr:
    def __init__(self, value): self.value = value
    def cast(self, kind): return Expr(("cast", self.value, kind))
    def alias(self, name): return Expr(("alias", self.value, name))


class Functions:
    @staticmethod
    def col(name): return Expr(("column", name))
    @staticmethod
    def struct(*items): return Expr(("struct", tuple(item.value for item in items)))


class PlanFrame:
    def __init__(self, columns): self.columns = tuple(columns); self.selection = None
    def select(self, *items):
        result = PlanFrame(self.columns)
        result.selection = tuple(item.value for item in items)
        return result


class PinnedDeltaGraphFramesTests(unittest.TestCase):
    def setUp(self):
        self.paths = {"object_current": "/tmp/fixture-nodes",
                      "edge_current": "/tmp/fixture-edges"}
        self.spark = FakeSpark(self.paths)
        self.fixture = SyntheticGraphFixture()

    def test_frozen_fixture_input_digest_and_independent_parallel_edge_oracle(self):
        self.assertEqual(fixture_input_digest(), FIXTURE_INPUT_SHA256)
        self.assertEqual(len(self.fixture.node_rows), 3)
        self.assertEqual(len(self.fixture.edge_rows), 2)
        self.assertEqual(self.fixture.edge_rows[0][2], 9001)
        self.assertEqual(self.fixture.edge_rows[1][2], 9002)
        self.assertEqual(len(self.fixture.expected_one_hop), 2)
        self.assertEqual(self.fixture.expected_one_hop[0][5], 9001)
        self.assertEqual(self.fixture.expected_one_hop[1][5], 9002)

    def test_open_records_real_metadata_and_reads_each_exact_delta_version(self):
        owner = PinnedSyntheticDeltaFixture(self.spark, self.fixture, self.paths)
        with hold_synthetic_delta_fixture(owner) as lease:
            evidence = lease.evidence()
            self.assertEqual(evidence["sourceProgress"], "absent")
            self.assertEqual(evidence["protectedAck"], "absent")
            self.assertEqual(evidence["tables"]["object_current"]["version"], 0)
            self.assertEqual(evidence["tables"]["object_current"]["protocol"], PROTOCOL)
            self.assertTrue(all(nullable for _, _, nullable in NODE_SCHEMA + EDGE_SCHEMA))
            frames = lease.current_frames()
            self.assertEqual(set(frames), {"object_current", "edge_current"})
        versioned_reads = [item for item in self.spark.reads if item[1]]
        self.assertEqual(versioned_reads, [
            (self.paths["object_current"], {"versionAsOf": 0}),
            (self.paths["edge_current"], {"versionAsOf": 0}),
        ])

    def test_changed_head_uuid_schema_or_protocol_refuses_before_result_release(self):
        for mutation in ("version", "uuid"):
            with self.subTest(mutation=mutation):
                spark = FakeSpark(self.paths)
                owner = PinnedSyntheticDeltaFixture(spark, self.fixture, self.paths)
                lease = owner.open()
                path = self.paths["edge_current"]
                if mutation == "version":
                    spark.version_by_path[path] = 1
                else:
                    spark.uuid_by_path[path] = "00000000-0000-0000-0000-000000000003"
                with self.assertRaises(PermissionError):
                    lease.renew()

    def test_expired_lease_refuses(self):
        owner = PinnedSyntheticDeltaFixture(self.spark, self.fixture, self.paths)
        lease = owner.open()
        lease._deadline = 0
        with self.assertRaises(PermissionError):
            lease.renew()

    def test_mixed_head_refuses_and_closing_renewal_does_not_release_result(self):
        owner = PinnedSyntheticDeltaFixture(self.spark, self.fixture, self.paths)
        with self.assertRaises(PermissionError):
            with hold_synthetic_delta_fixture(owner) as lease:
                self.spark.version_by_path[self.paths["edge_current"]] = 1

    def test_wrong_schema_and_protocol_refuse_at_observation(self):
        self.spark.schema_by_path[self.paths["object_current"]] = EDGE_SCHEMA
        with self.assertRaises(ValueError):
            PinnedSyntheticDeltaFixture(self.spark, self.fixture, self.paths).open()
        self.spark = FakeSpark(self.paths)
        self.spark.protocol_by_path[self.paths["edge_current"]] = {
            "minReaderVersion": 1, "minWriterVersion": 7,
            "tableFeatures": ["futureFeature"],
        }
        with self.assertRaisesRegex(ValueError, "futureFeature"):
            PinnedSyntheticDeltaFixture(self.spark, self.fixture, self.paths).open()

    def test_extra_or_changed_tiny_rows_refuse_before_graph_action(self):
        for mutation in ("extra", "changed", "null"):
            with self.subTest(mutation=mutation):
                spark = FakeSpark(self.paths)
                owner = PinnedSyntheticDeltaFixture(spark, self.fixture, self.paths)
                lease = owner.open()
                edge_rows = spark.content_by_path[self.paths["edge_current"]]
                if mutation == "extra":
                    edge_rows.append(dict(edge_rows[0], id=9003))
                else:
                    edge_rows[0]["target_id"] = None if mutation == "null" else 303
                with self.assertRaises(PermissionError):
                    lease.current_frames()

    def test_transient_views_preserve_payload_and_add_typed_identity_keys(self):
        nodes = PlanFrame(("source_system", "type_id", "id", "props_json"))
        edges = PlanFrame(("source_system", "rel_type_id", "id", "source_type",
                           "source_id", "target_type", "target_id", "props_json"))
        vertices, graph_edges = _graphframes_views(nodes, edges, Functions)
        self.assertEqual(vertices.columns, nodes.columns)
        vertex_names = [item[-1] if item[0] == "alias" else item[1]
                        for item in vertices.selection]
        self.assertEqual(vertex_names,
                         ["source_system", "type_id", "props_json", "ashlar_entity_id", "id"])
        edge_names = [item[-1] if item[0] == "alias" else item[1]
                      for item in graph_edges.selection]
        self.assertEqual(edge_names[-3:], ["src", "dst", "edge_id"])
        self.assertEqual(edge_names[:len(edges.columns)], list(edges.columns))
        identity = graph_edges.selection[-1][1]
        self.assertEqual(identity[0], "struct")
        self.assertEqual([item[2] for item in identity[1]],
                         ["source_system", "rel_type_id", "id"])

    def test_transient_mapping_rejects_incomplete_carrier_schema(self):
        with self.assertRaisesRegex(ValueError, "Canonical .4"):
            _graphframes_views(PlanFrame(("source_system", "id")),
                               PlanFrame(EDGE_SCHEMA), Functions)

    def test_canonical_holder_pins_exact_versions_without_collecting_rows(self):
        paths = {"object_current": "/tmp/canonical-nodes",
                 "edge_current": "/tmp/canonical-edges"}
        spark = CanonicalFakeSpark(paths)
        spark.version_by_path[paths["object_current"]] = 4
        spark.version_by_path[paths["edge_current"]] = 9
        owner = PinnedCanonicalSyntheticDeltaFixture(
            spark, canonical_fixture_descriptor(), paths)
        with hold_canonical_synthetic_delta_fixture(owner) as lease:
            evidence = lease.evidence()
            self.assertEqual(evidence["publication"], "absent")
            self.assertEqual(evidence["sourceProgress"], "absent")
            self.assertEqual(evidence["protectedAck"], "absent")
            self.assertEqual(evidence["unityCatalog"], "absent")
            self.assertEqual(evidence["retainedFileCustody"], "not-established")
            self.assertEqual(evidence["descriptorAuthority"],
                             "declared-and-digested-only; generator/source/oracle not independently audited")
            self.assertEqual(evidence["observedRuntime"], {"spark": "4.2.0"})
            self.assertEqual(evidence["tables"]["object_current"]["version"], 4)
            self.assertEqual(evidence["tables"]["edge_current"]["version"], 9)
            with self.assertRaises(TypeError):
                lease._pins["edge_current"] = lease._pins["object_current"]
            frames = lease.current_frames()
            self.assertEqual(set(frames), {"object_current", "edge_current"})
            self.assertEqual(
                [(path, options) for path, options in spark.reads if options],
                [(paths["object_current"], {"versionAsOf": 4}),
                 (paths["edge_current"], {"versionAsOf": 9})])
        self.assertTrue(all(getattr(frame, "collect_calls", 0) == 0
                            for frame in spark.loaded_frames))

    def test_canonical_fixture_descriptor_digest_binds_generator_formula_counts_and_oracle(self):
        descriptor = canonical_fixture_descriptor()
        self.assertNotEqual(descriptor.sha256,
                            canonical_fixture_descriptor(formula="changed formula").sha256)
        self.assertNotEqual(descriptor.sha256,
                            canonical_fixture_descriptor(row_counts=(3, 2)).sha256)
        self.assertNotEqual(descriptor.sha256,
                            canonical_fixture_descriptor(oracle_rows=()).sha256)

    def test_canonical_holder_refuses_bad_descriptor_schema_and_duplicate_uuid(self):
        paths = {"object_current": "/tmp/canonical-nodes",
                 "edge_current": "/tmp/canonical-edges"}
        spark = CanonicalFakeSpark(paths)
        with self.assertRaises(ValueError):
            PinnedCanonicalSyntheticDeltaFixture(spark, {}, paths)
        spark.schema_by_path[paths["edge_current"]] = EDGE_SCHEMA
        with self.assertRaisesRegex(ValueError, "generated `\.4`"):
            PinnedCanonicalSyntheticDeltaFixture(
                spark, canonical_fixture_descriptor(), paths)._observe_table(
                    "edge_current")
        spark = CanonicalFakeSpark(paths)
        spark.uuid_by_path[paths["edge_current"]] = spark.uuid_by_path[paths["object_current"]]
        with self.assertRaisesRegex(ValueError, "Distinct native Delta UUIDs"):
            PinnedCanonicalSyntheticDeltaFixture(
                spark, canonical_fixture_descriptor(), paths).open()

    def test_generated_long_types_are_normalized_to_spark_bigint(self):
        signature = _canonical_carrier_signature("object_current")
        self.assertEqual(signature[:3], (("source_system", "string", False),
                                         ("type_id", "bigint", False),
                                         ("id", "bigint", False)))

    def test_canonical_holder_refuses_version_drift_and_expired_lease(self):
        paths = {"object_current": "/tmp/canonical-nodes",
                 "edge_current": "/tmp/canonical-edges"}
        spark = CanonicalFakeSpark(paths)
        owner = PinnedCanonicalSyntheticDeltaFixture(
            spark, canonical_fixture_descriptor(), paths)
        lease = owner.open()
        spark.version_by_path[paths["edge_current"]] = 1
        with self.assertRaisesRegex(PermissionError, "changed"):
            lease.renew()
        lease = owner.open()
        lease._deadline = 0
        with self.assertRaisesRegex(PermissionError, "expired"):
            lease.renew()
        owner = PinnedCanonicalSyntheticDeltaFixture(
            spark, canonical_fixture_descriptor(), paths)
        with self.assertRaises(PermissionError):
            with hold_canonical_synthetic_delta_fixture(owner):
                spark.version_by_path[paths["object_current"]] = 3

    def test_paths_must_be_distinct(self):
        with self.assertRaises(ValueError):
            PinnedSyntheticDeltaFixture(self.spark, self.fixture, {
                "object_current": "/tmp/same", "edge_current": "/tmp/same"})


class CanonicalActionReadinessTests(unittest.TestCase):
    def _fixture(self):
        temporary = tempfile.TemporaryDirectory(prefix="ashlar-gf-action-")
        root = Path(temporary.name)
        os.chmod(root, 0o700)
        paths = {role: str(root / role) for role in ("object_current", "edge_current")}
        files = {}
        for role, table in paths.items():
            Path(table).mkdir()
            data = Path(table) / (role + ".parquet")
            data.write_bytes(b"frozen active file")
            log = Path(table) / "_delta_log"
            log.mkdir()
            (log / "00000000000000000000.json").write_text("{}\n")
            files[str(Path(table).resolve())] = [data.resolve().as_uri()]
        paths = {role: str(Path(path).resolve()) for role, path in paths.items()}
        spark = CanonicalFakeSpark(paths)
        spark.input_files_by_path = files
        descriptor, row = reviewed_action_descriptor(root / "review-anchor.json")
        owner = graphframes_module.PinnedCanonicalGraphFramesActionFixture(
            spark, descriptor, root, paths)
        return temporary, root, spark, owner, descriptor, row

    def test_descriptor_requires_exact_reviewed_bytes_and_rejects_drift(self):
        descriptor, _ = reviewed_action_descriptor()
        with self.assertRaises(ValueError):
            graphframes_module.PinnedCanonicalGraphFramesActionFixture(
                CanonicalFakeSpark({"object_current": "/tmp/a", "edge_current": "/tmp/b"}),
                canonical_fixture_descriptor(), "/tmp", {})
        with self.assertRaisesRegex(ValueError, "input bytes"):
            graphframes_module.ReviewedCanonicalActionDescriptor.from_reviewed_bytes(
                descriptor.manifest_bytes, b"changed generator", descriptor.source_bytes,
                descriptor.oracle_bytes,
                review_anchor_path=descriptor.review_anchor_path,
                selected_review_anchor_sha256=descriptor.review_anchor_sha256)

    def test_coherent_replacement_review_anchor_refuses_during_lease(self):
        temporary, _root, _spark, owner, descriptor, _row = self._fixture()
        lease = owner.open()
        try:
            replacement_manifest = json.loads(descriptor.manifest_bytes.decode())
            replacement_manifest["reviewId"] = "coherent-replacement-review"
            manifest_bytes = json.dumps(replacement_manifest, separators=(",", ":")).encode()
            replacement_anchor = {
                "profile": "ashlar-canonical-action-review-anchor/0.1",
                "reviewStatus": "independently-reviewed",
                "reviewId": replacement_manifest["reviewId"],
                "manifestSha256": hashlib.sha256(manifest_bytes).hexdigest(),
                "generatorSha256": descriptor.generator_sha256,
                "sourceSha256": descriptor.source_sha256,
                "oracleSha256": descriptor.oracle_sha256,
                "carrierDefinitionSha256": descriptor.fixture.carrier_definition_sha256,
            }
            Path(descriptor.review_anchor_path).write_text(
                json.dumps(replacement_anchor, separators=(",", ":")))
            with self.assertRaisesRegex(PermissionError, "review anchor changed"):
                lease.renew()
        finally:
            lease._closed = True
            os.close(lease._lock_fd)
            temporary.cleanup()

    def test_selection_mutation_causes_closing_failure(self):
        for field in ("descriptor", "table_paths", "root", "lock_path"):
            with self.subTest(field=field):
                temporary, _root, _spark, owner, _descriptor, _row = self._fixture()
                lease = owner.open()
                if field == "descriptor":
                    owner.descriptor, _ = reviewed_action_descriptor(
                        owner.root / "replacement-anchor.json")
                elif field == "table_paths":
                    paths = dict(owner.table_paths)
                    paths["edge_current"] = str(owner.root / "changed-edge-table")
                    owner.table_paths = paths
                elif field == "root":
                    replacement_root = owner.root / "replacement-root"
                    replacement_root.mkdir(mode=0o700)
                    owner.root = replacement_root
                else:
                    owner.lock_path = owner.root / "replacement.lock"
                with self.assertRaisesRegex(PermissionError, "Selected canonical"):
                    lease.close()
                temporary.cleanup()

    def test_action_lock_contention_and_retained_file_or_log_drift_refuse(self):
        for mutation in ("missing", "replaced", "added-log"):
            with self.subTest(mutation=mutation):
                temporary, _root, _spark, owner, _descriptor, _row = self._fixture()
                try:
                    lease = owner.open()
                    with self.assertRaisesRegex(PermissionError, "already in use"):
                        owner.open()
                    file_path = Path(owner.table_paths["edge_current"]) / "edge_current.parquet"
                    if mutation == "missing":
                        file_path.unlink()
                    elif mutation == "replaced":
                        replacement = file_path.with_suffix(".replacement")
                        replacement.write_bytes(b"replacement bytes differ")
                        os.replace(replacement, file_path)
                    else:
                        (Path(owner.table_paths["edge_current"]) / "_delta_log" /
                         "00000000000000000001.json").write_text("{}\n")
                    with self.assertRaises(PermissionError):
                        lease.renew()
                    lease._closed = True
                    os.close(lease._lock_fd)
                finally:
                    temporary.cleanup()

    def test_action_lease_refuses_expiry_and_delta_head_schema_or_protocol_drift(self):
        for mutation in ("expired", "version", "schema", "protocol"):
            with self.subTest(mutation=mutation):
                temporary, _root, spark, owner, _descriptor, _row = self._fixture()
                try:
                    lease = owner.open()
                    if mutation == "expired":
                        lease._deadline = 0
                    elif mutation == "version":
                        spark.version_by_path[owner.table_paths["edge_current"]] = 1
                    elif mutation == "schema":
                        spark.schema_by_path[owner.table_paths["edge_current"]] = EDGE_SCHEMA
                    else:
                        spark.protocol_by_path[owner.table_paths["edge_current"]] = {
                            "minReaderVersion": 1, "minWriterVersion": 7,
                            "tableFeatures": ["futureFeature"]}
                    with self.assertRaises((PermissionError, ValueError)):
                        lease.renew()
                    lease._closed = True
                    os.close(lease._lock_fd)
                finally:
                    temporary.cleanup()

    def test_loaded_runtime_spark_and_jar_mismatches_fail_closed(self):
        with self.assertRaisesRegex(PermissionError, "Spark 4.2.0"):
            graphframes_module._admit_canonical_graphframes_runtime(
                type("Spark", (), {"version": "3.5.0"})())
        with tempfile.TemporaryDirectory() as directory:
            paths = {}
            for name in ("delta-spark_4.2_2.13-4.4.1.jar",
                         "delta-storage-4.4.1.jar",
                         "graphframes-spark4_2.13-0.12.3.jar"):
                path = Path(directory) / name
                path.write_bytes(b"wrong jar bytes")
                paths[name] = path
            fake_jvm = types.SimpleNamespace(java=types.SimpleNamespace(
                lang=types.SimpleNamespace(System=types.SimpleNamespace(
                    getProperty=lambda _name: "21.0.13"))))
            spark = types.SimpleNamespace(version="4.2.0", _jvm=fake_jvm)
            expected = list(paths.values())
            with patch.object(graphframes_module, "_jvm_class_jar",
                              side_effect=[(path, hashlib.sha256(path.read_bytes()).hexdigest())
                                           for path in expected]):
                with self.assertRaisesRegex(PermissionError, "JAR bytes"):
                    graphframes_module._admit_canonical_graphframes_runtime(spark)

    def test_jvm_class_resolution_uses_spark_context_classloader(self):
        with tempfile.TemporaryDirectory() as directory:
            jar = Path(directory) / "spark-added.jar"
            jar.write_bytes(b"loaded by Spark context loader")
            calls = []
            location = types.SimpleNamespace(toURI=lambda: types.SimpleNamespace(
                getPath=lambda: str(jar)))
            java_class = types.SimpleNamespace(getProtectionDomain=lambda:
                types.SimpleNamespace(getCodeSource=lambda:
                    types.SimpleNamespace(getLocation=lambda: location)))

            class Loader:
                def loadClass(self, name):
                    calls.append(name)
                    return java_class

            loader = Loader()
            thread = types.SimpleNamespace(currentThread=lambda: types.SimpleNamespace(
                getContextClassLoader=lambda: loader))
            spark = types.SimpleNamespace(_jvm=types.SimpleNamespace(
                java=types.SimpleNamespace(lang=types.SimpleNamespace(Thread=thread))))
            found, digest = graphframes_module._jvm_class_jar(spark, "org.graphframes.GraphFrame")
            self.assertEqual(found, jar.resolve())
            self.assertEqual(digest, hashlib.sha256(jar.read_bytes()).hexdigest())
            self.assertEqual(calls, ["org.graphframes.GraphFrame"])

    def test_native_numfiles_bounds_inputfiles_and_requires_exact_match(self):
        temporary, _root, spark, owner, _descriptor, _row = self._fixture()
        try:
            edge = owner.table_paths["edge_current"]
            spark.native_num_files_by_path = {edge: graphframes_module.MAX_SNAPSHOT_FILES + 1}
            with self.assertRaisesRegex(ValueError, "numFiles"):
                owner.open()
            self.assertEqual(spark.input_file_calls, 0)
        finally:
            temporary.cleanup()

        temporary, _root, spark, owner, _descriptor, _row = self._fixture()
        try:
            edge = owner.table_paths["edge_current"]
            spark.native_num_files_by_path = {edge: 2}
            with self.assertRaisesRegex(PermissionError, "differs from pin-bound"):
                owner.open()
        finally:
            temporary.cleanup()

    def _run_action_with_rows(self, rows, runtime_values=None):
        temporary, _root, spark, owner, descriptor, expected = self._fixture()
        FakeActionGraphFrame.rows = rows
        fake_functions = types.ModuleType("pyspark.sql.functions")
        fake_functions.col = ActionFunctions.col
        fake_functions.lit = ActionFunctions.lit
        fake_functions.struct = ActionFunctions.struct
        pyspark = types.ModuleType("pyspark")
        pyspark.__path__ = []
        sql = types.ModuleType("pyspark.sql")
        sql.__path__ = []
        graphframes = types.ModuleType("graphframes")
        graphframes.GraphFrame = FakeActionGraphFrame
        modules = {"pyspark": pyspark, "pyspark.sql": sql,
                   "pyspark.sql.functions": fake_functions, "graphframes": graphframes}
        try:
            with patch.dict(sys.modules, modules), patch.object(
                    graphframes_module, "_admit_canonical_graphframes_runtime",
                    side_effect=runtime_values or [{"spark": "4.2.0", "delta": "4.4.1",
                                  "graphframes": "0.12.3", "jdk": "21.0.13", "jars": {}}] * 3):
                result = graphframes_module.run_canonical_graphframes_one_hop(spark, owner)
            self.assertTrue(all(getattr(frame, "collect_calls", 0) == 0
                                for frame in spark.loaded_frames))
            return result, FakeActionGraphFrame.last, temporary, expected
        except BaseException:
            temporary.cleanup()
            raise

    def test_bounded_action_passes_exact_occurrence_bag_without_input_collect(self):
        descriptor, row = reviewed_action_descriptor()
        result, query, temporary, _ = self._run_action_with_rows([dict(zip(
            descriptor.output_schema, row))])
        try:
            self.assertEqual(result["status"], "bounded-one-hop-passed")
            self.assertEqual(result["canonicalFixtureQualification"],
                             "unqualified-persisted-row-count-evidence-absent")
            self.assertEqual(result["persistedRowCountEvidence"], "absent")
            self.assertEqual(result["result"], [row])
            self.assertEqual(result["sourceProgress"], "absent")
            self.assertEqual(result["protectedAck"], "absent")
            self.assertEqual(query.limit_value, descriptor.cap + 1)
            self.assertIsInstance(result["lockWindow"]["closedMonotonicNs"], int)
        finally:
            temporary.cleanup()

    def test_cap_overflow_and_oracle_mismatch_release_no_receipt(self):
        descriptor, row = reviewed_action_descriptor()
        with self.assertRaisesRegex(ValueError, "exceeded cap"):
            _result, _query, temporary, _ = self._run_action_with_rows([
                dict(zip(descriptor.output_schema, row)),
                dict(zip(descriptor.output_schema, row))])
            temporary.cleanup()
        wrong = list(row)
        wrong[5] = 99
        with self.assertRaisesRegex(ValueError, "differs from independent oracle"):
            _result, _query, temporary, _ = self._run_action_with_rows([
                dict(zip(descriptor.output_schema, wrong))])
            temporary.cleanup()

    def test_runtime_drift_before_release_suppresses_success_receipt(self):
        descriptor, row = reviewed_action_descriptor()
        admitted = {"spark": "4.2.0", "delta": "4.4.1",
                    "graphframes": "0.12.3", "jdk": "21.0.13", "jars": {}}
        changed = dict(admitted, jars={"changed.jar": {"sha256": "0" * 64}})
        with self.assertRaisesRegex(PermissionError, "changed during action"):
            _result, _query, _temporary, _ = self._run_action_with_rows(
                [dict(zip(descriptor.output_schema, row))], [admitted, changed, changed])


if __name__ == "__main__":
    unittest.main()
