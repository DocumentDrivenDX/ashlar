import unittest

from ashlar_host.pinned_delta_graphframes import (
    EDGE_SCHEMA, FIXTURE_INPUT_SHA256, NODE_SCHEMA, PROTOCOL,
    PinnedSyntheticDeltaFixture, SyntheticGraphFixture,
    create_synthetic_delta_fixture, fixture_input_digest, _graphframes_views,
    hold_synthetic_delta_fixture,
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
        return FakeFrame(FakeSchema(self.spark.schema_by_path[path]),
                         self.spark.content_by_path[path])


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
        return self.rows if self.maximum is None else self.rows[:self.maximum]


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

    @property
    def read(self):
        return FakeReader(self)

    def sql(self, query):
        path = next((path for path in self.paths if "`" + path + "`" in query), None)
        if path is None:
            raise AssertionError("unexpected native query")
        if query.startswith("DESCRIBE DETAIL"):
            protocol = self.protocol_by_path[path]
            return FakeResult([{
                "format": "delta", "id": self.uuid_by_path[path],
                "minReaderVersion": protocol["minReaderVersion"],
                "minWriterVersion": protocol["minWriterVersion"],
                "tableFeatures": protocol["tableFeatures"],
            }])
        if query.startswith("DESCRIBE HISTORY"):
            return FakeResult([{"version": self.version_by_path[path]}])
        raise AssertionError("unexpected native query")


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

    def test_paths_must_be_distinct(self):
        with self.assertRaises(ValueError):
            PinnedSyntheticDeltaFixture(self.spark, self.fixture, {
                "object_current": "/tmp/same", "edge_current": "/tmp/same"})


if __name__ == "__main__":
    unittest.main()
