"""Small typed Delta writer controls with real file custody; no native qualification."""
import hashlib
import json
import tempfile
import unittest
from collections import Counter
from pathlib import Path
from types import SimpleNamespace

import test_finite_graph_export as fixtures
from ashlar.graph_release import release_columns
from ashlar_host.finite_pack import FinitePackDefinition
from ashlar_host.graphframes_pack_scenarios import prepare_scenarios
from ashlar_host.fabric_pack_preparation import scenario_properties
from ashlar_host.local_graph_export import LocalGraphExportSelection


class Frame:
    def __init__(self, spark, rows, schema):
        self.spark = spark
        self.rows = list(rows)
        fields = [part.strip().split() for part in schema.split(',')]
        self.names = [field[0] for field in fields]
        self.schema = SimpleNamespace(fields=[SimpleNamespace(
            name=name, dataType=SimpleNamespace(typeName=lambda kind=kind: 'long' if kind == 'BIGINT' else kind.lower()))
            for name, kind in fields])
        self.maximum = None

    @property
    def write(self): return Writer(self)

    def select(self, *names):
        assert tuple(names) == tuple(self.names)
        return self

    def limit(self, maximum):
        self.spark.limits.append(maximum)
        self.maximum = maximum
        return self

    def collect(self):
        assert self.maximum is not None, 'Native readback must be bounded'
        return self.rows[:self.maximum]


class Writer:
    def __init__(self, frame): self.frame = frame
    def format(self, value):
        assert value == 'delta'
        return self
    def mode(self, value):
        assert value == 'errorifexists'
        return self
    def save(self, path):
        spark = self.frame.spark
        if path in spark.tables: raise FileExistsError(path)
        if spark.fail_edge and path.endswith('/edges'):
            raise RuntimeError('Inert partial edge write')
        spark.tables[path] = self.frame


class Spark:
    def __init__(self):
        self.tables = {}
        self.limits = []
        self.metadata_reads = []
        self.fail_edge = False
        self.corruption = None
    def createDataFrame(self, rows, schema): return Frame(self, rows, schema)
    @property
    def read(self): return self
    def format(self, value):
        assert value == 'delta'
        return self
    def option(self, name, value):
        assert (name, value) == ('versionAsOf', 0)
        return self
    def load(self, path):
        frame = self.tables[path]
        if path.endswith('/nodes') and self.corruption:
            row = list(frame.rows[0])
            if self.corruption == 'row': row[0] += '-changed'
            else:
                index = next(i for i, f in enumerate(frame.schema.fields)
                             if f.dataType.typeName() == 'long')
                row[index] = 1.0
            frame.rows[0] = tuple(row)
        return frame
    def sql(self, sql):
        path = sql.split('`')[1]
        self.metadata_reads.append((path, sql))
        role = 1 if path.endswith('/nodes') else 2
        value = {'version': 0} if sql.startswith('DESCRIBE HISTORY') else {
            'id': '00000000-0000-0000-0000-00000000000' + str(role),
            'location': 'file:' + path, 'format': 'delta'}
        return SimpleNamespace(select=lambda *names: SimpleNamespace(first=lambda: value),
                               first=lambda: value)


class Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.release, custody = fixtures.Tests().pair()
        (self.root / 'release.json').write_bytes(self.release.payload)
        (self.root / 'custody.json').write_bytes(custody)
        selection = LocalGraphExportSelection(self.root / 'release.json', self.root / 'custody.json',
            self.release.sha256, hashlib.sha256(custody).hexdigest())
        source = Path('examples/domain-packs/archaeology/upstream').resolve()
        self.args = dict(pack='archaeology', model=source / 'ontology.json',
            graph=source / 'graph/fixture.json', pack_file=source / 'pack.json', selection=selection,
            node_path=str(self.root / 'nodes'), edge_path=str(self.root / 'edges'))
        self.profile = prepare_scenarios(FinitePackDefinition('archaeology'),
            *(self.args[name].read_bytes() for name in ('model', 'graph', 'pack_file')),
            json.loads(self.release.payload))
        self.spark = Spark()

    def run_writer(self):
        from ashlar_host.fabric_pack_delta import materialize_fabric_pack_delta
        return materialize_fabric_pack_delta(self.spark, **self.args)

    def test_complete_typed_node_and_unchanged_edge_bags(self):
        result = self.run_writer()
        properties = scenario_properties(self.profile)
        frame = self.spark.tables[str(self.root / 'nodes')]
        self.assertEqual([(f.name, f.dataType.typeName()) for f in frame.schema.fields],
            [(p['name'], 'long' if p['type'] == 'INT64' else 'string') for p in properties])
        expected = [tuple(row[p['name']] for p in properties) for row in self.profile['nodes']]
        self.assertEqual(Counter(frame.rows), Counter(expected))
        edges = self.spark.tables[str(self.root / 'edges')]
        self.assertEqual(edges.names, list(release_columns('edge')))
        self.assertTrue(all(f.dataType.typeName() == 'string' for f in edges.schema.fields))
        self.assertEqual(Counter(edges.rows), Counter(tuple(row[n] for n in release_columns('edge'))
            for row in json.loads(self.release.payload)['edges']))
        for role, count in [('nodes', 39), ('edges', 46)]:
            self.assertIn(count + 1, self.spark.limits)
            self.assertEqual(result['targets'][role]['rows'], count)
            self.assertEqual(result['targets'][role]['version'], 0)
        self.assertGreaterEqual(len(self.spark.metadata_reads), 8)

    def test_changed_native_row_or_float_numeric_refuses(self):
        for corruption in ('row', 'float'):
            with self.subTest(corruption=corruption):
                self.spark = Spark()
                self.spark.corruption = corruption
                with self.assertRaises(ValueError): self.run_writer()
                self.assertIn(str(self.root / 'nodes'), self.spark.tables)

    def test_partial_edge_write_preserved_and_overlapping_path_refuses(self):
        self.spark.fail_edge = True
        with self.assertRaises(RuntimeError): self.run_writer()
        self.assertEqual(set(self.spark.tables), {str(self.root / 'nodes')})
        self.spark = Spark()
        self.args['node_path'] = str(self.root)
        with self.assertRaises(ValueError): self.run_writer()
        self.assertEqual(self.spark.tables, {})
