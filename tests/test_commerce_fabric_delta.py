"""Actual nominal publication owner with inert Delta storage; no native qualification."""
import json
import tempfile
import unittest
from pathlib import Path
import test_commerce_graphframes_installed as commerce_fixture
from test_fabric_pack_delta import Spark, Writer
from ashlar.graph_release import release_columns
from ashlar_host.commerce_fabric_delta import materialize_commerce_fabric_delta
from unittest.mock import patch


class Tests(unittest.TestCase):
    def setUp(self):
        fixture = commerce_fixture.ReaderTests()
        self.addCleanup(fixture.doCleanups)
        self.session, self.events = fixture.active_session()
        self.opened = self.session.opened
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.spark = Spark()

    def run_writer(self):
        return materialize_commerce_fabric_delta(self.spark, self.opened,
            node_path=str(self.root / 'nodes'), edge_path=str(self.root / 'edges'))

    def test_complete_carriers_and_authority_held_through_both_saves(self):
        save = Writer.save
        observations = []
        def observed(writer, path):
            observations.append((self.opened.provider.active, self.opened.driver.held, self.opened.driver.pin_held))
            return save(writer, path)
        with patch.object(Writer, 'save', observed):
            report = self.run_writer()
        self.assertEqual(observations, [(True, True, True)] * 2)
        self.assertFalse(self.opened.provider.active)
        self.assertEqual(self.events[-1], 'writer-close')
        self.assertIn('rollback', self.events)
        self.assertIn('close', self.events)
        for role, kind, count in [('nodes', 'node', 11), ('edges', 'edge', 10)]:
            frame = self.spark.tables[str(self.root / role)]
            self.assertEqual(frame.names, list(release_columns(kind)))
            self.assertTrue(all(field.dataType.typeName() == 'string' for field in frame.schema.fields))
            expected = json.loads(commerce_fixture.RELEASE)[role]
            self.assertEqual(sorted(frame.rows), sorted(tuple(row[name] for name in release_columns(kind)) for row in expected))
            self.assertEqual(report['targets'][role]['rows'], count)
            self.assertEqual(report['targets'][role]['version'], 0)
        self.assertEqual(report['publication'], self.opened.manifest)
        self.assertEqual(set(report['snapshots']), set(json.loads(self.opened.manifest['table_versions_json'])))

    def test_partial_edge_failure_retains_nodes_and_closes(self):
        self.spark.fail_edge = True
        with self.assertRaises(RuntimeError): self.run_writer()
        self.assertEqual(set(self.spark.tables), {str(self.root / 'nodes')})
        self.assertFalse(self.opened.provider.active)
        self.assertIn('close', self.events)

    def test_readback_drift_withholds_report(self):
        self.spark.corruption = 'row'
        with self.assertRaises(ValueError): self.run_writer()
        self.assertEqual(set(self.spark.tables), {str(self.root / 'nodes')})
        self.assertFalse(self.opened.provider.active)

    def test_late_source_failure_and_cancellation_preserve_partial_writes(self):
        save = Writer.save
        cancellation = KeyboardInterrupt('cancel')
        def observed(writer, path):
            save(writer, path)
            if path.endswith('/edges'):
                self.opened.driver.source_admit.side_effect = PermissionError('closing source')
                raise cancellation
        with patch.object(Writer, 'save', observed):
            with self.assertRaises(KeyboardInterrupt) as raised: self.run_writer()
        self.assertIs(raised.exception, cancellation)
        self.assertEqual(len(self.spark.tables), 2)
        self.assertFalse(self.opened.provider.active)
        self.assertIn('close', self.events)

    def test_closing_ack_failure_withholds_report(self):
        save = Writer.save
        def observed(writer, path):
            save(writer, path)
            if path.endswith('/edges'):
                self.opened.provider._ack = lambda session: (_ for _ in ()).throw(PermissionError('ACK revoked'))
        with patch.object(Writer, 'save', observed):
            with self.assertRaises(PermissionError): self.run_writer()
        self.assertEqual(len(self.spark.tables), 2)
        self.assertFalse(self.opened.provider.active)

    def test_final_native_identity_drift_withholds_report(self):
        original = self.spark.sql
        counts = {}
        def sql(statement):
            result = original(statement)
            if statement.startswith('DESCRIBE DETAIL'):
                path = statement.split('`')[1]
                counts[path] = counts.get(path, 0) + 1
                if counts[path] > 1:
                    from types import SimpleNamespace
                    changed = dict(result.first())
                    changed['id'] = '99999999-9999-4999-8999-999999999999'
                    return SimpleNamespace(select=lambda *names: SimpleNamespace(first=lambda: changed))
            return result
        self.spark.sql = sql
        with self.assertRaises(ValueError): self.run_writer()
        self.assertEqual(len(self.spark.tables), 2)
        self.assertFalse(self.opened.provider.active)

    def test_replacement_during_final_edge_readback_refuses(self):
        original_load, original_sql = self.spark.load, self.spark.sql
        edge_reads = []
        replaced = []
        def load(path):
            frame = original_load(path)
            if path.endswith('/edges'):
                edge_reads.append(path)
                if len(edge_reads) == 2:
                    replaced.append(True)
            return frame
        def sql(statement):
            result = original_sql(statement)
            if replaced and statement.startswith('DESCRIBE DETAIL') and '/nodes`' in statement:
                from types import SimpleNamespace
                changed = dict(result.first())
                changed['id'] = '99999999-9999-4999-8999-999999999999'
                return SimpleNamespace(select=lambda *names: SimpleNamespace(first=lambda: changed))
            return result
        self.spark.load, self.spark.sql = load, sql
        with self.assertRaises(ValueError): self.run_writer()
        self.assertEqual(len(edge_reads), 2)
        self.assertEqual(len(self.spark.tables), 2)
        self.assertFalse(self.opened.provider.active)

    def test_canonical_destination_collision_refuses_before_save(self):
        target = next(iter(self.opened.driver.transport.targets.values())).path
        with self.assertRaises(ValueError):
            materialize_commerce_fabric_delta(self.spark, self.opened,
                node_path=str(target), edge_path=str(self.root / 'edges'))
        self.assertEqual(self.spark.tables, {})
