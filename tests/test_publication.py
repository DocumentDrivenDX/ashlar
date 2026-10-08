import copy
import json
import unittest
from ashlar.publication import ResolutionError, Snapshot, resolve_publication

TABLE = 'catalog.graph.object_current'
OTHER = 'catalog.graph.adjacency_forward'

class FakeBackend:
    def __init__(self):
        self.rows = [dict(publication_id='p1', profile_version='ashlar-delta/0.3',
                         table_versions_json=json.dumps({TABLE: 6, OTHER: 2}),
                         schema_revisions_json='{"source":"r1"}',
                         source_progress_json='{"source":{"position":9007199254740993}}',
                         validation_report_json='{"exact":true,"unknown":{"decimal":1.234567890123456789}}')]
        self.calls = []
        self.uuid = 'trusted-uuid'
        self.version_delta = 0
        self.deny = False
        self.invalid_report = False
    def authorize(self, context, publication_id, tables):
        self.calls.append('authorize')
        if self.deny:
            raise PermissionError('Denied by trusted adapter')
    def descriptors(self, publication_id):
        self.calls.append('descriptors')
        return self.rows
    def validate_descriptor(self, descriptor, context):
        self.calls.append('validate')
        if self.invalid_report:
            raise ResolutionError('Invalid source/validation custody')
    def validate_snapshot(self, table, uuid, version, columns):
        self.calls.append(('retention', table, version))
    def inspect_snapshot(self, table, version):
        self.calls.append(('snapshot', table, version))
        return Snapshot(table, self.uuid, version + self.version_delta)

class ResolverTests(unittest.TestCase):
    def resolve(self, backend):
        return resolve_publication(backend, 'p1', {TABLE: 'trusted-uuid'}, context=object(),
                                   supported_profiles=['ashlar-delta/0.3'],
                                   supported_revisions={'source': ['r1']})
    def test_pins_and_exact_unknown_metadata(self):
        backend = FakeBackend()
        resolved = self.resolve(backend)
        self.assertEqual(resolved.snapshots[TABLE].version, 6)
        self.assertEqual(resolved.descriptor.source_progress['source']['position'], 9007199254740993)
        self.assertEqual(str(resolved.descriptor.validation_report['unknown']['decimal']), '1.234567890123456789')
        self.assertEqual(backend.calls, ['authorize', 'descriptors', 'validate', ('snapshot', TABLE, 6)])
        backend.rows[0]['table_versions_json'] = '{"changed":99}'
        self.assertEqual(resolved.descriptor.versions[TABLE], 6)
        with self.assertRaises(TypeError):
            resolved.descriptor.versions[TABLE] = 99
        with self.assertRaises(TypeError):
            resolved.snapshots[TABLE] = Snapshot(TABLE, 'wrong', 9)
    def test_denial_precedes_manifest_read(self):
        backend = FakeBackend(); backend.deny = True
        with self.assertRaises(PermissionError): self.resolve(backend)
        self.assertEqual(backend.calls, ['authorize'])
    def test_descriptor_refusals_before_native_inspection(self):
        mutations = [
            ('absent', lambda b: b.rows.clear()),
            ('duplicate descriptors', lambda b: b.rows.append(copy.deepcopy(b.rows[0]))),
            ('identity', lambda b: b.rows[0].update(publication_id='other')),
            ('profile', lambda b: b.rows[0].update(profile_version='future')),
            ('duplicate JSON keys', lambda b: b.rows[0].update(table_versions_json='{"x":0,"x":1}')),
            ('nested duplicate', lambda b: b.rows[0].update(validation_report_json='{"x":{"a":1,"a":2}}')),
            ('bool version', lambda b: b.rows[0].update(table_versions_json=json.dumps({TABLE: True}))),
            ('negative', lambda b: b.rows[0].update(table_versions_json=json.dumps({TABLE: -1}))),
            ('overflow', lambda b: b.rows[0].update(table_versions_json=json.dumps({TABLE: 2**63}))),
            ('missing consumed', lambda b: b.rows[0].update(table_versions_json=json.dumps({OTHER: 2}))),
            ('invalid unconsumed', lambda b: b.rows[0].update(table_versions_json=json.dumps({TABLE: 6, 'bad;name': 1}))),
            ('revision', lambda b: b.rows[0].update(schema_revisions_json='{"source":"unknown"}')),
            ('empty revisions', lambda b: b.rows[0].update(schema_revisions_json='{}')),
            ('nonfinite', lambda b: b.rows[0].update(validation_report_json='{"n":NaN}')),
            ('bad source JSON', lambda b: b.rows[0].update(source_progress_json='{bad')),
            ('native custody refusal', lambda b: setattr(b, 'invalid_report', True)),
        ]
        for label, mutate in mutations:
            with self.subTest(label=label):
                backend = FakeBackend(); mutate(backend)
                with self.assertRaises(ResolutionError): self.resolve(backend)
                self.assertFalse(any(isinstance(call, tuple) for call in backend.calls))
    def test_snapshot_identity_and_version_refusals(self):
        for field, value in [('uuid', 'replacement-uuid'), ('version_delta', 1)]:
            with self.subTest(field=field):
                backend = FakeBackend(); setattr(backend, field, value)
                with self.assertRaises(ResolutionError): self.resolve(backend)



class NativeBackendTests(unittest.TestCase):
    def test_pinned_probe_and_schema_refusal(self):
        from ashlar.native import NativeBackend, SQLResult
        class Executor:
            def __init__(self): self.queries = []; self.columns = (('id', 'BIGINT'),)
            def query(self, sql, parameters):
                self.queries.append((sql, parameters))
                if sql.startswith('DESCRIBE'): return SQLResult([{'id': 'trusted-uuid'}])
                return SQLResult([], self.columns)
        executor = Executor()
        backend = NativeBackend(executor, FakeBackend(), 'catalog.graph.publication_manifest',
                                'trusted-uuid', {TABLE: [('id', 'BIGINT')]})
        self.assertEqual(backend.inspect_snapshot(TABLE, 6), Snapshot(TABLE, 'trusted-uuid', 6))
        self.assertIn('VERSION AS OF 6 LIMIT 0', executor.queries[1][0])
        executor.columns = (('id', 'STRING'),)
        with self.assertRaises(ResolutionError): backend.inspect_snapshot(TABLE, 6)
        with self.assertRaises(ResolutionError): backend.inspect_snapshot(TABLE, True)

class NativeCustodyTests(unittest.TestCase):
    def test_manifest_bound_lookup_and_replacement_refusal(self):
        from ashlar.native import NativeBackend, SQLResult
        class Executor:
            def __init__(self): self.detail_calls = 0; self.replace = False; self.parameters = None
            def query(self, sql, parameters):
                if sql.startswith('DESCRIBE'):
                    self.detail_calls += 1
                    return SQLResult([{'id': 'changed' if self.replace and self.detail_calls % 2 == 0 else 'manifest-uuid'}])
                self.parameters = parameters
                return SQLResult(FakeBackend().rows)
        executor = Executor()
        backend = NativeBackend(executor, FakeBackend(), 'catalog.graph.publication_manifest', 'manifest-uuid', {})
        self.assertEqual(len(backend.descriptors('p1')), 1)
        self.assertEqual(executor.parameters, {'publication_id': 'p1'})
        executor.replace = True
        with self.assertRaises(ResolutionError): backend.descriptors('p1')
    def test_nonconforming_authorization_provider_refused(self):
        backend = FakeBackend()
        backend.authorize = lambda *args: False
        with self.assertRaises(ResolutionError): ResolverTests().resolve(backend)
        self.assertEqual(backend.calls, [])

if __name__ == '__main__': unittest.main()
