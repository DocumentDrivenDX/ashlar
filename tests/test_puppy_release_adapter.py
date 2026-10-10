"""Public adapter wire-shape controls with inert HTTP/Cypher/runtime ports."""
from contextlib import contextmanager
from pathlib import Path
import hashlib
import json
import tempfile
import unittest
from ashlar.graph_release import GraphRelease
from ashlar_host.puppy_release import PuppyReleaseOwner, PuppyReleaseHandle, PuppyReleaseError
from puppy_release_adapter import PuppyNativeReleaseAdapter, encoded, original_model
from run_graph_release_graphframes import columns


class Policy:
    @contextmanager
    def writer(self, context): yield
    def admit(self, handle, context): pass  # inert only; not native authority


class Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name).resolve()
        root = Path(__file__).resolve().parents[1]
        folder = root / 'docs/helix/04-build/evidence/puppygraph-releases-20261009'
        self.releases = [GraphRelease(p.read_bytes(), p.name.split('.')[0]) for p in sorted(folder.glob('*.graph.json'))]
        self.prepared = {}; self.values = {}; self.schema = {'catalog': [], 'node': [], 'edge': []}
        self.calls = []; self.fail_upload = False; self.omit_catalog = False; self.runtime_calls = 0; self.drift = False; self.interrupt_upload = None
        for release in self.releases:
            handle = PuppyReleaseHandle(release); prefix = release.sha256
            database = self.directory / (prefix + '.duckdb'); database.write_bytes(b'inert-file-' + prefix.encode())
            model = {'catalog': [{'name': 'catalog_' + prefix}],
                'node': [{'label': handle.node_label}], 'edge': [{'label': handle.edge_label,
                'fromNodeLabel': handle.node_label, 'toNodeLabel': handle.node_label}]}
            modelpath = self.directory / (prefix + '.model.json'); modelpath.write_bytes(encoded(model))
            receiptpath = self.directory / (prefix + '.receipt.json')
            receiptpath.write_bytes(encoded({'release_sha256': prefix,
                'model_sha256': hashlib.sha256(modelpath.read_bytes()).hexdigest(),
                'database_sha256': hashlib.sha256(database.read_bytes()).hexdigest()}))
            self.prepared[prefix] = (database, modelpath, receiptpath, hashlib.sha256(receiptpath.read_bytes()).hexdigest())
            self.values[handle.node_label] = self.values[handle.edge_label] = json.loads(release.payload)
        self.adapter = PuppyNativeReleaseAdapter(prepared=self.prepared, prior_model=encoded(self.schema),
            http=self.http, cypher=self.query, runtime=self.runtime, evidence=self.directory / 'evidence')
        self.owner = PuppyReleaseOwner(self.adapter, Policy()); self.context = object()

    def http(self, path, data, context):
        self.calls.append((path, data))
        if path == '/schemajson':
            return encoded({k: v for k, v in self.schema.items() if not (self.omit_catalog and k == 'catalog')})
        self.assertEqual(path, '/schema?postUploadBehavior=none')
        if self.fail_upload: raise RuntimeError('ordinary HTTP upload failure')
        intents = sorted(self.adapter.evidence.glob('*-request.json'))
        self.assertTrue(intents)
        self.assertEqual(intents[-1].read_bytes(), data)
        self.schema = json.loads(data)
        if self.interrupt_upload is not None:
            error = self.interrupt_upload; self.interrupt_upload = None
            self.schema['edge'] = self.schema['edge'][:-1]
            raise error
        return encoded({'ok': True, 'version': len(self.schema['node'])})

    def runtime(self, context):
        self.runtime_calls += 1
        return ('changed' if self.drift and self.runtime_calls % 2 == 0 else 'inert-instance', 'inert-version')

    def query(self, query, context):
        self.assertNotIn(';', query)
        label = next(label for label in self.values if ':' + label in query)
        kind = 'node' if label.endswith('Node') else 'edge'; role = 'nodes' if kind == 'node' else 'edges'
        node = label if kind == 'node' else label[:-4] + 'Node'
        result = []
        for row in self.values[label][role]:
            actual = {name: row[name] for name in columns(kind)}
            actual['native_id'] = label + '[' + row['graph_id'] + ']'
            if kind == 'edge':
                actual.update(native_source=node + '[' + row['src'] + ']', native_target=node + '[' + row['dst'] + ']')
            result.append(actual)
        return result

    def test_actual_wire_shapes_and_retained_old_after_successor(self):
        old = self.owner.refresh(self.releases[0], context=self.context)
        before = self.owner.read(old, context=self.context)
        new = self.owner.refresh(self.releases[1], context=self.context)
        self.assertEqual(self.owner.current(context=self.context), new)
        self.assertEqual(self.owner.read(old, context=self.context), before)
        uploads = [data for path, data in self.calls if data is not None]
        self.assertEqual([len(json.loads(data)['node']) for data in uploads], [1, 2])
        self.assertTrue(any(p.name.endswith('request.json') for p in self.adapter.evidence.iterdir()))

    def test_unconfigured_native_first_install_retains_raw_shape(self):
        self.schema = {}
        handle = self.owner.refresh(self.releases[0], context=self.context)
        self.assertIsNotNone(handle.activation)
        before = list(self.adapter.evidence.glob('*-before.json'))
        self.assertEqual(before[0].read_bytes(), b'{}')
        self.assertEqual(len([data for path, data in self.calls if data is not None]), 1)

    def test_unconfigured_native_cannot_replace_retained_prior(self):
        self.owner.refresh(self.releases[0], context=self.context)
        count = len([data for path, data in self.calls if data is not None])
        self.schema = {}
        with self.assertRaises(PuppyReleaseError):
            self.owner.refresh(self.releases[1], context=self.context)
        self.assertEqual(len([data for path, data in self.calls if data is not None]), count)

    def test_short_catalog_collision_cannot_replace_original_carrier(self):
        name = 'ashlar_' + 'a' * 24
        prior = {'catalog': [{'name': name, 'type': 'duckdb',
            'jdbc': {'jdbcUri': 'jdbc:duckdb:/tmp/ashlar_' + 'a' * 64 + '.duckdb'}}],
            'node': [], 'edge': []}
        addition = json.loads(encoded(prior))
        addition['catalog'][0]['jdbc']['jdbcUri'] = 'jdbc:duckdb:/tmp/ashlar_' + 'a' * 24 + 'b' * 40 + '.duckdb'
        with self.assertRaises(PuppyReleaseError): original_model(prior, addition)
        self.assertEqual(original_model(prior, prior), prior)

    def test_native_catalog_omission_is_reported_not_fabricated(self):
        self.omit_catalog = True
        handle = self.owner.refresh(self.releases[0], context=self.context)
        actual = json.loads(self.owner.read(handle, context=self.context))
        self.assertEqual(handle.activation.catalog_visibility, 'omitted')
        self.assertEqual(actual['catalog_visibility'], 'omitted')
        self.assertNotIn('catalog', json.loads(actual['schema_json']))
        requests = [json.loads(data) for _, data in self.calls if data is not None]
        self.assertEqual(len(requests[0]['catalog']), 1)  # declared, not observed

    def test_failed_upload_preserves_old_native_read(self):
        old = self.owner.refresh(self.releases[0], context=self.context)
        before = self.owner.read(old, context=self.context); self.fail_upload = True
        with self.assertRaises(RuntimeError): self.owner.refresh(self.releases[1], context=self.context)
        self.assertEqual(self.owner.current(context=self.context), old)
        self.assertEqual(self.owner.read(old, context=self.context), before)

    def test_closing_runtime_drift_and_carrier_tamper_refuse(self):
        old = self.owner.refresh(self.releases[0], context=self.context)
        self.drift = True
        with self.assertRaises(PuppyReleaseError): self.owner.read(old, context=self.context)
        self.drift = False
        self.prepared[old.release.sha256][0].write_bytes(b'replacement')
        with self.assertRaises(PuppyReleaseError): self.owner.read(old, context=self.context)

    def test_interrupted_original_subset_resumes_exact_intent_and_old_stays_readable(self):
        old = self.owner.refresh(self.releases[0], context=self.context)
        before = self.owner.read(old, context=self.context)
        cancellation = KeyboardInterrupt('Interrupted original upload response')
        self.interrupt_upload = cancellation
        try: self.owner.refresh(self.releases[1], context=self.context)
        except BaseException as error: self.assertIs(error, cancellation)
        else: self.fail('Interrupted original staging exposed success')
        self.assertEqual(self.owner.current(context=self.context), old)
        self.assertEqual(self.owner.read(old, context=self.context), before)
        resumed = self.owner.refresh(self.releases[1], context=self.context)
        self.assertEqual(self.owner.current(context=self.context), resumed)
        self.assertEqual(self.owner.read(old, context=self.context), before)
        intents = sorted(self.adapter.evidence.glob('*-request.json'))
        self.assertEqual(intents[-2].read_bytes(), intents[-1].read_bytes())
        count = len([data for _, data in self.calls if data is not None])
        self.assertEqual(self.owner.refresh(self.releases[1], context=self.context), resumed)
        self.assertEqual(len([data for _, data in self.calls if data is not None]), count)

    def test_unknown_or_changed_staged_subset_refuses_without_mutation(self):
        old = self.owner.refresh(self.releases[0], context=self.context)
        new_handle = PuppyReleaseHandle(self.releases[1])
        self.schema['node'].append({'label': new_handle.node_label, 'foreign': 'changed-original'})
        count = len([data for _, data in self.calls if data is not None])
        with self.assertRaises(PuppyReleaseError): self.owner.refresh(self.releases[1], context=self.context)
        self.assertEqual(len([data for _, data in self.calls if data is not None]), count)
        self.schema['node'][-1] = {'label': 'UnknownStagedNeighbor'}
        with self.assertRaises(PuppyReleaseError): self.owner.refresh(self.releases[1], context=self.context)
        self.assertEqual(self.owner.current(context=self.context), old)

    def test_prior_native_mapping_change_refuses_before_upload(self):
        old = self.owner.refresh(self.releases[0], context=self.context)
        self.schema['node'][0]['label'] = 'replaced'
        previous = len([data for _, data in self.calls if data is not None])
        with self.assertRaises(PuppyReleaseError): self.owner.refresh(self.releases[1], context=self.context)
        self.assertEqual(len([data for _, data in self.calls if data is not None]), previous)
        with self.assertRaises(PuppyReleaseError): self.owner.read(old, context=self.context)


if __name__ == '__main__': unittest.main()
