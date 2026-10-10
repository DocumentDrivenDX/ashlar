"""Inert ownership controls; no actual PuppyGraph activation qualification."""
from contextlib import contextmanager
from pathlib import Path
import hashlib
import json
import unittest
from ashlar.graph_release import GraphRelease
from ashlar_host.puppy_release import (PuppyReleaseOwner, PuppyReleaseHandle,
    PuppyReleaseActivation, PuppyReleaseError)


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()


class Adapter:
    def __init__(self):
        self.native = {}
        self.fail = None
        self.mutate = None
        self.stages = 0

    def stage(self, handle, context):
        self.stages += 1
        schema = {'catalog': [{'name': 'immutable-test-carrier'}],
            'node': [{'label': handle.node_label}], 'edge': [{'label': handle.edge_label,
                'fromNodeLabel': handle.node_label, 'toNodeLabel': handle.node_label}]}
        schema_json = encoded(schema).decode()
        activation = PuppyReleaseActivation(handle.release.sha256, 'inert-engine-instance',
            hashlib.sha256(schema_json.encode()).hexdigest(), 'c' * 64)
        value = json.loads(handle.release.payload)
        actual = {'engine': 'PuppyGraph', 'version': 'inert', 'engine_id': activation.engine_id,
            'schema_json': schema_json, 'carrier_sha256': activation.carrier_sha256,
            'catalog_visibility': activation.catalog_visibility, 'node_label': handle.node_label, 'edge_label': handle.edge_label, 'nodes': [], 'edges': []}
        for role, label in [('nodes', handle.node_label), ('edges', handle.edge_label)]:
            for row in value[role]:
                item = {'row': dict(row), 'native_id': label + '[' + row['graph_id'] + ']'}
                if role == 'edges':
                    item.update(native_source=handle.node_label + '[' + row['src'] + ']',
                        native_target=handle.node_label + '[' + row['dst'] + ']')
                actual[role].append(item)
        self.native[handle.release.sha256] = actual
        if self.fail is not None:
            actual['edges'] = []  # interrupted external staging is observable
            raise self.fail
        return activation

    def observe(self, handle, context):
        actual = self.native[handle.release.sha256]
        if self.mutate is not None:
            self.mutate(actual)
        return encoded(actual)


class Policy:
    def __init__(self):
        self.close_error = None
        self.refuse = False
        self.held = False

    @contextmanager
    def writer(self, context):
        if self.held:
            raise PermissionError('No overlapping writer')
        self.held = True
        try:
            yield
            if self.close_error:
                raise self.close_error
        finally:
            self.held = False

    def admit(self, handle, context):
        if not self.held or self.refuse:
            raise PermissionError('Inert original release policy refusal')


class Tests(unittest.TestCase):
    def setUp(self):
        root = Path(__file__).resolve().parents[1]
        folder = root / 'docs/helix/04-build/evidence/puppygraph-releases-20261009'
        self.releases = tuple(GraphRelease((folder / (digest + '.graph.json')).read_bytes(), digest)
            for digest in ['ca4b40f78b1a27e3d6df50eb17816ec49e30f0f70d6ff7dabc195307c07642e7',
                'a6a4cb268f6bb3f8d193b35289d5b593ad08e33d8fc561e29487919fd1502e3a'])
        self.adapter = Adapter(); self.policy = Policy()
        self.owner = PuppyReleaseOwner(self.adapter, self.policy); self.context = object()

    def test_complete_successor_and_explicit_old_handle_reopening(self):
        old = self.owner.refresh(self.releases[0], context=self.context)
        before = self.owner.read(old, context=self.context)
        new = self.owner.refresh(self.releases[1], context=self.context)
        self.assertEqual(self.owner.current(context=self.context), new)
        self.assertEqual(self.owner.read(old, context=self.context), before)
        reopened = PuppyReleaseOwner(self.adapter, self.policy)
        self.assertEqual(reopened.read(PuppyReleaseHandle(old.release, old.activation), context=self.context), before)
        with self.assertRaises(PuppyReleaseError): reopened.current(context=self.context)

    def test_failed_and_cancelled_partial_successor_never_selected(self):
        old = self.owner.refresh(self.releases[0], context=self.context)
        before = self.owner.read(old, context=self.context)
        for error in (RuntimeError('upload failure'), KeyboardInterrupt('interrupted'),
                SystemExit('interrupted'), GeneratorExit('interrupted')):
            self.adapter.fail = error
            try: self.owner.refresh(self.releases[1], context=self.context)
            except BaseException as caught: self.assertIs(caught, error)
            else: self.fail('Interrupted stage exposed success')
            self.assertEqual(self.owner.current(context=self.context), old)
            self.assertEqual(self.owner.read(old, context=self.context), before)
            self.assertFalse(self.policy.held)
        self.adapter.fail = None
        self.owner.refresh(self.releases[1], context=self.context)

    def test_changed_old_or_mixed_endpoints_refuse(self):
        old = self.owner.refresh(self.releases[0], context=self.context)
        self.adapter.native[old.release.sha256]['nodes'][0]['row']['props_json'] = 'changed'
        with self.assertRaises(PuppyReleaseError): self.owner.current(context=self.context)
        with self.assertRaises(PuppyReleaseError): self.owner.refresh(self.releases[1], context=self.context)
        self.assertEqual(self.adapter.stages, 1)
        self.adapter = Adapter(); self.owner = PuppyReleaseOwner(self.adapter, self.policy)
        old = self.owner.refresh(self.releases[0], context=self.context)
        self.adapter.native[old.release.sha256]['edges'][0]['native_target'] = 'foreign-release'
        with self.assertRaises(PuppyReleaseError): self.owner.read(old, context=self.context)

    def test_closing_failure_withholds_new_handle_and_policy_is_mandatory(self):
        old = self.owner.refresh(self.releases[0], context=self.context)
        error = RuntimeError('writer closing refusal'); self.policy.close_error = error
        with self.assertRaises(RuntimeError) as caught: self.owner.refresh(self.releases[1], context=self.context)
        self.assertIs(caught.exception, error)
        self.policy.close_error = None
        self.assertEqual(self.owner.current(context=self.context), old)
        self.policy.refuse = True
        with self.assertRaises(PermissionError): self.owner.read(old, context=self.context)

    def test_engine_schema_carrier_identity_and_complete_values(self):
        old = self.owner.refresh(self.releases[0], context=self.context)
        for field, replacement in [('engine_id', 'new-instance'), ('schema_json', '{}'),
                ('carrier_sha256', 'd' * 64), ('version', ''), ('nodes', [])]:
            actual = self.adapter.native[old.release.sha256]; saved = actual[field]; actual[field] = replacement
            with self.assertRaises(PuppyReleaseError): self.owner.read(old, context=self.context)
            actual[field] = saved

    def test_unadmitted_handle_and_changed_original_bytes_refuse(self):
        with self.assertRaises(PuppyReleaseError): self.owner.read(PuppyReleaseHandle(self.releases[0]), context=self.context)
        release = self.releases[0]
        with self.assertRaises(PuppyReleaseError): PuppyReleaseHandle(GraphRelease(release.payload + b' ', release.sha256))
        with self.assertRaises(PuppyReleaseError): PuppyReleaseActivation(release.sha256, 'engine', 'x' * 64, 'c' * 64)


if __name__ == '__main__': unittest.main()
