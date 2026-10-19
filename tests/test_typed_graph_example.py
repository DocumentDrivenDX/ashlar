# SPDX-License-Identifier: MIT OR Apache-2.0
"""Authored graph fixture byte/topology checks; no public UMF or native proof."""
import hashlib
import json
import unittest
from pathlib import Path
from ashlar.source import jsonl_batches
from ashlar.whole_entity import changes_from_batch

ROOT = Path(__file__).resolve().parents[1] / 'examples/end-to-end'


class TypedGraphExampleTests(unittest.TestCase):
    def test_exact_refs_transaction_controls_and_declared_graph_cases(self):
        config = json.loads((ROOT / 'configured-typed-graph-source.json').read_bytes())
        for name in ('schema', 'bindings', 'source'):
            raw = (ROOT / config[name]['path']).read_bytes()
            self.assertEqual(hashlib.sha256(raw).hexdigest(), config[name]['sha256'])
        raw = (ROOT / config['source']['path']).read_bytes()
        batches = tuple(jsonl_batches(raw.splitlines(keepends=True), feed=config['feed'], epoch=config['epoch']))
        self.assertEqual([len(b.records) for b in batches], [8, 2, 3])
        self.assertEqual(int(batches[-1].cursor_after), len(raw))
        groups = [changes_from_batch(b) for b in batches]
        for group in groups:
            self.assertEqual(len({c.state.key for c in group}), len(group))
        first = groups[0]
        objects = [c for c in first if c.state.key.kind == 'object']
        edges = [c for c in first if c.state.key.kind == 'edge']
        self.assertEqual(len(objects), 4)
        self.assertEqual({c.state.key.type_id for c in objects}, {17, 18})
        self.assertEqual(edges[0].state.endpoints, edges[1].state.endpoints)
        self.assertNotEqual(edges[0].state.key, edges[1].state.key)
        self.assertEqual(edges[2].state.endpoints[0], edges[2].state.endpoints[1])
        self.assertEqual(tuple(k.type_id for k in edges[3].state.endpoints), (17, 18))
        self.assertTrue(all(c.state.props_json == '{}' for g in groups for c in g if c.state.key.kind == 'edge'))
        isolate = objects[2].state.key
        self.assertTrue(all(isolate not in c.state.endpoints for c in edges))
        self.assertEqual(groups[1][0].state.endpoints, tuple(reversed(edges[0].state.endpoints)))
        # Every delete carries its exact prior payload, with only the version advancing.
        prior = {c.state.key: c.state for c in first}
        for group in groups[1:]:
            for change in group:
                old = prior[change.state.key]
                if change.operation == 'delete':
                    self.assertEqual((change.state.props_json, change.state.retained_json, change.state.endpoints),
                                     (old.props_json, old.retained_json, old.endpoints))
                    del prior[change.state.key]
                else:
                    prior[change.state.key] = change.state
        self.assertEqual(sum(k.kind == 'object' for k in prior), 3)
        self.assertEqual(sum(k.kind == 'edge' for k in prior), 1)
        self.assertIn(isolate, prior)
