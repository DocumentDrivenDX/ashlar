# SPDX-License-Identifier: MIT OR Apache-2.0
"""Closed graph configuration controls; no producer or native execution."""
import json,tempfile,unittest
from pathlib import Path
from test_typed_graph_source import fixture,dump,sha
from ashlar.typed_graph_source_config import load_typed_graph_jsonl_configuration

class Tests(unittest.TestCase):
    def test_closed_profile_bindings_and_reference_custody(self):
        for mutation in ('producer','unknown','family','lifecycle'):
            with self.subTest(mutation=mutation),tempfile.TemporaryDirectory()as t:
                root=Path(t).resolve();c=fixture(root);v=json.loads(c.original)
                if mutation=='producer':v['producerRevision']='c45c72a2a8a3c4fba61c40c5927dd9091acf8cc3'
                elif mutation=='unknown':v['accepted']=True
                else:
                    name='bindings'if mutation=='family'else'schema';p=root/name;body=json.loads(p.read_bytes())
                    if mutation=='family':body['records']['18']['fields']['21'][2]='string'
                    else:body['modules'][0]['relationships'][0]['targetLifecycle']='dependent'
                    raw=dump(body);p.write_bytes(raw);v[name]['sha256']=sha(raw)
                c.path.write_bytes(dump(v))
                with self.assertRaises(ValueError):load_typed_graph_jsonl_configuration(c.path)
        with tempfile.TemporaryDirectory()as t:
            c=fixture(Path(t).resolve());c.verify()
            with self.assertRaises(TypeError):c.relationships['19']['source_type_id']=18
            self.assertIsNone(c.policy);self.assertEqual(c.objecttype_ids,(17,18))
