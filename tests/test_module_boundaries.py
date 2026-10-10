import json
import tempfile
import unittest
from pathlib import Path
from check_module_boundaries import check, scan, ROOT, POLICY


class BoundaryTests(unittest.TestCase):
    def tree(self, text):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        (root / 'src/ashlar').mkdir(parents=True)
        (root / 'tools').mkdir()
        (root / 'src/ashlar/example.py').write_text(text)
        return root

    def policy(self):
        return {'format': 'ashlar-boundary-baseline/0.1', 'limitations': [], 'baseline': []}

    def test_real_allowed_and_forbidden_edges(self):
        root = self.tree('from .publication import Descriptor\n')
        self.assertEqual(check(root, self.policy()), ([], []))
        (root / 'src/ashlar/example.py').write_text('from tools.host import Client\nfrom databricks.sdk import WorkspaceClient\nfrom .publication import _freeze\n')
        new, stale = check(root, self.policy())
        self.assertEqual({x[3] for x in new}, {'core-to-tools', 'core-to-sdk', 'private-cross-module'})
        self.assertFalse(stale)

    def test_exact_baseline_no_blanket_or_new_symbol(self):
        root = self.tree('from .publication import _freeze\n')
        edge = scan(root)[0]
        policy = self.policy()
        policy['baseline'] = [dict(zip(('source', 'target', 'symbol', 'reason'), edge), owner='resolver owner', removal_trigger='public codec extraction')]
        self.assertEqual(check(root, policy), ([], []))
        (root / 'src/ashlar/example.py').write_text('from .publication import _decode\n')
        new, stale = check(root, policy)
        self.assertEqual(new[0][2], '_decode')
        self.assertEqual(stale[0][2], '_freeze')
        policy['baseline'][0]['source'] = '*'
        self.assertTrue(check(root, policy)[0])

    def test_recursive_role_relative_and_alias_checks(self):
        root = self.tree('from .cli import main\n')
        self.assertEqual(check(root, self.policy())[0][0][3], 'core-to-composition')
        nested = root / 'src/ashlar/nested'
        nested.mkdir()
        (nested / 'probe.py').write_text('from tools.host import Client\nfrom ..publication import _decode\n')
        (root / 'tools/nested.py').write_text('from ashlar.nested import probe\nprobe._private(None)\n')
        (root / 'tools/probe.py').write_text('from ashlar import publication\npublication._freeze(None)\n')
        edges = scan(root)
        self.assertTrue(any(e[0].endswith('nested/probe.py') and e[3] == 'core-to-tools' for e in edges))
        self.assertTrue(any(e[1:3] == ('ashlar.publication', '_decode') for e in edges))
        self.assertTrue(any(e[1:3] == ('ashlar.nested.probe', '_private') for e in edges))
        self.assertTrue(any(e[0] == 'tools/probe.py' and e[2] == '_freeze' for e in edges))

    def test_model_runtime_and_consumer_producer_directions(self):
        root = self.tree('')
        (root / 'src/ashlar/schema.py').write_text('from .native import Executor\n')
        (root / 'src/ashlar/weft_query.py').write_text('from .publisher import Attempt\n')
        self.assertEqual({e[3] for e in scan(root)}, {'portable-to-runtime', 'consumer-to-producer'})

    def test_package_members_qualified_chain_and_native_libraries(self):
        root = self.tree('from . import cli\nfrom ashlar import cli\nimport ashlar.publication\nashlar.publication._decode(None)\nimport duckdb\nimport pyarrow\nfrom ashlar import _hidden\n')
        (root / 'src/ashlar/cli.py').write_text('')
        edges = scan(root)
        self.assertTrue(any(e[1] == 'ashlar.cli' and e[3] == 'core-to-composition' for e in edges))
        self.assertTrue(any(e[1:3] == ('ashlar.publication', '_decode') for e in edges))
        self.assertEqual({e[1] for e in edges if e[3] == 'core-to-sdk'}, {'duckdb', 'pyarrow'})
        self.assertTrue(any(e[1:3] == ('ashlar', '_hidden') and e[3] == 'private-cross-module' for e in edges))
        (root / 'src/ashlar/__init__.py').write_text('from ashlar import _self\n')
        self.assertFalse(any(e[0].endswith('__init__.py') for e in scan(root)))

    def test_compiler_installer_and_composition_roles(self):
        root = self.tree('')
        (root / 'src/ashlar/weft_installation.py').write_text('from .publisher import Attempt\nfrom .weft_distribution import configuration\n')
        (root / 'src/ashlar/weft_distribution.py').write_text('from .weft_installation import InstallationConfig\n')
        edges = scan(root)
        self.assertEqual({e[3] for e in edges}, {'consumer-to-producer', 'core-to-composition'})
        self.assertFalse(any(e[0].endswith('weft_distribution.py') for e in edges))

    def test_path_decoder_role_has_real_allow_and_deny_controls(self):
        root = self.tree('')
        path = root / 'src/ashlar/weft_path_decode.py'
        path.write_text('from .weft_decode import decode_exact_scalar\n')
        self.assertEqual(check(root, self.policy()), ([], []))
        for statement, reason in [('from .publisher import Attempt', 'consumer-to-producer'),
                                  ('from .cli import main', 'core-to-composition'),
                                  ('import pyspark', 'core-to-sdk'),
                                  ('from .weft_decode import _hidden', 'private-cross-module')]:
            path.write_text(statement + '\n')
            self.assertIn(reason, {e[3] for e in scan(root)})

    def test_paths_package_and_installer_have_consumer_boundaries(self):
        for name in ('weft_paths_package.py', 'weft_paths_installation.py'):
            with self.subTest(module=name):
                root = self.tree('')
                path = root / 'src/ashlar' / name
                path.write_text('from .weft_decode import decode_exact_scalar\n')
                self.assertEqual(check(root, self.policy()), ([], []))
                for statement, reason in [('from .publisher import Attempt', 'consumer-to-producer'),
                                          ('from .cli import main', 'core-to-composition'),
                                          ('import pyspark', 'core-to-sdk'),
                                          ('from ashlar_host.commerce import query_commerce', 'core-to-host-composition'),
                                          ('from .weft_decode import _hidden', 'private-cross-module')]:
                    path.write_text(statement + '\n')
                    self.assertIn(reason, {e[3] for e in scan(root)})

    def test_paths_composition_is_an_exact_host_role(self):
        root = self.tree('')
        (root / 'src/ashlar/weft_paths_distribution.py').write_text('from .weft_paths_installation import PathsInstallationConfig\n')
        self.assertEqual(check(root, self.policy()), ([], []))
        for name in ('weft_paths_package.py', 'weft_paths_installation.py', 'schema.py'):
            (root / 'src/ashlar' / name).write_text('from .weft_paths_distribution import open_paths_distribution\n')
        self.assertEqual({e[3] for e in scan(root)}, {'portable-to-runtime', 'core-to-composition'})

    def test_installed_host_has_owned_public_edges(self):
        root = self.tree('from ashlar_host.commerce import publish_commerce\n')
        host = root / 'src/ashlar_host'
        host.mkdir()
        (host / 'commerce.py').write_text('from .connection import open_connection\nfrom ashlar.publication import validate_table_identifier\n')
        (host / 'connection.py').write_text('')
        self.assertEqual({e[3] for e in scan(root)}, {'core-to-host-composition'})
        (root / 'src/ashlar/example.py').write_text('')
        (root / 'src/ashlar/cli.py').write_text('from ashlar_host.commerce import publish_commerce\n')
        self.assertEqual(check(root, self.policy()), ([], []))
        nested = root / 'src/ashlar/nested'
        nested.mkdir()
        (nested / 'cli.py').write_text('from ashlar_host.commerce import publish_commerce\n')
        self.assertEqual({e[3] for e in scan(root)}, {'core-to-host-composition'})
        (nested / 'cli.py').unlink()
        (host / 'commerce.py').write_text('from tools.runtime import execute\nfrom .connection import _hidden\nfrom ashlar.schema import _json\n')
        self.assertEqual({e[3] for e in scan(root)}, {'host-to-checkout-tools', 'private-cross-module'})

    def test_actual_repository_policy(self):
        self.assertEqual(check(ROOT, json.loads(POLICY.read_text())), ([], []))

    def test_policy_refuses_duplicate_unowned_and_unknown(self):
        root = self.tree('')
        policy = self.policy()
        policy['extra'] = True
        with self.assertRaises(ValueError):
            check(root, policy)
