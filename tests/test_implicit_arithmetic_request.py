# SPDX-License-Identifier: MIT OR Apache-2.0
"""Original four queries select 042 without changing binding or module bytes."""
import copy
from pathlib import Path
import unittest
from ashlar_host.commerce_original_scenarios import CASES
from ashlar_host.commerce_path_request import commerce_path_request
from ashlar_host.implicit_arithmetic_request import commerce_implicit_arithmetic_request
from test_host_paths_query import metadata, BINDINGS

ROOT = Path(__file__).resolve().parents[1]
PACK = (ROOT/'examples/domain-packs/commerce/upstream/pack.json').read_bytes()
MODEL = (ROOT/'src/ashlar_host/resources/ontology.json').read_bytes()


class Tests(unittest.TestCase):
    def test_complete_original_queries_and_metadata_preserved(self):
        manifest, registry, aliases = metadata()
        before = copy.deepcopy((BINDINGS, manifest, registry, aliases))
        for _, sql in CASES:
            original = commerce_path_request(sql, MODEL, BINDINGS, manifest,
                                              registry, aliases, profile='paths-keys')
            actual = commerce_implicit_arithmetic_request(sql, MODEL, BINDINGS,
                manifest, registry, aliases, pack_bytes=PACK)
            self.assertEqual(actual['interfaceVersion'], 'weft-compile/0.4.2')
            self.assertEqual(actual['dialect'], 'weft-sql/0.4.2')
            self.assertEqual(actual['target']['backendVersion'],
                             '0.4.2-implicit-arithmetic-labels-candidate')
            for key in ('interfaceVersion', 'dialect'):
                actual[key] = original[key]
            for key in ('backendId', 'backendVersion', 'targetProfile'):
                actual['target'][key] = original['target'][key]
            self.assertEqual(actual, original)
        self.assertEqual((BINDINGS, manifest, registry, aliases), before)

    def test_changed_sql_pack_and_executable_metadata_refuse(self):
        class Forged(dict):
            pass
        manifest, registry, aliases = metadata()
        for sql, pack, bindings in ((CASES[0][1]+' ', PACK, BINDINGS),
                                    (CASES[0][1], PACK+b' ', BINDINGS),
                                    (CASES[0][1], PACK, Forged(BINDINGS))):
            with self.assertRaises(ValueError):
                commerce_implicit_arithmetic_request(sql, MODEL, bindings,
                    manifest, registry, aliases, pack_bytes=pack)
