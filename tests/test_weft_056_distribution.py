# SPDX-License-Identifier: MIT OR Apache-2.0
"""Identity, schema and selected page-path contract for public Weft .56."""
import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ashlar.weft_056_distribution import (
    PROFILE, SCHEMA_SHA256, SOURCE_COMMIT, VERSION, WHEEL_SHA256,
    Weft056Distribution, Weft056Paths,
)


class Weft056DistributionTests(unittest.TestCase):
    def test_public_release_and_platform_wheel_identity(self):
        self.assertEqual(VERSION, '0.4.56')
        self.assertEqual(SOURCE_COMMIT, '2f62e1148d1b2030558550e4f6f2ffe518bac62d')
        self.assertEqual(PROFILE, 'ashlar-weft-056-runtime-index/0.1')
        self.assertEqual(WHEEL_SHA256, {
            ('Darwin', 'arm64'): '68f9297b4b55df483b86d3041040991c4dadfb9b4d63afcff1420c43f93ae41d',
            ('Linux', 'x86_64'): '32f4c38d9a614dd4c3a367f8b24d5cd4ba41ec80c2a4a2e3423e2082a294dbdd',
        })
        self.assertTrue(all(len(value) == 64 for value in WHEEL_SHA256.values()))

    def test_schema_bundle_matches_release_source_bytes(self):
        schema_dir = Path(__file__).parents[1] / 'docs/helix/04-build/evidence/current-upstream-release-20261029/r01-umf-weft-pins-20261029/weft056-schemas'
        self.assertEqual(set(SCHEMA_SHA256), {p.name for p in schema_dir.glob('*.json')})
        for name, expected in SCHEMA_SHA256.items():
            self.assertEqual(hashlib.sha256((schema_dir / name).read_bytes()).hexdigest(), expected)

    def test_page_compiler_selection_accepts_only_verified_versioned_profiles(self):
        from ashlar_host.configured_sdk_pages import _supported_page_compiler
        compiler = object.__new__(Weft056Distribution)
        with patch.object(Weft056Distribution, '_verify', return_value=True):
            self.assertTrue(_supported_page_compiler(compiler))
        self.assertFalse(_supported_page_compiler(object()))

    def test_index_digest_format_is_required(self):
        path = Path('/private/tmp/weft056-index.json')
        self.assertEqual(Weft056Paths(path, 'a' * 64).index, path)
        with self.assertRaises(ValueError):
            Weft056Paths(path, 'not-a-digest')

    def test_preexisting_bytecode_cache_is_refused_before_import(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'weft/__pycache__').mkdir(parents=True)
            (root / 'weft/__pycache__/__init__.cpython-312.pyc').write_bytes(b'forged cache')
            instance = object.__new__(Weft056Distribution)
            instance.index = {'root': str(root)}
            with self.assertRaises(ValueError):
                instance._refuse_bytecode_caches()


if __name__ == '__main__':
    unittest.main()
