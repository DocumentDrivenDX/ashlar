# SPDX-License-Identifier: MIT OR Apache-2.0
import unittest
from dataclasses import replace
from pathlib import Path

from ashlar.weft_path_decode import PathDecodeConfig
from ashlar_host.config import HostError, ProducerConfig, PrivatePostgresConfig
from ashlar_host.implicit_arithmetic_configuration import QueryCommerceImplicitArithmeticConfig
from ashlar_host.path_capture import PathCaptureConfig


class Tests(unittest.TestCase):
    def setUp(self):
        root = Path('/private/tmp/explicit-configuration-fixture')
        self.config = QueryCommerceImplicitArithmeticConfig(
            *(root / name for name in ('index', 'installation', 'publication', 'output',
                                      'jars', 'model', 'graph')),
            ProducerConfig(root / 'producer', root / 'bun', root / 'git', 30, 1024, 4096),
            PrivatePostgresConfig('ashlar-e2e-truss-pg17', '127.0.0.1', 15432, 'truss_e2e'),
            4096, PathCaptureConfig(100, 1024, 4096), PathDecodeConfig(1024))

    def test_explicit_endpoint_is_typed_closed_and_legacy_unchanged(self):
        from ashlar_host.implicit_arithmetic_configuration import ExplicitCommercePostgresEndpoint
        selected=ExplicitCommercePostgresEndpoint('original-private-fixture','127.0.0.1',25433,'truss_e2e')
        self.assertIs(replace(self.config,postgres=selected).postgres,selected)
        for changes in ({'port':True},{'port':25433.0},{'port':0},{'host':'remote'},{'database':'other'},{'container':'bad/name'}):
            with self.assertRaises(HostError):replace(selected,**changes)
        class Forged(ExplicitCommercePostgresEndpoint):pass
        with self.assertRaises(HostError):replace(self.config,postgres=Forged('chosen','127.0.0.1',25433,'truss_e2e'))
        with self.assertRaises(HostError):PrivatePostgresConfig('chosen','127.0.0.1',25433,'truss_e2e')

    def test_profile_is_closed_without_retargeting_settings(self):
        self.assertEqual(self.config.profile, 'implicit-arithmetic')
        with self.assertRaises(HostError):
            replace(self.config, profile='count-star')
        with self.assertRaises(HostError):
            replace(self.config, profile='0.4.3')
        self.assertEqual(self.config.capture.maximum_rows, 100)

    def test_relative_operator_path_and_untyped_settings_refuse(self):
        with self.assertRaises(HostError):
            replace(self.config, installation=Path('installation'))
        with self.assertRaises(HostError):
            replace(self.config, producer={})

    def test_decoder_cannot_exceed_capture_and_boolean_is_not_a_bound(self):
        with self.assertRaises(HostError):
            replace(self.config, decoder=PathDecodeConfig(1025))
        with self.assertRaises(HostError):
            replace(self.config, maximum_artifact_bytes=True)
        with self.assertRaises(HostError):
            replace(self.config, capture=PathCaptureConfig(1001, 1024, 4096))


if __name__ == '__main__':
    unittest.main()
