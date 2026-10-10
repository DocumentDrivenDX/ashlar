"""Trusted Paths composition selection; mocked observations grant no qualification."""
from pathlib import Path
import unittest
from unittest.mock import patch

from ashlar import weft_paths_keys_distribution as module


class PathsKeysDistributionTests(unittest.TestCase):
    def paths(self, package=True):
        return module.PathsKeysDistributionPaths(Path('/fixture/index.json'), Path('/fixture/installed'),
            Path('/fixture/package') if package else None)

    def observations(self):
        from contextlib import ExitStack
        stack = ExitStack()
        stack.enter_context(patch.object(module.sys, 'platform', 'darwin'))
        stack.enter_context(patch.object(module.platform, 'machine', return_value='arm64'))
        stack.enter_context(patch.object(module.platform, 'mac_ver', return_value=('27.0.1', ('', '', ''), 'arm64')))
        return stack

    def test_trusted_fixed_selection_and_restart_without_package(self):
        with self.observations(), patch.object(module.weft_paths_keys_installation, 'install', return_value='installed') as install:
            self.assertEqual(module.install_paths_keys_distribution(self.paths()), 'installed')
        config = install.call_args.args[0]
        self.assertEqual((config.index_revision, config.index_sha256, config.realization_id),
                         (module.INDEX_REVISION, module.INDEX_SHA256, module.REALIZATION_ID))
        self.assertEqual((config.observed_target, config.observed_os), ('aarch64-apple-darwin', '27.0.1'))
        self.assertEqual(config.package, Path('/fixture/package'))
        with self.observations(), patch.object(module.weft_paths_keys_installation, 'open_installation', return_value='open') as opened:
            self.assertEqual(module.open_paths_keys_distribution(self.paths()), 'open')
        self.assertIsNone(opened.call_args.args[0].package)

    def test_old_profile_configuration_is_rejected_before_effects(self):
        from ashlar.weft_paths_distribution import PathsDistributionPaths
        old = PathsDistributionPaths(Path('/fixture/index.json'), Path('/fixture/installed'), Path('/fixture/package'))
        with patch.object(module.weft_paths_keys_installation, 'install') as effect:
            with self.assertRaisesRegex(module.PathsKeysDistributionError, 'invalid-configuration'):
                module.install_paths_keys_distribution(old)
            effect.assert_not_called()

    def test_pins_identify_the_separate_registered_realization(self):
        from ashlar import weft_paths_distribution as old
        self.assertEqual(module.INDEX_REVISION, '6d82b7e89e43319c2a175b9895527298ff66ef36')
        self.assertEqual(module.INDEX_SHA256, 'bd541aa8cb261376f160661d3df67065b36fcba4f6caf4c0f42e33d710c4b9dc')
        self.assertEqual(module.REALIZATION_ID, 'weft-3a2a79c-paths-keys-aarch64-apple-darwin-candidate')
        self.assertNotEqual(module.INDEX_SHA256, old.INDEX_SHA256)
        self.assertNotEqual(module.REALIZATION_ID, old.REALIZATION_ID)

    def test_invalid_settings_fail_before_install_effects(self):
        cases = [object(), module.PathsKeysDistributionPaths(Path('relative'), Path('/out')),
                 module.PathsKeysDistributionPaths(Path('/index'), Path('relative')),
                 self.paths(False), module.PathsKeysDistributionPaths(Path('/index'), Path('/out'), Path('relative'))]
        with patch.object(module.weft_paths_keys_installation, 'install') as install:
            for paths in cases:
                with self.subTest(paths=type(paths).__name__), self.assertRaises(module.PathsKeysDistributionError):
                    module.install_paths_keys_distribution(paths)
            install.assert_not_called()

    def test_actual_observation_is_required_without_operator_override(self):
        with patch.object(module.sys, 'platform', 'linux'), patch.object(module.weft_paths_keys_installation, 'install') as install:
            with self.assertRaisesRegex(module.PathsKeysDistributionError, 'unsupported-platform'):
                module.install_paths_keys_distribution(self.paths())
            install.assert_not_called()
        for observed in ('27.0.2', '26.0'):
            with self.observations(), patch.object(module.platform, 'mac_ver', return_value=(observed, ('', '', ''), 'arm64')):
                with self.assertRaisesRegex(module.PathsKeysDistributionError, 'unsupported-platform'):
                    module.open_paths_keys_distribution(self.paths())

    def test_compiler_bytes_are_unchanged_and_failures_have_no_fallback(self):
        raw = b'{"interfaceVersion":"weft-compile/0.4.0","status":"blocked"}\n'
        with self.observations(), patch.object(module.weft_paths_keys_installation, 'open_installation', return_value='open'), patch.object(module.weft_paths_keys_installation, 'compile_request', return_value=raw) as compiler:
            self.assertIs(module.compile_paths_keys_distribution(self.paths(False), b'{"query":"synthetic"}'), raw)
            self.assertEqual(compiler.call_args.args, ('open', b'{"query":"synthetic"}'))
        with patch.object(module.weft_paths_keys_installation, 'open_installation') as opened:
            for request in (bytearray(), 'bad', b'x' * (module.REQUEST_LIMIT + 1)):
                with self.assertRaises(module.PathsKeysDistributionError):
                    module.compile_paths_keys_distribution(self.paths(False), request)
            opened.assert_not_called()
        with self.observations(), patch.object(module.weft_paths_keys_installation, 'open_installation', side_effect=module.weft_paths_keys_installation.PathsKeysInstallationError('synthetic private detail')):
            with self.assertRaisesRegex(module.PathsKeysDistributionError, '^installation-refused$'):
                module.open_paths_keys_distribution(self.paths(False))

    def test_os_errors_are_payload_free_and_cancellation_identity_survives(self):
        pairs = ((module.install_paths_keys_distribution, 'install', 'installation-refused'),
                 (module.open_paths_keys_distribution, 'open_installation', 'installation-refused'))
        for function, port, message in pairs:
            with self.observations(), patch.object(module.weft_paths_keys_installation, port, side_effect=OSError('private-sentinel-path')):
                with self.assertRaisesRegex(module.PathsKeysDistributionError, '^'+message+'$'):
                    function(self.paths())
            cancellation = KeyboardInterrupt('original')
            with self.observations(), patch.object(module.weft_paths_keys_installation, port, side_effect=cancellation):
                with self.assertRaises(KeyboardInterrupt) as raised:
                    function(self.paths())
                self.assertIs(raised.exception, cancellation)
        with self.observations(), patch.object(module.weft_paths_keys_installation, 'open_installation', return_value='open'), patch.object(module.weft_paths_keys_installation, 'compile_request', side_effect=OSError('private-sentinel-path')):
            with self.assertRaisesRegex(module.PathsKeysDistributionError, '^compilation-refused$'):
                module.compile_paths_keys_distribution(self.paths(False), b'{}')
        with self.observations(), patch.object(module.weft_paths_keys_installation, 'open_installation', return_value='open'), patch.object(module.weft_paths_keys_installation, 'installed_schema_bundle', side_effect=OSError('private-sentinel-path')):
            with self.assertRaisesRegex(module.PathsKeysDistributionError, '^installation-refused$'):
                module.installed_paths_keys_schema_bundle(self.paths(False))

    def test_owned_schema_bundle_is_returned_without_repair(self):
        bundle = (('compile-request-v0.4.schema.json', b'{"synthetic":true}'),)
        with self.observations(), patch.object(module.weft_paths_keys_installation, 'open_installation', return_value='open'), patch.object(module.weft_paths_keys_installation, 'installed_schema_bundle', return_value=bundle):
            self.assertIs(module.installed_paths_keys_schema_bundle(self.paths(False)), bundle)


if __name__ == '__main__':
    unittest.main()
