"""Trusted Paths composition selection; mocked observations grant no qualification."""
from pathlib import Path
import unittest
from unittest.mock import patch

from ashlar import weft_paths_distribution as module


class PathsDistributionTests(unittest.TestCase):
    def paths(self, package=True):
        return module.PathsDistributionPaths(Path('/fixture/index.json'), Path('/fixture/installed'),
            Path('/fixture/package') if package else None)

    def observations(self):
        from contextlib import ExitStack
        stack = ExitStack()
        stack.enter_context(patch.object(module.sys, 'platform', 'darwin'))
        stack.enter_context(patch.object(module.platform, 'machine', return_value='arm64'))
        stack.enter_context(patch.object(module.platform, 'mac_ver', return_value=('27.0.1', ('', '', ''), 'arm64')))
        return stack

    def test_trusted_fixed_selection_and_restart_without_package(self):
        with self.observations(), patch.object(module.weft_paths_installation, 'install', return_value='installed') as install:
            self.assertEqual(module.install_paths_distribution(self.paths()), 'installed')
        config = install.call_args.args[0]
        self.assertEqual((config.index_revision, config.index_sha256, config.realization_id),
                         (module.INDEX_REVISION, module.INDEX_SHA256, module.REALIZATION_ID))
        self.assertEqual((config.observed_target, config.observed_os), ('aarch64-apple-darwin', '27.0.1'))
        self.assertEqual(config.package, Path('/fixture/package'))
        with self.observations(), patch.object(module.weft_paths_installation, 'open_installation', return_value='open') as opened:
            self.assertEqual(module.open_paths_distribution(self.paths()), 'open')
        self.assertIsNone(opened.call_args.args[0].package)

    def test_invalid_settings_fail_before_install_effects(self):
        cases = [object(), module.PathsDistributionPaths(Path('relative'), Path('/out')),
                 module.PathsDistributionPaths(Path('/index'), Path('relative')),
                 self.paths(False), module.PathsDistributionPaths(Path('/index'), Path('/out'), Path('relative'))]
        with patch.object(module.weft_paths_installation, 'install') as install:
            for paths in cases:
                with self.subTest(paths=type(paths).__name__), self.assertRaises(module.PathsDistributionError):
                    module.install_paths_distribution(paths)
            install.assert_not_called()

    def test_actual_observation_is_required_without_operator_override(self):
        with patch.object(module.sys, 'platform', 'linux'), patch.object(module.weft_paths_installation, 'install') as install:
            with self.assertRaisesRegex(module.PathsDistributionError, 'unsupported-platform'):
                module.install_paths_distribution(self.paths())
            install.assert_not_called()
        for observed in ('27.0.2', '26.0'):
            with self.observations(), patch.object(module.platform, 'mac_ver', return_value=(observed, ('', '', ''), 'arm64')):
                with self.assertRaisesRegex(module.PathsDistributionError, 'unsupported-platform'):
                    module.open_paths_distribution(self.paths())

    def test_compiler_bytes_are_unchanged_and_failures_have_no_fallback(self):
        raw = b'{"interfaceVersion":"weft-compile/0.4.0","status":"blocked"}\n'
        with self.observations(), patch.object(module.weft_paths_installation, 'open_installation', return_value='open'), patch.object(module.weft_paths_installation, 'compile_request', return_value=raw) as compiler:
            self.assertIs(module.compile_paths_distribution(self.paths(False), b'{"query":"synthetic"}'), raw)
            self.assertEqual(compiler.call_args.args, ('open', b'{"query":"synthetic"}'))
        with patch.object(module.weft_paths_installation, 'open_installation') as opened:
            for request in (bytearray(), 'bad', b'x' * (module.REQUEST_LIMIT + 1)):
                with self.assertRaises(module.PathsDistributionError):
                    module.compile_paths_distribution(self.paths(False), request)
            opened.assert_not_called()
        with self.observations(), patch.object(module.weft_paths_installation, 'open_installation', side_effect=module.weft_paths_installation.PathsInstallationError('synthetic private detail')):
            with self.assertRaisesRegex(module.PathsDistributionError, '^installation-refused$'):
                module.open_paths_distribution(self.paths(False))

    def test_owned_schema_bundle_is_returned_without_repair(self):
        bundle = (('compile-request-v0.4.schema.json', b'{"synthetic":true}'),)
        with self.observations(), patch.object(module.weft_paths_installation, 'open_installation', return_value='open'), patch.object(module.weft_paths_installation, 'installed_schema_bundle', return_value=bundle):
            self.assertIs(module.installed_paths_schema_bundle(self.paths(False)), bundle)


if __name__ == '__main__':
    unittest.main()
