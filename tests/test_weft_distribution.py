import io
from pathlib import Path
import unittest
from unittest.mock import patch
from ashlar import weft_distribution as d
from ashlar import weft_installation as w


class CompositionTests(unittest.TestCase):
    def setUp(self):
        self.paths = d.DistributionPaths(Path('index.json'), Path('output'), Path('package'))
        self.platform = patch.object(d.sys, 'platform', 'darwin')
        self.machine = patch.object(d.platform, 'machine', return_value='arm64')
        self.version = patch.object(d.platform, 'mac_ver', return_value=('27.0.1', (), ''))
        for p in (self.platform, self.machine, self.version): p.start(); self.addCleanup(p.stop)

    def test_fixed_trust_and_actual_observations_forwarded(self):
        sentinel = object()
        with patch.object(w, 'install', return_value=sentinel) as install:
            self.assertIs(d.install_distribution(self.paths), sentinel)
        c = install.call_args.args[0]
        self.assertEqual((c.index_revision, c.index_sha256), (d.INDEX_REVISION, d.INDEX_SHA256))
        self.assertEqual(c.realization_id, d.REALIZATION_ID)
        self.assertEqual((c.observed_target, c.observed_os), ('aarch64-apple-darwin', '27.0.1'))
        self.assertEqual(c.package, self.paths.package.absolute())
        with self.assertRaises(TypeError): d.DistributionPaths(Path('i'), Path('o'), index_sha256='x')

    def test_retained_open_and_compile_need_no_original_package(self):
        paths = d.DistributionPaths(Path('index.json'), Path('output'))
        request = b'{"original":"unchanged"}\n'; response = b'{"status":"blocked"}\n'
        with patch.object(w, 'open_installation', return_value='held') as opening, patch.object(w, 'compile_request', return_value=response) as compile:
            self.assertIs(d.compile_distribution(paths, request), response)
        self.assertIsNone(opening.call_args.args[0].package)
        compile.assert_called_once_with('held', request)
        with patch.object(w, 'install', side_effect=AssertionError('unexpected effects')):
            with self.assertRaisesRegex(d.DistributionError, 'package-required'): d.install_distribution(paths)

    def test_platform_refuses_before_public_ports(self):
        controls = [(d.sys, 'platform', 'linux'), (d.platform, 'machine', 'x86_64'), (d.platform, 'mac_ver', ('27.0.2', (), ''))]
        for owner, name, value in controls:
            setting = patch.object(owner, name, value) if name == 'platform' else patch.object(owner, name, return_value=value)
            with setting, patch.object(w, 'install', side_effect=AssertionError('effects')), patch.object(w, 'open_installation', side_effect=AssertionError('effects')):
                with self.assertRaisesRegex(d.DistributionError, 'unsupported-platform'): d.install_distribution(self.paths)
                with self.assertRaisesRegex(d.DistributionError, 'unsupported-platform'): d.open_distribution(self.paths)

    def test_public_refusal_is_payload_free(self):
        for api, port in [(d.install_distribution, 'install'), (d.open_distribution, 'open_installation')]:
            with patch.object(w, port, side_effect=w.InstallationError('secret /raw/path')):
                with self.assertRaises(d.DistributionError) as error: api(self.paths)
                self.assertEqual(str(error.exception), 'installation-refused')
        with patch.object(w, 'open_installation', return_value='held'), patch.object(w, 'compile_request', side_effect=w.InstallationError('raw request secret')):
            with self.assertRaisesRegex(d.DistributionError, '^compilation-refused$'): d.compile_distribution(self.paths, b'{}')

    def test_requests_are_bounded_before_opening(self):
        with patch.object(w, 'open_installation', side_effect=AssertionError('opened')):
            for request in (bytearray(b'{}'), b'x' * (d.REQUEST_LIMIT + 1)):
                with self.assertRaises(d.DistributionError): d.compile_distribution(self.paths, request)
        self.assertEqual(d.read_request(io.BytesIO(b'raw\n')), b'raw\n')
        class Endless:
            total = 0
            def read(self, n): self.total += n; return b'x' * n
        stream = Endless()
        with self.assertRaisesRegex(d.DistributionError, 'request-too-large'): d.read_request(stream)
        self.assertEqual(stream.total, d.REQUEST_LIMIT + 1)
        with self.assertRaises(d.DistributionError): d.read_request(io.StringIO('wrong stream'))

    def test_cleanup_warning_separate_from_availability(self):
        class Result: cleanup_pending = True
        self.assertEqual(d.diagnostic(Result()), 'cleanup-pending')
        Result.cleanup_pending = False
        self.assertEqual(d.diagnostic(Result()), 'installed')


if __name__ == '__main__': unittest.main()
