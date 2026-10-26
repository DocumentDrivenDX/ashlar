"""Keep the PyPI distribution identity separate from Python import names."""
from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[1]
DIST = 'umf-ashlar'


class DistributionIdentityTests(unittest.TestCase):
    def test_distribution_and_release_manifests_use_umf_family_name(self):
        project = (ROOT / 'pyproject.toml').read_text()
        match = re.search(r'(?ms)^\[project\]\s+.*?^name\s*=\s*["\']([^"\']+)', project)
        self.assertIsNotNone(match)
        self.assertEqual(match.group(1), DIST)
        for workflow in ('release-candidate.yml', 'publish-pypi.yml'):
            text = (ROOT / '.github' / 'workflows' / workflow).read_text()
            self.assertIn('--package ' + DIST, text)

    def test_runtime_distribution_lookups_keep_ashlar_import_and_cli(self):
        runtime_files = (
            'src/ashlar_host/otel.py',
            'src/ashlar_host/diagnostics.py',
            'src/ashlar_host/diagnostic_composition.py',
            'src/ashlar_host/_otel_worker.py',
        )
        for filename in runtime_files:
            text = (ROOT / filename).read_text()
            self.assertRegex(text, r"metadata\.version\(['\"]" + DIST + r"['\"]\)")
            self.assertNotIn('ashlar-graph-toolkit', text)
        project = (ROOT / 'pyproject.toml').read_text()
        self.assertIn('ashlar = "ashlar.cli:main"', project)
        self.assertIn('include = ["ashlar", "ashlar.*", "ashlar_host", "ashlar_host.*"]', project)


if __name__ == '__main__':
    unittest.main()
