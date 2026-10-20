# SPDX-License-Identifier: MIT OR Apache-2.0
"""Real configured public core 0.10 typed graph producer transport."""
import os
from pathlib import Path
import shutil
import tempfile
import unittest
from ashlar_host.config import ProducerConfig
from ashlar_host.configured_source_admission_config import ConfiguredSourceAdmissionConfig
from ashlar_host.configured_typed_graph_source_admission import validate_configured_typed_graph_source
from ashlar_host.typed_graph_admission import UMF_REVISION

PIN='a251ea5f876b7c69ef5e429af43621852242d324'

class RealCore010Admission(unittest.TestCase):
    def test_configured_example_runs_current_umf_over_real_transport(self):
        source=os.environ.get('ASHLAR_UMF_CORE010_SOURCE')
        if not source:
            self.skipTest('Set ASHLAR_UMF_CORE010_SOURCE to a clean pinned core 0.10 checkout')
        bun=shutil.which('bun');git=Path('/opt/homebrew/bin/git') if Path('/opt/homebrew/bin/git').exists() else Path(shutil.which('git') or '')
        if not bun or not git.is_file():self.skipTest('Bun and Git executables are required for real producer transport')
        source_path=Path(source).resolve(strict=True)
        config_path=(Path(__file__).parents[1]/'examples/end-to-end/configured-typed-graph-source.json').resolve()
        with tempfile.TemporaryDirectory(prefix='ashlar-real-core010-',dir='/private/tmp') as directory:
            output=Path(directory)/'admission'
            selection=ConfiguredSourceAdmissionConfig(config_path,ProducerConfig(source_path,Path(bun).resolve(strict=True),git.resolve(strict=True),60,1048576,4194304),output,4194304)
            result=validate_configured_typed_graph_source(selection)
            self.assertEqual(UMF_REVISION,PIN)
            self.assertEqual(result['producer_revision'],PIN)
            self.assertEqual((result['batches'],result['records']),(3,13))
            self.assertFalse(result['published']);self.assertFalse(result['acknowledged'])
            self.assertTrue((output/'graph-1.json').is_file())

if __name__=='__main__':unittest.main()
