# SPDX-License-Identifier: MIT OR Apache-2.0
import io,json,unittest
from contextlib import redirect_stdout
from unittest.mock import patch
from ashlar.cli import main

class Tests(unittest.TestCase):
    def test_one_authored_case_dispatches_without_a_corpus_sweep(self):
        with patch('sys.argv',['ashlar','query-finite-pack','--configuration','/private/query.json','--case','missing-media']),patch('ashlar_host.pack_native_cli.run_finite_pack_command',return_value={'report':'/private/report.json'})as run,redirect_stdout(io.StringIO())as output:
            main()
        run.assert_called_once_with('/private/query.json','query',case_ids=('missing-media',))
        self.assertEqual(json.loads(output.getvalue()),{'report':'/private/report.json'})
