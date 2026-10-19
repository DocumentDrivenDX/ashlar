# SPDX-License-Identifier: MIT OR Apache-2.0
import io,unittest
from contextlib import redirect_stdout
from unittest.mock import patch
from ashlar.cli import main

class Tests(unittest.TestCase):
    def test_selected_authored_case_is_forwarded_to_host(self):
        args=['ashlar','query-graphframes','--configuration','/private/config.json','--provider','/private/provider.py','--provider-sha256','a'*64,'--case','missing-media']
        with patch('sys.argv',args),patch('ashlar_host.graphframes_cli.run_graphframes_command',return_value={'complete':True})as run,redirect_stdout(io.StringIO()):
            main()
        run.assert_called_once_with('/private/config.json','/private/provider.py','a'*64,selected_case_ids=('missing-media',))
