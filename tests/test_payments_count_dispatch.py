# SPDX-License-Identifier: MIT OR Apache-2.0
import io,unittest
from contextlib import redirect_stdout
from unittest.mock import patch
from ashlar.cli import main

class Tests(unittest.TestCase):
    def test_explicit_record_dispatch(self):
        with patch('sys.argv',['ashlar','query-payments-count','--configuration','/configuration','--record','ledger_entries']),patch('ashlar_host.payments_count_cli.run_query_payments_count_command',return_value={'report':'/report'})as command,redirect_stdout(io.StringIO()):main()
        command.assert_called_once_with('/configuration',record='ledger_entries')
