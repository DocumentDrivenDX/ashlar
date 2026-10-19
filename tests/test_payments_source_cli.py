# SPDX-License-Identifier: MIT OR Apache-2.0
import io,unittest
from contextlib import redirect_stdout
from types import SimpleNamespace
from unittest.mock import patch
from ashlar.cli import main
from ashlar_host.finite_pack import FinitePackDefinition

class Tests(unittest.TestCase):
    def test_selected_payments_public_source_command(self):
        with patch('sys.argv',['ashlar','payments-source','--ontology','/model','--graph','/graph','--output','/output','--source-system','selected']),patch('ashlar.payments_source.write_candidate',return_value=(SimpleNamespace(records=range(27)),{}))as command,redirect_stdout(io.StringIO())as output:
            main()
        command.assert_called_once_with('/model','/graph','/output',source_system='selected')
        self.assertIn('candidate payments transaction: 27 events',output.getvalue())
        self.assertIn('development bindings only',output.getvalue())
        facts=FinitePackDefinition('payments').facts()
        self.assertEqual((facts['objects'],facts['edges'],facts['present_nulls']),(13,14,0))
        self.assertEqual(facts['umf_version'],'0.8.0')
