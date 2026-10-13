"""CLI selects each explicit public typed edge mutation wrapper."""
import io
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch
from ashlar.cli import main

class Tests(unittest.TestCase):
    def test_explicit_edge_dispatch(self):
        for action in ('update', 'delete'):
            with self.subTest(action=action), patch('sys.argv', ['ashlar', action+'-truss-edge', '--configuration', '/configuration', '--output', '/output']), patch('ashlar_host.truss_typed_edge_'+action+'.run_'+action+'_truss_edge_command') as command, redirect_stdout(io.StringIO()):
                main()
            command.assert_called_once_with('/configuration', '/output')
