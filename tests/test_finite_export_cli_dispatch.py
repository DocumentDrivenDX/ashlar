import io,json,unittest
from contextlib import redirect_stdout
from unittest.mock import patch
from ashlar.cli import main
class Tests(unittest.TestCase):
    def test_dispatch_uses_selected_existing_publication_configuration(self):
        with patch('sys.argv',['ashlar','export-original-finite-pack','--configuration','/private/export.json','--output','/private/export']),patch('ashlar_host.finite_export_cli.run_export_original_finite_pack_command',return_value={'output':'/private/export'})as run,redirect_stdout(io.StringIO())as output:
            main()
        run.assert_called_once_with('/private/export.json','/private/export')
        self.assertEqual(json.loads(output.getvalue()),{'output':'/private/export'})
