# SPDX-License-Identifier: MIT OR Apache-2.0
"""Preparation custody controls; no native module or Spark execution."""
import hashlib
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from ashlar_host import publication_current_cli as runner


class ExampleTests(unittest.TestCase):
    def test_output_requires_absolute_private_canonical_parent(self):
        with self.assertRaises(ValueError):runner.require_fresh_output(Path('relative-output'))
        with tempfile.TemporaryDirectory()as directory:
            parent=Path(directory).resolve();parent.chmod(0o700)
            runner.require_fresh_output(parent/'fresh')
            parent.chmod(0o755)
            with self.assertRaises(ValueError):runner.require_fresh_output(parent/'fresh')
    def test_weft_preload_and_actual_imported_origin_refuse(self):
        with patch.dict(sys.modules,{'weft':SimpleNamespace()}):
            with self.assertRaisesRegex(ValueError,'Preloaded'):runner.load_selected_weft(Path('/unused'),'a'*64,'b'*64)
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();(root/'weft').mkdir()
            module=root/'weft/weft.abi3.so';shim=root/'weft/__init__.py'
            module.write_bytes(b'inert-native');shim.write_bytes(b'inert-shim')
            digests=[hashlib.sha256(x.read_bytes()).hexdigest()for x in (module,shim)]
            def imported(name):return SimpleNamespace(__file__=str(shim if name=='weft'else root/'wrong.so'))
            with patch.object(runner.importlib,'import_module',side_effect=imported),patch.object(sys,'path',list(sys.path)):
                with self.assertRaisesRegex(ValueError,'origins differ'):runner.load_selected_weft(root,*digests)

    def test_installed_cli_dispatches_explicit_paths_without_checkout_runner(self):
        from ashlar.cli import main
        args=['ashlar','query-publication-current','--configuration','/private/config.json',
              '--weft-root','/private/weft','--java-home','/private/java','--jars','/private/jars',
              '--output','/private/output','--weft-module-sha256','a'*64,'--weft-shim-sha256','b'*64]
        with patch.object(sys,'argv',args),patch.object(runner,'run_command') as execute:
            main()
        selected=execute.call_args.args[0]
        self.assertEqual(selected.configuration,Path('/private/config.json'))
        self.assertEqual(selected.weft_module_sha256,'a'*64)
        with patch.object(sys,'argv',['ashlar','query-publication-current']),patch.object(runner,'run_command') as execute:
            with self.assertRaises(SystemExit) as refusal:main()
            self.assertEqual(refusal.exception.code,2)
            execute.assert_not_called()
