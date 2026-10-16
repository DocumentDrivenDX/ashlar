"""Preparation custody controls; no native module or Spark execution."""
import hashlib
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import run_publication_current_query_example as runner


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
