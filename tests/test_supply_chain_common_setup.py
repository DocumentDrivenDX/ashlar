# SPDX-License-Identifier: MIT OR Apache-2.0
"""Real configuration parsers only; no SDK, producer or engine execution."""
import json,sys,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from ashlar_host.supply_chain_configuration import make_supply_chain_native_configuration
from ashlar_host.supply_chain_common_setup import run_configure_supply_chain_common_command
from ashlar_host.supply_chain_common_cli import parse_common_configuration
from ashlar import cli

class Tests(unittest.TestCase):
    def fixture(self,root):
        paths={name:root/name for name in ('output','jars','pack','model','graph','producer_source','bun','git','count_star_index','count_star_installation','paths_index','paths_installation')}
        for name,path in paths.items():
            if name in ('output','jars','producer_source','count_star_installation','paths_installation'):path.mkdir()
            else:path.write_text('original')
        java=root/'jdk';(java/'bin').mkdir(parents=True);(java/'bin/java').write_text('selected')
        raw=make_supply_chain_native_configuration(**paths);config=root/'native.json';config.write_bytes(raw)
        return config,java,json.loads(raw)
    def test_real_parser_preserves_original_selections_private(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();config,java,original=self.fixture(root);output=root/'common.json'
            result=run_configure_supply_chain_common_command(config,Path(sys.executable).resolve(),java,output)
            value=json.loads(output.read_bytes());parsed=parse_common_configuration(output.read_bytes())
            self.assertEqual(value['producer'],original['producer']);self.assertEqual(value['compiler'],original['compiler'])
            self.assertEqual(value['operation_capacity'],original['operation_capacity']);self.assertEqual(str(parsed.root),original['output'])
            self.assertEqual(output.stat().st_mode&0o777,0o600);self.assertEqual(result,{'output':str(output)})
    def test_mismatch_overwrite_and_cli_dispatch(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();config,java,_=self.fixture(root);output=root/'common.json';foreign=root/'python';foreign.write_text('foreign')
            with self.assertRaises(ValueError):run_configure_supply_chain_common_command(config,foreign,java,output)
            self.assertFalse(output.exists());output.write_text('preserve')
            with self.assertRaises(ValueError):run_configure_supply_chain_common_command(config,Path(sys.executable).resolve(),java,output)
            self.assertEqual(output.read_text(),'preserve')
            with patch.object(sys,'argv',['ashlar','configure-supply-chain-common','--configuration',str(config),'--python',sys.executable,'--java-home',str(java),'--output',str(output)]),patch('ashlar_host.supply_chain_common_setup.run_configure_supply_chain_common_command',return_value={'output':str(output)})as run,patch('builtins.print'):
                cli.main();run.assert_called_once_with(str(config),sys.executable,str(java),str(output))
