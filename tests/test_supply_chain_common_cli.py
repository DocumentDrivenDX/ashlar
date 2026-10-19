# SPDX-License-Identifier: MIT OR Apache-2.0
import io,json,sys,tempfile,unittest
from contextlib import contextmanager,redirect_stderr
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import ashlar_host.supply_chain_common_cli as command
from ashlar import cli

class Tests(unittest.TestCase):
    def test_selected_public_lifecycle_closes_before_report(self):
        for fail_close in (False,True):
            with tempfile.TemporaryDirectory()as directory:
                root=Path(directory).resolve();configuration=root/'config.json';configuration.write_text('{}');output=root/'output'
                config=SimpleNamespace(root=root/'native',python=root/'python',java_home=root/'jdk',jars=root/'jars',pack=root/'pack',model=root/'model',graph=root/'graph',producer=SimpleNamespace(source=root/'source',bun=root/'bun',git=root/'git'),query=SimpleNamespace(distribution=SimpleNamespace(index=root/'index',output=root/'compiler')),capacity={})
                events=[]
                @contextmanager
                def opened(reader):
                    events.append('open');yield 'actual-opened';self.assertFalse((output/'result.json').exists());events.append('close')
                    if fail_close:raise RuntimeError('owned close failed')
                def query(owner,query,*,observe,selected_case_ids):
                    self.assertEqual(owner,'actual-opened');self.assertEqual(selected_case_ids,('singleton',));events.append('query');observe(0,0,b'original-request',b'original-response');return {'queries':[{'case_id':'singleton'}]}
                with patch.object(command,'parse_common_configuration',return_value=config),patch.object(command,'HeldSupplyChainDataset',return_value=SimpleNamespace(original_invocation=lambda:object())),patch.object(command,'SupplyChainCommonRuntime'),patch.object(command,'SupplyChainCommonReadConfig'),patch.object(command,'open_supply_chain_common_read',side_effect=opened),patch.object(command,'query_supply_chain_common',side_effect=query):
                    if fail_close:
                        with self.assertRaisesRegex(RuntimeError,'owned close failed'):command.run_supply_chain_common_command(configuration,output,selected_case_ids=('singleton',))
                        self.assertFalse((output/'result.json').exists())
                    else:
                        result=command.run_supply_chain_common_command(configuration,output,selected_case_ids=('singleton',));self.assertEqual(result['selected_case_ids'],['singleton']);self.assertEqual((output/'result.json').stat().st_mode&0o777,0o600)
                    self.assertEqual(events,['open','query','close'])
    def test_cli_dispatch_and_invalid_case_preflight(self):
        with patch.object(sys,'argv',['ashlar','query-supply-chain-common','--configuration','/selected/config','--output','/fresh/output','--case','singleton']),patch.object(command,'run_supply_chain_common_command',return_value={'output':'/fresh/output/result.json','selected_case_ids':['singleton']})as run,patch('builtins.print'):
            cli.main();run.assert_called_once_with('/selected/config','/fresh/output',selected_case_ids=('singleton',))
        with patch.object(command.InvocationFile,'read')as read:
            with self.assertRaises(ValueError):command.run_supply_chain_common_command('/selected/config','/fresh/output',selected_case_ids=('foreign',))
            read.assert_not_called()
