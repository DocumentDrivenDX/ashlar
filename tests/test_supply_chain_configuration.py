"""Basic configuration generation and public CLI controls; no SDK execution."""
import contextlib,io,json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from ashlar import cli
from ashlar_host.supply_chain_configuration import make_supply_chain_native_configuration
from ashlar_host.supply_chain_native_cli import parse_supply_chain_native_invocation

class Tests(unittest.TestCase):
 def paths(self,root):
  return {name:root/name for name in ('output','jars','pack','model','graph','producer_source','bun','git','count_star_index','count_star_installation','paths_index','paths_installation')}
 def test_generated_configuration_roundtrips_supported_parser(self):
  with tempfile.TemporaryDirectory()as tmp:
   paths=self.paths(Path(tmp).resolve());raw=make_supply_chain_native_configuration(**paths)
   selected=parse_supply_chain_native_invocation(raw);document=json.loads(raw)
   self.assertEqual(selected.profile,'ashlar-original-supply-chain-native-invocation/0.3')
   self.assertEqual(selected.native.output,paths['output']);self.assertEqual(document['compiler_evidence_format'],'binary-protocol/0.1')
   self.assertEqual(document['operation_capacity']['max_intent_bytes'],8388608)
   self.assertFalse(paths['output'].exists())
 def test_relative_and_overlapping_paths_refuse(self):
  with tempfile.TemporaryDirectory()as tmp:
   paths=self.paths(Path(tmp).resolve())
   for changed in ({'model':'relative.json'},{'output':paths['count_star_installation']}):
    with self.assertRaises(ValueError):make_supply_chain_native_configuration(**{**paths,**changed})
 def test_public_command_outputs_json_without_running_native(self):
  with tempfile.TemporaryDirectory()as tmp:
   paths=self.paths(Path(tmp).resolve());argv=['ashlar','configure-supply-chain-native']
   for name,value in paths.items():argv.extend(['--'+name.replace('_','-'),str(value)])
   output=io.StringIO()
   with patch('sys.argv',argv),contextlib.redirect_stdout(output),patch('ashlar_host.supply_chain_native_cli.publish_query_supply_chain_native',side_effect=AssertionError('engine must not run')):
    cli.main()
   self.assertEqual(parse_supply_chain_native_invocation(output.getvalue().encode()).native.output,paths['output'])
   self.assertFalse(paths['output'].exists())
