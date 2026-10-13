"""Dedicated dispatch and final custody refusal; no native systems opened."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import test_pack_native_cli as fixtures
from ashlar_host import pack_native_cli
from ashlar_host.payments_count_cli import run_query_payments_count_command

class Tests(unittest.TestCase):
    def configuration(self,root):
        path,value=fixtures.NativeCliTests().configuration(root)
        value['pack']='payments';path.write_text(json.dumps(value))
        return path,value

    def test_dedicated_closed_report_and_regular_scenario_refusal(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();configuration,value=self.configuration(root)
            with self.assertRaises(ValueError):pack_native_cli.parse_finite_pack_invocation(configuration.read_bytes(),'query')
            with patch.object(pack_native_cli,'owned_finite_pack_query',return_value={'record':'ledger_entries','qualification':'inert count'})as owner:
                result=run_query_payments_count_command(configuration,record='ledger_entries')
            invocation,operation=owner.call_args.args
            self.assertEqual(invocation.native.producer.definition.name,'payments')
            self.assertTrue(callable(operation))
            report=json.loads(Path(result['report']).read_bytes())
            self.assertTrue(report['outerCleanupComplete']);self.assertTrue(report['invocationCustodyComplete'])

    def test_custody_renewed_after_owner_failure_without_success_report(self):
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory).resolve();configuration,value=self.configuration(root)
            from ashlar_host.evolution_cli import InvocationFile
            original=InvocationFile.renew;renewed=[]
            def renew(owner,limit):renewed.append(True);return original(owner,limit)
            with patch.object(pack_native_cli,'owned_finite_pack_query',side_effect=OSError('owner closure failed')),patch.object(InvocationFile,'renew',renew):
                with self.assertRaises(OSError):run_query_payments_count_command(configuration,record='ledger_entries')
            self.assertEqual(renewed,[True])
            self.assertFalse((Path(value['output'])/'report.json').exists())

if __name__=='__main__':unittest.main()
