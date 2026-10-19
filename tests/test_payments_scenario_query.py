# SPDX-License-Identifier: MIT OR Apache-2.0
"""Original request and independent oracle, not a native qualification."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import test_payments_count_query as fixtures
import test_payments_count_cli as cli_fixtures
from ashlar_host.payments_scenario_query import payments_scenario_request,payments_scenario_oracle
from ashlar_host.payments_scenario_cli import run_query_payments_scenario_command
from ashlar_host import pack_native_cli

class Tests(unittest.TestCase):
    def test_exact_authored_request_and_independent_currency_set(self):
        model,graph,*metadata=fixtures.Tests().inputs()
        root=Path(__file__).resolve().parents[1]/'examples/domain-packs/payments/upstream'
        pack=(root/'pack.json').read_bytes()
        request=payments_scenario_request('currency',pack,model,graph,*metadata)
        original=next(c['sql']for c in json.loads(pack)['scenario_checks']if c['id']=='currency')
        self.assertEqual(request['sql'],original)
        self.assertEqual(request['modules'][0]['documentJson'].encode(),model)
        self.assertEqual(request['interfaceVersion'],'weft-compile/0.4.2')
        self.assertEqual(payments_scenario_oracle(model,graph)['rows'],[['USD']])
        with tempfile.TemporaryDirectory()as directory:
            configuration,_=cli_fixtures.Tests().configuration(Path(directory).resolve())
            with patch.object(pack_native_cli,'owned_finite_pack_query',return_value={'case':'currency','qualification':'inert'})as owner:
                result=run_query_payments_scenario_command(configuration,case='currency')
            self.assertEqual(owner.call_args.kwargs['query_profile'],'payments-scenario')
            report=json.loads(Path(result['report']).read_bytes())
            self.assertTrue(report['outerCleanupComplete']);self.assertIn('currency',report['qualification'])

    def test_unadmitted_financial_case_refuses_before_owner(self):
        with patch.object(pack_native_cli,'owned_finite_pack_query')as owner:
            for case in ('net-settlement','balance','settled'):
                with self.assertRaises(ValueError):run_query_payments_scenario_command('/inert',case=case)
            owner.assert_not_called()

if __name__=='__main__':unittest.main()
