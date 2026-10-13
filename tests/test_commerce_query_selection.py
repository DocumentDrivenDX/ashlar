"""Authored selection preflight, no compiler/SDK/native execution."""
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from ashlar_host.commerce_query_selection import select_commerce_query_cases
from ashlar_host.count_star_query import query_commerce_count_star
import test_commerce_implicit_arithmetic as fixtures
from ashlar_host import commerce_implicit_arithmetic as owner

class Tests(unittest.TestCase):
    def test_explicit_endpoint_requires_actual_ports_before_runtime_effects(self):
        from ashlar_host.count_star_configuration import QueryCommerceCountStarConfig
        from ashlar_host.implicit_arithmetic_configuration import ExplicitCommercePostgresEndpoint
        from ashlar_host.config import ProducerConfig,HostError
        from ashlar_host.path_capture import PathCaptureConfig
        from ashlar.weft_path_decode import PathDecodeConfig
        from ashlar_host.commerce_query_lifecycle import _query_commerce
        producer=ProducerConfig(Path('/source'),Path('/bun'),Path('/git'),5,1000,1000)
        config=QueryCommerceCountStarConfig(*[Path('/'+name)for name in ('index','installation','publication','output','jars','model','graph')],producer,
            ExplicitCommercePostgresEndpoint('original','127.0.0.1',25435,'truss_e2e'),1000,PathCaptureConfig(10,1000,10000),PathDecodeConfig(1000))
        with patch('ashlar_host.commerce_query_lifecycle.runtime_paths',side_effect=AssertionError('No runtime effect before ports')):
            with self.assertRaisesRegex(HostError,'complete-ordinary-query-ports-required'):
                _query_commerce(config,SimpleNamespace(phase=lambda *args:None),selected_case_ids=('expanded-count-having',))

    def test_selected_original_case_compiles_and_executes_only_its_original_sql(self):
        fixture=fixtures.Tests();fixture.setUp();selected=('refund',);requests=[]
        def compiler(paths,raw):
            import json
            requests.append(json.loads(raw)['sql']);return b'{"original":"compiler"}\n'
        with patch.object(owner,'compile_implicit_arithmetic_distribution',side_effect=compiler),patch.object(owner,'execute_commerce_implicit_arithmetic',return_value={'provisional':True})as execute:
            result=owner.execute_original_commerce_implicit_arithmetic_cases(fixture.opened,fixture.paths,config=fixture.config,pack_bytes=fixture.pack,selected_case_ids=selected)
        original=dict(owner.commerce_original_cases(fixture.pack))['refund']
        self.assertEqual(requests,[original,original]);self.assertEqual([case['case']for case in result],['refund'])
        self.assertEqual(execute.call_count,1);self.assertEqual(execute.call_args.args[1]['sql'],original)

    def test_original_sql_author_order_defaults_and_bad_selection(self):
        original=select_commerce_query_cases('count-star');self.assertEqual(len(original),14)
        selected=select_commerce_query_cases('count-star',('expanded-count-having','property-equality-join'))
        self.assertEqual(selected,tuple(case for case in original if case[0]in ('expanded-count-having','property-equality-join')))
        self.assertEqual(len(select_commerce_query_cases('implicit-arithmetic')),4)
        for selection in (['plain-count'],('unknown',),('plain-count','plain-count'),(True,)):
            with self.assertRaises(ValueError):select_commerce_query_cases('count-star',selection)
        config=object();ports=dict(ordinary_session=object(),source_factory=object(),invocation_custody=object())
        with patch('ashlar_host.count_star_query.run_count_star_query',return_value={'selected':True})as run:
            self.assertEqual(query_commerce_count_star(config,selected_case_ids=('expanded-count-having',),**ports),{'selected':True})
            self.assertEqual(run.call_args.kwargs['selected_case_ids'],('expanded-count-having',))
            for key,value in ports.items():self.assertIs(run.call_args.kwargs[key],value)
