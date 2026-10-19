# SPDX-License-Identifier: MIT OR Apache-2.0
"""SDK-free composition controls; injected inert ports prove no native success."""
import json
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase, mock
from dataclasses import replace

from ashlar.weft_path_decode import PathDecodeConfig
from ashlar.weft_paths_distribution import PathsDistributionPaths
from ashlar_host.path_capture import PathCaptureConfig
from ashlar_host.supply_chain_two_hop_query import SupplyChainTwoHopQueryConfig, query_supply_chain_two_hop
from ashlar_host import supply_chain_two_hop_query as owner
from ashlar_host.supply_chain_publication import FiniteSupplyChainDriver
from ashlar_host.supply_chain_finite_source import FiniteSupplyChainSource
import test_supply_chain_common_requests as fixtures


class Controls(TestCase):
    def setUp(self):
        self.config = SupplyChainTwoHopQueryConfig(PathsDistributionPaths(Path('/index'), Path('/installation')),
            1024*1024, PathCaptureConfig(100, 65536, 1024*1024), PathDecodeConfig(65536))

    def test_config_exact_types_bounds_existing_absolute_paths(self):
        for changes in ({'maximum_artifact_bytes': True}, {'maximum_artifact_bytes': 0},
                        {'maximum_artifact_bytes': 16*1024*1024+1}, {'capture': object()}, {'decoder': object()},
                        {'distribution': PathsDistributionPaths(Path('index'), Path('/installation'))},
                        {'distribution': PathsDistributionPaths(Path('/index'), Path('/installation'), Path('/package'))}):
            with self.assertRaises(ValueError): replace(self.config, **changes)

    def test_wrong_owner_refuses_before_compiler_or_native(self):
        with mock.patch.object(owner, 'compile_paths_distribution') as compile:
            with self.assertRaises(ValueError): query_supply_chain_two_hop(object(), b'', {}, self.config, observe=lambda *a: None)
            compile.assert_not_called()

    def _inert_driver(self):
        t = fixtures.SupplyChainCommonRequestsTests(); t.setUp()
        # Nominal instances without native constructors are isolated test doubles;
        # production code still requires actual owners, never these test objects.
        source = object.__new__(FiniteSupplyChainSource)
        source.model, source.graph, source.bindings = t.raw[1], t.raw[2], t.bindings
        source.receipt, source.raw = b'inert-receipt', b'inert-transaction'
        source.renew = mock.Mock(); source.metadata = mock.Mock()
        driver = object.__new__(FiniteSupplyChainDriver)
        driver.source, driver.manifest, driver.context = source, t.manifest, object()
        driver.transport = SimpleNamespace(targets={i: SimpleNamespace(table=row['table'], uuid=row['uuid']) for i,row in enumerate(t.registry)})
        return t, driver

    def test_complete_twelve_twice_compile_and_same_real_context_passthrough(self):
        t, driver = self._inert_driver(); seen=[]; observations=[]
        def execute(opened, request, artifact, trusted, *, config, original_oracle):
            self.assertIs(opened.context, driver.context)
            self.assertEqual(opened.native_files(), opened.original_native_files)
            oracle=original_oracle(opened.model, opened.graph, request)
            seen.append(oracle)
            return {'inert_only': True}
        schemas={'compile-request-v0.4.schema.json':b'a','compile-response-v0.4.schema.json':b'b','logical-plan-v0.4.schema.json':b'c'}
        with mock.patch.object(owner,'installed_paths_schema_bundle',return_value=tuple(schemas.items())), \
             mock.patch.object(owner,'make_offline_path_schema_validation'), \
             mock.patch.object(owner,'PathAdmissionConfig'), mock.patch.object(owner,'PathExecutionConfig'), \
             mock.patch.object(owner,'FiniteSupplyChainProvider'), \
             mock.patch.object(owner,'compile_paths_distribution',return_value=b'{}') as compile, \
             mock.patch.object(owner,'execute_commerce_path',side_effect=execute):
            result=query_supply_chain_two_hop(driver,t.raw[0],t.aliases,self.config,observe=lambda *a:observations.append(a))
        self.assertEqual(len(result['queries']),12);self.assertEqual(compile.call_count,24);self.assertEqual(len(observations),24)
        self.assertEqual(sum(len(json.loads(row[1])['items']) for oracle in seen for row in oracle['rows']),18)
        self.assertEqual(driver.source.renew.call_count,2)

    def test_primary_compile_failure_still_closes_source_and_installation(self):
        t,driver=self._inert_driver(); primary=KeyboardInterrupt('inert cancellation')
        schemas={'compile-request-v0.4.schema.json':b'a','compile-response-v0.4.schema.json':b'b','logical-plan-v0.4.schema.json':b'c'}
        with mock.patch.object(owner,'installed_paths_schema_bundle',return_value=tuple(schemas.items())) as bundle, \
             mock.patch.object(owner,'make_offline_path_schema_validation'), \
             mock.patch.object(owner,'PathAdmissionConfig'), mock.patch.object(owner,'PathExecutionConfig'), \
             mock.patch.object(owner,'compile_paths_distribution',side_effect=primary), \
             mock.patch.object(owner,'execute_commerce_path') as execute:
            with self.assertRaises(KeyboardInterrupt) as caught:
                query_supply_chain_two_hop(driver,t.raw[0],t.aliases,self.config,observe=lambda *a:None)
        self.assertIs(caught.exception,primary);self.assertEqual(bundle.call_count,2);self.assertEqual(driver.source.renew.call_count,2);execute.assert_not_called()

    def test_schema_open_failure_still_renews_original_source(self):
        t,driver=self._inert_driver();primary=ValueError('inert installation refusal')
        with mock.patch.object(owner,'installed_paths_schema_bundle',side_effect=primary), \
             mock.patch.object(owner,'compile_paths_distribution') as compile:
            with self.assertRaises(ValueError) as caught:
                query_supply_chain_two_hop(driver,t.raw[0],t.aliases,self.config,observe=lambda *a:None)
        self.assertIs(caught.exception,primary);self.assertEqual(driver.source.renew.call_count,2);compile.assert_not_called()

    def test_recompile_drift_refuses_before_execution(self):
        t,driver=self._inert_driver()
        schemas={'compile-request-v0.4.schema.json':b'a','compile-response-v0.4.schema.json':b'b','logical-plan-v0.4.schema.json':b'c'}
        with mock.patch.object(owner,'installed_paths_schema_bundle',return_value=tuple(schemas.items())), \
             mock.patch.object(owner,'make_offline_path_schema_validation'), \
             mock.patch.object(owner,'PathAdmissionConfig'), mock.patch.object(owner,'PathExecutionConfig'), \
             mock.patch.object(owner,'compile_paths_distribution',side_effect=[b'{}',b'{ }']), \
             mock.patch.object(owner,'execute_commerce_path') as execute:
            with self.assertRaises(ValueError):
                query_supply_chain_two_hop(driver,t.raw[0],t.aliases,self.config,observe=lambda *a:None)
        execute.assert_not_called();self.assertEqual(driver.source.renew.call_count,2)

    def test_invalid_or_oversized_response_never_reaches_observer(self):
        schemas={'compile-request-v0.4.schema.json':b'a','compile-response-v0.4.schema.json':b'b','logical-plan-v0.4.schema.json':b'c'}
        class Borrowed(bytes):pass
        for response in (b'x'*(self.config.maximum_artifact_bytes+1), '{}', Borrowed(b'{}')):
            with self.subTest(type=type(response).__name__):
                t,driver=self._inert_driver();observe=mock.Mock()
                with mock.patch.object(owner,'installed_paths_schema_bundle',return_value=tuple(schemas.items())) as bundle, \
                     mock.patch.object(owner,'make_offline_path_schema_validation'), \
                     mock.patch.object(owner,'PathAdmissionConfig'), mock.patch.object(owner,'PathExecutionConfig'), \
                     mock.patch.object(owner,'compile_paths_distribution',return_value=response), \
                     mock.patch.object(owner,'execute_commerce_path') as execute:
                    with self.assertRaises(ValueError):
                        query_supply_chain_two_hop(driver,t.raw[0],t.aliases,self.config,observe=observe)
                observe.assert_not_called();execute.assert_not_called()
                self.assertEqual(driver.source.renew.call_count,2);self.assertEqual(bundle.call_count,2)
