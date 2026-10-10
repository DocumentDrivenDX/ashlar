"""Execution-port controls over actual compiler metadata, without native claims."""
import copy
import unittest
from types import SimpleNamespace

from test_host_paths_keys_admission import PAIRS
import test_run_commerce_path_weft as ports
from test_weft_path_plan import SCHEMAS
from ashlar.weft_path_decode import PathDecodeConfig
from ashlar_host.path_admission import PathAdmissionConfig, PathSchemaValidation
from ashlar_host.path_capture import PathCaptureConfig
from ashlar_host.path_execution import PathExecutionConfig, PathExecutionError, execute_commerce_path


class PathsKeysExecutionTests(unittest.TestCase):
    def setup(self, rows):
        request, artifact = copy.deepcopy(PAIRS['original'])
        provider = ports.Provider(artifact, rows)
        opened = SimpleNamespace(provider=provider, context=provider.context,
            model=request['modules'][0]['documentJson'].encode('utf8'), graph=b'fixture graph',
            original_native_files={'fixture': 'fixed'}, native_files=lambda: {'fixture': 'fixed'})
        config = PathExecutionConfig(PathAdmissionConfig(16777216,
            PathSchemaValidation(SCHEMAS, lambda *args: None), 'paths-keys'),
            PathCaptureConfig(100, 10000, 100000), PathDecodeConfig(10000), None, None)
        oracle = lambda *args: {'rows': rows, 'witnesses': {'fixture': True}, 'scope': 'port test only'}
        return request, artifact, opened, config, oracle

    def execute(self, setup):
        request, artifact, opened, config, oracle = setup
        return execute_commerce_path(opened, request, artifact,
            copy.deepcopy(artifact), config=config, original_oracle=oracle)

    def test_original_carrier_and_duplicate_occurrences_survive_held_execution(self):
        raw = ' {"items":[["S1"],["S1"]],"truncated":true} '
        setup = self.setup([['P1', raw]])
        result = self.execute(setup)
        decoded = result['decoded'][0][1]
        self.assertEqual(decoded.original, raw.encode('utf8'))
        self.assertEqual(decoded.items, (('S1',), ('S1',)))
        self.assertTrue(decoded.truncated)
        self.assertFalse(setup[2].provider.active)
        self.assertTrue(result['interval']['closed'])

    def test_empty_result_still_runs_complete_original_collection_guards(self):
        setup = self.setup([])
        result = self.execute(setup)
        expected = [check for obligation in setup[1]['obligations']
            if obligation['id'].startswith('ashlar.relatedKeys.')
            for check in obligation['parameters']['checks']]
        actual = [row['check'] for row in result['guards']
            if row['obligation'].startswith('ashlar.relatedKeys.')]
        self.assertEqual(actual, expected)
        self.assertEqual(result['native_result']['rows'], [])

    def test_owning_collection_failure_withholds_user_query(self):
        setup = self.setup([])
        artifact, provider = setup[1], setup[2].provider
        sql = next(o for o in artifact['obligations']
            if o['id'] == 'ashlar.relatedKeys.collectionIntegrity')['parameters']['checks'][0]['sql']
        original = provider.sql
        calls = []
        def failing(statement, args):
            calls.append(statement)
            return ports.frame(['violations'], [['1']]) if statement == sql else original(statement, args)
        provider.driver.transport.spark.sql = failing
        with self.assertRaises(PathExecutionError): self.execute(setup)
        self.assertNotIn(artifact['sql'], calls)
        self.assertFalse(provider.active)

    def test_bad_native_identity_type_refuses_before_any_sql_even_empty(self):
        setup = self.setup([])
        provider = setup[2].provider
        original = provider.native_table_schema
        def wrong_type(table, *args):
            value = original(table, *args)
            value['schema']['fields'][0]['type'] = 'string'
            value['nativeTypes'][0][1] = 'STRING'
            return value
        provider.native_table_schema = wrong_type
        with self.assertRaises(PathExecutionError): self.execute(setup)
        self.assertFalse(any(isinstance(call, tuple) for call in provider.calls))

    def test_malformed_or_null_collection_never_returns_evidence(self):
        for raw in (None, '{"items":[["S2"],["S1"]],"truncated":false}',
                    '{"items":[["S1"]],"truncated":true}'):
            with self.subTest(raw=raw):
                setup = self.setup([['P1', raw]])
                with self.assertRaises(ValueError): self.execute(setup)
                self.assertFalse(setup[2].provider.active)

    def test_closing_publication_drift_withholds_decoded_result(self):
        setup = self.setup([['P1', '{"items":[],"truncated":false}']])
        values = iter(['original vector', 'changed vector'])
        setup[2].provider.resolve = lambda context: next(values)
        with self.assertRaises(PathExecutionError): self.execute(setup)
        self.assertFalse(setup[2].provider.active)
