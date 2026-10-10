"""Explicit profile wiring with original source and inert composition ports only."""
from copy import deepcopy
from dataclasses import replace
import json
import unittest
from unittest.mock import Mock, patch

from ashlar_host import commerce_path_request as request_module
from ashlar_host import paths_query as module
from ashlar_host.config import HostError
from ashlar_host.path_admission import BACKEND, PATHS_KEYS_BACKEND
import test_host_paths_query as fixture


class RequestProfileTests(unittest.TestCase):
    def test_all_ten_requests_change_only_the_explicit_backend_selection(self):
        manifest, registry, aliases = fixture.metadata()
        before = deepcopy((fixture.BINDINGS, manifest, registry, aliases))
        for _, sql in fixture.oracle.commerce_path_cases():
            args = (sql, fixture.MODEL, fixture.BINDINGS, manifest, registry, aliases)
            original = request_module.commerce_path_request(*args)
            explicit = request_module.commerce_path_request(*args, profile='paths')
            selected = request_module.commerce_path_request(*args, profile='paths-keys')
            self.assertEqual(original, explicit)
            expected = deepcopy(original)
            for key in ('backendId', 'backendVersion', 'targetProfile'):
                self.assertEqual(original['target'][key], BACKEND[key])
                expected['target'][key] = PATHS_KEYS_BACKEND[key]
            self.assertEqual(selected, expected)
            self.assertEqual(selected['modules'][0]['documentJson'].encode(), fixture.MODEL)
        self.assertEqual((fixture.BINDINGS, manifest, registry, aliases), before)

    def test_invalid_selection_refuses_before_original_request_construction(self):
        with patch.object(request_module, 'base_request', side_effect=AssertionError('base reached')) as base:
            for profile in (None, True, '', 'unknown', ['paths']):
                with self.subTest(profile=profile), self.assertRaisesRegex(ValueError, '^Explicit supported path profile required$'):
                    request_module.commerce_path_request('unused', b'', {}, {}, [], {}, profile=profile)
            base.assert_not_called()


class HostProfileWiringTests(unittest.TestCase):
    def environment(self, profile='paths-keys'):
        # Reuse the existing source-only lifecycle fixture, not its test cases.
        case = fixture.LifecycleTests()
        case.setUp()
        self.addCleanup(case.doCleanups)
        case.config = replace(case.config, profile=profile)
        return case

    def test_selected_profile_owns_all_ten_requests_admission_and_closing(self):
        for profile, path_type, backend, compile_name, schema_name in (
            ('paths', module.PathsDistributionPaths, BACKEND,
             'compile_paths_distribution', 'installed_paths_schema_bundle'),
            ('paths-keys', module.PathsKeysDistributionPaths, PATHS_KEYS_BACKEND,
             'compile_paths_keys_distribution', 'installed_paths_keys_schema_bundle'),
        ):
            with self.subTest(profile=profile):
                case = self.environment(profile)
                calls = []; admissions = []
                bundle = tuple(fixture.schemas().items())
                if profile == 'paths-keys':
                    bundle += (('application-result-v0.2.schema.json', b'fixture inherited schema'),)
                schemas = Mock(return_value=bundle)
                def compile_selected(paths, raw):
                    self.assertIs(type(paths), path_type)
                    self.assertEqual((paths.index, paths.output, paths.package),
                                     (case.config.index, case.config.installation, None))
                    calls.append(raw)
                    request = json.loads(raw)
                    for key in ('backendId', 'backendVersion', 'targetProfile'):
                        self.assertEqual(request['target'][key], backend[key])
                    # Later global rebinding cannot redirect this run's selected ports.
                    setattr(module, compile_name, Mock(side_effect=AssertionError('reselected compiler')))
                    setattr(module, schema_name, Mock(side_effect=AssertionError('reselected schemas')))
                    return b'{"status":"compiled"}\n'
                def execute(opened, request, artifact, recompiled, *, config, original_oracle):
                    admissions.append(config.admission.profile)
                    return {'oracle': original_oracle(fixture.MODEL, fixture.GRAPH, request)}
                forbidden_compile = Mock(side_effect=AssertionError('other profile compiler'))
                forbidden_schema = Mock(side_effect=AssertionError('other profile schemas'))
                other_compile = 'compile_paths_keys_distribution' if profile == 'paths' else 'compile_paths_distribution'
                other_schema = 'installed_paths_keys_schema_bundle' if profile == 'paths' else 'installed_paths_schema_bundle'
                result = case.compose(**{compile_name: compile_selected, schema_name: schemas,
                    other_compile: forbidden_compile, other_schema: forbidden_schema,
                    'execute_commerce_path': execute})
                self.assertEqual(len(result['cases']), 10)
                self.assertEqual(admissions, [profile] * 10)
                self.assertEqual(len(calls), 20)
                self.assertTrue(all(calls[n] == calls[n+1] for n in range(0,20,2)))
                self.assertEqual(schemas.call_count, 2)
                self.assertIs(schemas.call_args_list[0].args[0], schemas.call_args_list[1].args[0])
                forbidden_compile.assert_not_called(); forbidden_schema.assert_not_called()
                self.assertEqual(set(result['schema_hashes']), {name for name, _ in bundle})
                case.spark.stop.assert_called_once()

    def test_selected_schema_failure_has_no_fallback_or_producer_effects(self):
        case = self.environment()
        producer = Mock(side_effect=AssertionError('producer reached'))
        old = Mock(side_effect=AssertionError('old schemas reached'))
        with self.assertRaisesRegex(HostError, '^paths-query-refused$'):
            case.compose(installed_paths_keys_schema_bundle=Mock(side_effect=OSError('private payload')),
                         installed_paths_schema_bundle=old, recompute_dataset=producer)
        old.assert_not_called(); producer.assert_not_called(); case.spark.getOrCreate.assert_not_called()
        self.assertFalse(case.config.output.exists())

    def test_selected_compiler_failure_never_uses_old_compiler_or_releases_report(self):
        case = self.environment()
        old = Mock(side_effect=AssertionError('old compiler reached'))
        with self.assertRaisesRegex(HostError, '^paths-query-refused$'):
            case.compose(installed_paths_keys_schema_bundle=lambda _: tuple(fixture.schemas().items()),
                         compile_paths_keys_distribution=Mock(side_effect=OSError('private payload')),
                         compile_paths_distribution=old)
        old.assert_not_called(); case.spark.stop.assert_called_once()
        self.assertFalse((case.config.output/'report.json').exists())

    def test_selected_compiler_cancellation_survives_required_cleanup_failure(self):
        case = self.environment(); primary = KeyboardInterrupt()
        case.spark.stop.side_effect = OSError('cleanup')
        with self.assertRaises(KeyboardInterrupt) as caught:
            case.compose(installed_paths_keys_schema_bundle=lambda _: tuple(fixture.schemas().items()),
                         compile_paths_keys_distribution=Mock(side_effect=primary))
        self.assertIs(caught.exception, primary); self.assertTrue(primary.cleanup_failed)
        self.assertFalse((case.config.output/'report.json').exists())

    def test_selected_closing_schema_drift_withholds_after_spark_stop(self):
        case = self.environment()
        original = tuple(fixture.schemas().items())
        changed = original + (('application-result-v0.2.schema.json', b'changed'),)
        schema = Mock(side_effect=[original, changed])
        with self.assertRaisesRegex(HostError, '^paths-installation-drift$'):
            case.compose(installed_paths_keys_schema_bundle=schema,
                         compile_paths_keys_distribution=lambda *args: b'{"status":"compiled"}\n')
        case.spark.stop.assert_called_once(); self.assertEqual(schema.call_count, 2)
        self.assertFalse((case.config.output/'report.json').exists())


if __name__ == '__main__':
    unittest.main()
