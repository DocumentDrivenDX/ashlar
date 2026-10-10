"""Actual offline validator tests; no native execution or distribution claim."""
import copy
import unittest
from unittest.mock import patch

import jsonschema
import referencing
from referencing.exceptions import NoSuchResource

from test_weft_path_plan import FIXTURES, SCHEMAS
from weft_path_plan import PathAdmissionConfig, PathPlanError, admit_path_artifact
from weft_path_schema import make_offline_path_schema_validation


REQUEST = 'compile-request-v0.4.schema.json'
RESPONSE = 'compile-response-v0.4.schema.json'
PLAN = 'logical-plan-v0.4.schema.json'
REFUSAL = 'Pinned public schema validation refused'


class PathSchemaTests(unittest.TestCase):
    def setUp(self):
        self.port = make_offline_path_schema_validation(SCHEMAS)

    def validate(self, name, value):
        return self.port.validate(self.port.schemas, name, value)

    def compiled(self):
        return copy.deepcopy(next(x for x in FIXTURES if x['response']['status'] == 'compiled'))

    def assert_refused(self, callback):
        with self.assertRaises(PathPlanError) as caught:
            callback()
        self.assertEqual(str(caught.exception), REFUSAL)
        self.assertTrue(caught.exception.__suppress_context__)

    def test_actual_validation_of_complete_retained_artifacts_without_mutation(self):
        compiled = 0
        for original in FIXTURES:
            fixture = copy.deepcopy(original)
            with self.subTest(case=fixture['test'], status=fixture['response']['status']):
                self.assertIsNone(self.validate(REQUEST, fixture['request']))
                self.assertIsNone(self.validate(RESPONSE, fixture['response']))
                if fixture['response']['status'] == 'compiled':
                    self.assertIsNone(self.validate(PLAN, fixture['response']['logicalPlan']))
                    compiled += 1
                self.assertEqual(fixture, original)
        self.assertEqual((len(FIXTURES), compiled), (21, 17))

    def test_real_port_composes_with_admission_for_all_retained_compiled_artifacts(self):
        config = PathAdmissionConfig(16 * 1024 * 1024, self.port)
        count = 0
        for original in FIXTURES:
            if original['response']['status'] != 'compiled':
                continue
            fixture = copy.deepcopy(original)
            with self.subTest(case=fixture['test']):
                admission = admit_path_artifact(fixture['request'], fixture['response'],
                                               fixture['response'], config=config)
                self.assertTrue(admission.columns)
                self.assertEqual(fixture, original)
                count += 1
        self.assertEqual(count, 17)

    def test_root_shapes_and_referenced_logical_plan_are_actually_checked(self):
        fixture = self.compiled()
        for name in [REQUEST, RESPONSE, PLAN]:
            for value in [{}, [], None, 'private invalid payload']:
                with self.subTest(root=name, value_type=type(value).__name__):
                    self.assert_refused(lambda: self.validate(name, value))
        request = copy.deepcopy(fixture['request'])
        request['interfaceVersion'] = 'weft-compile/0.3.0'
        self.assert_refused(lambda: self.validate(REQUEST, request))
        response = copy.deepcopy(fixture['response'])
        response['private invalid property'] = 'private invalid payload'
        self.assert_refused(lambda: self.validate(RESPONSE, response))
        response = copy.deepcopy(fixture['response'])
        response['logicalPlan']['irVersion'] = 'weft-ir/0.3.0'
        self.assert_refused(lambda: self.validate(RESPONSE, response))
        self.assert_refused(lambda: self.validate(PLAN, response['logicalPlan']))

    def test_factory_and_callback_require_exact_bundle_and_full_root_name(self):
        changed = dict(SCHEMAS)
        changed[REQUEST] += b' '
        self.assert_refused(lambda: make_offline_path_schema_validation(changed))
        for changed in [{}, {REQUEST: SCHEMAS[REQUEST]}, dict(SCHEMAS, unexpected=b'{}')]:
            self.assert_refused(lambda: make_offline_path_schema_validation(changed))
        changed = dict(SCHEMAS)
        changed[REQUEST] += b' '
        self.assert_refused(lambda: self.port.validate(changed, REQUEST, self.compiled()['request']))
        for name in ['compile-request', '../' + REQUEST, 'https://example.invalid/' + REQUEST, None]:
            self.assert_refused(lambda: self.validate(name, {}))
        source = dict(SCHEMAS)
        port = make_offline_path_schema_validation(source)
        source.clear()
        self.assertIsNone(port.validate(port.schemas, REQUEST, self.compiled()['request']))
        with self.assertRaises(TypeError):
            port.schemas[REQUEST] = b'{}'

    def test_registry_is_closed_offline_and_constructor_failures_are_sanitized(self):
        actual_registry = referencing.Registry
        held = []
        def registry_factory(*args, **kwargs):
            registry = actual_registry(*args, **kwargs)
            held.append(registry)
            return registry
        with patch.object(referencing, 'Registry', side_effect=registry_factory), \
                patch('builtins.open', side_effect=AssertionError('filesystem retrieval')), \
                patch('socket.create_connection', side_effect=AssertionError('network retrieval')):
            port = make_offline_path_schema_validation(SCHEMAS)
            fixture = self.compiled()
            self.assertIsNone(port.validate(port.schemas, RESPONSE, fixture['response']))
            for uri in ['https://example.invalid/private', 'file:///private/unknown', 'missing.schema.json']:
                with self.assertRaises(NoSuchResource):
                    held[0].get_or_retrieve(uri)
        for target, member in [(jsonschema, 'Draft202012Validator'), (referencing, 'Registry')]:
            with patch.object(target, member, side_effect=RuntimeError('private constructor payload')):
                self.assert_refused(lambda: make_offline_path_schema_validation(SCHEMAS))


if __name__ == '__main__':
    unittest.main()
