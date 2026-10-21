# SPDX-License-Identifier: MIT OR Apache-2.0
import hashlib
import json
import base64
import unittest

from test_managed_current_core import _config, _current_admission
from ashlar_host.managed_current_core import bind_current_core
from ashlar_host.managed_current_weft_binding import (
    BACKEND02_DIALECT, BACKEND02_INTERFACE, BACKEND02_VERSION,
    BACKEND03_DIALECT, BACKEND03_INTERFACE, BACKEND03_VERSION, BACKEND_ID, DATA_ROLES,
    managed_current_count_star_request, managed_current_sdk_pages_request,
)
from ashlar_host.managed_current_core import PROFILE


def _inputs(bundle):
    data_roles = {role: {'name': ('client_dev', 'offline_test_schema', role),
                         'uuid': f'00000000-0000-4000-8000-{index:012x}',
                         'version': index + 10}
                  for index, role in enumerate(DATA_ROLES, 1)}
    manifest = {
        'publication_id': 'ashlar-current:weft-binding-test',
        'profile_version': 'ashlar-delta/0.3',
        'table_versions_json': json.dumps(
            {'.'.join(item['name']): item['version'] for item in data_roles.values()},
            ensure_ascii=False, separators=(',', ':'), sort_keys=True),
        'schema_revisions_json': json.dumps(bundle['source']['reportSha256'],
            ensure_ascii=False, separators=(',', ':'), sort_keys=True),
    }
    return {'manifest': manifest, 'manifestUuid': '00000000-0000-4000-8000-000000000005',
            'dataRoles': data_roles}


def _managed_config():
    return _config(
        {'surface': 'Backend03', 'backendId': BACKEND_ID,
         'backendVersion': BACKEND03_VERSION, 'targetProfile': PROFILE,
         'weftInterface': BACKEND03_INTERFACE, 'weftDialect': BACKEND03_DIALECT},
        {'surface': 'Backend02', 'backendId': BACKEND_ID,
         'backendVersion': BACKEND02_VERSION, 'targetProfile': PROFILE,
         'weftInterface': BACKEND02_INTERFACE, 'weftDialect': BACKEND02_DIALECT})


class ManagedCurrentWeftBindingTests(unittest.TestCase):
    def test_actual_accepted_umf_and_truss_assignments_form_distinct_public_requests(self):
        bundle = bind_current_core(_current_admission(), _managed_config())
        inputs = json.loads(json.dumps(_inputs(bundle)))
        grouped_sql = ('SELECT p.person_name, COUNT(*) AS total FROM Person p '
                       'GROUP BY p.person_name HAVING COUNT(*) > 1 ORDER BY p.person_name')
        count_request = managed_current_count_star_request(bundle, inputs, grouped_sql)
        self.assertEqual(count_request['sql'], grouped_sql)
        self.assertEqual(count_request['interfaceVersion'], 'weft-compile/0.4.1')
        self.assertEqual(count_request['target']['backendVersion'], BACKEND03_VERSION)
        binding = json.loads(count_request['target']['bindingJson'])
        self.assertEqual(binding['profile'], 'ashlar-managed-current-core/0.1')
        self.assertEqual({table['name'][2] for table in binding['publication']['tables']}, set(DATA_ROLES))
        self.assertEqual(len(binding['publication']['tables']), 4)
        self.assertEqual({record['typeId'] for record in binding['records']}, {'1', '2'})
        installation = json.loads(base64.b64decode(
            bundle['source']['cursorBeforeBase64'], validate=True))['installationId']
        self.assertTrue(all(record['sourceSystem'] == installation for record in binding['records']))
        person = next(record for record in binding['records']
                      if record['logical']['element'] == 'Person')
        self.assertEqual({prop['home']['propertyId'] for prop in person['properties']}, {'1', '2', '3'})
        self.assertTrue(any(prop['home'].get('encoding') == 'ashlar-weft-json-native-null/0.1-candidate'
                            for prop in person['properties']))
        self.assertEqual({item['typeId'] for item in binding['relationships']}, {'1', '2'})
        self.assertEqual(count_request['target']['bindingSha256'],
                         hashlib.sha256(count_request['target']['bindingJson'].encode()).hexdigest())

        page_sql = 'SELECT p.person_key,p.person_name FROM Person p'
        page_request = managed_current_sdk_pages_request(bundle, inputs, page_sql, {})
        self.assertEqual(page_request['sql'], page_sql)
        self.assertEqual(page_request['interfaceVersion'], 'weft-compile/0.2.7')
        self.assertEqual(page_request['target']['backendVersion'], BACKEND02_VERSION)
        page_binding = json.loads(page_request['target']['bindingJson'])
        page_person = next(record for record in page_binding['records']
                           if record['logical']['element'] == 'Person')
        self.assertEqual({prop['home']['propertyId'] for prop in page_person['properties']}, {'1', '2'})
        self.assertTrue(all(type(item['name']) is list for item in inputs['dataRoles'].values()))

    def test_old_target_or_manifest_vector_drift_refuses(self):
        bundle = bind_current_core(_current_admission(), _managed_config())
        inputs = _inputs(bundle)
        with self.assertRaisesRegex(ValueError, 'four-role heads differ'):
            inputs['manifest']['table_versions_json'] = '{}'
            managed_current_count_star_request(bundle, inputs, 'SELECT COUNT(*) FROM Person p')
        inputs = _inputs(bundle)
        for role in DATA_ROLES:
            inputs['dataRoles'][role]['name'] = ('client_dev', 'other_schema', role)
        inputs['manifest']['table_versions_json'] = json.dumps(
            {'.'.join(item['name']): item['version'] for item in inputs['dataRoles'].values()},
            ensure_ascii=False, separators=(',', ':'), sort_keys=True)
        with self.assertRaisesRegex(ValueError, 'explicitly selected managed namespace'):
            managed_current_count_star_request(bundle, inputs, 'SELECT COUNT(*) FROM Person p')
        with self.assertRaisesRegex(ValueError, 'Digest-bound original accepted'):
            forged = dict(bundle)
            forged['weftTargets'] = {'Backend03': {'backendId': 'ashlar.databricks',
                'backendVersion': '0.1.0-qualified', 'targetProfile': 'dbsql2026.39-qualified'}}
            managed_current_count_star_request(forged, _inputs(bundle), 'SELECT COUNT(*) FROM Person p')


if __name__ == '__main__':
    unittest.main()
