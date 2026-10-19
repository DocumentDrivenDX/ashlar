# SPDX-License-Identifier: MIT OR Apache-2.0
import base64
import hashlib
import json
import unittest
import test_truss_graph_admission as cases
from ashlar_host.delta_custody import encoded
from ashlar_host.truss_graph_admission import TrussGraphAdmission
from ashlar_host.truss_weft_query import truss_string_compile_request


class Tests(unittest.TestCase):
    def test_actual_accepted_ids_original_umf_bytes_and_publication_binding(self):
        batch,catalogs=cases.Tests().original();admission=TrussGraphAdmission(batch,catalogs)
        tables={f'local.truss.{role}':0 for role in ('object_current','edge_current','tombstone','whole_source_history')}
        manifest={'publication_id':'original','profile_version':'ashlar-delta/0.3','recorded_at':'1',
            'table_versions_json':encoded(tables),'schema_revisions_json':encoded({json.loads(batch.begin)['prior']['installationId']:'1'}),
            'source_progress_json':'{"feed":{"position":"1"}}','validation_report_json':encoded({'complete':True,'source_admission':admission.metadata()})}
        uuids={table:'00000000-0000-0000-0000-'+str(index).zfill(12)for index,table in enumerate(tables,1)}
        request=truss_string_compile_request(admission,manifest,uuids,'00000000-0000-0000-0000-000000000005','SELECT a.id FROM A a',revision='1',document_id='cohort')
        original=admission.revisions['1']['models']['cohort'][1]
        self.assertEqual(request['modules'][0]['documentJson'].encode(),original)
        self.assertEqual(request['modules'][0]['pin']['revision'],hashlib.sha256(original).hexdigest())
        binding=json.loads(request['target']['bindingJson'])
        self.assertEqual([record['typeId']for record in binding['records']],['1','2'])
        self.assertEqual(binding['records'][0]['schemaRevision'],'1')
        self.assertEqual(binding['records'][0]['sourceSystem'],admission.changes[0].state.key.source)
        self.assertNotEqual(binding['records'][0]['sourceSystem'],admission.batch.feed)
        self.assertEqual(binding['records'][0]['properties'][0]['home']['propertyId'],next(iter(admission.revisions['1']['types']['1']['fields'])))
        self.assertEqual(len(binding['publication']['tables']),4)
        with self.assertRaises(ValueError):truss_string_compile_request(admission,manifest,uuids,'uuid','SELECT a.id FROM A a',revision='2',document_id='cohort')

        changed=dict(manifest,schema_revisions_json=encoded({json.loads(batch.begin)['prior']['installationId']:'2'}))
        with self.assertRaisesRegex(ValueError,'physical catalog revision differs'):
            truss_string_compile_request(admission,changed,uuids,'00000000-0000-0000-0000-000000000005','SELECT a.id FROM A a',revision='1',document_id='cohort')
