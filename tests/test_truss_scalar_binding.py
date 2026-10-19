# SPDX-License-Identifier: MIT OR Apache-2.0
import hashlib,json,unittest
from ashlar_host.delta_custody import encoded
from ashlar_host.truss_graph_admission import TrussGraphAdmission
from ashlar_host.truss_weft_query import truss_scalar_compile_request,truss_string_compile_request
import test_truss_typed_admission

class Tests(unittest.TestCase):
    def fixture(self):
        batch,catalogs=test_truss_typed_admission.Tests().fixture();admission=TrussGraphAdmission(batch,catalogs)
        tables={f'local.truss.{role}':0 for role in ('object_current','edge_current','tombstone','whole_source_history')}
        manifest={'publication_id':'original','profile_version':'ashlar-delta/0.3','recorded_at':'1','table_versions_json':encoded(tables),'schema_revisions_json':encoded({admission.changes[0].state.key.source:'1'}),'source_progress_json':'{"feed":{"position":"1"}}','validation_report_json':encoded({'complete':True,'source_admission':admission.metadata()})}
        uuids={table:'00000000-0000-0000-0000-'+str(index).zfill(12)for index,table in enumerate(tables,1)}
        return admission,manifest,uuids
    def test_typed_binding_preserves_original_model_facets_and_allocations(self):
        admission,manifest,uuids=self.fixture();request=truss_scalar_compile_request(admission,manifest,uuids,'00000000-0000-0000-0000-000000000005','SELECT a.typed FROM A a',revision='1',document_id='cohort')
        original=admission.revisions['1']['models']['cohort'][1];self.assertEqual(request['modules'][0]['documentJson'].encode(),original);self.assertEqual(request['modules'][0]['pin']['sha256'],hashlib.sha256(original).hexdigest())
        binding=json.loads(request['target']['bindingJson']);self.assertTrue(any(p['home']['propertyId']=='99'for r in binding['records']for p in r['properties']))
        field=next(e for m in json.loads(original)['modules']for e in m['elements']if e['id']=='typed');self.assertNotIn('facets',field)
        with self.assertRaises(ValueError):truss_string_compile_request(admission,manifest,uuids,'00000000-0000-0000-0000-000000000005','SELECT a.typed FROM A a',revision='1',document_id='cohort')
    def test_changed_physical_revision_or_published_admission_refuses(self):
        admission,manifest,uuids=self.fixture()
        for changed in (dict(manifest,schema_revisions_json='{}'),dict(manifest,validation_report_json='{"complete":true,"source_admission":{}}')):
            with self.assertRaises(ValueError):truss_scalar_compile_request(admission,changed,uuids,'00000000-0000-0000-0000-000000000005','SELECT COUNT(*) FROM A a',revision='1',document_id='cohort')
