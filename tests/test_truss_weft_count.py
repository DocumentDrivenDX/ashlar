"""Small accepted-catalog/global-count IR controls; no native execution."""
import copy
import gzip
import json
import unittest
import test_truss_graph_admission as cases
from ashlar.weft_decode import decode_exact_scalar
from ashlar_host.delta_custody import encoded
from ashlar_host.truss_graph_admission import TrussGraphAdmission
from ashlar_host.truss_weft_query import truss_string_compile_request,admit_truss_global_count


class Tests(unittest.TestCase):
    def test_actual_record_allocation_count_and_closed_ir_refusals(self):
        batch,catalogs=cases.Tests().original();admission=TrussGraphAdmission(batch,catalogs)
        installation=json.loads(batch.begin)['prior']['installationId']
        tables={f'local.truss.{role}':0 for role in ('object_current','edge_current','tombstone','whole_source_history')}
        manifest={'publication_id':'original','profile_version':'ashlar-delta/0.3','recorded_at':'1',
            'table_versions_json':encoded(tables),'schema_revisions_json':encoded({installation:'1'}),
            'source_progress_json':'{"feed":{"position":"1"}}','validation_report_json':encoded({'complete':True,'source_admission':admission.metadata()})}
        uuids={table:'00000000-0000-0000-0000-'+str(index).zfill(12)for index,table in enumerate(tables,1)}
        request=truss_string_compile_request(admission,manifest,uuids,'00000000-0000-0000-0000-000000000005','SELECT COUNT(*) AS n FROM A a',revision='1',document_id='cohort')
        binding=json.loads(request['target']['bindingJson']);pin=binding['modelPins'][0];identity=binding['records'][0]['logical']
        with gzip.open('docs/helix/04-build/evidence/commerce-python311-native-20261010/queries-report.json.gz','rt')as stream:
            original=json.load(stream)
        artifact=copy.deepcopy(next(query['artifact']for query in original['queries']if query['name']=='global-count'))
        artifact['modelPins']=[pin];artifact['logicalPlan']['modulePins']=[pin]
        artifact['logicalPlan']['source']={'occurrence':'s0','pin':pin,'record':identity}
        artifact['columns'][0]['sourceIdentities']=[identity]
        self.assertEqual(dict(admit_truss_global_count(admission,artifact,binding,revision='1',document_id='cohort')),identity)
        self.assertEqual(decode_exact_scalar(artifact['columns'][0]['representation'],'1',count=True).value,1)
        changed=copy.deepcopy(artifact);changed['logicalPlan']['filters']=[{}]
        with self.assertRaises(ValueError):admit_truss_global_count(admission,changed,binding,revision='1',document_id='cohort')
        changed=copy.deepcopy(artifact);changed['logicalPlan']['outputs'][0]['expression']['op']='sum'
        with self.assertRaises(ValueError):admit_truss_global_count(admission,changed,binding,revision='1',document_id='cohort')
        changed=copy.deepcopy(binding);changed['records'][0]['sourceSystem']='foreign'
        with self.assertRaises(ValueError):admit_truss_global_count(admission,artifact,changed,revision='1',document_id='cohort')
