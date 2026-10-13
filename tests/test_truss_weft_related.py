"""Accepted Truss key/edge oracle controls; no native relationship execution."""
import copy
import gzip
import json
import unittest
import test_truss_graph_admission as cases
from ashlar.apply import empty_state,plan_apply
from ashlar_host.delta_custody import encoded
from ashlar_host.schema_rows import fixture_columns
from ashlar_host.resources import RESOURCE_ROOT
from ashlar_host.truss_graph_admission import TrussGraphAdmission
from ashlar_host.truss_note_publication import graph_expected_rows
from ashlar_host.truss_weft_query import truss_string_compile_request,admit_truss_related_keys


class Tests(unittest.TestCase):
    def test_actual_relationship_ids_authored_key_order_and_occurrence_prefix(self):
        batch,catalogs=cases.Tests().original();admission=TrussGraphAdmission(batch,catalogs)
        installation=json.loads(batch.begin)['prior']['installationId']
        tables={f'local.truss.{role}':0 for role in ('object_current','edge_current','tombstone','whole_source_history')}
        manifest={'publication_id':'original','profile_version':'ashlar-delta/0.3','recorded_at':'1',
            'table_versions_json':encoded(tables),'schema_revisions_json':encoded({installation:'1'}),
            'source_progress_json':'{"feed":{"position":"1"}}','validation_report_json':encoded({'complete':True,'source_admission':admission.metadata()})}
        uuids={table:'00000000-0000-0000-0000-'+str(index).zfill(12)for index,table in enumerate(tables,1)}
        request=truss_string_compile_request(admission,manifest,uuids,'00000000-0000-0000-0000-000000000005','SELECT RELATED_KEYS(a."A-B", 2) AS related FROM A a',revision='1',document_id='cohort')
        binding=json.loads(request['target']['bindingJson']);mapping=binding['relationships'][0]
        self.assertEqual(mapping['typeId'],next(iter(admission.revisions['1']['edges'])))
        self.assertEqual(mapping['acceptedDefinition'],admission.revisions['1']['edges'][mapping['typeId']]['declaration'])
        pin=binding['modelPins'][0];logical=lambda element:{'documentId':'cohort','revision':pin['revision'],'module':'m','element':element}
        key=lambda prefix:{'id':'a-primary','fields':[logical(prefix+'-value'),logical(prefix+'-label')],'types':[{'family':'string','facets':{},'nullable':False}]*2}
        declaration=mapping['acceptedDefinition'];relation={'from':mapping['source'],'to':mapping['target'],'identity':mapping['logical'],'inverse':False,
            'sourceKey':key('A'),'targetKey':key('B'),'sourceMultiplicity':declaration['sourceMultiplicity'],'targetMultiplicity':declaration['targetMultiplicity'],'targetLifecycle':declaration['targetLifecycle']}
        with gzip.open('docs/helix/04-build/evidence/commerce-python311-native-20261010/queries-report.json.gz','rt')as stream:original=json.load(stream)
        artifact=copy.deepcopy(original['relationship']['artifact']);artifact['modelPins']=[pin]
        plan=artifact['logicalPlan'];plan['order']=[];plan['modulePins']=[pin];plan['source']={'occurrence':'s0','pin':pin,'record':mapping['source']}
        plan['outputs']=[{'name':'related','expression':{'op':'relatedKeys','scan':'s0','bound':2,'relationship':relation}}]
        artifact['columns']=[{'position':1,'outputName':'related','nullable':False,'representation':{'kind':'relatedKeys','bound':2,'relationship':mapping['logical'],'key':key('B')},'sourceIdentities':[mapping['source'],mapping['target'],*key('B')['fields']]}]
        columns=fixture_columns(RESOURCE_ROOT);previous={role:[]for role in columns}
        after=plan_apply(empty_state(),admission.changes,schema_policy=admission.admit)
        expected=graph_expected_rows(previous,after,admission.changes,batch,columns,'2026-10-12T00:00:00+00:00',admission.parsed.event_bytes,admission.parsed.edge_order_keys)
        rows=admit_truss_related_keys(admission,artifact,binding,expected,revision='1',document_id='cohort')
        self.assertEqual(rows[0]['related'],{'items':[['exact-B-value','exact-B-label']],'truncated':False})
        # The pure oracle retains equal key occurrences from distinct edge IDs;
        # native relationship multiplicity guards remain independently required.
        changed=copy.deepcopy(expected);changed['edge_current'].append(dict(changed['edge_current'][0],id='10'))
        rows=admit_truss_related_keys(admission,artifact,binding,changed,revision='1',document_id='cohort')
        self.assertEqual(len(rows[0]['related']['items']),2)
        wrong=copy.deepcopy(artifact);wrong['columns'][0]['representation']['key']['fields'].reverse()
        with self.assertRaises(ValueError):admit_truss_related_keys(admission,wrong,binding,expected,revision='1',document_id='cohort')
        wrong=copy.deepcopy(artifact);wrong['logicalPlan']['filters']=[{}]
        with self.assertRaises(ValueError):admit_truss_related_keys(admission,wrong,binding,expected,revision='1',document_id='cohort')

        wrong=copy.deepcopy(artifact);wrong['logicalPlan']['order']=[{}]
        with self.assertRaises(ValueError):admit_truss_related_keys(admission,wrong,binding,expected,revision='1',document_id='cohort')
        wrong=copy.deepcopy(expected);wrong['object_current'][1]['schema_revision']='2'
        with self.assertRaises(ValueError):admit_truss_related_keys(admission,artifact,binding,wrong,revision='1',document_id='cohort')
