# SPDX-License-Identifier: MIT OR Apache-2.0
"""Public Weft0.2 observed filter shape; inert accepted-source controls."""
import copy,json,unittest
from types import SimpleNamespace
from unittest.mock import patch
import test_truss_graph_admission as cases
from ashlar.apply import empty_state,plan_apply
from ashlar_host.truss_graph_admission import TrussGraphAdmission
from ashlar_host.truss_weft_query import truss_string_compile_request,_execute_string_artifact
from ashlar_host.truss_relationship_filter import admit_truss_relationship_filter
from ashlar_host.delta_custody import encoded
from ashlar_host.schema_rows import fixture_columns
from ashlar_host.resources import RESOURCE_ROOT
from ashlar_host.truss_note_publication import graph_expected_rows
OBSERVED={'status': 'compiled', 'interfaceVersion': 'weft-compile/0.2.0', 'dialect': 'weft-sql/0.2.0', 'compilerVersion': 'weft/0.1.0', 'modelPins': [{'documentId': 'cohort', 'revision': '1578697fb45f9c863e567ab01bed269bf1a8b2836898d71368177361d5f3c70a', 'sha256': '1578697fb45f9c863e567ab01bed269bf1a8b2836898d71368177361d5f3c70a', 'umfVersion': '0.7.0'}], 'logicalPlan': {'aggregate': False, 'filters': [{'key': [{'kind': 'literal', 'span': {'end': 77, 'start': 68}, 'type': {'facets': {}, 'family': 'string', 'nullable': False}, 'value': 'Value-B'}, {'kind': 'literal', 'span': {'end': 84, 'start': 78}, 'type': {'facets': {}, 'family': 'string', 'nullable': False}, 'value': 'Beta'}], 'op': 'hasRelated', 'relationship': {'from': {'documentId': 'cohort', 'element': 'A', 'module': 'm', 'revision': '1578697fb45f9c863e567ab01bed269bf1a8b2836898d71368177361d5f3c70a'}, 'identity': {'documentId': 'cohort', 'module': 'm', 'relationship': 'A-B', 'revision': '1578697fb45f9c863e567ab01bed269bf1a8b2836898d71368177361d5f3c70a'}, 'inverse': False, 'sourceKey': {'fields': [{'documentId': 'cohort', 'element': 'A-value', 'module': 'm', 'revision': '1578697fb45f9c863e567ab01bed269bf1a8b2836898d71368177361d5f3c70a'}, {'documentId': 'cohort', 'element': 'A-label', 'module': 'm', 'revision': '1578697fb45f9c863e567ab01bed269bf1a8b2836898d71368177361d5f3c70a'}], 'id': 'a-primary', 'types': [{'facets': {}, 'family': 'string', 'nullable': False}, {'facets': {}, 'family': 'string', 'nullable': False}]}, 'sourceMultiplicity': {'max': 1, 'min': 0}, 'targetKey': {'fields': [{'documentId': 'cohort', 'element': 'B-value', 'module': 'm', 'revision': '1578697fb45f9c863e567ab01bed269bf1a8b2836898d71368177361d5f3c70a'}, {'documentId': 'cohort', 'element': 'B-label', 'module': 'm', 'revision': '1578697fb45f9c863e567ab01bed269bf1a8b2836898d71368177361d5f3c70a'}], 'id': 'a-primary', 'types': [{'facets': {}, 'family': 'string', 'nullable': False}, {'facets': {}, 'family': 'string', 'nullable': False}]}, 'targetLifecycle': 'independent', 'targetMultiplicity': {'max': 2, 'min': 0}, 'to': {'documentId': 'cohort', 'element': 'B', 'module': 'm', 'revision': '1578697fb45f9c863e567ab01bed269bf1a8b2836898d71368177361d5f3c70a'}}, 'scan': 's0'}], 'groups': [], 'irVersion': 'weft-ir/0.2.0', 'joins': [], 'limit': None, 'modulePins': [{'documentId': 'cohort', 'revision': '1578697fb45f9c863e567ab01bed269bf1a8b2836898d71368177361d5f3c70a', 'sha256': '1578697fb45f9c863e567ab01bed269bf1a8b2836898d71368177361d5f3c70a', 'umfVersion': '0.7.0'}], 'order': [], 'outputs': [{'expression': {'identity': {'documentId': 'cohort', 'element': 'A-label', 'module': 'm', 'revision': '1578697fb45f9c863e567ab01bed269bf1a8b2836898d71368177361d5f3c70a'}, 'op': 'field', 'scan': 's0'}, 'name': 'label'}], 'pageKey': None, 'readProfile': None, 'requiredCapabilities': ['filter', 'project', 'relationship.exists', 'scan', 'type.string'], 'source': {'occurrence': 's0', 'pin': {'documentId': 'cohort', 'revision': '1578697fb45f9c863e567ab01bed269bf1a8b2836898d71368177361d5f3c70a', 'sha256': '1578697fb45f9c863e567ab01bed269bf1a8b2836898d71368177361d5f3c70a', 'umfVersion': '0.7.0'}, 'record': {'documentId': 'cohort', 'element': 'A', 'module': 'm', 'revision': '1578697fb45f9c863e567ab01bed269bf1a8b2836898d71368177361d5f3c70a'}}, 'typeGraph': [{'availability': 'required', 'identity': {'documentId': 'cohort', 'element': 'A-label', 'module': 'm', 'revision': '1578697fb45f9c863e567ab01bed269bf1a8b2836898d71368177361d5f3c70a'}, 'kind': 'scalar', 'type': {'facets': {}, 'family': 'string', 'nullable': False}}]}, 'columns': [{'nullable': False, 'outputName': 'label', 'position': 1, 'representation': {'carrier': 'text', 'decoder': 'text', 'kind': 'scalar', 'logicalType': {'facets': {}, 'family': 'string', 'nullable': False}}, 'sourceIdentities': [{'documentId': 'cohort', 'element': 'A-label', 'module': 'm', 'revision': '1578697fb45f9c863e567ab01bed269bf1a8b2836898d71368177361d5f3c70a'}]}]}

class Tests(unittest.TestCase):
    def fixture(self):
        batch,catalogs=cases.Tests().original();admission=TrussGraphAdmission(batch,catalogs);installation=json.loads(batch.begin)['prior']['installationId']
        tables={f'local.truss.{role}':0 for role in ('object_current','edge_current','tombstone','whole_source_history')}
        manifest={'publication_id':'original','profile_version':'ashlar-delta/0.3','recorded_at':'1','table_versions_json':encoded(tables),'schema_revisions_json':encoded({installation:'1'}),'source_progress_json':'{"feed":{"position":"1"}}','validation_report_json':encoded({'complete':True,'source_admission':admission.metadata()})}
        uuids={table:'00000000-0000-0000-0000-'+str(index).zfill(12)for index,table in enumerate(tables,1)}
        request=truss_string_compile_request(admission,manifest,uuids,'00000000-0000-0000-0000-000000000005',"""SELECT a."A-label" AS label FROM A a WHERE HAS_RELATED(a."A-B", KEY('exact-B-value','exact-B-label'))""",revision='1',document_id='cohort')
        binding=json.loads(request['target']['bindingJson']);pin=binding['modelPins'][0]
        original_pin=OBSERVED['modelPins'][0]
        artifact=json.loads(json.dumps(OBSERVED).replace(original_pin['revision'],pin['revision']))
        predicate=artifact['logicalPlan']['filters'][0]
        for literal,value in zip(predicate['key'],('exact-B-value','exact-B-label')):literal['value']=value
        columns=fixture_columns(RESOURCE_ROOT);previous={role:[]for role in columns};after=plan_apply(empty_state(),admission.changes,schema_policy=admission.admit)
        expected=graph_expected_rows(previous,after,admission.changes,batch,columns,'2026-10-12T00:00:00+00:00',admission.parsed.event_bytes,admission.parsed.edge_order_keys)
        return admission,artifact,binding,expected

    def test_compound_key_exact_match_source_once_and_refusal(self):
        admission,artifact,binding,expected=self.fixture()
        run=lambda item,data=expected,mapping=binding:admit_truss_relationship_filter(admission,item,mapping,data,revision='1',document_id='cohort')
        self.assertEqual(run(artifact),[{'label':'exact-A-label'}])
        # Add a key-compatible second target, same first component but different
        # second component. EXISTS still emits the source only once.
        target=next(item for item in binding['records']if item['logical']['element']=='B');homes={item['logical']['element']:item['home']['propertyId']for item in target['properties']}
        changed=copy.deepcopy(expected);row=next(item for item in changed['object_current']if item['type_id']==target['typeId']);props=json.loads(row['props_json']);props[homes['B-label']]='other-label'
        new_id='30';changed['object_current'].append(dict(row,id=new_id,props_json=encoded(props)))
        changed['edge_current'].append(dict(changed['edge_current'][0],id='31',target_id=new_id))
        self.assertEqual(run(artifact,changed),[{'label':'exact-A-label'}])
        missing=copy.deepcopy(artifact);missing['logicalPlan']['filters'][0]['key'][1]['value']='missing'
        self.assertEqual(run(missing,changed),[])
        wrong=copy.deepcopy(artifact);wrong['logicalPlan']['filters'][0]['relationship']['targetKey']['fields'].reverse()
        with self.assertRaises(ValueError):run(wrong)
        wrong=copy.deepcopy(artifact);wrong['interfaceVersion']='weft-compile/9.0.0'
        with self.assertRaises(ValueError):run(wrong)
        wrong=copy.deepcopy(binding);wrong['relationships'][0]['typeId']=999
        with self.assertRaises(ValueError):run(artifact,mapping=wrong)
        wrong=copy.deepcopy(binding);wrong['relationships'][0]['table']=binding['records'][0]['table']
        with self.assertRaises(ValueError):run(artifact,mapping=wrong)
        wrong=copy.deepcopy(expected);wrong['object_current'].append(copy.deepcopy(wrong['object_current'][0]))
        with self.assertRaises(ValueError):run(artifact,data=wrong)
        artifact.update(parameters=[],sql='WITH original SELECT original',obligations=[
            {'id':'ashlar.candidate.scalarIntegrity','parameters':{'checks':[{'sql':'SELECT scalar','failureCode':'WFT-BINDING','record':binding['records'][0]['logical']}]}},
            {'id':'ashlar.candidate.relationshipIntegrity','parameters':{'checks':[{'sql':'SELECT relationship','failureCode':'WFT-BINDING','relationship':binding['relationships'][0]['logical']}]}}])
        calls=[]
        def sql(statement,**kwargs):calls.append(statement);return statement
        def capture(statement,**kwargs):
            return {'schema':[['label','STRING']],'rows':[['exact-A-label']]}if statement==artifact['sql']else {'schema':[['violations','STRING']],'rows':[['0']]}
        with patch('ashlar_host.path_capture.capture_string_frame',side_effect=capture):
            rows,_=_execute_string_artifact(SimpleNamespace(sql=sql),artifact,2,relationship_guards=True)
        self.assertEqual(rows,[{'label':'exact-A-label'}])
        self.assertEqual(calls,['SELECT scalar','SELECT relationship',artifact['sql']])

    def inverse_fixture(self):
        admission,artifact,binding,expected=self.fixture()
        # Pure declaration candidate only; no rewrite of retained accepted evidence.
        mapping=binding['relationships'][0];mapping['acceptedDefinition']['inverse']='B-A'
        admission.revisions['1']['edges'][mapping['typeId']]['declaration']['inverse']='B-A'
        plan=artifact['logicalPlan'];predicate=plan['filters'][0];relation=predicate['relationship']
        relation['from'],relation['to']=relation['to'],relation['from']
        relation['sourceKey'],relation['targetKey']=relation['targetKey'],relation['sourceKey']
        relation['inverse']=True
        plan['requiredCapabilities'].insert(3,'relationship.inverse')
        plan['source']['record']=mapping['target']
        identity=dict(plan['outputs'][0]['expression']['identity'],element='B-label')
        plan['outputs'][0]['expression']['identity']=identity
        artifact['columns'][0]['sourceIdentities']=[identity]
        plan['typeGraph'][0]['identity']=identity
        for literal,value in zip(predicate['key'],('exact-A-value','exact-A-label')):literal['value']=value
        return admission,artifact,binding,expected

    def test_inverse_exists_selects_source_once(self):
        admission,artifact,binding,expected=self.inverse_fixture()
        expected['edge_current'].append(dict(expected['edge_current'][0],id='12'))
        self.assertEqual(admit_truss_relationship_filter(admission,artifact,binding,expected,
                         revision='1',document_id='cohort'),[{'label':'exact-B-label'}])
        artifact['logicalPlan']['filters'][0]['key'][1]['value']='missing'
        self.assertEqual(admit_truss_relationship_filter(admission,artifact,binding,expected,
                         revision='1',document_id='cohort'),[])

    def test_inverse_name_capability_and_key_correspondence_required(self):
        for change in ('name','capability','key'):
            admission,artifact,binding,expected=self.inverse_fixture()
            if change=='name':del admission.revisions['1']['edges'][binding['relationships'][0]['typeId']]['declaration']['inverse']
            elif change=='capability':artifact['logicalPlan']['requiredCapabilities'].remove('relationship.inverse')
            else:artifact['logicalPlan']['filters'][0]['relationship']['targetKey']['fields'].reverse()
            with self.subTest(change=change),self.assertRaises(ValueError):
                admit_truss_relationship_filter(admission,artifact,binding,expected,revision='1',document_id='cohort')
