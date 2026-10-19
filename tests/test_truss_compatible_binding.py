# SPDX-License-Identifier: MIT OR Apache-2.0
import base64,copy,hashlib,json,unittest
from ashlar.truss_native_feed import native_feed_batch
from ashlar_host.delta_custody import encoded
from ashlar_host.truss_graph_admission import TrussGraphAdmission
from ashlar_host.truss_compatible_binding import truss_compatible_compile_request,admit_truss_compatible_revisions,BACKEND,REQUIREMENTS
import test_truss_graph_admission

class Tests(unittest.TestCase):
    def fixture(self,new_required=False,interspersed=False,family='integer',queryable=False):
        batch,catalogs=test_truss_graph_admission.Tests().original();payload=json.loads(batch.begin)
        old=payload['catalogPrerequisites'][0];report=json.loads(base64.b64decode(old['reportBytesBase64']))
        if queryable:
            # Separately authored inert named models; never repair real source bytes.
            artifact=report['acceptedInput']['documents'][0]['artifact'];model=json.loads(base64.b64decode(artifact['bytesBase64']))
            for module in model['modules']:
                for element in module['elements']:
                    if element.get('kind')=='record':element['name']=element['id']
            raw=json.dumps(model).encode();artifact.update(bytesBase64=base64.b64encode(raw).decode(),sha256=hashlib.sha256(raw).hexdigest())
            raw=json.dumps(report).encode();old.update(reportBytesBase64=base64.b64encode(raw).decode(),reportSha256=hashlib.sha256(raw).hexdigest())
        report=copy.deepcopy(report);report['revision']='2'
        catalog=json.loads(catalogs['1']);catalog['catalogRevision']='2'
        artifact=report['acceptedInput']['documents'][0]['artifact'];model=json.loads(base64.b64decode(artifact['bytesBase64']));module=model['modules'][0]
        members=next(e for e in module['elements']if e['id']=='A')['members']
        members.insert(1 if interspersed else len(members),{'module':'m','element':'new-field'})
        module['elements'].append({'id':'new-field','name':'new-field','extensions':{},'kind':'field','scalarType':family,'cardinality':'one','nullability':'required'if new_required else'absent-allowed'})
        raw=json.dumps(model).encode();artifact.update(bytesBase64=base64.b64encode(raw).decode(),sha256=hashlib.sha256(raw).hexdigest())
        prop=copy.deepcopy(next(p for p in report['properties']if p['typeId']=='1'));prop.update(elementId='new-field',propertyId='99');report['properties'].append(prop)
        ref=copy.deepcopy(next(e['reference']for e in catalog['definitions']if e['reference']['kind']=='property'and e['reference']['typeId']=='1'));ref.update(authoredIdentity='new-field',propertyId='99',definitionPin='new-field-pin');catalog['definitions'].append({'reference':ref})
        raw=json.dumps(report).encode();payload['catalogPrerequisites'].append({'revision':'2','reportBytesBase64':base64.b64encode(raw).decode(),'reportSha256':hashlib.sha256(raw).hexdigest()});catalogs['2']=json.dumps(catalog).encode()
        raw=json.dumps(payload).encode();batch=native_feed_batch(raw,sha256=hashlib.sha256(raw).hexdigest(),prior=base64.b64decode(batch.cursor_before),next=base64.b64decode(batch.cursor_after),feed=batch.feed);admission=TrussGraphAdmission(batch,catalogs)
        tables={f'local.truss.{role}':0 for role in ('object_current','edge_current','tombstone','whole_source_history')};installation=json.loads(batch.begin)['prior']['installationId']
        manifest={'publication_id':'original','profile_version':'ashlar-delta/0.3','recorded_at':'1','table_versions_json':encoded(tables),'schema_revisions_json':encoded({installation:'2'}),'source_progress_json':'{"feed":{"position":"1"}}','validation_report_json':encoded({'complete':True,'source_admission':admission.metadata()})}
        uuids={table:'00000000-0000-0000-0000-'+str(index).zfill(12)for index,table in enumerate(tables,1)}
        return admission,manifest,uuids
    def request(self,admission,manifest,uuids,revisions=('1','2')):
        return truss_compatible_compile_request(admission,manifest,uuids,'00000000-0000-0000-0000-000000000005','SELECT a."A-label" FROM A a',revision='2',document_id='cohort',physical_revisions=revisions)
    def artifact(self,binding):
        record=binding['records'][0]
        return {'status':'compiled','backend':BACKEND,'bindingSha256':hashlib.sha256(encoded(binding).encode()).hexdigest(),'modelPins':binding['modelPins'],'logicalPlan':{'joins':[],'requiredCapabilities':[],'source':{'occurrence':'s0','pin':binding['modelPins'][0],'record':record['logical']}},'columns':[],'obligations':[{'id':'ashlar.compatibleSchemaRevisions','owner':'host','failureCode':'WFT-OBLIGATION','parameters':{'records':[{'logical':record['logical'],'primarySchemaRevision':'2','compatibleSchemaRevisions':['1','2']}],'modelPins':binding['modelPins'],'publication':binding['publication'],'requirements':REQUIREMENTS}}]}
    def test_original_models_allocations_and_exact_obligation(self):
        admission,manifest,uuids=self.fixture();request=self.request(admission,manifest,uuids);binding=json.loads(request['target']['bindingJson'])
        self.assertEqual(request['modules'][0]['documentJson'].encode(),admission.revisions['2']['models']['cohort'][1]);self.assertEqual(binding['records'][0]['compatibleSchemaRevisions'],['1','2']);self.assertEqual(request['target']['backendVersion'],BACKEND['backendVersion'])
        result=admit_truss_compatible_revisions(admission,self.artifact(binding),binding,revision='2',document_id='cohort');self.assertEqual(result['physical_revisions'],['1','2'])
        admission,manifest,uuids=self.fixture(queryable=True,family='string');request=self.request(admission,manifest,uuids)
        self.assertEqual(json.loads(request['modules'][0]['documentJson'])['modules'][0]['elements'][0]['name'],'A')
        admission,manifest,uuids=self.fixture(interspersed=True);request=self.request(admission,manifest,uuids);binding=json.loads(request['target']['bindingJson'])
        self.assertEqual(admit_truss_compatible_revisions(admission,self.artifact(binding),binding,revision='2',document_id='cohort')['physical_revisions'],['1','2'])
    def test_drift_new_required_and_malformed_refuse(self):
        admission,manifest,uuids=self.fixture()
        for revisions in (('1',),('2','2'),('0','2'),('2','3'),['1','2']):
            with self.assertRaises(ValueError):self.request(admission,manifest,uuids,revisions)
        request=self.request(admission,manifest,uuids);binding=json.loads(request['target']['bindingJson']);artifact=self.artifact(binding)
        artifact['obligations'][0]['parameters']['requirements']=[]
        with self.assertRaises(ValueError):admit_truss_compatible_revisions(admission,artifact,binding,revision='2',document_id='cohort')
        admission.revisions['2']['types']['1']['fields'][next(iter(admission.revisions['1']['types']['1']['fields']))]='changed'
        with self.assertRaises(ValueError):self.request(admission,manifest,uuids)
        admission,manifest,uuids=self.fixture(new_required=True)
        with self.assertRaises(ValueError):self.request(admission,manifest,uuids)
