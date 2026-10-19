# SPDX-License-Identifier: MIT OR Apache-2.0
import base64,hashlib,json,unittest,copy
from dataclasses import replace
from ashlar.truss_native_feed import native_feed_batch
from ashlar_host.truss_graph_admission import TrussGraphAdmission,TYPED_PROFILE
import test_truss_graph_admission

class Tests(unittest.TestCase):
    def fixture(self,family='integer',nullability='required',value=None,facets=None):
        batch,catalogs=test_truss_graph_admission.Tests().original();payload=json.loads(batch.begin);prereq=payload['catalogPrerequisites'][0];report=json.loads(base64.b64decode(prereq['reportBytesBase64']));catalog=json.loads(catalogs['1'])
        artifact=report['acceptedInput']['documents'][0]['artifact'];model=json.loads(base64.b64decode(artifact['bytesBase64']));module=model['modules'][0];record=next(e for e in module['elements']if e['id']=='A');record['members'].append({'module':module['id'],'element':'typed'})
        module['elements'].append({'id':'typed','kind':'field','scalarType':family,'nullability':nullability,'cardinality':'one'})
        if facets is not None:module['elements'][-1]['facets']=facets
        raw=json.dumps(model).encode();artifact.update(bytesBase64=base64.b64encode(raw).decode(),sha256=hashlib.sha256(raw).hexdigest())
        prop=dict(next(p for p in report['properties']if p['typeId']=='1'));prop.update(elementId='typed',propertyId='99');report['properties'].append(prop)
        ref=dict(next(e['reference']for e in catalog['definitions']if e['reference']['kind']=='property'and e['reference']['typeId']=='1'));ref.update(authoredIdentity='typed',propertyId='99',definitionPin='typed-pin');catalog['definitions'].append({'reference':ref})
        row=payload['changes'][0];wrapper=row['new_value'];event=json.loads(base64.b64decode(wrapper['bytesBase64']));event['after']['properties'].append({'propertyId':'99','definitionPin':'typed-pin','value':value or {'kind':'integer','text':'9007199254740993'}})
        raw=json.dumps(event).encode();wrapper.update(bytesBase64=base64.b64encode(raw).decode(),bytesSha256=hashlib.sha256(raw).hexdigest());raw=json.dumps(report).encode();prereq.update(reportBytesBase64=base64.b64encode(raw).decode(),reportSha256=hashlib.sha256(raw).hexdigest());content=json.dumps(payload).encode()
        return native_feed_batch(content,sha256=hashlib.sha256(content).hexdigest(),prior=base64.b64decode(batch.cursor_before),next=base64.b64decode(batch.cursor_after),feed=batch.feed),{'1':json.dumps(catalog).encode()}
    def test_exact_declared_typed_field_and_nullable_values_admit(self):
        for family,nullability,value in (('integer','required',{'kind':'integer','text':'9007199254740993'}),('decimal','absent-allowed',{'kind':'decimal','text':'1.2300'}),('boolean','required',{'kind':'boolean','value':True}),('string','required',{'kind':'string','text':'雪'})):
            batch,catalogs=self.fixture(family,nullability,value);admission=TrussGraphAdmission(batch,catalogs)
            self.assertEqual(admission.metadata()['profile'],TYPED_PROFILE if family!='string'else 'ashlar-truss-scalar-graph-semantic-admission/0.1');self.assertEqual(admission.revisions['1']['types']['1']['domains']['99'],(family,nullability))
    def test_wrong_family_and_forbidden_null_refuse(self):
        for family,nullability,value in (('integer','required',{'kind':'string','text':'1'}),('string','absent-allowed',{'kind':'null'}),('string','required',{'kind':'null'})):
            batch,catalogs=self.fixture(family,nullability,value)
            with self.assertRaises(PermissionError):TrussGraphAdmission(batch,catalogs)

        batch,catalogs=self.fixture('string','nullable',{'kind':'null'})
        with self.assertRaises(ValueError):TrussGraphAdmission(batch,catalogs)
    def test_declared_numeric_facets_preserve_exact_values_and_refuse_loss(self):
        signed={'integerWidth':{'bits':64,'signed':True}};decimal={'precision':18,'scale':4}
        for family,facets,tokens in (('integer',signed,['9007199254740993','-9223372036854775808','9223372036854775807']),('decimal',decimal,['1.2300','1.23000','1.2','123e-2','99999999999999.9999','0e999999999999'])):
            for token in tokens:
                batch,catalogs=self.fixture(family,value={'kind':family,'text':token},facets=facets);admission=TrussGraphAdmission(batch,catalogs)
                self.assertEqual(admission.revisions['1']['types']['1']['facets']['99'],facets)
                self.assertEqual(admission.revisions['1']['types']['1']['domains']['99'],(family,'required'))
                event=json.loads(base64.b64decode(json.loads(batch.begin)['changes'][0]['new_value']['bytesBase64']))
                self.assertEqual(next(p['value']['text']for p in event['after']['properties']if p['propertyId']=='99'),token)
        for family,facets,token in (('integer',signed,'9223372036854775808'),('integer',signed,'-9223372036854775809'),('decimal',decimal,'1.23001'),('decimal',decimal,'100000000000000.0000')):
            batch,catalogs=self.fixture(family,value={'kind':family,'text':token},facets=facets)
            with self.assertRaises(PermissionError):TrussGraphAdmission(batch,catalogs)
        for family,facets in (('integer',{'integerWidth':{'bits':32,'signed':True}}),('decimal',{'precision':19,'scale':4}),('boolean',{'integerWidth':{'bits':64,'signed':True}})):
            batch,catalogs=self.fixture(family,value={'kind':family,'value':True}if family=='boolean'else None,facets=facets)
            with self.assertRaises(ValueError):TrussGraphAdmission(batch,catalogs)
        batch,catalogs=self.fixture(facets=signed);admission=TrussGraphAdmission(batch,catalogs);change=admission.changes[0]
        admission.revisions['2']=copy.deepcopy(admission.revisions['1']);admission.revisions['2']['types']['1']['facets']['99']={}
        with self.assertRaisesRegex(PermissionError,'additive accepted'):admission.admit_transition(replace(change.state,schema_revision='2'),change)
