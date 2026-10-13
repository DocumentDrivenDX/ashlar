import json,unittest
from ashlar_host.truss_record_read import read_original_truss_record
import test_truss_singleton

class Tests(unittest.TestCase):
    def fixture(self,present=True):
        driver,row,state,calls=test_truss_singleton.Tests().fixture(present=present)
        row['schema_revision']='1';row['props_json']='{"1":"雪"}'
        if present:driver.policy.active['expected']['object_current']=[dict(row)]
        fields=[{'id':'label','kind':'field','scalarType':'string','nullability':'required'},{'id':'tag','kind':'field','scalarType':'string','nullability':'absent-allowed'}]
        model={'modules':[{'id':'m','elements':[{'id':'B','kind':'record','members':[{'module':'m','element':f['id']}for f in fields]},*fields]}]}
        properties=[{'typeId':'1','moduleId':'m','elementId':f['id'],'propertyId':str(i+1)}for i,f in enumerate(fields)]
        definitions=[{'reference':{'kind':'property','owner':{'documentId':'cohort','moduleId':'m'},'typeId':'1','authoredIdentity':f['id'],'propertyId':str(i+1),'definitionPin':'pin'+str(i+1)}}for i,f in enumerate(fields)]
        driver.source_admission.revisions={'1':{'types':{'1':{'owner':('cohort','m','B'),'fields':{'1':'pin1','2':'pin2'}}},'models':{'cohort':(model,b'original')},'report':json.dumps({'properties':properties}).encode()}}
        driver.source_admission.catalog_views={'1':json.dumps({'definitions':definitions}).encode()}
        return driver,row
    def test_complete_unicode_fields_optional_absence_and_presence_and_missing_row(self):
        driver,row=self.fixture();result=read_original_truss_record(driver,type_id=1,entity_id=2)
        self.assertEqual(result['row'],row);self.assertEqual([f['state']for f in result['fields']],['value','absent']);self.assertEqual(result['fields'][0]['value'],'雪')
        row['props_json']='{"1":"雪","2":"reviewed"}';driver.policy.active['expected']['object_current']=[dict(row)]
        self.assertEqual(read_original_truss_record(driver,type_id=1,entity_id=2)['fields'][1]['value'],'reviewed')
        # Inert already-authorized carrier observation; not native nullability admission.
        row['props_json']='{"1":"雪","2":null}';driver.policy.active['expected']['object_current']=[dict(row)]
        observation=read_original_truss_record(driver,type_id=1,entity_id=2)['fields'][1]
        self.assertEqual(observation['state'],'null');self.assertIsNone(observation['value'])
        driver,_=self.fixture(False);self.assertIsNone(read_original_truss_record(driver,type_id=1,entity_id=2))
    def test_unknown_allocation_and_scalar_and_pin_refuse(self):
        for fault in ('unknown','scalar','allocation'):
            driver,row=self.fixture()
            if fault=='unknown':row['props_json']='{"1":"雪","9":"foreign"}'
            elif fault=='scalar':row['props_json']='{"1":12}'
            else:
                report=json.loads(driver.source_admission.revisions['1']['report']);report['properties'][0]['propertyId']='9';driver.source_admission.revisions['1']['report']=json.dumps(report).encode()
            driver.policy.active['expected']['object_current']=[dict(row)]
            with self.assertRaises(PermissionError):read_original_truss_record(driver,type_id=1,entity_id=2)
