# SPDX-License-Identifier: MIT OR Apache-2.0
import base64,hashlib,json,unittest
from pathlib import Path
from ashlar.truss_native_feed import native_feed_batch
from ashlar_host.truss_graph_admission import TrussGraphAdmission

class Tests(unittest.TestCase):
    def original(self,bad_endpoint=False,bad_pin=False):
        root=Path('docs/helix/04-build/evidence/truss-schema-acceptance-20261012')
        report_raw=(root/'report.json').read_bytes();report=json.loads(report_raw);catalog_raw=(root/'catalog.json').read_bytes();catalog=json.loads(catalog_raw)
        refs=[entry['reference']for entry in catalog['definitions']]
        pin=lambda kind,type_id,authored:next(ref['definitionPin']for ref in refs if ref['kind']==kind and ref.get('typeId')==type_id and ref['authoredIdentity']==authored)
        ids=[{'typeId':'1','id':'7','definitionPin':pin('type','1','A')},{'typeId':'2','id':'8','definitionPin':pin('type','2','B')},{'typeId':'1','id':'9','definitionPin':pin('relationship',None,'A-B')}]
        for identity in ids:identity['owner']={'documentId':'cohort','moduleId':'m'}
        rows=[];encode=lambda value:json.dumps(value).encode()
        for index,identity in enumerate(ids):
            edge=index==2;properties=[]
            if not edge:
                properties=[{'propertyId':prop['propertyId'],'definitionPin':pin('property',identity['typeId'],prop['elementId']),'value':{'kind':'string','text':'exact-'+prop['elementId']}}for prop in report['properties']if prop['typeId']==identity['typeId']]
            image={'interfaceVersion':'truss-history-record/0.1.0','kind':'edge'if edge else'object','identity':dict(identity),'recordVersion':'1','catalogRevision':'1','properties':properties,'retained':[]}
            if edge:image.update(source=ids[0],target=ids[0]if bad_endpoint else ids[1],orderKey={'state':'null'})
            else:image['ownership']={'state':'rootless'}
            if bad_pin and edge:image['identity']['definitionPin']='0'*64
            event={'interfaceVersion':'truss-history-event/0.2.0-proposal','sourceEpoch':'epoch','xid':'42','seq':str(index+1),'eventVersion':'1','eventCatalogRevision':'1','identity':identity,'operation':'create','after':image}
            raw=encode(event)
            rows.append({'seq':index+1,'xid':'42','entity_kind':'e'if edge else'o','entity_id':int(identity['id']),'entity_type':int(identity['typeId']),'ver':1,'new_value':{'interfaceVersion':'truss-journal-exact-bytes/0.2.0-proposal','component':'event','bytesBase64':base64.b64encode(raw).decode(),'bytesSha256':hashlib.sha256(raw).hexdigest()}})
        prior={'profile':'truss-caller-asserted-feed/0.1','installationId':report['installationId'],'sourceEpoch':'epoch','consumerId':'consumer','registrationId':'registration','nextXid':'0'};following={**prior,'nextXid':'43'}
        payload={'interfaceVersion':'truss-native-feed/0.1','prior':prior,'next':following,'writerXid':'42','changes':rows,'configuration':{},'catalogPrerequisites':[{'revision':'1','reportBytesBase64':base64.b64encode(report_raw).decode(),'reportSha256':hashlib.sha256(report_raw).hexdigest()}],'revisions':[],'sources':[],'reservations':[]}
        content=encode(payload)
        return native_feed_batch(content,sha256=hashlib.sha256(content).hexdigest(),prior=encode(prior),next=encode(following),feed='feed'),{'1':catalog_raw}
    def test_actual_retained_report_umf_catalog_ids_and_pins_admit_graph(self):
        batch,catalogs=self.original();admission=TrussGraphAdmission(batch,catalogs)
        self.assertEqual(len(admission.changes),3);self.assertEqual(admission.metadata()['declared_umf'],{'1':{'cohort':'0.7.0'}})
        self.assertIsNone(admission.admit(admission.changes[2]))
    def test_foreign_endpoint_or_relationship_pin_refuses(self):
        for arguments in ({'bad_endpoint':True},{'bad_pin':True}):
            with self.subTest(arguments=arguments):
                batch,catalogs=self.original(**arguments)
                with self.assertRaises(PermissionError):TrussGraphAdmission(batch,catalogs)
    def test_scoped_catalog_cannot_supply_complete_graph_interpretation(self):
        batch,catalogs=self.original();catalog=json.loads(catalogs['1'])
        catalog['scope']={'kind':'module','documentId':'cohort','moduleId':'m'}
        with self.assertRaisesRegex(ValueError,'Complete public historical catalog view required'):
            TrussGraphAdmission(batch,{'1':json.dumps(catalog).encode()})
