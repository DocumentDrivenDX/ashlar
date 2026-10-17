"""Pure original metadata correspondence; inert receipt, no compiler/native work."""
import hashlib,json,unittest,uuid
from dataclasses import replace
from unittest.mock import patch
from ashlar.typed_source_config import load_typed_jsonl_configuration,TypedJsonlConfiguration
from ashlar.source import jsonl_batches
from ashlar.manifest import manifest_pin_vector
from ashlar.weft_binding import LAYOUT_SHA256
from ashlar_host.configured_typed_weft_binding import configured_typed_count_star_request
from ashlar_host.count_star_admission import BACKEND
from test_configured_typed_source_local import inert_receipt,EXAMPLE

class Tests(unittest.TestCase):
    def fixture(self):
        c=load_typed_jsonl_configuration(EXAMPLE)
        batches=tuple(jsonl_batches(c.source_bytes.splitlines(keepends=True),feed=c.feed,epoch=c.epoch))
        bindings={t:{'record':b['record'],'fields':dict(b['fields'])}for t,b in c.bindings.items()}
        c=c.with_receipt(inert_receipt(None,c.model_source,bindings,batches,source_system=c.source_system,schema_revision=c.document_revision))
        roles=('object_current','edge_current','tombstone','whole_source_history');names=['local.original.'+r for r in roles]
        row={'publication_id':'original','profile_version':'ashlar-delta/0.3','table_versions_json':json.dumps({n:i for i,n in enumerate(names)}),
            'schema_revisions_json':json.dumps({c.schema_alias:c.document_revision}),'source_progress_json':json.dumps({c.feed:{'position':'3'}}),
            'validation_report_json':'{"complete":true}','recorded_at':'1'}
        vector=manifest_pin_vector(row,{n:str(uuid.uuid5(uuid.NAMESPACE_URL,n))for n in names},authority='original')
        aliases={n:'spark_catalog.ashlar_configured_query_test.'+n.split('.')[-1]for n in names}
        return c,row,vector,str(uuid.uuid5(uuid.NAMESPACE_URL,'manifest')),aliases
    def test_original_bytes_member_order_complete_vector_and_optional_home(self):
        args=self.fixture();c,row,vector,manifest_uuid,aliases=args
        sql='SELECT i.quantity, i.amount, i.active, i.note FROM Item i'
        request=configured_typed_count_star_request(*args,sql,layout_sha256=LAYOUT_SHA256)
        self.assertEqual(request['sql'],sql);self.assertEqual(request['modules'][0]['documentJson'].encode(),c.model_source)
        self.assertEqual(request['modules'][0]['pin'],{'documentId':'typed-source-example','revision':'1','umfVersion':'0.8.0','sha256':c.model_sha256})
        for key in ('backendId','backendVersion','targetProfile'):self.assertEqual(request['target'][key],BACKEND[key])
        text=request['target']['bindingJson'];self.assertEqual(hashlib.sha256(text.encode()).hexdigest(),request['target']['bindingSha256'])
        b=json.loads(text);record=b['records'][0]
        self.assertEqual((record['sourceSystem'],record['typeId'],record['schemaRevision']),('typed-source','17','1'))
        self.assertEqual([p['logical']['element']for p in record['properties']],['quantity','amount','active','note'])
        self.assertEqual([p['home']['propertyId']for p in record['properties']],['21','22','23','24'])
        self.assertEqual(record['properties'][-1]['home']['encoding'],'ashlar-weft-json-native-null/0.1-candidate')
        self.assertEqual(len(b['publication']['tables']),4)
        for item,name in zip(b['publication']['tables'],sorted(vector.targets)):
            self.assertEqual(item,{'name':aliases[name].split('.'),'uuid':vector.targets[name][0],'version':vector.targets[name][1]})
        self.assertEqual(b['publication']['manifestUuid'],manifest_uuid)
        # The request is detached from caller-owned inputs.
        b['records'].clear();self.assertTrue(json.loads(request['target']['bindingJson'])['records']);c.verify()
    def test_missing_admission_vector_alias_revision_and_closing_drift_refuse(self):
        args=list(self.fixture());sql='SELECT COUNT(*) FROM Item i'
        variants=[]
        a=list(args);a[0]=replace(a[0],policy=None,original_receipt=None);variants.append(a)
        a=list(args);a[1]={**a[1],'schema_revisions_json':'{"typed-items":"2"}'};variants.append(a)
        a=list(args);a[4]={};variants.append(a)
        a=list(args);a[3]=next(iter(a[2].targets.values()))[0];variants.append(a)
        for a in variants:
            with self.subTest(selection=a[3]),self.assertRaises(ValueError):configured_typed_count_star_request(*a,sql,layout_sha256=LAYOUT_SHA256)
        verify=TypedJsonlConfiguration.verify;calls=[]
        def closing(c):
            calls.append(c);verify(c)
            if len(calls)==2:raise ValueError('fresh closing drift')
        with patch.object(TypedJsonlConfiguration,'verify',closing),self.assertRaises(ValueError):
            configured_typed_count_star_request(*args,sql,layout_sha256=LAYOUT_SHA256)
        self.assertEqual(len(calls),2)

if __name__=='__main__':unittest.main()
