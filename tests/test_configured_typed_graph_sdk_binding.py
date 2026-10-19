# SPDX-License-Identifier: MIT OR Apache-2.0
"""Required-STRING metadata projection only; no SDK/native execution."""
import hashlib,json,unittest,uuid
from pathlib import Path
from unittest.mock import patch
from ashlar.manifest import manifest_pin_vector
from ashlar.typed_graph_source import build_typed_graph_request
from ashlar.typed_graph_source_config import load_typed_graph_jsonl_configuration,TypedGraphJsonlConfiguration
from ashlar.weft_binding import LAYOUT_SHA256
from ashlar_host.configured_typed_graph_weft_binding import configured_typed_graph_count_star_request
from ashlar_host.configured_typed_graph_sdk_binding import configured_typed_graph_sdk_request
from test_configured_typed_graph_projection_oracle import inert_receipt

class Tests(unittest.TestCase):
    def fixture(self):
        path=Path(__file__).resolve().parents[1]/'examples/end-to-end/configured-typed-graph-source.json'
        c=load_typed_graph_jsonl_configuration(path);c=c.with_receipt(inert_receipt(c,build_typed_graph_request(c)))
        names=['local.original.'+r for r in ('object_current','edge_current','tombstone','whole_source_history')]
        row={'publication_id':'original','profile_version':'ashlar-delta/0.3','table_versions_json':json.dumps({n:i for i,n in enumerate(names)}),
            'schema_revisions_json':json.dumps({c.schema_alias:c.document_revision}),'source_progress_json':json.dumps({c.feed:{'position':'3'}}),'validation_report_json':'{"complete":true}','recorded_at':'1'}
        vector=manifest_pin_vector(row,{n:str(uuid.uuid5(uuid.NAMESPACE_URL,n))for n in names},authority='original')
        aliases={n:'spark_catalog.ashlar_configured_query_test.'+n.split('.')[-1]for n in names}
        return [c,row,vector,str(uuid.uuid5(uuid.NAMESPACE_URL,'manifest')),aliases]
    def test_original_model_relationships_vector_and_all_required_string_homes(self):
        args=self.fixture();sql='SELECT p.person_key,p.person_name FROM Person p';parameters={'name':{'family':'string','value':'雪\0'}}
        original=configured_typed_graph_count_star_request(*args,sql,layout_sha256=LAYOUT_SHA256)
        result=configured_typed_graph_sdk_request(*args,sql,parameters,layout_sha256=LAYOUT_SHA256)
        self.assertEqual((result['interfaceVersion'],result['dialect']),('weft-compile/0.2.2','weft-sql/0.2.0'))
        self.assertEqual(result['modules'],original['modules']);self.assertEqual(result['sql'],sql)
        self.assertEqual(result['target']['backendVersion'],'0.1.0-local-spark4-pages-candidate')
        before=json.loads(original['target']['bindingJson']);after=json.loads(result['target']['bindingJson'])
        self.assertEqual(before['publication'],after['publication']);self.assertEqual(before['relationships'],after['relationships'])
        self.assertEqual(len(after['records']),2)
        self.assertEqual([[p['logical']['element']for p in r['properties']]for r in after['records']],[['person_key','person_name'],['place_key','place_name']])
        self.assertIn('quantity',args[0].model_source.decode());self.assertEqual(len(before['records'][0]['properties']),3)
        self.assertEqual(result['target']['bindingSha256'],hashlib.sha256(result['target']['bindingJson'].encode()).hexdigest())
        parameters['name']['value']='caller mutation';self.assertEqual(result['parameters']['name']['value'],'雪\0');args[0].verify()
    def test_closed_parameters_and_fresh_closing(self):
        args=self.fixture();sql='SELECT p.person_key FROM Person p'
        for bad in ({'n':{'family':'integer','value':'1'}},{'n':{'family':'string','value':True}},{'n':{'family':'string','value':'x','extra':True}},{'n':{'family':'string','value':'x'*65537}}):
            with self.subTest(parameters=list(bad)),self.assertRaises(ValueError):configured_typed_graph_sdk_request(*args,sql,bad,layout_sha256=LAYOUT_SHA256)
        original=TypedGraphJsonlConfiguration.verify;calls=[]
        def verify(c):
            calls.append(c);original(c)
            if len(calls)==3:raise ValueError('final projection closing drift')
        with patch.object(TypedGraphJsonlConfiguration,'verify',verify),self.assertRaises(ValueError):configured_typed_graph_sdk_request(*args,sql,{},layout_sha256=LAYOUT_SHA256)
        self.assertEqual(len(calls),3)
