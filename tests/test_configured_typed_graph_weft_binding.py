# SPDX-License-Identifier: MIT OR Apache-2.0
"""Pure graph metadata tests with inert receipts; no compiler/native authority."""
import hashlib,json,tempfile,unittest,uuid
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch
from ashlar.manifest import manifest_pin_vector
from ashlar.pins import PinVector
from ashlar.typed_graph_source import build_typed_graph_request
from ashlar.typed_graph_source_config import TypedGraphJsonlConfiguration
from ashlar.weft_binding import LAYOUT_SHA256
from ashlar_host.configured_typed_graph_weft_binding import configured_typed_graph_count_star_request
from ashlar_host.count_star_admission import CURRENT_BACKEND
from test_typed_graph_source import fixture,inert_receipt

class Tests(unittest.TestCase):
    def selected(self,root):
        config=fixture(root);request=build_typed_graph_request(config)
        config=config.with_receipt(inert_receipt(config,request))
        roles=('object_current','edge_current','tombstone','whole_source_history');names=['local.original.'+r for r in roles]
        manifest={'publication_id':'original','profile_version':'ashlar-delta/0.3','table_versions_json':json.dumps({n:i for i,n in enumerate(names)}),
            'schema_revisions_json':json.dumps({config.schema_alias:config.document_revision}),'source_progress_json':json.dumps({config.feed:{'position':'3'}}),
            'validation_report_json':'{"complete":true}','recorded_at':'1'}
        vector=manifest_pin_vector(manifest,{n:str(uuid.uuid5(uuid.NAMESPACE_URL,n))for n in names},authority='original')
        aliases={n:'spark_catalog.ashlar_configured_query_test.'+n.split('.')[-1]for n in names}
        return [config,manifest,vector,str(uuid.uuid5(uuid.NAMESPACE_URL,'manifest')),aliases]
    def test_original_model_homes_relationship_definition_and_full_vector(self):
        with tempfile.TemporaryDirectory()as t:
            args=self.selected(Path(t).resolve());config,manifest,vector,manifest_uuid,aliases=args
            sql='SELECT a.id FROM A a CROSS JOIN EXPAND_PATHS(a.links,links) AS p'
            result=configured_typed_graph_count_star_request(*args,sql,layout_sha256=LAYOUT_SHA256)
            self.assertEqual(result['sql'],sql);self.assertEqual(result['interfaceVersion'],'weft-compile/0.4.1')
            self.assertEqual(result['modules'][0]['documentJson'].encode(),config.model_source)
            pin=result['modules'][0]['pin'];self.assertEqual((pin['umfVersion'],pin['revision'],pin['sha256']),('0.10.0','1',config.model_sha256))
            binding=json.loads(result['target']['bindingJson']);self.assertEqual(hashlib.sha256(result['target']['bindingJson'].encode()).hexdigest(),result['target']['bindingSha256'])
            for key in ('backendId','backendVersion','targetProfile'):self.assertEqual(result['target'][key],CURRENT_BACKEND[key])
            self.assertEqual([r['typeId']for r in binding['records']],['17','18'])
            self.assertEqual([r['properties'][0]['home']for r in binding['records']],[{'kind':'props','propertyId':'21'}]*2)
            by_type={r['typeId']:r['logical']for r in binding['records']};edge=binding['relationships'][0]
            self.assertEqual(set(edge),{'logical','acceptedDefinition','table','kind','sourceSystem','typeId','schemaRevision','source','target'})
            self.assertEqual(edge['logical'],{'documentId':'graph','revision':'1','module':'m','relationship':'links'})
            self.assertEqual(edge['acceptedDefinition'],json.loads(config.model_source)['modules'][0]['relationships'][0])
            self.assertEqual((edge['source'],edge['target']),(by_type['17'],by_type['18']))
            self.assertEqual((edge['typeId'],edge['sourceSystem'],edge['schemaRevision'],edge['kind']),('19','s','1','edge'))
            self.assertEqual(binding['publication']['tables'][edge['table']]['name'][-1],'edge_current')
            for item,name in zip(binding['publication']['tables'],sorted(vector.targets)):
                self.assertEqual(item,{'name':aliases[name].split('.'),'uuid':vector.targets[name][0],'version':vector.targets[name][1]})
            # Mutating a decoded result cannot mutate the original declaration/config.
            edge['acceptedDefinition']['id']='caller mutation';config.verify()
    def test_missing_policy_foreign_vector_alias_and_fresh_closing_refuse(self):
        with tempfile.TemporaryDirectory()as t:
            args=self.selected(Path(t).resolve());variants=[]
            a=list(args);a[0]=replace(a[0],policy=None,original_receipt=None);variants.append(a)
            a=list(args);a[2]=PinVector(args[2].authority,'manifest','foreign',args[2].custody_digest,dict(args[2].targets));variants.append(a)
            a=list(args);a[4]={};variants.append(a)
            a=list(args);a[1]={**args[1],'schema_revisions_json':'{"graph":"2"}'};variants.append(a)
            a=list(args);a[3]=next(iter(args[2].targets.values()))[0];variants.append(a)
            for a in variants:
                with self.subTest(selection=a[3]),self.assertRaises(ValueError):configured_typed_graph_count_star_request(*a,'SELECT COUNT(*) FROM A a',layout_sha256=LAYOUT_SHA256)
            original=TypedGraphJsonlConfiguration.verify;calls=[]
            def verify(config):
                calls.append(config);original(config)
                if len(calls)==2:raise ValueError('fresh closing drift')
            with patch.object(TypedGraphJsonlConfiguration,'verify',verify),self.assertRaises(ValueError):
                configured_typed_graph_count_star_request(*args,'SELECT COUNT(*) FROM A a',layout_sha256=LAYOUT_SHA256)
            self.assertEqual(len(calls),2)
