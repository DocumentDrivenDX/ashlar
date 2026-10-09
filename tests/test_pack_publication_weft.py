import copy,hashlib,json,unittest
from pathlib import Path
from run_pack_publication_weft import compiler_request,original_ports,ROOT

class Tests(unittest.TestCase):
    def inputs(self,pack):
        converter,native,_,_=original_ports(pack)
        model=(converter.ROOT/'ontology.json').read_bytes();graph=(converter.ROOT/'graph/fixture.json').read_bytes()
        _,bindings=converter.build_transaction(model,graph,source_system=native.SOURCE_SYSTEM)
        roles=['object_current','edge_current','tombstone','whole_source_history','attempts','manifest']
        registry=[{'table':'local.'+pack+'.'+role,'uuid':'00000000-0000-0000-0000-'+str(i+1).zfill(12)} for i,role in enumerate(roles)]
        versions={row['table']:2 for row in registry[:4]}
        manifest={'table_versions_json':json.dumps(versions),'source_progress_json':json.dumps({native.SOURCE_SYSTEM:{}}),'publication_id':'binding-test-only'}
        aliases={table:table.replace('local.','spark_catalog.',1) for table in versions}
        return model,graph,bindings,manifest,registry,aliases

    def test_complete_original_qualified_records_fields_and_sql(self):
        for pack,records,fields in [('archaeology',22,110),('ecology',19,81)]:
            inputs=self.inputs(pack);model,graph,bindings,manifest,registry,aliases=inputs
            sql=json.loads((ROOT/'examples/domain-packs'/pack/'upstream/pack.json').read_bytes())['scenario_checks'][0]['sql']
            request=compiler_request(pack,sql,*inputs);binding=json.loads(request['target']['bindingJson'])
            self.assertEqual(request['sql'],sql);self.assertEqual(request['modules'][0]['documentJson'].encode(),model)
            self.assertEqual(len(binding['records']),records);self.assertEqual(sum(len(r['properties'])for r in binding['records']),fields)
            self.assertEqual(len(binding['publication']['tables']),4)
            self.assertEqual(binding['publication']['manifestUuid'],registry[-1]['uuid'])
            self.assertEqual(request['target']['bindingSha256'],hashlib.sha256(request['target']['bindingJson'].encode()).hexdigest())
            admitted={tuple(p['identity']):p['property_id'] for p in bindings['properties']}
            for record in binding['records']:
                for property in record['properties']:
                    logical=property['logical'];self.assertEqual(property['home']['propertyId'],admitted[(logical['documentId'],logical['module'],logical['element'])])

    def test_changed_source_property_catalog_source_scope_or_full_vector_refuses(self):
        for pack in ('archaeology','ecology'):
            values=self.inputs(pack)
            for index in (0,1):
                changed=list(copy.deepcopy(values));changed[index]+=b' '
                with self.assertRaises(ValueError):compiler_request(pack,'SELECT * FROM assets',*changed)
            changed=list(copy.deepcopy(values));changed[2]['properties'][0]['property_id']='9000'
            with self.assertRaises(ValueError):compiler_request(pack,'SELECT * FROM assets',*changed)
            changed=list(copy.deepcopy(values));changed[3]['source_progress_json']='{"wrong-source":{}}'
            with self.assertRaises(ValueError):compiler_request(pack,'SELECT * FROM assets',*changed)
            changed=list(copy.deepcopy(values));changed[4].pop()
            with self.assertRaises(ValueError):compiler_request(pack,'SELECT * FROM assets',*changed)
            changed=list(copy.deepcopy(values));changed[5].pop(next(iter(changed[5])))
            with self.assertRaises(ValueError):compiler_request(pack,'SELECT * FROM assets',*changed)

            changed=list(copy.deepcopy(values));changed[4][1]['uuid']=changed[4][0]['uuid']
            with self.assertRaises(ValueError):compiler_request(pack,'SELECT * FROM assets',*changed)
            changed=list(copy.deepcopy(values));versions=json.loads(changed[3]['table_versions_json']);versions[next(iter(versions))]=True;changed[3]['table_versions_json']=json.dumps(versions)
            with self.assertRaises(ValueError):compiler_request(pack,'SELECT * FROM assets',*changed)
