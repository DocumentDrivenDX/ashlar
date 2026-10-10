"""Actual debug compiler metadata; trusted-port mocks confer no runtime authority.

Exact original request/response bytes retained from the independently reviewed
PathsKeys source artifacts. Controls mutate matched recompile metadata solely to
exercise local admission; they do not claim compiler or native authenticity.
"""
import copy
import hashlib
import json
from pathlib import Path
import unittest
import zlib
from test_weft_path_plan import SCHEMAS, FIXTURES as OLD_FIXTURES
from ashlar_host.path_admission import (PathAdmissionConfig, PathSchemaValidation,
    PathPlanError, admit_path_artifact)

# Fixed owned fixture bounds; no external candidate inputs are read here.
with (Path(__file__).resolve().parent / 'fixtures/weft_paths_keys_admission.json.zlib').open('rb') as stream:
    compressed=stream.read(262145)
if len(compressed)>262144:raise ValueError('Fixture compressed bound')
expander=zlib.decompressobj()
FIXTURE_BYTES=expander.decompress(compressed,2097153)
if len(FIXTURE_BYTES)>2097152 or not expander.eof or expander.unused_data or expander.unconsumed_tail:
    raise ValueError('Fixture decoded bound')
FIXTURE_SHA = '6e0ec2e463936b1699633b470ca8c8da6581a2b2a068eba9d24281ff93b42515'
PAIRS = {r['name']:(json.loads(r['requestText']),
                     json.loads(r['responseText']))
         for r in json.loads(FIXTURE_BYTES)}


class PathsKeysAdmissionTests(unittest.TestCase):
    def config(self, profile='paths-keys'):
        return PathAdmissionConfig(16777216, PathSchemaValidation(SCHEMAS, lambda *args: None), profile)

    def pair(self, name='original'):
        return copy.deepcopy(PAIRS[name])

    def admit(self, request, response, profile='paths-keys'):
        return admit_path_artifact(request, response, copy.deepcopy(response), config=self.config(profile))

    def obligation(self, response, name):
        return next(o['parameters'] for o in response['obligations'] if o['id']==name)

    def refuses(self, request, response):
        with self.assertRaises(PathPlanError): self.admit(request, response)

    def test_all_actual_compiled_metadata_and_blocked_cases(self):
        self.assertEqual(hashlib.sha256(FIXTURE_BYTES).hexdigest(), FIXTURE_SHA)
        self.assertEqual(len(PAIRS),20)
        compiled=0
        for name,(request,response) in PAIRS.items():
            with self.subTest(name=name):
                if response['status']=='compiled':
                    admitted=self.admit(request,response);compiled+=1
                    self.assertEqual(len(admitted.columns),len(response['columns']))
                    with self.assertRaises(TypeError): admitted.artifact['sql']='changed'
                else:self.refuses(request,response)
        self.assertEqual(compiled,10)

    def test_profile_selection_has_no_cross_profile_fallback(self):
        for bad in ('', 'unknown', None, True):
            with self.assertRaises(PathPlanError): self.config(bad)
        request,response=self.pair()
        with self.assertRaises(PathPlanError):self.admit(request,response,'paths')
        original=next(f for f in OLD_FIXTURES if f['response'].get('status')=='compiled')
        self.admit(original['request'],original['response'],'paths')
        with self.assertRaises(PathPlanError):self.admit(original['request'],original['response'])

    def test_standalone_schema_and_full_empty_outer_inventory(self):
        for name in ('original','empty_outer'):
            request,response=self.pair(name);p=self.obligation(response,'ashlar.relatedKeys.collectionIntegrity')
            admitted=self.admit(request,response)
            self.assertEqual(len(admitted.edge_schemas),1)
            for key in ('collections','edgeSchemas','checks'):
                r,a=self.pair(name);self.obligation(a,'ashlar.relatedKeys.collectionIntegrity')[key]=[];self.refuses(r,a)
            for key,value in (('identityColumn','other'),('nativeType','STRING')):
                r,a=self.pair(name);self.obligation(a,'ashlar.relatedKeys.collectionIntegrity')['edgeSchemas'][0][key]=value;self.refuses(r,a)
            r,a=self.pair(name);self.obligation(a,'ashlar.relatedKeys.collectionIntegrity')['edgeSchemas'][0]['table']['version']+=1;self.refuses(r,a)

    def test_exact_ordered_collection_and_capacity_inventories(self):
        for name in ('mixed_reverse_order','repeated_expressions','two_roots'):
            request,response=self.pair(name);self.admit(request,response)
            for obligation in ('ashlar.relatedKeys.collectionIntegrity','ashlar.relatedKeys.ordinalCapacity'):
                for key in ('collections','checks'):
                    r,a=self.pair(name);p=self.obligation(a,obligation);p[key]=list(reversed(p[key])) if len(p[key])>1 else p[key]*2;self.refuses(r,a)
            r,a=self.pair(name);p=self.obligation(a,'ashlar.relatedKeys.ordinalCapacity');p['checks'][0]['sql']='';self.refuses(r,a)
        for key,value in (('maximum','2147483647'),('nativeRepresentation','signed64'),('noPartialPublication',False)):
            r,a=self.pair();self.obligation(a,'ashlar.relatedKeys.ordinalCapacity')[key]=value;self.refuses(r,a)
        r,a=self.pair();self.obligation(a,'ashlar.relatedKeys.collectionIntegrity')['forged']=True;self.refuses(r,a)

    def test_onehop_original_key_and_relationship_correspondence(self):
        # Keep recompilation matched and all guard inventories synchronized: local source checks own refusal.
        for key in ('sourceKey','targetKey'):
            for mutation in ('order','type','identity','keyid'):
                r,a=self.pair('composite');expr=next(o['expression'] for o in a['logicalPlan']['outputs'] if o['expression']['op']=='relatedKeys');k=expr['relationship'][key]
                if mutation=='order':
                    if len(k['fields'])>1:k['fields'].reverse()
                    else:k['fields']*=2;k['types']*=2
                elif mutation=='type':k['types'][0]['family']='integer'
                elif mutation=='identity':k['fields'][0]['revision']='foreign'
                else:k['id']='foreign'
                self.obligation(a,'ashlar.relatedKeys.collectionIntegrity')['collections'][0]['relationship']=copy.deepcopy(expr['relationship'])
                if key=='targetKey':a['columns'][-1]['representation']['key']=copy.deepcopy(k)
                self.refuses(r,a)
        for mutation in ('inverse','multiplicity','endpoint','revision'):
            r,a=self.pair();expr=a['logicalPlan']['outputs'][1]['expression'];h=expr['relationship']
            if mutation=='inverse':h['inverse']=True
            elif mutation=='multiplicity':h['targetMultiplicity']['min']=0
            elif mutation=='endpoint':h['to']['element']='products'
            else:h['identity']['revision']='foreign'
            self.obligation(a,'ashlar.relatedKeys.collectionIntegrity')['collections'][0]['relationship']=copy.deepcopy(h)
            self.refuses(r,a)

    def test_required_root_and_closed_descriptor_no_null_fallback(self):
        for mutation in ('nullable','generic','bound','extra'):
            r,a=self.pair();column=a['columns'][1]
            if mutation=='nullable':column['nullable']=True
            elif mutation=='generic':column['representation']={'kind':'presence','value':column['representation']}
            elif mutation=='bound':column['representation']['bound']=1
            else:column['representation']['outerJoin']={}
            self.refuses(r,a)
        for key,value in (('aggregate',True),('groups',[{'forged':True}]),('pathExpansion',{'forged':True})):
            r,a=self.pair();a['logicalPlan'][key]=value;self.refuses(r,a)

    def test_relationship_guard_complete_consumed_inventory(self):
        r,a=self.pair();self.obligation(a,'ashlar.candidate.relationshipIntegrity')['checks']=[];self.refuses(r,a)
        r,a=self.pair();p=self.obligation(a,'ashlar.candidate.relationshipIntegrity');p['checks'][0]['relationship']['relationship']='foreign';self.refuses(r,a)

    def test_actual_public_schemas_when_explicitly_available(self):
        try:
            import jsonschema
        except ImportError:
            self.skipTest('Separate explicit real-schema environment required')
        from ashlar_host.path_schema import make_offline_path_schema_validation
        config=PathAdmissionConfig(16777216, make_offline_path_schema_validation(SCHEMAS),'paths-keys')
        for name,(request,response) in PAIRS.items():
            if response['status']=='compiled':
                with self.subTest(name=name):admit_path_artifact(request,response,response,config=config)

    def test_standalone_edge_schema_does_not_require_synthetic_record_binding(self):
        request,response=self.pair()
        binding=json.loads(request['target']['bindingJson'])
        edge=self.obligation(response,'ashlar.relatedKeys.collectionIntegrity')['edgeSchemas'][0]
        self.assertFalse(any(binding['publication']['tables'][r['table']]==edge['table'] for r in binding['records']))
        admitted=self.admit(request,response)
        self.assertEqual(dict(admitted.edge_schemas[0])['identityColumn'],'id')

    def test_positioned_onehop_map_and_guard_remain_exact(self):
        request,response=self.pair('repeated_expressions')
        for output,column in zip(response['logicalPlan']['outputs'],response['columns']):
            output['name']=column['outputName']='same'
            column['carrierName']='position_'+str(column['position'])
        response['logicalPlan']['requiredCapabilities'].append('project.positionedOutputs')
        mapping=[{k:c[k] for k in ('position','outputName','carrierName','sourceIdentities')} for c in response['columns']]
        response['obligations'].append(dict(id='weft.output.positioned',owner='host',failureCode='WFT-OBLIGATION',
            parameters=dict(profile='weft-positioned-output/0.3.0',columns=mapping)))
        self.admit(request,response)
        response['obligations'][-1]['parameters']['columns'].reverse()
        self.refuses(request,response)

    def test_generic_obligation_numeric_identity_cannot_use_boolean_equality(self):
        # First selected collection in this query is position1, the vulnerable Python True==1 case.
        for obligation in ('ashlar.relatedKeys.collectionIntegrity','ashlar.relatedKeys.ordinalCapacity'):
            for inventory in ('collections','checks'):
                r,a=self.pair('repeated_expressions')
                self.obligation(a,obligation)[inventory][0]['outputPosition']=True
                self.refuses(r,a)
        r,a=self.pair('repeated_expressions')
        self.obligation(a,'ashlar.relatedKeys.collectionIntegrity')['collections'][0]['relationship']['inverse']=0
        self.refuses(r,a)

    def rebind_document(self, request, response, document):
        raw=json.dumps(document,separators=(',',':'))
        request['modules'][0]['documentJson']=raw
        digest=hashlib.sha256(raw.encode()).hexdigest()
        def pins(value):
            if isinstance(value,dict):
                if {'umfVersion','documentId','revision','sha256'} <= set(value):value['sha256']=digest
                for child in value.values():pins(child)
            elif isinstance(value,list):
                for child in value:pins(child)
        pins(request);pins(response)
        binding=json.loads(request['target']['bindingJson']);pins(binding)
        request['target']['bindingJson']=json.dumps(binding,separators=(',',':'))
        request['target']['bindingSha256']=response['bindingSha256']=hashlib.sha256(request['target']['bindingJson'].encode()).hexdigest()

    def test_original_field_membership_allows_cross_module_but_refuses_optional_source(self):
        # Separately authored metadata variants, not relabeled original compiler or public-source proof.
        r,a=self.pair();doc=json.loads(r['modules'][0]['documentJson'])
        domain=doc['modules'][0]
        field=next(e for e in domain['elements'] if e['id']=='suppliers.id')
        domain['elements'].remove(field)
        doc['modules'].append(dict(id='shared',name='shared',elements=[field],relationships=[]))
        record=next(e for e in domain['elements'] if e['id']=='suppliers')
        record['members'][0]['module']='shared';record['keys'][0]['fields'][0]['module']='shared'
        def fields(value):
            if isinstance(value,dict):
                if value.get('element')=='suppliers.id' and value.get('module')=='domain':value['module']='shared'
                for child in value.values():fields(child)
            elif isinstance(value,list):
                for child in value:fields(child)
        fields(a)
        binding=json.loads(r['target']['bindingJson']);fields(binding)
        r['target']['bindingJson']=json.dumps(binding,separators=(',',':'))
        self.rebind_document(r,a,doc)
        self.admit(r,a)
        field['nullability']='optional'
        self.rebind_document(r,a,doc)
        self.refuses(r,a)
