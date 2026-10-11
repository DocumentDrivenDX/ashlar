import hashlib,json,unittest
from ashlar_host.publication_reader import PublicationProvider

class Tests(unittest.TestCase):
    def setUp(self):
        self.context=object()
        self.binding={'original':{'source':'original','uuid':'original','version':2}}
        self.raw=json.dumps(self.binding,sort_keys=True,separators=(',',':'))
        self.provider=PublicationProvider(None,{},None,b'original',b'original',{},context=self.context,
            original_bindings=(self.raw.encode(),))
        self.request={'target':{'bindingJson':self.raw,'bindingSha256':hashlib.sha256(self.raw.encode()).hexdigest()}}

    def test_selection_owns_bytes_but_does_not_activate_reader(self):
        self.provider.select_original_binding(self.request,context=self.context)
        self.request['target']['bindingJson']='changed'
        self.assertEqual(self.provider.expected_binding,self.raw)
        self.assertFalse(self.provider.active)
        with self.assertRaises(PermissionError):self.provider.sql('SELECT 1',{})
        with self.assertRaises(PermissionError):self.provider.expected_binding='changed'

    def test_context_lifecycle_and_original_correspondence(self):
        with self.assertRaises(PermissionError):self.provider.select_original_binding(self.request,context=object())
        for name in ('active','_reader_closed'):
            setattr(self.provider,name,True)
            with self.assertRaises(PermissionError):self.provider.select_original_binding(self.request,context=self.context)
            setattr(self.provider,name,False)
        for raw in ('{}',self.raw.replace('2','3')):
            self.request['target']={'bindingJson':raw,'bindingSha256':hashlib.sha256(raw.encode()).hexdigest()}
            with self.assertRaises(ValueError):self.provider.select_original_binding(self.request,context=self.context)
        self.assertFalse(self.provider._binding_selected)

    def test_forged_carriers_digest_and_bounds_refuse_before_selection(self):
        class Forged(dict):
            def items(self):raise AssertionError('executable carrier touched')
        with self.assertRaises(ValueError):self.provider.select_original_binding(Forged(self.request),context=self.context)
        self.request['target']['bindingSha256']='0'*64
        with self.assertRaises(ValueError):self.provider.select_original_binding(self.request,context=self.context)
        self.request['extra']='x'*(1024*1024+1)
        with self.assertRaises(ValueError):self.provider.select_original_binding(self.request,context=self.context)
        self.assertFalse(self.provider._binding_selected)

    def test_original_builder_field_inventory_and_both_shapes(self):
        from ashlar_host.source_identity import build_transaction
        from pathlib import Path
        ROOT=Path(__file__).resolve().parents[1]/'examples/domain-packs/commerce/upstream'
        from ashlar_host.publication_reader import compiler_request,encoded
        model=(ROOT/'ontology.json').read_bytes();graph=(ROOT/'graph/fixture.json').read_bytes()
        batch,bindings=build_transaction(model,graph,source_system='private-original-commerce-fixture')
        roles=('object_current','edge_current','tombstone','whole_source_history','attempts','manifest')
        registry=[{'table':'local.commerce.'+role,'uuid':role+'-fixture'} for role in roles]
        versions={r['table']:2 for r in registry[:4]}
        manifest={'publication_id':'fixture-only','table_versions_json':encoded(versions),'source_progress_json':encoded({batch.feed:{}})}
        aliases={t:t.replace('local.','spark_catalog.') for t in versions}
        requests=[compiler_request('',model,bindings,manifest,registry,aliases,fields=fields) for fields in (False,True)]
        provider=PublicationProvider(None,{},None,b'original',b'original',{},context=self.context,
            original_bindings=tuple(r['target']['bindingJson'].encode() for r in requests))
        for request in requests:
            provider.select_original_binding(request,context=self.context)
            self.assertEqual(provider.expected_binding,request['target']['bindingJson'])
        changed=json.loads(requests[1]['target']['bindingJson'])
        changed['records'][0]['properties'][0]['home']['propertyId']='forged'
        raw=encoded(changed)
        requests[1]['target']={'bindingJson':raw,'bindingSha256':hashlib.sha256(raw.encode()).hexdigest()}
        with self.assertRaises(ValueError):provider.select_original_binding(requests[1],context=self.context)

    def test_legacy_setter_and_unconfigured_public_selection(self):
        self.provider.expected_binding='legacy'
        self.assertEqual(self.provider.expected_binding,'legacy')
        provider=PublicationProvider(None,{},None,b'original',b'original',{},context=self.context)
        with self.assertRaises(ValueError):provider.select_original_binding(self.request,context=self.context)

if __name__=='__main__':unittest.main()
