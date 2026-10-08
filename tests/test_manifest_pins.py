from dataclasses import replace
import unittest
from ashlar.manifest import manifest_pin_vector,bind_manifest_pins,ManifestError
from ashlar.publication import Descriptor,_decode,_freeze
from ashlar.pins import PinVector
from test_manifest import ROW

class ManifestPinTests(unittest.TestCase):
    def pair(self,row=None):
        row=dict(ROW if row is None else row)
        d=Descriptor(row['publication_id'],row['profile_version'],
            _freeze(_decode(row['table_versions_json'])),_freeze(_decode(row['schema_revisions_json'])),
            _freeze(_decode(row['source_progress_json'])),_freeze(_decode(row['validation_report_json'])),_freeze(row))
        v=manifest_pin_vector(row,{'c.s.t':'original-uuid'},authority='selected')
        return d,v
    def test_original_manifest_matches_held_vector(self):
        d,v=self.pair(dict(ROW,source_progress_json='{"f":{"cursor":"1","nested":[1,2]}}'))
        self.assertIsNone(bind_manifest_pins(d,v,{'c.s.t':'original-uuid'},authority='selected'))
    def test_semantically_equal_changed_bytes_do_not_reuse_original_pins(self):
        d,v=self.pair()
        changed,_=self.pair(dict(ROW,source_progress_json='{ "f": {"cursor":"1"} }'))
        with self.assertRaises(ManifestError):bind_manifest_pins(changed,v,{'c.s.t':'original-uuid'},authority='selected')
    def test_full_uuid_inventory_and_exact_scope_required(self):
        d,v=self.pair()
        for uuids in [{},{'c.s.t':'original-uuid','c.s.other':'other'},{'c.s.t':'substitute'}]:
            with self.assertRaises(ManifestError):bind_manifest_pins(d,v,uuids,authority='selected')
        for field,value in [('scope_kind','recovery'),('scope_id','other'),('authority','other'),('custody_digest','0'*64),('targets',{'c.s.t':('original-uuid',1)})]:
            with self.assertRaises(ManifestError):bind_manifest_pins(d,replace(v,**dict({'targets':dict(v.targets)},**{field:value})),{'c.s.t':'original-uuid'},authority='selected')
    def test_parsed_descriptor_substitution_refuses(self):
        d,v=self.pair()
        for field,value in [('publication_id','other'),('profile','other'),('versions',{'c.s.t':1}),('revisions',{'f':'3'}),('source_progress',{'f':{'cursor':'9'}}),('validation_report',{'complete':True,'changed':True})]:
            with self.assertRaises(ManifestError):bind_manifest_pins(replace(d,**{field:value}),v,{'c.s.t':'original-uuid'},authority='selected')
    def test_singleton_composes_real_manifest_binding_before_row_read(self):
        from test_singleton import Pins,Executor,ROW as OBJECT,SOURCE
        from test_publication import FakeBackend,TABLE,OTHER
        from ashlar.singleton import read_singleton
        b=FakeBackend();b.rows[0].update(recorded_at='1791450000000000',validation_report_json='{"complete":true,"unknown":{"decimal":1.234567890123456789}}')
        uuids={TABLE:'trusted-uuid',OTHER:'trusted-uuid'}
        vector=manifest_pin_vector(b.rows[0],uuids,authority='selected')
        class Policy:
            def bind_descriptor(self,d,v,c):return bind_manifest_pins(d,v,uuids,authority='selected')
            def authorize_row(self,d,t,r,c):pass  # Explicit fixture authorization only.
        pins=Pins();ex=Executor(pins)
        row=read_singleton(ex,b,pins,vector,Policy(),publication_id='p1',table=TABLE,kind='object',source=SOURCE,type_id=1,entity_id=9223372036854775807,context='fixture',supported_profiles=['ashlar-delta/0.3'],supported_revisions={'source':['r1']})
        self.assertEqual(row['props_json'],OBJECT['props_json'])
        altered=replace(vector,custody_digest='0'*64,targets=dict(vector.targets))
        ex.calls=[]
        with self.assertRaises(ManifestError):read_singleton(ex,b,pins,altered,Policy(),publication_id='p1',table=TABLE,kind='object',source=SOURCE,type_id=1,entity_id=9223372036854775807,context='fixture',supported_profiles=['ashlar-delta/0.3'],supported_revisions={'source':['r1']})
        self.assertEqual(ex.calls,[])
if __name__=='__main__':unittest.main()

