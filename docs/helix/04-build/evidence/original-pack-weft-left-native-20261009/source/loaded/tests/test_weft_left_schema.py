"""Held native metadata remains distinct from row non-nullness."""
import json
import unittest
from types import SimpleNamespace
from pathlib import Path
from run_commerce_publication_weft import PublicationProvider

class Schema:
    def __init__(self,nullable=True):
        self.value={'type':'struct','fields':[{'name':'id','type':'long','nullable':nullable,'metadata':{}},{'name':'props','type':'string','nullable':True,'metadata':{'original':'retained'}}]}
        self.fields=[SimpleNamespace(name='id',dataType=SimpleNamespace(simpleString=lambda:'bigint')),SimpleNamespace(name='props',dataType=SimpleNamespace(simpleString=lambda:'string'))]
    def json(self):return json.dumps(self.value)

class Read:
    def __init__(self,schema):self.schema=schema;self.calls=[]
    def format(self,value):self.calls.append(('format',value));return self
    def option(self,key,value):self.calls.append(('option',key,value));return self
    def load(self,path):self.calls.append(('load',path));return SimpleNamespace(schema=self.schema)

class HeldSchemaTests(unittest.TestCase):
    def setUp(self):
        self.context=object();self.reader=Read(Schema());self.table={'name':['spark_catalog','private','objects'],'uuid':'original-uuid','version':7}
        target=SimpleNamespace(uuid='original-uuid',path=Path('/private/tmp/original-immutable-objects'))
        driver=SimpleNamespace(transport=SimpleNamespace(spark=SimpleNamespace(read=self.reader),targets={'native.objects':target}))
        self.provider=PublicationProvider(driver,{'native.objects':'spark_catalog.private.objects'},None,b'original',b'original',{},context=self.context)
        self.provider.active=True;self.resolved=SimpleNamespace(snapshots={'native.objects':SimpleNamespace(uuid='original-uuid',version=7)})
    def test_empty_source_schema_retains_actual_nullable_and_complete_metadata(self):
        receipt=self.provider.native_table_schema(self.table,self.resolved,self.context)
        self.assertIs(receipt['schema']['fields'][0]['nullable'],True)
        self.assertEqual(receipt['nativeTypes'],[['id','BIGINT'],['props','STRING']])
        self.assertEqual(receipt['schema']['fields'][1]['metadata'],{'original':'retained'})
        self.assertEqual(self.reader.calls,[('format','delta'),('option','versionAsOf',7),('load','/private/tmp/original-immutable-objects')])
        receipt['table']['version']=0;self.assertEqual(self.table['version'],7)
    def test_unheld_foreign_or_unbound_schema_refuses_before_native_read(self):
        for change in ({'uuid':'swapped'},{'version':8},{'version':True},{'name':['spark_catalog','private','other']}):
            with self.assertRaises(ValueError):self.provider.native_table_schema({**self.table,**change},self.resolved,self.context)
        with self.assertRaises(PermissionError):self.provider.native_table_schema(self.table,self.resolved,object())
        self.provider.active=False
        with self.assertRaises(PermissionError):self.provider.native_table_schema(self.table,self.resolved,self.context)
        self.assertEqual(self.reader.calls,[])
    def test_numeric_nullable_flag_cannot_be_reported_as_boolean(self):
        self.reader.schema=Schema(1)
        with self.assertRaises(ValueError):self.provider.native_table_schema(self.table,self.resolved,self.context)

if __name__=='__main__':unittest.main()
