# SPDX-License-Identifier: MIT OR Apache-2.0
"""Fresh native detail projection controls; inert frames, no Spark execution."""
import copy,json,sys,tempfile,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from ashlar_host.delta_custody import LocalDeltaTransport,LocalDeltaError

KEYS=('ashlar.local.installation.id','ashlar.local.registry.sha256','ashlar.local.journal.sha256')
class Column:
    def __init__(self,name,key=None,label=None):self.name=name;self.key=key;self.label=label or name
    def getItem(self,key):return Column(self.name,key)
    def alias(self,label):return Column(self.name,self.key,label)
    def value(self,row):return row[self.name]if self.key is None else row[self.name].get(self.key)
class Tests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.marker=self.root/'reservation';self.marker.write_text('original');self.marker.chmod(0o600)
        self.target=SimpleNamespace(path=self.root,uuid='uuid')
        self.transport=object.__new__(LocalDeltaTransport);t=self.transport
        t._registered=True;t.installation_id='installation';t.registry_sha='registry';t.journal_sha='journal';t.reservation='original'
        t._reservation_path=lambda target:self.marker;t._reservation_text=lambda path:path.read_text();t.observation_bounds=object()
        self.raw={'id':'uuid','properties':dict(zip(KEYS,('installation','registry','journal'))),'location':'file:///original','large_unrelated':{'native':'retained'}}
        self.calls=[];self.captured=[];self.id_family='string';self.properties_family='map<string,string>'
        outer=self
        class Frame:
            def __init__(self,row):
                self.row=row;self.schema=SimpleNamespace(fields=[SimpleNamespace(name=n,dataType=SimpleNamespace(simpleString=lambda f=f:f))for n,f in (('id',outer.id_family),('properties',outer.properties_family))])
            def select(self,*columns):return Frame({c.label:c.value(self.row)for c in columns})
            def first(self):return SimpleNamespace(asDict=lambda:self.row)
        def sql(statement):self.calls.append(statement);return Frame(copy.deepcopy(self.raw))
        t.spark=SimpleNamespace(sql=sql)
        def capture(frame,*,maximum_rows):
            self.assertEqual(maximum_rows,1);self.captured.append(copy.deepcopy(frame.row))
            if any(type(v)is str and len(v.encode())>128 for v in frame.row.values()):raise LocalDeltaError('Bounded selected value required')
            return [frame.row]
        t._bounded_native_rows=capture
        self.addCleanup(patch.stopall)
        patch.dict(sys.modules,{'pyspark':SimpleNamespace(),'pyspark.sql':SimpleNamespace(functions=SimpleNamespace(col=Column))}).start()
    def test_every_call_fresh_four_primitive_cells_bounded_and_unbounded(self):
        for _ in range(2):
            result=self.transport._detail(self.target)
            self.assertEqual(result,{'id':'uuid','properties':dict(zip(KEYS,('installation','registry','journal')))})
        self.assertEqual(len(self.calls),2);self.assertEqual([len(r)for r in self.captured],[4,4])
        self.assertTrue(all(all(v is None or type(v)is str for v in r.values())for r in self.captured))
        self.transport.observation_bounds=None;self.assertEqual(self.transport._detail(self.target),self.raw)
        self.assertEqual(len(self.calls),3)
    def test_full_row_retains_unknown_properties_and_native_location(self):
        self.raw['properties']['ashlar.local.unknown']='must inspect at creation'
        result=self.transport._detail(self.target,full=True)
        self.assertEqual(result,self.raw);self.assertIn('ashlar.local.unknown',result['properties'])
        self.assertEqual(result['location'],'file:///original')
        self.transport._registered=False
        self.assertEqual(self.transport._detail(self.target,full=True),self.raw)
    def test_uuid_properties_reservation_types_null_and_bounds_refuse(self):
        original=copy.deepcopy(self.raw)
        for control in ('uuid',*KEYS,'null','wrong-native-id-type','wrong-native-map-type','selected-oversize','reservation'):
            with self.subTest(control=control):
                self.raw=copy.deepcopy(original);self.id_family='string';self.properties_family='map<string,string>';self.marker.write_text('original')
                if control=='uuid':self.raw['id']='other'
                elif control in KEYS:self.raw['properties'][control]='other'
                elif control=='null':self.raw['properties'][KEYS[0]]=None
                elif control=='wrong-native-id-type':self.id_family='int'
                elif control=='wrong-native-map-type':self.properties_family='map<string,int>'
                elif control=='selected-oversize':self.raw['properties'][KEYS[0]]='x'*129
                else:self.marker.write_text('different')
                with self.assertRaises(LocalDeltaError):self.transport._detail(self.target)
