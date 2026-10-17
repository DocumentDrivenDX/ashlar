"""Inert exact binary comparison/action controls; no native engine execution."""
import copy,sys,unittest
from types import SimpleNamespace
from unittest.mock import patch
import test_projected_native_detail as legacy
from ashlar_host.evolution_verification_types import VerificationBounds
from ashlar_host.delta_custody import LocalDeltaTransport,LocalDeltaError
from ashlar_host.registered_detail_proof import capture_registered_detail_proof,RegisteredDetailProofError,NAMES,LEGACY_NAMES

class Column:
    def __init__(self,expression,label=None):self.expression=expression;self.label=label
    def getItem(self,key):return Column(('item',self.expression,key))
    def eqNullSafe(self,other):return Column(('binary-equal-nullsafe',self.expression,other.expression))
    def alias(self,name):return Column(self.expression,name)
    def value(self,row):
        def get(e):
            if e[0]=='col':return row[e[1]]
            if e[0]=='lit':return e[1]
            if e[0]=='item':
                value=get(e[1]);return None if value is None else value.get(e[2])
            if e[0]=='encode':
                value=get(e[1]);return None if value is None else value.encode('utf-8')
            if e[0]=='binary-equal-nullsafe':return get(e[1])==get(e[2])
            raise AssertionError('Unknown expression')
        return get(self.expression)
class Tests(unittest.TestCase):
    def setUp(self):
        self.legacy=legacy.Tests();self.legacy.setUp();self.addCleanup(self.legacy.doCleanups)
        self.owner=self.legacy.transport;self.target=self.legacy.target;self.events=[];self.expressions=[];self.proof_rows=None;self.family='boolean';self.cell_override=None;self.cancel=None
        self.owner.observation_bounds=VerificationBounds(65536,10,1024,65536,65536,65536)
        outer=self
        def field(name,family,nullable):return SimpleNamespace(name=name,dataType=SimpleNamespace(simpleString=lambda:family),nullable=nullable,metadata={})
        class Frame:
            def __init__(self,rows,fields):self.rows=rows;self.schema=SimpleNamespace(fields=fields)
            def select(self,*columns):
                outer.events.append('select');outer.expressions.extend(c.expression for c in columns)
                names=[c.label or c.expression[1]for c in columns]
                proof=tuple(names)==NAMES
                values=[{name:c.value(row)for name,c in zip(names,columns)}for row in self.rows]
                if proof and outer.proof_rows is not None:values=outer.proof_rows
                if proof and outer.cell_override is not None:
                    for row in values:row['i0']=outer.cell_override
                return Frame(values,[field(n,outer.family if proof else'string',False if proof else True)for n in names])
            def limit(self,n):outer.events.append(('limit',n));return Frame(self.rows[:n],self.schema.fields)
            def collect(self):
                outer.events.append('collect')
                if outer.cancel is not None:raise outer.cancel
                return [SimpleNamespace(asDict=lambda row=row:row)for row in self.rows]
        def sql(statement):
            self.events.append('DESCRIBE')
            return Frame([copy.deepcopy(self.legacy.raw)],[field('id','string',True),field('properties','map<string,string>',True)])
        self.owner.spark=SimpleNamespace(sql=sql)
        functions=SimpleNamespace(col=lambda name:Column(('col',name)),lit=lambda value:Column(('lit',value)),encode=lambda column,encoding:Column(('encode',column.expression,encoding)))
        self.module_patch=patch.dict(sys.modules,{'pyspark':SimpleNamespace(),'pyspark.sql':SimpleNamespace(functions=functions)});self.module_patch.start();self.addCleanup(self.module_patch.stop)
    def test_exact_binary_fresh_comparisons_and_unchanged_dictionary(self):
        expected={'id':self.target.uuid,'properties':dict(zip(legacy.KEYS,('installation','registry','journal')))}
        for _ in range(2):self.assertEqual(self.owner._detail(self.target),expected)
        self.assertEqual(self.events.count('DESCRIBE'),2);self.assertEqual(self.events.count('collect'),2)
        self.assertEqual(self.events.count(('limit',2)),2);self.assertEqual(self.legacy.captured,[])
        equal=[e for e in self.expressions if e[0]=='binary-equal-nullsafe'];self.assertEqual(len(equal),8)
        self.assertTrue(all(e[1][0]=='encode'and e[2][0]=='encode'and e[1][2]==e[2][2]=='UTF-8'for e in equal))
    def test_false_null_multiplicity_boolean_and_reservation_priority(self):
        for control in ('uuid','null-uuid','property0','property1','property2','null-property','null-map','zero','two','schema','cell','reservation-priority'):
            with self.subTest(control=control):
                self.legacy.raw={'id':'uuid','properties':dict(zip(legacy.KEYS,('installation','registry','journal')))};self.proof_rows=None;self.family='boolean';self.cell_override=None;self.legacy.marker.write_text('original')
                if control=='uuid':self.legacy.raw['id']='other'
                elif control=='null-uuid':self.legacy.raw['id']=None
                elif control.startswith('property'):self.legacy.raw['properties'][legacy.KEYS[int(control[-1])]]='other'
                elif control=='null-property':self.legacy.raw['properties'][legacy.KEYS[0]]=None
                elif control=='null-map':self.legacy.raw['properties']=None
                elif control=='zero':self.proof_rows=[]
                elif control=='two':self.proof_rows=[dict.fromkeys(NAMES,True)]*2
                elif control=='schema':self.family='string'
                elif control=='cell':self.cell_override=1
                else:self.legacy.marker.write_text('changed');self.legacy.raw['properties'][legacy.KEYS[0]]='other'
                with self.assertRaises(LocalDeltaError)as error:self.owner._detail(self.target)
                if control=='reservation-priority':self.assertIn('reservation',str(error.exception))
    def test_budget_fallback_no_proof_collect_and_cancellation_identity(self):
        # Conservative proof inability falls back to the unchanged bounded guard.
        self.owner.observation_bounds=VerificationBounds(100,1,100,100,100,100)
        with patch.object(self.owner,'_bounded_native_rows',wraps=LocalDeltaTransport._bounded_native_rows.__get__(self.owner))as fallback:
            with self.assertRaises(LocalDeltaError):self.owner._detail(self.target)
        self.assertEqual(fallback.call_count,1);self.assertNotIn('collect',self.events)
        self.owner.observation_bounds=VerificationBounds(65536,10,1024,65536,65536,65536)
        fields=[SimpleNamespace(name=name,dataType=SimpleNamespace(simpleString=lambda:'string'),nullable=True,metadata={'unknown':'retained'}if name=='id'else{})for name in LEGACY_NAMES]
        self.assertIsNone(capture_registered_detail_proof(SimpleNamespace(schema=SimpleNamespace(fields=fields)),('uuid','installation','registry','journal'),bounds=self.owner.observation_bounds))
        primary=KeyboardInterrupt('body');self.cancel=primary
        with self.assertRaises(KeyboardInterrupt)as error:self.owner._detail(self.target)
        self.assertIs(error.exception,primary)
        self.cancel=None;self.legacy.marker.write_text('original')
        with patch.object(self.owner,'_reservation_text',side_effect=primary):
            with self.assertRaises(KeyboardInterrupt)as error:self.owner._detail(self.target)
        self.assertIs(error.exception,primary)
