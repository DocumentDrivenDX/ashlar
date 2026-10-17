"""Inert native-plan/typed-capture controls; no Spark execution or authority."""
from decimal import Decimal
from datetime import datetime,timezone
import sys,unittest
from types import SimpleNamespace
from unittest.mock import patch
import test_evolution_verification as native_fixture
from ashlar_host.native_singleton_capture import capture_native_singleton,SingletonCaptureError
from ashlar_host.evolution_verification_types import VerificationBounds


class Tests(unittest.TestCase):
    def setUp(self):
        self.bounds=VerificationBounds(65536,10,1024,65536,65536,65536)
        self.events=[];self.stats=None;self.rows=[];self.values=[];self.names=['value'];self.transferred=[];self.cancel=None;self.close_error=None;self.close_calls=0;self.summary_mutator=lambda row:row;self.extra=False;self.empty_summary=False
        outer=self
        def function(name,*args):
            outer.events.append((name,args))
            if name=='count':outer.assertEqual(args[0].expression,('lit',1))
            return native_fixture.Column(outer.events,args[0].strip('`') if name=='col' else name,(name,*args)if name in ('lit','col','coalesce','when')else None)
        functions=SimpleNamespace(**{n:(lambda *a,n=n:function(n,*a))for n in ('col','lit','count','max','greatest','sum','when','coalesce','collect_list','struct')})
        class Iterator:
            def __init__(self,rows):self.rows=iter(rows)
            def __iter__(self):return self
            def __next__(self):
                if outer.cancel is not None:raise outer.cancel
                return native_fixture.Row(next(self.rows))
            def close(self):
                outer.close_calls+=1
                if outer.close_error:raise outer.close_error
        class Frame:
            schema=SimpleNamespace(json=lambda:'original native schema')
            def __init__(self,summary=None):self.summary=summary
            def limit(self,count):outer.events.append(('limit',count));outer.assertEqual(count,2);return self
            def select(self,*columns):
                outer.events.append(('select',len(columns)))
                if self.summary is None:return self
                rows=[{c.label:c.evaluate(row)for c in columns}for row in self.summary]
                return Frame(rows)
            def agg(self,*args):
                outer.events.append(('agg',len(args)))
                count=len(outer.rows);values=[v for row in outer.values for v in row]
                stats=outer.stats if outer.stats is not None else (count,max(values,default=None),sum(values)if values else None)
                aliases=['_ashlar_verification_cell_'+str(i)for i in range(len(outer.names))]
                payload=[native_fixture.Row({**row,**dict(zip(aliases,lengths))})for row,lengths in zip(outer.rows,outer.values)]
                return Frame([dict(zip(['_ashlar_verification_count','_ashlar_verification_max','_ashlar_verification_total'],stats),_ashlar_verification_payload=payload)])
            def toLocalIterator(self):
                outer.events.append('iterator');rows=[outer.summary_mutator(dict(row))for row in self.summary]
                if outer.extra:rows=rows+rows
                if outer.empty_summary:rows=[]
                outer.transferred.extend(rows);return Iterator(rows)
        self.frame=Frame();self.functions=functions
        self.patcher=patch.dict(sys.modules,{'pyspark':SimpleNamespace(),'pyspark.sql':SimpleNamespace(functions=functions)});self.patcher.start();self.addCleanup(self.patcher.stop)
    def capture(self):
        measurements=[self.functions.lit(1)for _ in self.names]
        return capture_native_singleton(self.frame,self.names,measurements,bounds=self.bounds,base=256,row_constant=64)
    def test_zero_one_allnull_and_nested_native_preserved_without_window(self):
        self.assertEqual(self.capture(),([],(0,0,0)))
        self.rows=[{'value':None}];self.values=[[14]]
        self.assertEqual(self.capture(),([{'value':None}],(1,14,14)))
        instant=datetime(2026,10,17,tzinfo=timezone.utc)
        nested={'map':{'snow':'雪','missing':None},'array':[None,'x'],'stamp':instant,'decimal':Decimal('3.14')}
        self.rows=[{'value':nested}];self.values=[[90]]
        rows,stats=self.capture();self.assertIs(rows[0]['value'],nested);self.assertIs(rows[0]['value']['stamp'],instant)
        self.assertEqual(self.events.count('iterator'),3);self.assertEqual(self.close_calls,3)
        self.assertEqual(sum(isinstance(v,tuple)and v[0]=='agg'for v in self.events),3)
        self.assertNotIn('global-window',self.events);self.assertNotIn('union-sentinel',self.events)
        self.assertLess(self.events.index(('limit',2)),next(i for i,v in enumerate(self.events)if isinstance(v,tuple)and v[0]=='agg'))
    def test_native_invalid_rows_cell_total_and_nonempty_null_mask_payload(self):
        for stats in ((2,1,2),(1,1025,1025),(1,1,65536),(1,None,1),(1,1,None)):
            self.rows=[{'value':'x'}]*(2 if stats[0]==2 else 1);self.values=[[1]]*len(self.rows);self.stats=stats
            with self.assertRaises(SingletonCaptureError):self.capture()
            self.assertIsNone(self.transferred[-1]['_ashlar_verification_payload'])
        self.assertEqual(self.close_calls,5)
    def test_forged_metrics_shape_summary_count_and_capacity_refuse(self):
        self.rows=[{'value':'x'}];self.values=[[10]]
        for mutate in (lambda r:{**r,'extra':0},lambda r:{**r,'_ashlar_verification_valid':1},lambda r:{**r,'_ashlar_verification_count':True},lambda r:{**r,'_ashlar_verification_total':9},lambda r:{**r,'_ashlar_verification_payload':[]},lambda r:{**r,'_ashlar_verification_max':Decimal('NaN')}):
            self.summary_mutator=mutate
            with self.assertRaises(SingletonCaptureError):self.capture()
        self.summary_mutator=lambda r:r;self.extra=True
        with self.assertRaises(SingletonCaptureError):self.capture()
        self.extra=False;self.empty_summary=True
        with self.assertRaises(SingletonCaptureError):self.capture()
        self.empty_summary=False
        with self.assertRaises(SingletonCaptureError):capture_native_singleton(self.frame,self.names,[self.functions.lit(1)],bounds=self.bounds,base=65536,row_constant=64)
    def test_success_body_close_failure_and_schema_drift_refuse(self):
        self.rows=[{'value':'x'}];self.values=[[10]];primary=OSError('owned close')
        self.close_error=primary
        with self.assertRaises(OSError)as caught:self.capture()
        self.assertIs(caught.exception,primary);self.assertEqual(self.close_calls,1)
        self.close_error=None;calls=[]
        def schema():calls.append(1);return 'before'if len(calls)==1 else 'changed'
        self.frame.schema.json=schema
        with self.assertRaises(SingletonCaptureError):self.capture()
        self.assertEqual(self.close_calls,2)

    def test_cancel_and_body_failure_close_without_replacing_primary(self):
        primary=KeyboardInterrupt();self.cancel=primary;self.close_error=OSError()
        with self.assertRaises(KeyboardInterrupt)as caught:self.capture()
        self.assertIs(caught.exception,primary);self.assertEqual(self.close_calls,1)
        self.cancel=None;self.rows=[{'value':'x'}];self.values=[[10]];self.summary_mutator=lambda r:{**r,'extra':1}
        with self.assertRaises(SingletonCaptureError):self.capture()
        self.assertEqual(self.close_calls,2)
