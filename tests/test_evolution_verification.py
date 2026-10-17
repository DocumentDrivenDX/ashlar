"""Public verification guard controls with inert native frames; no native qualification."""
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from decimal import Decimal
from ashlar_host.delta_custody import LocalDeltaTransport, LocalDeltaError, DeltaTarget, encoded
from ashlar_host.evolution_verification_types import VerificationBounds
from ashlar_host.evolution_verification_cli import parse_verification_invocation


class Row(dict):
    def asDict(self): return dict(self)


class Column:
    def __init__(self, events, label='column', expression=None):
        self.events=events;self.label=label;self.expression=expression or ('lit',None)
    def cast(self,value):self.events.append(('cast',value));return self
    def alias(self,value):self.label=value;return self
    def over(self,value):self.events.append('window-expression');return self
    def isNotNull(self):return Column(self.events,expression=('nonnull',self))
    def otherwise(self,value):self.expression=('when',self.expression[1],self.expression[2],value);return self
    def binary(self,op,other):return Column(self.events,expression=(op,self,other))
    def __add__(self,other):return self.binary('+',other)
    def __mul__(self,other):return self.binary('*',other)
    def __and__(self,other):return self.binary('&',other)
    def __or__(self,other):return self.binary('|',other)
    def __eq__(self,other):return self.binary('==',other)
    def __ge__(self,other):return self.binary('>=',other)
    def __le__(self,other):return self.binary('<=',other)
    def evaluate(self,row):
        op,*args=self.expression
        if op=='lit':return args[0]
        if op=='col':return row[args[0].strip('`')]
        values=[arg.evaluate(row) if isinstance(arg,Column) else arg for arg in args]
        if op=='nonnull':return values[0] is not None
        if op=='when':return values[1] if values[0] else values[2]
        if op=='coalesce':return next((v for v in values if v is not None),None)
        if op in ('&','|'):return bool(values[0] and values[1]) if op=='&' else bool(values[0] or values[1])
        if any(v is None for v in values):return None
        return {'+':lambda:values[0]+values[1],'*':lambda:values[0]*values[1],
            '==':lambda:values[0]==values[1],'>=':lambda:values[0]>=values[1],'<=':lambda:values[0]<=values[1]}[op]()


class Tests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name).resolve(); self.table=self.root/'table'; self.table.mkdir()
        self.bounds=VerificationBounds(65536,10,1024,65536,65536,65536)
        self.events=[]; self.rows=[{'id':'9007199254740993','text':None},{'id':'9007199254740993','text':'雪\n"'}]
        self.stats=None; self.cancel=None; self.measurements=None; self.common_aliases=[];self.transferred=[];self.capture_mutator=lambda row:row;self.projections=[]
        self.fields=[SimpleNamespace(name=name,nullable=True,metadata={},dataType=SimpleNamespace(simpleString=lambda family=family:family,typeName=lambda family=family:{'bigint':'long','string':'string'}[family])) for name,family in [('id','bigint'),('text','string')]]
        outer=self
        class Frame:
            def limit(self,count):
                outer.events.append(('limit',count));outer.rows=outer.rows[:count];return self
            schema=SimpleNamespace(fields=outer.fields,json=lambda:outer.schema_json())
            def __init__(self,sentinel=False):self.sentinel=sentinel
            def select(self,*args):
                outer.events.append(('projection',tuple(getattr(x,'label',x) for x in args)));outer.projections.append(args)
                if any(getattr(x,'label','')=='_ashlar_verification_valid' for x in args):outer.valid_expression=args[-1]
                aliases=[item.label for item in args if getattr(item,'label','').startswith('_ashlar_verification_cell_')]
                if aliases:outer.common_aliases=aliases
                return self
            def unionByName(self,other):outer.events.append('union-sentinel');return self
            def filter(self,expression):outer.events.append('filter-before-transfer');outer.filter_expression=expression;return self
            def agg(self,*args):raise AssertionError('Separate native aggregate action forbidden')
            def toLocalIterator(self):
                outer.events.append('iterate')
                if outer.cancel is not None:raise outer.cancel
                def rows():
                    for row in outer.rows:
                        captured=outer.measured_row(row)
                        if outer.filter_expression.evaluate(captured):
                            outer.transferred.append(captured);yield Row(outer.capture_mutator(captured))
                    sentinel=outer.measured_row({name:None for name in [f.name for f in outer.fields]},sentinel=True)
                    outer.transferred.append(sentinel);yield Row(outer.capture_mutator(sentinel))
                return rows()
        self.frame=Frame()
        self.transport=LocalDeltaTransport.__new__(LocalDeltaTransport)
        self.transport.spark=SimpleNamespace(range=lambda count:Frame(True),read=SimpleNamespace(format=lambda value:SimpleNamespace(option=lambda *args:SimpleNamespace(load=lambda path:self.frame))))
        self.transport._detail=lambda target:self.events.append('identity')
        self.target=DeltaTarget('local.runtime.items',self.table,'uuid')
        functions=SimpleNamespace(**{name:(lambda *args,name=name:self.function(name,args)) for name in ('col','unix_micros','coalesce','length','encode','lit','count','max','greatest','sum','when','struct','to_json')})
        class Window:
            unboundedPreceding=-9223372036854775808;unboundedFollowing=9223372036854775807
            @staticmethod
            def partitionBy():outer.events.append('global-window');return Window()
            def rowsBetween(self,a,b):outer.events.append(('full-window',a,b));return self
        self.functions_patch=patch.dict(sys.modules,{'pyspark':SimpleNamespace(),'pyspark.sql':SimpleNamespace(functions=functions,Row=Row,Window=Window)})
        self.functions_patch.start(); self.addCleanup(self.functions_patch.stop)

    def measured_row(self,row,sentinel=False):
        stats=self.statistics();count=stats['count'];cell=stats['cell'];total=stats['total']
        if count==0 and cell is None and total is None:cell=total=0
        values=self.measurements
        if sentinel:values=[0]*len(self.common_aliases)
        elif values is None:
            # Explicit inert engine metrics, independent of production plan.
            if self.stats is None:
                values=[0 if value is None else len(value.encode()) for value in row.values()]
            else:
                values=[int(cell or 0)]+[max(0,int(total or 0)//len(self.rows)-int(cell or 0))]*(len(self.common_aliases)-1)
        captured={**row,**dict(zip(self.common_aliases,values)),
            '_ashlar_verification_real':0 if sentinel else 1,
            '_ashlar_verification_count':count,'_ashlar_verification_max':cell,'_ashlar_verification_total':total}
        captured['_ashlar_verification_valid']=self.valid_expression.evaluate(captured)
        return captured

    def function(self,name,args):
        self.events.append((name,args[1] if name in ('encode','to_json') and len(args)>1 else None))
        if name in ('lit','col','coalesce','when'):return Column(self.events,name,(name,*args))
        return Column(self.events,name)

    def schema_json(self):
        self.events.append('schema-json')
        return json.dumps({'type':'struct','fields':[{'name':field.name,'type':field.dataType.simpleString(),'nullable':field.nullable,'metadata':field.metadata} for field in self.fields]})

    def statistics(self):
        if self.stats is not None:return Row(self.stats)
        lengths=[len(value.encode()) for row in self.rows for value in row.values() if value is not None]
        return Row(count=len(self.rows),cell=Decimal(max(lengths,default=0)) if self.rows else None,total=Decimal(sum(lengths)) if self.rows else None)

    def capture(self,**kwargs):
        with self.transport.bounded_observations(self.bounds):
            return self.transport.bounded_snapshot(self.target,2,**kwargs)

    def test_bounded_snapshot_has_exact_opening_and_closing_identity(self):
        captured, _ = self.capture()
        self.assertEqual(captured['rows'], sorted(self.rows, key=encoded))
        self.assertEqual(self.events.count('identity'), 2)
        self.assertEqual(self.events[0], 'identity')
        self.assertEqual(self.events[-1], 'identity')
        self.assertEqual(self.events.count('iterate'), 1)
        self.assertIsNone(self.transport.observation_bounds)

    def test_bounded_snapshot_closing_drift_and_failed_capture_renew_identity(self):
        def detail(target):
            self.events.append('identity')
            if self.events.count('identity') == 2:
                raise LocalDeltaError('Original native Delta UUID changed')
        self.transport._detail = detail
        with self.assertRaisesRegex(LocalDeltaError, 'UUID changed'):
            self.capture()
        self.assertEqual(self.events.count('identity'), 2)
        self.assertEqual(self.events.count('iterate'), 1)
        self.assertIsNone(self.transport.observation_bounds)
        self.events.clear()
        primary = GeneratorExit('capture cancelled')
        self.cancel = primary
        with self.assertRaises(GeneratorExit) as caught:
            self.capture()
        self.assertIs(caught.exception, primary)
        self.assertEqual(self.events.count('identity'), 2)
        self.assertEqual(self.events[-1], 'identity')
        self.assertIsNone(self.transport.observation_bounds)

    def test_common_native_query_preserves_datetime_map_and_bounds_before_transfer(self):
        from datetime import datetime, timezone
        instant = datetime(2026,10,11,1,2,3,456789,tzinfo=timezone.utc,fold=1)
        self.rows = [{'stamp':instant,'properties':{'operation':'WRITE','extra':None}}]
        self.fields[:] = [SimpleNamespace(name='stamp',nullable=True,metadata={},dataType=SimpleNamespace(typeName=lambda:'timestamp',simpleString=lambda:'timestamp')),
            SimpleNamespace(name='properties',nullable=True,metadata={},dataType=SimpleNamespace(typeName=lambda:'map',simpleString=lambda:'map<string,string>',
                keyType=SimpleNamespace(typeName=lambda:'string'),valueType=SimpleNamespace(typeName=lambda:'string')))]
        self.stats={'count':1,'cell':Decimal(80),'total':Decimal(100)}
        from pyspark.sql import functions as F
        F.to_json=lambda *args:Column(self.events);F.struct=lambda *args:Column(self.events)
        with self.transport.bounded_observations(self.bounds):
            result=self.transport._bounded_native_rows(self.frame)
        self.assertIs(result[0]['stamp'],instant);self.assertEqual(result[0]['properties'],self.rows[0]['properties'])
        self.assertLess(self.events.index('filter-before-transfer'),self.events.index('iterate'));self.assertIn('global-window',self.events)
        for stats in ({'count':11,'cell':Decimal(1),'total':Decimal(11)},
                {'count':1,'cell':Decimal(1025),'total':Decimal(1025)},
                {'count':1,'cell':Decimal(1),'total':None}):
            self.stats=stats;self.events.clear()
            with self.transport.bounded_observations(self.bounds):
                with self.assertRaises(LocalDeltaError):self.transport._bounded_native_rows(self.frame)
            self.assertNotIn('aggregate',self.events)

    def test_common_query_history_detail_route_and_iterator_cancellation(self):
        from pyspark.sql import functions as F
        F.to_json=lambda *args:Column(self.events);F.struct=lambda *args:Column(self.events)
        self.stats={'count':2,'cell':Decimal(30),'total':Decimal(80)}
        self.transport.spark.sql=lambda *args,**kwargs:self.frame
        self.transport.targets={}
        with self.transport.bounded_observations(self.bounds):
            self.assertEqual(self.transport.query('SELECT id,text',{}).rows,self.rows)
            self.assertEqual(self.transport.original_history(self.target),self.rows)
            with patch.object(self.transport.spark,'sql')as effect:
                with self.assertRaises(LocalDeltaError):self.transport.query('SELECT '+('x'*(self.bounds.input_bytes+1)),{})
                with self.assertRaises(LocalDeltaError):self.transport.query('SELECT id',{'value':'x'*(self.bounds.input_bytes+1)})
                effect.assert_not_called()
            from ashlar_host.native_singleton_capture import SingletonCaptureError
            with patch('ashlar_host.native_singleton_capture.capture_native_singleton',
                    side_effect=SingletonCaptureError('native count exceeds one'))as singleton:
                with self.assertRaises(LocalDeltaError):LocalDeltaTransport._detail(self.transport,self.target,full=True)
                self.assertIs(singleton.call_args.args[0],self.frame)
                self.assertEqual(singleton.call_args.kwargs['bounds'],self.bounds)
        primary=KeyboardInterrupt('native query iterator');events=[]
        first=dict(self.rows[0]);self_owner=self
        class Partial:
            def __iter__(self):return self
            def __next__(self):
                if not events:events.append('row');return Row(self_owner.measured_row(first))
                raise primary
            def close(self):events.append('close');raise OSError('closing')
        self.frame.toLocalIterator=lambda:Partial()
        with self.transport.bounded_observations(self.bounds):
            with self.assertRaises(KeyboardInterrupt)as caught:self.transport._bounded_native_rows(self.frame)
        self.assertIs(caught.exception,primary);self.assertEqual(events,['row','close'])

    def test_common_reader_small_aggregate_larger_native_cell_refuses_and_closes(self):
        from pyspark.sql import functions as F
        F.to_json=lambda *args:Column(self.events);F.struct=lambda *args:Column(self.events)
        failures=[]
        for index,value in enumerate(('x'*2000, {'nested':['x'*2000]}, b'x'*2000, ['x'*2000],
                'x'*30, {'nested':'x'*30})):
            self.rows=[{'id':value,'text':None}]
            self.stats={'count':1,'cell':Decimal(10),'total':Decimal(20)}
            self.measurements=[2000 if index<4 else 30,0]
            events=[];first=dict(self.rows[0]);self_owner=self
            class Iterator:
                def __iter__(self):return self
                def __next__(self):
                    if not events:events.append('row');return Row(self_owner.measured_row(first))
                    raise StopIteration
                def close(self):events.append('close');raise OSError('independent close failure')
            self.frame.toLocalIterator=lambda:Iterator()
            with self.transport.bounded_observations(self.bounds):
                with self.assertRaises(LocalDeltaError)as caught:self.transport._bounded_native_rows(self.frame)
            failures.append(caught.exception);self.assertEqual(events,['row','close'])
        self.assertEqual(len(failures),6)

    def test_common_native_carriers_preserved_and_measurement_correspondence(self):
        from datetime import datetime, timezone
        from pyspark.sql import functions as F
        F.to_json=lambda *args:Column(self.events);F.struct=lambda *args:Column(self.events)
        stamp=datetime(2026,10,11,1,2,3,123456,tzinfo=timezone.utc,fold=1)
        self.rows=[{'id':{'array':[None,Decimal('1.25'),stamp,b'abc'], 'bool':True},'text':None}]
        self.stats={'count':1,'cell':Decimal(500),'total':Decimal(500)};self.measurements=[500,0]
        with self.transport.bounded_observations(self.bounds):result=self.transport._bounded_native_rows(self.frame)
        self.assertIs(result[0]['id'],self.rows[0]['id']);self.assertIs(result[0]['id']['array'][2],stamp)
        self.rows=[{'id':None,'text':None}];self.stats={'count':1,'cell':Decimal(2),'total':Decimal(4)};self.measurements=[2,2]
        with self.transport.bounded_observations(self.bounds):self.assertEqual(self.transport._bounded_native_rows(self.frame),self.rows)
        self.rows=[];self.stats={'count':0,'cell':None,'total':None};self.measurements=[]
        with self.transport.bounded_observations(self.bounds):self.assertEqual(self.transport._bounded_native_rows(self.frame),[])
        self.rows=[{'id':'original','text':None}];self.stats={'count':1,'cell':Decimal(500),'total':Decimal(500)}
        for values in ([False,0],[None,0],[Decimal('1.5'),0],[-1,0],[501,0],[499,0]):
            self.measurements=values
            with self.transport.bounded_observations(self.bounds):
                with self.assertRaises(LocalDeltaError):self.transport._bounded_native_rows(self.frame)
        self.fields[0].name='_ashlar_verification_cell_0';self.events.clear()
        with self.transport.bounded_observations(self.bounds):
            with self.assertRaises(LocalDeltaError):self.transport._bounded_native_rows(self.frame)
        self.assertNotIn('aggregate',self.events)

    def test_single_action_overflow_filters_all_real_payloads_before_transfer(self):
        for stats in ({'count':1000000,'cell':Decimal(1),'total':Decimal(1000000)},
                {'count':2,'cell':Decimal(1025),'total':Decimal(2050)},
                {'count':2,'cell':Decimal(1),'total':Decimal(9999999)},
                {'count':2,'cell':None,'total':None}):
            with self.subTest(stats=stats):
                self.stats=stats;self.events.clear();self.transferred.clear()
                with self.assertRaises(LocalDeltaError):self.capture()
                self.assertEqual(len(self.transferred),1)
                self.assertEqual(self.transferred[0]['_ashlar_verification_real'],0)
                self.assertTrue(all(self.transferred[0][field.name] is None for field in self.fields))
                self.assertFalse(self.transferred[0]['_ashlar_verification_valid'])
                self.assertEqual(self.events.count('iterate'),1)
                self.assertIn('union-sentinel',self.events)
                self.assertLess(self.events.index('filter-before-transfer'),self.events.index('iterate'))
                self.assertNotIn('aggregate',self.events)

    def test_common_null_field_names_are_measured_on_every_row(self):
        name='long_'+('k'*895)
        self.fields[0].name=name
        self.rows=[{name:None,'text':None}]*10
        self.bounds=VerificationBounds(65536,10,1024,40000,65536,65536)
        # Explicit inert native to_json struct bytes include the original key
        # even for NULL; the owning expression option is asserted separately.
        self.stats={'count':10,'cell':Decimal(910),'total':Decimal(9200)}
        self.measurements=[910,10]
        with self.transport.bounded_observations(self.bounds):
            with self.assertRaises(LocalDeltaError):self.transport._bounded_native_rows(self.frame)
        options=[event[1] for event in self.events if type(event)is tuple and event[0]=='to_json']
        self.assertEqual(options,[{'ignoreNullFields':'false'}]*2)
        self.assertEqual(len(self.transferred),1)
        self.assertFalse(self.transferred[0]['_ashlar_verification_valid'])

    def test_repeated_metric_envelope_bounds_many_null_and_wide_rows(self):
        self.bounds=VerificationBounds(65536,10,1024,6000,65536,65536)
        self.rows=[{'id':None,'text':None}]*2
        self.assertEqual(self.capture()[0]['rows'],self.rows)
        self.rows=[{'id':None,'text':None}]*4
        self.transferred.clear()
        with self.assertRaises(LocalDeltaError):self.capture()
        self.assertEqual(len(self.transferred),1)
        self.assertFalse(self.transferred[0]['_ashlar_verification_valid'])
        self.assertEqual(self.transferred[0]['_ashlar_verification_real'],0)
        self.bounds=VerificationBounds(65536,10,1024,20000,65536,65536)
        self.fields[:]=[SimpleNamespace(name='column_'+str(i),nullable=True,metadata={},
            dataType=SimpleNamespace(simpleString=lambda:'string',typeName=lambda:'string'))for i in range(80)]
        self.rows=[{field.name:None for field in self.fields}]*2
        self.transferred.clear()
        with self.assertRaises(LocalDeltaError):self.capture()
        self.assertEqual(len(self.transferred),1)
        self.assertFalse(self.transferred[0]['_ashlar_verification_valid'])
        self.assertTrue(all(self.transferred[0][field.name] is None for field in self.fields))

    def test_owning_projection_masks_payload_and_uses_complete_window(self):
        self.capture()
        payload=self.projections[-1][:len(self.fields)]
        for column,field in zip(payload,self.fields):
            self.assertEqual(column.label,field.name)
            self.assertEqual(column.expression[0],'when')
            _,condition,original,otherwise=column.expression
            self.assertEqual(original.expression,('col','`'+field.name+'`'))
            self.assertIsNone(otherwise.expression[1])
            self.assertFalse(condition.evaluate({'_ashlar_verification_valid':False,'_ashlar_verification_real':1}))
            self.assertFalse(condition.evaluate({'_ashlar_verification_valid':True,'_ashlar_verification_real':0}))
            self.assertTrue(condition.evaluate({'_ashlar_verification_valid':True,'_ashlar_verification_real':1}))
        self.assertIn(('full-window',-9223372036854775808,9223372036854775807),self.events)
        self.assertEqual(self.events.count('iterate'),1)
        self.assertEqual(self.events.count('union-sentinel'),1)
        self.assertEqual(self.events.count('window-expression'),3)

    def test_closed_sentinel_stats_markers_and_envelope_refuse(self):
        for key,value in (('_ashlar_verification_real',False),('_ashlar_verification_real',2),
                ('_ashlar_verification_valid',1),('_ashlar_verification_count',Decimal('1.5')),
                ('_ashlar_verification_total',None)):
            with self.subTest(key=key):
                self.capture_mutator=lambda row,key=key,value=value:{**row,key:value}
                with self.assertRaises(LocalDeltaError):self.capture()
        self.capture_mutator=lambda row:{**row,'id':'not-null'} if row['_ashlar_verification_real']==0 else row
        with self.assertRaises(LocalDeltaError):self.capture()
        self.capture_mutator=lambda row:row
        for alias in ('_ashlar_verification_real','_ashlar_verification_valid','_ashlar_verification_total'):
            original=self.fields[0].name;self.fields[0].name=alias;self.events.clear()
            with self.assertRaises(LocalDeltaError):self.capture()
            self.assertNotIn('iterate',self.events);self.fields[0].name=original
        self.bounds=VerificationBounds(1024,10,1024,65536,65536,65536)
        self.events.clear()
        with self.assertRaises(LocalDeltaError):self.capture()
        self.assertNotIn('iterate',self.events)

    def test_single_valid_sentinel_is_required_even_for_empty(self):
        self.rows=[]
        self.assertEqual(self.capture()[0]['rows'],[])
        self.assertEqual(len(self.transferred),1)
        self.assertEqual(self.transferred[0]['_ashlar_verification_real'],0)
        self.frame.toLocalIterator=lambda:iter(())
        with self.assertRaises(LocalDeltaError):self.capture()

    def test_constructor_bounds_precede_first_native_read_and_reset_on_cancel(self):
        primary=GeneratorExit('constructor native read');seen=[]
        def opening(owner,*args,**kwargs):
            seen.append(owner.observation_bounds);raise primary
        with patch.object(LocalDeltaTransport,'_open',opening):
            with self.assertRaises(GeneratorExit)as caught:
                LocalDeltaTransport(None,self.root/'journal','installation',(),None,verification_bounds=self.bounds)
        self.assertIs(caught.exception,primary);self.assertEqual(seen,[self.bounds])
        with self.assertRaises(LocalDeltaError):
            LocalDeltaTransport(None,self.root/'journal','installation',(),None,verification_bounds={})

    def test_native_reservation_bound_before_read_and_exact_original_bytes(self):
        path=self.root/'reservation';path.write_text('original')
        with self.transport.bounded_observations(self.bounds):
            self.assertEqual(self.transport._reservation_text(path),'original')
            path.write_bytes(b'x'*(self.bounds.input_bytes+1))
            with patch('ashlar_host.delta_custody.os.read')as read:
                with self.assertRaises(LocalDeltaError):self.transport._reservation_text(path)
            read.assert_not_called()

    def test_complete_lexical_null_unicode_duplicate_and_empty(self):
        self.rows.append(dict(self.rows[0]))
        bag,schema=self.capture()
        self.assertEqual(len(bag['rows']),3); self.assertEqual(bag['rows'].count(self.rows[0]),2)
        self.assertEqual(schema['fields'][0]['metadata'],{})
        self.assertIn(('encode','UTF-8'),self.events);self.assertIn(('cast','decimal(38,0)'),self.events)
        self.assertLess(self.events.index('filter-before-transfer'),self.events.index('iterate'));self.assertIn('global-window',self.events)
        self.rows=[]; self.events.clear(); self.assertEqual(self.capture()[0]['rows'],[])

    def test_nonempty_all_null_native_aggregate_is_zero_not_absent(self):
        self.rows=[{'id':None,'text':None}]
        self.assertEqual(self.capture()[0]['rows'], self.rows)

    def test_owned_iterator_closes_after_partial_cancel_and_close_failure(self):
        primary=GeneratorExit('partial native capture');events=[];self_owner=self
        class Iterator:
            def __iter__(self):return self
            def __next__(self):
                if not events:events.append('row');return Row(self_owner.measured_row({'id':'1','text':'x'}))
                raise primary
            def close(self):events.append('close');raise OSError('iterator close')
        self.frame.toLocalIterator=lambda:Iterator()
        self.stats={'count':2,'cell':Decimal(1),'total':Decimal(4)}
        with self.assertRaises(GeneratorExit)as caught:self.capture()
        self.assertIs(caught.exception,primary);self.assertEqual(events,['row','close'])
        self.assertIsNone(self.transport.observation_bounds)

    def test_row_cell_total_overflow_refuse_before_iteration(self):
        for stats in ({'count':11,'cell':Decimal(1),'total':Decimal(11)},
                {'count':1,'cell':Decimal(1025),'total':Decimal(1025)},
                {'count':1,'cell':Decimal(1024),'total':Decimal(20000)},
                {'count':1,'cell':None,'total':None},
                {'count':True,'cell':Decimal(1),'total':Decimal(1)},
                {'count':1,'cell':Decimal('0.5'),'total':Decimal(1)}):
            with self.subTest(stats=stats):
                self.stats=stats;self.events.clear()
                with self.assertRaises(LocalDeltaError):self.capture()
                self.assertNotIn('aggregate',self.events);self.assertEqual(self.events[-1],'identity')

    def test_schema_bounds_before_schema_serialization_or_rows(self):
        self.fields[0].metadata={'oversized':'x'*1025}
        with self.assertRaises(LocalDeltaError):self.capture()
        self.assertNotIn('schema-json',self.events);self.assertNotIn('aggregate',self.events)
        self.fields[0].metadata={};self.fields.extend(self.fields*130);self.events.clear()
        with self.assertRaises(LocalDeltaError):self.capture()
        self.assertNotIn('schema-json',self.events)

    def test_nested_schema_refuses_before_recursive_type_string(self):
        self.fields[0].dataType=SimpleNamespace(typeName=lambda:'struct',simpleString=lambda:self.fail('recursive type serialized'))
        with self.assertRaises(LocalDeltaError):self.capture()
        self.assertNotIn('schema-json',self.events);self.assertNotIn('aggregate',self.events)

    def test_cumulative_remaining_bound_and_capture_statistics_drift(self):
        with self.assertRaises(LocalDeltaError):self.capture(remaining_bytes=100)
        self.assertNotIn('aggregate',self.events)
        self.stats={'count':2,'cell':Decimal(16),'total':Decimal(32)}
        with self.assertRaises(LocalDeltaError):self.capture()

    def test_cancellation_closing_identity_and_interval_release(self):
        primary=KeyboardInterrupt('body');self.cancel=primary
        original=self.transport._detail;calls=[]
        def detail(target):
            calls.append(target)
            if len(calls)>1:raise OSError('closing')
            return original(target)
        self.transport._detail=detail
        with self.assertRaises(KeyboardInterrupt)as raised:self.capture()
        self.assertIs(raised.exception,primary);self.assertGreaterEqual(len(calls),2)
        self.assertIsNone(self.transport.observation_bounds)

    def configuration(self):
        return {'profile':'ashlar-evolution-verification-invocation/0.1','ledger_path':str(self.root/'ledger.sqlite'),'receipt_path':str(self.root/'receipt.json'),
            'producer':{'source':str(self.root),'bun':str(self.root/'bun'),'git':str(self.root/'git'),'timeout_seconds':1,'maximum_output_bytes':1024,'maximum_receipt_bytes':4194304},
            'expected_sha256':'a'*64,'ordinal':0,'bounds':dict(vars(self.bounds)),
            'supervision':{'deadline_seconds':300,'capture_bytes':65536,'owner':'operator-owned-process-supervisor/0.1'}}

    def test_closed_native_schema_profile_selection(self):
        value=self.configuration()
        self.assertEqual(parse_verification_invocation(json.dumps(value).encode()).native_schema_profile,'legacy-sql-bootstrap/0.1')
        value['native_schema_profile']='canonical-managed-carriers/0.1'
        self.assertEqual(parse_verification_invocation(json.dumps(value).encode()).native_schema_profile,value['native_schema_profile'])
        for bad in (None,True,{},'unknown','canonical-managed-carriers/0.2'):
            value['native_schema_profile']=bad
            with self.assertRaises((ValueError,PermissionError)):parse_verification_invocation(json.dumps(value).encode())

    def test_closed_parser_old_ordinal_explicit_bounds_no_defaults(self):
        value=self.configuration(); self.assertEqual(parse_verification_invocation(json.dumps(value).encode()).ordinal,0)
        value['supervision']['deadline_seconds']=600
        selected=parse_verification_invocation(json.dumps(value).encode())
        self.assertEqual(selected.supervision.deadline_seconds,600)
        self.assertEqual(selected.bounds.capture_bytes,65536)
        self.assertEqual(selected.supervision.owner,'operator-owned-process-supervisor/0.1')
        for capacity in (16777216,33554432):
            value['bounds']['total_bytes']=capacity
            self.assertEqual(parse_verification_invocation(json.dumps(value).encode()).bounds.total_bytes,capacity)
        for invalid in (33554433,True,33554432.0,0):
            value['bounds']['total_bytes']=invalid
            with self.assertRaises((ValueError,PermissionError)):parse_verification_invocation(json.dumps(value).encode())
        for mutation in (lambda v:v.update(ordinal=True),lambda v:v.update(ordinal=8),lambda v:v.update(ordinal=0.0),
                lambda v:v['bounds'].update(rows=True),lambda v:v['supervision'].update(deadline_seconds=601),
                lambda v:v['supervision'].update(deadline_seconds=True),lambda v:v['supervision'].update(deadline_seconds=0),
                lambda v:v['supervision'].update(deadline_seconds=1.5),lambda v:v['supervision'].update(owner='unknown'),
                lambda v:v['supervision'].update(capture_bytes=1),lambda v:v.update(extra=1)):
            value=self.configuration();mutation(value)
            with self.assertRaises((ValueError,PermissionError)):parse_verification_invocation(json.dumps(value).encode())
        with self.assertRaises(ValueError):parse_verification_invocation(b'{"ordinal":0,"ordinal":1}')

    def test_complete_existing_journal_preconstructor_no_repair(self):
        path=self.root/'native.sqlite'; db=sqlite3.connect(path)
        definitions=(
            'CREATE TABLE local_installation(id INTEGER PRIMARY KEY CHECK(id=1), original TEXT NOT NULL)',
            'CREATE TABLE local_operation(operation TEXT PRIMARY KEY,intent TEXT NOT NULL,intent_sha TEXT NOT NULL,before_version INTEGER NOT NULL,state TEXT NOT NULL,receipt TEXT)',
            'CREATE TABLE local_plan(operation TEXT PRIMARY KEY,original TEXT NOT NULL,digest TEXT NOT NULL)',
            'CREATE TABLE local_publication_artifact (request_digest TEXT PRIMARY KEY,request TEXT NOT NULL,artifact TEXT NOT NULL)',
            'CREATE TABLE local_source_plan (request_digest TEXT PRIMARY KEY,original TEXT NOT NULL)')
        for sql in definitions:db.execute(sql)
        registry={'installation':'installed','targets':[{'table':self.target.table,'path':str(self.target.path),'uuid':self.target.uuid}]}
        db.execute('INSERT INTO local_installation VALUES(1,?)',(encoded(registry),));db.commit();db.close();os.chmod(path,0o600)
        LocalDeltaTransport.require_existing_journal(path,'installed',(self.target,))
        db=sqlite3.connect(path);db.execute('DROP TABLE local_source_plan');db.commit();db.close()
        before=path.read_bytes()
        with self.assertRaises(LocalDeltaError):LocalDeltaTransport.require_existing_journal(path,'installed',(self.target,))
        self.assertEqual(path.read_bytes(),before)
        with self.assertRaises(FileNotFoundError):LocalDeltaTransport.require_existing_journal(self.root/'missing.sqlite','installed',(self.target,))
        self.assertFalse((self.root/'missing.sqlite').exists())


class DriverTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import test_evolution_run_lineage as lineage
        lineage.Tests.setUpClass()

    def setUp(self):
        import test_evolution_run_lineage as lineage
        from ashlar_host.evolution_composition import NativeEvolutionRunPolicy
        self.host=lineage.Tests();self.host.setUp();self.addCleanup(self.host.doCleanups)
        self.driver=self.host.driver;self.driver.pin_held=False;self.original=self.host.original
        self.policy=NativeEvolutionRunPolicy(self.driver,self.host.host.policy)
        self.bounds=VerificationBounds(262144,10000,1048576,16777216,1048576,1048576)
        self.records={};self.manifests={};self.slots=list(self.host.empty)
        self.host.patch.stop()
        outer=self
        from contextlib import contextmanager
        class Store:
            def __init__(self,*args,original_request):self.request=original_request
            @contextmanager
            def session(self,context):
                yield SimpleNamespace(read=lambda *args:tuple(outer.records.get(self.request['request_digest'],())))
        self.store=patch('ashlar.attempt_store.DeltaAttemptStore',Store);self.store.start();self.addCleanup(self.store.stop)
        self.driver.transport.inspect_committed=lambda *args,**kwargs:None
        self.driver.descriptors=lambda publication:[dict(self.manifests[publication])] if publication in self.manifests else []
        from ashlar_host.delta_custody import LocalDeltaEffects
        self.effect=patch.object(LocalDeltaEffects,'observe',side_effect=lambda operation,digest,steps,**kwargs:{'states':['committed']*len(steps)})
        self.effect.start();self.addCleanup(self.effect.stop)
        from ashlar_host.ack_sessions import RegisteredOutboxAcks
        self.ack=patch.object(RegisteredOutboxAcks,'reconcile',return_value=b'original selected receipt')
        self.ack_mock=self.ack.start();self.addCleanup(self.ack.stop)
        self.captures=[]
        def snapshot(target,version,**kwargs):
            self.captures.append((target.table,version))
            role=next(role for role,table in self.driver.tables.items()if table==target.table)
            value=dict(self.host.host.snapshots[(target.table,version)])
            pairs=[[name,family.lower()]for name,family in self.driver.columns[role]]
            value['schema']=pairs
            families={'bigint':'long','string':'string','timestamp':'timestamp'}
            schema={'type':'struct','fields':[{'name':name,'type':families[family],'nullable':True,'metadata':{}}for name,family in pairs]}
            return value,schema
        self.driver.transport.bounded_snapshot=snapshot

    def complete(self,ordinal):
        import hashlib
        from ashlar.attempt_store import PhaseRecord,PHASES
        from ashlar_host.driver import encoded
        previous={} if ordinal==0 else self.plans[-1].document()['progress']
        plan=self.driver.plan_evolution_transaction(self.original,ordinal,previous,context=self.driver.context)
        if not hasattr(self,'plans'):self.plans=[]
        self.plans.append(plan);value=plan.document();request=value['request'];digest=request['request_digest']
        source={'request':request,'generated_steps':value['generated_steps'],'selected_steps':value['selected_steps'],
            'zero_match_elisions':value['zero_match_elisions'],'complete_prior_oracle':value['previous_expected'],'observed_native_prior':value['observed_native_prior']}
        for role,rows in value['expected'].items():
            table=self.driver.tables[role];self.host.host.versions[table]=ordinal+1
            self.host.host.snapshots[(table,ordinal+1)]={'rows':sorted(rows,key=encoded),'row_sha256':'a'*64}
        row={'publication_id':value['publication_id'],'profile_version':'ashlar-delta/0.3',
            'table_versions_json':encoded({self.driver.tables[role]:ordinal+1 for role in value['expected']}),
            'schema_revisions_json':request['schema_revisions_json'],'source_progress_json':encoded(value['progress']),
            'validation_report_json':encoded({'complete':True,'request_digest':digest,'predecessor':request['predecessor'],'source_admission':value['source_admission']}),'recorded_at':value['recorded_at']}
        artifact=encoded({'effects':{'original_source_plan_sha256':hashlib.sha256(encoded(source).encode()).hexdigest()},'manifest':row})
        records=[]
        for index,phase in enumerate(PHASES):
            payload=encoded({'request':request,'result_json':artifact if index>=2 else None,'descriptor_json':encoded(row)if index==4 else None})
            records.append(PhaseRecord(phase,digest,payload,hashlib.sha256(payload.encode()).hexdigest()))
        self.records[digest]=records;self.manifests[row['publication_id']]=row
        handles=[(key,{'request_digest':digest,'table':self.driver.tables['object_current'],**step})for key,step in zip(value['operations'],value['selected_steps'])]
        for record in records:
            handles.append((str(ordinal)+'-phase-'+record.phase,{'request_digest':digest,'table':self.driver.tables['attempts'],'statement':'inert phase','parameters':{'payload':encoded({'stream':request['stream'],'batch_id':request['batch_id'],**vars(record)})}}))
        handles.append((str(ordinal)+'-manifest',{'request_digest':digest,'table':self.driver.tables['manifest'],'statement':'inert manifest','parameters':{'row':encoded(row)}}))
        db=self.driver.transport.db
        for key,intent in handles:db.execute('INSERT INTO local_operation VALUES(?,?,?,?,?,?)',(key,encoded(intent),'inert',0,'committed','inert'))
        db.execute('INSERT INTO local_source_plan VALUES(?,?)',(digest,encoded(source)))
        db.execute('INSERT INTO local_publication_artifact VALUES(?,?,?)',(digest,encoded(request),artifact))
        self.slots[ordinal]=(ordinal,'completed',plan.raw,plan.sha256,encoded(row).encode())

    def verify(self,ordinal,native_schema_profile='legacy-sql-bootstrap/0.1'):
        from ashlar_host.evolution_run import EvolutionRunLedgerView
        with LocalDeltaTransport.bounded_observations(self.driver.transport,self.bounds):
            from ashlar_host.delta_custody import LocalDeltaEffects
            from ashlar_host.ack_sessions import RegisteredOutboxAcks
            with patch.object(LocalDeltaEffects,'prepare',side_effect=AssertionError('no prepare')), \
                    patch.object(RegisteredOutboxAcks,'acknowledge',side_effect=AssertionError('no ACK submit')), \
                    patch.object(self.driver,'publish_evolution_attempt',side_effect=AssertionError('no publication')), \
                    patch.object(self.driver,'observe_completed_evolution_attempt',side_effect=AssertionError('no publisher replay')):
                return self.driver.verify_evolution_publication(self.original,EvolutionRunLedgerView(tuple(self.slots)),ordinal,
                    context=self.driver.context,policy=self.policy,bounds=self.bounds,native_schema_profile=native_schema_profile)

    def test_canonical_profile_exact_fields_on_opening_and_closing(self):
        from ashlar_host.managed_carriers import load_managed_carriers
        self.complete(0);lower=self.driver.transport.bounded_snapshot;carriers=load_managed_carriers();calls=[]
        def canonical(target,version,**kwargs):
            bag,schema=lower(target,version,**kwargs);role=target.table.split('.')[-1]
            fields=carriers.ordered_fields(role)
            for actual,(name,family,nullable) in zip(schema['fields'],fields):
                self.assertEqual(actual['name'],name);actual['nullable']=nullable
            calls.append(role);return bag,schema
        self.driver.transport.bounded_snapshot=canonical
        result=self.verify(0,'canonical-managed-carriers/0.1')
        self.assertEqual(result['generated_carriers_sha256'],carriers.generated_sha256)
        self.assertEqual(result['native_schema_profile'],'canonical-managed-carriers/0.1')
        self.assertEqual(len(calls),8)
        with self.assertRaises(PermissionError):self.verify(0)
        calls.clear()
        def drift(target,version,**kwargs):
            bag,schema=canonical(target,version,**kwargs)
            if len(calls)==5:schema['fields'][0]['nullable']=not schema['fields'][0]['nullable']
            return bag,schema
        self.driver.transport.bounded_snapshot=drift
        with self.assertRaises(PermissionError):self.verify(0,'canonical-managed-carriers/0.1')
        self.assertEqual(len(calls),8)
        self.assertFalse(self.driver.pin_held)

    def test_actual_complete_validator_old_publication_after_newer_zero_mutation(self):
        self.complete(0);self.complete(1)
        changes=self.driver.transport.db.total_changes
        result=self.verify(0)
        self.assertEqual(result['completed_prefix'],2);self.assertEqual(result['descriptor']['publication_id'],'publication-0')
        self.assertEqual(set(result['bags']),set(self.driver.columns));self.assertGreaterEqual(self.ack_mock.call_count,4)
        self.assertEqual(self.driver.transport.db.total_changes,changes)
        self.assertFalse(self.driver.pin_held)

    def test_unselected_full_inventory_drift_missing_ack_unfinished_selection(self):
        self.complete(0);self.complete(1)
        original=self.slots[7];self.slots[7]=(7,'completed',None,None,None)
        with self.assertRaises((PermissionError,ValueError)):self.verify(0)
        self.slots[7]=original
        self.ack_mock.return_value=None
        with self.assertRaises(PermissionError):self.verify(0)
        self.ack_mock.return_value=b'exact'
        with self.assertRaises(PermissionError):self.verify(2)
        self.assertFalse(self.driver.pin_held)

    def test_complete_bag_and_full_schema_drift_refuse(self):
        self.complete(0)
        lower=self.driver.transport.bounded_snapshot
        def wrong(target,version,**kwargs):
            bag,schema=lower(target,version,**kwargs)
            schema['fields'][0]['nullable']=False
            return bag,schema
        self.driver.transport.bounded_snapshot=wrong
        with self.assertRaises(PermissionError):self.verify(0)
        self.driver.transport.bounded_snapshot=lower
        table=self.driver.tables['object_current'];self.host.host.snapshots[(table,1)]['rows'].pop()
        with self.assertRaises((PermissionError,ValueError)):self.verify(0)

    def test_each_acquired_role_closes_independently_after_cancel(self):
        self.complete(0);lower=self.driver.transport.bounded_snapshot;calls=[]
        primary=SystemExit('selected capture cancel')
        def capture(target,version,**kwargs):
            self.assertTrue(self.driver.pin_held);calls.append(target.table)
            if len(calls)==4:raise primary
            if len(calls)==5:raise OSError('first role closing')
            return lower(target,version,**kwargs)
        self.driver.transport.bounded_snapshot=capture
        with self.assertRaises(SystemExit)as caught:self.verify(0)
        self.assertIs(caught.exception,primary);self.assertEqual(len(calls),8)
        self.assertEqual(calls[:4],calls[4:]);self.assertFalse(self.driver.pin_held)

    def test_readonly_connection_blocks_constructor_repair_after_preflight_race(self):
        from ashlar_host.driver import NativeDriver
        transport=self.driver.transport
        transport.db.execute('DROP TABLE local_publication_artifact')
        transport.db.execute('PRAGMA query_only=ON')
        closed=[];transport.close=lambda:closed.append('transport')
        before=transport.db.total_changes
        with self.assertRaises(PermissionError):
            NativeDriver.with_registered_evolution(transport,self.driver.policy,self.driver.context,
                self.driver.tables,self.driver.allowed_changes,self.driver.columns,
                source_admission=self.driver.source_admission,source_sessions=self.driver.source_sessions,
                ack_factory=lambda driver:self.driver.ack_sessions)
        with self.assertRaises(sqlite3.OperationalError):
            NativeDriver(transport,self.driver.policy,self.driver.context,self.driver.tables,{},
                self.driver.allowed_changes,self.driver.columns,source_admission=self.driver.source_admission,
                source_sessions=self.driver.source_sessions,ack_sessions=self.driver.ack_sessions)
        self.assertEqual(closed,['transport']);self.assertEqual(transport.db.total_changes,before)
        self.assertEqual(transport.db.execute("SELECT name FROM sqlite_master WHERE name='local_publication_artifact'").fetchall(),[])


class CommandTests(unittest.TestCase):
    def setUp(self):
        from ashlar_host.evolution_verification_types import VerifyCommerceEvolutionConfig
        self.fixture=Tests();self.fixture.setUp();self.addCleanup(self.fixture.doCleanups)
        self.root=self.fixture.root;self.configuration=self.root/'config.json';self.provider=self.root/'provider.py'
        self.configuration.write_text(json.dumps(self.fixture.configuration()))
        self.events=[];self.opening=lambda invocation:None;self.closing=lambda:None
        outer=self
        def config(invocation):
            outer.opening(invocation)
            return VerifyCommerceEvolutionConfig(object(),invocation.ledger_path,object(),invocation.producer,object(),
                invocation.expected_sha256,invocation.ordinal,invocation.bounds,invocation.supervision,object(),invocation.native_schema_profile)
        self.ports=SimpleNamespace(config=config,events=self.events,closing=lambda:self.closing())
        self.module=patch.dict(sys.modules,{'verification_test_ports':self.ports});self.module.start();self.addCleanup(self.module.stop)
        self.provider.write_text('from contextlib import contextmanager\nfrom verification_test_ports import config,events,closing\n@contextmanager\ndef open_verification(invocation):\n    try:\n        yield config(invocation)\n    finally:\n        events.append("provider-close")\n        closing()\n')
        self.answer={'profile':'ashlar-evolution-publication-verification/0.1','original_sha256':'a'*64,'ordinal':0,
            'descriptor':{'publication_id':'original'},'completed_prefix':2,'run_size':8,'qualification':'selected-completed-publication-only','bags':{},'schemas':{}}
        self.invocations=0
        def verify(config):self.invocations+=1;return self.answer
        self.verify=patch('ashlar_host.evolution_verification_cli.verify_commerce_evolution',side_effect=verify)
        self.verify_mock=self.verify.start();self.addCleanup(self.verify.stop)

    def call(self):
        import hashlib
        from ashlar_host.evolution_verification_cli import run_verification_command
        return run_verification_command(str(self.configuration),str(self.provider),hashlib.sha256(self.provider.read_bytes()).hexdigest())

    def test_canonical_receipt_and_profile_mismatch_before_verification(self):
        from ashlar_host.managed_carriers import GENERATED_SHA256
        value=self.fixture.configuration();value['native_schema_profile']='canonical-managed-carriers/0.1'
        self.configuration.write_text(json.dumps(value));self.answer.update(native_schema_profile=value['native_schema_profile'],generated_carriers_sha256=GENERATED_SHA256)
        self.assertEqual(self.call()['generated_carriers_sha256'],GENERATED_SHA256)
        self.fixture.root.joinpath('receipt.json').unlink()
        original=self.ports.config
        from dataclasses import replace
        self.ports.config=lambda invocation:replace(original(invocation),native_schema_profile='legacy-sql-bootstrap/0.1')
        # Provider code imports config anew when loaded.
        before=self.invocations
        with self.assertRaises(ValueError):self.call()
        self.assertEqual(self.invocations,before)

    def test_receipt_only_after_provider_cleanup_and_exact_custody(self):
        from ashlar_host.evolution_verification_cli import HostError
        result=self.call();self.assertEqual(result,self.answer);self.assertEqual(self.events,['provider-close'])
        self.assertEqual(json.loads((self.root/'receipt.json').read_bytes()),result)
        with self.assertRaises(HostError):self.call()
        self.assertEqual(self.invocations,1)

    def test_provider_opening_input_drift_zero_native_observation(self):
        def mutate(invocation):self.configuration.write_text(self.configuration.read_text()+' ')
        self.opening=mutate
        with self.assertRaises(ValueError):self.call()
        self.assertEqual(self.invocations,0);self.assertEqual(self.events,['provider-close'])
        self.assertFalse((self.root/'receipt.json').exists())

    def test_provider_exec_drift_zero_native_observation(self):
        self.provider.write_text('from pathlib import Path\nPath('+repr(str(self.configuration))+').write_text("changed")\n'+self.provider.read_text())
        with self.assertRaises(ValueError):self.call()
        self.assertEqual(self.invocations,0)

    def test_provider_cancel_multiple_cleanup_failures_preserve_primary(self):
        primary=KeyboardInterrupt('body')
        self.verify_mock.side_effect=primary
        self.closing=lambda:(_ for _ in()).throw(OSError('provider cleanup'))
        with self.assertRaises(KeyboardInterrupt)as caught:self.call()
        self.assertIs(caught.exception,primary);self.assertEqual(self.events,['provider-close'])
        self.assertFalse((self.root/'receipt.json').exists())

    def test_receipt_bound_before_serialization_and_no_output(self):
        self.answer['oversized']='x'*65536
        with patch('ashlar_host.evolution_verification_cli.json.dumps',side_effect=AssertionError('unbounded serialization'))as encode:
            with self.assertRaises(LocalDeltaError):self.call()
            encode.assert_not_called()
        self.assertFalse((self.root/'receipt.json').exists())

    def test_actual_zero_partial_write_fsync_and_cancel_remove_owned_output(self):
        real_write=os.write;real_fsync=os.fsync
        for kind in ('zero','partial-zero','fsync','cancel'):
            with self.subTest(kind=kind):
                primary=GeneratorExit('output cancel');writes=[]
                def write(fd,value):
                    if kind=='cancel':raise primary
                    if kind=='zero':return 0
                    if kind=='partial-zero':
                        if not writes:writes.append('partial');return real_write(fd,value[:1])
                        return 0
                    return real_write(fd,value)
                def fsync(fd):
                    if kind=='fsync':raise OSError('output fsync')
                    return real_fsync(fd)
                with patch('ashlar_host.evolution_verification_cli.os.write',side_effect=write),patch('ashlar_host.evolution_verification_cli.os.fsync',side_effect=fsync):
                    with self.assertRaises((ValueError,OSError,GeneratorExit))as caught:self.call()
                if kind=='cancel':self.assertIs(caught.exception,primary)
                self.assertFalse((self.root/'receipt.json').exists())

    def test_installed_closed_dispatch_success_refusal_and_cancellation(self):
        import io
        from contextlib import redirect_stdout,redirect_stderr
        from ashlar.cli import main
        argv=['ashlar','verify-commerce-evolution-publication','--configuration','config','--provider','provider','--provider-sha256','a'*64]
        with patch.object(sys,'argv',argv),patch('ashlar_host.evolution_verification_cli.run_verification_command',return_value=self.answer)as dispatch,redirect_stdout(io.StringIO())as stdout:
            main()
        dispatch.assert_called_once_with('config','provider','a'*64);self.assertEqual(json.loads(stdout.getvalue()),self.answer)
        with patch.object(sys,'argv',argv[:-2]),patch('ashlar_host.evolution_verification_cli.run_verification_command')as dispatch,redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):main()
        dispatch.assert_not_called()
        with patch.object(sys,'argv',argv),patch('ashlar_host.evolution_verification_cli.run_verification_command',side_effect=ValueError('private payload')),redirect_stderr(io.StringIO())as stderr:
            with self.assertRaises(SystemExit)as caught:main()
        self.assertEqual(caught.exception.code,2);self.assertNotIn('private payload',stderr.getvalue())
        primary=KeyboardInterrupt('cancel')
        with patch.object(sys,'argv',argv),patch('ashlar_host.evolution_verification_cli.run_verification_command',side_effect=primary):
            with self.assertRaises(KeyboardInterrupt)as caught:main()
        self.assertIs(caught.exception,primary)

    def test_public_registered_ack_reconcile_reads_existing_receipt_zero_write(self):
        from test_protected_outbox_ack import Harness,FakeSession
        from test_registered_outbox_ack import Tests as AckTests
        from ashlar_host.ack import receipt_bytes
        h=Harness();owner=AckTests().owner(h)
        # Seed only the inert retained service record through its existing owner.
        # The verification call itself is the real registered SELECT branch.
        AckTests().call(owner,h,'acknowledge')
        before=len(h.connections)
        result=AckTests().call(owner,h,'reconcile')
        self.assertEqual(result,receipt_bytes(h.scope,h.request_bytes,h.manifest_bytes)[3])
        self.assertEqual(h.connections[before].writes,0);self.assertTrue(h.connections[before].closed)


class ProviderTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import test_evolution_operator as operator
        operator.Tests.setUpClass()

    def setUp(self):
        import test_evolution_operator as operator
        from ashlar_host.evolution_operator import ExistingEvolutionLease
        from ashlar_host.evolution_run import EvolutionRunLedger
        from ashlar_host.evolution_verification_types import VerifyEvolutionInvocation
        from dataclasses import replace
        from ashlar_host.existing_outbox_authority import OutboxAuthorityRegistration
        self.host=operator.Tests();self.host.setUp();self.addCleanup(self.host.doCleanups)
        self.operator,fresh=self.host.installed();self.driver=self.host.driver
        # The legacy inert fixture deliberately has weak schemas. This test's
        # persisted original journal uses the current canonical owner schema.
        db=sqlite3.connect(self.operator.journal_path)
        statements={
            'local_operation':'CREATE TABLE local_operation(operation TEXT PRIMARY KEY,intent TEXT NOT NULL,intent_sha TEXT NOT NULL,before_version INTEGER NOT NULL,state TEXT NOT NULL,receipt TEXT)',
            'local_plan':'CREATE TABLE local_plan(operation TEXT PRIMARY KEY,original TEXT NOT NULL,digest TEXT NOT NULL)',
            'local_publication_artifact':'CREATE TABLE local_publication_artifact (request_digest TEXT PRIMARY KEY,request TEXT NOT NULL,artifact TEXT NOT NULL)',
            'local_source_plan':'CREATE TABLE local_source_plan (request_digest TEXT PRIMARY KEY,original TEXT NOT NULL)'}
        for name,sql in statements.items():db.execute('DROP TABLE '+name);db.execute(sql)
        db.commit();db.close()
        context=self.driver.context
        with ExistingEvolutionLease.open(self.operator,fresh,context)as lease:
            with self.driver.writer('seed-original',context):
                original=self.driver.capture_evolution_run(fresh.request,context=context);lease.admit_run(original,context)
                ledger=EvolutionRunLedger.open(fresh.ledger_path,lease,context=context,original=original);ledger.close()
        self.invocation=VerifyEvolutionInvocation(fresh.ledger_path,fresh.receipt_path,fresh.producer,original.sha256,0,
            VerificationBounds(262144,10000,1048576,16777216,1048576,1048576),
            __import__('ashlar_host.evolution_verification_types',fromlist=['VerificationSupervision']).VerificationSupervision(300,1048576,'operator-owned-process-supervisor/0.1'))
        self.events=[];outer=self
        class Connection:
            def rollback(self):outer.events.append('rollback')
            def close(self):outer.events.append('ordinary-close')
        self.operator=replace(self.operator,registrations=tuple(OutboxAuthorityRegistration(item.source,item.source_role,item.ack_role,item.database,lambda context:Connection())for item in self.operator.registrations))
        self.acquired={}
        def query(statement,parameters):
            current=self.acquired['driver'];current.require(current.context)
            self.events.append('held-native')
            if statement.startswith('DESCRIBE DETAIL '):return SimpleNamespace(rows=[{'id':current.transport.targets[current.tables['attempts']].uuid}])
            if statement.startswith('SELECT phase,request_digest,payload_json,payload_digest '):return SimpleNamespace(rows=[])
            raise AssertionError('unexpected native query')
        self.driver.transport.query=query
        transport=self.driver.transport
        transport.bounded_observations=lambda bounds:LocalDeltaTransport.bounded_observations(transport,bounds)
        def construct(*args,**kwargs):return transport
        construct.require_existing_journal=LocalDeltaTransport.require_existing_journal
        admissions=self.driver.source_admission.admissions
        def producer(config,*,source_system,epoch):return next(item for item in admissions if item.prepared.source_system==source_system)
        for item in (patch('ashlar_host.evolution_operator.LocalDeltaTransport',construct),
                patch('ashlar_host.evolution_operator.admit_commerce_evolution',side_effect=producer),
                patch('ashlar_host.evolution_operator.ExistingOutboxAuthority.admit_scope',side_effect=lambda *args:self.events.append('ack')),
                patch('ashlar_host.evolution_operator.RegisteredOutboxSources.admit',side_effect=lambda *args,**kwargs:self.events.append('source'))):
            item.start();self.addCleanup(item.stop)

    def test_real_provider_holds_bounds_and_readonly_before_constructor_then_full_closing(self):
        from dataclasses import replace
        self.invocation=replace(self.invocation,native_schema_profile='canonical-managed-carriers/0.1')
        with self.operator.open_verification(self.invocation)as config:
            self.assertEqual(config.native_schema_profile,self.invocation.native_schema_profile)
            self.acquired['driver']=config.driver
            self.assertIs(config.driver.transport.observation_bounds,self.invocation.bounds)
            self.assertEqual(config.driver.transport.db.execute('PRAGMA query_only').fetchone(),(1,))
        self.assertEqual(self.events.count('ack'),2);self.assertEqual(self.events.count('source'),2)
        self.assertEqual(self.events.count('ordinary-close'),2);self.assertIn('held-native',self.events)
        self.assertEqual(self.host.closes,['transport']);self.assertEqual(self.host.native_cleanup,['native'])
        self.assertIsNone(self.driver.transport.observation_bounds)

    def test_acquired_transport_queryonly_cancel_closes_before_factory(self):
        primary=KeyboardInterrupt('query only opening')
        self.driver.transport.db=SimpleNamespace(execute=lambda *args:(_ for _ in()).throw(primary))
        with patch('ashlar_host.evolution_operator.NativeDriver.with_registered_evolution')as factory:
            with self.assertRaises(KeyboardInterrupt)as caught:
                with self.operator.open_verification(self.invocation):self.fail('opening released')
        self.assertIs(caught.exception,primary);factory.assert_not_called()
        self.assertEqual(self.host.closes,['transport']);self.assertEqual(self.host.native_cleanup,['native'])

    def test_body_cancel_multiple_independent_closing_failures_all_resources_close(self):
        primary=GeneratorExit('verification body')
        with patch('ashlar_host.evolution_operator.ExistingOutboxAuthority.admit_scope',side_effect=PermissionError('ACK closing'))as ack:
            with self.assertRaises(GeneratorExit)as caught:
                with self.operator.open_verification(self.invocation)as config:
                    self.acquired['driver']=config.driver
                    raise primary
        self.assertIs(caught.exception,primary);self.assertEqual(ack.call_count,2)
        self.assertEqual(self.events.count('source'),2);self.assertEqual(self.events.count('ordinary-close'),2)
        self.assertIn('held-native',self.events);self.assertEqual(self.host.closes,['transport'])
        self.assertEqual(self.host.native_cleanup,['native'])
