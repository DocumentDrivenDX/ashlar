import json
import types
import unittest
from ashlar_host.path_capture import PathCaptureConfig
from ashlar_host.databricks_current_source_capture import capture_current_query_sources


def field(name,kind):return types.SimpleNamespace(name=name,dataType=types.SimpleNamespace(simpleString=lambda:kind),nullable=False)
class Frame:
    def __init__(self,rows,fields,log,count=None):self.rows=rows;self.schema=types.SimpleNamespace(fields=fields);self.log=log;self.override=count
    def filter(self,expression):
        self.log.append(('filter',expression));return Frame([r for r in self.rows if r[1] is True],self.schema.fields,self.log,self.override)
    def count(self):self.log.append('count');return len(self.rows) if self.override is None else self.override
    def selectExpr(self,*expressions):
        self.log.append(('select',expressions));rows=[[r[0],str(r[2]),r[3]]for r in self.rows]
        return Frame(rows,[field('__weft_key','string'),field('__weft_value_0','string'),field('__weft_value_1','string')],self.log)
    def limit(self,n):self.log.append(('limit',n));return self
    def toLocalIterator(self,prefetchPartitions):return iter(self.rows)
class Reader:
    def __init__(self,frame):self.frame=frame
    def format(self,value):self.frame.log.append(('format',value));return self
    def option(self,key,value):self.frame.log.append((key,value));return self
    def table(self,value):self.frame.log.append(('table',value));return self.frame


class CaptureTests(unittest.TestCase):
    def setup(self,count=None):
        self.log=[];self.frame=Frame([['k1',True,-2**63,'é'],['k2',False,1,'excluded'],['k3',True,2**63-1,'raw\0text']],
            [field('key','string'),field('eligible','boolean'),field('n','bigint'),field('text','string')],self.log,count)
        self.spark=types.SimpleNamespace(read=Reader(self.frame))
        self.binding=json.dumps({'version':'weft.databricks.current-values/0.1-candidate','scans':[{'scan':'s0','relation':{'catalog':'c','schema':'s','table':'t','version':'3','rowKeyColumn':'key','eligibleColumn':'eligible'}}],
            'values':[{'scan':'s0','column':'n'},{'scan':'s0','column':'text'},{'scan':'s0','column':'n'}]})
        self.config=PathCaptureConfig(10,256,1024)
    def test_complete_pinned_population_and_union_cells(self):
        self.setup();capture=capture_current_query_sources(self.spark,self.binding,config=self.config)
        self.assertIs(capture.original_binding,self.binding);scan=json.loads(capture.scans_json)[0]
        self.assertEqual(scan['expectedRowCount'],'2');self.assertEqual(len(scan['rows']),2)
        self.assertEqual(scan['rows'][0]['cells']['n'],{'family':'integer','value':str(-2**63)})
        self.assertEqual(scan['rows'][1]['cells']['text']['value'],'raw\0text')
        self.assertIn(('versionAsOf','3'),self.log);self.assertIn(('table','`c`.`s`.`t`'),self.log)
        self.assertLess(self.log.index('count'),next(i for i,x in enumerate(self.log)if type(x)is tuple and x[0]=='select'))
    def test_count_mismatch_limits_and_native_family_refuse(self):
        self.setup(count=3)
        with self.assertRaises(ValueError):capture_current_query_sources(self.spark,self.binding,config=self.config)
        self.setup()
        with self.assertRaises(ValueError):capture_current_query_sources(self.spark,self.binding,config=PathCaptureConfig(1,256,1024))
        self.assertFalse(any(type(x)is tuple and x[0]=='select'for x in self.log))
        self.setup();self.frame.schema.fields[2]=field('n','double')
        with self.assertRaises(ValueError):capture_current_query_sources(self.spark,self.binding,config=self.config)
        self.assertNotIn('count',self.log)

    def test_structural_capacity_refuses_before_capture(self):
        self.setup(count=10_000)
        binding=json.loads(self.binding)
        for i in range(10):
            name='empty'+str(i)
            binding['values'].append({'scan':'s0','column':name})
            self.frame.schema.fields.append(field(name,'string'))
        with self.assertRaisesRegex(ValueError,'structural cell'):
            capture_current_query_sources(self.spark,json.dumps(binding),config=PathCaptureConfig(10_000,256,1024))
        self.assertFalse(any(type(x)is tuple and x[0]=='select'for x in self.log))

    def test_distinct_spark_binding_requires_explicit_engine(self):
        self.setup();binding=self.binding.replace('weft.databricks.current-values','weft.spark.current-values')
        with self.assertRaises(ValueError):capture_current_query_sources(self.spark,binding,config=self.config)
        result=capture_current_query_sources(self.spark,binding,config=self.config,engine='spark')
        self.assertEqual(json.loads(result.scans_json)[0]['expectedRowCount'],'2')
