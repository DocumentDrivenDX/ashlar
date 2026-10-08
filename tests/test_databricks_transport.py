import sys
from pathlib import Path
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from databricks_transport import sql_result,DatabricksTransport
from durable_sql import SQLCustodyError
class TransportTests(unittest.TestCase):
    def response(self):return {'status':{'state':'SUCCEEDED'},'manifest':{'schema':{'columns':[{'name':'id','type_text':'STRING','position':0}]}},'result':{'data_array':[['9223372036854775807']]}}
    def test_exact_native_string_and_schema(self):
        r=sql_result(self.response());self.assertEqual(r.rows,[{'id':'9223372036854775807'}]);self.assertEqual(r.columns,(('id','STRING'),))
    def test_truncated_missing_shape_or_terminal_failure_refuses(self):
        for mode in ['truncated','shape','position','failed','count']:
            r=self.response()
            if mode=='truncated':r['manifest']['truncated']=True
            if mode=='shape':r['result']['data_array']=[[]]
            if mode=='position':r['manifest']['schema']['columns'][0]['position']=1
            if mode=='failed':r['status']['state']='FAILED'
            if mode=='count':r['manifest']['total_row_count']=2
            with self.assertRaises(SQLCustodyError):sql_result(r)

class OperationRoutingTests(unittest.TestCase):
    def test_explicit_mutation_identity_keeps_reads_fresh(self):
        from databricks_transport import OperationExecutor
        class Transport:
            def __init__(self):self.calls=[]
            def query(self,sql,p):self.calls.append(('read',sql));return 'fresh'
            def mutation(self,op,sql,p):self.calls.append(('write',op));return 'original'
        t=Transport();e=OperationExecutor(t,'batch-original')
        self.assertEqual(e.query('DESCRIBE DETAIL c.s.t',{}),'fresh')
        self.assertEqual(e.query('MERGE INTO c.s.t',{}),'original')
        self.assertEqual(e.query('SELECT * FROM c.s.t',{}),'fresh')
        self.assertEqual(t.calls,[('read','DESCRIBE DETAIL c.s.t'),('write','batch-original'),('read','SELECT * FROM c.s.t')])

if __name__=='__main__':unittest.main()
