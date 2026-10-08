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
if __name__=='__main__':unittest.main()
