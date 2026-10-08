import importlib.util
from pathlib import Path
import tempfile
import unittest

spec=importlib.util.spec_from_file_location('durable_sql',Path(__file__).resolve().parents[1]/'tools/durable_sql.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
class API:
    def __init__(self,state='SUCCEEDED',lost=False):self.state=state;self.lost=lost;self.calls=[]
    def do(self,method,path,**kwargs):
        self.calls.append((method,path))
        if method=='POST' and self.lost:raise RuntimeError('Response lost')
        return {'statement_id':'original','status':{'state':self.state},'manifest':{'truncated':False},'result':{'data_array':[['1']]}}
class DurableSQLTests(unittest.TestCase):
    def client(self,path,api,authority='principal'):return m.DurableSQL(path,api,'2439e1f2e37ac563',authority)
    def test_fresh_client_replays_terminal_without_network_and_conflicts_refuse(self):
        with tempfile.TemporaryDirectory() as d:
            path=str(Path(d)/'journal.sqlite');api=API();c=self.client(path,api)
            original=c.query('op','SELECT :v',{'v':'1'});c.close()
            c=self.client(path,api)
            self.assertEqual(c.query('op','SELECT :v',{'v':'1'}),original)
            with self.assertRaises(m.SQLCustodyError):c.query('op','SELECT :v',{'v':'2'})
            c.close();c=self.client(path,api,'other')
            with self.assertRaises(m.SQLCustodyError):c.query('op','SELECT :v',{'v':'1'})
            c.close();self.assertEqual(len(api.calls),1)
    def test_pending_recovery_uses_only_original_handle(self):
        with tempfile.TemporaryDirectory() as d:
            path=str(Path(d)/'journal.sqlite');api=API('RUNNING');c=self.client(path,api)
            with self.assertRaises(m.SQLPending):c.query('op','SELECT 1',{})
            c.close();api.state='SUCCEEDED';c=self.client(path,api)
            c.query('op','SELECT 1',{});c.close()
            self.assertEqual(api.calls,[('POST','/api/2.0/sql/statements'),('GET','/api/2.0/sql/statements/original')])
    def test_lost_submission_never_reposts(self):
        with tempfile.TemporaryDirectory() as d:
            path=str(Path(d)/'journal.sqlite');api=API(lost=True);c=self.client(path,api)
            with self.assertRaises(RuntimeError):c.query('op','SELECT 1',{})
            c.close();api.lost=False;c=self.client(path,api)
            with self.assertRaises(m.SQLCustodyError):c.query('op','SELECT 1',{})
            c.close();self.assertEqual(len(api.calls),1)
    def test_terminal_failure_is_original_failure_on_replay(self):
        with tempfile.TemporaryDirectory() as d:
            api=API('FAILED');c=self.client(str(Path(d)/'journal.sqlite'),api)
            for _ in range(2):
                with self.assertRaises(m.SQLCustodyError):c.query('op','invalid sql',{})
            c.close();self.assertEqual(len(api.calls),1)
if __name__=='__main__':unittest.main()
