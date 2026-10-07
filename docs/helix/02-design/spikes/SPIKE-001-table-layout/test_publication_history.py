"""Shared-scan and refusal controls; no authentication/native work."""
import tempfile,unittest
from pathlib import Path
from types import SimpleNamespace
from publication_history import collect_history,HistoryPending
class Api:
 def __init__(self,pages):self.pages=iter(pages);self.calls=[]
 def do(self,*args,**kwargs):self.calls.append((args,kwargs));return next(self.pages)
def record(qid):return {'statement_id':qid,'response':{'status':{'state':'SUCCEEDED'}}}
def query(qid,final=True,status='FINISHED'):return {'query_id':qid,'is_final':final,'status':status,'metrics':{'read_bytes':7}}
class Checks(unittest.TestCase):
 def collect(self,pages,records,**kwargs):
  api=Api(pages)
  with tempfile.TemporaryDirectory() as d:result=collect_history(SimpleNamespace(api_client=api),records,Path(d)/'history.json',**kwargs)
  return result,api
 def test_one_scan_for_multiple_lanes(self):
  h,a=self.collect([{'res':[query('a'),query('b'),query('unrelated')]}],[record('a'),record('b')]);self.assertEqual(set(h),{'a','b'});self.assertEqual(len(a.calls),1)
 def test_pagination(self):
  h,a=self.collect([{'res':[query('a')],'has_next_page':True,'next_page_token':'next'},{'res':[query('b')]}],[record('a'),record('b')]);self.assertEqual(len(a.calls),2);self.assertEqual(a.calls[1][1]['query']['page_token'],'next')
 def test_unknown_failed_and_duplicate_client_evidence(self):
  for rows in [[record(None)],[record('a'),record('a')],[{'statement_id':'a','response':{'status':{'state':'UNKNOWN'}}}]]:
   with self.assertRaises(ValueError):self.collect([],rows)
 def test_missing_failed_and_nonfinal_native_evidence(self):
  for pages in [[{'res':[]}],[{'res':[query('a',status='FAILED')]}],[{'res':[query('a',False)]}]]:
   with self.assertRaises(ValueError):self.collect(pages,[record('a')])
 def test_completion_mode_preserves_nonfinal_state(self):
  h,a=self.collect([{'res':[query('a',False)]}],[record('a')],require_final=False);self.assertIs(h['a']['is_final'],False)
 def test_pending_is_distinct_from_terminal_failure(self):
  with self.assertRaises(HistoryPending):self.collect([{'res':[query('a',False)]}],[record('a')])
  with self.assertRaises(HistoryPending):self.collect([{'res':[query('a',status='RUNNING')]}],[record('a')])
  try:self.collect([{'res':[query('a',status='FAILED')]}],[record('a')])
  except ValueError as error:self.assertNotIsInstance(error,HistoryPending)
  else:self.fail('Failed native query accepted')
if __name__=='__main__':unittest.main()
