"""Verify isolation targets reach both transports without live compute."""
import tempfile,unittest
from unittest.mock import Mock,patch
from persistent_sql import Client,WAREHOUSE
from driver_sql import DriverClient
class Routing(unittest.TestCase):
 def test_rest_target_and_evidence(self):
  with tempfile.TemporaryDirectory() as out,patch('persistent_sql.WorkspaceClient') as factory:
   factory.return_value.api_client.do.return_value={'statement_id':'q','status':{'state':'SUCCEEDED'},'result':{'data_array':[['ok']]}}
   c=Client(out,warehouse_id='abcdef0123456789');self.assertEqual(c.sql('probe','SELECT 1'),[['ok']])
   self.assertEqual(factory.return_value.api_client.do.call_args.kwargs['body']['warehouse_id'],'abcdef0123456789')
   self.assertEqual(c.records[0]['warehouse_id'],'abcdef0123456789')
 def test_driver_target_and_default(self):
  for target in [WAREHOUSE,'abcdef0123456789']:
   with tempfile.TemporaryDirectory() as out,patch('persistent_sql.WorkspaceClient'),patch('driver_sql.dbsql.connect') as connect:
    c=DriverClient(out,warehouse_id=target)
    self.assertEqual(connect.call_args.kwargs['http_path'],'/sql/1.0/warehouses/'+target)
    self.assertEqual(connect.call_args.kwargs['session_configuration'],{'use_cached_result':'false'})
    c.close()
 def test_invalid_target_before_auth(self):
  with patch('persistent_sql.WorkspaceClient') as factory:
   for bad in ['','../other','abc','ABCDEF0123456789']:
    with self.assertRaises(ValueError):Client('/tmp/unused-ashlar-routing',warehouse_id=bad)
   factory.assert_not_called()
if __name__=='__main__':unittest.main()
