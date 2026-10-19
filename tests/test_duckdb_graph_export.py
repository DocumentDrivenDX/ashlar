# SPDX-License-Identifier: MIT OR Apache-2.0
import copy,hashlib,json,unittest
from unittest.mock import patch
import duckdb
from ashlar.graph_release import GraphRelease
from ashlar_host.duckdb_graph_export import query_duckdb_export,run_query_duckdb_export_command
from ashlar_host.local_graph_export import LocalGraphExportUse
import test_private_graph_custody as fixture
import test_graph_export_diff as pairs

class Tests(unittest.TestCase):
 def test_real_duckdb_parallel_cycle_and_isolate(self):
  release,_=fixture.Tests().fixture();graph=json.loads(release.payload)
  a,b=graph['nodes'][:2];isolate=copy.deepcopy(a);isolate['graph_id']='isolated';graph['nodes']=[a,b,isolate]
  edge=copy.deepcopy(graph['edges'][0]);edge.update(graph_id='parallel-a',src=a['graph_id'],dst=b['graph_id'])
  parallel=copy.deepcopy(edge);parallel['graph_id']='parallel-b'
  back=copy.deepcopy(edge);back.update(graph_id='back',src=b['graph_id'],dst=a['graph_id'])
  loop=copy.deepcopy(edge);loop.update(graph_id='self',src=b['graph_id'],dst=b['graph_id'])
  graph['edges']=[edge,parallel,back,loop];raw=json.dumps(graph).encode();release=GraphRelease(raw,hashlib.sha256(raw).hexdigest())
  con=duckdb.connect(':memory:')
  try:result=query_duckdb_export(con,release,2)
  finally:con.close()
  self.assertEqual(len(result['one_hop']),4);self.assertEqual(len(result['two_hop']),8)
  counts={r[0]:r[1:]for r in result['two_hop_counts']};self.assertEqual(counts[a['graph_id']],[4,2]);self.assertEqual(counts['isolated'],[0,0]);self.assertEqual(result['engine']['name'],'DuckDB');self.assertTrue(result['engine']['version'])
 def test_wrong_digest_and_owner_final_close_withhold_output(self):
  helper=pairs.Tests();root,configs,_,_=helper.fixture();self.addCleanup(helper.doCleanups);out=root/'duck.json'
  original=configs[0].read_bytes();bad=json.loads(original);bad['release_sha256']='0'*64;configs[0].write_text(json.dumps(bad))
  with self.assertRaises(ValueError):run_query_duckdb_export_command(configs[0],out)
  self.assertFalse(out.exists());configs[0].write_bytes(original)
  renewal=LocalGraphExportUse.renew;seen=set()
  def close(owner):
   renewal(owner)
   if owner._active:seen.add(owner.context)
   elif owner.context in seen:raise PermissionError('Close refused')
  with patch.object(LocalGraphExportUse,'renew',close):
   with self.assertRaises(PermissionError):run_query_duckdb_export_command(configs[0],out)
  self.assertFalse(out.exists())
