"""Fixed-vector SQL traversal parity; narrow adjacency versus canonical typed endpoints."""
import json
from pathlib import Path
from driver_sql import DriverClient
BASE=Path(__file__).resolve().parent;out=BASE/'out/native/ashlar_graph_traversal_20261005_l3';c=DriverClient(out);F='client_dev.ashlar_edge_ingest_20261005_l1'
c.sql('statement-cap','SET STATEMENT_TIMEOUT=180')
rows=c.sql('descriptor',f"SELECT table_versions_json FROM {F}.publication_manifest WHERE publication_id='synthetic-graph-release-1'")
assert len(rows)==1;v=json.loads(rows[0][0])
for rep in range(31):
 root=1+(rep*7919)%200000
 for shape in ['outgoing','reverse','twohop']:
  baseline=None
  for name in ['adjacency','edge_current']:
   table=f'{F}.{name} VERSION AS OF {v[F+"."+name]}'
   if shape=='outgoing':query=f"SELECT id,source_id,target_id FROM {table} WHERE source_system='pilot' AND rel_type_id=7 AND source_type=1 AND source_id={root} ORDER BY id"
   elif shape=='reverse':query=f"SELECT id,source_id,target_id FROM {table} WHERE source_system='pilot' AND rel_type_id=7 AND target_type=1 AND target_id={root} ORDER BY id"
   else:query=f"SELECT a.id,b.id,a.target_id,b.target_id FROM {table} a JOIN {table} b ON a.source_system=b.source_system AND a.target_type=b.source_type AND a.target_id=b.source_id WHERE a.source_system='pilot' AND a.rel_type_id=7 AND b.rel_type_id=7 AND a.source_type=1 AND a.source_id={root} ORDER BY a.id,b.id"
   result=c.sql(f'{name}-{shape}-{rep}',query)
   assert len(result)==(25 if shape=='twohop' else 5)
   if baseline is None:baseline=result
   else:assert result==baseline
 if rep%10==0:print('round',rep,flush=True)
c.history();c.close();(out/'summary-scope.json').write_text(json.dumps({'state':'completed','versions':v,'shape':'regular five-edge degree; two parallel edges preserved; 25 ordered edge-ID path pairs','scope':'fixed graph-vector native SQL parity/performance; no hubs, budget rejection, external engines, concurrent writes or billion-scale admission'},indent=2)+'\n');print('Fixed-vector traversal controls completed',flush=True)
