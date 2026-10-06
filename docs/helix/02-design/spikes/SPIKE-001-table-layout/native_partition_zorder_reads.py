"""Alternating matched full-carrier singleton layout comparison."""
import hashlib,json
from pathlib import Path
from driver_sql import DriverClient
from edge_fused_guard import COLS
B=Path(__file__).resolve().parent
F='client_dev.ashlar_composite_edges_20261005_q1.edge_current'
N='client_dev.ashlar_partition_zorder_20261005_r29.edge_current'
build=json.loads((B/'out/native/ashlar_partition_zorder_build_20261005_r29/summary.json').read_text());assert build['state']=='passed'
prior=[json.loads(l) for l in (B/'out/native/ashlar_edge_recluster_reads_20261005_r27/statements.jsonl').read_text().splitlines()]
expected={int(r['label'].rsplit('-',1)[1]):[row[:17] for row in r['response']['result']['data_array']] for r in prior if r['label'].startswith('after-prime-')}
out=B/'out/native/ashlar_partition_zorder_reads_20261005_r30';c=DriverClient(out);c.sql('statement-cap','SET STATEMENT_TIMEOUT=180')
for phase_i,phase in enumerate(['prime','repeat','repeat2']):
 for rep in range(51):
  domain=rep%10;source='pilot:'+str(domain//2);rel=7+domain%2;id=101+(rep*196613)%999900
  h=hashlib.sha256(json.dumps(dict(source_system=source,rel_type_id=rel,id=id),separators=(',',':')).encode()).hexdigest();bucket=int(h[0],16)//4
  order=['lc','partition'] if (rep+phase_i)%2==0 else ['partition','lc']
  for layout in order:
   table,v=(F,19) if layout=='lc' else (N,build['version']);extra='' if layout=='lc' else f' AND lookup_bucket={bucket}'
   q=f"SELECT {','.join(COLS)} FROM {table} VERSION AS OF {v} WHERE lookup_hash='{h}' AND source_system='{source}' AND rel_type_id={rel} AND id={id}{extra}"
   assert c.sql(f'{layout}-{phase}-{rep}',q)==expected[rep], f'Canonical mismatch {layout}/{phase}/{rep}'
 print(phase,'completed',flush=True)
c.history();c.close()
(out/'scope.json').write_text(json.dumps({'state':'completed','versions':{'lc':19,'partition':build['version']},'scope':'identical 51 keys/17-field projection, three alternating rounds, exact prior independently-checked carrier equality, explicit bucket/hash plus native predicates; no ingest or full-scale admission'},indent=2))
print('Matched partition/Z-order singleton reads passed',flush=True)
