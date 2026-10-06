"""Deliberate changed-key, alternating native singleton comparison."""
import hashlib,json
from pathlib import Path
from driver_sql import DriverClient
from edge_fused_guard import COLS
B=Path(__file__).resolve().parent;F='client_dev.ashlar_composite_edges_20261005_q1';N='client_dev.ashlar_partition_zorder_20261005_r29'
run=json.loads((B/'out/native/ashlar_partition_guarded_apply_20261005_r32/summary.json').read_text());assert run['state']=='completed'
assert json.loads((B/'out/native/ashlar_partition_history_verify_20261005_r34/summary.json').read_text())['state']=='passed'
versions={r['layout']:r['versions'] for r in run['batches']}
keys=[('pilot:'+str((rep%10)//2),7+rep%2,210001+(rep*7919)%30000) for rep in range(51)]
assert len(set(keys))==51 and all(210001<=k[2]<=240000 for k in keys) and len({k[:2] for k in keys})==10
out=B/'out/native/ashlar_partition_changed_reads_20261005_r36';c=DriverClient(out);c.sql('statement-cap','SET STATEMENT_TIMEOUT=180')
where=' OR '.join(f"(source_system='{s}' AND rel_type_id={r} AND id={i})" for s,r,i in keys)
oldrows=c.sql('setup-stage-values',f"SELECT {','.join(COLS)} FROM {N}.stage_r31 WHERE {where}")
old={(r[0],int(r[1]),int(r[2])):r for r in oldrows};assert len(oldrows)==len(old)==51
expected={}
for phase_i,phase in enumerate(['prime','repeat','repeat2']):
 for rep,(source,rel,id) in enumerate(keys):
  h=hashlib.sha256(json.dumps(dict(source_system=source,rel_type_id=rel,id=id),separators=(',',':')).encode()).hexdigest();bucket=int(h[0],16)//4
  payload=''.join(hashlib.sha256(f'{source}:{rel}:{id}:r31:{i}'.encode()).hexdigest() for i in range(32))
  for layout in (['lc','partition'] if (rep+phase_i)%2==0 else ['partition','lc']):
   ns=F if layout=='lc' else N;v=versions[layout][ns+'.edge_current'];extra='' if layout=='lc' else f' AND lookup_bucket={bucket}'
   rows=c.sql(f'{layout}-{phase}-{rep}',f"SELECT {','.join(COLS)} FROM {ns}.edge_current VERSION AS OF {v} WHERE lookup_hash='{h}' AND source_system='{source}' AND rel_type_id={rel} AND id={id}{extra}")
   assert len(rows)==1 and rows[0][15] is not None
   want=list(old[(source,rel,id)]);want[8]='8';want[9]=json.dumps({'201':payload},separators=(',',':'));want[12]='fixture-layout-compare';want[13]='e';want[14]='1';want[15]=rows[0][15]
   assert rows==[want] and rows[0][16]==h, f'Changed full carrier mismatch {layout}/{phase}/{rep}'
   if phase=='prime':expected[(layout,rep)]=rows
   else:assert rows==expected[(layout,rep)]
 print(phase,'completed',flush=True)
c.history();c.close();(out/'scope.json').write_text(json.dumps({'state':'completed','versions':versions,'changed_keys':51,'native_domains':10,'scope':'all keys updated in r32; identical alternating full 17-field reads; independent changed property/hash and prior carrier, publication timestamp non-null then repeat equality; no maintenance/concurrent reads/full-scale admission'},indent=2));print('Deliberate changed-key comparison passed',flush=True)
