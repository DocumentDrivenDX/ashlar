"""Wide canonical 64MiB copy: first-touch reads before exhaustive parity scans."""
import hashlib,json,time
from pathlib import Path
from driver_sql import DriverClient
from edge_fused_guard import COLS
B=Path(__file__).resolve().parent;F='client_dev.ashlar_composite_edges_20261005_q1';N='client_dev.ashlar_lc64_20261005_r42'
assert json.loads((B/'out/native/ashlar_conditional_verify_20261006_r53/summary.json').read_text())['state']=='passed'
out=B/'out/native/ashlar_hash_candidate_reads_20261006_r58';c=DriverClient(out);c.sql('statement-cap','SET STATEMENT_TIMEOUT=180')
keys=[('pilot:'+str((i%10)//2),7+i%2,330001+(i*7919)%30000) for i in range(51)]
where=' OR '.join(f"(source_system='{s}' AND rel_type_id={r} AND id={i})" for s,r,i in keys)
oldrows=c.sql('source-sample',f"SELECT {','.join(COLS)} FROM {N}.edge_current VERSION AS OF 1 WHERE {where}")
expected={(r[0],int(r[1]),int(r[2])):[r] for r in oldrows};assert len(expected)==len(oldrows)==51
prior=expected
sample=c.sql('updated-sample',f"SELECT {','.join(COLS)},apply_batch_id FROM {N}.edge_current VERSION AS OF 5 WHERE {where}")
expected={}
for row in sample:
 source,rel,id=row[0],int(row[1]),int(row[2]);old=prior[(source,rel,id)][0]
 payload=''.join(hashlib.sha256(f'{source}:{rel}:{id}:r51:{i}'.encode()).hexdigest() for i in range(32))
 assert all(row[i]==old[i] for i in range(17) if i not in [8,9,12,13,14,15])
 assert row[9]==json.dumps({'201':payload},separators=(',',':')) and row[8]=='11' and row[12:15]==['fixture-conditional','e','1'] and row[15] is not None and row[17]=='r52:publisher:1:1'
 expected[(source,rel,id)]=[row[:17]]
assert len(expected)==51
v=5
for phase_i,phase in enumerate(['prime','repeat','repeat2']):
 for rep,(source,rel,id) in enumerate(keys):
  h=hashlib.sha256(json.dumps(dict(source_system=source,rel_type_id=rel,id=id),separators=(',',':')).encode()).hexdigest()
  for layout in (['exact','candidate'] if (rep+phase_i)%2==0 else ['candidate','exact']):
   ns,version=N,v;identity=f" AND source_system='{source}' AND rel_type_id={rel} AND id={id}" if layout=='exact' else ''
   rows=c.sql(f'lc{layout}-{phase}-{rep}',f"SELECT {','.join(COLS)} FROM {ns}.edge_current VERSION AS OF {version} WHERE lookup_hash='{h}' {identity}")
   assert len(rows)<1000, 'Refuse unbounded/truncated candidate set'
   matches=[row for row in rows if (row[0],int(row[1]),int(row[2]))==(source,rel,id)]
   assert len(matches)==1, 'Missing or duplicate exact native identity'
   assert matches==expected[(source,rel,id)] and matches[0][16]==h
 print(phase,'completed',flush=True)
c.history();c.close();(out/'scope.json').write_text(json.dumps({'state':'completed','candidate_version':5,'scope':'51 deliberately updated identities across ten domains; independent payload and all unchanged17-field carriers checked against prior version1; paired exact SQL versus hash candidates with exact client identity filtering and bounded-set refusal; three repeated phases; no result cache; exhaustive scans already warmed data; no cold/concurrent/scale admission'},indent=2));print('Conditional updated singleton reads passed',flush=True)
