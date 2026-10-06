"""Matched native singleton reads around one counted incremental OPTIMIZE."""
import hashlib,json,time
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;F='client_dev.ashlar_composite_edges_20261005_q1'
out=B/'out/native/ashlar_edge_recluster_reads_20261005_r27';c=DriverClient(out)
c.sql('statement-cap','SET STATEMENT_TIMEOUT=180')
before=int(c.sql('before-version',f'DESCRIBE HISTORY {F}.edge_current LIMIT 1')[0][0])
assert before==16, 'Unexpected current snapshot; stop before maintenance'
c.sql('before-detail',f'DESCRIBE DETAIL {F}.edge_current')
expected={};versions={'before':before};maintenance=None
for layout in ['before','after']:
 if layout=='after':
  t=time.perf_counter();c.sql('incremental-optimize',f'OPTIMIZE {F}.edge_current');maintenance=time.perf_counter()-t
  versions[layout]=int(c.sql('after-version',f'DESCRIBE HISTORY {F}.edge_current LIMIT 1')[0][0])
  c.sql('after-detail',f'DESCRIBE DETAIL {F}.edge_current')
  c.sql('maintenance-history',f'DESCRIBE HISTORY {F}.edge_current LIMIT 3')
 for phase in ['prime','repeat','repeat2']:
  for rep in range(51):
   domain=rep%10;source='pilot:'+str(domain//2);rel=7+domain%2;typ=1+domain%2
   id=101+(rep*196613)%999900;src=1+((id-1)*104729)%200000;slot=(id-1)//200000
   target=1+(src+(17 if slot<2 else slot*17)-1)%200000
   h=hashlib.sha256(json.dumps(dict(source_system=source,rel_type_id=rel,id=id),separators=(',',':')).encode()).hexdigest()
   rows=c.sql(f'{layout}-{phase}-{rep}',f"SELECT e.*,e._metadata.row_id,e._metadata.row_commit_version FROM {F}.edge_current VERSION AS OF {versions[layout]} e WHERE lookup_hash='{h}' AND source_system='{source}' AND rel_type_id={rel} AND id={id}")
   assert len(rows)==1 and rows[0][:7]==[source,str(rel),str(id),str(typ),str(src),str(typ),str(target)] and rows[0][16]==h
   band=(((id-1)*104729)%1000000)//20000
   tag='r17' if 120001<=id<=210000 else 'r16' if 30001<=id<=120000 else 'q2' if band==0 else 'r13' if band in [1,3,4,6] else None
   payload=''.join(hashlib.sha256((f'{source}:{rel}:{id}:{tag}:{i}' if tag else f'{id}:edge:{i}').encode()).hexdigest() for i in range(32 if tag else 8))
   assert rows[0][9]==json.dumps({'201':payload},separators=(',',':'))
   assert rows[0][10]=='{"future":{"preserve":true}}', rows[0][10]
   if layout=='before' and phase=='prime':expected[rep]=rows
   else:assert rows==expected[rep], f'Full 17-field plus hidden metadata mismatch: {layout}/{phase}/{rep}'
  print(layout,phase,'completed',flush=True)
  (out/'progress.json').write_text(json.dumps({'layout':layout,'phase':phase,'versions':versions,'maintenance_wall_s':maintenance},indent=2))
c.history();c.close()
(out/'scope.json').write_text(json.dumps({'state':'completed','versions':versions,'maintenance_wall_s':maintenance,'scope':'51 keys in ten native domains, three phases each before/after; full 17 carrier fields plus hidden row identity/commit version parity; independently computed property201; one incremental OPTIMIZE; no exhaustive preservation, continuous policy, cold-cache or scale admission'},indent=2))
print('Edge reclustering comparison completed',flush=True)
