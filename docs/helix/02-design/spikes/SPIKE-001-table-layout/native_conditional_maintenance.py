"""Count layout maintenance then matched changed-key reads; no publication promotion."""
import hashlib,json,time
from pathlib import Path
from driver_sql import DriverClient
from edge_fused_guard import COLS
B=Path(__file__).resolve().parent;F='client_dev.ashlar_composite_edges_20261005_q1';N='client_dev.ashlar_lc64_20261005_r42'
prior=B/'out/native/ashlar_conditional_reads_20261006_r55';scope=json.loads((prior/'scope.json').read_text());assert scope['state']=='completed'
rs=[json.loads(l) for l in (prior/'statements.jsonl').read_text().splitlines()]
expected={(r['label'].split('-')[0],int(r['label'].rsplit('-',1)[1])):r['response']['result']['data_array'] for r in rs if '-prime-' in r['label']}
out=B/'out/native/ashlar_conditional_maintenance_20261006_r56';c=DriverClient(out);c.sql('statement-cap','SET STATEMENT_TIMEOUT=180')
versions={};maintenance=[]
for layout,ns in [('lc64',N)]:
 before=int(c.sql('before-version-'+layout,f'DESCRIBE HISTORY {ns}.edge_current LIMIT 1')[0][0]);assert before==scope['candidate_version']
 c.sql('before-detail-'+layout,f'DESCRIBE DETAIL {ns}.edge_current');t=time.perf_counter()
 suffix=' ZORDER BY (lookup_hash)' if layout=='partition' else ''
 c.sql('maintain-'+layout,f'OPTIMIZE {ns}.edge_current{suffix}');wall=time.perf_counter()-t
 after=int(c.sql('after-version-'+layout,f'DESCRIBE HISTORY {ns}.edge_current LIMIT 1')[0][0]);versions[layout]=after
 c.sql('after-detail-'+layout,f'DESCRIBE DETAIL {ns}.edge_current');c.sql('history-'+layout,f'DESCRIBE HISTORY {ns}.edge_current LIMIT 8')
 maintenance.append({'layout':layout,'before':before,'after':after,'wall_s':wall});(out/'progress.json').write_text(json.dumps({'state':'maintenance','maintenance':maintenance},indent=2));print(json.dumps(maintenance[-1]),flush=True)
for phase_i,phase in enumerate(['prime','repeat','repeat2']):
 for rep in range(51):
  source='pilot:'+str((rep%10)//2);rel=7+rep%2;id=330001+(rep*7919)%30000
  h=hashlib.sha256(json.dumps(dict(source_system=source,rel_type_id=rel,id=id),separators=(',',':')).encode()).hexdigest();bucket=int(h[0],16)//4
  for layout in ['lc64']:
   ns=N;extra=''
   rows=c.sql(f'{layout}-{phase}-{rep}',f"SELECT {','.join(COLS)} FROM {ns}.edge_current VERSION AS OF {versions[layout]} WHERE lookup_hash='{h}' AND source_system='{source}' AND rel_type_id={rel} AND id={id}{extra}")
   assert rows==expected[(layout,rep)], f'Full carrier preservation mismatch {layout}/{phase}/{rep}'
 print(phase,'completed',flush=True)
c.history();c.close();(out/'scope.json').write_text(json.dumps({'state':'completed','versions':versions,'maintenance':maintenance,'scope':'one incremental LC64 maintenance on conditional-update snapshot3; same51 conditional changed keys/full17-field projection exact equality; new versions not published, exhaustive hidden-metadata parity separate; no continuous policy, rate, scale or external-reader admission'},indent=2));print('Conditional LC64 maintenance and changed-key control passed',flush=True)
