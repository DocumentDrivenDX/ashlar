"""Alternating full-carrier singleton comparison after the guarded batch."""
import hashlib,json
from pathlib import Path
from driver_sql import DriverClient
from edge_fused_guard import COLS
B=Path(__file__).resolve().parent;F='client_dev.ashlar_composite_edges_20261005_q1';N='client_dev.ashlar_partition_zorder_20261005_r29'
run=json.loads((B/'out/native/ashlar_partition_guarded_apply_20261005_r32/summary.json').read_text());assert run['state']=='completed'
versions={r['layout']:r['versions'] for r in run['batches']}
prior=[json.loads(l) for l in (B/'out/native/ashlar_partition_zorder_reads_20261005_r30/statements.jsonl').read_text().splitlines()]
old={int(r['label'].rsplit('-',1)[1]):r['response']['result']['data_array'] for r in prior if r['label'].startswith('lc-prime-')}
out=B/'out/native/ashlar_partition_post_reads_20261005_r35';c=DriverClient(out);expected={}
c.sql('statement-cap','SET STATEMENT_TIMEOUT=180')
for layout,ns in [('lc',F),('partition',N)]:c.sql('detail-'+layout,f'DESCRIBE DETAIL {ns}.edge_current')
for phase_i,phase in enumerate(['prime','repeat','repeat2']):
 for rep in range(51):
  source='pilot:'+str((rep%10)//2);rel=7+rep%2;id=101+(rep*196613)%999900
  h=hashlib.sha256(json.dumps(dict(source_system=source,rel_type_id=rel,id=id),separators=(',',':')).encode()).hexdigest();bucket=int(h[0],16)//4
  for layout in (['lc','partition'] if (rep+phase_i)%2==0 else ['partition','lc']):
   ns=F if layout=='lc' else N;v=versions[layout][ns+'.edge_current'];extra='' if layout=='lc' else f' AND lookup_bucket={bucket}'
   rows=c.sql(f'{layout}-{phase}-{rep}',f"SELECT {','.join(COLS)} FROM {ns}.edge_current VERSION AS OF {v} WHERE lookup_hash='{h}' AND source_system='{source}' AND rel_type_id={rel} AND id={id}{extra}")
   want=[list(r) for r in old[rep]]
   if 210001<=id<=240000:
    assert len(rows)==1 and rows[0][15] is not None
    payload=''.join(hashlib.sha256(f'{source}:{rel}:{id}:r31:{i}'.encode()).hexdigest() for i in range(32))
    want[0][8]='8';want[0][9]=json.dumps({'201':payload},separators=(',',':'));want[0][12]='fixture-layout-compare';want[0][13]='e';want[0][14]='1';want[0][15]=rows[0][15]
   assert rows==want, f'Expected prior/new carrier mismatch {layout}/{phase}/{rep}'
   if phase=='prime':expected[(layout,rep)]=rows
   else:assert rows==expected[(layout,rep)]
 print(phase,'completed',flush=True)
c.history();c.close();(out/'scope.json').write_text(json.dumps({'state':'completed','versions':versions,'scope':'51 identical alternating native keys and full 17-field projection after guarded publication; independent changed property payload and unchanged fields; timestamp non-null then repeat equality; no maintenance/concurrent readers/full-scale admission'},indent=2));print('Paired post-ingest reads passed',flush=True)
