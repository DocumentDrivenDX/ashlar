"""Audit both native writer arms and verify cross-stage identity/structure cohort."""
import json,time
from pathlib import Path
from bounded_reads_r145 import BoundedReads
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_lc_writer_isolation_r195';Q=O/'cohort-audit';assert not Q.exists()
s=json.loads((O/'summary.json').read_text());assert s['state']=='Both sequential stage writer arms exact, custody and full20M IDs passed'
records=[json.loads(x) for x in (O/'statements.jsonl').read_text().splitlines()];h={x['query_id']:x for x in json.loads((O/'query-history.json').read_text())};assert all(r['response']['status']['state']=='SUCCEEDED' and h[r['statement_id']]['is_final'] and h[r['statement_id']]['status']=='FINISHED' for r in records)
for name,x in s['arms'].items():
 assert x['version']==1 and len(x['hot_files'])==6
 ordered=sorted(x['hot_files'],key=lambda r:r[3]);assert all(a[4]<b[3] for a,b in zip(ordered,ordered[1:]))
 r=next(r for r in records if r['label']==name+'-clone-detail');d=dict(zip([p['name'] for p in r['response']['manifest']['schema']['columns']],r['response']['result']['data_array'][0]));props=json.loads(d['properties']);assert props['delta.enableRowTracking']=='true' and props['delta.enableDeletionVectors']=='true';assert d['minReaderVersion']=='3' and d['minWriterVersion']=='7'
 x['audit_disjoint_live_ranges']=True
c=BoundedReads(Q);c.sql('timeout','SET STATEMENT_TIMEOUT=30')
cols='source_system,rel_type_id,id,source_type,source_id,target_type,target_id,lookup_hash'
a='SELECT '+cols+' FROM '+s['arms']['fresh']['stage']+' VERSION AS OF 0';b='SELECT '+cols+' FROM '+s['arms']['prior']['stage']+' VERSION AS OF 0'
assert c.sql('same-identity-structure-cohort',f'SELECT count(*) FROM (({a} EXCEPT ALL {b}) UNION ALL ({b} EXCEPT ALL {a}))')==[['0']]
for name,x in s['arms'].items():assert c.sql(name+'-version','DESCRIBE HISTORY '+x['table']+' LIMIT 1')[0][0]=='1'
c.close()
for attempt in range(8):
 ah={x['query_id']:x for x in c.history()}
 if all(r['statement_id'] in ah and ah[r['statement_id']]['is_final'] for r in c.records):break
 time.sleep(2)
assert all(ah[r['statement_id']]['is_final'] and ah[r['statement_id']]['status']=='FINISHED' for r in c.records)
s['audit_costs']={k:sum(ah[r['statement_id']]['metrics'].get(k,0) for r in c.records) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};assert s['audit_costs']['read_bytes']<=2000000000 and s['audit_costs']['write_remote_bytes']==0
s['cohort_qualification']='Full100k same native identity, typed endpoints and lookup hashes proven across stages. Values/epochs/delivery/timestamps and physical layouts differ. Both sequential outputs have six disjoint live-file hash extents; does not prove future writer determinism or that concurrency caused earlier broad output.'
(O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n');print(json.dumps({'costs':s['costs'],'audit_costs':s['audit_costs'],'apply_callers_ms':{n:x['apply_metrics']['caller_ms'] for n,x in s['arms'].items()}},indent=2))
