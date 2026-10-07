"""Finalize full-copy costs and inspect exact owned lineage; no writes."""
import json,time
from pathlib import Path
from persistent_sql import Client
from bounded_reads_r145 import BoundedReads
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_bucket_build_r166'
s=json.loads((O/'checkpoint.json').read_text());assert s['state'].startswith('Full20M') and len(s['chunks'])==4 and s['version']==4
Q=O/'audit';assert not (Q/'statements.jsonl').exists(),'Inspect same audit handles'
c=BoundedReads(Q);c.sql('timeout','SET STATEMENT_TIMEOUT=30');T=s['table']
rows=c.sql('detail','DESCRIBE DETAIL '+T);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];s['detail']=dict(zip(names,rows[0]));assert s['detail']['id']==s['id']
rows=c.sql('history','DESCRIBE HISTORY '+T);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];history=[dict(zip(names,r)) for r in rows]
assert [int(r['version']) for r in history]==[4,3,2,1,0]
for chunk in s['chunks']:
 row=next(r for r in history if int(r['version'])==chunk['version']);assert row['operation']=='WRITE' and row['queryHistoryStatementId']==chunk['append_query_id'];assert json.loads(row['operationMetrics'])['numOutputRows']==chunk['expected_count']
s['delta_history']=history
s['bucket_distribution']=c.sql('distribution',f'SELECT lookup_bucket,count(*) FROM {T} VERSION AS OF 4 GROUP BY lookup_bucket ORDER BY lookup_bucket');assert len(s['bucket_distribution'])==64 and sum(int(x[1]) for x in s['bucket_distribution'])==20000000
pins=c.sql('publication',"SELECT table_versions_json FROM client_dev.ashlar_entropy_20261006_r86.publication_manifest_r89 WHERE publication_id='r139-b1'");s['publication_vector']=json.loads(pins[0][0]);assert s['publication_vector'][s['source']]==23
c.close();records=[];metrics={}
for out in [B/'out/native/ashlar_bucket_build_r164',B/'out/native/ashlar_bucket_build_r165',O,Q]:
 client=Client(out);client.records=[json.loads(x) for x in (out/'statements.jsonl').read_text().splitlines()];records.extend(client.records)
 for attempt in range(3):
  h={q['query_id']:q for q in client.history()}
  if all(r['statement_id'] in h and h[r['statement_id']]['is_final'] for r in client.records):break
  if attempt<2:time.sleep(2)
 assert all(h[r['statement_id']]['is_final'] and h[r['statement_id']]['status']=='FINISHED' for r in client.records);metrics.update(h)
s['costs_total']={k:sum(metrics[r['statement_id']]['metrics'].get(k,0) for r in records) for k in ('read_bytes','write_remote_bytes')}
s['appends']=[{'statement_id':r['statement_id'],'caller_ms':r['wall_ms'],'metrics':metrics[r['statement_id']]['metrics']} for r in records if r['label'].startswith('append-')];assert len(s['appends'])==4
s['qualification']='Four immutable E23 disjoint hash-range appends; all20 logical columns plus derived bucket selected. Native20M count/globalIDs and bucket derivation/distribution pass. Not yet full20-field parity, ZORDER, update/read comparison, cold, publication-rate or billion admission. r164 result-array-shape assertion failed after successful commit; r165 missing local variable failed before an append; r166 inspected native version1 and continued only remaining ranges. Candidate retained at UUID/version4 for the next validation phase; no canonical publication change.'
(O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n');print(json.dumps({'costs':s['costs_total'],'files':s['detail']['numFiles'],'bytes':s['detail']['sizeInBytes'],'append_caller_ms':[x['caller_ms'] for x in s['appends']]},indent=2))
