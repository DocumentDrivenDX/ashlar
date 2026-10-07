"""Read-only audit of committed updates after local cost-guard stop; no replay."""
import json,time
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from persistent_sql import Client
from property_apply_queries import PropertyApply
from bucket_apply_queries import bucket_apply
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_bucket_update_r176';s=json.loads((O/'checkpoint.json').read_text())
assert s['state'].startswith('Stopped') and 'budget crossed' in s['error'];assert s['owned']['lc']['version']==1 and s['owned']['part']['version']==9
r=[json.loads(x) for x in (O/'statements.jsonl').read_text().splitlines()];assert not any('-read-' in x['label'] for x in r)
for name in ('lc','part'):
 assert next(x for x in r if x['label']==name+'-intended')['response']['result']['data_array']==[['0']]
 assert next(x for x in r if x['label']==name+'-output')['response']['result']['data_array']==[['0']]
 assert next(x for x in r if x['label']==name+'-identities')['response']['result']['data_array']==[['20000000','20000000']]
 x=s['owned'][name];q=PropertyApply(x['table'],s['stage'],x['old_version'],15,'r176-b1','r139-b1','bucket-r176',9007199254742501,eligibility_placement='on');actual=next(y['sql'] for y in r if y['label']==name+'-apply');assert actual==(q.apply() if name=='lc' else bucket_apply(q))
Q=O/'audit';assert not (Q/'statements.jsonl').exists(),'Inspect prior audit IDs'
c=BoundedReads(Q);c.sql('timeout','SET STATEMENT_TIMEOUT=30')
for name,x in s['owned'].items():
 rows=c.sql(name+'-detail','DESCRIBE DETAIL '+x['table']);names=[p['name'] for p in c.records[-1]['response']['manifest']['schema']['columns']];d=dict(zip(names,rows[0]));assert d['id']==x['id'];x['audited_detail']=d
 rows=c.sql(name+'-history','DESCRIBE HISTORY '+x['table']+' LIMIT 1');names=[p['name'] for p in c.records[-1]['response']['manifest']['schema']['columns']];last=dict(zip(names,rows[0]));assert int(last['version'])==x['version'] and last['operation']=='MERGE' and last['queryHistoryStatementId']==x['apply_query_id'];m=json.loads(last['operationMetrics']);assert m['numTargetRowsUpdated']=='100000' and m['numTargetRowsInserted']=='0' and m['numTargetRowsDeleted']=='0';x['merge_history']=last;x['merge_operation_metrics']=m
rows=c.sql('stage-detail','DESCRIBE DETAIL '+s['stage']);names=[p['name'] for p in c.records[-1]['response']['manifest']['schema']['columns']];assert dict(zip(names,rows[0]))['id']==s['stage_id'];assert c.sql('stage-version','DESCRIBE HISTORY '+s['stage']+' LIMIT 1')[0][0]=='0'
pins=c.sql('publication',"SELECT table_versions_json FROM client_dev.ashlar_entropy_20261006_r86.publication_manifest_r89 WHERE publication_id='r139-b1'");s['publication_vector']=json.loads(pins[0][0]);assert s['publication_vector'][s['source']]==23
c.close();records=[];h={}
for out in (O,Q):
 client=Client(out);client.records=[json.loads(x) for x in (out/'statements.jsonl').read_text().splitlines()];records.extend(client.records)
 for attempt in range(12):
  local={q['query_id']:q for q in client.history()}
  if all(x['statement_id'] in local and local[x['statement_id']]['is_final'] for x in client.records):break
  if attempt<11:time.sleep(5)
 assert all(local[x['statement_id']]['is_final'] and local[x['statement_id']]['status']=='FINISHED' and x['response']['status']['state']=='SUCCEEDED' for x in client.records);h.update(local)
s['costs_with_audit']={k:sum(h[x['statement_id']]['metrics'].get(k,0) for x in records) for k in ('read_bytes','write_remote_bytes','spill_to_disk_bytes')}
s['apply_metrics']={name:{'caller_ms':next(x['wall_ms'] for x in r if x['statement_id']==v['apply_query_id']),'metrics':h[v['apply_query_id']]['metrics']} for name,v in s['owned'].items()}
s['validation_costs']={x['label']:{'caller_ms':x['wall_ms'],'metrics':h[x['statement_id']]['metrics']} for x in r if x['label'].endswith('-intended') or x['label'].endswith('-output')}
s['state']='Both owned100k full20M updates exact; native commits/cost guard audited; no point reads ran'
s['qualification']='Guard stopped after physical updates plus checks exceeded75GB/4GB plans: caps are admission checks, not in-flight IO caps. Bucket copied2,987,212 unchanged rows and added381DVs while rewriting67files; this is measured mixed update behavior, no asserted mechanism or forced-DV guarantee. LC copied0, removed16hot files/added6. No identical file histories or causal partition-only comparison. No publisher freshness, source/fencing, cold/latency,1B/5B admission. Retain LC1/bucket9/stage0 for exhaustive final-wide preservation; all19.9M physical custody cannot hold for bucket because copied rows moved. Canonical r139 unchanged; no write replay.'
(O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n');print(json.dumps({'costs':s['costs_with_audit'],'apply':s['apply_metrics'],'copied':{k:x['merge_operation_metrics']['numTargetRowsCopied'] for k,x in s['owned'].items()}},indent=2))
