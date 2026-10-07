"""Finalize saved IDs and verify wrong-origin bounds refuse all missing records."""
import json
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from persistent_sql import Client
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_origin_validation_r151'
s=json.loads((O/'summary.json').read_text());assert s['state'].startswith('Three alternating')
records=[json.loads(l) for l in (O/'statements.jsonl').read_text().splitlines()]
query=next(r['sql'] for r in records if r['label']=='bounded-0')
Q=O/'refusal-controls';assert not (Q/'statements.jsonl').exists(),'Inspect prior control handle; no replay'
c=BoundedReads(Q);c.sql('timeout','SET STATEMENT_TIMEOUT=30')
def lit(value):return "decode(unhex('"+value.encode().hex()+"'),'UTF-8')"
feed,epoch=s['origins'][0][:2]
for name,value in [('source_feed',feed),('source_epoch',epoch)]:
 old=name+'='+lit(value);new=name+'='+lit('ashlar-r151-intentionally-absent-origin')
 assert query.count(old)==1
 assert c.sql('wrong-'+name,query.replace(old,new))==[['100000']]
c.close()
h={};all_records=[]
for out in (O,Q):
 x=Client(out);x.records=[json.loads(l) for l in (out/'statements.jsonl').read_text().splitlines()]
 all_records.extend(x.records);h.update({q['query_id']:q for q in x.history()})
assert all(r['response']['status']['state']=='SUCCEEDED' and h[r['statement_id']]['is_final'] and h[r['statement_id']]['status']=='FINISHED' for r in all_records),'Inspect same saved IDs'
s['costs']={r['label']:{'caller_ms':r['wall_ms'],'metrics':h[r['statement_id']]['metrics']} for r in all_records}
for mode in ('control','bounded'):
 rs=[r for r in records if r['label'].startswith(mode+'-')];assert len(rs)==3
 assert all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in rs)
 s[mode]={'caller_ms':[r['wall_ms'] for r in rs],'engine_ms':[h[r['statement_id']]['metrics']['execution_time_ms'] for r in rs],'files':[h[r['statement_id']]['metrics']['read_files_count'] for r in rs],'read_bytes':[h[r['statement_id']]['metrics']['read_bytes'] for r in rs],'remote_bytes':[h[r['statement_id']]['metrics']['read_remote_bytes'] for r in rs]}
s['refusals']='Wrong explicit feed and epoch each produce100k validation failures; no missing-row suppression'
s['state']='Three exact paired origin-filter validations and two wrong-origin refusal controls finalized'
s['cost_totals']={'read_bytes':sum(v['metrics'].get('read_bytes',0) for v in s['costs'].values()),'write_remote_bytes':sum(v['metrics'].get('write_remote_bytes',0) for v in s['costs'].values())}
(O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n')
print(json.dumps({k:s[k] for k in ['control','bounded','refusals','cost_totals']},indent=2))
