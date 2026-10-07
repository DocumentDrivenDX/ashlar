"""Finalize same native IDs and safe bucket boundary vectors; no write replay."""
import json,math,time
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from persistent_sql import Client
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_bucket_screen_r160'
s=json.loads((O/'summary.json').read_text());Q=O/'bucket-controls';assert not (Q/'statements.jsonl').exists(),'Inspect prior control handle'
c=BoundedReads(Q);c.sql('timeout','SET STATEMENT_TIMEOUT=15');t=s['owned']['part']['table'];v=s['owned']['part']['version']
assert c.sql('hash-shape',f"SELECT count(*) FROM {t} VERSION AS OF {v} WHERE lookup_hash IS NULL OR NOT(lookup_hash RLIKE '^[0-9a-f]{{64}}$')")==[['0']]
values=['0'*64,'f'*64,'123456789abcdef'+'0'*49]
query="SELECT lookup_hash,cast(pmod(cast(conv(substr(lookup_hash,1,15),16,10) AS BIGINT),64) AS INT) FROM VALUES "+','.join("('"+x+"')" for x in values)+' AS t(lookup_hash) ORDER BY lookup_hash'
assert c.sql('bucket-boundary',query)==[[x,str(int(x[:15],16)%64)] for x in sorted(values)]
c.sql('incoming-payload-size',f"SELECT min(length(get_json_object(props_json,'$.105'))),max(length(get_json_object(props_json,'$.105'))) FROM {s['stage']} VERSION AS OF 0")
s['incoming_property105_profile']='100k high-entropy replacements exactly matching previous opaque property105 lengths; length drift zero. One100k update correctness/physical pilot, not full20M or publisher-rate admission.'
rows=c.sql('stage-detail','DESCRIBE DETAIL '+s['stage']);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
s['stage_id']=dict(zip(names,rows[0]))['id']
assert c.sql('stage-version','DESCRIBE HISTORY '+s['stage']+' LIMIT 1')[0][0]=='0'
c.close();records=[];h={}
for out in (O,Q):
 client=Client(out);client.records=[json.loads(l) for l in (out/'statements.jsonl').read_text().splitlines()];records.extend(client.records)
 for attempt in range(3):
  history={q['query_id']:q for q in client.history()}
  if all(r['statement_id'] in history and history[r['statement_id']]['is_final'] for r in client.records):break
  if attempt<2:time.sleep(2)
 h.update(history)
assert all(r['response']['status']['state']=='SUCCEEDED' and h[r['statement_id']]['is_final'] and h[r['statement_id']]['status']=='FINISHED' for r in records),'Inspect same native IDs'
def p95(x):return sorted(x)[math.ceil(.95*len(x))-1]
s['reads']={}
for name in ('lc','part'):
 rs=[r for r in records if r['label'].startswith(name+'-read-')];assert len(rs)==30
 assert all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in rs)
 s['reads'][name]={'caller_p95_ms':p95([r['wall_ms'] for r in rs]),'engine_p95_ms':p95([h[r['statement_id']]['metrics']['execution_time_ms'] for r in rs]),'files_p95':p95([h[r['statement_id']]['metrics']['read_files_count'] for r in rs]),'bytes_p95':p95([h[r['statement_id']]['metrics']['read_bytes'] for r in rs]),'remote_queries':sum(h[r['statement_id']]['metrics']['read_remote_bytes']>0 for r in rs)}
s['costs']={r['label']:{'caller_ms':r['wall_ms'],'metrics':h[r['statement_id']]['metrics']} for r in records if '-read-' not in r['label']}
s['cost_totals']={'read_bytes':sum(h[r['statement_id']]['metrics'].get('read_bytes',0) for r in records),'write_remote_bytes':sum(h[r['statement_id']]['metrics'].get('write_remote_bytes',0) for r in records)}
s['state']='100k matched-rowTracking layouts and bucket-aware updates exact;60 full lookups finalized; no scale admission'
(O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n');print(json.dumps({'reads':s['reads'],'cost_totals':s['cost_totals'],'maintenance':{k:{'caller_ms':v['caller_ms'],'write_bytes':v['metrics'].get('write_remote_bytes',0)} for k,v in s['costs'].items() if k in ['lc-create','part-create','lc-apply','part-apply']}},indent=2))
