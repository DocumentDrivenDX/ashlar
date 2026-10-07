"""Final metrics for incremental maintenance and full-carrier read screen."""
import json,math
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_singleton_optimize_r140'
s=json.loads((B/'out/native/ashlar_singleton_custody_r141/summary.json').read_text());c=Client(O)
c.records=[json.loads(l) for l in (O/'statements.jsonl').read_text().splitlines()];h={q['query_id']:q for q in c.history()}
assert all(h[r['statement_id']]['is_final'] for r in c.records),'Refresh same IDs only'
failed=[r for r in c.records if r['response']['status']['state']!='SUCCEEDED']
assert len(failed)==1 and failed[0]['label']=='full-carrier-digests'
assert '180 seconds' in failed[0]['response']['status']['error']['message']
proof_out=B/'out/native/ashlar_singleton_custody_r141';proof_client=Client(proof_out)
proof_client.records=[json.loads(l) for l in (proof_out/'statements.jsonl').read_text().splitlines()]
proof_history={q['query_id']:q for q in proof_client.history()}
assert all(r['response']['status']['state']=='SUCCEEDED' and proof_history[r['statement_id']]['is_final'] for r in proof_client.records),'Refresh same IDs only'

def p95(values):return sorted(values)[math.ceil(.95*len(values))-1]
reads={}
for phase in ('before','after'):
 rs=[r for r in c.records if r['label'].startswith(phase+'-read-')];assert len(rs)==30
 assert all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in rs)
 reads[phase]={'caller_p95_ms':p95([r['wall_ms'] for r in rs]),'engine_p95_ms':p95([h[r['statement_id']]['metrics']['execution_time_ms'] for r in rs]),'files_p95':p95([h[r['statement_id']]['metrics']['read_files_count'] for r in rs]),'bytes_p95':p95([h[r['statement_id']]['metrics']['read_bytes'] for r in rs]),'remote_queries':sum(h[r['statement_id']]['metrics']['read_remote_bytes']>0 for r in rs)}
costs={r['label']:{'caller_ms':r['wall_ms'],'metrics':h[r['statement_id']]['metrics']} for r in c.records if r['label'] in ('clone','optimize','full-carrier-digests','identities')}
s.update(state='60 exact singleton reads audited; exact100k carriers and19.9M custody pass; full digest timeout retained',reads=reads,costs=costs,failed_full_digest=failed[0]['response']['status'],custody_costs={r['label']:{'caller_ms':r['wall_ms'],'metrics':proof_history[r['statement_id']]['metrics']} for r in proof_client.records if r['label'] in ('rewritten-exact','untouched-custody','identities')})
(O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n');print(json.dumps({'reads':reads,'costs':{k:{'caller_ms':v['caller_ms'],'engine_ms':v['metrics'].get('execution_time_ms'),'read_bytes':v['metrics'].get('read_bytes'),'write_bytes':v['metrics'].get('write_remote_bytes')} for k,v in costs.items()}},indent=2))
