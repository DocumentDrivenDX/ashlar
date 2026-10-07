"""Offline audit of terminal read-only eligibility observations."""
import hashlib,json
from pathlib import Path
B=Path(__file__).resolve().parent

def main():
 for name in ['ashlar_third_publisher_preflight_r386','ashlar_node_head_inspect_r387','ashlar_third_publisher_preflight_r388','ashlar_third_publisher_preflight_r389']:
  p=B/'out/native'/name;rs=[json.loads(x) for x in (p/'statements.jsonl').read_text().splitlines()];h=json.loads((p/'shared-history.json').read_text());qs={q['query_id']:q for q in h['queries']};assert h['require_final'] and not h['missing_ids'] and len(qs)==len(rs)
  for r in rs:
   q=qs[r['statement_id']];assert q['is_final'] and q['status']=='FINISHED' and q['query_text']==r['sql'] and r['response']['status']['state']=='SUCCEEDED';assert r['sql'].startswith(('DESCRIBE ','SELECT count(*) FROM '))
  a={'state':'Terminal read-only observation audit passes','statements':len(rs),'costs':{k:sum(q['metrics'].get(k,0) for q in qs.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']},'files':{n:hashlib.sha256((p/n).read_bytes()).hexdigest() for n in ['statements.jsonl','shared-history.json']}}
  assert a['costs']['write_remote_bytes']==a['costs']['spill_to_disk_bytes']==0
  if (p/'summary.json').exists():
   s=json.loads((p/'summary.json').read_text());assert a['costs']==s['costs'];a['observed_state']=s['state'];a['files']['summary.json']=hashlib.sha256((p/'summary.json').read_bytes()).hexdigest()
  if name.endswith('r389'):
   assert len(rs)==31 and s['state'].startswith('Nine exact mutable/input heads');assert len(s['tables'])==10
   for role,t in s['tables'].items():
    assert t['detail']['id']==t['pin']['id'];assert int(t['head']['version'])==(8 if role=='base-object_current' else t['pin']['version'])
   a['qualification']='Read-only node6 remains pinned/count8M; known maintenance head8 not substituted. Mutable/input heads exact. Explicit edge/adjacency CDF and append-role rowTracking profiles observed, not a new CDF execution qualification. Repeat checks inside future integrated clock; no worker fence.'
  if (p/'live-statement.json').exists():(p/'live-statement.json').rename(p/'completed-last-request.json')
  (p/'audited-summary.json').write_text(json.dumps(a,indent=2)+'\n');print(name,a['statements'],a['costs'])
if __name__=='__main__':main()
