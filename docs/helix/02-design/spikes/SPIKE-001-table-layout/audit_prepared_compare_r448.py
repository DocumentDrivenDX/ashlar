"""Offline terminal receipt, exact SQL, source binding and cost audit."""
import json,hashlib
from pathlib import Path
B=Path(__file__).resolve().parent
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def main():
 p=B/'out/native/ashlar_prepared_mutation_r445';c=B/'out/native/ashlar_prepared_join_compare_r447';ps=json.loads((p/'summary.json').read_text());cs=json.loads((c/'summary.json').read_text());assert cs['prepared_source_sha256']==sha(p/'summary.json')
 for name,h in ps['code_sha256'].items():assert sha(B/name)==h
 assert len(ps['fields'])==42 and ps['table']['version']==0
 assert ps['complete_digest_result']==[['100000','100000','100000','10000','90000',ps['oracle_digest']]]
 audits=[]
 for d,s,n in [(p,ps,10),(c,cs,2)]:
  records=[json.loads(x) for x in (d/'statements.jsonl').read_text().splitlines()];hist={x['query_id']:x for x in json.loads((d/'shared-history.json').read_text())['queries']};assert len(records)==n
  costs={k:0 for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']}
  for r in records:
   q=hist[r['statement_id']];assert q['is_final'] and q['status']=='FINISHED' and q['query_text']==r['sql'];assert r['response']['status']['state']=='SUCCEEDED'
   for k in costs:costs[k]+=q['metrics'].get(k,0)
   if d==c:assert r['sql'].startswith('/* ashlar prepared_join_compare_r447 ') and not q['metrics'].get('result_from_cache') and r['response']['result']['data_array']==[['100000','0',ps['oracle_digest']]]
  assert costs==s['costs'] and all(v<=s['bounds'][k] for k,v in costs.items())
  audits.append({'directory':d.name,'native_final_statements':n,'costs':costs})
  live=d/'live-statement.json'
  if live.exists():
   marker=json.loads(live.read_text());assert marker['statement_id'] in hist;live.rename(d/'completed-last-statement.json')
 wide=(p/'prepared-wide-plan.txt').read_text();assert 'PhotonShuffledHashJoin' in wide and 'from_json' not in wide
 result={'state':'Offline terminal receipts, exact SQL, complete42field digest bindings and costs pass','audits':audits,'prepared_source_sha256':sha(p/'summary.json'),'comparison_source_sha256':sha(c/'summary.json'),'qualification':'Checks retained native receipts; independent generated source oracle belongs to r445. Initial plans are not MERGE runtime evidence; fixed-order pair is not causal.'}
 (B/'out/prepared-compare-audit-r448.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
if __name__=='__main__':main()
