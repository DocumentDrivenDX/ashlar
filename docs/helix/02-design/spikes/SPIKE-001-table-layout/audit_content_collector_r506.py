"""Offline native extracted-collector evidence and independent control audit."""
import hashlib,json
from pathlib import Path
B=Path(__file__).resolve().parent

def main():
 d=B/'out/native/content_collector_native_r505';s=json.loads((d/'summary.json').read_text());assert s['state']=='Extracted collector accepts all four complete native immutable content checks';p=B/'out/native/ashlar_fifth_guard_publish_r481/statements.jsonl';assert s['source_sha256']==hashlib.sha256(p.read_bytes()).hexdigest();prior={r['label']:r for r in map(json.loads,p.read_text().splitlines())};h={};costs={k:0 for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']}
 for w in sorted(d.glob('content_r505_worker*')):
  rs=[json.loads(l) for l in (w/'statements.jsonl').read_text().splitlines()];qs={q['query_id']:q for q in json.loads((w/'shared-history.json').read_text())['queries']};assert len(rs)==len(qs)==3
  for r in rs:
   q=qs[r['statement_id']];assert q['is_final'] and q['status']=='FINISHED' and r['response']['status']['state']=='SUCCEEDED';assert q['query_text']=='/* ashlar '+w.name+' '+r['label']+' */ '+r['sql']
   for k in costs:costs[k]+=q['metrics'].get(k,0) or 0
   if r['label'] not in ['timeout','cache']:
    assert r['sql']==prior[r['label']]['sql'] and sorted(r['response']['result']['data_array'])==sorted(prior[r['label']]['response']['result']['data_array']);assert not q['metrics'].get('result_from_cache')
  marker=json.loads((w/'completed-last-request.json').read_text());assert marker['query_id'] in qs;h.update(qs)
 assert len(h)==12 and len(s['accepted'])==4 and len({v['statement_id'] for v in s['accepted'].values()})==4
 for label,v in s['accepted'].items():assert sorted(v['result'])==sorted(prior[label]['response']['result']['data_array']) and v['metrics']==h[v['statement_id']]['metrics']
 controls=json.loads((B/'out/content-controls-r504.json').read_text());assert len(controls['controls'])==9 and controls['state'].startswith('Complete cohort accepted')
 for info in [s,controls]:
  for n,v in info['code_sha256'].items():assert hashlib.sha256((B/n).read_bytes()).hexdigest()==v
 assert costs==s['costs'] and all(v<=s['bounds'][k] for k,v in costs.items()) and s['wall_s']<=s['bounds']['wall_s']
 result={'state':'Extracted collector native complete-content cohort/exact SQL/final costs and nine local fail-closed controls pass','native_final_statements':12,'complete_checks':4,'cohort_wall_s':s['cohort_wall_s'],'setup_s':s['setup_s'],'wall_s':s['wall_s'],'costs':costs,'controls':controls['controls'],'qualification':'Private read-only component only; caller still owns immutable version/UUID custody, terminal history, closing checks and fencing. No partial acceptance or new publication/source ACK; no freshness/billion admission.'};(B/'out/content-collector-audit-r506.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
if __name__=='__main__':main()
