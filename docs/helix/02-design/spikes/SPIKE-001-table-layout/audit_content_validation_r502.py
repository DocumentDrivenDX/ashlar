"""Offline exact-content/custody/terminal-cost scheduler comparison audit."""
import hashlib,json
from pathlib import Path
B=Path(__file__).resolve().parent

def main():
 d=B/'out/native/content_validation_compare_r501';s=json.loads((d/'summary.json').read_text());assert s['state']=='All12 uncached complete-content checks match fifth publication results';p=B/'out/native/ashlar_fifth_guard_publish_r481/statements.jsonl';assert s['source_sha256']==hashlib.sha256(p.read_bytes()).hexdigest();assert s['code_sha256']==hashlib.sha256((B/'content_validation_compare_r501.py').read_bytes()).hexdigest();prior={r['label']:r for r in map(json.loads,p.read_text().splitlines())};allh={};costs={k:0 for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};counts=[]
 for i,label in enumerate(s['labels']):
  w=d/('worker'+str(i));rs=[json.loads(l) for l in (w/'statements.jsonl').read_text().splitlines()];h={q['query_id']:q for q in json.loads((w/'shared-history.json').read_text())['queries']};assert len(rs)==len(h)==5
  for r in rs:
   q=h[r['statement_id']];assert q['is_final'] and q['status']=='FINISHED' and r['response']['status']['state']=='SUCCEEDED';assert q['query_text']=='/* ashlar '+w.name+' '+r['label']+' */ '+r['sql']
   for k in costs:costs[k]+=q['metrics'].get(k,0) or 0
   if r['label']=='cache':assert r['response']['result']['data_array'][0][-1].lower()=='false'
   if r['label'] not in ['timeout','cache']:
    assert r['sql']==prior[label]['sql'] and sorted(r['response']['result']['data_array'])==sorted(prior[label]['response']['result']['data_array']);assert not q['metrics'].get('result_from_cache')
  marker=json.loads((w/'completed-last-request.json').read_text());assert marker['query_id'] in h;allh.update(h);counts.append(len(rs))
 assert len(allh)==20 and costs==s['costs'] and all(v<=s['bounds'][k] for k,v in costs.items()) and s['wall_s']<=s['bounds']['wall_s']
 assert [p['phase'] for p in s['phases']]==['serial-before','concurrent','serial-after'];phases=[];enrichment=[]
 for phase in s['phases']:
  assert {r['label'] for r in phase['results']}==set(s['labels']) and len(phase['results'])==4
  for r in phase['results']:
   latest=allh[r['statement_id']]['metrics'];old=r['metrics'];diff={k for k in set(latest)|set(old) if latest.get(k)!=old.get(k)}
   assert not diff or (diff=={'result_fetch_time_ms'} and 'result_fetch_time_ms' not in old and isinstance(latest['result_fetch_time_ms'],int) and latest['result_fetch_time_ms']>=0)
   if diff:enrichment.append({'phase':phase['phase'],'label':r['label'],'field':'result_fetch_time_ms','earlier':'absent despite is_final','later':latest['result_fetch_time_ms'],'qualification':'Added after cursor closure; not a caller fetch-duration measurement or added engine cost.'})
   assert sorted(r['result'])==s['expected'][r['label']]
  phases.append({'phase':phase['phase'],'wall_s':phase['wall_s'],'caller_sum_s':sum(r['caller_ms'] for r in phase['results'])/1000,'engine_sum_s':sum(r['metrics']['execution_time_ms'] for r in phase['results'])/1000,'read_bytes':sum(r['metrics']['read_bytes'] for r in phase['results']),'remote_read_bytes':sum(r['metrics'].get('read_remote_bytes',0) for r in phase['results'])})
 shape=json.loads((d/'warehouse-observation.json').read_text());assert shape['cluster_size']=='2X-Small' and shape['min_num_clusters']==shape['max_num_clusters']==1
 result={'state':'Twelve exact-content uncached checks/native SQL/final costs pass across20 statement handles','phases':phases,'late_history_enrichment':enrichment,'costs':costs,'wall_s':s['wall_s'],'setup_s':s['setup_s'],'warehouse':shape,'concurrent_reduction_vs_serial_before':1-phases[1]['wall_s']/phases[0]['wall_s'],'concurrent_reduction_vs_serial_after':1-phases[1]['wall_s']/phases[2]['wall_s'],'qualification':'Fixed serial/concurrent/serial order and shared compute/storage cache; component comparison only, no new publication or60s/10k/s admission. Do not sum parallel callers/engines as cohort wall.'};(B/'out/content-validation-audit-r502.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
if __name__=='__main__':main()
