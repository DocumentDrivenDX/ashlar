"""Offline exact-handle/schema/byte audit of stopped SDK and uncached driver runs."""
import hashlib,json,statistics
from pathlib import Path
B=Path(__file__).resolve().parent

def main():
 audits=[];summaries={}
 for tag in ['scaled_fingerprint_r493','scaled_fingerprint_reads_r494']:
  d=B/'out/native'/tag;s=json.loads((d/'summary.json').read_text());summaries[tag]=s
  records=[json.loads(l) for l in (d/'statements.jsonl').read_text().splitlines()];h={q['query_id']:q for q in json.loads((d/'shared-history.json').read_text())['queries']};assert len(records)==len(h)
  costs={k:0 for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};cached=[]
  for r in records:
   q=h[r['statement_id']];assert q['is_final'] and q['status']=='FINISHED' and r['response']['status']['state']=='SUCCEEDED'
   sql=('/* ashlar '+d.name+' '+r['label']+' */ '+r['sql']) if r.get('transport')=='sql-driver' else r['sql'];assert q['query_text']==sql
   for k in costs:costs[k]+=q['metrics'].get(k,0) or 0
   if q['metrics'].get('result_from_cache'):cached.append(r['label'])
  assert costs==s['costs'] and all(v<=s['bounds'][k] for k,v in costs.items())
  if tag.endswith('r493'):
   assert cached==['fingerprint-2'] and s['state'].startswith('Stopped')
   for n,v in s['code_sha256'].items():assert hashlib.sha256((B/n).read_bytes()).hexdigest()==v
   assert s['parent_parity']==[['2498646','0']] and s['fixture_integrity']==[['2498646','2498646','0']]
   live=d/'live-statement.json'
   if live.exists():assert json.loads(live.read_text())['statement_id'] in h;live.rename(d/'completed-last-statement.json')
  else:
   assert not cached and len(s['reads'])==6
   assert s['source_sha256']==hashlib.sha256((B/'out/native/scaled_fingerprint_r493/summary.json').read_bytes()).hexdigest()
   assert s['code_sha256']==hashlib.sha256((B/'scaled_fingerprint_reads_r494.py').read_bytes()).hexdigest()
   for r in s['reads'].values():assert r['result']==[['6227','0']] and r['metrics']==h[r['statement_id']]['metrics']
   full=(d/'plan-full.txt').read_text();finger=(d/'plan-fingerprint.txt').read_text();assert 'PhotonShuffledHashJoin' in full and 'PhotonShuffledHashJoin' in finger
   assert 'ReadSchema: struct<source_system:string,rel_type_id:bigint,id:bigint,lookup_hash:string,carrier_fingerprint:string>' in finger
  audits.append({'run':tag,'final_native_statements':len(records),'cached_labels':cached,'costs':costs,'wall_s':s['wall_s']})
 s=summaries['scaled_fingerprint_reads_r494'];families={}
 for family in ['full','fingerprint']:
  rows=[r for k,r in s['reads'].items() if k.startswith(family+'-')]
  families[family]={'observations':len(rows),'caller_ms':{'median':statistics.median(r['caller_ms'] for r in rows),'values':[r['caller_ms'] for r in rows]},'engine_ms':{'median':statistics.median(r['metrics']['execution_time_ms'] for r in rows),'values':[r['metrics']['execution_time_ms'] for r in rows]},'read_bytes_each':list({r['metrics']['read_bytes'] for r in rows}),'remote_bytes_each':list({r['metrics']['read_remote_bytes'] for r in rows}),'files_each':list({r['metrics']['read_files_count'] for r in rows})}
 result={'state':'Retained final handles, exact SQL, code bindings, full parity and six uncached read checks pass','runs':audits,'families':families,'read_reduction_fraction':1-families['fingerprint']['read_bytes_each'][0]/families['full']['read_bytes_each'][0],'engine_median_reduction_fraction':1-families['fingerprint']['engine_ms']['median']/families['full']['engine_ms']['median'],'qualification':'Read-only2.498646M private slice; not MERGE/publication timing, cold storage, p95, billion-scale, source fencing or external-reader admission. Cached third SDK observation invalid for timing and retained.'}
 (B/'out/scaled-fingerprint-audit-r495.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
if __name__=='__main__':main()
