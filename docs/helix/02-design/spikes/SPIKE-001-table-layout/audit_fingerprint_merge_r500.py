"""Offline complete-receipt/native-cost/correctness audit; no network or writes on import."""
import hashlib,json
from pathlib import Path
B=Path(__file__).resolve().parent

def main():
 audits=[];summaries={}
 for name in ['scaled_fingerprint_merge_r497','full_guard_clone_compare_r498','fingerprint_semantic_controls_r499']:
  d=B/'out/native'/name;s=json.loads((d/'summary.json').read_text());summaries[name]=s;rs=[json.loads(l) for l in (d/'statements.jsonl').read_text().splitlines()];raw=json.loads((d/('shared-history.json' if name.endswith('r498') else 'query-history.json')).read_text());h={q['query_id']:q for q in (raw['queries'] if isinstance(raw,dict) else raw)};assert len(rs)==len(h)
  costs={k:0 for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};failed=[];cached=[]
  for r in rs:
   q=h[r['statement_id']];state=r['response']['status']['state'];assert state in ['SUCCEEDED','FAILED'];assert q['is_final'] and q['query_text']==r['sql'] and q['status']==('FINISHED' if state=='SUCCEEDED' else 'FAILED')
   for k in costs:costs[k]+=q['metrics'].get(k,0) or 0
   if state=='FAILED':failed.append(r['label']);assert 'ASHLAR_FINGERPRINT_' in json.dumps(r['response']['status'])
   if q['metrics'].get('result_from_cache'):cached.append(r['label'])
  assert costs==s['costs'] and all(v<=s['bounds'][k] for k,v in costs.items()) and s['wall_s']<=s['bounds']['wall_s']
  for n,v in s['code_sha256'].items():assert hashlib.sha256((B/n).read_bytes()).hexdigest()==v
  if name.endswith('r497'):
   assert failed==['changed_property_token','missing_predecessor'];assert s['baseline_version']==2 and s['after_version']==3;assert s['source_counts']==[['6227','6227','618','5609']];assert s['full_cdf_parity']==[['11836','0','618','5609','5609','0','0']] and s['result_integrity']==[['2498028','2498028','0']]
   for n,v in s['source_sha256'].items():assert hashlib.sha256((B/n).read_bytes()).hexdigest()==v
   assert s['merge_metrics']==h[s['merge_statement_id']]['metrics'] and not s['merge_metrics']['result_from_cache']
  elif name.endswith('r498'):
   assert not failed;assert s['source_sha256']==hashlib.sha256((B/'out/native/scaled_fingerprint_merge_r497/summary.json').read_bytes()).hexdigest();assert int(s['clone_result'][0]['num_copied_files'])==0 and int(s['clone_result'][0]['copied_files_size'])==0;assert s['complete-before-parity']==[['2498646','0']] and s['complete-cdf-parity']==[['11836','0']] and s['complete-result-parity']==[['2498028','0']];assert s['merge_metrics']==h[s['merge_statement_id']]['metrics'] and not s['merge_metrics']['result_from_cache']
  else:assert failed==['nonadvancing_version','changed_after_identity'] and len(s['controls'])==2
  marker=json.loads((d/'completed-last-statement.json').read_text());assert marker['statement_id'] in h
  audits.append({'run':name,'final_native_statements':len(rs),'expected_failed':failed,'cached_labels':cached,'costs':costs,'wall_s':s['wall_s']})
 g=summaries['scaled_fingerprint_merge_r497'];f=summaries['full_guard_clone_compare_r498'];result={'state':'Exact SQL/terminal receipts/code bindings/private MERGE preservation/comparator/semantic refusals pass','runs':audits,'merge':{'fingerprint':{'read_bytes':g['merge_metrics']['read_bytes'],'engine_ms':g['merge_metrics']['execution_time_ms'],'caller_ms':f['fingerprint_merge_caller_ms'],'write_remote_bytes':g['merge_metrics']['write_remote_bytes']},'full':{'read_bytes':f['merge_metrics']['read_bytes'],'engine_ms':f['merge_metrics']['execution_time_ms'],'caller_ms':f['merge_caller_ms'],'write_remote_bytes':f['merge_metrics']['write_remote_bytes']}},'relative_read_difference':abs(g['merge_metrics']['read_bytes']-f['merge_metrics']['read_bytes'])/f['merge_metrics']['read_bytes'],'decision':'Do not promote generated fingerprint as default MERGE I/O optimization or backfill40M canonical table. Read-only projection benefit did not transfer to actual MERGE reported reads. One pair cannot establish causal CPU/latency improvement.','qualification':'Private2.5M slice/6227 mutations only; not full multi-role publication, freshness p95, cold lookup, fencing, source admission, external-reader or billion-scale evidence.'};(B/'out/fingerprint-merge-audit-r500.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
if __name__=='__main__':main()
