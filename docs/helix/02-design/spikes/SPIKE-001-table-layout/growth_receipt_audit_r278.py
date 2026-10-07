"""Offline terminal receipt audit for completed split-growth runs."""
import json,hashlib,sys
from pathlib import Path
from mixed_grouped_verification_r249 import expected_mixed
B=Path(__file__).resolve().parent

def audit(directory):
 p=Path(directory);a=json.loads((p/'summary.json').read_text());end=a['edge_slice'][1]
 assert a['state']==f'Complete8M-node/{end//1000000}M-edge staging passes every role field and typed endpoint closure'
 records=[json.loads(s) for s in (p/'statements.jsonl').read_text().splitlines()];h=json.loads((p/'shared-history.json').read_text());assert h['require_final'] and not h['missing_ids'];history={q['query_id']:q for q in h['queries']};assert len(history)==len(records)
 for r in records:
  q=history[r['statement_id']];assert r['response']['status']['state']=='SUCCEEDED' and q['status']=='FINISHED' and q['is_final'];assert not r['response'].get('manifest',{}).get('truncated');assert q['query_text']==r['sql']
 costs={k:sum(q['metrics'].get(k,0) for q in history.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};assert costs==a['costs']
 for k,bound in [('read_bytes','read_bytes'),('write_remote_bytes','write_bytes'),('spill_to_disk_bytes','spill_bytes')]:assert costs[k]<=a['bounds'][bound]
 assert a['wall_s']<a['bounds']['wall_s']
 nodes=json.loads((B/'out/native/ashlar_scale_growth_r248/audited-summary.json').read_text())['oracle'];edges=a['edge_oracle'];assert len(nodes)==80 and len(edges)==end//100000
 for role in a['tables']:
  chunks=nodes if role=='object_current' else edges
  expected=expected_mixed(nodes,edges,role) if role in ['source_record','property_journal'] else [[str(i),str(x['roles'][role]['rows']),x['roles'][role]['digest']] for i,x in enumerate(chunks)]
  label=('prior-' if role=='object_current' else 'complete-')+role;check=a['checks'][label];assert check['groups']==expected and check['rows']==sum(int(r[1]) for r in expected)
  rows=[]
  for r in records:
   if (r['label']==label or r['label'].startswith(label+'-')) and not r['label'].endswith('-count'):rows+=r['response'].get('result',{}).get('data_array',[])
  assert rows==expected,role
  assert a['tables'][role]['version']==check['version'] and a['tables'][role]['rows']==check['rows'];assert a['active_details'][role]['id']==a['tables'][role]['id']
 selected=[r for r in records if r['label'] in ['edge-identities','typed-endpoint-closure']]
 assert len(selected)==2
 for r in selected:
  expected=[[str(end),str(end),'8000001',str(8000000+end)]] if r['label']=='edge-identities' else [[str(end),'0']]
  assert r['response']['result']['data_array']==expected
  assert not history[r['statement_id']]['metrics'].get('result_from_cache')
 for r in records:
  if r['label'].startswith('complete-'):assert not history[r['statement_id']]['metrics'].get('result_from_cache')
 assert len(a['commit_events'])==5 and len(a['edge_chunk_receipts'])==2
 bysid={r['statement_id']:r for r in records}
 for e in a['commit_events']:
  r=bysid[e['statement_id']];assert r['label'].startswith('append-');assert e['history']['queryHistoryStatementId']==e['statement_id'] and int(e['history']['version'])==e['version']
 a['audit']={'source_sha256':{n:hashlib.sha256((p/n).read_bytes()).hexdigest() for n in ['summary.json','shared-history.json','statements.jsonl']},'audit_script_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'final_statements':len(records),'cached_prior_statements':sum(bool(history[r['statement_id']]['metrics'].get('result_from_cache')) for r in records),'complete_expanded_roles_uncached':True,'active_bytes':sum(int(d['sizeInBytes']) for d in a['active_details'].values()),'active_files':sum(int(d['numFiles']) for d in a['active_details'].values()),'append_telemetry':{r['label']:{'caller_ms':r['wall_ms'],'metrics':history[r['statement_id']]['metrics']} for r in records if r['label'].startswith('append-')},'sql_channels':sorted({json.dumps(q.get('channel_used'),sort_keys=True) for q in history.values()}),'qualification':'All terminal client/native receipts and complete role groups/identities/endpoints checked. Prior cached checks not fresh-scan latency evidence; sequential active metadata not retained or atomic inventory. No publisher/freshness, complete canonical DDL, caller/cold/concurrency or billion admission.'}
 (p/'audited-summary.json').write_text(json.dumps(a,indent=2)+'\n');print(json.dumps({'state':a['state'],'audit':{k:a['audit'][k] for k in ['final_statements','active_bytes','active_files']}},indent=2))
if __name__=='__main__':audit(sys.argv[1])
