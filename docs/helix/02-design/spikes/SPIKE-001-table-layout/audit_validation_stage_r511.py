"""Offline integrated stage and stopped-trial audit, including provisional costs."""
import hashlib,json
from pathlib import Path
from publisher_validation_r507 import plan
from validation_plan_oracle_r508 import source
B=Path(__file__).resolve().parent

def main():
 pub,src,prior=source();expected={c.label:c for c in plan(pub['tables'],pub['inputs'],src)};audits=[]
 for tag in ['r509','r510']:
  d=B/'out/native'/('validation_stage_native_'+tag);s=json.loads((d/'summary.json').read_text());path=d/('recovered-history.json' if tag=='r509' else 'shared-history.json');h={q['query_id']:q for q in json.loads(path.read_text())['queries']};records=[]
  for p in d.rglob('statements.jsonl'):
   for r in map(json.loads,p.read_text().splitlines()):records.append((p.parent,r))
  assert len(records)==len(h)==(32 if tag=='r509' else 63);costs={k:0 for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};checks=[];nonfinal=[]
  for directory,r in records:
   q=h[r['statement_id']];assert q['status']=='FINISHED' and r['response']['status']['state']=='SUCCEEDED';query=('/* ashlar '+directory.name+' '+r['label']+' */ '+r['sql']) if r.get('transport')=='sql-driver' else r['sql'];assert q['query_text']==query
   if not q.get('is_final'):nonfinal.append(r['statement_id'])
   for k in costs:costs[k]+=q['metrics'].get(k,0) or 0
   if r['label'] in expected:
    e=expected[r['label']];assert r['sql']==e.sql and sorted(tuple(x) for x in r['response']['result']['data_array'])==sorted(e.expected);assert not q['metrics'].get('result_from_cache');checks.append(r['label'])
  for n,v in s['code_sha256'].items():assert hashlib.sha256((B/n).read_bytes()).hexdigest()==v
  assert s['source_sha256']==hashlib.sha256((B/'out/native/ashlar_fifth_guard_publish_r481/summary.json').read_bytes()).hexdigest()
  if tag=='r510':
   assert not nonfinal and set(checks)==set(expected) and len(checks)==15 and s['state'].startswith('Integrated15-check stage passes')
   assert s['custody_before']==s['custody_after'] and len(s['custody_before'])==10
   assert costs==s['costs'] and all(v<=s['bounds'][k] for k,v in costs.items()) and s['wall_s']<=s['bounds']['wall_s']
   assert [x['offset'] for x in s['cohorts']]==[0,4,8,12]
   for label,v in s['accepted'].items():assert v['metrics']==h[v['statement_id']]['metrics'] and sorted(v['result'])==sorted(prior[label]['response']['result']['data_array'])
  else:
   recovery=json.loads((d/'recovery.json').read_text());assert set(nonfinal)==set(recovery['pending_metric_ids']) and costs==recovery['reported_costs'];assert len(checks)==4 and s['state'].startswith('Stopped')
  for marker in d.rglob('completed-last-request.json'):assert json.loads(marker.read_text())['query_id'] in h
  assert json.loads((d/'completed-last-statement.json').read_text())['statement_id'] in h
  audits.append({'trial':tag,'execution_finished':len(records),'final_metric_records':len(h)-len(nonfinal),'pending_metric_ids':nonfinal,'complete_checks':len(checks),'reported_costs':costs,'cost_final':not nonfinal,'qualification':'r509 is stopped first cohort only and costs remain provisional; r510 is complete read-only post-commit integration, not new apply/publication.'})
 result={'state':'Corrected integrated15-check stage passes exact SQL/results, final metrics and ten-table closing custody; stopped trial preserved separately','trials':audits,'validation_with_telemetry_s':s['validation_with_telemetry_s'],'total_corrected_s':s['wall_s'],'qualification':'No new role/manifest/ACK writes or ready-input clock. Pending old metrics must be re-observed by same IDs, not silently omitted or counted as final. Published pins remainN6/E8/R5/J5/A6/T5; physicalN head8 is separately recorded.'};(B/'out/validation-stage-audit-r511.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
if __name__=='__main__':main()
