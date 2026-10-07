"""Audit matched private full-field mutations and the exact stopped clone handle."""
import hashlib,json
from pathlib import Path
from inline_guard_sql_r421 import guard_merge_relation
B=Path(__file__).resolve().parent;OLD=B/'out/native/file_target_merge_r577';O=B/'out/native/file_target_merge_resume_r578';a=json.loads((O/'summary.json').read_text());assert a['state']=='Recovered matched64/256 full-guard mutations preserve all21fields/CDF and complete typed uniqueness';assert hashlib.sha256((OLD/'summary.json').read_bytes()).hexdigest()==a['stopped_source_sha256']
for k,n in a['sources'].items():assert hashlib.sha256((B/n).read_bytes()).hexdigest()==a['source_sha256'][k]
for n,v in a['code_sha256'].items():assert hashlib.sha256((B/n).read_bytes()).hexdigest()==v
r=[dict(json.loads(x),run=d.name) for d in [OLD,O] for x in (d/'statements.jsonl').read_text().splitlines()];h={x['query_id']:x for x in json.loads((O/'combined-history.json').read_text())['queries']};assert len(r)==len(h)==len({x['statement_id'] for x in r})==38
failed=[];cached=[]
for x in r:
 q=h[x['statement_id']];assert q['is_final'] and q['query_text']==x['sql'];bad=x['label']=='clone-file256';assert q['status']==('FAILED' if bad else 'FINISHED') and x['response']['status']['state']==('FAILED' if bad else 'SUCCEEDED')
 if bad:assert 'CANNOT_SHALLOW_CLONE_NESTED' in json.dumps(x['response']['status']);failed.append(x['statement_id'])
 if q['metrics'].get('result_from_cache'):cached.append({'label':x['label'],'run':x['run'],'statement_id':x['statement_id']})
assert len(failed)==1
by={x['label']:x for x in r if x['run']==O.name};assert by['complete-initial-pair']['response']['result']['data_array']==a['complete-initial-pair']==[['2498646','0']]
merges={}
for family,f in a['fixtures'].items():
 sid=f['merge_statement_id'];rec=next(x for x in r if x['statement_id']==sid);assert rec['sql']==f['merge_sql']==guard_merge_relation(f['table'],a['source_relation']);assert rec['wall_ms']==f['merge_caller_ms'] and h[sid]['metrics']==f['merge_metrics'] and not f['merge_metrics'].get('result_from_cache')
 assert f['after_version']==f['baseline_version']+1 and f['before']['id']==f['after']['id']==f['id']
 assert int(f['history'][0]['version'])==f['after_version'] and f['history'][0]['queryHistoryStatementId']==sid and f['history'][1:]==f['initial_history']
 for prefix,expected in [('complete-cdf-',[['11836','0']]),('complete-result-',[['2498028','0']]),('unique-counts-',[['2498028','2498028']])]:assert by[prefix+family]['response']['result']['data_array']==expected
 assert by['closing-history-'+family]['response']['result']['data_array']==by['post-history-'+family]['response']['result']['data_array']
 assert json.loads(f['before']['properties'])['delta.targetFileSize']==str(f['target_bytes']) and json.loads(f['before']['properties'])['delta.enableChangeDataFeed']=='true'
 merges[family]={'table':f['table'],'id':f['id'],'baseline_version':f['baseline_version'],'after_version':f['after_version'],'before_files':f['before']['numFiles'],'after_files':f['after']['numFiles'],'caller_ms':f['merge_caller_ms'],'metrics':f['merge_metrics']}
cost={k:sum(x['metrics'].get(k,0) or 0 for x in h.values()) for k in a['costs']};assert cost==a['costs'] and all(v<=a['bounds'][k] for k,v in cost.items()) and a['wall_s']<=300
result={'state':'All38 native statements including one exact nested-clone refusal and both full-guard mutations verified','merges':merges,'costs':cost,'resume_wall_s':a['wall_s'],'cached_immutable_preflight':cached,'whole_uninterrupted_clock':None,'qualification':a['qualification']+' Original clone attempt stopped; resume clock is separate. Existing private larger fixture gains CDF3 and mutation4; older0/2 pins retained. Full21field/CDF parity inherits independently qualified original reference. No new source UUID custody or production fence claim.'};(B/'out/file-target-merge-audit-r579.json').write_text(json.dumps(result,indent=2)+'\n')
marker=OLD/'live-statement.json'
if marker.exists():assert json.loads(marker.read_text())['statement_id']==failed[0];marker.rename(OLD/'completed-failed-clone-r579.json')
print(json.dumps({'state':result['state'],'costs':cost,'resume_wall_s':a['wall_s'],'cached':cached,'merges':merges},indent=2))
