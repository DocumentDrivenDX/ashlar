"""Independent actual-query, whole-statement rollback and complete CDF audit."""
import copy,json,hashlib
from pathlib import Path
from inline_guard_controls_r424 import relation,digest
from inline_guard_sql_r421 import guard_merge_relation
from overlay_sql_r395 import FIELDS
from mixed_change_queries_r230 import INTS
from third_changes_r378 import ThirdChanges
B=Path(__file__).resolve().parent
def main():
 folders=['ashlar_inline_guard_controls_r422','ashlar_inline_guard_recover_r423','ashlar_inline_guard_controls_r424','ashlar_inline_guard_finish_r425'];summaries={};records={};queries={};costs={k:0 for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']}
 for name in folders:
  p=B/'out/native'/name;a=json.loads((p/'summary.json').read_text());summaries[name]=a;rr=[json.loads(x) for x in (p/'statements.jsonl').read_text().splitlines()];hh=json.loads((p/'query-history.json').read_text());qs={q['query_id']:q for q in hh};assert set(qs)=={r['statement_id'] for r in rr}
  for r in rr:
   q=qs[r['statement_id']];assert q['is_final'] and q['query_text']==r['sql'] and q['status']==('FAILED' if r['response']['status']['state']=='FAILED' else 'FINISHED');assert r['statement_id'] not in records;records[r['statement_id']]=r;queries[r['statement_id']]=q
  for k in costs:costs[k]+=sum(q['metrics'].get(k,0) for q in hh)
  for n,h in a.get('code_sha256',{}).items():assert hashlib.sha256((B/n).read_bytes()).hexdigest()==h
 old=summaries[folders[0]];recover=summaries[folders[1]];corrected=summaries[folders[2]];final=summaries[folders[3]];assert len(old['controls'])==15 and old['error']=='Invalid source was accepted: mismatch-published_at';assert recover['state']=='Original valid timestamp commit1 and preserved pins0/1 audited read-only';assert final['state']=='All20field/26atomic refusals and clean5update/1delete complete CDF pass'
 for n,h in recover['source_sha256'].items():assert hashlib.sha256((B/'out/native'/folders[0]/n).read_bytes()).hexdigest()==h
 for n,h in final['source_sha256'].items():assert hashlib.sha256((B/'out/native'/folders[2]/n).read_bytes()).hexdigest()==h
 changes=[ThirdChanges().change(i) for i in [0,1,2,3,4,19]];before=digest([x['before'] for x in changes]);after=digest([x['after'] for x in changes if x['after'] is not None]);assert before==old['before_digest']==corrected['before_digest'] and after==old['after_digest']==corrected['after_digest']
 controls={}
 for f in FIELDS:
  bad=copy.deepcopy(changes);v=bad[-1]['before'][f];bad[-1]['before'][f]=str(int(v or '0')+1) if f in INTS else '2026-10-08T00:00:00Z' if f=='published_at' else ('altered' if v is None else v+'x');assert bad[-1]['before'][f]!=v;controls['mismatch-'+f]=bad
 for label,mutate in [('deletion-before-mismatch',lambda x:x[0]['before'].update(props_json='{}')),('missing-deletion',lambda x:x[0]['before'].update(id='999999999')),('missing-update',lambda x:x[-1]['before'].update(id='999999999')),('stale-after-version',lambda x:x[-1]['after'].update(entity_version='1')),('changed-after-identity',lambda x:x[-1]['after'].update(id='999999999')),('duplicate-update-source',lambda x:x.append(copy.deepcopy(x[-1])))]:
  bad=copy.deepcopy(changes);mutate(bad);controls[label]=bad
 assert len(controls)==26 and {x['label'] for x in final['controls']}==set(controls)
 rr=[r for r in records.values() if r['statement_id'] in {x['statement_id'] for x in final['controls']}];assert len(rr)==26
 for control in final['controls']:
  r=records[control['statement_id']];assert r['sql']==guard_merge_relation(final['table'],relation(controls[control['label']])) and r['response']['status']['state']=='FAILED' and any(code in json.dumps(r['response']['status']) for code in control['expected_codes'])
 # Every corrected refusal binds an explicit unchanged head0 and complete
 # cardinality/identity/full20field digest; missing-deletion uses recovery checks.
 for control in final['controls']:
  label='missing-deletion-recovery' if control['label']=='missing-deletion' else control['label'];matches=[r for r in records.values() if r['label']==label+'-digest' and r['sql'].startswith('SELECT count(*),count(DISTINCT') and final['table'] in r['sql']];heads=[r for r in records.values() if r['label']==label+'-head' and final['table'] in r['sql']];assert len(matches)==len(heads)==1 and matches[0]['response']['result']['data_array']==[['6','6',before]] and heads[0]['response']['result']['data_array'][0][0]=='0'
 expected={'delete':['1',digest([changes[0]['before']])],'update_preimage':['5',digest([x['before'] for x in changes if x['after'] is not None])],'update_postimage':['5',after]};assert final['cdf']==expected
 cdf=next(r for r in records.values() if r['label']=='complete-CDF');assert {r[0]:r[1:] for r in cdf['response']['result']['data_array']}==expected
 assert records[final['clean_statement_id']]['sql']==guard_merge_relation(final['table'],relation(changes)) and records[final['clean_statement_id']]['response']['status']['state']=='SUCCEEDED';assert len(final['history'])==2 and [x[0] for x in final['history']]==['1','0']
 # The stopped first timestamp request is genuinely a clean positive request.
 original=next(r for r in records.values() if r['label']=='mismatch-published_at' and old['table'] in r['sql']);assert original['sql']==guard_merge_relation(old['table'],relation(changes)) and original['statement_id']==recover['history'][0]['queryHistoryStatementId']
 assert costs==final['costs'] and all(v<=final['bounds'][k] for k,v in costs.items())
 p=B/'out/native'/folders[3];a={'state':'Inline full20field guards,26atomic refusals, clean CDF and all stopped costs independently audited','controls':26,'native_final_statements':len(records),'costs':costs,'table':final['table'],'code_sha256':{n:hashlib.sha256((B/n).read_bytes()).hexdigest() for n in ['inline_guard_sql_r421.py','inline_guard_audit_r426.py']},'source_sha256':{name:{n:hashlib.sha256((B/'out/native'/name/n).read_bytes()).hexdigest() for n in ['summary.json','statements.jsonl','query-history.json']} for name in folders},'qualification':'Tiny synthetic update/delete-only guard evidence, not full ingest/freshness, current insertion/idempotent replay, source authority or production writer fencing.'};(p/'audited-summary.json').write_text(json.dumps(a,indent=2)+'\n');live=json.loads((p/'completed-last-statement.json').read_text());assert live['statement_id'] in records;print(json.dumps(a,indent=2))
if __name__=='__main__':main()
