"""Offline full carrier preservation and actual multi-commit prefix audit."""
import json,hashlib
from pathlib import Path
B=Path(__file__).resolve().parent
def main():
 p=B/'out/native/ashlar_lc_prefix_maintain_r350';a=json.loads((p/'summary.json').read_text());assert a['state']=='Bounded LC prefix maintenance preserves all39.98M complete carriers' and a['before_version']==3
 records=[json.loads(x) for x in (p/'statements.jsonl').read_text().splitlines()];h=json.loads((p/'shared-history.json').read_text());native={q['query_id']:q for q in h['queries']};assert len(records)==len(native)==len({r['statement_id'] for r in records}) and not h['missing_ids'] and h['require_final']
 for r in records:
  q=native[r['statement_id']];assert q['status']=='FINISHED' and q['is_final'] and q['query_text']==r['sql'];assert r['response']['status']['state']=='SUCCEEDED' and not r['response'].get('manifest',{}).get('truncated')
 previous=B/'out/native/ashlar_lc_incremental_maintain_r343';pr=[json.loads(x) for x in (previous/'statements.jsonl').read_text().splitlines()];pn={q['query_id']:q for q in json.loads((previous/'query-history.json').read_text())};before=[];after=[]
 for i in range(0,400,100):
  r=next(r for r in pr if r['label']=='before-all-'+str(i));q=pn[r['statement_id']];assert q['status']=='FINISHED' and q['is_final'] and q['query_text']==r['sql'] and not q['metrics'].get('result_from_cache');before.extend(r['response']['result']['data_array']);r=next(r for r in records if r['label']=='after-all-'+str(i));assert not native[r['statement_id']]['metrics'].get('result_from_cache');after.extend(r['response']['result']['data_array'])
 assert before==after==a['checks']['before']==a['checks']['after'];assert len(before)==400 and [int(x[0]) for x in before]==list(range(400)) and sum(int(x[1]) for x in before)==39980000;assert a['before_detail']['id']==a['after_detail']['id']==a['uuid']
 assert [int(x['version']) for x in a['commits']]==list(range(a['after_version'],3,-1));assert all(x['operation']=='OPTIMIZE' and x['queryHistoryStatementId']==a['optimize_statement_id'] for x in a['commits']);opt=next(r for r in records if r['label']=='optimize');assert opt['sql']=='OPTIMIZE '+a['table']+' FULL WHERE '+a['predicate'] and opt['statement_id']==a['optimize_statement_id'];assert a['scope']['overlapping_live_file_bytes']<=2500000000
 assert a['costs']=={k:sum(q['metrics'].get(k,0) for q in native.values()) for k in a['costs']};assert all(v<=a['bounds'][k] for k,v in a['costs'].items()) and a['wall_s']<a['bounds']['wall_s'];prior=json.loads((B/'out/lc-maintenance-canceled-disposition-r349.json').read_text())['all_attempt_costs'];assert prior==a['prior_canceled_read']['all_attempt_costs'];assert a['combined_attempt_costs']=={k:v+prior[k] for k,v in a['costs'].items()} and a['combined_attempt_costs']['read_bytes']<=500000000000
 a['audit']={'final_statements':len(records),'complete_carrier_rows':39980000,'uncached_full_carrier_groups':400,'source_sha256':{n:hashlib.sha256((p/n).read_bytes()).hexdigest() for n in ['summary.json','statements.jsonl','shared-history.json']},'qualification':a['qualification']};(p/'audited-summary.json').write_text(json.dumps(a,indent=2)+'\n');print(json.dumps({'state':a['state'],'audit':a['audit']},indent=2))
if __name__=='__main__':main()
