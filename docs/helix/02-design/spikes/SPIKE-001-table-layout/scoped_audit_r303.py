"""Offline whole-carrier maintenance receipt audit; actual multi-commit history."""
import json,hashlib
from pathlib import Path
B=Path(__file__).resolve().parent

def main():
 p=B/'out/native/ashlar_scoped_maintenance_r300';a=json.loads((p/'summary.json').read_text());assert a['state']=='Scoped maintenance commits with all39.99M complete carriers preserved' and a['before_version']==2 and a['after_version']==4
 r=[json.loads(x) for x in (p/'statements.jsonl').read_text().splitlines()];history=json.loads((p/'shared-history.json').read_text());h={q['query_id']:q for q in history['queries']};assert history['require_final'] and not history['missing_ids'] and len(h)==len(r)==len({x['statement_id'] for x in r})
 for x in r:
  q=h[x['statement_id']];assert q['status']=='FINISHED' and q['is_final'] and q['query_text']==x['sql'];assert x['response']['status']['state']=='SUCCEEDED' and not x['response'].get('manifest',{}).get('truncated')
 def result(label):
  x=[x for x in r if x['label']==label];assert len(x)==1;return x[0]['response'].get('result',{}).get('data_array',[])
 before=sum((result('before-'+str(i)) for i in range(0,400,100)),[]);after=sum((result('after-'+str(i)) for i in range(0,400,100)),[]);assert before==after==a['checks']['before']==a['checks']['after'];assert [int(x[0]) for x in before]==list(range(400)) and sum(int(x[1]) for x in before)==39990000;assert result('final-count')==[['39990000']]
 assert [int(x['version']) for x in a['commits']]==[4,3] and all(x['operation']=='OPTIMIZE' and x['queryHistoryStatementId']==a['optimize_statement_id'] for x in a['commits']);rewrite=json.loads(a['commits'][1]['operationMetrics']);empty=json.loads(a['commits'][0]['operationMetrics']);assert rewrite['numRemovedFiles']=='55' and rewrite['numAddedFiles']=='40';assert empty['numRemovedFiles']==empty['numAddedFiles']=='0'
 assert a['uuid']==a['after_detail']['id']=='780781cb-9c42-434b-857c-dcddc663e735';assert a['costs']=={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in a['costs']};assert a['costs']['read_bytes']<=320000000000 and a['costs']['write_remote_bytes']<=5000000000 and a['costs']['spill_to_disk_bytes']<=5000000000 and a['wall_s']<1200
 digest=[x for x in r if x['label'].startswith(('before-','after-'))];assert len(digest)==8 and all(not h[x['statement_id']]['metrics'].get('result_from_cache') for x in digest)
 a['audit']={'source_sha256':{n:hashlib.sha256((p/n).read_bytes()).hexdigest() for n in ['summary.json','stopped-summary.json','statements.jsonl','shared-history.json']},'audit_script_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'final_statements':len(r),'uncached_full_carrier_digest_queries':8,'rewrite_metrics':rewrite,'zero_file_commit_metrics':empty,'qualification':a['qualification']};(p/'audited-summary.json').write_text(json.dumps(a,indent=2)+'\n');print(a['state'],a['costs'])
if __name__=='__main__':main()
