"""Offline native-final audit for the second range32 MERGE."""
import hashlib,json
from pathlib import Path
B=Path(__file__).resolve().parent
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def main():
 o=B/'out/native/ashlar_lc_second_apply_r337';p=o/'summary.json';a=json.loads(p.read_text());assert a['state']=='Liquid-clustered second-batch MERGE preserves complete190k edge images'
 records=[json.loads(x) for x in (o/'statements.jsonl').read_text().splitlines()];h=json.loads((o/'shared-history.json').read_text());queries={q['query_id']:q for q in h['queries']};assert len(records)==len({r['statement_id'] for r in records})
 totals={k:0 for k in a['costs']}
 for r in records:
  q=queries[r['statement_id']];assert q['status'] in ['FINISHED','SUCCEEDED'] and q['is_final'];assert q['query_text']==r['sql'];assert r['response']['status']['state']=='SUCCEEDED'
  for k in totals:totals[k]+=q['metrics'].get(k,0)
  if r['label'].startswith('all-images-'):assert not q['metrics'].get('result_from_cache')
 assert totals==a['costs'];assert all(v<=a['bounds'][k] for k,v in totals.items())
 t=a['tables']['edge_current'];assert t['clone_version']==0 and t['cdf_enable_version']==1 and t['first_version']==2 and t['version']==3
 assert [int(x['history']['version']) for x in a['commits']]==[0,1,2,3]
 oracle=json.loads((B/'out/second-cdf-image-oracle-r327.json').read_text())['roles']['edge_current'];want=[[kind,str(x['rows']),'3','3',x['digest']] for kind,x in sorted(oracle['images'].items())];assert a['checks']['edge_current']['groups']==want
 merge=next(r for r in records if r['label']=='merge-edge_current');assert 'b.lookup_partition' not in merge['sql'] and "b.apply_batch_id='mixed-bootstrap'" in merge['sql']
 first=json.loads((B/'out/cdf-image-oracle-r288.json').read_text())['roles']['edge_current'];want_first=[[kind,str(x['rows']),'2','2',x['digest']] for kind,x in sorted(first['images'].items())];assert a['checks']['first']['groups']==want_first
 for label,want_result in [('prepare-first-images',want_first),('prepared-count',[['39990000']]),('second-predecessors',[['100000','0']])]:
  r=next(r for r in records if r['label']==label);assert r['response']['result']['data_array']==want_result and not queries[r['statement_id']]['metrics'].get('result_from_cache')
 a['audit']={'final_statements':len(records),'complete20field_images':190000,'source_sha256':{n:sha(o/n) for n in ['summary.json','statements.jsonl','shared-history.json']},'partition_predicate_present':False,'qualification':a['qualification']}
 (o/'audited-summary.json').write_text(json.dumps(a,indent=2)+'\n');print(json.dumps(a['audit'],indent=2))
if __name__=='__main__':main()
