"""Offline native-final supplemental baseline receipt audit."""
from pathlib import Path
import json,hashlib
B=Path(__file__).resolve().parent
def main():
 o=B/'out/native/ashlar_second_baseline_custody_r334';a=json.loads((o/'summary.json').read_text());assert a['state']=='All reconstructed first-batch role intervals match complete independent digests';records=[json.loads(x) for x in (o/'statements.jsonl').read_text().splitlines()];h=json.loads((o/'shared-history.json').read_text());native={q['query_id']:q for q in h['queries']};assert len(records)==len(native)==4
 for r in records:
  q=native[r['statement_id']];assert q['status']=='FINISHED' and q['is_final'] and q['query_text']==r['sql'] and not q['metrics'].get('result_from_cache');assert r['response']['status']['state']=='SUCCEEDED' and not r['response']['manifest'].get('truncated');role=r['label'].removeprefix('first-custody-');assert r['response']['result']['data_array']==a['checks'][role]['expected']
 assert a['costs']=={k:sum(q['metrics'].get(k,0) for q in native.values()) for k in a['costs']};assert a['publisher_summary_sha256']==hashlib.sha256((B/'out/native/ashlar_incremental_publish_r332/summary.json').read_bytes()).hexdigest()
 a['audit']={'final_statements':4,'uncached_complete_first_intervals':True,'source_sha256':{n:hashlib.sha256((o/n).read_bytes()).hexdigest() for n in ['summary.json','statements.jsonl','shared-history.json']}};(o/'audited-summary.json').write_text(json.dumps(a,indent=2)+'\n');print(a['audit'])
if __name__=='__main__':main()
