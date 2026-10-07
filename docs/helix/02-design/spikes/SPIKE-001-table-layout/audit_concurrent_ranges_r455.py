"""Audit both cached rejected and uncached corrected terminal range cohorts."""
import json,hashlib
from pathlib import Path
B=Path(__file__).resolve().parent
reports=[]
for rev,filename in [(452,'audited-summary.json'),(454,'summary.json')]:
 d=B/f'out/native/ashlar_concurrent_ranges_r{rev}';s=json.loads((d/filename).read_text());hist={q['query_id']:q for q in json.loads((d/'shared-history.json').read_text())['queries']};records=[json.loads(line) for p in d.glob('*/statements.jsonl') for line in p.read_text().splitlines()];assert len(records)==(8 if rev==452 else 16)
 costs={k:0 for k in s['costs']}
 for r in records:
  q=hist[r['statement_id']];expected=r['sql'] if rev==452 else '/* ashlar '+next(p.parent.name for p in d.glob('*/statements.jsonl') if any(json.loads(x)['statement_id']==r['statement_id'] for x in p.read_text().splitlines()))+' '+r['label']+' */ '+r['sql'];assert q['query_text']==expected and q['status']=='FINISHED' and q['is_final'] and r['response']['status']['state']=='SUCCEEDED'
  for k in costs:costs[k]+=q['metrics'].get(k,0)
 assert costs==s['costs'] and costs['write_remote_bytes']==costs['spill_to_disk_bytes']==0
 for phase in s['phases'].values():
  for r in phase['reads']:
   assert r['result']==[[str(s['expected_counts'][r['range']]),'0']]
   if rev==454:assert not hist[r['statement_id']]['metrics'].get('result_from_cache')
 reports.append({'revision':rev,'final_statements':len(records),'costs':costs,'cached_reads':sum(hist[r['statement_id']]['metrics'].get('result_from_cache',False) for p in s['phases'].values() for r in p['reads']),'summary_sha256':hashlib.sha256((d/filename).read_bytes()).hexdigest()})
(B/'out/concurrent-ranges-audit-r455.json').write_text(json.dumps({'state':'Both terminal cohorts exact SQL/results/cache status/costs audited','reports':reports},indent=2)+'\n');print(reports)
