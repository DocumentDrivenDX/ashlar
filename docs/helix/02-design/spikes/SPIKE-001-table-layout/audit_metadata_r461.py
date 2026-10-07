"""Same-handle history recovery and offline metadata result audit."""
import json,hashlib,time
from pathlib import Path
from databricks.sdk import WorkspaceClient
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent;w=WorkspaceClient(profile='aidev-cus');reports=[]
for rev in [458,460]:
 d=B/f'out/native/ashlar_metadata_custody_r{rev}';s=json.loads((d/'summary.json').read_text());records=[json.loads(x) for p in d.glob('*/statements.jsonl') for x in p.read_text().splitlines()]
 for i in range(20):
  try:h=collect_history(w,records,d/'shared-history.json');break
  except HistoryPending:
   if i==19:raise
   time.sleep(1)
 for p in d.glob('*/statements.jsonl'):
  for x in p.read_text().splitlines():
   r=json.loads(x);q=h[r['statement_id']];assert q['query_text']=='/* ashlar '+p.parent.name+' '+r['label']+' */ '+r['sql'];assert q['is_final'] and q['status']=='FINISHED'
 costs={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};assert costs['write_remote_bytes']==costs['spill_to_disk_bytes']==0
 if rev==460:
  assert len(records)==65 and s['state']=='All10table complete custody results equal across phases' and s['costs']==costs and s['phases']['sequential']['tables']==s['phases']['concurrent']['tables']
 else:
  assert len(records)==4;s.update(terminal_disposition='All4native statements terminal; driver metadata map ordering caused local assertion, no table change',costs=costs);(d/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n')
 for p in d.glob('*/inflight-request.json'):
  m=json.loads(p.read_text());assert m['query_id'] in h;p.rename(p.with_name('completed-last-request.json'))
 reports.append({'revision':rev,'statements':len(records),'costs':costs,'source_sha256':hashlib.sha256((d/'summary.json').read_bytes()).hexdigest()})
(B/'out/metadata-audit-r461.json').write_text(json.dumps({'state':'Both stopped/corrected native receipts and custody comparison audited','reports':reports},indent=2)+'\n');print(reports)
