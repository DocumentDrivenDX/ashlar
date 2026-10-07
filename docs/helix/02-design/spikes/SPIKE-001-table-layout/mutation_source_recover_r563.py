"""Recover completed source probe; no query replay or mutation."""
import hashlib,json,time
from pathlib import Path
from databricks.sdk import WorkspaceClient
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent;O=B/'out/native/mutation_source_probe_r562';a=json.loads((O/'summary.json').read_text());r=[json.loads(x) for x in (O/'statements.jsonl').read_text().splitlines()];assert len(r)==4 and r[-1]['label']=='first';rows=r[-1]['response']['result']['data_array'];wanted=a['expected'];assert rows==[[x[0].capitalize(),*x[1:]] for x in wanted]
w=WorkspaceClient(profile='aidev-cus');start=time.monotonic()
for i in range(20):
 try:h=collect_history(w,r,O/'recovered-history-r563.json');break
 except HistoryPending:
  if i==19:raise
  time.sleep(1)
for x in r:
 q=h[x['statement_id']];assert q['status']=='FINISHED' and q['is_final'] and q['query_text']=='/* ashlar '+O.name+' '+x['label']+' */ '+x['sql']
cost={k:sum(q['metrics'].get(k,0) or 0 for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};assert all(v<=a['bounds'][k] for k,v in cost.items())
p=O/'source-plan.txt';rec=next(x for x in r if x['label']=='source-plan');text='\n'.join(str(x[0]) for x in rec['response']['result']['data_array'])+'\n';assert text==p.read_text() and hashlib.sha256(text.encode()).hexdigest()==a['plans']['source-plan']['sha256']
source=B/'out/sixth-cdf-image-oracle-r529.json';oracle=json.loads(source.read_text())['roles']['edge_current'];assert hashlib.sha256(source.read_bytes()).hexdigest()==a['sources']['cdf_oracle']
assert wanted[0][3:]==[oracle['images']['update_preimage']['digest'],oracle['images']['update_postimage']['digest']] and wanted[1][3]==oracle['images']['delete']['digest']
result={'state':'First complete mutation source probe passes all carrier digests; harness Boolean casing stop recovered without replay','runner_sha256':hashlib.sha256((B/'mutation_source_probe_r562.py').read_bytes()).hexdigest(),'native_statements':len(r),'first_statement_id':r[-1]['statement_id'],'first_caller_ms':r[-1]['wall_ms'],'first_metrics':h[r[-1]['statement_id']]['metrics'],'result':rows,'costs':cost,'original_stopped_wall_s':a.get('wall_s'),'recovery_s':time.monotonic()-start,'qualification':a['qualification'] if 'qualification' in a else 'One full20field before/after aggregate over immutable sixth normalized source inputs; no target scan or mutation. Hash/group/sort validation adds work. No repeat ran, no warm p95 or naked materialization clock. Driver Boolean rendering False/True exactly qualified; original failed assertion retained.'}
(B/'out/mutation-source-recovery-r563.json').write_text(json.dumps(result,indent=2)+'\n');(O/'inflight-request.json').rename(O/'completed-last-request-r563.json');print(json.dumps({k:result[k] for k in ['state','costs','first_caller_ms','first_metrics']},indent=2))
