"""Recover correlation for lost driver submission; no validation or write replay."""
import json,time
from pathlib import Path
from persistent_sql import Client
from bounded_reads_r145 import BoundedReads
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_queue_r189';Q=O/'transport-audit';assert not (Q/'versions/statements.jsonl').exists(),'Inspect prior version-read handles'
s=json.loads((O/'summary.json').read_text());records=[json.loads(x) for x in (O/'statements.jsonl').read_text().splitlines()];lost=records[-1];assert lost['label']=='intended-r189-b1' and lost['statement_id'] is None
w=Client(Q);tag='/* ashlar ashlar_queue_r189 intended-r189-b1 */';matches=[]
for attempt in range(8):
 res=w.w.api_client.do('GET','/api/2.0/sql/history/queries',query={'max_results':1000,'include_metrics':'true'})
 matches=[x for x in res.get('res',[]) if x.get('query_text','').startswith(tag)]
 (Q/'correlation-history.json').write_text(json.dumps(matches,indent=2)+'\n')
 if len(matches)==1 and matches[0].get('is_final'):break
 time.sleep(2)
assert len(matches)<=1 and (not matches or matches[0]['is_final']),'Inspect same native correlation; no replay'
observed=matches[0] if matches else {'query_id':None,'status':'NOT_OBSERVED_IN_BOUNDED_HISTORY_SEARCH','is_final':None,'metrics':{}}
c=BoundedReads(Q/'versions');c.sql('timeout','SET STATEMENT_TIMEOUT=30');versions={}
for i,t in enumerate(s['owned_tables']):versions[t]=c.sql('version-'+str(i),'DESCRIBE HISTORY '+t+' LIMIT 1')[0][0];assert versions[t]=='0'
M=s['owned_tables'][-1];assert c.sql('manifest-count','SELECT count(*) FROM '+M)==[['0']]
pins=c.sql('publication',"SELECT table_versions_json FROM client_dev.ashlar_entropy_20261006_r86.publication_manifest_r89 WHERE publication_id='r139-b1'");assert json.loads(pins[0][0])['client_dev.ashlar_entropy_20261006_r86.edge_current']==23
c.close()
for attempt in range(8):
 h={x['query_id']:x for x in c.history()}
 if all(r['statement_id'] in h and h[r['statement_id']]['is_final'] for r in c.records):break
 time.sleep(2)
assert all(h[r['statement_id']]['status']=='FINISHED' and h[r['statement_id']]['is_final'] for r in c.records)
s.update(state='Integrated range-input publication stopped before write phase; owned version0 verified; validation submission unresolved', lost_submission={'query_id':observed['query_id'],'status':observed['status'],'is_final':observed['is_final'],'metrics':observed.get('metrics',{}),'result_qualification':'No matching native query or validation result was observed; intended zero-difference proof is unavailable.'},owned_versions=versions,qualification='Driver UNKNOWN/null ID preserved; bounded history search finds no native statement for the unique intended tag; absence is not proof of server nonexecution. All owned clone/manifest heads remain0 and manifest empty; no raw/journal/current publication write admitted. No replay. Stage exists for a later separately validated run. No integrated performance claim. Original preparing summary is preserved, not treated as successful publication.')
allhist={observed['query_id']:observed} if matches else {}
for D in [O,O/'journal-lane',O/'apply-lane']:
 z=Client(D);z.records=[json.loads(x) for x in (D/'statements.jsonl').read_text().splitlines()];allhist.update({x['query_id']:x for x in z.history()})
allhist.update(h);s['cost_qualification']='Known recorded native queries only; unavailable lost-submission work cannot be priced as zero.'
s['audited_costs']={k:sum(x.get('metrics',{}).get(k,0) for x in allhist.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']}
(O/'transport-audited-summary.json').write_text(json.dumps(s,indent=2)+'\n');print(json.dumps({'state':s['state'],'lost':s['lost_submission'],'costs':s['audited_costs']},indent=2))
