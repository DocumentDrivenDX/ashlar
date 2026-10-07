"""Read-only native EXPLAIN of the selected-source target join; no MERGE execution."""
import json,hashlib
from pathlib import Path
from persistent_sql import Client
from normalized_apply_sql_r276 import mutation_source,pin
B=Path(__file__).resolve().parent

def main():
 p=B/'out/native/ashlar_merge_plan_r295';assert not p.exists();p.mkdir()
 base=json.loads((B/'out/native/ashlar_scale_edges_r274/audited-summary.json').read_text());inputs=json.loads((B/'out/native/ashlar_normalized_delta_stage_r275/audited-summary.json').read_text());e=base['tables']['edge_current'];r=inputs['tables']['source_record'];a=inputs['tables']['current_replacement'];source=mutation_source(r['table'],r['version'],a['table'],a['version']);c=Client(p,observation_timeout=120,cancel_after=60)
 plans={}
 for name,hint in [('default',''),('broadcast-source','/*+ BROADCAST(s) */')]:
  q=f"EXPLAIN FORMATTED SELECT {hint} b.*,s.is_delete FROM {pin(e['table'],e['version'])} b INNER JOIN ({source}) s ON b.lookup_hash=s.lookup_hash AND b.source_system=s.source_system AND b.rel_type_id=s.rel_type_id AND b.id=s.id WHERE b.entity_version=1 AND b.apply_batch_id='mixed-bootstrap'"
  rows=c.sql(name,q);plans[name]='\n'.join(str(x[0]) for x in rows)
 history=c.history();assert all(r['response']['status']['state']=='SUCCEEDED' for r in c.records)
 result={'format':'ashlar-merge-join-plan/1','state':'Two read-only native EXPLAIN plans captured','base':e,'inputs':inputs['tables'],'plans':plans,'native_history_final':all(q.get('is_final') and q.get('status')=='FINISHED' for q in history) and len(history)==len(c.records),'qualification':'Read-only SELECT join plans at pinned snapshots; EXPLAIN neither executes MERGE nor proves runtime file pruning, broadcast memory suitability, or performance. Target/source semantic join matches mutation lookup; actual Delta MERGE planning may differ.'}
 (p/'summary.json').write_text(json.dumps(result,indent=2)+'\n');(p/'live-statement.json').unlink();print(json.dumps({k:len(v) for k,v in plans.items()}))
if __name__=='__main__':main()
